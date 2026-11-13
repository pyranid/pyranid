/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class TraditionalTransactionInterruptionTests {
	@AfterEach
	void clearInterruption() {
		Thread.interrupted();
	}

	@Test
	void handledIndependentCancellationLeavesSurvivingSavepointWorkCommittable() {
		for (String boundary : List.of("same-database", "other-database", "other-retry")) {
			Fixture outer = new Fixture(null), other = new Fixture(null);
			AtomicInteger policies = new AtomicInteger(), attempts = new AtomicInteger();
			InterruptedException cancellation = new InterruptedException("inner operation cancelled");
			DatabaseException retryFailure = new DatabaseException("cancelled serialization",
					new SQLException("serialization", "40001", cancellation));
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				policies.incrementAndGet();
				return true;
			});
			AtomicReference<TransactionCompletion> completed = new AtomicReference<>();
			TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completed::set).build();
			IllegalStateException businessRollback = new IllegalStateException("discard the attempted branch");

			outer.database.transaction(options, () -> {
				outer.insert(1);
				Transaction transaction = outer.database.currentTransaction().orElseThrow();
				assertSame(businessRollback, assertThrows(IllegalStateException.class, () -> transaction.withSavepoint(() -> {
					outer.insert(2);
					RuntimeException handled = assertThrows(RuntimeException.class, () -> {
						if (boundary.equals("other-retry")) {
							other.database.transactionWithRetry(policy, () -> {
								attempts.incrementAndGet();
								throw retryFailure;
							});
						} else {
							Database nested = boundary.equals("same-database") ? outer.database : other.database;
							nested.transaction(() -> { throw cancellation; });
						}
					});
					if (boundary.equals("other-retry")) assertSame(retryFailure, handled);
					else assertSame(cancellation, handled.getCause());
					assertFalse(Thread.currentThread().isInterrupted());
					throw businessRollback;
				})));
				outer.insert(3);
			});

			assertFalse(Thread.currentThread().isInterrupted());
			assertEquals(TransactionResult.COMMITTED, completed.get().getResult(), boundary);
			assertTrue(completed.get().getFailure().isEmpty());
			assertEquals(List.of(1, 3), outer.ids(), boundary);
			assertEquals(0, policies.get());
			assertEquals(boundary.equals("other-retry") ? 1 : 0, attempts.get());
		}
	}

	@Test
	void connectionCleanupHooksAndObservationPrecedeFailureDerivedPublication() {
		for (String kind : List.of("checked", "runtime", "error")) {
			Fixture fixture = new Fixture(null), audit = new Fixture(null);
			InterruptedException cancellation = new InterruptedException("callback cancelled");
			Throwable primary = switch (kind) {
				case "runtime" -> new IllegalStateException("callback cancelled", cancellation);
				case "error" -> new AssertionError("callback cancelled", cancellation);
				default -> cancellation;
			};
			AtomicInteger hooks = new AtomicInteger(), observations = new AtomicInteger();
			TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completion -> {
				assertFalse(Thread.currentThread().isInterrupted());
				assertEquals(List.of(1), audit.ids());
				observations.incrementAndGet();
			}).build();
			try {
				Throwable thrown = assertThrows(Throwable.class, () -> fixture.database.transaction(options, () -> {
					fixture.insert(1);
					fixture.database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> {
						assertFalse(Thread.currentThread().isInterrupted());
						audit.database.transaction(() -> audit.insert(1));
						hooks.incrementAndGet();
					});
					if (primary instanceof Error error) throw error;
					throw (Exception) primary;
				}));
				if (primary instanceof InterruptedException) assertSame(primary, thrown.getCause());
				else assertSame(primary, thrown);
				assertTrue(Thread.currentThread().isInterrupted());
				assertEquals(List.of(false), fixture.interruptedAtClose);
				assertEquals(1, hooks.get());
				assertEquals(1, observations.get());
			} finally {
				Thread.interrupted();
			}
			assertEquals(List.of(), fixture.ids());
		}
	}

	@Test
	void handledCleanupAndHookCancellationAlsoRemainUnpublishedInsideAnEnclosingTransaction() {
		for (String phase : List.of("cleanup", "hook")) {
			InterruptedException cancellation = new InterruptedException("inner " + phase + " cancelled");
			Fixture outer = new Fixture(null);
			Fixture inner = new Fixture(phase.equals("cleanup") ? new SQLException("close failed", cancellation) : null);
			IllegalStateException businessRollback = new IllegalStateException("discard the attempted branch");
			outer.database.transaction(() -> {
				outer.insert(1);
				assertSame(businessRollback, assertThrows(IllegalStateException.class,
						() -> outer.database.currentTransaction().orElseThrow().withSavepoint(() -> {
							outer.insert(2);
							RuntimeException handled = assertThrows(RuntimeException.class, () -> inner.database.transaction(() -> {
								inner.insert(1);
								if (phase.equals("hook")) inner.database.currentTransaction().orElseThrow()
										.addPostTransactionOperation(result -> { throw new IllegalStateException(cancellation); });
							}));
							assertEquals(1, ThrowableGraph.inspect(handled, node -> node == cancellation ? 1 : 0, 0));
							assertFalse(Thread.currentThread().isInterrupted());
							throw businessRollback;
						})));
				outer.insert(3);
			});
			assertFalse(Thread.currentThread().isInterrupted());
			assertEquals(List.of(1, 3), outer.ids());
			assertEquals(List.of(1), inner.ids());
		}
	}

	@Test
	void finalBoundaryErrorsRetainTheirPublicationContractInsideAnEnclosingTransaction() {
		for (boolean outcomeApi : new boolean[]{false, true}) {
			Fixture outer = new Fixture(null), inner = new Fixture(null);
			AssertionError fatal = new AssertionError("fatal cancellation", new InterruptedException("cancelled"));
			AtomicBoolean observedBeforePublication = new AtomicBoolean();
			TransactionOptions options = TransactionOptions.withReadOnly(null)
					.completionObserver(completion -> observedBeforePublication.set(!Thread.currentThread().isInterrupted())).build();
			outer.database.transaction(() -> {
				assertSame(fatal, assertThrows(AssertionError.class, () -> {
					if (outcomeApi) inner.database.transactionWithOutcome(options, () -> { throw fatal; });
					else inner.database.transaction(options, () -> { throw fatal; });
				}));
				assertTrue(Thread.interrupted());
				outer.insert(1);
			});
			assertTrue(observedBeforePublication.get());
			assertEquals(List.of(1), outer.ids());
		}
	}

	@Test
	void participationKeepsRollbackOnlyWithoutPublishingInsideTheOwningThread() {
		Fixture fixture = new Fixture(null);
		AtomicReference<TransactionCompletion> completed = new AtomicReference<>();
		TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completed::set).build();
		fixture.database.transaction(options, () -> {
			fixture.insert(1);
			Transaction transaction = fixture.database.currentTransaction().orElseThrow();
			InterruptedException cancellation = new InterruptedException("participant cancelled");
			RuntimeException handled = assertThrows(RuntimeException.class,
					() -> fixture.database.participate(transaction, () -> { throw cancellation; }));
			assertSame(cancellation, handled.getCause());
			assertTrue(transaction.isRollbackOnly());
			assertFalse(Thread.currentThread().isInterrupted());
			fixture.insert(2);
		});
		assertEquals(TransactionResult.ROLLED_BACK, completed.get().getResult());
		assertTrue(completed.get().getFailure().isEmpty());
		assertFalse(Thread.currentThread().isInterrupted());
		assertEquals(List.of(), fixture.ids());
	}

	@Test
	void participationOnAnUnenclosedWorkerRestoresAfterRemovingItsBinding() throws Exception {
		Fixture fixture = new Fixture(null);
		ExecutorService worker = Executors.newSingleThreadExecutor();
		try {
			fixture.database.transaction(() -> {
				fixture.insert(1);
				Transaction transaction = fixture.database.currentTransaction().orElseThrow();
				assertTrue(worker.submit(() -> {
					try {
						assertThrows(RuntimeException.class, () -> fixture.database.participate(transaction,
								() -> { throw new InterruptedException("worker cancelled"); }));
						assertTrue(fixture.database.currentTransaction().isEmpty());
						return Thread.currentThread().isInterrupted();
					} finally {
						Thread.interrupted();
					}
				}).get(10, TimeUnit.SECONDS));
				assertTrue(transaction.isRollbackOnly());
				assertFalse(Thread.currentThread().isInterrupted());
			});
		} finally {
			worker.shutdownNow();
			assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
		}
		assertEquals(List.of(), fixture.ids());
	}

	@Test
	void retryGraphCancellationBlocksPolicyWithoutPublishingAFlag() {
		for (String edge : List.of("cause", "suppressed", "next", "cycle")) {
			InterruptedException cancellation = new InterruptedException("cancelled " + edge);
			SQLException sql = new SQLException("serialization", "40001");
			if (edge.equals("cause") || edge.equals("cycle")) sql.initCause(cancellation);
			if (edge.equals("next")) sql.setNextException(new SQLException("detail", "40001", cancellation));
			if (edge.equals("cycle")) cancellation.initCause(sql);
			DatabaseException failure = new DatabaseException("cancelled serialization", sql);
			if (edge.equals("suppressed")) failure.addSuppressed(cancellation);
			AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), ignored -> {
				policies.incrementAndGet();
				return true;
			});
			assertSame(failure, assertThrows(DatabaseException.class, () -> executor(delay -> fail()).execute(policy, () -> {
				attempts.incrementAndGet();
				throw failure;
			})));
			assertEquals(1, attempts.get(), edge);
			assertEquals(0, policies.get(), edge);
			assertFalse(Thread.currentThread().isInterrupted());
		}
	}

	@Test
	void backoffCancellationRemainsUnpublishedInsideAnEnclosingTransaction() {
		Fixture outer = new Fixture(null), inner = new Fixture(null);
		AtomicInteger policies = new AtomicInteger(), attempts = new AtomicInteger(), sleeps = new AtomicInteger();
		InterruptedException cancellation = new InterruptedException("backoff cancelled");
		DatabaseException failure = new DatabaseException("serialization", new SQLException("serialization", "40001"));
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ofMillis(1)), ignored -> {
			policies.incrementAndGet();
			return true;
		});
		RetryExecutor retries = executor(delay -> {
			sleeps.incrementAndGet();
			// Interrupted sleep reports cancellation with the thread flag cleared.
			throw cancellation;
		});
		IllegalStateException businessRollback = new IllegalStateException("discard the attempted branch");
		outer.database.transaction(() -> {
			outer.insert(1);
			assertSame(businessRollback, assertThrows(IllegalStateException.class,
					() -> outer.database.currentTransaction().orElseThrow().withSavepoint(() -> {
						outer.insert(2);
						assertSame(failure, assertThrows(DatabaseException.class, () -> retries.execute(policy,
								() -> inner.database.transaction(() -> {
									attempts.incrementAndGet();
									throw failure;
								}))));
						assertSame(cancellation, failure.getSuppressed()[0]);
						assertFalse(Thread.currentThread().isInterrupted());
						throw businessRollback;
					})));
			outer.insert(3);
		});
		assertEquals(1, attempts.get());
		assertEquals(1, policies.get());
		assertEquals(1, sleeps.get());
		assertFalse(Thread.currentThread().isInterrupted());
		assertEquals(List.of(1, 3), outer.ids());
	}

	@Test
	void cancellationAddedByPolicyBackoffOrSleepCannotAdmitAnotherAttempt() {
		for (String phase : List.of("condition", "backoff", "sleep")) {
			DatabaseException failure = new DatabaseException("serialization", new SQLException("serialization", "40001"));
			InterruptedException cancellation = new InterruptedException("cancelled during " + phase);
			AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger(), backoffs = new AtomicInteger(), sleeps = new AtomicInteger();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, (attempt, ignored) -> {
				backoffs.incrementAndGet();
				if (phase.equals("backoff")) failure.addSuppressed(cancellation);
				return phase.equals("sleep") ? Duration.ofMillis(1) : Duration.ZERO;
			}, ignored -> {
				policies.incrementAndGet();
				if (phase.equals("condition")) failure.addSuppressed(cancellation);
				return true;
			});
			RetryExecutor executor = executor(delay -> {
				sleeps.incrementAndGet();
				failure.addSuppressed(cancellation);
			});
			assertSame(failure, assertThrows(DatabaseException.class, () -> executor.execute(policy, () -> {
				attempts.incrementAndGet();
				throw failure;
			})));
			assertEquals(1, attempts.get(), phase);
			assertEquals(1, policies.get(), phase);
			assertEquals(phase.equals("condition") ? 0 : 1, backoffs.get(), phase);
			assertEquals(phase.equals("sleep") ? 1 : 0, sleeps.get(), phase);
			assertFalse(Thread.currentThread().isInterrupted());
		}
	}

	@Test
	void retryInspectionRetainsCancellationConsumedOrHiddenByLaterAccessors() {
		for (boolean transientFlag : new boolean[]{false, true}) {
			AtomicBoolean armed = new AtomicBoolean();
			AtomicInteger reads = new AtomicInteger(), policies = new AtomicInteger();
			InterruptedException cancellation = new InterruptedException("one-time cancellation report");
			SQLException sql = new SQLException("serialization", "40001") {
				@Override public Throwable getCause() {
					if (!armed.get()) return null;
					if (transientFlag) {
						Thread.currentThread().interrupt();
						return null;
					}
					return reads.getAndIncrement() == 0 ? cancellation : null;
				}
				@Override public SQLException getNextException() {
					if (armed.get() && transientFlag) Thread.interrupted();
					return null;
				}
			};
			DatabaseException failure = new DatabaseException("serialization", sql);
			armed.set(true);
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), ignored -> {
				policies.incrementAndGet();
				return true;
			});
			assertSame(failure, assertThrows(DatabaseException.class, () -> executor(delay -> fail()).execute(policy, () -> { throw failure; })));
			assertEquals(0, policies.get());
			assertFalse(Thread.currentThread().isInterrupted());
			Throwable retained = failure.getSuppressed()[0];
			assertInstanceOf(InterruptedException.class, retained);
			if (!transientFlag) assertSame(cancellation, retained);
			armed.set(false);
			Database.restoreInterruptIfNeeded(failure);
			assertTrue(Thread.interrupted());
		}
	}

	private static RetryExecutor executor(RetryExecutor.Sleeper sleeper) {
		return new RetryExecutor(() -> 0, sleeper);
	}

	private static final class Fixture {
		final Database database, inspection;
		final List<Boolean> interruptedAtClose = new ArrayList<>();

		Fixture(SQLException closeFailure) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:traditional_interruption_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			this.inspection = Database.with(source, DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work(id INT PRIMARY KEY)").execute();
			DataSource wrapped = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> {
						Object value = invoke(source, method, args);
						if (!(value instanceof Connection connection)) return value;
						return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
								(handle, operation, parameters) -> {
									if (operation.getName().equals("close")) this.interruptedAtClose.add(Thread.currentThread().isInterrupted());
									Object result = invoke(connection, operation, parameters);
									if (operation.getName().equals("close") && closeFailure != null) throw closeFailure;
									return result;
								});
					});
			this.database = Database.with(wrapped, DatabaseType.GENERIC).build();
		}

		void insert(int id) {
			this.database.query("INSERT INTO work VALUES (:id)").bind("id", id).execute();
		}

		List<Integer> ids() {
			return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class);
		}
	}

	private static Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
		try {
			return method.invoke(target, arguments);
		} catch (InvocationTargetException failure) {
			throw failure.getCause();
		}
	}
}
