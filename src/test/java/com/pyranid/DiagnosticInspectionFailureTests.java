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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DiagnosticInspectionFailureTests {
	@AfterEach
	void clearInterruptFlag() { Thread.interrupted(); }

	@Test
	void externalInterruptDuringOneShotSqlStateInspectionCannotAdmitAnotherAttempt() throws Exception {
		for (boolean untyped : new boolean[]{false, true}) {
			for (boolean persistent : new boolean[]{false, true}) {
				CountDownLatch entered = new CountDownLatch(1);
				AtomicBoolean first = new AtomicBoolean(true);
				AtomicReference<RuntimeException> diagnostic = new AtomicReference<>();
				SQLException original = new SQLException("invalid temporal value", "HY000") {
					@Override public String getSQLState() {
						if (first.compareAndSet(true, false)) {
							entered.countDown();
							try {
								new CountDownLatch(1).await(5, TimeUnit.SECONDS);
								throw new AssertionError("No external interruption arrived");
							} catch (InterruptedException interruption) {
								RuntimeException failure = new IllegalStateException("interrupted JDBC diagnostic", interruption);
								diagnostic.set(failure);
								throw failure;
							}
						}
						if (persistent) throw diagnostic.get();
						return super.getSQLState();
					}
				};
				Fixture fixture = new Fixture(untyped, original, null);
				AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
				AtomicReference<Throwable> thrown = new AtomicReference<>();
				AtomicBoolean interruptedInside = new AtomicBoolean(), interruptedAtExit = new AtomicBoolean();
				Thread worker = new Thread(() -> {
					try {
						fixture.database.transactionWithRetry(retry(policyCalls), () -> {
							attempts.incrementAndGet();
							try { fixture.executeStatements(); }
							catch (RuntimeException failure) {
								interruptedInside.set(Thread.currentThread().isInterrupted());
								throw failure;
							}
						});
					} catch (Throwable failure) { thrown.set(failure); }
					finally { interruptedAtExit.set(Thread.currentThread().isInterrupted()); }
				}, "diagnostic-inspection-worker");
				worker.setDaemon(true);
				worker.start();
				try {
					assertTrue(entered.await(5, TimeUnit.SECONDS));
					worker.interrupt();
					worker.join(5_000);
					assertFalse(worker.isAlive(), "Worker must leave after cleanup");
				} finally {
					worker.interrupt();
					worker.join(5_000);
				}

				DatabaseException failure = assertInstanceOf(DatabaseException.class, thrown.get());
				assertSame(original, failure.getCause());
				assertTrue(contains(failure, diagnostic.get()));
				assertFalse(interruptedInside.get(), "Inspection must not publish interruption inside the transaction");
				assertTrue(interruptedAtExit.get());
				assertEquals(1, attempts.get());
				assertEquals(0, policyCalls.get());
				fixture.assertAborted();
			}
		}
	}

	@Test
	void oneShotCauseAndNextInspectionFailuresRetainCancellation() {
		for (boolean untyped : new boolean[]{false, true}) {
			for (String accessor : List.of("cause", "next")) {
				AtomicBoolean first = new AtomicBoolean(true);
				RuntimeException diagnostic = cancelledDiagnostic(accessor);
				SQLException original = new SQLException("invalid temporal value", "HY000") {
					@Override public Throwable getCause() {
						if (accessor.equals("cause") && first.compareAndSet(true, false)) throw diagnostic;
						return super.getCause();
					}
					@Override public SQLException getNextException() {
						if (accessor.equals("next") && first.compareAndSet(true, false)) throw diagnostic;
						return super.getNextException();
					}
				};
				assertCancelledBinding(untyped, original, diagnostic);
			}
		}
	}

	@Test
	void messageInspectionCannotForgetCancellationOrAcceptLaterUnsupportedEvidence() {
		for (boolean untyped : new boolean[]{false, true}) {
			for (boolean persistent : new boolean[]{false, true}) {
				for (boolean positiveNext : new boolean[]{false, true}) {
					AtomicInteger messages = new AtomicInteger();
					RuntimeException diagnostic = cancelledDiagnostic("message");
					SQLException original = new SQLException("invalid temporal value", "HY000") {
						@Override public String getMessage() {
							if (messages.getAndIncrement() == 0 || persistent) throw diagnostic;
							return super.getMessage();
						}
					};
					if (positiveNext) original.setNextException(new SQLFeatureNotSupportedException("unsupported detail", "0A000"));
					assertCancelledBinding(untyped, original, diagnostic);
				}
			}
		}
	}

	@Test
	void privateClassifierAccessorFailuresFailClosedAfterTheInitialGuard() {
		for (boolean untyped : new boolean[]{false, true}) {
			for (String accessor : List.of("state", "cause", "next")) {
				RuntimeException diagnostic = cancelledDiagnostic(accessor);
				SQLException original;
				if (accessor.equals("state")) {
					AtomicInteger reads = new AtomicInteger();
					original = new SQLException("not implemented", "HY000") {
						@Override public String getSQLState() {
							if (reads.incrementAndGet() == 2) throw diagnostic;
							return super.getSQLState();
						}
					};
				} else original = classifierEdgeFailure(accessor, diagnostic);
				assertCancelledBinding(untyped, original, diagnostic);
			}
		}
	}

	@Test
	void diagnosticOutcomeEvidenceCannotBeLostBeforeRetryAdmission() {
		for (boolean committed : new boolean[]{false, true}) {
			DatabaseException diagnostic = new DatabaseException("diagnostic carries an unsafe outcome");
			if (committed) diagnostic.markTransactionOutcomeCommitted();
			else diagnostic.markTransactionOutcomeIndeterminate();
			AtomicBoolean first = new AtomicBoolean(true);
			SQLException original = new SQLException("invalid temporal value", "HY000") {
				@Override public String getSQLState() {
					if (first.compareAndSet(true, false)) throw diagnostic;
					return super.getSQLState();
				}
			};
			Fixture fixture = new Fixture(false, original, null);
			AtomicInteger policyCalls = new AtomicInteger();

			DatabaseException failure = assertThrows(DatabaseException.class,
					() -> fixture.database.transactionWithRetry(retry(policyCalls), fixture::executeStatements));

			assertSame(original, failure.getCause());
			assertTrue(contains(failure, diagnostic));
			assertEquals(committed, failure.isTransactionOutcomeCommitted());
			assertEquals(!committed, failure.isTransactionOutcomeIndeterminate());
			assertEquals(0, policyCalls.get());
			fixture.assertAborted();
		}
	}

	@Test
	void fatalDiagnosticAndCleanupRemainAttachedToTheOriginalOperationFailure() {
		AssertionError diagnostic = new AssertionError("fatal diagnostic");
		SQLException cleanup = new SQLException("statement close failed", "HY000");
		AtomicBoolean first = new AtomicBoolean(true);
		SQLException original = new SQLException("invalid temporal value", "HY000") {
			@Override public String getSQLState() {
				if (first.compareAndSet(true, false)) throw diagnostic;
				return super.getSQLState();
			}
		};
		Fixture fixture = new Fixture(false, original, cleanup);

		DatabaseException failure = assertThrows(DatabaseException.class,
				() -> fixture.database.transaction(fixture::executeStatements));

		assertSame(original, failure.getCause());
		assertTrue(contains(failure, diagnostic));
		assertTrue(contains(failure, cleanup));
		fixture.assertAborted();
	}

	@Test
	void retainedDiagnosticsAreDeferredAndIdentityDeduplicatedAcrossScans() {
		AtomicInteger nestedReads = new AtomicInteger();
		RuntimeException diagnostic = new IllegalStateException("diagnostic has an unreadable cause") {
			@Override public Throwable getCause() { nestedReads.incrementAndGet(); throw this; }
		};
		SQLException original = new SQLException("invalid value", "HY000") {
			@Override public String getSQLState() { throw diagnostic; }
		};

		assertTrue(JdbcFallbacks.isTerminal(original));
		assertEquals(0, nestedReads.get(), "Do not traverse newly attached diagnostics during the same inspection");
		assertArrayEquals(new Throwable[]{diagnostic}, original.getSuppressed());
		assertTrue(JdbcFallbacks.isTerminal(original));
		assertEquals(1, nestedReads.get());
		assertArrayEquals(new Throwable[]{diagnostic}, original.getSuppressed());
		assertFalse(Thread.currentThread().isInterrupted());
	}

	@Test
	void aDeepUnreadableChainRetainsEveryDistinctDiagnosticOnce() {
		RuntimeException[] diagnostics = new RuntimeException[1_000];
		SQLException[] failures = new SQLException[diagnostics.length];
		for (int i = 0; i < failures.length; i++) {
			RuntimeException diagnostic = new IllegalStateException("diagnostic " + i);
			diagnostics[i] = diagnostic;
			failures[i] = new SQLException("driver failure " + i, "HY000") {
				@Override public String getSQLState() { throw diagnostic; }
			};
			if (i > 0) failures[i - 1].setNextException(failures[i]);
		}
		RuntimeException cleanup = new IllegalStateException("existing cleanup failure");
		failures[0].addSuppressed(cleanup);
		failures[0].addSuppressed(diagnostics[500]);

		assertTrue(JdbcFallbacks.isTerminal(failures[0]));

		Throwable[] retained = failures[0].getSuppressed();
		assertEquals(diagnostics.length + 1, retained.length);
		assertSame(cleanup, retained[0]);
		assertSame(diagnostics[500], retained[1]);
		java.util.Set<Throwable> identities = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
		java.util.Collections.addAll(identities, retained);
		assertEquals(retained.length, identities.size());
		for (Throwable diagnostic : diagnostics) assertTrue(identities.contains(diagnostic));
		assertFalse(Thread.currentThread().isInterrupted());
	}

	@Test
	void selfFailureAndCyclesCannotIntroduceSelfSuppressionOrRepeatedTraversal() {
		AssertionError original = new AssertionError("diagnostic throws its own operation failure") {
			@Override public Throwable getCause() { throw this; }
		};
		assertTrue(JdbcFallbacks.isTerminal(original));
		assertArrayEquals(new Throwable[0], original.getSuppressed());
		SQLException cyclic = new SQLException("unsupported feature", "0A000");
		cyclic.setNextException(cyclic);
		assertFalse(JdbcFallbacks.isTerminal(cyclic));
		assertArrayEquals(new Throwable[0], cyclic.getSuppressed());
	}

	@Test
	void ordinaryEvidenceInspectionAndCompletionObserversKeepTheirIsolationContract() {
		RuntimeException diagnostic = cancelledDiagnostic("isolated diagnostic");
		SQLException original = new SQLException("operation", "HY000") {
			@Override public String getSQLState() { throw diagnostic; }
		};
		assertEquals(1, ThrowableGraph.inspect(original, node -> {
			if (node instanceof SQLException sql) sql.getSQLState();
			return 0;
		}, 1));
		assertArrayEquals(new Throwable[0], original.getSuppressed());
		Fixture fixture = new Fixture(false, null, null);
		TransactionOptions options = TransactionOptions.withReadOnly(null)
				.completionObserver(completion -> { throw diagnostic; }).build();
		fixture.database.transaction(options, fixture::executeStatements);
		assertEquals(List.of(1, 2), fixture.ids());
		assertFalse(Thread.currentThread().isInterrupted());
	}

	@Test
	void readableBenignUnsupportedEvidenceStillUsesTheOriginalPositiveEdges() {
		for (boolean untyped : new boolean[]{false, true}) {
			SQLException cause = new SQLException("invalid value", "HY000", new SQLException("unsupported", "0A000"));
			SQLException next = new SQLException("invalid value", "HY000");
			next.setNextException(new SQLFeatureNotSupportedException("unsupported", "0A000"));
			SQLException cycle = new SQLException("invalid value", "HY000");
			SQLException detail = new SQLException("not implemented", "HY000");
			cycle.setNextException(detail); detail.setNextException(cycle);
			for (SQLException original : List.of(cause, next, cycle)) {
				Fixture fixture = new Fixture(untyped, original, null);
				fixture.database.transaction(fixture::executeStatements);
				assertEquals(List.of(1, 2), fixture.ids());
				assertEquals(1, fixture.fallbacks.get());
				assertEquals(1, fixture.commits.get());
				assertFalse(Thread.currentThread().isInterrupted());
			}
		}
	}

	private static SQLException classifierEdgeFailure(String accessor, RuntimeException diagnostic) {
		AtomicInteger reads = new AtomicInteger();
		SQLException original = new SQLException("invalid value", "HY000") {
			@Override public Throwable getCause() {
				if (accessor.equals("cause") && reads.incrementAndGet() == 2) throw diagnostic;
				return super.getCause();
			}
			@Override public SQLException getNextException() {
				if (accessor.equals("next") && reads.incrementAndGet() == 2) throw diagnostic;
				return super.getNextException();
			}
		};
		if (accessor.equals("cause")) original.initCause(new SQLFeatureNotSupportedException("unsupported detail", "0A000"));
		else original.setNextException(new SQLFeatureNotSupportedException("unsupported detail", "0A000"));
		return original;
	}

	private static RuntimeException cancelledDiagnostic(String name) {
		return new IllegalStateException("interrupted " + name, new InterruptedException("cancelled"));
	}

	private static RetryPolicy retry(AtomicInteger calls) {
		return RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			calls.incrementAndGet(); return true;
		});
	}

	private static boolean contains(Throwable failure, Throwable original) {
		return ThrowableGraph.inspect(failure, node -> node == original ? 1 : 0, 0) != 0;
	}

	private static void assertCancelledBinding(boolean untyped, SQLException original, Throwable diagnostic) {
		Fixture fixture = new Fixture(untyped, original, null);
		AtomicInteger attempts = new AtomicInteger(), calls = new AtomicInteger();
		AtomicBoolean insideInterrupted = new AtomicBoolean();
		DatabaseException thrown = assertThrows(DatabaseException.class,
				() -> fixture.database.transactionWithRetry(retry(calls), () -> {
					attempts.incrementAndGet();
					try { fixture.executeStatements(); }
					catch (RuntimeException failure) {
						insideInterrupted.set(Thread.currentThread().isInterrupted()); throw failure;
					}
				}));
		assertTrue(Thread.interrupted());
		assertSame(original, thrown.getCause());
		assertTrue(contains(thrown, diagnostic));
		assertFalse(insideInterrupted.get());
		assertEquals(1, attempts.get());
		assertEquals(0, calls.get());
		fixture.assertAborted();
	}

	private static final class Fixture {
		private final Database database, inspection;
		private final AtomicInteger fallbacks = new AtomicInteger(), secondExecutions = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger();
		private final AtomicInteger statementCloses = new AtomicInteger(), connectionCloses = new AtomicInteger();

		private Fixture(boolean untyped, Throwable failure, SQLException cleanup) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:diagnostic_inspection_" + UUID.randomUUID());
			source.setUser("sa"); source.setPassword("");
			this.inspection = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work(id INT PRIMARY KEY, value_timestamp TIMESTAMP)").execute();
			AtomicBoolean armed = new AtomicBoolean(failure != null);
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object value = invoke(source, method, args);
				if (!(value instanceof Connection connection)) return value;
				return proxy(Connection.class, (operation, parameters) -> {
					String operationName = operation.getName();
					if (operationName.equals("commit")) this.commits.incrementAndGet();
					if (operationName.equals("rollback")) this.rollbacks.incrementAndGet();
					if (operationName.equals("close")) this.connectionCloses.incrementAndGet();
					Object result = invoke(connection, operation, parameters);
					if (!(result instanceof PreparedStatement statement)) return result;
					boolean second = String.valueOf(parameters[0]).contains("VALUES (2,");
					return proxy(PreparedStatement.class, (jdbc, values) -> {
						String name = jdbc.getName();
						if (second && name.equals("setObject") && armed.compareAndSet(true, false)) throw failure;
						if (second && name.equals("setTimestamp")) this.fallbacks.incrementAndGet();
						if (second && (name.equals("executeLargeUpdate") || name.equals("executeUpdate"))) this.secondExecutions.incrementAndGet();
						if (name.equals("close")) {
							this.statementCloses.incrementAndGet();
							Object closeResult = invoke(statement, jdbc, values);
							if (second && cleanup != null) throw cleanup;
							return closeResult;
						}
						return invoke(statement, jdbc, values);
					});
				});
			});
			this.database = Database.withDataSource(wrapped).databaseType(untyped ? DatabaseType.DUCK_DB : DatabaseType.GENERIC).build();
		}

		private void executeStatements() {
			this.database.query("INSERT INTO work VALUES (1, TIMESTAMP '2020-01-01 00:00:00')").execute();
			this.database.query("INSERT INTO work VALUES (2, :value)")
					.bind("value", LocalDateTime.of(2024, 1, 2, 3, 4)).execute();
		}
		private List<Integer> ids() { return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class); }
		private void assertAborted() {
			assertEquals(List.of(), ids());
			assertEquals(0, this.fallbacks.get());
			assertEquals(0, this.secondExecutions.get());
			assertEquals(0, this.commits.get());
			assertEquals(1, this.rollbacks.get());
			assertEquals(2, this.statementCloses.get());
			assertEquals(1, this.connectionCloses.get());
			assertTrue(this.database.currentTransaction().isEmpty());
		}
	}

	@FunctionalInterface private interface Call { Object run(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> call.run(method, args)));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
