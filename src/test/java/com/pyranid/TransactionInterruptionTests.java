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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class TransactionInterruptionTests {
	@AfterEach
	void clearInterruptFlag() {
		Thread.interrupted();
	}

	@Test
	void zeroBackoffDoesNotRetryInterruptedPhysicalTransaction() {
		Fixture fixture = new Fixture(null);
		AtomicInteger attempts = new AtomicInteger();
		DatabaseException original = new DatabaseException("operation interrupted",
				new SQLException("aborted", "40001", new InterruptedException("request cancelled")));
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			fail("The retry condition must not see an interrupted attempt");
			return true;
		});

		assertSame(original, assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(policy, () -> {
			attempts.incrementAndGet();
			assertEquals(1, fixture.database.query("VALUES (1)").fetchObject(Integer.class).orElseThrow());
			throw original;
		})));
		assertEquals(1, attempts.get());
		assertEquals(1, fixture.rollbacks.get());
		assertEquals(1, fixture.closes.get());
		assertTrue(fixture.database.currentTransaction().isEmpty());
		assertTrue(Thread.currentThread().isInterrupted());
	}

	@Test
	void conditionInterruptionStopsBeforeBackoffAndRetainsEarlierFailure() {
		AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger(), backoffs = new AtomicInteger();
		DatabaseException first = new DatabaseException("first"), second = new DatabaseException("second");
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(3, (attempt, failure) -> {
			backoffs.incrementAndGet();
			return Duration.ZERO;
		}, failure -> {
			if (conditions.incrementAndGet() == 2) Thread.currentThread().interrupt();
			return true;
		});
		assertSame(second, assertThrows(DatabaseException.class, () -> executor(delay -> fail()).execute(policy, () -> {
			if (attempts.incrementAndGet() == 1) throw first;
			throw second;
		})));
		assertEquals(2, attempts.get());
		assertEquals(1, backoffs.get());
		assertArrayEquals(new Throwable[]{first}, second.getSuppressed());
		assertTrue(Thread.currentThread().isInterrupted());
	}

	@Test
	void backoffInterruptionStopsBeforeSleepEvenWhenDelayIsZero() {
		for (Duration delay : List.of(Duration.ZERO, Duration.ofMillis(1))) {
			AtomicInteger attempts = new AtomicInteger(), sleeps = new AtomicInteger();
			DatabaseException first = new DatabaseException("first"), second = new DatabaseException("second");
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(3, (attempt, failure) -> {
				if (attempt == 2) Thread.currentThread().interrupt();
				return delay;
			}, failure -> true);
			try {
				assertSame(second, assertThrows(DatabaseException.class, () -> executor(ignored -> sleeps.incrementAndGet())
						.execute(policy, () -> {
							if (attempts.incrementAndGet() == 1) throw first;
							throw second;
						})));
				assertEquals(2, attempts.get());
				assertEquals(delay.isZero() ? 0 : 1, sleeps.get());
				assertArrayEquals(new Throwable[]{first}, second.getSuppressed());
				assertTrue(Thread.currentThread().isInterrupted());
			} finally { Thread.interrupted(); }
		}
	}

	@Test
	void interruptAtEndOfBackoffCannotAdmitAnotherAttempt() {
		for (boolean bounded : List.of(false, true)) {
			AtomicInteger attempts = new AtomicInteger();
			DatabaseException original = new DatabaseException("original");
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ofMillis(1)), failure -> true);
			if (bounded) policy = policy.withMaxElapsedTime(Duration.ofSeconds(1));
			RetryPolicy finalPolicy = policy;
			try {
				assertSame(original, assertThrows(DatabaseException.class, () -> executor(delay -> Thread.currentThread().interrupt())
						.execute(finalPolicy, () -> {
							attempts.incrementAndGet();
							throw original;
						})));
				assertEquals(1, attempts.get());
				assertEquals(0, original.getSuppressed().length);
				assertTrue(Thread.currentThread().isInterrupted());
			} finally { Thread.interrupted(); }
		}
	}

	@Test
	void interruptedEntryStillAllowsInitialSuccessfulOperation() {
		Thread.currentThread().interrupt();
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> true);
		TransactionRetryResult<String> result = executor(delay -> fail()).execute(policy, () -> Optional.of("completed"));
		assertEquals(Optional.of("completed"), result.getValue());
		assertEquals(1, result.getAttemptCount());
		assertTrue(Thread.currentThread().isInterrupted());
	}

	@Test
	void cleanupInterruptionRestoresFlagForCommitAndRollbackWithoutChangingFailureOrOutcome() {
		for (boolean failBody : List.of(false, true)) {
			SQLException cleanup = new SQLException("close interrupted", new InterruptedException("release cancelled"));
			Fixture fixture = new Fixture(cleanup);
			RuntimeException primary = failBody ? new IllegalStateException("body failed") : null;
			try {
				RuntimeException thrown = assertThrows(RuntimeException.class, () -> fixture.run(primary, null));
				if (failBody) {
					assertSame(primary, thrown);
					assertSame(cleanup, thrown.getSuppressed()[0].getCause());
				} else {
					assertInstanceOf(DatabaseException.class, thrown);
					assertSame(cleanup, thrown.getCause());
				}
				fixture.assertCompleted(thrown, failBody);
				assertTrue(Thread.currentThread().isInterrupted());
			} finally { Thread.interrupted(); }
		}
	}

	@Test
	void postTransactionHookInterruptionRestoresFlagAndRemainingHooksStillRun() {
		for (boolean failBody : List.of(false, true)) {
			Fixture fixture = new Fixture(null);
			RuntimeException primary = failBody ? new IllegalStateException("body failed") : null;
			RuntimeException hookFailure = new IllegalStateException("hook interrupted", new InterruptedException("hook cancelled"));
			try {
				RuntimeException thrown = assertThrows(RuntimeException.class, () -> fixture.run(primary, hookFailure));
				Throwable hookWrapper = failBody ? thrown.getSuppressed()[0] : thrown;
				assertInstanceOf(PostTransactionOperationException.class, hookWrapper);
				assertSame(hookFailure, hookWrapper.getCause());
				if (failBody) assertSame(primary, thrown);
				assertEquals(1, fixture.laterHooks.get());
				fixture.assertCompleted(thrown, failBody);
				assertTrue(Thread.currentThread().isInterrupted());
			} finally { Thread.interrupted(); }
		}
	}

	@Test
	void interruptionSuppressedUnderEarlierCleanupFailureIsStillRestored() {
		SQLException cleanup = new SQLException("close failed");
		Fixture fixture = new Fixture(cleanup);
		RuntimeException hookFailure = new IllegalStateException("hook interrupted", new InterruptedException("hook cancelled"));
		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.run(null, hookFailure));
		assertSame(cleanup, thrown.getCause());
		assertSame(hookFailure, thrown.getSuppressed()[0].getCause());
		assertEquals(1, fixture.laterHooks.get());
		fixture.assertCompleted(thrown, false);
		assertTrue(Thread.currentThread().isInterrupted());
	}

	@Test
	void isolatedCompletionObserverFailureDoesNotIntroduceInterruptEvidence() {
		Fixture fixture = new Fixture(null);
		TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completion -> {
			throw new IllegalStateException("diagnostic failed", new InterruptedException("diagnostic cancelled"));
		}).build();
		fixture.database.transaction(options, () -> fixture.database.query("VALUES (1)").fetchObject(Integer.class));
		assertFalse(Thread.currentThread().isInterrupted());
		assertEquals(1, fixture.closes.get());
	}

	@Test
	void pendingInterruptSurvivesCompletionObserversWithoutThrowableEvidence() {
		for (boolean failBody : List.of(false, true)) {
			for (boolean observerThrows : List.of(false, true)) {
				Fixture fixture = new Fixture(null);
				AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger(), observations = new AtomicInteger();
				DatabaseException original = new DatabaseException("cancelled serialization", new SQLException("serialization", "40001"));
				RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
					conditions.incrementAndGet();
					return true;
				});
				TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completion -> {
					observations.incrementAndGet();
					assertTrue(Thread.currentThread().isInterrupted());
					try { Thread.sleep(1); }
					catch (InterruptedException interrupted) {
						if (observerThrows) throw new IllegalStateException("observer interrupted", interrupted);
					}
				}).build();
				ReturningTransactionalOperation<Integer> operation = () -> {
					attempts.incrementAndGet();
					assertEquals(1, fixture.database.query("VALUES (1)").fetchObject(Integer.class).orElseThrow());
					Thread.currentThread().interrupt();
					if (failBody) throw original;
					return Optional.of(1);
				};
				try {
					if (failBody) assertSame(original, assertThrows(DatabaseException.class,
							() -> fixture.database.transactionWithRetry(policy, options, operation)));
					else assertEquals(Optional.of(1), fixture.database.transactionWithRetry(policy, options, operation).getValue());
					assertEquals(1, attempts.get());
					assertEquals(0, conditions.get());
					assertEquals(1, observations.get());
					assertEquals(1, fixture.closes.get());
					assertEquals(failBody ? 1 : 0, fixture.rollbacks.get());
					assertTrue(Thread.currentThread().isInterrupted());
				} finally { Thread.interrupted(); }
			}
		}
	}

	@Test
	void transactionMetricsCannotConsumeCancellationBeforeRetryAdmission() {
		for (boolean consumeOnRollback : List.of(false, true)) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:metrics_interruption_" + UUID.randomUUID());
			source.setUser("sa"); source.setPassword("");
			AtomicInteger diagnostics = new AtomicInteger(), attempts = new AtomicInteger(), conditions = new AtomicInteger();
			MetricsCollector collector = new MetricsCollector() {
				@Override public void didRollbackPhysicalTransaction(Transaction transaction, DatabaseType type, Duration duration) {
					if (consumeOnRollback) consumeInterrupt();
				}
				@Override public void didExitTransactionClosure(Transaction transaction, TransactionClosureOutcome outcome,
						DatabaseType type, Duration duration, Throwable failure) {
					if (!consumeOnRollback) consumeInterrupt();
				}
				private void consumeInterrupt() {
					diagnostics.incrementAndGet();
					try { Thread.sleep(1); }
					catch (InterruptedException interrupted) { throw new IllegalStateException("metrics interrupted", interrupted); }
				}
			};
			Database database = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).metricsCollector(collector).build();
			DatabaseException original = new DatabaseException("cancelled serialization", new SQLException("serialization", "40001"));
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				conditions.incrementAndGet(); return true;
			});
			try {
				assertSame(original, assertThrows(DatabaseException.class, () -> database.transactionWithRetry(policy, () -> {
					attempts.incrementAndGet();
					database.query("VALUES (1)").fetchObject(Integer.class);
					Thread.currentThread().interrupt();
					throw original;
				})));
				assertEquals(1, attempts.get());
				assertEquals(0, conditions.get());
				assertEquals(1, diagnostics.get());
				assertTrue(Thread.currentThread().isInterrupted());
			} finally { Thread.interrupted(); }
		}
	}

	@Test
	void completionObserverPreservesPendingInterruptWithoutRetryScope() {
		Fixture fixture = new Fixture(null);
		TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completion -> Thread.interrupted()).build();
		fixture.database.transaction(options, () -> {
			fixture.database.query("VALUES (1)").fetchObject(Integer.class);
			Thread.currentThread().interrupt();
		});
		assertTrue(Thread.currentThread().isInterrupted());
		assertEquals(1, fixture.closes.get());
	}

	private static RetryExecutor executor(RetryExecutor.Sleeper sleeper) {
		return new RetryExecutor(() -> 0, sleeper);
	}

	private static final class Fixture {
		private final Database database;
		private final AtomicInteger closes = new AtomicInteger(), rollbacks = new AtomicInteger(), laterHooks = new AtomicInteger();
		private final AtomicReference<TransactionCompletion> observed = new AtomicReference<>();

		private Fixture(SQLException closeFailure) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:transaction_interruption_" + UUID.randomUUID());
			source.setUser("sa"); source.setPassword("");
			DataSource wrapped = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> {
						Object value = invoke(source, method, args);
						if (value instanceof Connection connection)
							return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
									(connectionProxy, operation, parameters) -> {
										Object result = invoke(connection, operation, parameters);
										if (operation.getName().equals("rollback")) this.rollbacks.incrementAndGet();
										if (operation.getName().equals("close")) {
											this.closes.incrementAndGet();
											if (closeFailure != null) throw closeFailure;
										}
										return result;
									});
						return value;
					});
			this.database = Database.withDataSource(wrapped).databaseType(DatabaseType.GENERIC).build();
		}

		private void run(RuntimeException primary, RuntimeException hookFailure) {
			TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completion -> {
				this.observed.set(completion);
				// Observers are diagnostics; consuming status must not erase the business failure's interruption.
				Thread.interrupted();
			}).build();
			this.database.transaction(options, () -> {
				assertEquals(1, this.database.query("VALUES (1)").fetchObject(Integer.class).orElseThrow());
				if (hookFailure != null) {
					Transaction transaction = this.database.currentTransaction().orElseThrow();
					transaction.addPostTransactionOperation(result -> { throw hookFailure; });
					transaction.addPostTransactionOperation(result -> this.laterHooks.incrementAndGet());
				}
				if (primary != null) throw primary;
			});
		}

		private void assertCompleted(Throwable thrown, boolean rolledBack) {
			assertEquals(1, this.closes.get());
			assertEquals(rolledBack ? 1 : 0, this.rollbacks.get());
			assertSame(thrown, this.observed.get().getFailure().orElseThrow());
			assertEquals(rolledBack ? TransactionResult.ROLLED_BACK : TransactionResult.COMMITTED, this.observed.get().getResult());
			assertTrue(this.database.currentTransaction().isEmpty());
		}
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
