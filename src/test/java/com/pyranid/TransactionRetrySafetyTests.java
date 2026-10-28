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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionRetrySafetyTests {
	@Test
	public void libraryRawConnectionWrapperCannotReplayCommittedInnerCleanupFailure() {
		IllegalStateException cleanupFailure = new IllegalStateException("pool cleanup failed");
		Fixture fixture = closeFailingOnce(cleanupFailure);
		AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
		List<TransactionCompletion> completions = new ArrayList<>();
		TransactionOptions options = options(completions);
		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(
				policy(policyCalls), options, () -> {
					attempts.incrementAndGet();
					fixture.database.useRawConnection(connection -> {
						fixture.database.transaction(options, () -> insert(fixture));
						return java.util.Optional.empty();
					});
				}));
		assertSame(cleanupFailure, thrown.getCause());
		assertUnsafeSingleAttempt(fixture, attempts, policyCalls, completions, thrown, cleanupFailure, TransactionResult.COMMITTED);
	}

	@Test
	public void replacementFailuresPreserveCommittedAndUnknownOutcomesWithoutReplacingFailures() {
		for (boolean unknown : List.of(false, true)) {
			AtomicBoolean failOnce = new AtomicBoolean(true);
			AssertionError driverFailure = new AssertionError("driver failed after completion");
			Fixture fixture = fixture((connection, method, args) -> {
				Object value = invoke(connection, method, args);
				if (method.getName().equals(unknown ? "commit" : "close") && failOnce.compareAndSet(true, false))
					throw driverFailure;
				return value;
			});
			AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
			List<TransactionCompletion> completions = new ArrayList<>();
			TransactionOptions options = options(completions);
			DatabaseException contextual = new DatabaseException("application context");
			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(
					policy(policyCalls), options, () -> {
						attempts.incrementAndGet();
						try { fixture.database.transaction(options, () -> insert(fixture)); }
						catch (AssertionError failure) { assertSame(driverFailure, failure); throw contextual; }
					}));
			assertSame(contextual, thrown);
			assertUnsafeSingleAttempt(fixture, attempts, policyCalls, completions, thrown, driverFailure,
					unknown ? TransactionResult.IN_DOUBT : TransactionResult.COMMITTED);
		}
	}

	@Test
	public void participatingThreadSharesEvidenceEvenWhenCallerDiscardsFailureCause() {
		IllegalStateException cleanupFailure = new IllegalStateException("worker cleanup");
		Fixture fixture = closeFailingOnce(cleanupFailure);
		AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
		List<TransactionCompletion> completions = new CopyOnWriteArrayList<>();
		TransactionOptions options = options(completions);
		DatabaseException contextual = new DatabaseException("worker failed");
		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(
				policy(policyCalls), options, () -> {
					attempts.incrementAndGet();
					Transaction transaction = fixture.database.currentTransaction().orElseThrow();
					AtomicReference<Throwable> workerFailure = new AtomicReference<>();
					AtomicBoolean bindingRestored = new AtomicBoolean();
					Thread worker = new Thread(() -> {
						try { fixture.database.participate(transaction, () -> fixture.database.transaction(options, () -> insert(fixture))); }
						catch (Throwable failure) { workerFailure.set(failure); }
						bindingRestored.set(TransactionRetrySafety.currentOrNew().outcomes() == 0);
					});
					worker.start(); worker.join(5_000);
					assertFalse(worker.isAlive()); assertSame(cleanupFailure, workerFailure.get()); assertTrue(bindingRestored.get());
					throw contextual;
				}));
		assertSame(contextual, thrown);
		assertUnsafeSingleAttempt(fixture, attempts, policyCalls, completions, thrown, cleanupFailure, TransactionResult.COMMITTED);
	}

	@Test
	public void nestedRetryAttemptPropagatesEvidenceToEnclosingAttempt() {
		Fixture outer = fixture(TransactionRetrySafetyTests::invoke);
		IllegalStateException cleanupFailure = new IllegalStateException("independent cleanup");
		Fixture inner = closeFailingOnce(cleanupFailure);
		AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
		DatabaseException contextual = new DatabaseException("inner failed");
		assertSame(contextual, assertThrows(DatabaseException.class, () -> outer.database.transactionWithRetry(policy(policyCalls), () -> {
			attempts.incrementAndGet();
			try { inner.database.transactionWithRetry(policy(policyCalls), () -> insert(inner)); }
			catch (IllegalStateException failure) { assertSame(cleanupFailure, failure); throw contextual; }
		})));
		assertEquals(1, attempts.get()); assertEquals(0, policyCalls.get()); assertEquals(1L, count(inner));
		assertTrue(contextual.isTransactionOutcomeRetryUnsafe());

		// Neither the failed attempt nor a nested scope may contaminate a later independent retry.
		AtomicInteger laterAttempts = new AtomicInteger();
		TransactionRetryResult<Void> later = outer.database.transactionWithRetry(policy(new AtomicInteger()), () -> {
			if (laterAttempts.incrementAndGet() == 1) throw new DatabaseException("safe rollback");
		});
		assertEquals(2, later.getAttemptCount());
	}

	@Test
	public void postTransactionHookWorkRetainsEvidenceAfterAmbientStackIsPopped() {
		IllegalStateException cleanupFailure = new IllegalStateException("hook transaction cleanup");
		Fixture fixture = closeFailingOnce(cleanupFailure);
		AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
		DatabaseException primary = new DatabaseException("original body failure");
		assertSame(primary, assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(policy(policyCalls), () -> {
			attempts.incrementAndGet();
			fixture.database.currentTransaction().orElseThrow().addPostTransactionOperation(result ->
					fixture.database.transaction(() -> insert(fixture)));
			throw primary;
		})));
		assertEquals(1, attempts.get()); assertEquals(0, policyCalls.get()); assertEquals(1L, count(fixture));
		assertEquals(1, primary.getSuppressed().length);
		assertSame(cleanupFailure, primary.getSuppressed()[0].getCause());
		assertTrue(primary.isTransactionOutcomeRetryUnsafe());
	}

	@Test
	public void wrappersAndSuppressedEvidenceAreCycleSafe() {
		DatabaseException committed = new DatabaseException("committed");
		committed.markTransactionOutcomeCommitted();
		RuntimeException first = new RuntimeException("first"), second = new RuntimeException("second");
		first.initCause(second); second.initCause(first);
		second.addSuppressed(committed);
		DatabaseException wrapper = new DatabaseException("context", first);
		assertTrue(wrapper.isTransactionOutcomeCommitted());
		assertTrue(wrapper.isTransactionOutcomeRetryUnsafe());
		DatabaseException late = new DatabaseException("late");
		late.addSuppressed(wrapper); wrapper.addSuppressed(late);
		assertTrue(late.isTransactionOutcomeRetryUnsafe());
		DatabaseException safe = new DatabaseException("safe");
		safe.addSuppressed(new RuntimeException("ordinary"));
		assertFalse(safe.isTransactionOutcomeRetryUnsafe());
	}

	@Test
	public void jdbcNextExceptionsRetainCommittedOutcomeAfterTheOriginalScopeEnds() {
		for (boolean attachBeforeWrapping : List.of(false, true)) {
			AtomicBoolean failOnce = new AtomicBoolean(true);
			SQLException cleanupFailure = new SQLException("pool cleanup failed", "08006");
			Fixture fixture = fixture((connection, method, args) -> {
				Object value = invoke(connection, method, args);
				if (method.getName().equals("close") && failOnce.compareAndSet(true, false)) throw cleanupFailure;
				return value;
			});
			List<TransactionCompletion> originalCompletions = new ArrayList<>();
			DatabaseException committedFailure = assertThrows(DatabaseException.class,
					() -> fixture.database.transaction(options(originalCompletions), () -> insert(fixture)));
			assertSame(cleanupFailure, committedFailure.getCause());
			assertEquals(TransactionResult.COMMITTED, originalCompletions.get(0).getResult());
			assertSame(committedFailure, originalCompletions.get(0).getFailure().orElseThrow());
			assertEquals(0, TransactionRetrySafety.currentOrNew().outcomes());

			SQLException primary = new SQLException("combined database failures", "08006");
			SQLException detail = new SQLException("transaction cleanup detail", committedFailure);
			if (attachBeforeWrapping) primary.setNextException(detail);
			DatabaseException chained = new DatabaseException("JDBC aggregate wrapper", primary);
			if (!attachBeforeWrapping) primary.setNextException(detail);
			detail.setNextException(primary);
			AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
			List<TransactionCompletion> completions = new ArrayList<>();
			assertSame(chained, assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(
					policy(policyCalls), options(completions), () -> {
						attempts.incrementAndGet();
						throw chained;
					})));
			assertEquals(1, attempts.get()); assertEquals(0, policyCalls.get()); assertEquals(1L, count(fixture));
			assertSame(chained, completions.get(0).getFailure().orElseThrow());
			assertSame(primary, chained.getCause()); assertSame(detail, primary.getNextException());
			assertSame(primary, detail.getNextException()); assertSame(committedFailure, detail.getCause());
			assertTrue(chained.isTransactionOutcomeRetryUnsafe());

			SQLException partiallyReadable = new SQLException("partially readable diagnostics") {
				@Override public Throwable getCause() { throw new IllegalStateException("cause accessor failed"); }
			};
			partiallyReadable.setNextException(new SQLException("committed detail", committedFailure));
			DatabaseException conservative = new DatabaseException("partial failure graph", partiallyReadable);
			assertTrue(conservative.isTransactionOutcomeCommitted(), "unreadable edges must not hide known outcome evidence");
			assertTrue(conservative.isTransactionOutcomeIndeterminate());
		}
	}

	@Test
	public void unreadableJdbcNextExceptionNeverAuthorizesReplay() {
		SQLException diagnostic = new SQLException("driver diagnostics unavailable") {
			@Override public SQLException getNextException() { throw new AssertionError("next exception accessor failed"); }
		};
		DatabaseException primary = new DatabaseException("operation failed", diagnostic);
		Fixture fixture = fixture(TransactionRetrySafetyTests::invoke);
		AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
		assertSame(primary, assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(policy(policyCalls), () -> {
			attempts.incrementAndGet(); throw primary;
		})));
		assertEquals(1, attempts.get()); assertEquals(0, policyCalls.get());
		assertSame(diagnostic, primary.getCause()); assertTrue(primary.isTransactionOutcomeIndeterminate());
	}

	@Test
	public void ordinaryJdbcNextExceptionCyclesStillAllowSafeRetry() {
		SQLException first = new SQLException("first"), second = new SQLException("second");
		first.setNextException(second); second.setNextException(first);
		DatabaseException primary = new DatabaseException("safe failure", first);
		Fixture fixture = fixture(TransactionRetrySafetyTests::invoke);
		AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
		TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(policy(policyCalls), () -> {
			if (attempts.incrementAndGet() == 1) throw primary;
		});
		assertEquals(2, result.getAttemptCount()); assertEquals(1, policyCalls.get());
		assertFalse(primary.isTransactionOutcomeRetryUnsafe());
	}

	@Test
	public void containedObserverFailuresCannotChangeRetryDecisions() {
		Fixture fixture = closeFailingOnce(new IllegalStateException("observer cleanup"));
		AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger(), observations = new AtomicInteger();
		DatabaseException firstFailure = new DatabaseException("safe rollback");
		TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completion -> {
			observations.incrementAndGet();
			fixture.database.transaction(() -> insert(fixture));
		}).build();
		TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(policy(policyCalls), options, () -> {
			if (attempts.incrementAndGet() == 1) throw firstFailure;
		});
		assertEquals(2, result.getAttemptCount()); assertEquals(1, policyCalls.get()); assertEquals(2, observations.get());
		assertFalse(firstFailure.isTransactionOutcomeRetryUnsafe());
	}

	@Test
	public void containedCollectorFailuresCannotChangeRetryDecisionsOrLeakBinding() {
		Fixture fixture = closeFailingOnce(new IllegalStateException("collector cleanup"));
		AtomicBoolean observeOnce = new AtomicBoolean(true);
		MetricsCollector collector = new MetricsCollector() {
			@Override
			public void didExitTransactionClosure(Transaction transaction, TransactionClosureOutcome outcome,
					DatabaseType databaseType, Duration duration, Throwable failure) {
				if (observeOnce.compareAndSet(true, false)) fixture.database.transaction(() -> insert(fixture));
			}
		};
		Database database = Database.withDataSource(fixture.database.getDataSource()).databaseType(DatabaseType.GENERIC)
				.metricsCollector(collector).build();
		AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
		DatabaseException firstFailure = new DatabaseException("safe rollback");
		TransactionRetryResult<Void> result = database.transactionWithRetry(policy(policyCalls), () -> {
			if (attempts.incrementAndGet() == 1) throw firstFailure;
		});
		assertEquals(2, result.getAttemptCount()); assertEquals(1, policyCalls.get()); assertEquals(1L, count(fixture));
		assertFalse(firstFailure.isTransactionOutcomeRetryUnsafe());
		assertEquals(0, TransactionRetrySafety.currentOrNew().outcomes());
	}

	@Test
	public void containedObserverLoggingFailuresCannotChangeRetryDecisions() {
		Fixture fixture = closeFailingOnce(new IllegalStateException("logging cleanup"));
		java.util.logging.Logger logger = java.util.logging.Logger.getLogger(TransactionCoordinator.class.getName());
		java.util.logging.Level previousLevel = logger.getLevel();
		boolean previousUseParentHandlers = logger.getUseParentHandlers();
		java.util.logging.Handler handler = new java.util.logging.Handler() {
			@Override public void publish(java.util.logging.LogRecord record) { fixture.database.transaction(() -> insert(fixture)); }
			@Override public void flush() {}
			@Override public void close() {}
		};
		logger.addHandler(handler); logger.setLevel(java.util.logging.Level.ALL); logger.setUseParentHandlers(false);
		try {
			AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
			DatabaseException firstFailure = new DatabaseException("safe rollback");
			TransactionOptions options = TransactionOptions.withReadOnly(null)
					.completionObserver(completion -> { throw new IllegalStateException("observer failed"); }).build();
			TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(policy(policyCalls), options, () -> {
				if (attempts.incrementAndGet() == 1) throw firstFailure;
			});
			assertEquals(2, result.getAttemptCount()); assertEquals(1, policyCalls.get());
			assertFalse(firstFailure.isTransactionOutcomeRetryUnsafe()); assertEquals(0, TransactionRetrySafety.currentOrNew().outcomes());
		} finally {
			logger.removeHandler(handler); logger.setLevel(previousLevel); logger.setUseParentHandlers(previousUseParentHandlers);
		}
	}

	private static void assertUnsafeSingleAttempt(Fixture fixture, AtomicInteger attempts, AtomicInteger policyCalls,
			List<TransactionCompletion> completions, DatabaseException thrown, Throwable innerFailure, TransactionResult innerResult) {
		assertEquals(1, attempts.get()); assertEquals(0, policyCalls.get()); assertEquals(1L, count(fixture));
		assertTrue(thrown.isTransactionOutcomeRetryUnsafe()); assertEquals(2, completions.size());
		assertEquals(innerResult, completions.get(0).getResult());
		assertEquals(TransactionResult.ROLLED_BACK, completions.get(1).getResult());
		assertSame(innerFailure, completions.get(0).getFailure().orElseThrow());
		assertSame(thrown, completions.get(1).getFailure().orElseThrow());
	}

	private static TransactionOptions options(List<TransactionCompletion> completions) {
		return TransactionOptions.withReadOnly(null).completionObserver(completions::add).build();
	}
	private static RetryPolicy policy(AtomicInteger calls) {
		return RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> { calls.incrementAndGet(); return true; });
	}
	private static void insert(Fixture fixture) { fixture.database.query("INSERT INTO events VALUES (1)").execute(); }
	private static long count(Fixture fixture) { return fixture.setup.query("SELECT COUNT(*) FROM events").fetchObject(Long.class).orElseThrow(); }
	private static Fixture closeFailingOnce(RuntimeException failure) {
		AtomicBoolean failOnce = new AtomicBoolean(true);
		return fixture((connection, method, args) -> {
			Object value = invoke(connection, method, args);
			if (method.getName().equals("close") && failOnce.compareAndSet(true, false)) throw failure;
			return value;
		});
	}
	private static Fixture fixture(ConnectionInvocation invocation) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:retry_safety_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		Database setup = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
		setup.query("CREATE TABLE events (id INT)").execute();
		DataSource wrapped = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> {
					Object value = invoke(source, method, args);
					if (value instanceof Connection connection)
						return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
								(connectionProxy, connectionMethod, connectionArgs) -> invocation.invoke(connection, connectionMethod, connectionArgs));
					return value;
				});
		return new Fixture(Database.withDataSource(wrapped).databaseType(DatabaseType.GENERIC).build(), setup);
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
	private record Fixture(Database database, Database setup) {}
	@FunctionalInterface private interface ConnectionInvocation {
		Object invoke(Connection connection, Method method, Object[] args) throws Throwable;
	}
}
