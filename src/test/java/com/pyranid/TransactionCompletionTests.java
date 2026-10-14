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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionCompletionTests {
	@Test
	public void normalAndRollbackOnlyReturnsObserveLogicalAndPhysicalOutcomes() {
		for (boolean physical : List.of(false, true)) {
			for (boolean rollbackOnly : List.of(false, true)) {
				Fixture fixture = fixture(TransactionCompletionTests::invoke);
				List<TransactionCompletion> completions = new ArrayList<>();
				Optional<String> value = fixture.database.transaction(options(completions::add), () -> {
					if (physical)
						fixture.database.query("INSERT INTO events VALUES (1)").execute();
					fixture.database.currentTransaction().orElseThrow().setRollbackOnly(rollbackOnly);
					return Optional.of("result");
				});
				assertEquals(Optional.of("result"), value);
				TransactionCompletion completion = only(completions);
				assertEquals(rollbackOnly ? TransactionResult.ROLLED_BACK : TransactionResult.COMMITTED, completion.getResult());
				assertTrue(completion.getFailure().isEmpty());
				assertTrue(completion.getEnclosingTransactionId().isEmpty());
				assertEquals(physical && !rollbackOnly ? 1L : 0L, rowCount(fixture));
			}
		}
	}

	@Test
	public void bodyFailuresPreserveRuntimeErrorAndCheckedWrapperIdentity() {
		for (Throwable failure : List.of(new IllegalStateException("runtime"), new AssertionError("error"), new Exception("checked"))) {
			Fixture fixture = fixture(TransactionCompletionTests::invoke);
			List<TransactionCompletion> completions = new ArrayList<>();
			Throwable thrown = assertThrows(Throwable.class, () -> fixture.database.transaction(options(completions::add), () -> {
				fixture.database.query("INSERT INTO events VALUES (1)").execute();
				if (failure instanceof Error error) throw error;
				throw (Exception) failure;
			}));
			assertSame(thrown, only(completions).getFailure().orElseThrow());
			assertEquals(TransactionResult.ROLLED_BACK, only(completions).getResult());
			assertEquals(0L, rowCount(fixture));
			if (failure instanceof RuntimeException || failure instanceof Error) assertSame(failure, thrown);
			else assertSame(failure, thrown.getCause());
		}
	}

	@Test
	public void failedBeginReportsRollbackEvenWhenCandidateCleanupRollbackFails() {
		Fixture fixture = fixture((connection, method, args) -> {
			if ((method.getName().equals("setAutoCommit") && Boolean.FALSE.equals(args[0])) || method.getName().equals("rollback"))
				throw new SQLException("begin/cleanup failed", "08006");
			return invoke(connection, method, args);
		});
		List<TransactionCompletion> completions = new ArrayList<>();
		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transaction(options(completions::add), () -> {
			fixture.database.query("INSERT INTO events VALUES (1)").execute();
		}));
		assertSame(thrown, only(completions).getFailure().orElseThrow());
		assertEquals(TransactionResult.ROLLED_BACK, only(completions).getResult());
		assertFalse(thrown.isTransactionOutcomeRetryUnsafe());
		assertEquals(0L, rowCount(fixture));
	}

	@Test
	public void serializationCommitFailureFollowedByRollbackIsKnownRollback() {
		Fixture fixture = fixture((connection, method, args) -> {
			if (method.getName().equals("commit")) throw new SQLException("serialization", "40001");
			return invoke(connection, method, args);
		});
		List<TransactionCompletion> completions = new ArrayList<>();
		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transaction(options(completions::add), () -> {
			fixture.database.query("INSERT INTO events VALUES (1)").execute();
		}));
		assertEquals(TransactionResult.ROLLED_BACK, only(completions).getResult());
		assertSame(thrown, only(completions).getFailure().orElseThrow());
		assertFalse(thrown.isTransactionOutcomeRetryUnsafe());
		assertEquals(0L, rowCount(fixture));
	}

	@Test
	public void commitAcknowledgementLossAndRollbackFailureAreInDoubt() {
		for (boolean commitFailure : List.of(true, false)) {
			Fixture fixture = fixture((connection, method, args) -> {
				if (commitFailure && method.getName().equals("commit")) {
					invoke(connection, method, args);
					throw new SQLException("lost commit acknowledgement", "08006");
				}
				if (!commitFailure && method.getName().equals("rollback")) throw new SQLException("rollback failed", "08006");
				return invoke(connection, method, args);
			});
			DatabaseException bodyFailure = new DatabaseException("body");
			List<TransactionCompletion> completions = new ArrayList<>();
			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transaction(options(completions::add), () -> {
				fixture.database.query("INSERT INTO events VALUES (1)").execute();
				if (!commitFailure) throw bodyFailure;
			}));
			assertEquals(TransactionResult.IN_DOUBT, only(completions).getResult());
			assertSame(thrown, only(completions).getFailure().orElseThrow());
			assertTrue(thrown.isTransactionOutcomeRetryUnsafe());
			if (!commitFailure) assertSame(bodyFailure, thrown);
			assertEquals(commitFailure ? 1L : 0L, rowCount(fixture));
		}
	}

	@Test
	public void completionFollowsCleanupAndEveryHookWithFinalSuppressedFailures() {
		List<String> order = new ArrayList<>();
		AtomicBoolean observedWithoutAmbientTransaction = new AtomicBoolean();
		AtomicInteger observedSuppressedCount = new AtomicInteger(-1);
		Fixture fixture = fixture((connection, method, args) -> {
			Object value = invoke(connection, method, args);
			if (method.getName().equals("close")) {
				order.add("close");
				throw new SQLException("close failed", "08006");
			}
			return value;
		});
		List<TransactionCompletion> completions = new ArrayList<>();
		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transaction(options(completion -> {
			order.add("observer");
			completions.add(completion);
			observedWithoutAmbientTransaction.set(fixture.database.currentTransaction().isEmpty());
			observedSuppressedCount.set(completion.getFailure().orElseThrow().getSuppressed().length);
		}), () -> {
			fixture.database.query("INSERT INTO events VALUES (1)").execute();
			fixture.database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> {
				order.add("hook1"); assertEquals(TransactionResult.COMMITTED, result); throw new IllegalStateException("hook1");
			});
			fixture.database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> {
				order.add("hook2"); throw new AssertionError("hook2");
			});
		}));
		assertEquals(List.of("close", "hook1", "hook2", "observer"), order);
		assertTrue(observedWithoutAmbientTransaction.get());
		assertEquals(2, observedSuppressedCount.get());
		assertSame(thrown, only(completions).getFailure().orElseThrow());
		assertEquals(TransactionResult.COMMITTED, only(completions).getResult());
		assertTrue(thrown.isTransactionOutcomeRetryUnsafe());
		assertEquals(2, thrown.getSuppressed().length);
		assertEquals(1L, rowCount(fixture));
	}

	@Test
	public void hookFailureWithoutCleanupFailureIsTheObservedThrownWrapper() {
		Fixture fixture = fixture(TransactionCompletionTests::invoke);
		List<TransactionCompletion> completions = new ArrayList<>();
		IllegalStateException failure = new IllegalStateException("hook");
		PostTransactionOperationException thrown = assertThrows(PostTransactionOperationException.class, () ->
				fixture.database.transaction(options(completions::add), () -> {
					fixture.database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> { throw failure; });
				}));
		assertSame(failure, thrown.getCause());
		assertSame(thrown, only(completions).getFailure().orElseThrow());
		assertEquals(TransactionResult.COMMITTED, only(completions).getResult());
	}

	@Test
	public void bodyFailureKeepsCleanupAndHookFailuresBeforeObservation() {
		AtomicInteger observedSuppressedCount = new AtomicInteger(-1);
		Fixture fixture = fixture((connection, method, args) -> {
			Object value = invoke(connection, method, args);
			if (method.getName().equals("close")) throw new SQLException("close failed", "08006");
			return value;
		});
		List<TransactionCompletion> completions = new ArrayList<>();
		DatabaseException bodyFailure = new DatabaseException("body");
		Throwable thrown = assertThrows(DatabaseException.class, () -> fixture.database.transaction(options(completion -> {
			observedSuppressedCount.set(completion.getFailure().orElseThrow().getSuppressed().length);
			completions.add(completion);
		}), () -> {
			fixture.database.query("INSERT INTO events VALUES (1)").execute();
			fixture.database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> { throw new AssertionError("hook"); });
			throw bodyFailure;
		}));
		assertSame(bodyFailure, thrown);
		assertEquals(1, observedSuppressedCount.get());
		assertSame(thrown, only(completions).getFailure().orElseThrow());
		assertEquals(1, thrown.getSuppressed()[0].getSuppressed().length);
		assertEquals(TransactionResult.ROLLED_BACK, only(completions).getResult());
		assertFalse(bodyFailure.isTransactionOutcomeRetryUnsafe());
	}

	@Test
	public void nestedBoundaryResultsDoNotEraseInnerCommitOrPermitRetry() {
		Fixture fixture = fixture((connection, method, args) -> {
			Object value = invoke(connection, method, args);
			if (method.getName().equals("close")) throw new SQLException("cleanup after commit", "08006");
			return value;
		});
		List<TransactionCompletion> completions = new ArrayList<>();
		TransactionOptions options = options(completions::add);
		AtomicInteger attempts = new AtomicInteger();
		AtomicInteger conditions = new AtomicInteger();
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(3, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			conditions.incrementAndGet(); return true;
		});
		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(policy, options, () -> {
			attempts.incrementAndGet();
			fixture.database.transaction(options, () -> { fixture.database.query("INSERT INTO events VALUES (1)").execute(); });
		}));
		assertEquals(2, completions.size());
		TransactionCompletion inner = completions.get(0), outer = completions.get(1);
		assertEquals(TransactionResult.COMMITTED, inner.getResult());
		assertEquals(TransactionResult.ROLLED_BACK, outer.getResult());
		assertEquals(outer.getTransactionId(), inner.getEnclosingTransactionId().orElseThrow());
		assertTrue(outer.getEnclosingTransactionId().isEmpty());
		assertNotEquals(inner.getTransactionId(), outer.getTransactionId());
		assertSame(thrown, inner.getFailure().orElseThrow());
		assertSame(thrown, outer.getFailure().orElseThrow());
		assertTrue(thrown.isTransactionOutcomeRetryUnsafe());
		assertEquals(1, attempts.get());
		assertEquals(0, conditions.get());
		assertEquals(1L, rowCount(fixture));
	}

	@Test
	public void observerAndLoggingFailuresCannotReplaceFailureOrTriggerRetry() {
		Fixture fixture = fixture(TransactionCompletionTests::invoke);
		Logger logger = Logger.getLogger(TransactionCoordinator.class.getName());
		java.util.logging.Level previousLevel = logger.getLevel();
		Handler handler = new Handler() {
			@Override public void publish(LogRecord record) { throw new AssertionError("logger"); }
			@Override public void flush() {}
			@Override public void close() {}
		};
		logger.addHandler(handler); logger.setLevel(java.util.logging.Level.ALL);
		try {
			AtomicInteger observations = new AtomicInteger();
			TransactionOptions options = options(completion -> { observations.incrementAndGet(); throw new AssertionError("observer"); });
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(3, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> true);
			assertEquals(1, fixture.database.transactionWithRetry(policy, options, () -> {}).getAttemptCount());
			IllegalArgumentException failure = new IllegalArgumentException("body");
			assertSame(failure, assertThrows(IllegalArgumentException.class, () -> fixture.database.transaction(options, () -> { throw failure; })));
			assertEquals(2, observations.get());
			assertEquals(0, failure.getSuppressed().length);
		} finally {
			logger.removeHandler(handler); logger.setLevel(previousLevel);
		}
	}

	@Test
	public void observesEveryRetryAttemptAndSkipsValidationAndParticipationScopes() {
		Fixture fixture = fixture(TransactionCompletionTests::invoke);
		List<TransactionCompletion> completions = new ArrayList<>();
		TransactionOptions options = options(completions::add);
		assertThrows(NullPointerException.class, () -> fixture.database.transaction(options, (TransactionalOperation) null));
		assertTrue(completions.isEmpty());
		AtomicInteger attempts = new AtomicInteger();
		DatabaseException firstFailure = new DatabaseException("retry");
		TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(
				RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> true), options, () -> {
					fixture.database.participate(fixture.database.currentTransaction().orElseThrow(), () -> {});
					if (attempts.incrementAndGet() == 1) throw firstFailure;
				});
		assertEquals(2, result.getAttemptCount());
		assertEquals(2, completions.size());
		assertSame(firstFailure, completions.get(0).getFailure().orElseThrow());
		assertEquals(TransactionResult.ROLLED_BACK, completions.get(0).getResult());
		assertEquals(TransactionResult.COMMITTED, completions.get(1).getResult());
		assertTrue(completions.get(1).getFailure().isEmpty());
		assertNotEquals(completions.get(0).getTransactionId(), completions.get(1).getTransactionId());
	}

	@Test
	public void retryBudgetIncludesConnectionCleanupAndSynchronousObserver() {
		AtomicLong clock = new AtomicLong();
		Fixture fixture = fixture((connection, method, args) -> {
			Object value = invoke(connection, method, args);
			if (method.getName().equals("close")) clock.addAndGet(6);
			return value;
		});
		AtomicInteger attempts = new AtomicInteger();
		List<TransactionCompletion> completions = new ArrayList<>();
		TransactionOptions options = options(completion -> { clock.addAndGet(5); completions.add(completion); });
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> true)
				.withMaxElapsedTime(Duration.ofNanos(12));
		DatabaseException failure = new DatabaseException("body");
		RetryExecutor executor = new RetryExecutor(clock::get, delay -> fail("budget exhausted before sleep"));
		assertSame(failure, assertThrows(DatabaseException.class, () -> executor.execute(policy, () -> fixture.database.transaction(options, () -> {
			attempts.incrementAndGet(); clock.addAndGet(2);
			fixture.database.query("INSERT INTO events VALUES (1)").execute();
			throw failure;
		}))));
		assertEquals(13L, clock.get());
		assertEquals(1, attempts.get());
		assertSame(failure, only(completions).getFailure().orElseThrow());
	}

	@Test
	public void observerCanBeRemovedAndOptionsKeepCallbackIdentity() {
		Consumer<TransactionCompletion> observer = completion -> {};
		TransactionOptions.Builder builder = TransactionOptions.withReadOnly(null).completionObserver(observer);
		TransactionOptions withObserver = builder.build();
		assertSame(observer, withObserver.getCompletionObserver().orElseThrow());
		assertEquals(withObserver, options(observer));
		assertEquals(withObserver.hashCode(), options(observer).hashCode());
		assertNotEquals(withObserver, builder.completionObserver(null).build());
		assertTrue(builder.build().getCompletionObserver().isEmpty());
		assertSame(observer, withObserver.getCompletionObserver().orElseThrow());
	}

	private static TransactionOptions options(Consumer<TransactionCompletion> observer) {
		return TransactionOptions.withReadOnly(null).completionObserver(observer).build();
	}

	private static TransactionCompletion only(List<TransactionCompletion> completions) {
		assertEquals(1, completions.size());
		return completions.get(0);
	}

	private static long rowCount(Fixture fixture) {
		return fixture.setup.query("SELECT COUNT(*) FROM events").fetchObject(Long.class).orElseThrow();
	}

	private static Fixture fixture(ConnectionInvocation invocation) {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl("jdbc:hsqldb:mem:completion_" + UUID.randomUUID());
		dataSource.setUser("sa"); dataSource.setPassword("");
		Database setup = Database.withDataSource(dataSource).databaseType(DatabaseType.GENERIC).build();
		setup.query("CREATE TABLE events (id INT)").execute();
		DataSource wrapped = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> {
					Object value = invoke(dataSource, method, args);
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
	@FunctionalInterface
	private interface ConnectionInvocation {
		Object invoke(Connection connection, Method method, Object[] args) throws Throwable;
	}
}
