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
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
public class TransactionCompletionSafetyTests {
	@Test
	public void pendingInterruptNormalClosureCommitsExactlyOnceAndPreservesFlag() {
		DataSource delegate = createInMemoryDataSource("pending_interrupt_commit");
		AtomicInteger commits = new AtomicInteger();
		Database database = Database.withDataSource(completionCountingDataSource(delegate, commits, new AtomicInteger())).build();
		Database setupDatabase = Database.withDataSource(delegate).build();
		setupDatabase.query("CREATE TABLE events (id INT)").execute();

		try {
			database.transaction(() -> {
				database.query("INSERT INTO events(id) VALUES (1)").execute();
				Thread.currentThread().interrupt();
			});

			Assertions.assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
		}

		Assertions.assertEquals(1, commits.get());
		Assertions.assertEquals(1L, rowCount(setupDatabase));
	}

	@Test
	public void pendingInterruptThrowingClosureRollsBackAndPreservesIdentityAndFlag() {
		DataSource delegate = createInMemoryDataSource("pending_interrupt_throw");
		AtomicInteger rollbacks = new AtomicInteger();
		Database database = Database.withDataSource(completionCountingDataSource(delegate, new AtomicInteger(), rollbacks)).build();
		Database setupDatabase = Database.withDataSource(delegate).build();
		setupDatabase.query("CREATE TABLE events (id INT)").execute();
		IllegalStateException failure = new IllegalStateException("work failed");

		try {
			IllegalStateException thrown = Assertions.assertThrows(IllegalStateException.class, () ->
					database.transaction(() -> {
						database.query("INSERT INTO events(id) VALUES (1)").execute();
						Thread.currentThread().interrupt();
						throw failure;
					}));

			Assertions.assertSame(failure, thrown);
			Assertions.assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
		}

		Assertions.assertEquals(1, rollbacks.get());
		Assertions.assertEquals(0L, rowCount(setupDatabase));
	}

	@Test
	public void pendingInterruptRollbackOnlyClosureRollsBackAndPreservesFlag() {
		DataSource delegate = createInMemoryDataSource("pending_interrupt_rollback_only");
		AtomicInteger commits = new AtomicInteger();
		AtomicInteger rollbacks = new AtomicInteger();
		Database database = Database.withDataSource(completionCountingDataSource(delegate, commits, rollbacks)).build();
		Database setupDatabase = Database.withDataSource(delegate).build();
		setupDatabase.query("CREATE TABLE events (id INT)").execute();

		try {
			database.transaction(() -> {
				database.query("INSERT INTO events(id) VALUES (1)").execute();
				database.currentTransaction().orElseThrow().setRollbackOnly(true);
				Thread.currentThread().interrupt();
			});

			Assertions.assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
		}

		Assertions.assertEquals(0, commits.get());
		Assertions.assertEquals(1, rollbacks.get());
		Assertions.assertEquals(0L, rowCount(setupDatabase));
	}

	@Test
	public void rollbackFailureSkipsRestorationAbortsThenClosesAndReportsInDoubt() {
		DataSource delegate = createInMemoryDataSource("rollback_failure_discard");
		Database setupDatabase = Database.withDataSource(delegate).build();
		setupDatabase.query("CREATE TABLE events (id INT)").execute();
		List<String> events = new ArrayList<>();
		AtomicInteger restorationCallsAfterFailure = new AtomicInteger();
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = Database.withDataSource(rollbackFailingDataSource(delegate, events, restorationCallsAfterFailure))
				.metricsCollector(metricsCollector)
				.build();
		AtomicReference<TransactionResult> transactionResult = new AtomicReference<>();
		IllegalStateException failure = new IllegalStateException("work failed");

		IllegalStateException thrown = Assertions.assertThrows(IllegalStateException.class, () ->
				database.transaction(() -> {
					database.currentTransaction().orElseThrow().addPostTransactionOperation(transactionResult::set);
					database.query("INSERT INTO events(id) VALUES (1)").execute();
					throw failure;
				}));

		Assertions.assertSame(failure, thrown);
		Assertions.assertEquals(TransactionResult.IN_DOUBT, transactionResult.get());
		Assertions.assertEquals(0, restorationCallsAfterFailure.get());
		Assertions.assertTrue(events.contains("abort"));
		Assertions.assertTrue(events.contains("close"));
		Assertions.assertTrue(events.indexOf("abort") < events.indexOf("close"));
		Assertions.assertEquals(0L, rowCount(setupDatabase));
		Assertions.assertTrue(List.of(thrown.getSuppressed()).stream().anyMatch(suppressed ->
				"Unable to roll back transaction".equals(suppressed.getMessage())));

		MetricsCollector.Snapshot snapshot = metricsCollector.snapshot().orElseThrow();
		Assertions.assertEquals(1L, snapshot.physicalTransactionsRollbackFailed());
		Assertions.assertEquals(1L, snapshot.transactionClosuresFailed());
		Assertions.assertEquals(0L, snapshot.connectionReleaseFailuresTransactionScope());
	}

	@Test
	public void retryableDatabaseFailureWithRollbackFailureIsNeverReplayed() {
		DataSource delegate = createInMemoryDataSource("rollback_failure_no_retry");
		Database setupDatabase = Database.withDataSource(delegate).build();
		setupDatabase.query("CREATE TABLE events (id INT)").execute();
		Database database = Database.withDataSource(rollbackFailingDataSource(delegate, new ArrayList<>(), new AtomicInteger())).build();
		AtomicInteger attempts = new AtomicInteger();
		AtomicInteger conditionCalls = new AtomicInteger();
		DatabaseException failure = new DatabaseException("retryable", new SQLException("serialization", "40001"));
		RetryPolicy retryPolicy = RetryPolicy.ofMaxAttempts(3, RetryPolicy.Backoff.fixed(Duration.ZERO), exception -> {
			conditionCalls.incrementAndGet();
			return true;
		});

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class, () ->
				database.transactionWithRetry(retryPolicy, () -> {
					attempts.incrementAndGet();
					database.query("INSERT INTO events(id) VALUES (1)").execute();
					throw failure;
				}));

		Assertions.assertSame(failure, thrown);
		Assertions.assertTrue(thrown.isTransactionOutcomeIndeterminate());
		Assertions.assertEquals(1, attempts.get());
		Assertions.assertEquals(0, conditionCalls.get());
		Assertions.assertEquals(0L, rowCount(setupDatabase));
	}

	@Test
	public void sharedPrimaryAndRollbackFailureRetainsIdentityAndDiscardsConnection() {
		DataSource delegate = createInMemoryDataSource("shared_primary_rollback_failure");
		Database setupDatabase = Database.withDataSource(delegate).build();
		setupDatabase.query("CREATE TABLE events (id INT)").execute();
		DatabaseException failure = new DatabaseException("shared failure", new SQLException("serialization", "40001"));
		Database database = Database.withDataSource(completionFailingDataSource(delegate, "rollback", failure)).build();

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class, () ->
				database.transaction(() -> {
					database.query("INSERT INTO events(id) VALUES (1)").execute();
					throw failure;
				}));

		Assertions.assertSame(failure, thrown);
		Assertions.assertTrue(thrown.isTransactionOutcomeIndeterminate());
		Assertions.assertEquals(0, thrown.getSuppressed().length);
		Assertions.assertEquals(0L, rowCount(setupDatabase));
	}

	@Test
	public void commitAcknowledgementFailureThatPersistedWorkIsNeverReplayed() {
		DataSource delegate = createInMemoryDataSource("commit_failure_no_retry");
		Database setupDatabase = Database.withDataSource(delegate).build();
		setupDatabase.query("CREATE TABLE events (id INT)").execute();
		Database database = Database.withDataSource(commitAcknowledgementFailingDataSource(delegate)).build();
		AtomicInteger attempts = new AtomicInteger();
		AtomicInteger conditionCalls = new AtomicInteger();
		RetryPolicy retryPolicy = RetryPolicy.ofMaxAttempts(3, RetryPolicy.Backoff.fixed(Duration.ZERO), exception -> {
			conditionCalls.incrementAndGet();
			return true;
		});

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class, () ->
				database.transactionWithRetry(retryPolicy, () -> {
					attempts.incrementAndGet();
					database.query("INSERT INTO events(id) VALUES (1)").execute();
				}));

		Assertions.assertTrue(thrown.isTransactionOutcomeIndeterminate());
		Assertions.assertEquals(1, attempts.get());
		Assertions.assertEquals(0, conditionCalls.get());
		Assertions.assertEquals(1L, rowCount(setupDatabase));
	}

	@Test
	public void uncheckedCommitAndRollbackFailuresEmitMetricsAndPreserveIdentity() {
		for (Throwable failure : List.of(new IllegalStateException("commit runtime"), new AssertionError("commit error")))
			assertUncheckedCompletionFailure("commit", failure);

		for (Throwable failure : List.of(new IllegalStateException("rollback runtime"), new AssertionError("rollback error")))
			assertUncheckedCompletionFailure("rollback", failure);
	}

	private void assertUncheckedCompletionFailure(@NonNull String operation,
																						 @NonNull Throwable failure) {
		String databaseName = format("unchecked_%s_%s", operation, failure instanceof Error ? "error" : "runtime");
		DataSource delegate = createInMemoryDataSource(databaseName);
		Database setupDatabase = Database.withDataSource(delegate).build();
		setupDatabase.query("CREATE TABLE events (id INT)").execute();
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = Database.withDataSource(completionFailingDataSource(delegate, operation, failure))
				.metricsCollector(metricsCollector)
				.build();

		Throwable thrown = Assertions.assertThrows(failure.getClass(), () -> database.transaction(() -> {
			database.query("INSERT INTO events(id) VALUES (1)").execute();

			if ("rollback".equals(operation))
				database.currentTransaction().orElseThrow().setRollbackOnly(true);
		}));

		Assertions.assertSame(failure, thrown);
		MetricsCollector.Snapshot snapshot = metricsCollector.snapshot().orElseThrow();

		if ("commit".equals(operation)) {
			Assertions.assertEquals(1L, snapshot.physicalTransactionsCommitFailed());
			Assertions.assertEquals(1L, snapshot.physicalTransactionsRolledBack());
		} else {
			Assertions.assertEquals(1L, snapshot.physicalTransactionsRollbackFailed());
		}

		Assertions.assertEquals(1L, snapshot.transactionClosuresFailed());
	}

	@NonNull
	private DataSource completionCountingDataSource(@NonNull DataSource delegate,
																							 @NonNull AtomicInteger commits,
																							 @NonNull AtomicInteger rollbacks) {
		return connectionWrappingDataSource(delegate, connection -> connectionProxy(connection, (method, args) -> {
			if ("commit".equals(method.getName()) && method.getParameterCount() == 0)
				commits.incrementAndGet();
			else if ("rollback".equals(method.getName()) && method.getParameterCount() == 0)
				rollbacks.incrementAndGet();

			return invoke(connection, method, args);
		}));
	}

	@NonNull
	private DataSource rollbackFailingDataSource(@NonNull DataSource delegate,
																						@NonNull List<String> events,
																						@NonNull AtomicInteger restorationCallsAfterFailure) {
		return connectionWrappingDataSource(delegate, connection -> {
			AtomicBoolean rollbackFailed = new AtomicBoolean();

			return connectionProxy(connection, (method, args) -> {
				String methodName = method.getName();

				if ("rollback".equals(methodName) && method.getParameterCount() == 0) {
					events.add("rollback");
					rollbackFailed.set(true);
					throw new SQLException("rollback failed", "40001");
				}

				if (rollbackFailed.get() && List.of("setTransactionIsolation", "setReadOnly", "setAutoCommit").contains(methodName))
					restorationCallsAfterFailure.incrementAndGet();

				if ("abort".equals(methodName))
					events.add("abort");
				else if ("close".equals(methodName))
					events.add("close");

				return invoke(connection, method, args);
			});
		});
	}

	@NonNull
	private DataSource commitAcknowledgementFailingDataSource(@NonNull DataSource delegate) {
		return connectionWrappingDataSource(delegate, connection -> connectionProxy(connection, (method, args) -> {
			Object result = invoke(connection, method, args);

			if ("commit".equals(method.getName()) && method.getParameterCount() == 0)
				throw new SQLException("commit acknowledgement lost", "40001");

			return result;
		}));
	}

	@NonNull
	private DataSource completionFailingDataSource(@NonNull DataSource delegate,
																							 @NonNull String operation,
																							 @NonNull Throwable failure) {
		return connectionWrappingDataSource(delegate, connection -> connectionProxy(connection, (method, args) -> {
			if (operation.equals(method.getName()) && method.getParameterCount() == 0)
				throw failure;

			return invoke(connection, method, args);
		}));
	}

	@NonNull
	private DataSource connectionWrappingDataSource(@NonNull DataSource delegate,
																							 @NonNull ConnectionWrapper connectionWrapper) {
		requireNonNull(delegate);
		requireNonNull(connectionWrapper);

		return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> {
					Object result = invoke(delegate, method, args);
					return result instanceof Connection ? connectionWrapper.wrap((Connection) result) : result;
				});
	}

	@NonNull
	private Connection connectionProxy(@NonNull Connection connection,
																			 @NonNull ConnectionInvocation connectionInvocation) {
		return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
				(proxy, method, args) -> connectionInvocation.invoke(method, args));
	}

	private Object invoke(@NonNull Object target,
									@NonNull Method method,
									Object[] args) throws Throwable {
		try {
			return method.invoke(target, args);
		} catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}

	private long rowCount(@NonNull Database database) {
		return database.query("SELECT COUNT(*) FROM events").fetchObject(Long.class).orElseThrow();
	}

	@NonNull
	private DataSource createInMemoryDataSource(@NonNull String databaseName) {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");
		return dataSource;
	}

	@FunctionalInterface
	private interface ConnectionWrapper {
		@NonNull
		Connection wrap(@NonNull Connection connection) throws Throwable;
	}

	@FunctionalInterface
	private interface ConnectionInvocation {
		Object invoke(@NonNull Method method, Object[] args) throws Throwable;
	}
}
