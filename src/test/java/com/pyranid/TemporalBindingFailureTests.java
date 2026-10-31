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
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransactionRollbackException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TemporalBindingFailureTests {
	@AfterEach
	void clearInterruptFlag() {
		Thread.interrupted();
	}

	@Test
	void unsupportedDetailCannotHideARollbackAndCommitOnlyTheLaterStatement() {
		for (boolean untyped : new boolean[]{false, true}) {
			SQLException rollback = rollbackWithUnsupportedDetail();
			Fixture fixture = new Fixture(untyped, rollback, true);

			DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction);

			assertSame(rollback, thrown.getCause());
			assertTrue(thrown.isSerializationFailure());
			fixture.assertAborted();
			assertEquals(untyped ? 2 : 3, fixture.firstSetObjectArgumentCount);
		}
	}

	@Test
	void cancelledFeatureFailureRestoresInterruptionAndNeverReachesTheRetryCondition() {
		for (boolean untyped : new boolean[]{false, true}) {
			SQLException cancelled = new SQLFeatureNotSupportedException("typed binding is unavailable", "0A000",
					new InterruptedException("cancelled while binding"));
			Fixture fixture = new Fixture(untyped, cancelled, false);
			AtomicInteger attempts = new AtomicInteger(), conditionCalls = new AtomicInteger();
			RetryPolicy retry = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				conditionCalls.incrementAndGet();
				return true;
			});

			DatabaseException thrown = assertThrows(DatabaseException.class,
					() -> fixture.database.transactionWithRetry(retry, () -> {
						attempts.incrementAndGet();
						fixture.executeStatements();
					}));

			// Check before the inspection connection can consume a restored interrupt.
			boolean interruptedAtExit = Thread.interrupted();
			assertSame(cancelled, thrown.getCause());
			assertTrue(interruptedAtExit);
			assertEquals(1, attempts.get());
			assertEquals(0, conditionCalls.get());
			fixture.assertAborted();
		}
	}

	@Test
	void terminalEvidenceWinsOverUnsupportedCausesNextExceptionsAndCycles() {
		for (boolean untyped : new boolean[]{false, true}) {
			SQLException causedRollback = new SQLFeatureNotSupportedException("optional typed bind unavailable", "0A000",
					new SQLTransactionRollbackException("transaction rolled back", "40001"));
			SQLException chainedConnection = new SQLFeatureNotSupportedException("optional typed bind unavailable", "0A000");
			chainedConnection.setNextException(new SQLNonTransientConnectionException("connection lost", "08006"));
			SQLException cyclic = new SQLException("typed setObject is not supported", "HY000");
			SQLException rollbackDetail = new SQLException("transaction rolled back", "40001");
			cyclic.setNextException(rollbackDetail);
			rollbackDetail.setNextException(cyclic);
			SQLException suppressedCancellation = new SQLFeatureNotSupportedException("optional typed bind unavailable", "0A000");
			suppressedCancellation.addSuppressed(new InterruptedException("cancelled"));

			for (SQLException failure : List.of(causedRollback, chainedConnection, cyclic, suppressedCancellation)) {
				Fixture fixture = new Fixture(untyped, failure, false);

				DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction);

				boolean interruptedAtExit = Thread.interrupted();
				assertSame(failure, thrown.getCause());
				assertEquals(failure == suppressedCancellation, interruptedAtExit);
				fixture.assertAborted();
			}
		}
	}

	@Test
	void unreadableTerminalEvidenceCannotBeOverriddenByAnUnsupportedMessage() {
		for (boolean untyped : new boolean[]{false, true}) {
			SQLException failure = new SQLException("typed setObject is not supported", "HY000") {
				@Override public String getSQLState() {
					throw new IllegalStateException("SQLState is unavailable");
				}
			};
			Fixture fixture = new Fixture(untyped, failure, false);

			DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction);

			assertSame(failure, thrown.getCause());
			fixture.assertAborted();
			assertFalse(Thread.currentThread().isInterrupted());
		}
	}

	@Test
	void rollbackFailureRetriesTheEntireClosureWithoutLosingEarlierWork() {
		for (boolean untyped : new boolean[]{false, true}) {
			SQLException rollback = rollbackWithUnsupportedDetail();
			Fixture fixture = new Fixture(untyped, rollback, true);
			AtomicInteger attempts = new AtomicInteger(), conditionCalls = new AtomicInteger();
			RetryPolicy retry = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				conditionCalls.incrementAndGet();
				return failure.isSerializationFailure();
			});

			TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(retry, () -> {
				attempts.incrementAndGet();
				fixture.executeStatements();
			});

			assertEquals(2, result.getAttemptCount());
			assertEquals(2, attempts.get());
			assertEquals(1, conditionCalls.get());
			assertSame(rollback, result.getFailures().get(0).getCause());
			assertEquals(List.of(1, 2), fixture.ids());
			assertEquals(0, fixture.fallbackCalls.get());
			assertEquals(1, fixture.secondExecutions.get());
			assertEquals(1, fixture.commits.get());
			assertEquals(1, fixture.rollbacks.get());
			fixture.assertClosed(2);
		}
	}

	@Test
	void benignUnsupportedTemporalBindsStillFallbackSuccessfully() {
		for (boolean untyped : new boolean[]{false, true}) {
			for (Throwable unsupported : List.of(new SQLFeatureNotSupportedException("typed bind unavailable"),
					new SQLException("feature unavailable", "0A000"),
					new SQLException("typed setObject is not implemented", "HY000"),
					new AbstractMethodError("setObject"))) {
				Fixture fixture = new Fixture(untyped, unsupported, false);

				fixture.executeInTransaction();

				assertEquals(List.of(1, 2), fixture.ids());
				assertEquals(1, fixture.fallbackCalls.get());
				assertEquals(1, fixture.secondExecutions.get());
				assertEquals(1, fixture.commits.get());
				assertEquals(0, fixture.rollbacks.get());
				fixture.assertClosed(1);
			}
		}
	}

	@Test
	void ordinaryTemporalBindFailuresRemainFailuresWithoutFallback() {
		for (boolean untyped : new boolean[]{false, true}) {
			SQLException rejected = new SQLException("invalid temporal value", "22007");
			Fixture fixture = new Fixture(untyped, rejected, false);

			DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction);

			assertSame(rejected, thrown.getCause());
			fixture.assertAborted();
		}
	}

	private static SQLException rollbackWithUnsupportedDetail() {
		SQLException rollback = new SQLTransactionRollbackException("driver rolled back the transaction", "40001");
		rollback.setNextException(new SQLFeatureNotSupportedException("optional typed bind detail unavailable", "0A000"));
		return rollback;
	}

	private static final class Fixture {
		private final Database database, inspectionDatabase;
		private final AtomicInteger fallbackCalls = new AtomicInteger(), secondExecutions = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger();
		private final AtomicInteger statementsClosed = new AtomicInteger(), connectionsClosed = new AtomicInteger();
		private int firstSetObjectArgumentCount;

		private Fixture(boolean untyped, Throwable failure, boolean driverRollback) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:temporal_binding_failure_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			this.inspectionDatabase = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
			this.inspectionDatabase.query("CREATE TABLE temporal_work (id INT PRIMARY KEY, value_timestamp TIMESTAMP)").execute();
			AtomicBoolean armed = new AtomicBoolean(true);
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object value = invoke(source, method, args);
				if (!(value instanceof Connection connection)) return value;
				return proxy(Connection.class, (operation, parameters) -> {
					Object result = invoke(connection, operation, parameters);
					if (operation.getName().equals("commit")) commits.incrementAndGet();
					if (operation.getName().equals("rollback")) rollbacks.incrementAndGet();
					if (operation.getName().equals("close")) connectionsClosed.incrementAndGet();
					if (!(result instanceof PreparedStatement statement)) return result;
					boolean second = String.valueOf(parameters[0]).contains("VALUES (2,");
					return proxy(PreparedStatement.class, (binding, values) -> {
						String name = binding.getName();
						if (second && name.equals("setObject") && armed.compareAndSet(true, false)) {
							firstSetObjectArgumentCount = values.length;
							if (driverRollback) connection.rollback();
							throw failure;
						}
						if (second && name.equals("setTimestamp")) fallbackCalls.incrementAndGet();
						if (second && (name.equals("executeLargeUpdate") || name.equals("executeUpdate")))
							secondExecutions.incrementAndGet();
						Object bound = invoke(statement, binding, values);
						if (name.equals("close")) statementsClosed.incrementAndGet();
						return bound;
					});
				});
			});
			// DuckDB's dialect exercises untyped setObject; both paths use actual HSQLDB storage.
			this.database = Database.withDataSource(wrapped)
					.databaseType(untyped ? DatabaseType.DUCK_DB : DatabaseType.GENERIC).build();
		}

		private void executeStatements() {
			this.database.query("INSERT INTO temporal_work (id, value_timestamp) VALUES (1, TIMESTAMP '2020-01-01 00:00:00')").execute();
			this.database.query("INSERT INTO temporal_work (id, value_timestamp) VALUES (2, :value)")
					.bind("value", LocalDateTime.of(2020, 1, 2, 3, 4)).execute();
		}

		private void executeInTransaction() {
			this.database.transaction(() -> { executeStatements(); });
		}

		private List<Integer> ids() {
			return this.inspectionDatabase.query("SELECT id FROM temporal_work ORDER BY id").fetchList(Integer.class);
		}

		private void assertAborted() {
			assertEquals(List.of(), ids());
			assertEquals(0, this.fallbackCalls.get());
			assertEquals(0, this.secondExecutions.get());
			assertEquals(0, this.commits.get());
			assertEquals(1, this.rollbacks.get());
			assertTrue(this.database.currentTransaction().isEmpty());
			assertClosed(1);
		}

		private void assertClosed(int transactions) {
			assertEquals(transactions * 2, this.statementsClosed.get());
			assertEquals(transactions, this.connectionsClosed.get());
		}
	}

	@FunctionalInterface
	private interface Invocation { Object invoke(Method method, Object[] args) throws Throwable; }

	private static <T> T proxy(Class<T> type, Invocation invocation) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> invocation.invoke(method, args)));
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
