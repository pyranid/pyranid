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
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransactionRollbackException;
import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BatchParameterCleanupTests {
	@AfterEach
	void clearInterruptFlag() {
		Thread.interrupted();
	}

	@Test
	void resetRollbackCannotReportSuccessOrCommitOnlyLaterChunks() {
		SQLException resetFailure = new SQLTransactionRollbackException("reset aborted the transaction", "40001");
		Fixture fixture = new Fixture((connection, reset) -> {
			if (reset == 1) {
				connection.rollback();
				throw resetFailure;
			}
		});

		DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction);

		assertSame(resetFailure, thrown.getCause());
		assertTrue(thrown.isSerializationFailure());
		fixture.assertRolledBack(1);
		assertFalse(Thread.currentThread().isInterrupted());
	}

	@Test
	void unsupportedResetCannotHideRollbackEvidence() {
		for (String edge : List.of("next", "cause", "suppressed", "cycle", "unreadable")) {
			SQLException rollback = new SQLTransactionRollbackException("transaction rolled back", "40001");
			SQLException resetFailure = edge.equals("unreadable")
					? new SQLFeatureNotSupportedException("reset unsupported", "0A000") {
					@Override public String getSQLState() { throw new IllegalStateException("unreadable state"); }
				} : new SQLFeatureNotSupportedException("reset unsupported", "0A000");
			switch (edge) {
				case "next", "cycle" -> resetFailure.setNextException(rollback);
				case "cause" -> resetFailure.initCause(rollback);
				default -> resetFailure.addSuppressed(rollback);
			}
			if (edge.equals("cycle")) rollback.setNextException(resetFailure);
			Fixture fixture = new Fixture((connection, reset) -> {
				if (reset == 1) {
					connection.rollback();
					throw resetFailure;
				}
			});

			DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction, edge);

			assertSame(resetFailure, thrown.getCause(), edge);
			fixture.assertRolledBack(1);
			assertFalse(Thread.currentThread().isInterrupted(), edge);
		}
	}

	@Test
	void unsupportedResetCannotHideCancellationOrAdmitARetry() {
		for (String kind : List.of("sql", "runtime", "legacy")) {
			InterruptedException interruption = new InterruptedException("cancelled reset");
			Throwable resetFailure = switch (kind) {
				case "sql" -> new SQLFeatureNotSupportedException("reset unsupported", "0A000", interruption);
				case "runtime" -> new UnsupportedOperationException("reset unsupported", interruption);
				default -> new AbstractMethodError("clearParameters");
			};
			if (kind.equals("legacy")) resetFailure.addSuppressed(interruption);
			Fixture fixture = new Fixture((connection, reset) -> {
				if (reset == 1) {
					if (resetFailure instanceof SQLException sql) throw sql;
					if (resetFailure instanceof Error error) throw error;
					throw (RuntimeException) resetFailure;
				}
			});
			AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				conditions.incrementAndGet();
				return true;
			});

			Class<? extends Throwable> expected = kind.equals("legacy") ? Error.class : DatabaseException.class;
			Throwable thrown = assertThrows(expected,
					() -> fixture.database.transactionWithRetry(policy, () -> {
						attempts.incrementAndGet();
						fixture.executeBatch();
					}), kind);

			assertTrue(Thread.interrupted(), kind);
			assertTrue(ThrowableGraph.inspect(thrown, node -> node == resetFailure ? 1 : 0, 0) != 0, kind);
			assertEquals(1, attempts.get(), kind);
			assertEquals(0, conditions.get(), kind);
			fixture.assertRolledBack(1);
		}
	}

	@Test
	void unsupportedResetWithRollbackEvidenceRetriesTheEntireBatch() {
		SQLException resetFailure = new SQLFeatureNotSupportedException("reset unsupported", "0A000");
		resetFailure.setNextException(new SQLTransactionRollbackException("transaction rolled back", "40001"));
		Fixture fixture = new Fixture((connection, reset) -> {
			if (reset == 1) {
				connection.rollback();
				throw resetFailure;
			}
		});
		AtomicInteger attempts = new AtomicInteger();
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO),
				DatabaseException::isSerializationFailure);

		TransactionRetryResult<List<Long>> result = fixture.database.transactionWithRetry(policy, () -> {
			attempts.incrementAndGet();
			return Optional.of(fixture.executeBatch());
		});

		assertEquals(2, attempts.get());
		assertEquals(2, result.getAttemptCount());
		assertSame(resetFailure, result.getFailures().get(0).getCause());
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(3, fixture.executions.get());
		assertEquals(1, fixture.commits.get());
		assertEquals(1, fixture.rollbacks.get());
		fixture.assertResourcesReleased(3, 2);
	}

	@Test
	void resetCancellationRestoresInterruptionAndCannotAdmitARetry() {
		SQLException resetFailure = new SQLException("reset was cancelled", "57014", new InterruptedException("cancelled"));
		Fixture fixture = new Fixture((connection, reset) -> {
			if (reset == 1) {
				// Simulate a blocking driver call consuming the pending flag and retaining only throwable evidence.
				Thread.currentThread().interrupt();
				Thread.interrupted();
				throw resetFailure;
			}
		});
		AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			conditions.incrementAndGet();
			return true;
		});

		DatabaseException thrown = assertThrows(DatabaseException.class,
				() -> fixture.database.transactionWithRetry(policy, () -> {
					attempts.incrementAndGet();
					fixture.executeBatch();
				}));

		// Inspect before another connection checkout can consume the restored flag.
		boolean interruptedAtExit = Thread.interrupted();
		assertSame(resetFailure, thrown.getCause());
		assertTrue(interruptedAtExit);
		assertEquals(1, attempts.get());
		assertEquals(0, conditions.get());
		fixture.assertRolledBack(1);
	}

	@Test
	void ordinaryResetSqlFailuresAbortTheBatchAndReleaseItsResources() {
		List<SQLException> resetFailures = List.of(
				new SQLNonTransientConnectionException("connection lost during reset", "08006"),
				new SQLException("reset rejected", "42000"),
				new SQLException("plain unsupported SQLState", "0A000"),
				new SQLException("plain optional-feature SQLState", "HYC00"));

		for (SQLException resetFailure : resetFailures) {
			Fixture fixture = new Fixture((connection, reset) -> {
				if (reset == 1)
					throw resetFailure;
			});

			DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction);

			assertSame(resetFailure, thrown.getCause());
			fixture.assertRolledBack(1);
			assertFalse(Thread.currentThread().isInterrupted());
		}
	}

	@Test
	void resetSerializationFailureRetriesTheEntireBatchAfterRollback() {
		SQLException resetFailure = new SQLTransactionRollbackException("reset aborted the transaction", "40001");
		Fixture fixture = new Fixture((connection, reset) -> {
			if (reset == 1) {
				connection.rollback();
				throw resetFailure;
			}
		});
		AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			conditions.incrementAndGet();
			return failure.isSerializationFailure();
		});

		TransactionRetryResult<List<Long>> result = fixture.database.transactionWithRetry(policy, () -> {
			attempts.incrementAndGet();
			return Optional.of(fixture.executeBatch());
		});

		assertEquals(Optional.of(List.of(1L, 1L)), result.getValue());
		assertEquals(2, result.getAttemptCount());
		assertEquals(2, attempts.get());
		assertEquals(1, conditions.get());
		assertSame(resetFailure, result.getFailures().get(0).getCause());
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(3, fixture.executions.get());
		assertEquals(1, fixture.commits.get());
		assertEquals(1, fixture.rollbacks.get());
		fixture.assertResourcesReleased(3, 2);
	}

	@Test
	void aLaterResetFailureRetainsTheEarlierArrayReleaseFailure() {
		SQLException resetFailure = new SQLException("second chunk reset failed", "42000");
		SQLException freeFailure = new SQLException("first chunk array release failed", "08006");
		Fixture fixture = new Fixture((connection, reset) -> {
			if (reset == 2)
				throw resetFailure;
		}, freeFailure);

		DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction);

		assertSame(resetFailure, thrown.getCause());
		assertArrayEquals(new Throwable[]{freeFailure}, thrown.getSuppressed());
		fixture.assertRolledBack(2);
	}

	@Test
	void explicitlyUnsupportedResetKeepsTheBatchSuccessfulAndReleasesAllArrays() {
		List<Throwable> unsupported = List.of(new SQLFeatureNotSupportedException("reset unsupported"),
				new UnsupportedOperationException("reset unsupported"), new AbstractMethodError("clearParameters"));

		for (Throwable failure : unsupported) {
			Fixture fixture = new Fixture((connection, reset) -> {
				if (failure instanceof SQLException sqlException)
					throw sqlException;
				if (failure instanceof Error error)
					throw error;
				throw (RuntimeException) failure;
			});

			Optional<List<Long>> result = fixture.database.transaction(() -> Optional.of(fixture.executeBatch()));

			assertEquals(Optional.of(List.of(1L, 1L)), result);
			assertEquals(List.of(1, 2), fixture.ids());
			assertEquals(2, fixture.executions.get());
			assertEquals(1, fixture.commits.get());
			assertEquals(0, fixture.rollbacks.get());
			fixture.assertResourcesReleased(2, 1);
		}
	}

	private static final class Fixture {
		private final Database database;
		private final Database inspectionDatabase;
		private final AtomicInteger resets = new AtomicInteger(), executions = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger();
		private final AtomicInteger arraysCreated = new AtomicInteger(), arraysFreed = new AtomicInteger();
		private final AtomicInteger statementsClosed = new AtomicInteger(), connectionsClosed = new AtomicInteger();

		private Fixture(ResetFault fault) {
			this(fault, null);
		}

		private Fixture(ResetFault fault, SQLException firstArrayFreeFailure) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:batch_parameter_cleanup_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			this.inspectionDatabase = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
			this.inspectionDatabase.query("CREATE TABLE batch_work (id INT PRIMARY KEY, values_array INTEGER ARRAY)").execute();
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(source, method, args);
				if (!(result instanceof Connection connection))
					return result;
				Connection[] handle = new Connection[1];
				Map<Array, Array> nativeArrays = new IdentityHashMap<>();
				handle[0] = proxy(Connection.class, (operation, parameters) -> {
					Object value = invoke(connection, operation, parameters);
					if (operation.getName().equals("commit")) commits.incrementAndGet();
					if (operation.getName().equals("rollback")) rollbacks.incrementAndGet();
					if (operation.getName().equals("close")) connectionsClosed.incrementAndGet();
					if (operation.getName().equals("createArrayOf")) {
						Array array = (Array) value;
						int index = arraysCreated.incrementAndGet();
						Array resource = proxy(Array.class, (arrayMethod, arrayArgs) -> {
							Object arrayValue = invoke(array, arrayMethod, arrayArgs);
							if (arrayMethod.getName().equals("free")) {
								arraysFreed.incrementAndGet();
								if (index == 1 && firstArrayFreeFailure != null)
									throw firstArrayFreeFailure;
								}
							return arrayValue;
						});
						nativeArrays.put(resource, array);
						return resource;
					}
					if (!(value instanceof PreparedStatement statement))
						return value;
					return proxy(PreparedStatement.class, (statementMethod, statementArgs) -> {
						String name = statementMethod.getName();
						if (name.equals("getConnection"))
							return handle[0];
						if (name.equals("clearParameters"))
							fault.beforeClear(connection, resets.incrementAndGet());
						if (name.equals("executeLargeBatch") || name.equals("executeBatch"))
							executions.incrementAndGet();
						if (name.equals("setArray")) {
							statementArgs = statementArgs.clone();
							statementArgs[1] = nativeArrays.get((Array) statementArgs[1]);
						}
						Object statementResult = invoke(statement, statementMethod, statementArgs);
						if (name.equals("close")) statementsClosed.incrementAndGet();
						return statementResult;
					});
				});
				return handle[0];
			});
			this.database = Database.withDataSource(wrapped).databaseType(DatabaseType.GENERIC).build();
		}

		private List<Long> executeBatch() {
			return this.database.query("INSERT INTO batch_work (id, values_array) VALUES (:id, :values)")
					.batchChunkSize(1).executeBatch(List.of(
							Map.of("id", 1, "values", Parameters.sqlArrayOf("INTEGER", List.of(3))),
							Map.of("id", 2, "values", Parameters.sqlArrayOf("INTEGER", List.of(4)))));
		}

		private void executeInTransaction() {
			this.database.transaction(() -> { executeBatch(); });
		}

		private List<Integer> ids() {
			return this.inspectionDatabase.query("SELECT id FROM batch_work ORDER BY id").fetchList(Integer.class);
		}

		private void assertRolledBack(int completedChunks) {
			assertEquals(List.of(), ids());
			assertEquals(completedChunks, this.executions.get());
			assertEquals(completedChunks, this.resets.get());
			assertEquals(0, this.commits.get());
			assertEquals(1, this.rollbacks.get());
			assertTrue(this.database.currentTransaction().isEmpty());
			assertResourcesReleased(completedChunks, 1);
		}

		private void assertResourcesReleased(int arrays, int transactions) {
			assertEquals(arrays, this.arraysCreated.get());
			assertEquals(arrays, this.arraysFreed.get());
			assertEquals(transactions, this.statementsClosed.get());
			assertEquals(transactions, this.connectionsClosed.get());
		}
	}

	@FunctionalInterface
	private interface ResetFault {
		void beforeClear(Connection connection, int reset) throws SQLException;
	}

	@FunctionalInterface
	private interface Invocation {
		Object invoke(Method method, Object[] args) throws Throwable;
	}

	private static <T> T proxy(Class<T> type, Invocation invocation) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> invocation.invoke(method, args)));
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try {
			return method.invoke(target, args);
		} catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}
}
