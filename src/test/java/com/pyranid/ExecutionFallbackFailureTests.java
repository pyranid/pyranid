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
import java.sql.SQLTransactionRollbackException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionFallbackFailureTests {
	@AfterEach
	void clearInterruptFlag() { Thread.interrupted(); }

	@Test
	void rollbackEvidencePreventsFallbackAndDoesNotPoisonTheCapabilityCache() {
		for (String operation : List.of("update", "batch")) {
			for (boolean typed : new boolean[]{false, true}) {
				SQLException failure;
				if (typed) {
					failure = new SQLFeatureNotSupportedException("large operation unsupported", "0A000");
					failure.setNextException(new SQLTransactionRollbackException("transaction rolled back", "40001"));
				} else failure = new SQLTransactionRollbackException("transaction rolled back", "40001",
						new SQLFeatureNotSupportedException("optional driver detail unavailable", "0A000"));
				Fixture fixture = new Fixture(operation, failure, true);

				DatabaseException thrown = assertThrows(DatabaseException.class,
						() -> fixture.database.transaction(() -> fixture.work(0)));

				assertSame(failure, thrown.getCause());
				assertEquals(List.of(), fixture.ids());
				fixture.assertReleased(1, 0, 1);
				assertEquals(0, fixture.fallbacks.get());
				assertEquals(1, fixture.largeCalls.get());
				// A terminal failure must not cache NO and change the next transaction's execution method.
				fixture.database.transaction(() -> fixture.work(0));
				assertEquals(fixture.expected(0), fixture.ids());
				assertEquals(2, fixture.largeCalls.get());
				assertEquals(0, fixture.fallbacks.get());
				fixture.assertReleased(2, 1, 1);
			}
		}
	}

	@Test
	void unsupportedExecutionWithCancellationCannotFallbackOrRetry() {
		for (String operation : List.of("update", "batch")) {
			for (String edge : List.of("cause", "next", "suppressed", "cycle", "runtime", "legacy")) {
				InterruptedException interruption = new InterruptedException("cancelled execution");
				Throwable failure = switch (edge) {
					case "runtime" -> new UnsupportedOperationException("unsupported", interruption);
					case "legacy" -> new AbstractMethodError("large operation");
					default -> new SQLFeatureNotSupportedException("unsupported", "0A000");
				};
				if (edge.equals("cause")) failure.initCause(interruption);
				else if (edge.equals("next") || edge.equals("cycle")) {
					SQLException detail = new SQLException("cancelled", "57014", interruption);
					((SQLException) failure).setNextException(detail);
					if (edge.equals("cycle")) detail.setNextException((SQLException) failure);
				} else if (!edge.equals("runtime")) failure.addSuppressed(interruption);
				Fixture fixture = new Fixture(operation, failure, false);
				AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
				RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), ignored -> {
					policyCalls.incrementAndGet(); return true;
				});
				Class<? extends Throwable> expected = edge.equals("legacy") ? Error.class : DatabaseException.class;

				Throwable thrown = assertThrows(expected, () -> fixture.database.transactionWithRetry(policy, () -> {
					attempts.incrementAndGet(); fixture.work(0);
				}));

				assertTrue(Thread.interrupted(), operation + " " + edge);
				assertTrue(contains(thrown, failure));
				assertEquals(1, attempts.get());
				assertEquals(0, policyCalls.get());
				assertEquals(List.of(), fixture.ids());
				assertEquals(0, fixture.fallbacks.get());
				fixture.assertReleased(1, 0, 1);
			}
		}
	}

	@Test
	void fatalAndCommittedEvidenceCannotAuthorizeFallback() {
		for (String operation : List.of("update", "batch")) {
			for (boolean committed : new boolean[]{false, true}) {
				SQLException failure = new SQLFeatureNotSupportedException("unsupported", "0A000");
				if (committed) {
					DatabaseException completed = new DatabaseException("independent transaction committed");
					completed.markTransactionOutcomeCommitted();
					failure.addSuppressed(completed);
				} else failure.addSuppressed(new AssertionError("fatal driver failure"));
				Fixture fixture = new Fixture(operation, failure, false);
				AtomicInteger policyCalls = new AtomicInteger();
				RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), ignored -> {
					policyCalls.incrementAndGet(); return true;
				});

				DatabaseException thrown = assertThrows(DatabaseException.class, () -> {
					if (committed) fixture.database.transactionWithRetry(policy, () -> fixture.work(0));
					else fixture.database.transaction(() -> fixture.work(0));
				});

				assertSame(failure, thrown.getCause());
				assertEquals(0, policyCalls.get());
				assertEquals(0, fixture.fallbacks.get());
				assertEquals(List.of(), fixture.ids());
				fixture.assertReleased(1, 0, 1);
			}
		}
	}

	@Test
	void unreadableCapabilityEvidenceRetainsTheOriginalFailure() {
		for (String operation : List.of("update", "batch")) {
			for (String accessor : List.of("state", "cause", "message")) {
				RuntimeException diagnostic = new RuntimeException("unreadable " + accessor,
						accessor.equals("message") ? new InterruptedException("cancelled diagnostic") : null);
				SQLException failure = new SQLException("unsupported driver method", "HY000") {
					@Override public String getSQLState() {
						if (accessor.equals("state")) throw diagnostic;
						return super.getSQLState();
					}
					@Override public Throwable getCause() {
						if (accessor.equals("cause")) throw diagnostic;
						return super.getCause();
					}
					@Override public String getMessage() {
						if (accessor.equals("message")) throw diagnostic;
						return super.getMessage();
					}
				};
				Fixture fixture = new Fixture(operation, failure, false);

				DatabaseException thrown = assertThrows(DatabaseException.class,
						() -> fixture.database.transaction(() -> fixture.work(0)));

				assertSame(failure, thrown.getCause());
				assertEquals(accessor.equals("message"), Thread.interrupted());
				assertEquals(0, fixture.fallbacks.get());
				assertEquals(List.of(), fixture.ids());
				fixture.assertReleased(1, 0, 1);
			}
		}
	}

	@Test
	void serializationRetryRepeatsTheWholeTransaction() {
		for (String operation : List.of("update", "batch")) {
			SQLException failure = new SQLTransactionRollbackException("transaction rolled back", "40001",
					new SQLFeatureNotSupportedException("optional detail unsupported", "0A000"));
			Fixture fixture = new Fixture(operation, failure, true);
			AtomicInteger attempts = new AtomicInteger();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO),
					DatabaseException::isSerializationFailure);

			TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(policy, () -> {
				attempts.incrementAndGet(); fixture.work(0);
			});

			assertEquals(2, attempts.get());
			assertEquals(2, result.getAttemptCount());
			assertSame(failure, result.getFailures().get(0).getCause());
			assertEquals(fixture.expected(0), fixture.ids());
			assertEquals(2, fixture.largeCalls.get());
			assertEquals(0, fixture.fallbacks.get());
			fixture.assertReleased(2, 1, 1);
		}
	}

	@Test
	void benignUnsupportedMethodsStillFallbackAndCacheTheirCapability() {
		for (String operation : List.of("update", "batch")) {
			for (Throwable failure : List.of(new SQLFeatureNotSupportedException("unsupported", "0A000"),
					new UnsupportedOperationException("unsupported"), new AbstractMethodError("large operation"),
					new SQLException("unavailable", "0A000"), new SQLException("unavailable", "HYC00"),
					new SQLException("not implemented", "HY000"),
					new SQLException("unavailable", "HY000", new UnsupportedOperationException("optional method")))) {
				Fixture fixture = new Fixture(operation, failure, false);

				fixture.database.transaction(() -> { fixture.work(0); fixture.work(10); });

				List<Integer> expected = new java.util.ArrayList<>(fixture.expected(0));
				expected.addAll(fixture.expected(10));
				assertEquals(expected, fixture.ids());
				assertEquals(1, fixture.largeCalls.get());
				assertEquals(2, fixture.fallbacks.get());
				fixture.assertReleased(2, 1, 0);
			}
		}
	}

	@Test
	void anOrdinarySqlFailureNeverAuthorizesFallback() {
		for (String operation : List.of("update", "batch")) {
			SQLException failure = new SQLException("invalid statement", "42000");
			Fixture fixture = new Fixture(operation, failure, false);

			DatabaseException thrown = assertThrows(DatabaseException.class,
					() -> fixture.database.transaction(() -> fixture.work(0)));

			assertSame(failure, thrown.getCause());
			assertEquals(0, fixture.fallbacks.get());
			assertEquals(List.of(), fixture.ids());
			fixture.assertReleased(1, 0, 1);
		}
	}

	private static boolean contains(Throwable failure, Throwable original) {
		return ThrowableGraph.inspect(failure, current -> current == original ? 1 : 0, 0) != 0;
	}

	private static final class Fixture {
		private final String operation;
		private final Database database, inspection;
		private final AtomicInteger largeCalls = new AtomicInteger(), fallbacks = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger();
		private final AtomicInteger statementCloses = new AtomicInteger(), connectionCloses = new AtomicInteger();

		private Fixture(String operation, Throwable failure, boolean physicalRollback) {
			this.operation = operation;
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:execution_fallback_" + UUID.randomUUID());
			source.setUser("sa"); source.setPassword("");
			this.inspection = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
			AtomicBoolean armed = new AtomicBoolean(true);
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(source, method, args);
				if (!(result instanceof Connection connection)) return result;
				Connection[] handle = new Connection[1];
				handle[0] = proxy(Connection.class, (call, values) -> {
					String name = call.getName();
					if (name.equals("commit")) this.commits.incrementAndGet();
					if (name.equals("rollback")) this.rollbacks.incrementAndGet();
					if (name.equals("close")) this.connectionCloses.incrementAndGet();
					Object value = invoke(connection, call, values);
					if (!(value instanceof PreparedStatement statement)) return value;
					return proxy(PreparedStatement.class, (jdbc, parameters) -> {
						String jdbcName = jdbc.getName();
						if (jdbcName.equals("getConnection")) return handle[0];
						if (jdbcName.equals("executeLargeUpdate") || jdbcName.equals("executeLargeBatch")) {
							this.largeCalls.incrementAndGet();
							if (armed.compareAndSet(true, false)) {
								if (physicalRollback) connection.rollback();
								throw failure;
							}
						}
						if (jdbcName.equals("executeUpdate") || jdbcName.equals("executeBatch")) this.fallbacks.incrementAndGet();
						if (jdbcName.equals("close")) this.statementCloses.incrementAndGet();
						return invoke(statement, jdbc, parameters);
					});
				});
				return handle[0];
			});
			this.database = Database.withDataSource(wrapped).databaseType(DatabaseType.GENERIC).build();
		}

		private void work(int offset) {
			this.database.useRawConnection(connection -> {
				try (java.sql.Statement statement = connection.createStatement()) {
					statement.executeUpdate("INSERT INTO work VALUES (" + (offset + 1) + ")");
				}
				return Optional.empty();
			});
			if (this.operation.equals("update")) this.database.query("INSERT INTO work VALUES (:id)").bind("id", offset + 2).execute();
			else this.database.query("INSERT INTO work VALUES (:id)")
					.executeBatch(List.of(Map.of("id", offset + 2), Map.of("id", offset + 3)));
		}

		private List<Integer> expected(int offset) {
			return this.operation.equals("update") ? List.of(offset + 1, offset + 2) : List.of(offset + 1, offset + 2, offset + 3);
		}

		private List<Integer> ids() { return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class); }

		private void assertReleased(int statements, int commits, int rollbacks) {
			assertEquals(statements, this.statementCloses.get());
			assertEquals(commits, this.commits.get());
			assertEquals(rollbacks, this.rollbacks.get());
			assertEquals(commits + rollbacks, this.connectionCloses.get());
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
