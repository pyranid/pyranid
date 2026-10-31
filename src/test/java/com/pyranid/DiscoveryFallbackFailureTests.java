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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransactionRollbackException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DiscoveryFallbackFailureTests {
	@AfterEach
	void clearInterruptFlag() { Thread.interrupted(); }

	@Test
	void initializationCancellationCannotRunStatementsOrAdmitAnotherAttempt() {
		for (String getter : List.of("getDatabaseProductName", "getDatabaseProductVersion")) {
			for (String edge : List.of("cause", "next", "suppressed", "cycle", "unreadable")) {
				SQLException original = cancelled(edge);
				Fixture fixture = new Fixture((connection, method, call) -> {
					if (method.equals(getter)) throw original;
				});
				AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
				RetryPolicy policy = retry(conditions);

				DatabaseException thrown = assertThrows(DatabaseException.class, () ->
						fixture.database.transactionWithRetry(policy, () -> {
							attempts.incrementAndGet();
							fixture.database.query("INSERT INTO work VALUES (1)").execute();
							throw new DatabaseException(new SQLTransactionRollbackException("later failure", "40001"));
						}), getter + edge);

				assertTrue(Thread.interrupted(), getter + edge);
				assertTrue(contains(thrown, original), getter + edge);
				assertEquals(1, attempts.get());
				assertEquals(0, conditions.get());
				assertEquals(0, fixture.prepared.get());
				assertEquals(0, fixture.commits.get());
				assertEquals(List.of(), fixture.ids());
				fixture.assertConnectionsClosed(1);
			}
		}
	}

	@Test
	void initializationTerminalFailuresCannotBeReplacedWithGenericType() {
		for (Throwable original : List.of(
				new SQLNonTransientConnectionException("connection lost", "08006"),
				new SQLTransactionRollbackException("initialization rolled back", "40001"),
				new AssertionError("fatal metadata failure"))) {
			Fixture fixture = new Fixture((connection, method, call) -> {
				if (method.equals("getDatabaseProductName")) throw original;
			});

			Class<? extends Throwable> expected = original instanceof Error ? Error.class : DatabaseException.class;
			Throwable thrown = assertThrows(expected,
					() -> fixture.database.transaction(() -> fixture.rawInsert(1)));

			if (original instanceof Error) assertSame(original, thrown);
			else assertTrue(contains(thrown, original));
			assertEquals(0, fixture.commits.get());
			assertEquals(List.of(), fixture.ids());
			fixture.assertConnectionsClosed(1);
		}
	}

	@Test
	void optionalVersionDiscoveryRetainsTerminalFailureAndClosesItsCandidate() throws SQLException {
		for (String entry : List.of("source", "connection")) {
			SQLException original = cancelled("cause");
			Fixture fixture = new Fixture((connection, method, call) -> {
				if (method.equals("getDatabaseProductVersion")) throw original;
			});

			DatabaseException thrown = assertThrows(DatabaseException.class, () -> {
				if (entry.equals("source")) DatabaseType.fromDataSource(fixture.source);
				else try (Connection connection = fixture.source.getConnection()) {
					DatabaseType.fromConnection(connection);
				}
			});

			assertTrue(contains(thrown, original));
			assertFalse(Thread.currentThread().isInterrupted());
			fixture.assertConnectionsClosed(1);
		}
	}

	@Test
	void benignInitializationDiscoveryFailuresRemainOptional() {
		for (String getter : List.of("getDatabaseProductName", "getDatabaseProductVersion")) {
			for (SQLException original : List.of(new SQLFeatureNotSupportedException("optional metadata", "0A000"),
					new SQLException("metadata unavailable", "HY000"))) {
				Fixture fixture = new Fixture((connection, method, call) -> {
					if (method.equals(getter)) throw original;
				});

				fixture.database.transaction(() -> fixture.rawInsert(1));

				assertEquals(List.of(1), fixture.ids());
				assertEquals(1, fixture.commits.get());
				assertFalse(Thread.currentThread().isInterrupted());
				fixture.assertConnectionsClosed(1);
			}
		}
	}

	@Test
	void streamDiscoveryRollbackCannotCommitOnlyLaterWrites() {
		SQLException original = new SQLTransactionRollbackException("stream metadata rolled back", "40001");
		Fixture fixture = streamFixture(original, true);

		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transaction(() -> {
			fixture.rawInsert(1);
			fixture.database.query("SELECT id FROM work").fetchStream(Integer.class, values -> values.toList());
			fixture.rawInsert(2);
		}));

		assertTrue(contains(thrown, original));
		assertEquals(List.of(), fixture.ids());
		assertEquals(0, fixture.prepared.get());
		assertEquals(0, fixture.commits.get());
		assertEquals(1, fixture.fullRollbacks.get());
		fixture.assertConnectionsClosed(1);
	}

	@Test
	void streamDiscoveryCancellationCannotAdmitAnotherAttempt() {
		for (String edge : List.of("cause", "next", "suppressed", "cycle", "unreadable")) {
			SQLException original = cancelled(edge);
			Fixture fixture = streamFixture(original, false);
			AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();

			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(
					retry(conditions), () -> {
						attempts.incrementAndGet();
						fixture.rawInsert(1);
						fixture.database.query("SELECT id FROM work").fetchStream(Integer.class, values -> values.toList());
						fixture.rawInsert(2);
					}));

			assertTrue(Thread.interrupted(), edge);
			assertTrue(contains(thrown, original), edge);
			assertEquals(1, attempts.get());
			assertEquals(0, conditions.get());
			assertEquals(List.of(), fixture.ids());
			assertEquals(0, fixture.prepared.get());
			assertEquals(0, fixture.commits.get());
			fixture.assertConnectionsClosed(1);
		}
	}

	@Test
	void streamDiscoverySerializationFailureRetriesTheEntireTransaction() {
		SQLException original = new SQLTransactionRollbackException("stream metadata rolled back", "40001");
		Fixture fixture = streamFixture(original, true);
		AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();

		TransactionRetryResult<List<Integer>> result = fixture.database.transactionWithRetry(retry(conditions), () -> {
			attempts.incrementAndGet();
			fixture.rawInsert(1);
			List<Integer> values = fixture.database.query("SELECT id FROM work").fetchStream(Integer.class, stream -> stream.toList());
			fixture.rawInsert(2);
			return Optional.of(values);
		});

		assertEquals(Optional.of(List.of(1)), result.getValue());
		assertEquals(2, result.getAttemptCount());
		assertEquals(2, attempts.get());
		assertEquals(1, conditions.get());
		assertTrue(contains(result.getFailures().get(0), original));
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(1, fixture.commits.get());
		assertEquals(1, fixture.fullRollbacks.get());
		assertEquals(1, fixture.prepared.get());
		assertEquals(1, fixture.statementsClosed.get());
		fixture.assertConnectionsClosed(2);
	}

	@Test
	void benignStreamDiscoveryFailureKeepsGenericFallbackWorking() {
		Fixture fixture = streamFixture(new SQLFeatureNotSupportedException("optional metadata", "0A000"), false);

		fixture.database.transaction(() -> {
			fixture.rawInsert(1);
			assertEquals(List.of(1), fixture.database.query("SELECT id FROM work").fetchStream(Integer.class, values -> values.toList()));
			fixture.rawInsert(2);
		});

		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(1, fixture.commits.get());
		assertEquals(1, fixture.statementsClosed.get());
		fixture.assertConnectionsClosed(1);
	}

	@Test
	void standaloneStreamDiscoveryFailureClosesBeforePreparingTheStatement() {
		for (SQLException original : List.of(cancelled("cause"),
				new SQLNonTransientConnectionException("connection lost", "08006"))) {
			Fixture fixture = new Fixture((connection, method, call) -> {
				if (method.equals("getDatabaseProductName") && call == 1) throw original;
			});

			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database
					.query("SELECT id FROM work").fetchStream(Integer.class, values -> values.toList()));

			assertTrue(contains(thrown, original));
			assertEquals(0, fixture.prepared.get());
			assertFalse(Thread.currentThread().isInterrupted());
			fixture.assertConnectionsClosed(1);
		}
	}

	private static RetryPolicy retry(AtomicInteger conditions) {
		return RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			conditions.incrementAndGet();
			return true;
		});
	}

	private static Fixture streamFixture(SQLException original, boolean rolledBack) {
		return new Fixture((connection, method, call) -> {
			if (!method.equals("getDatabaseProductName")) return;
			if (call == 1) throw new SQLFeatureNotSupportedException("initial metadata unavailable", "0A000");
			if (call == 2) {
				if (rolledBack) connection.rollback();
				throw original;
			}
		});
	}

	private static SQLException cancelled(String edge) {
		InterruptedException interruption = new InterruptedException("metadata cancelled");
		SQLException original = edge.equals("unreadable")
				? new SQLFeatureNotSupportedException("optional metadata", "0A000") {
					@Override public String getSQLState() { throw new IllegalStateException("unreadable state"); }
				} : new SQLFeatureNotSupportedException("optional metadata", "0A000");
		switch (edge) {
			case "cause" -> original.initCause(interruption);
			case "next", "cycle" -> {
				SQLException next = new SQLException("cancelled", "57014", interruption);
				original.setNextException(next);
				if (edge.equals("cycle")) next.setNextException(original);
			}
			default -> original.addSuppressed(interruption);
		}
		return original;
	}

	private static boolean contains(Throwable failure, Throwable original) {
		return ThrowableGraph.inspect(failure, node -> node == original ? 1 : 0, 0) != 0;
	}

	private static final class Fixture {
		private final DataSource source;
		private final Database database, inspection;
		private final AtomicInteger connections = new AtomicInteger(), closes = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger(), fullRollbacks = new AtomicInteger();
		private final AtomicInteger prepared = new AtomicInteger(), statementsClosed = new AtomicInteger();
		private final AtomicInteger productNames = new AtomicInteger(), versions = new AtomicInteger();

		private Fixture(MetadataFault fault) {
			JDBCDataSource nativeSource = new JDBCDataSource();
			nativeSource.setUrl("jdbc:hsqldb:mem:discovery_fallback_" + UUID.randomUUID());
			nativeSource.setUser("sa");
			nativeSource.setPassword("");
			this.inspection = Database.withDataSource(nativeSource).databaseType(DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
			this.source = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(nativeSource, method, args);
				if (!(result instanceof Connection connection)) return result;
				this.connections.incrementAndGet();
				return proxy(Connection.class, (operation, parameters) -> {
					String name = operation.getName();
					if (name.equals("commit")) this.commits.incrementAndGet();
					if (name.equals("rollback") && (parameters == null || parameters.length == 0)) this.fullRollbacks.incrementAndGet();
					if (name.equals("close")) this.closes.incrementAndGet();
					Object value = invoke(connection, operation, parameters);
					if (value instanceof DatabaseMetaData metadata) return proxy(DatabaseMetaData.class, (getter, values) -> {
						int call = switch (getter.getName()) {
							case "getDatabaseProductName" -> this.productNames.incrementAndGet();
							case "getDatabaseProductVersion" -> this.versions.incrementAndGet();
							default -> 0;
						};
						fault.beforeRead(connection, getter.getName(), call);
						return invoke(metadata, getter, values);
					});
					if (value instanceof PreparedStatement statement) {
						this.prepared.incrementAndGet();
						return proxy(PreparedStatement.class, (action, values) -> {
							if (action.getName().equals("close")) this.statementsClosed.incrementAndGet();
							return invoke(statement, action, values);
						});
					}
					return value;
				});
			});
			this.database = Database.withDataSource(this.source).build();
		}

		private void rawInsert(int id) {
			this.database.useRawConnection(connection -> {
				try (java.sql.Statement statement = connection.createStatement()) {
					statement.executeUpdate("INSERT INTO work VALUES (" + id + ")");
				}
				return Optional.empty();
			});
		}

		private List<Integer> ids() {
			return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class);
		}

		private void assertConnectionsClosed(int expected) {
			assertEquals(expected, this.connections.get());
			assertEquals(expected, this.closes.get());
			assertTrue(this.database.currentTransaction().isEmpty());
		}
	}

	@FunctionalInterface
	private interface MetadataFault {
		void beforeRead(Connection connection, String method, int call) throws Throwable;
	}

	@FunctionalInterface
	private interface Invocation { Object call(Method method, Object[] arguments) throws Throwable; }

	private static <T> T proxy(Class<T> type, Invocation invocation) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(object, method, arguments) -> invocation.call(method, arguments)));
	}

	private static Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
		try { return method.invoke(target, arguments); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
