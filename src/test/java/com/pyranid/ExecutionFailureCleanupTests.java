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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionFailureCleanupTests {
	@Test
	void streamOpenDiagnosticAccessorFailuresRetainCauseAndCloseEveryResource() throws Exception {
		for (String accessor : List.of("message", "sqlState", "errorCode", "cause")) {
			SQLException original = unreadableFailure(accessor);
			try (Fixture fixture = new Fixture(original, null, false)) {
				DatabaseException thrown = assertThrows(DatabaseException.class,
						() -> fixture.database.query("VALUES (1)").fetchStream(Integer.class, stream -> stream.findFirst()));
				assertSame(original, thrown.getCause());
				assertEquals(1, fixture.statementCloses.get());
				assertEquals(1, fixture.connectionCloses.get());
				assertTrue(fixture.connections.get(0).isClosed());
			}
		}
	}

	@Test
	void streamOpenDiagnosticFailureReleasesTransactionLockForJoinedWorker() throws Exception {
		try (Fixture fixture = new Fixture(unreadableFailure("sqlState"), null, false)) {
			fixture.database.transaction(() -> {
				Transaction transaction = fixture.database.currentTransaction().orElseThrow();
				assertThrows(DatabaseException.class,
						() -> fixture.database.query("VALUES (1)").fetchStream(Integer.class, stream -> stream.findFirst()));
				fixture.bodyFailure = null;
				FutureTask<Optional<Integer>> work = new FutureTask<>(() -> fixture.database.participate(transaction,
						() -> fixture.database.query("VALUES (1)").fetchObject(Integer.class)));
				Thread worker = new Thread(work, "stream-cleanup-participant");
				worker.start();
				try {
					assertEquals(Optional.of(1), work.get(2, TimeUnit.SECONDS));
				} finally {
					worker.interrupt();
					worker.join(2_000);
				}
			});
			assertEquals(2, fixture.statementCloses.get());
			assertEquals(1, fixture.connectionCloses.get());
		}
	}

	@Test
	void streamFailureMetadataFallbackStillScrubsSecureParameters() throws Exception {
		String secret = "sensitive-driver-echo";
		SQLException original = new SQLException("rejected " + secret, "42000") {
			@Override public String getSQLState() { throw new IllegalStateException("unreadable SQLState"); }
		};
		try (Fixture fixture = new Fixture(original, null, false)) {
			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.query("VALUES (:value)")
					.bind("value", Parameters.secure(secret)).fetchStream(Integer.class, stream -> stream.findFirst()));
			assertSame(original, thrown.getCause());
			assertFalse(thrown.getMessage().contains(secret));
			assertTrue(thrown.getMessage().contains("<redacted>"));
			assertEquals(1, fixture.statementCloses.get());
			assertEquals(1, fixture.connectionCloses.get());
		}
	}

	@Test
	void identicalStatementExecutionAndCloseFailuresPreserveOriginal() throws Exception {
		for (Throwable original : List.of(new SQLException("cached failure", "08006"), new AssertionError("cached fatal failure"))) {
			try (Fixture fixture = new Fixture(original, original, false)) {
				Throwable thrown = assertThrows(Throwable.class, () -> fixture.database.query("VALUES (1)").fetchObject(Integer.class));
				assertOriginal(original, thrown);
				assertEquals(0, original.getSuppressed().length);
				assertEquals(1, fixture.statementCloses.get());
				assertEquals(1, fixture.connectionCloses.get());
			}
		}
	}

	@Test
	void identicalResultSetIterationAndCloseFailuresPreserveFatalErrors() throws Exception {
		for (boolean list : List.of(false, true)) {
			AssertionError original = new AssertionError("cached row failure");
			try (Fixture fixture = new Fixture(original, original, true)) {
				Throwable thrown = assertThrows(Throwable.class, () -> {
					if (list) fixture.database.query("VALUES (1)").fetchList(Integer.class);
					else fixture.database.query("VALUES (1)").fetchObject(Integer.class);
				});
				assertSame(original, thrown);
				assertEquals(0, original.getSuppressed().length);
				assertEquals(1, fixture.resultCloses.get());
				assertEquals(1, fixture.statementCloses.get());
				assertEquals(1, fixture.connectionCloses.get());
			}
		}
	}

	@Test
	void identicalGeneratedKeyIterationAndCloseFailuresPreserveFatalErrors() throws Exception {
		for (boolean list : List.of(false, true)) {
			try (Fixture fixture = new Fixture(null, null, true)) {
				fixture.database.query("CREATE TABLE generated_items (id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)").execute();
				AssertionError original = new AssertionError("cached generated-key failure");
				fixture.bodyFailure = original;
				fixture.cleanupFailure = original;
				Throwable thrown = assertThrows(Throwable.class, () -> {
					if (list) fixture.database.query("INSERT INTO generated_items DEFAULT VALUES").executeReturningGeneratedKeys(Integer.class);
					else fixture.database.query("INSERT INTO generated_items DEFAULT VALUES").executeReturningGeneratedKey(Integer.class);
				});
				assertSame(original, thrown);
				assertEquals(0, original.getSuppressed().length);
				assertEquals(1, fixture.resultCloses.get());
				assertEquals(2, fixture.statementCloses.get());
				assertEquals(2, fixture.connectionCloses.get());
			}
		}
	}

	@Test
	void identicalStatementErrorNeverEntersTransactionRetryPolicy() throws Exception {
		AssertionError original = new AssertionError("cached fatal failure");
		try (Fixture fixture = new Fixture(original, original, false)) {
			AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				conditions.incrementAndGet(); return true;
			});
			assertSame(original, assertThrows(AssertionError.class, () -> fixture.database.transactionWithRetry(policy, () -> {
				attempts.incrementAndGet();
				fixture.database.query("VALUES (1)").fetchObject(Integer.class);
			})));
			assertEquals(1, attempts.get());
			assertEquals(0, conditions.get());
			assertEquals(1, fixture.rollbacks.get());
			assertEquals(1, fixture.statementCloses.get());
			assertEquals(1, fixture.connectionCloses.get());
		}
	}

	@Test
	void distinctStatementCloseFailureRemainsSuppressedOnFatalPrimary() throws Exception {
		AssertionError original = new AssertionError("operation"), cleanup = new AssertionError("close");
		try (Fixture fixture = new Fixture(original, cleanup, false)) {
			assertSame(original, assertThrows(AssertionError.class, () -> fixture.database.query("VALUES (1)").fetchObject(Integer.class)));
			assertArrayEquals(new Throwable[]{cleanup}, original.getSuppressed());
			assertEquals(1, fixture.statementCloses.get());
			assertEquals(1, fixture.connectionCloses.get());
		}
	}

	@Test
	void cleanupOnlyFailureStillPropagates() throws Exception {
		AssertionError cleanup = new AssertionError("statement close");
		try (Fixture fixture = new Fixture(null, cleanup, false)) {
			assertSame(cleanup, assertThrows(AssertionError.class, () -> fixture.database.query("VALUES (1)").fetchObject(Integer.class)));
			assertEquals(1, fixture.resultCloses.get());
			assertEquals(1, fixture.statementCloses.get());
			assertEquals(1, fixture.connectionCloses.get());
		}
	}

	private static SQLException unreadableFailure(String accessor) {
		return new SQLException("original driver failure", "08006") {
			@Override public String getMessage() { if (accessor.equals("message")) throw new IllegalStateException("unreadable message"); return super.getMessage(); }
			@Override public String getSQLState() { if (accessor.equals("sqlState")) throw new IllegalStateException("unreadable SQLState"); return super.getSQLState(); }
			@Override public int getErrorCode() { if (accessor.equals("errorCode")) throw new IllegalStateException("unreadable error code"); return super.getErrorCode(); }
			@Override public synchronized Throwable getCause() { if (accessor.equals("cause")) throw new IllegalStateException("unreadable cause"); return super.getCause(); }
		};
	}

	private static void assertOriginal(Throwable original, Throwable thrown) {
		if (original instanceof Error) assertSame(original, thrown);
		else { assertInstanceOf(DatabaseException.class, thrown); assertSame(original, thrown.getCause()); }
	}

	private static final class Fixture implements AutoCloseable {
		private final List<Connection> connections = new ArrayList<>();
		private final AtomicInteger statementCloses = new AtomicInteger(), connectionCloses = new AtomicInteger(), resultCloses = new AtomicInteger(), rollbacks = new AtomicInteger();
		private Throwable bodyFailure;
		private Throwable cleanupFailure;
		private final Database database;

		private Fixture(Throwable bodyFailure, Throwable cleanupFailure, boolean failResult) {
			this.bodyFailure = bodyFailure;
			this.cleanupFailure = cleanupFailure;
			JDBCDataSource actual = new JDBCDataSource();
			actual.setUrl("jdbc:hsqldb:mem:execution_cleanup_" + UUID.randomUUID());
			actual.setUser("sa"); actual.setPassword("");
			DataSource source = proxy(DataSource.class, (method, args) -> {
				if (!method.getName().equals("getConnection")) return invoke(actual, method, args);
				Connection connection = (Connection) invoke(actual, method, args);
				this.connections.add(connection);
				return proxy(Connection.class, (operation, parameters) -> {
					if (operation.getName().equals("close")) this.connectionCloses.incrementAndGet();
					if (operation.getName().equals("rollback")) this.rollbacks.incrementAndGet();
					Object value = invoke(connection, operation, parameters);
					if (!(value instanceof PreparedStatement statement)) return value;
					return proxy(PreparedStatement.class, (statementMethod, statementArgs) -> {
						if (statementMethod.getName().equals("close")) {
							this.statementCloses.incrementAndGet();
							invoke(statement, statementMethod, statementArgs);
							if (!failResult && this.cleanupFailure != null) throw this.cleanupFailure;
							return null;
						}
						if (statementMethod.getName().equals("executeQuery") && !failResult && this.bodyFailure != null) throw this.bodyFailure;
						Object result = invoke(statement, statementMethod, statementArgs);
						if (!(result instanceof ResultSet rows)) return result;
						return proxy(ResultSet.class, (rowMethod, rowArgs) -> {
							if (rowMethod.getName().equals("next") && failResult && this.bodyFailure != null) throw this.bodyFailure;
							if (rowMethod.getName().equals("close")) {
								this.resultCloses.incrementAndGet();
								invoke(rows, rowMethod, rowArgs);
								if (failResult && this.cleanupFailure != null) throw this.cleanupFailure;
								return null;
							}
							return invoke(rows, rowMethod, rowArgs);
						});
					});
				});
			});
			this.database = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
		}

		@Override public void close() throws Exception {
			for (Connection connection : this.connections) if (!connection.isClosed()) connection.close();
		}
	}

	private interface Call { Object run(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> call.run(method, args)));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
