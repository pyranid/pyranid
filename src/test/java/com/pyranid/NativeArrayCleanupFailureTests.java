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

import org.duckdb.FaultArray;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Struct;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NativeArrayCleanupFailureTests {
	@AfterEach
	void clearInterruptFlag() { Thread.interrupted(); }

	@Test
	void uncheckedReadFailureAndSqlCleanupCannotBecomeARetry() {
		for (boolean fatal : new boolean[]{false, true}) {
			for (boolean cached : new boolean[]{false, true}) {
				for (boolean stream : new boolean[]{false, true}) {
					Throwable primary = fatal ? new AssertionError("fatal nested ARRAY read")
							: new IllegalStateException("cancelled nested ARRAY read", new InterruptedException("cancelled"));
					SQLException cleanup = new SQLException("array free failed", "HY000");
					Fixture fixture = new Fixture(primary, cleanup, cached, null);

					Throwable thrown = fixture.assertFailed(stream, fatal ? Error.class : RuntimeException.class);

					if (fatal) assertSame(primary, thrown);
					assertTrue(contains(thrown, primary));
					assertArrayEquals(new Throwable[]{cleanup}, primary.getSuppressed());
					assertEquals(!fatal, Thread.interrupted());
					fixture.assertAborted();
				}
			}
		}
	}

	@Test
	void uncheckedCleanupCannotReplaceASqlReadFailure() {
		for (Throwable cleanup : List.of(new IllegalStateException("free failed"), new AssertionError("fatal free failure"))) {
			SQLException primary = new SQLException("cancelled nested read", "57014", new InterruptedException("cancelled"));
			Fixture fixture = new Fixture(primary, cleanup, true, null);

			Throwable thrown = fixture.assertFailed(false, DatabaseException.class);

			assertTrue(contains(thrown, primary));
			assertArrayEquals(new Throwable[]{cleanup}, primary.getSuppressed());
			assertTrue(Thread.interrupted());
			fixture.assertAborted();
		}
	}

	@Test
	void reusingThePrimaryDuringFreeNeverSelfSuppresses() {
		for (Throwable primary : List.of(new AssertionError("same fatal failure"),
				new IllegalStateException("same cancelled failure", new InterruptedException("cancelled")),
				new SQLException("same cancelled SQL failure", "57014", new InterruptedException("cancelled")))) {
			Fixture fixture = new Fixture(primary, primary, true, null);

			Throwable thrown = fixture.assertFailed(false, primary instanceof Error ? Error.class : DatabaseException.class);

			assertTrue(contains(thrown, primary));
			assertArrayEquals(new Throwable[0], primary.getSuppressed());
			assertEquals(!(primary instanceof Error), Thread.interrupted());
			fixture.assertAborted();
		}
	}

	@Test
	void readFailuresWithoutCleanupFailureRetainFatalAndCancellationBehavior() {
		for (Throwable primary : List.of(new AssertionError("fatal read"),
				new IllegalStateException("cancelled read", new InterruptedException("cancelled")),
				new SQLException("cancelled SQL read", "57014", new InterruptedException("cancelled")))) {
			Fixture fixture = new Fixture(primary, null, false, null);

			Throwable thrown = fixture.assertFailed(false, primary instanceof Error ? Error.class : DatabaseException.class);

			assertTrue(contains(thrown, primary));
			assertArrayEquals(new Throwable[0], primary.getSuppressed());
			assertEquals(!(primary instanceof Error), Thread.interrupted());
			fixture.assertAborted();
		}
	}

	@Test
	void cleanupOnlyFailuresRemainFailuresWithTheirOriginalIdentity() {
		for (Throwable cleanup : List.of(new AssertionError("fatal free"), new IllegalStateException("free failed"),
				new SQLException("free failed", "HY000"))) {
			Fixture fixture = new Fixture(null, cleanup, true, null);
			Class<? extends Throwable> expected = cleanup instanceof Error ? Error.class : DatabaseException.class;

			Throwable thrown = assertThrows(expected, () -> fixture.database.transaction(() -> {
				fixture.attempts.incrementAndGet(); fixture.work(false);
			}));

			if (cleanup instanceof Error) assertSame(cleanup, thrown);
			else assertTrue(contains(thrown, cleanup));
			fixture.assertAborted();
		}
	}

	@Test
	void cleanupOnlySerializationFailureStillRetriesAllApplicationWork() {
		SQLException cleanup = new SQLTransactionRollbackException("free failed", "40001");
		Fixture fixture = new Fixture(null, cleanup, true, null);

		TransactionRetryResult<Void> result = fixture.executeWithRetry(false);

		assertEquals(2, result.getAttemptCount());
		assertTrue(contains(result.getFailures().get(0), cleanup));
		assertEquals(2, fixture.attempts.get());
		assertEquals(1, fixture.conditions.get());
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(1, fixture.commits.get());
		assertEquals(1, fixture.rollbacks.get());
		fixture.assertReleased(2, 5);
	}

	@Test
	void successfulNativeArrayMappingRetainsContentsAndClosesOnce() {
		for (boolean stream : new boolean[]{false, true}) {
			Fixture fixture = new Fixture(null, null, true, null);

			TransactionRetryResult<Void> result = fixture.executeWithRetry(stream);

			assertEquals(1, result.getAttemptCount());
			assertEquals(List.of(1, 2), fixture.ids());
			Map<?, ?> nested = assertInstanceOf(Map.class, fixture.lastRow.get().values().iterator().next());
			assertArrayEquals(new Object[]{7}, assertInstanceOf(Object[].class, nested.get("nested")));
			assertEquals(0, fixture.conditions.get());
			assertEquals(1, fixture.commits.get());
			assertEquals(0, fixture.rollbacks.get());
			fixture.assertReleased(1, 3);
		}
	}

	@Test
	void realDuckDbStructCannotLoseFatalOrCancelledArrayRead(@TempDir Path directory) {
		Assumptions.assumeTrue(duckDbAvailable(), "DuckDB is supplied by the integration profile");
		for (boolean fatal : new boolean[]{false, true}) {
			for (boolean stream : new boolean[]{false, true}) {
				Throwable primary = fatal ? new AssertionError("fatal DuckDB nested read")
						: new IllegalStateException("cancelled DuckDB nested read", new InterruptedException("cancelled"));
				SQLException cleanup = new SQLException("DuckDB array free failed", "HY000");
				Fixture fixture = new Fixture(primary, cleanup, true, directory.resolve(UUID.randomUUID() + ".duckdb"));

				Throwable thrown = fixture.assertFailed(stream, fatal ? Error.class : RuntimeException.class);

				if (fatal) assertSame(primary, thrown);
				assertTrue(contains(thrown, primary));
				assertArrayEquals(new Throwable[]{cleanup}, primary.getSuppressed());
				assertEquals(!fatal, Thread.interrupted());
				fixture.assertAborted();
			}
		}
	}

	private static boolean duckDbAvailable() {
		try { Class.forName("org.duckdb.DuckDBDriver"); return true; }
		catch (ClassNotFoundException ignored) { return false; }
	}

	private static boolean contains(Throwable failure, Throwable original) {
		return ThrowableGraph.inspect(failure, node -> node == original ? 1 : 0, 0) != 0;
	}

	private static final class Fixture {
		private final Database database, inspection;
		private final AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
		private final AtomicInteger reads = new AtomicInteger(), frees = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger();
		private final AtomicInteger connectionsClosed = new AtomicInteger(), statementsClosed = new AtomicInteger(), rowsClosed = new AtomicInteger();
		private final AtomicBoolean interruptedInside = new AtomicBoolean();
		private final AtomicReference<Map<String, Object>> lastRow = new AtomicReference<>();
		private final String readSql;

		private Fixture(Throwable readFailure, Throwable cleanupFailure, boolean cached, Path duckDbFile) {
			DataSource source;
			if (duckDbFile == null) {
				JDBCDataSource hsql = new JDBCDataSource();
				hsql.setUrl("jdbc:hsqldb:mem:native_array_cleanup_" + UUID.randomUUID());
				hsql.setUser("sa"); hsql.setPassword(""); source = hsql;
			} else source = proxy(DataSource.class, (method, args) -> {
				if (method.getName().equals("getConnection")) return DriverManager.getConnection("jdbc:duckdb:" + duckDbFile);
				throw new UnsupportedOperationException(method.getName());
			});
			this.readSql = duckDbFile == null ? "VALUES (1)" : "SELECT {'nested': [7]} AS value";
			this.inspection = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(source, method, args);
				if (!(result instanceof Connection connection)) return result;
				return proxy(Connection.class, (jdbc, parameters) -> {
					String name = jdbc.getName();
					if (name.equals("commit")) this.commits.incrementAndGet();
					if (name.equals("rollback")) this.rollbacks.incrementAndGet();
					if (name.equals("close")) this.connectionsClosed.incrementAndGet();
					Object value = invoke(connection, jdbc, parameters);
					if (!(value instanceof PreparedStatement statement)) return value;
					boolean read = String.valueOf(parameters[0]).equals(this.readSql);
					return proxy(PreparedStatement.class, (operation, values) -> {
						if (operation.getName().equals("close")) this.statementsClosed.incrementAndGet();
						Object returned = invoke(statement, operation, values);
						if (!(returned instanceof ResultSet rows) || !read) return returned;
						return proxy(ResultSet.class, (getter, indexes) -> {
							if (getter.getName().equals("close")) this.rowsClosed.incrementAndGet();
							if (getter.getName().equals("getObject")) {
								FaultArray array = new FaultArray(this.attempts.get() == 1 ? readFailure : null,
										this.attempts.get() == 1 ? cleanupFailure : null, this.reads, this.frees);
								if (duckDbFile == null) return Map.of("nested", array);
								Struct actual = (Struct) invoke(rows, getter, indexes);
								Object[] attributes = actual.getAttributes();
								// The fixture replaces this driver-created handle, so it owns the displaced handle.
								if (attributes[0] instanceof Array displaced) displaced.free();
								attributes[0] = array;
								return actual;
							}
							return invoke(rows, getter, indexes);
						});
					});
				});
			});
			this.database = Database.withDataSource(wrapped).databaseType(DatabaseType.DUCK_DB)
					.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
		}

		private Throwable assertFailed(boolean stream, Class<? extends Throwable> expected) {
			return assertThrows(expected, () -> executeWithRetry(stream));
		}

		private TransactionRetryResult<Void> executeWithRetry(boolean stream) {
			RetryPolicy retry = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				this.conditions.incrementAndGet(); return true;
			});
			return this.database.transactionWithRetry(retry, () -> {
				this.attempts.incrementAndGet();
				try { work(stream); }
				catch (RuntimeException | Error failure) {
					this.interruptedInside.set(Thread.currentThread().isInterrupted());
					throw failure;
				}
			});
		}

		private void work(boolean stream) {
			this.database.query("INSERT INTO work VALUES (1)").execute();
			this.lastRow.set((stream ? this.database.query(this.readSql).fetchStream(Query.mapRowType(), rows -> rows.findFirst())
					: this.database.query(this.readSql).fetchObject(Query.mapRowType())).orElseThrow());
			this.database.query("INSERT INTO work VALUES (2)").execute();
		}

		private List<Integer> ids() { return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class); }
		private void assertAborted() {
			assertEquals(1, this.attempts.get());
			assertEquals(0, this.conditions.get());
			assertEquals(List.of(), ids());
			assertEquals(0, this.commits.get());
			assertEquals(1, this.rollbacks.get());
			assertFalse(this.interruptedInside.get(), "Cleanup does not publish interruption inside the transaction");
			assertReleased(1, 2);
		}
		private void assertReleased(int attempts, int statements) {
			assertEquals(attempts, this.reads.get());
			assertEquals(attempts, this.frees.get());
			assertEquals(attempts, this.connectionsClosed.get());
			assertEquals(attempts, this.rowsClosed.get());
			assertEquals(statements, this.statementsClosed.get());
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
