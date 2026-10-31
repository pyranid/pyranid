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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Struct;
import java.sql.Time;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ReadFallbackFailureTests {
	private enum Site { TEMPORAL, ARRAY_TYPE, ARRAY_NAME, TIME_READER, TIME_ELEMENT, STRUCT_TYPE }
	private static final List<Site> ARRAY_SITES = List.of(Site.ARRAY_TYPE, Site.ARRAY_NAME, Site.TIME_READER, Site.TIME_ELEMENT);
	private static final List<String> EDGES = List.of("cause", "next", "suppressed", "cycle", "unreadable");

	@AfterEach
	void clearInterruptFlag() {
		Thread.interrupted();
	}

	@Test
	void temporalReadRollbackCannotCommitOnlyLaterWork() {
		for (boolean cached : new boolean[]{false, true}) {
			for (boolean stream : new boolean[]{false, true}) {
				SQLException original = new SQLTransactionRollbackException("typed getter rolled back", "40001");
				Fixture fixture = new Fixture(Site.TEMPORAL, original, true, cached);

				DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.execute(stream));

				assertTrue(contains(thrown, original));
				fixture.assertRolledBack(1);
				assertFalse(Thread.currentThread().isInterrupted());
			}
		}
	}

	@Test
	void arrayMetadataRollbackCannotCommitOnlyLaterWork() {
		for (Site site : List.of(Site.ARRAY_TYPE, Site.ARRAY_NAME)) {
			for (String edge : EDGES) {
				SQLException original = unsupportedWith(new SQLTransactionRollbackException("array metadata rolled back", "40001"), edge);
				Fixture fixture = new Fixture(site, original, true, true);

				DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.execute(false), site + " " + edge);

				assertTrue(contains(thrown, original));
				fixture.assertRolledBack(1);
			}
		}
	}

	@Test
	void terminalTimeArrayReaderFailureCannotBeDeferredForNativeTargets() {
		for (Site site : List.of(Site.TIME_READER, Site.TIME_ELEMENT)) {
			SQLException original = unsupportedWith(new SQLTransactionRollbackException("array read rolled back", "40001"), "next");
			Fixture fixture = new Fixture(site, original, true, true);

			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.execute(false), site.name());

			assertTrue(contains(thrown, original));
			fixture.assertRolledBack(1);
		}
	}

	@Test
	void readCancellationCannotAdmitARetry() {
		for (Site site : Site.values()) {
			for (String edge : EDGES) {
				SQLException original = unsupportedWith(new InterruptedException("cancelled optional read"), edge);
				Fixture fixture = new Fixture(site, original, false, true);
				AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
				RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
					conditions.incrementAndGet();
					return true;
				});

				DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(policy, () -> {
					attempts.incrementAndGet();
					try {
						fixture.work(false);
					} catch (DatabaseException failure) {
						assertFalse(Thread.currentThread().isInterrupted(), "The read fallback does not publish interruption inside the closure");
						throw failure;
					}
				}), site + " " + edge);

				assertTrue(Thread.interrupted(), site + " " + edge);
				assertTrue(contains(thrown, original));
				assertEquals(1, attempts.get());
				assertEquals(0, conditions.get());
				fixture.assertRolledBack(1);
			}
		}
	}

	@Test
	void serializationFailureRetriesTheWholeReadTransaction() {
		for (Site site : Site.values()) {
			SQLException original = unsupportedWith(new SQLTransactionRollbackException("optional read rolled back", "40001"), "next");
			Fixture fixture = new Fixture(site, original, true, true);
			AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				conditions.incrementAndGet();
				return failure.isSerializationFailure();
			});

			TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(policy, () -> {
				attempts.incrementAndGet();
				fixture.work(false);
				return Optional.empty();
			});

			assertEquals(2, attempts.get(), site.name());
			assertEquals(1, conditions.get(), site.name());
			assertEquals(2, result.getAttemptCount());
			assertTrue(contains(result.getFailures().get(0), original));
			assertEquals(List.of(1, 2), fixture.ids());
			assertEquals(1, fixture.commits.get());
			assertEquals(1, fixture.rollbacks.get());
			fixture.assertReleased(2);
		}
	}

	@Test
	void legacyCapabilityFailureCannotHideFatalEvidence() {
		for (Site site : Site.values()) {
			AssertionError fatal = new AssertionError("fatal driver failure");
			AbstractMethodError original = new AbstractMethodError("optional read unavailable");
			original.addSuppressed(fatal);
			Fixture fixture = new Fixture(site, original, false, true);
			AtomicInteger conditions = new AtomicInteger();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				conditions.incrementAndGet();
				return true;
			});

			assertSame(original, assertThrows(AbstractMethodError.class,
					() -> fixture.database.transactionWithRetry(policy, () -> fixture.work(false)), site.name()));

			assertEquals(0, conditions.get());
			fixture.assertRolledBack(1);
		}
	}

	@Test
	void unsupportedReadFallbacksStillCompleteAndReleaseResources() {
		for (Site site : Site.values()) {
			for (boolean legacy : new boolean[]{false, true}) {
				Throwable unsupported = legacy ? new AbstractMethodError("optional read unavailable")
						: new SQLFeatureNotSupportedException("optional read unavailable", "0A000");
				Fixture fixture = new Fixture(site, unsupported, false, false);

				fixture.execute(false);

				assertEquals(List.of(1, 2), fixture.ids(), site + " " + legacy);
				assertEquals(1, fixture.commits.get());
				assertEquals(0, fixture.rollbacks.get());
				assertFalse(Thread.currentThread().isInterrupted());
				fixture.assertReleased(1);
			}
		}
	}

	@Test
	void runtimeCapabilityFallbackRetainsTerminalEvidenceAndBenignCompatibility() {
		for (Site site : List.of(Site.ARRAY_TYPE, Site.ARRAY_NAME, Site.TIME_READER, Site.TIME_ELEMENT, Site.STRUCT_TYPE)) {
			for (boolean cancelled : new boolean[]{false, true}) {
				UnsupportedOperationException unsupported = new UnsupportedOperationException("optional read unavailable",
						cancelled ? new InterruptedException("cancelled read") : null);
				Fixture fixture = new Fixture(site, unsupported, false, true);
				if (cancelled) {
					DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.execute(false), site.name());
					assertTrue(contains(thrown, unsupported));
					assertTrue(Thread.interrupted());
					fixture.assertRolledBack(1);
				} else {
					fixture.execute(false);
					assertEquals(List.of(1, 2), fixture.ids());
					fixture.assertReleased(1);
				}
			}
		}
	}

	@Test
	void duckDbStructMetadataRollbackCannotCommitOnlyLaterWork() {
		SQLException original = unsupportedWith(new SQLTransactionRollbackException("STRUCT metadata rolled back", "40001"), "next");
		Fixture fixture = new Fixture(Site.STRUCT_TYPE, original, true, true);

		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.execute(false));

		assertTrue(contains(thrown, original));
		fixture.assertRolledBack(1);
	}

	private static SQLException unsupportedWith(Throwable terminal, String edge) {
		SQLException original = edge.equals("unreadable")
				? new SQLFeatureNotSupportedException("optional read unavailable", "0A000") {
				@Override public String getSQLState() { throw new IllegalStateException("unreadable state"); }
			} : new SQLFeatureNotSupportedException("optional read unavailable", "0A000");
		if (edge.equals("cause")) original.initCause(terminal);
		else if (edge.equals("next") || edge.equals("cycle")) {
			SQLException next = terminal instanceof SQLException sql ? sql : new SQLException("cancelled detail", "HY000", terminal);
			original.setNextException(next);
			if (edge.equals("cycle")) next.setNextException(original);
		} else original.addSuppressed(terminal);
		return original;
	}

	private static boolean contains(Throwable failure, Throwable original) {
		return ThrowableGraph.inspect(failure, current -> current == original ? 1 : 0, 0) != 0;
	}

	private static final class Fixture {
		private final Site site;
		private final Throwable failure;
		private final boolean physicalRollback;
		private final AtomicBoolean faulted = new AtomicBoolean();
		private final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger(), connectionsClosed = new AtomicInteger();
		private final AtomicInteger reads = new AtomicInteger(), statementsClosed = new AtomicInteger(), rowsClosed = new AtomicInteger();
		private final AtomicInteger arraysFreed = new AtomicInteger(), elementRowsOpened = new AtomicInteger(), elementRowsClosed = new AtomicInteger();
		private final Database database;
		private final Database inspection;
		private Connection nativeConnection;

		private Fixture(Site site, Throwable failure, boolean physicalRollback, boolean cached) {
			this.site = site;
			this.failure = failure;
			this.physicalRollback = physicalRollback;
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:read_fallback_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			this.inspection = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
			DataSource wrapped = proxy(DataSource.class, (method, arguments) -> {
				Object value = invoke(source, method, arguments);
				if (!(value instanceof Connection connection)) return value;
				this.nativeConnection = connection;
				return proxy(Connection.class, (operation, parameters) -> {
					if (operation.getName().equals("commit")) this.commits.incrementAndGet();
					if (operation.getName().equals("rollback")) this.rollbacks.incrementAndGet();
					if (operation.getName().equals("close")) this.connectionsClosed.incrementAndGet();
					Object result = invoke(connection, operation, parameters);
					if (!(result instanceof PreparedStatement statement) || !String.valueOf(parameters[0]).startsWith("VALUES (")) return result;
					this.reads.incrementAndGet();
					return proxy(PreparedStatement.class, (statementOperation, statementParameters) -> {
						if (statementOperation.getName().equals("close")) this.statementsClosed.incrementAndGet();
						Object statementValue = invoke(statement, statementOperation, statementParameters);
						if (!(statementValue instanceof ResultSet rows)) return statementValue;
						return proxy(ResultSet.class, (rowOperation, rowParameters) -> {
							String name = rowOperation.getName();
							if (name.equals("close")) this.rowsClosed.incrementAndGet();
							if (this.site == Site.TEMPORAL && name.equals("getObject") && rowParameters.length == 2
									&& rowParameters[1] instanceof Class<?>) failOnce(connection);
							Object rowValue = invoke(rows, rowOperation, rowParameters);
							if (!(rowValue instanceof Array array)) return rowValue;
							return wrapArray(array, connection);
						});
					});
				});
			});
			this.database = Database.withDataSource(wrapped).databaseType(DatabaseType.GENERIC)
					.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
		}

		private Array wrapArray(Array array, Connection connection) {
			return proxy(Array.class, (operation, parameters) -> {
				String name = operation.getName();
				if (name.equals("free")) this.arraysFreed.incrementAndGet();
				if (this.site == Site.ARRAY_TYPE && name.equals("getBaseType")
						|| this.site == Site.ARRAY_NAME && name.equals("getBaseTypeName")
						|| this.site == Site.TIME_READER && name.equals("getResultSet")) failOnce(connection);
				if ((this.site == Site.TIME_READER || this.site == Site.TIME_ELEMENT) && name.equals("getArray"))
					return new Time[]{Time.valueOf("12:34:56")};
				Object value = invoke(array, operation, parameters);
				if (!(value instanceof ResultSet rows)) return value;
				this.elementRowsOpened.incrementAndGet();
				return proxy(ResultSet.class, (rowOperation, rowParameters) -> {
					if (rowOperation.getName().equals("close")) this.elementRowsClosed.incrementAndGet();
					if (this.site == Site.TIME_ELEMENT && rowOperation.getName().equals("getObject")
							&& rowParameters.length == 2 && rowParameters[1] instanceof Class<?>) failOnce(connection);
					return invoke(rows, rowOperation, rowParameters);
				});
			});
		}

		private void failOnce(Connection connection) throws SQLException {
			if (!this.faulted.compareAndSet(false, true)) return;
			if (this.physicalRollback) connection.rollback();
			if (this.failure instanceof SQLException sql) throw sql;
			if (this.failure instanceof RuntimeException runtime) throw runtime;
			throw (Error) this.failure;
		}

		private void work(boolean stream) {
			this.database.query("INSERT INTO work VALUES (1)").execute();
			read(stream);
			this.database.query("INSERT INTO work VALUES (2)").execute();
		}

		private void execute(boolean stream) {
			this.database.transaction(() -> work(stream));
		}

		private void read(boolean stream) {
			if (this.site == Site.STRUCT_TYPE) {
				this.database.useRawConnection(connection -> {
					try (MappingResources resources = new MappingResources(null, null)) {
						Map<?, ?> attributes = structAttributes(new TestStruct(() -> failOnce(this.nativeConnection)), resources);
						assertEquals(Map.of("value", 7), attributes);
					}
					return Optional.empty();
				});
				return;
			}
			String sql = this.site == Site.TEMPORAL ? "VALUES (TIMESTAMP '2024-01-02 12:34:56')"
					: this.site == Site.ARRAY_TYPE || this.site == Site.ARRAY_NAME ? "VALUES (ARRAY[TIMESTAMP '2024-01-02 12:34:56'])"
					: "VALUES (ARRAY[TIME '12:34:56'])";
			Class<?> target = this.site == Site.TEMPORAL ? LocalDateTime.class : Object[].class;
			Optional<?> value = stream ? this.database.query(sql).fetchStream(target, rows -> rows.findFirst())
					: this.database.query(sql).fetchObject(target);
			assertTrue(value.isPresent());
		}

		private List<Integer> ids() {
			return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class);
		}

		private void assertRolledBack(int attempts) {
			assertEquals(List.of(), ids());
			assertEquals(0, this.commits.get());
			assertEquals(attempts, this.rollbacks.get());
			assertTrue(this.database.currentTransaction().isEmpty());
			assertReleased(attempts);
		}

		private void assertReleased(int attempts) {
			assertEquals(attempts, this.connectionsClosed.get());
			assertEquals(this.reads.get(), this.statementsClosed.get());
			assertEquals(this.reads.get(), this.rowsClosed.get());
			assertEquals(ARRAY_SITES.contains(this.site) ? this.reads.get() : 0, this.arraysFreed.get());
			assertEquals(this.elementRowsOpened.get(), this.elementRowsClosed.get());
		}
	}

	/** Exercises the vendor metadata branch without adding a DuckDB dependency to the unit-test runtime. */
	private static Map<?, ?> structAttributes(TestStruct value, MappingResources resources) {
		try {
			Method method = DuckDbDialect.class.getDeclaredMethod("structAttributesByName", Object.class, boolean.class, MappingResources.class);
			method.setAccessible(true);
			return (Map<?, ?>) method.invoke(DuckDbDialect.INSTANCE, value, false, resources);
		} catch (InvocationTargetException failure) {
			if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
			if (failure.getCause() instanceof Error error) throw error;
			throw new AssertionError(failure.getCause());
		} catch (ReflectiveOperationException failure) {
			throw new AssertionError(failure);
		}
	}

	public static final class TestStruct implements Struct {
		private final SqlFault fault;
		private TestStruct(SqlFault fault) { this.fault = fault; }
		public Map<String, Object> getMap() { return Map.of("value", 7); }
		@Override public String getSQLTypeName() throws SQLException { this.fault.perform(); return "STRUCT(value INTEGER)"; }
		@Override public Object[] getAttributes() { return new Object[]{7}; }
		@Override public Object[] getAttributes(Map<String, Class<?>> types) { return getAttributes(); }
	}

	@FunctionalInterface
	private interface SqlFault { void perform() throws SQLException; }
	@FunctionalInterface
	private interface Invocation { Object perform(Method method, Object[] arguments) throws Throwable; }
	private static <T> T proxy(Class<T> type, Invocation invocation) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(object, method, arguments) -> invocation.perform(method, arguments)));
	}
	private static Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
		try { return method.invoke(target, arguments); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
