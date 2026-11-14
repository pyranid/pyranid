/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlStreamingIT {
	private static final String SQL = "SELECT generate_series(1, 1025)";
	@Container private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_streaming").withUsername("pyranid").withPassword("pyranid");

	@Test
	void existingTransactionUsesABoundedPortalAndRetainsTransactionOwnershipAcrossAllClosePaths() {
		for (String finish : List.of("limit", "exhaust", "failure")) {
			Fixture fixture = new Fixture(null);
			IllegalStateException original = new IllegalStateException("application stream callback failed");
			fixture.database.transaction(() -> {
				if (finish.equals("failure")) {
					assertSame(original, assertThrows(IllegalStateException.class, () -> fixture.database.query(SQL)
							.fetchStream(Integer.class, stream -> {
								fixture.assertInitialBuffer(256);
								throw original;
							})));
				} else {
					fixture.database.query(SQL).fetchStream(Integer.class, stream -> {
						fixture.assertInitialBuffer(256);
						consume(stream, finish);
						return null;
					});
				}
				fixture.assertCursorClosed();
				assertEquals(0, fixture.commits.get(), "Closing the stream cannot commit its enclosing transaction");
				assertEquals(0, fixture.rollbacks.get(), "An application callback failure cannot independently roll back the owner");
				assertEquals(0, fixture.closes.get(), "The enclosing transaction still owns the physical connection");
				assertEquals(1, fixture.database.query("SELECT 1").fetchObject(Integer.class).orElseThrow());
				assertEquals(1, fixture.opens.get(), "A subsequent statement must use the same connection");
			});
			assertEquals(1, fixture.commits.get());
			assertEquals(0, fixture.rollbacks.get());
			fixture.assertConnectionReleased();
		}
	}

	@Test
	void standaloneStreamsCommitOrRollBackTheirOwnTransactionAndRestoreAutoCommit() {
		for (String finish : List.of("limit", "exhaust", "failure")) {
			Fixture fixture = new Fixture(null);
			IllegalStateException original = new IllegalStateException("application stream callback failed");
			if (finish.equals("failure")) {
				assertSame(original, assertThrows(IllegalStateException.class, () -> fixture.database.query(SQL)
						.fetchStream(Integer.class, stream -> { fixture.assertInitialBuffer(256); throw original; })));
			} else {
				fixture.database.query(SQL).fetchStream(Integer.class, stream -> {
					fixture.assertInitialBuffer(256);
					consume(stream, finish);
					return null;
				});
			}
			fixture.assertCursorClosed();
			assertEquals(finish.equals("failure") ? 0 : 1, fixture.commits.get());
			assertEquals(finish.equals("failure") ? 1 : 0, fixture.rollbacks.get());
			fixture.assertConnectionReleased();
		}
	}

	@Test
	void explicitDatabaseQueryAndCustomizerSizesRetainPrecedenceIncludingDeliberateBuffering() throws Exception {
		for (boolean transaction : new boolean[]{false, true}) {
			for (String setting : List.of("database-positive", "database-zero", "query-positive", "query-zero",
					"query-null", "custom-positive", "custom-zero", "custom-zero-without-setting", "custom-zero-with-query-setting")) {
				Integer databaseSize = 17;
				if (setting.equals("database-zero")) databaseSize = 0;
				if (setting.equals("custom-zero-without-setting")) databaseSize = null;
				Fixture fixture = new Fixture(databaseSize);
				Query query = fixture.database.query(SQL);
				int expected = switch (setting) {
					case "database-positive", "query-null" -> 17;
					case "query-zero", "custom-zero-with-query-setting" -> 0;
					case "query-positive" -> 41;
					case "custom-positive" -> 13;
					case "database-zero", "custom-zero", "custom-zero-without-setting" -> 256;
					default -> throw new AssertionError(setting);
				};
				if (setting.equals("query-positive")) query.fetchSize(41);
				if (setting.equals("query-zero")) query.fetchSize(0);
				if (setting.equals("query-null")) query.fetchSize(null);
				if (setting.equals("custom-positive")) query.fetchSize(41).customize((context, statement) -> statement.setFetchSize(13));
				if (setting.equals("custom-zero-with-query-setting")) query.fetchSize(41);
				if (setting.startsWith("custom-zero")) query.customize((context, statement) -> statement.setFetchSize(0));
				TransactionalOperation operation = () -> query.fetchStream(Integer.class, stream -> {
					fixture.assertInitialBuffer(expected);
					assertEquals(List.of(1, 2, 3), stream.limit(3).toList());
					return null;
				});
				if (transaction) fixture.database.transaction(operation); else operation.perform();
				fixture.assertCursorClosed();
				fixture.assertConnectionReleased();
			}
		}
	}


	@Test
	void handledFailureOnALaterPortalFetchStillRefusesTheEnclosingCommit() {
		Fixture fixture = new Fixture(null);
		String table = "stream_failure_" + UUID.randomUUID().toString().replace("-", "");
		fixture.database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY)").execute();
		try {
			AtomicBoolean enteredCallback = new AtomicBoolean();
			AtomicInteger delivered = new AtomicInteger();
			AtomicReference<DatabaseException> handled = new AtomicReference<>();
			TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
				fixture.database.query("INSERT INTO " + table + " VALUES (1)").execute();
				fixture.database.query("SELECT CASE WHEN n=300 THEN 1/(n-n) ELSE n END FROM generate_series(1,600) AS g(n)")
						.fetchStream(Integer.class, stream -> {
							enteredCallback.set(true);
							fixture.assertInitialBuffer(256);
							try { stream.forEach(value -> delivered.incrementAndGet()); }
							catch (DatabaseException failure) { handled.set(failure); }
							return null;
						});
				assertNotNull(handled.get());
				fixture.assertCursorClosed();
				assertEquals(0, fixture.commits.get());
				assertEquals(0, fixture.rollbacks.get(), "Cursor closure leaves rollback to the enclosing transaction");
			});
			assertTrue(enteredCallback.get(), "The first page must be delivered before the server reaches the failing row");
			assertTrue(delivered.get() >= 256 && delivered.get() < 300);
			assertEquals("22012", handled.get().getSqlState().orElseThrow());
			assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
			DatabaseException refusal = assertInstanceOf(DatabaseException.class, outcome.getFailure().orElseThrow());
			assertSame(handled.get(), refusal.getCause());
			assertEquals(0, fixture.commits.get());
			assertEquals(1, fixture.rollbacks.get());
			assertEquals(0L, fixture.database.query("SELECT COUNT(*) FROM " + table).fetchObject(Long.class).orElseThrow());
		} finally { fixture.database.query("DROP TABLE " + table).execute(); }
	}

	private static void consume(Stream<Integer> stream, String finish) {
		if (finish.equals("limit")) assertEquals(List.of(1, 2, 3), stream.limit(3).toList());
		else assertEquals(1025L * 1026 / 2, stream.mapToLong(Integer::longValue).sum());
	}

	private static final class Fixture {
		final AtomicInteger opens = new AtomicInteger(), closes = new AtomicInteger(), commits = new AtomicInteger(), rollbacks = new AtomicInteger();
		final Database database;
		PreparedStatement streamStatement;
		ResultSet streamRows;
		Boolean autoCommitBeforeClose;

		Fixture(Integer databaseSize) {
			PGSimpleDataSource source = new PGSimpleDataSource();
			source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
			DataSource counted = proxy(DataSource.class, (method, args) -> {
				Object value = invoke(source, method, args);
				if (!(value instanceof Connection connection)) return value;
				opens.incrementAndGet();
				return proxy(Connection.class, (jdbc, parameters) -> {
					if (jdbc.getName().equals("commit")) commits.incrementAndGet();
					if (jdbc.getName().equals("rollback") && (parameters == null || parameters.length == 0)) rollbacks.incrementAndGet();
					if (jdbc.getName().equals("close")) { autoCommitBeforeClose = connection.getAutoCommit(); closes.incrementAndGet(); }
					Object result = invoke(connection, jdbc, parameters);
					if (!(result instanceof PreparedStatement statement) || !String.valueOf(parameters[0]).contains("generate_series")) return result;
					streamStatement = statement;
					return proxy(PreparedStatement.class, (call, values) -> {
						Object returned = invoke(statement, call, values);
						if (call.getName().equals("executeQuery")) streamRows = (ResultSet) returned;
						return returned;
					});
				});
			});
			this.database = Database.with(counted, DatabaseType.POSTGRESQL).fetchSize(databaseSize).build();
		}

		void assertInitialBuffer(int fetchSize) {
			try {
				assertEquals(fetchSize, streamStatement.getFetchSize());
				// Driver-internal state is test-only evidence: a configured fetch size alone does not prove
				// the server portal is incremental. The zero control must buffer the entire result instead.
				List<?> rows = (List<?>) field(streamRows, "rows");
				Object cursor = field(streamRows, "cursor");
				assertEquals(fetchSize == 0 ? 1025 : fetchSize, rows.size());
				if (fetchSize == 0) assertNull(cursor); else assertNotNull(cursor, "A live server portal must retain the remaining rows");
			} catch (ReflectiveOperationException | SQLException failure) { throw new AssertionError(failure); }
		}
		void assertCursorClosed() {
			try { assertTrue(streamRows.isClosed()); assertTrue(streamStatement.isClosed()); }
			catch (SQLException failure) { throw new AssertionError(failure); }
		}
		void assertConnectionReleased() {
			assertEquals(1, opens.get()); assertEquals(1, closes.get());
			assertEquals(Boolean.TRUE, autoCommitBeforeClose, "Pool return must restore the original auto-commit setting");
		}
	}

	private static Object field(Object target, String name) throws ReflectiveOperationException {
		for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
			try { Field field = type.getDeclaredField(name); field.setAccessible(true); return field.get(target); }
			catch (NoSuchFieldException missing) { /* Try the driver's base class. */ }
		}
		throw new NoSuchFieldException(name);
	}
	@FunctionalInterface private interface Call { Object run(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> call.run(method, args)));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
