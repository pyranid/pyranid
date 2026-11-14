/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mariadb.jdbc.client.result.Result;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class MariaDbStreamingIT {
	@Container private static final MariaDBContainer MARIA_DB = new MariaDBContainer(DockerImageName.parse(
			System.getProperty("mariadb.integration.image", "mariadb:11.4")).asCompatibleSubstituteFor("mariadb"))
			.withDatabaseName("streaming_regression").withUsername("pyranid").withPassword("pyranid");
	@TempDir Path directory;

	@Test
	void defaultsReadIncrementallyWhileConfiguredSizesAndExplicitZeroRemainEffective() throws Exception {
		for (boolean transaction : new boolean[]{false, true}) {
			for (String setting : List.of("default", "databasePositive", "queryPositive", "customizerPositive", "databaseZero", "queryZero")) {
				Fixture fixture = new Fixture();
				Database.Builder builder = Database.with(fixture.source, DatabaseType.MARIA_DB);
				if (setting.equals("databasePositive")) builder.fetchSize(41);
				if (setting.equals("databaseZero")) builder.fetchSize(0);
				Database database = builder.build();
				Query query = database.query("SELECT seq FROM seq_1_to_4096");
				if (setting.equals("queryPositive")) query.fetchSize(31);
				if (setting.equals("queryZero")) query.fetchSize(0);
				if (setting.equals("customizerPositive")) query.customize((context, statement) -> statement.setFetchSize(23));
				int expectedFetchSize = setting.equals("databasePositive") ? 41 : setting.equals("queryPositive") ? 31
						: setting.equals("customizerPositive") ? 23 : setting.equals("queryZero") ? 0 : 256;
				ReturningTransactionalOperation<Long> operation = () -> {
					long sum = query.fetchStream(Integer.class, rows -> {
						Observed observed = fixture.observed.get();
						try {
							assertEquals(ResultSet.TYPE_FORWARD_ONLY, observed.result.getType());
							assertEquals(expectedFetchSize, observed.statement.getFetchSize());
							assertEquals(expectedFetchSize > 0, observed.result.streaming(), setting);
							assertEquals(expectedFetchSize == 0, observed.result.loaded(), setting);
							if (expectedFetchSize > 0) assertBoundedRows(observed.result, expectedFetchSize);
						} catch (ReflectiveOperationException | java.sql.SQLException failure) { throw new AssertionError(failure); }
						AtomicInteger row = new AtomicInteger();
						return rows.mapToLong(value -> {
							int index = row.incrementAndGet();
							assertEquals(index, value);
							if (expectedFetchSize > 0 && index == 2_049) {
								assertFalse(observed.result.loaded(), "More than the first fetch must still leave unread server rows");
								try { assertBoundedRows(observed.result, expectedFetchSize); }
								catch (ReflectiveOperationException failure) { throw new AssertionError(failure); }
							}
							return value;
						}).sum();
					});
					Observed observed = fixture.observed.get();
					assertTrue(observed.result.isClosed());
					assertTrue(observed.statement.isClosed());
					assertEquals(!transaction, observed.connection.isClosed());
					if (transaction) {
						assertEquals(0, fixture.commits.get());
						assertEquals(0, fixture.rollbacks.get());
						assertEquals(42, database.query("SELECT 42").fetchObject(Integer.class).orElseThrow());
					}
					return Optional.of(sum);
				};
				long sum = transaction ? database.transaction(operation).orElseThrow() : operation.perform().orElseThrow();
				assertEquals(4096L * 4097 / 2, sum);
				assertEquals(transaction ? 1 : 0, fixture.commits.get());
				assertEquals(0, fixture.rollbacks.get());
				assertTrue(fixture.observed.get().connection.isClosed());
			}
		}
	}

	@Test
	void completeEarlyAndFailedCallbacksReleaseResultsWithoutTakingOverTheTransaction() throws Exception {
		for (boolean transaction : new boolean[]{false, true}) {
			for (String completion : List.of("complete", "early", "failure")) {
				Fixture fixture = new Fixture();
				Database database = Database.with(fixture.source, DatabaseType.MARIA_DB).build();
				Database inspection = Database.with(delegate(), DatabaseType.MARIA_DB).build();
				String table = "stream_work_" + UUID.randomUUID().toString().replace("-", "");
				inspection.query("CREATE TABLE " + table + " (id INT PRIMARY KEY)").execute();
				IllegalStateException callbackFailure = new IllegalStateException("application callback failed");
				AtomicReference<TransactionCompletion> completed = new AtomicReference<>();
				try {
					ReturningTransactionalOperation<Long> operation = () -> {
						if (transaction) database.query("INSERT INTO " + table + " VALUES (1)").execute();
						long count = database.query("SELECT seq FROM seq_1_to_4096").fetchStream(Integer.class, rows -> {
							if (completion.equals("failure")) {
								assertEquals(1, rows.findFirst().orElseThrow());
								throw callbackFailure;
							}
							return completion.equals("early") ? rows.limit(7).count() : rows.count();
						});
						assertEquals(completion.equals("early") ? 7 : 4096, count);
						assertTrue(fixture.observed.get().result.isClosed());
						assertTrue(fixture.observed.get().statement.isClosed());
						assertEquals(!transaction, fixture.observed.get().connection.isClosed());
						if (transaction) {
							assertEquals(0, fixture.commits.get());
							assertEquals(0, fixture.rollbacks.get());
							assertEquals(42, database.query("SELECT 42").fetchObject(Integer.class).orElseThrow(),
									"Early close must drain the protocol so the owner connection remains usable");
						}
						return Optional.of(count);
					};
					if (completion.equals("failure")) {
						assertSame(callbackFailure, assertThrows(IllegalStateException.class, () -> {
							if (transaction) database.transaction(TransactionOptions.withReadOnly(null).completionObserver(completed::set).build(), operation);
							else operation.perform();
						}));
					} else if (transaction) database.transaction(TransactionOptions.withReadOnly(null).completionObserver(completed::set).build(), operation);
					else operation.perform();
					assertTrue(fixture.observed.get().result.isClosed());
					assertTrue(fixture.observed.get().statement.isClosed());
					assertTrue(fixture.observed.get().connection.isClosed());
					assertEquals(transaction && !completion.equals("failure") ? 1 : 0, fixture.commits.get());
					assertEquals(transaction && completion.equals("failure") ? 1 : 0, fixture.rollbacks.get());
					assertEquals(transaction && !completion.equals("failure") ? 1 : 0,
							inspection.query("SELECT COUNT(*) FROM " + table).fetchObject(Integer.class).orElseThrow());
					if (transaction) assertEquals(completion.equals("failure") ? TransactionResult.ROLLED_BACK : TransactionResult.COMMITTED,
							completed.get().getResult());
				} finally { inspection.query("DROP TABLE " + table).execute(); }
			}
		}
	}

	@Test
	void largeResultsFitInABoundedChildHeapInsideAndOutsideTransactions() throws Exception {
		Path output = directory.resolve("mariadb-stream-heap.log");
		Path java = Path.of(System.getProperty("java.home"), "bin", System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java");
		Process process = new ProcessBuilder(java.toString(), "-Xmx64m", "-XX:+UseSerialGC", "-cp", System.getProperty("java.class.path"),
				MariaDbStreamingHeapProbe.class.getName(), MARIA_DB.getJdbcUrl(), MARIA_DB.getUsername(), MARIA_DB.getPassword())
				.redirectErrorStream(true).redirectOutput(output.toFile()).start();
		boolean finished;
		try { finished = process.waitFor(60, TimeUnit.SECONDS); }
		finally { if (process.isAlive()) process.destroyForcibly(); }
		String diagnostics = Files.readString(output);
		assertTrue(finished, () -> "MariaDB bounded-heap probe exceeded its time budget:\n" + diagnostics);
		assertEquals(0, process.exitValue(), () -> "MariaDB bounded-heap probe failed:\n" + diagnostics);
		assertTrue(diagnostics.contains("PASS: MariaDB streams 524288 rows"), diagnostics);
	}

	private static void assertBoundedRows(Result result, int expectedFetchSize) throws ReflectiveOperationException {
		Field count = Result.class.getDeclaredField("dataSize"), rows = Result.class.getDeclaredField("data");
		count.setAccessible(true); rows.setAccessible(true);
		assertTrue(count.getInt(result) <= expectedFetchSize);
		assertTrue(((byte[][]) rows.get(result)).length <= Math.max(10, expectedFetchSize));
	}

	private static DataSource delegate() { return new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
			MARIA_DB.getJdbcUrl(), MARIA_DB.getUsername(), MARIA_DB.getPassword()); }

	private record Observed(Connection connection, PreparedStatement statement, Result result) {}
	private static final class Fixture {
		final AtomicReference<Observed> observed = new AtomicReference<>();
		final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger();
		final DataSource source = proxy(DataSource.class, (method, args) -> {
			Object value = invoke(delegate(), method, args);
			if (!(value instanceof Connection connection)) return value;
			return proxy(Connection.class, (jdbc, parameters) -> {
				if (jdbc.getName().equals("commit")) commits.incrementAndGet();
				if (jdbc.getName().equals("rollback")) rollbacks.incrementAndGet();
				Object result = invoke(connection, jdbc, parameters);
				if (!(result instanceof PreparedStatement statement)) return result;
				boolean streamQuery = parameters != null && parameters.length > 0 && "SELECT seq FROM seq_1_to_4096".equals(parameters[0]);
				return proxy(PreparedStatement.class, (call, values) -> {
					Object rows = invoke(statement, call, values);
					if (call.getName().equals("executeQuery") && rows instanceof Result driverResult
							&& streamQuery)
						observed.set(new Observed(connection, statement, driverResult));
					return rows;
				});
			});
		});
	}
	@FunctionalInterface private interface Call { Object invoke(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> call.invoke(method, args)));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
