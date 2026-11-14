/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.jdbc.JdbcConnection;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class MySqlStreamingIT {
	@Container private static final MySQLContainer MYSQL = new MySQLContainer(DockerImageName.parse(
			System.getProperty("mysql.integration.image", "mysql:8.4")).asCompatibleSubstituteFor("mysql"))
			.withDatabaseName("pyranid_streaming").withUsername("pyranid").withPassword("pyranid");

	@Test
	void positiveHintsKeepActualStreamingOrCursorRowsAcrossDriverAndTransactionModes() throws Exception {
		for (boolean cursor : new boolean[]{false, true}) {
			for (boolean serverPrepared : new boolean[]{false, true}) {
				try (Connection physical = connect(cursor, serverPrepared)) {
					seed(physical);
					for (boolean transaction : new boolean[]{false, true}) {
						for (String setting : List.of("unset", "query-positive", "database-positive", "query-zero", "database-zero", "customizer-positive")) {
							Lease lease = new Lease(physical, false);
							Database.Builder builder = Database.with(lease.source(), DatabaseType.MYSQL);
							if (setting.equals("database-positive")) builder.fetchSize(17);
							if (setting.equals("database-zero")) builder.fetchSize(0);
							Database database = builder.build();
							ReturningTransactionalOperation<Void> operation = () -> {
								Query query = database.query("SELECT n FROM stream_work ORDER BY n");
								if (setting.equals("query-positive")) query.fetchSize(17);
								if (setting.equals("query-zero")) query.fetchSize(0);
								if (setting.equals("customizer-positive")) query.customize((context, statement) -> statement.setFetchSize(17));
								assertEquals(List.of(1, 2, 3), query.fetchStream(Integer.class, stream -> stream.limit(3).toList()));
								boolean zero = setting.equals("query-zero");
								boolean positive = setting.endsWith("positive");
								String expected = zero ? "ResultsetRowsStatic" : cursor && positive ? "ResultsetRowsCursor" : "ResultsetRowsStreaming";
								assertEquals(expected, lease.rowsClass, cursor + " " + serverPrepared + " " + transaction + " " + setting);
								assertEquals(zero ? 0 : cursor && positive ? 17 : Integer.MIN_VALUE, lease.fetchAtExecute);
								assertTrue(lease.resultClosed);
								assertEquals(1, lease.statementCloses.get());
								// The same physical connection must be usable once an early-close stream is drained/closed.
								assertEquals(513L, database.query("SELECT COUNT(*) FROM stream_work").fetchObject(Long.class).orElseThrow());
								return Optional.empty();
							};
							if (transaction) database.transaction(operation); else operation.perform();
							assertTrue(physical.getAutoCommit());
						}
					}
				}
			}
		}
	}

	@Test
	void opaqueConnectionsUseTheSentinelAndFullConsumptionOrCallbackFailureReleasesResources() throws Exception {
		for (boolean transaction : new boolean[]{false, true}) {
			for (String finish : List.of("complete", "throw")) {
				try (Connection physical = connect(true, true)) {
					seed(physical);
					Lease lease = new Lease(physical, true);
					Database database = Database.with(lease.source(), DatabaseType.MYSQL).fetchSize(17).build();
					RuntimeException marker = new IllegalStateException("callback marker");
					ReturningTransactionalOperation<Void> operation = () -> {
						if (finish.equals("throw")) {
							RuntimeException failure = assertThrows(RuntimeException.class, () -> database.query("SELECT n FROM stream_work ORDER BY n")
									.fetchSize(11).fetchStream(Integer.class, stream -> { assertEquals(1, stream.findFirst().orElseThrow()); throw marker; }));
							assertSame(marker, failure);
						} else {
							long count = database.query("SELECT n FROM stream_work ORDER BY n").fetchSize(11).fetchStream(Integer.class, stream -> stream.count());
							assertEquals(513L, count);
						}
						assertEquals("ResultsetRowsStreaming", lease.rowsClass);
						assertEquals(Integer.MIN_VALUE, lease.fetchAtExecute);
						assertTrue(lease.resultClosed);
						assertEquals(1, lease.statementCloses.get());
						assertEquals(513L, database.query("SELECT COUNT(*) FROM stream_work").fetchObject(Long.class).orElseThrow());
						return Optional.empty();
					};
					if (transaction) database.transaction(operation); else operation.perform();
					assertTrue(physical.getAutoCommit());
				}
			}
		}
	}

	@Test
	void clientPreparedEmulationKeepsRowStreamingEvenWithCursorFetchingEnabled() throws Exception {
		for (boolean transaction : new boolean[]{false, true}) {
			try (Connection physical = connect(true, true)) {
				seed(physical);
				Lease lease = new Lease(physical, false, true);
				Database database = Database.with(lease.source(), DatabaseType.MYSQL).build();
				ReturningTransactionalOperation<Void> operation = () -> {
					assertEquals(List.of(1, 2), database.query("SELECT n FROM stream_work ORDER BY n").fetchSize(17)
							.fetchStream(Integer.class, stream -> stream.limit(2).toList()));
					assertEquals("ResultsetRowsStreaming", lease.rowsClass);
					assertEquals(Integer.MIN_VALUE, lease.fetchAtExecute);
					assertTrue(lease.resultClosed);
					assertEquals(513L, database.query("SELECT COUNT(*) FROM stream_work").fetchObject(Long.class).orElseThrow());
					return Optional.empty();
				};
				if (transaction) database.transaction(operation); else operation.perform();
			}
		}
	}

	private static Connection connect(boolean cursor, boolean serverPrepared) throws Exception {
		String url = MYSQL.getJdbcUrl() + (MYSQL.getJdbcUrl().contains("?") ? "&" : "?") + "useCursorFetch=" + cursor + "&useServerPrepStmts=" + serverPrepared;
		return DriverManager.getConnection(url, MYSQL.getUsername(), MYSQL.getPassword());
	}
	private static void seed(Connection connection) throws Exception {
		try (Statement statement = connection.createStatement()) {
			statement.execute("CREATE TEMPORARY TABLE stream_work (n INT PRIMARY KEY)");
			statement.executeUpdate("INSERT INTO stream_work WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<513) SELECT n FROM numbers");
		}
	}

	private static final class Lease {
		final Connection physical;
		final boolean opaque;
		final boolean forceClientPrepared;
		final AtomicInteger statementCloses = new AtomicInteger();
		boolean resultClosed;
		String rowsClass;
		int fetchAtExecute;
		Connection wrapped;

		Lease(Connection physical, boolean opaque) { this(physical, opaque, false); }
		Lease(Connection physical, boolean opaque, boolean forceClientPrepared) {
			this.physical = physical; this.opaque = opaque; this.forceClientPrepared = forceClientPrepared;
		}
		DataSource source() {
			this.wrapped = proxy(Connection.class, (method, args) -> {
				if (method.getName().equals("close")) return null; // A one-connection pool; finally closes the physical lease.
				if (opaque && method.getName().equals("isWrapperFor")) return false;
				Object value;
				if (forceClientPrepared && method.getName().equals("prepareStatement") && args[0].equals("SELECT n FROM stream_work ORDER BY n"))
					value = physical.unwrap(JdbcConnection.class).clientPrepareStatement((String) args[0], ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				else value = invoke(physical, method, args);
				if (value instanceof PreparedStatement statement && args[0].equals("SELECT n FROM stream_work ORDER BY n"))
					return wrap(statement);
				return value;
			});
			return proxy(DataSource.class, (method, args) -> method.getName().equals("getConnection") ? wrapped : null);
		}
		PreparedStatement wrap(PreparedStatement statement) {
			return proxy(PreparedStatement.class, (method, args) -> {
				if (method.getName().equals("getConnection")) return wrapped;
				if (method.getName().equals("close")) statementCloses.incrementAndGet();
				Object value = invoke(statement, method, args);
				if (method.getName().equals("executeQuery") && value instanceof ResultSet rows) {
					fetchAtExecute = statement.getFetchSize();
					rowsClass = ((Resultset) rows).getRows().getClass().getSimpleName();
					return proxy(ResultSet.class, (jdbc, parameters) -> {
						if (jdbc.getName().equals("close")) resultClosed = true;
						return invoke(rows, jdbc, parameters);
					});
				}
				return value;
			});
		}
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
