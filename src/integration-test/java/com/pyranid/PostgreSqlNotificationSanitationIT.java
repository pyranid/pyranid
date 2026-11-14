/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises inherited server transactions that are invisible to JDBC's autocommit setting. */
@Testcontainers
class PostgreSqlNotificationSanitationIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("notification_sanitation").withUsername("pyranid").withPassword("pyranid");

	@Test
	@Timeout(value = 20, unit = TimeUnit.SECONDS)
	void rawBeginWithAutocommitTrueIsRolledBackBeforeRegistrationAndDelivery() throws Exception {
		verify(true, false);
	}

	@Test
	@Timeout(value = 20, unit = TimeUnit.SECONDS)
	void inheritedJdbcTransactionIsDiscardedAndOriginalAutocommitRestored() throws Exception {
		verify(false, false);
	}

	@Test
	@Timeout(value = 20, unit = TimeUnit.SECONDS)
	void failedRawTransactionWithAutocommitTrueIsRecoveredBeforeRegistration() throws Exception {
		verify(true, true);
	}

	private static void verify(boolean autoCommit, boolean failedTransaction) throws Exception {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		Database application = Database.with(source, DatabaseType.POSTGRESQL).build();
		String suffix = UUID.randomUUID().toString().replace("-", "");
		String table = "notification_inherited_" + suffix;
		String staleChannel = "stale_" + suffix;
		String channel = "fresh_" + suffix;
		application.query("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY)").execute();
		try (Connection physical = source.getConnection()) {
			int pid;
			try (Statement statement = physical.createStatement(); ResultSet rows = statement.executeQuery("SELECT pg_backend_pid()")) {
				assertTrue(rows.next()); pid = rows.getInt(1);
			}
			try (Statement statement = physical.createStatement()) { statement.execute("LISTEN \"" + staleChannel + "\""); }
			physical.setAutoCommit(autoCommit);
			try (Statement statement = physical.createStatement()) {
				if (autoCommit) statement.execute("BEGIN");
				statement.execute("INSERT INTO " + table + " VALUES (1)");
				if (failedTransaction) {
					SQLException failed = assertThrows(SQLException.class, () -> statement.execute("SELECT 1/0"));
					assertEquals("22012", failed.getSQLState());
				}
			}
			assertEquals(autoCommit, physical.getAutoCommit());
			assertEquals(failedTransaction ? "idle in transaction (aborted)" : "idle in transaction", state(application, pid));
			AtomicInteger checkouts = new AtomicInteger(), returns = new AtomicInteger(), aborts = new AtomicInteger();
			Connection borrowed = (Connection) Proxy.newProxyInstance(PostgreSqlNotificationSanitationIT.class.getClassLoader(),
					new Class<?>[]{Connection.class}, (proxy, method, args) -> {
						if (method.getName().equals("close")) { returns.incrementAndGet(); return null; }
						if (method.getName().equals("abort")) aborts.incrementAndGet();
						try { return method.invoke(physical, args); }
						catch (InvocationTargetException failure) { throw failure.getCause(); }
					});
			DataSource listenerSource = (DataSource) Proxy.newProxyInstance(PostgreSqlNotificationSanitationIT.class.getClassLoader(),
					new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
						if (method.getName().equals("getConnection")) { checkouts.incrementAndGet(); return borrowed; }
						throw new AssertionError(method.getName());
					});
			Database listener = Database.with(listenerSource, DatabaseType.POSTGRESQL).build();
			listener.withNotificationSession(channel, session -> {
				assertEquals("idle", state(application, pid), "The listener must not retain a server transaction");
				assertEquals(0L, application.query("SELECT COUNT(*) FROM " + table).fetchObject(Long.class).orElseThrow(),
						"Inherited application work must be discarded, not committed");
				try {
					assertTrue(physical.getAutoCommit());
					assertEquals(List.of(channel), listeningChannels(physical));
				} catch (SQLException failure) { throw new DatabaseException(failure); }
				application.sendNotification(channel, "after-reset");
				List<Notification> received = session.awaitNotifications(Duration.ofSeconds(5));
				assertEquals(1, received.size());
				assertEquals(channel, received.get(0).getChannel());
				assertEquals("after-reset", received.get(0).getPayload());
			});
			assertEquals(1, checkouts.get());
			assertEquals(1, returns.get());
			assertEquals(0, aborts.get(), "A sanitized healthy connection should be returned normally");
			assertFalse(physical.isClosed());
			assertEquals(autoCommit, physical.getAutoCommit());
			assertEquals("idle", state(application, pid));
			assertEquals(List.of(), listeningChannels(physical));
			assertEquals(0L, application.query("SELECT COUNT(*) FROM " + table).fetchObject(Long.class).orElseThrow());
			if (!autoCommit) physical.rollback();
		} finally { application.query("DROP TABLE " + table).execute(); }
	}

	private static String state(Database application, int pid) {
		return application.query("SELECT state FROM pg_stat_activity WHERE pid = :pid")
				.bind("pid", pid).fetchObject(String.class).orElseThrow();
	}

	private static List<String> listeningChannels(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery("SELECT pg_listening_channels()")) {
			java.util.ArrayList<String> channels = new java.util.ArrayList<>();
			while (rows.next()) channels.add(rows.getString(1));
			return channels;
		}
	}
}
