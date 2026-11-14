/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class NotificationSanitationTests {
	@Test
	void sqlRollbackClearsInheritedServerTransactionsRegardlessOfJdbcAutocommit() throws Exception {
		for (boolean autoCommit : new boolean[]{true, false}) {
			Fixture fixture = new Fixture(autoCommit);
			fixture.database().withNotificationSession("sanitation", session -> {
				assertFalse(fixture.serverTransaction, "LISTEN must be committed before the callback starts");
				assertTrue(fixture.listening);
				assertTrue(fixture.autoCommit);
				fixture.events.add("callback");
			});
			assertTrue(fixture.events.indexOf("ROLLBACK") < fixture.events.indexOf("UNLISTEN *"));
			assertTrue(fixture.events.indexOf("ROLLBACK") < fixture.events.indexOf("LISTEN \"sanitation\""));
			assertEquals(autoCommit, fixture.autoCommit, "Healthy cleanup restores borrowed JDBC state");
			assertFalse(fixture.listening);
			assertTrue(fixture.closed);
			assertFalse(fixture.aborted);
			assertEquals(4, fixture.closedStatements);
			if (autoCommit) {
				assertFalse(fixture.events.contains("jdbcRollback"));
				assertFalse(fixture.events.contains("setAutoCommit:true"));
			} else {
				assertTrue(fixture.events.indexOf("jdbcRollback") < fixture.events.indexOf("setAutoCommit:true"));
				assertTrue(fixture.events.indexOf("setAutoCommit:true") < fixture.events.indexOf("ROLLBACK"));
				assertTrue(fixture.events.indexOf("callback") < fixture.events.indexOf("setAutoCommit:false"));
			}
		}
	}

	@Test
	void rollbackExecutionFailuresNeverRegisterOrInvokeCallbacksAndDiscardTheCandidate() {
		for (Throwable failure : List.of(new SQLException("reset failed", "42000"),
				new IllegalStateException("reset failed"), new AssertionError("reset failed"))) {
			Fixture fixture = new Fixture(true);
			fixture.rollbackFailure = failure;
			AtomicBoolean callback = new AtomicBoolean();
			Class<? extends Throwable> expectedType = failure instanceof Error ? Error.class : DatabaseException.class;
			Throwable thrown = assertThrows(expectedType,
					() -> fixture.database().withNotificationSession("sanitation", session -> callback.set(true)));
			assertSame(failure, failure instanceof Error ? thrown : thrown.getCause());
			assertFalse(callback.get());
			assertEquals(List.of("ROLLBACK", "abort", "close"), fixture.events);
			assertEquals(1, fixture.closedStatements);
			assertTrue(fixture.aborted);
			assertTrue(fixture.closed);
		}
	}

	@Test
	void rollbackStatementCloseFailureAlsoFailsClosedAndKeepsExecutionFailurePrimary() {
		SQLException closeFailure = new SQLException("reset statement close failed", "0A000");
		for (boolean executionFails : new boolean[]{false, true}) {
			Fixture fixture = new Fixture(true);
			SQLException executionFailure = new SQLException("reset failed", "42000");
			fixture.rollbackFailure = executionFails ? executionFailure : null;
			fixture.rollbackCloseFailure = closeFailure;
			DatabaseException thrown = assertThrows(DatabaseException.class,
					() -> fixture.database().withNotificationSession("sanitation", session -> fail("Sanitation failed")));
			assertSame(executionFails ? executionFailure : closeFailure, thrown.getCause());
			if (executionFails) assertArrayEquals(new Throwable[]{closeFailure}, executionFailure.getSuppressed());
			assertEquals(List.of("ROLLBACK", "abort", "close"), fixture.events);
			assertTrue(fixture.aborted);
			assertTrue(fixture.closed);
		}
	}

	private static final class Fixture {
		private boolean autoCommit, serverTransaction = true, listening, closed, aborted;
		private Throwable rollbackFailure;
		private SQLException rollbackCloseFailure;
		private int closedStatements;
		private final List<String> events = new ArrayList<>();

		private Fixture(boolean autoCommit) { this.autoCommit = autoCommit; }

		private Database database() {
			Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
					new Class<?>[]{Connection.class, PGConnection.class}, (proxy, method, args) -> switch (method.getName()) {
						case "getAutoCommit" -> this.autoCommit;
						case "setAutoCommit" -> { this.autoCommit = (boolean) args[0]; this.events.add("setAutoCommit:" + args[0]); yield null; }
						case "rollback" -> { this.serverTransaction = false; this.events.add("jdbcRollback"); yield null; }
						case "createStatement" -> statement();
						case "getNetworkTimeout" -> 0;
						case "setNetworkTimeout" -> null;
						case "getNotifications" -> new PGNotification[0];
						case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
						case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
						case "isClosed" -> this.closed;
						case "abort" -> { this.aborted = true; this.events.add("abort"); yield null; }
						case "close" -> { this.closed = true; this.events.add("close"); yield null; }
						default -> throw new AssertionError(method.getName());
					});
			DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> {
						if (method.getName().equals("getConnection")) return connection;
						throw new AssertionError(method.getName());
					});
			return Database.with(source, DatabaseType.POSTGRESQL).build();
		}

		private Statement statement() {
			String[] executed = {null};
			return (Statement) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Statement.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "execute" -> {
							String sql = (String) args[0];
							executed[0] = sql;
							this.events.add(sql);
							if (sql.equals("ROLLBACK")) {
								if (this.rollbackFailure != null) throw this.rollbackFailure;
								this.serverTransaction = false;
							} else if (sql.startsWith("LISTEN ")) {
								assertFalse(this.serverTransaction);
								this.listening = true;
							} else if (sql.equals("UNLISTEN *")) this.listening = false;
							yield false;
						}
						case "close" -> {
							this.closedStatements++;
							if ("ROLLBACK".equals(executed[0]) && this.rollbackCloseFailure != null) throw this.rollbackCloseFailure;
							yield null;
						}
						default -> throw new AssertionError(method.getName());
					});
		}
	}
}
