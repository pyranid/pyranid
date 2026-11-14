/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.ToolProvider;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MySqlStreamingConfigurationTests {
	@TempDir Path directory;

	@Test
	void positiveHintsNeedPositiveCursorEvidenceButConfiguredZeroRemainsTheDriverDefault() throws Exception {
		for (boolean transactional : new boolean[]{false, true}) {
			for (boolean configured : new boolean[]{false, true}) {
				for (int hint : new int[]{0, 9, Integer.MIN_VALUE}) {
					AtomicInteger fetch = new AtomicInteger(hint);
					MySqlDialect.INSTANCE.configureStreamingPreparedStatement(statement(fetch, null, null), DatabaseStreamState.none(), transactional, configured);
					assertEquals(hint == 0 && configured ? 0 : Integer.MIN_VALUE, fetch.get());
				}
			}
		}
	}

	@Test
	void directAndWrappedPublicDriverPropertiesPreservePositiveCursorHintsWithoutLinkingTheDriver() throws Exception {
		try (URLClassLoader driver = driverApi()) {
			ClassLoader previous = Thread.currentThread().getContextClassLoader();
			try {
				Thread.currentThread().setContextClassLoader(driver);
				for (boolean enabled : new boolean[]{false, true}) {
					Connection nativeConnection = connection(driver, enabled, null);
					for (boolean wrapped : new boolean[]{false, true}) {
						Connection connection = wrapped ? wrapper(nativeConnection, null) : nativeConnection;
						for (boolean configured : new boolean[]{false, true}) {
							AtomicInteger fetch = new AtomicInteger(17);
							MySqlDialect.INSTANCE.configureStreamingPreparedStatement(statement(fetch, connection, null), DatabaseStreamState.none(), false, configured);
							assertEquals(enabled ? 17 : Integer.MIN_VALUE, fetch.get());
						}
					}
				}
			} finally { Thread.currentThread().setContextClassLoader(previous); }
		}
	}

	@Test
	void aClientPreparedStatementCannotAuthorizeCursorHintsFromTheConnectionPropertyAlone() throws Exception {
		try (URLClassLoader driver = driverApi()) {
			AtomicInteger fetch = new AtomicInteger(29);
			MySqlDialect.INSTANCE.configureStreamingPreparedStatement(statement(fetch, connection(driver, true, null), null, false),
					DatabaseStreamState.none(), false, true);
			assertEquals(Integer.MIN_VALUE, fetch.get());
		}
	}

	@Test
	void unavailableOptionalPropertiesFallBackButTerminalDriverEvidencePropagatesWithoutChangingTheHint() throws Exception {
		try (URLClassLoader driver = driverApi()) {
			ClassLoader previous = Thread.currentThread().getContextClassLoader();
			try {
				Thread.currentThread().setContextClassLoader(driver);
				for (String location : List.of("properties", "property", "value", "unwrap")) {
					Throwable unsupported = new SQLFeatureNotSupportedException("optional inspection unavailable", "0A000");
					Connection connection = location.equals("unwrap") ? wrapper(connection(driver, true, null), unsupported)
							: connection(driver, true, new Fault(location, unsupported));
					AtomicInteger fetch = new AtomicInteger(19);
					MySqlDialect.INSTANCE.configureStreamingPreparedStatement(statement(fetch, connection, null), DatabaseStreamState.none(), true, true);
					assertEquals(Integer.MIN_VALUE, fetch.get(), location);
					SQLException terminal = new SQLNonTransientConnectionException("connection lost during inspection", "08006");
					connection = location.equals("unwrap") ? wrapper(connection(driver, true, null), terminal)
							: connection(driver, true, new Fault(location, terminal));
					AtomicInteger unchanged = new AtomicInteger(19);
					PreparedStatement statement = statement(unchanged, connection, null);
					SQLException thrown = assertThrows(SQLException.class, () -> MySqlDialect.INSTANCE.configureStreamingPreparedStatement(
							statement, DatabaseStreamState.none(), true, true));
					assertSame(terminal, thrown);
					assertEquals(19, unchanged.get());
				}
			} finally { Thread.currentThread().setContextClassLoader(previous); }
		}
	}

	@Test
	void fatalPropertyInspectionErrorsPropagateWithTheirOriginalIdentity() throws Exception {
		try (URLClassLoader driver = driverApi()) {
			Error original = new AssertionError("fatal property inspection");
			AtomicInteger fetch = new AtomicInteger(23);
			PreparedStatement statement = statement(fetch, connection(driver, true, new Fault("value", original)), null);
			Error thrown = assertThrows(Error.class, () -> MySqlDialect.INSTANCE.configureStreamingPreparedStatement(
					statement, DatabaseStreamState.none(), false, true));
			assertSame(original, thrown);
			assertEquals(23, fetch.get());
		}
	}

	@Test
	void statementInspectionAndConfigurationFailuresAreNotConvertedIntoAnotherMode() {
		for (String location : List.of("getFetchSize", "getConnection", "setFetchSize")) {
			SQLException original = new SQLException("statement operation failed", "HY000");
			AtomicInteger fetch = new AtomicInteger(21);
			PreparedStatement statement = statement(fetch, null, new Fault(location, original));
			SQLException thrown = assertThrows(SQLException.class, () -> MySqlDialect.INSTANCE.configureStreamingPreparedStatement(
					statement, DatabaseStreamState.none(), false, true));
			assertSame(original, thrown);
			assertEquals(21, fetch.get());
		}
	}

	private URLClassLoader driverApi() throws Exception {
		Path root = Files.createTempDirectory(directory, "driver_");
		Path source = root.resolve("com/mysql/cj/jdbc"); Files.createDirectories(source);
		List<String> files = new ArrayList<>();
		for (String[] definition : new String[][]{
				{"JdbcConnection", "extends java.sql.Connection { Properties getPropertySet() throws java.sql.SQLException; }"},
				{"ServerPreparedStatement", "extends java.sql.PreparedStatement {}"},
				{"Properties", "{ Property getBooleanProperty(String name) throws java.sql.SQLException; }"},
				{"Property", "{ Object getValue() throws java.sql.SQLException; }"}}) {
			Path file = source.resolve(definition[0] + ".java");
			Files.writeString(file, "package com.mysql.cj.jdbc; public interface " + definition[0] + " " + definition[1]);
			files.add(file.toString());
		}
		List<String> arguments = new ArrayList<>(List.of("--release", "17", "-d", root.toString())); arguments.addAll(files);
		assertEquals(0, ToolProvider.getSystemJavaCompiler().run(null, null, null, arguments.toArray(String[]::new)));
		return new URLClassLoader(new java.net.URL[]{root.toUri().toURL()}, ClassLoader.getPlatformClassLoader());
	}

	private static Connection connection(ClassLoader loader, boolean enabled, Fault fault) throws Exception {
		Class<?> valueApi = loader.loadClass("com.mysql.cj.jdbc.Property");
		Object property = Proxy.newProxyInstance(loader, new Class<?>[]{valueApi}, (proxy, method, args) -> {
			failIf(fault, "value"); return enabled;
		});
		Object properties = Proxy.newProxyInstance(loader, new Class<?>[]{loader.loadClass("com.mysql.cj.jdbc.Properties")}, (proxy, method, args) -> {
			assertEquals("useCursorFetch", args[0]); failIf(fault, "property"); return property;
		});
		return (Connection) Proxy.newProxyInstance(loader, new Class<?>[]{loader.loadClass("com.mysql.cj.jdbc.JdbcConnection")}, (proxy, method, args) -> {
			if (method.getName().equals("getPropertySet")) { failIf(fault, "properties"); return properties; }
			return defaultValue(method);
		});
	}

	private static Connection wrapper(Connection nativeConnection, Throwable failure) {
		return (Connection) Proxy.newProxyInstance(MySqlStreamingConfigurationTests.class.getClassLoader(), new Class<?>[]{Connection.class}, (proxy, method, args) -> {
			if (method.getName().equals("isWrapperFor")) return ((Class<?>) args[0]).isInstance(nativeConnection);
			if (method.getName().equals("unwrap")) { if (failure != null) throw failure; return nativeConnection; }
			return defaultValue(method);
		});
	}

	private static PreparedStatement statement(AtomicInteger fetch, Connection connection, Fault fault) {
		return statement(fetch, connection, fault, true);
	}
	private static PreparedStatement statement(AtomicInteger fetch, Connection connection, Fault fault, boolean serverPrepared) {
		ClassLoader loader = connection == null ? null : connection.getClass().getClassLoader();
		Class<?> statementApi = PreparedStatement.class;
		for (ClassLoader candidate : new ClassLoader[]{Thread.currentThread().getContextClassLoader(), loader}) {
			if (candidate == null || !serverPrepared) continue;
			try {
				Class<?> api = Class.forName("com.mysql.cj.jdbc.ServerPreparedStatement", false, candidate);
				if (api.isInterface()) { statementApi = api; break; }
			} catch (ClassNotFoundException ignored) {}
		}
		return (PreparedStatement) Proxy.newProxyInstance(statementApi.getClassLoader(), new Class<?>[]{statementApi}, (proxy, method, args) -> {
			failIf(fault, method.getName());
			return switch (method.getName()) {
				case "getFetchSize" -> fetch.get();
				case "getConnection" -> connection;
				case "setFetchSize" -> { fetch.set((Integer) args[0]); yield null; }
				default -> defaultValue(method);
			};
		});
	}
	private static Object defaultValue(Method method) {
		if (method.getReturnType() == boolean.class) return false;
		if (method.getReturnType() == int.class) return 0;
		if (method.getReturnType() == long.class) return 0L;
		return null;
	}
	private static void failIf(Fault fault, String location) throws Throwable {
		if (fault != null && fault.location().equals(location)) throw fault.failure();
	}
	private record Fault(String location, Throwable failure) {}
}
