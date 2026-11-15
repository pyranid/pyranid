/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at
 * https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class PostgresTransactionStateCacheTests {
	@TempDir Path directory;

	public interface StateReader { Object getTransactionState() throws Throwable; }
	public enum State { OPEN, IDLE, FAILED, UNRECOGNIZED }

	@Test
	void cachedReadersStillReadEachPhysicalStateAndCacheOnlyJdkMetadata() throws Exception {
		Path apiClasses = compileApis();
		try (ApiLoader loader = new ApiLoader(apiClasses)) {
			AtomicReference<Object> state = new AtomicReference<>(State.OPEN);
			Connection driver = driver(loader, "org.postgresql.PGConnection", state, null, true);
			for (State value : State.values()) {
				state.set(value);
				var read = PostgresDriverSupport.transactionState(driver);
				assertEquals(switch (value) {
					case OPEN, IDLE -> PostgresDriverSupport.TransactionState.USABLE;
					case FAILED -> PostgresDriverSupport.TransactionState.FAILED;
					case UNRECOGNIZED -> PostgresDriverSupport.TransactionState.UNKNOWN;
				}, read.state());
				assertNull(read.failure());
			}
			for (Object invalid : new Object[]{"OPEN", null}) {
				state.set(invalid);
				assertEquals(PostgresDriverSupport.TransactionState.UNKNOWN, PostgresDriverSupport.transactionState(driver).state());
			}
			Object[] first = readerMetadata(driver.getClass());
			assertInstanceOf(Method.class, first[0]);
			assertNull(first[1]);
			PostgresDriverSupport.transactionState(driver);
			assertSame(first, readerMetadata(driver.getClass()));
			assertSame(driver.getClass(), ((Method) first[0]).getDeclaringClass());
		}
	}

	@Test
	void missingReaderIsCachedWithoutRetainingAThrowableOrPretendingTheTransactionIsUsable() throws Exception {
		try (ApiLoader loader = new ApiLoader(compileApis())) {
			Connection driver = driver(loader, "org.postgresql.PGConnection", new AtomicReference<>(), null, false);
			var first = PostgresDriverSupport.transactionState(driver);
			var second = PostgresDriverSupport.transactionState(driver);
			assertEquals(PostgresDriverSupport.TransactionState.UNKNOWN, first.state());
			assertInstanceOf(NoSuchMethodException.class, first.failure());
			assertInstanceOf(NoSuchMethodException.class, second.failure());
			assertNotSame(first.failure(), second.failure(), "A cached backtrace could retain the bridge loader");
			Object[] metadata = readerMetadata(driver.getClass());
			assertNull(metadata[0]);
			assertInstanceOf(String.class, metadata[1]);
			assertSame(metadata, readerMetadata(driver.getClass()));
		}
	}

	@Test
	void onePoolClassCanWrapDifferentDriversAndContextVisibilityCanChange() throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (ApiLoader firstLoader = new ApiLoader(compileApis()); ApiLoader secondLoader = new ApiLoader(compileApis())) {
			Connection first = driver(firstLoader, "com.yugabyte.PGConnection", new AtomicReference<>(State.OPEN), null, true);
			Connection second = driver(secondLoader, "org.postgresql.PGConnection", new AtomicReference<>(State.FAILED), null, true);
			AtomicReference<Connection> actual = new AtomicReference<>(first);
			Connection pooled = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, arguments) -> switch (method.getName()) {
						case "isWrapperFor" -> ((Class<?>) arguments[0]).isInstance(actual.get());
						case "unwrap" -> ((Class<?>) arguments[0]).cast(actual.get());
						default -> throw new AssertionError(method);
					});
			Thread.currentThread().setContextClassLoader(Connection.class.getClassLoader());
			assertEquals(PostgresDriverSupport.TransactionState.UNKNOWN, PostgresDriverSupport.transactionState(pooled).state());
			Thread.currentThread().setContextClassLoader(firstLoader);
			assertEquals(PostgresDriverSupport.TransactionState.USABLE, PostgresDriverSupport.transactionState(pooled).state());
			actual.set(second);
			Thread.currentThread().setContextClassLoader(secondLoader);
			assertEquals(PostgresDriverSupport.TransactionState.FAILED, PostgresDriverSupport.transactionState(pooled).state());
			Thread.currentThread().setContextClassLoader(Connection.class.getClassLoader());
			assertEquals(PostgresDriverSupport.TransactionState.UNKNOWN, PostgresDriverSupport.transactionState(pooled).state());
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	@Test
	void invocationAndWrapperFailuresRetainTheirOriginalIdentityAndFirstDiagnostic() throws Exception {
		for (Throwable failure : List.of(new SQLException("driver state unavailable"), new AssertionError("fatal driver state"),
				new InterruptedException("interrupted driver state"))) {
			try (ApiLoader loader = new ApiLoader(compileApis())) {
				Connection driver = driver(loader, "org.postgresql.PGConnection", new AtomicReference<>(State.OPEN), failure, true);
				var read = PostgresDriverSupport.transactionState(driver);
				assertEquals(PostgresDriverSupport.TransactionState.UNKNOWN, read.state());
				assertSame(failure, read.failure());
			}
		}
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (ApiLoader loader = new ApiLoader(compileApis())) {
			Connection actual = driver(loader, "com.yugabyte.PGConnection", new AtomicReference<>(State.OPEN), null, true);
			SQLException first = new SQLException("first wrapper failure");
			Connection pooled = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, arguments) -> {
						Class<?> api = (Class<?>) arguments[0];
						if (api.getName().equals("org.postgresql.PGConnection")) throw first;
						return method.getName().equals("isWrapperFor") ? api.isInstance(actual) : api.cast(actual);
					});
			Thread.currentThread().setContextClassLoader(loader);
			var read = PostgresDriverSupport.transactionState(pooled);
			assertEquals(PostgresDriverSupport.TransactionState.USABLE, read.state());
			assertSame(first, read.failure());
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	@Test
	void nullUnwrapRemainsUnknownAndStateReadsDoNotConsumeAnInterruptFlag() {
		Connection empty = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
				(proxy, method, arguments) -> method.getName().equals("isWrapperFor") ? true : null);
		assertEquals(PostgresDriverSupport.TransactionState.UNKNOWN, PostgresDriverSupport.transactionState(empty).state());
		try {
			Thread.currentThread().interrupt();
			PostgresDriverSupport.transactionState(empty);
			assertTrue(Thread.currentThread().isInterrupted());
		} finally { Thread.interrupted(); }
	}

	@Test
	void parentDriverKeysDoNotRetainChildBridgeLoadersAndChildDriverLoadersRemainCollectible() throws Exception {
		Path java = Path.of(System.getProperty("java.home"), "bin",
				System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java");
		Process process = new ProcessBuilder(java.toString(), "-Xmx96m", "-XX:+UseSerialGC", "-XX:+ClassUnloading",
				"-cp", System.getProperty("java.class.path"), PostgresTransactionStateCacheProbe.class.getName(), compileApis().toString())
				.redirectErrorStream(true).start();
		boolean completed;
		try { completed = process.waitFor(30, TimeUnit.SECONDS); }
		finally { if (process.isAlive()) process.destroyForcibly(); }
		String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		assertTrue(completed, output);
		assertEquals(0, process.exitValue(), output);
		assertTrue(output.contains("PASS: transaction state cache loader reclamation"), output);
	}

	private Path compileApis() throws Exception {
		Path sources = Files.createTempDirectory(directory, "state-api-sources-");
		Path classes = Files.createTempDirectory(directory, "state-api-classes-");
		List<Path> files = new java.util.ArrayList<>();
		for (String root : List.of("org.postgresql", "com.yugabyte")) {
			Path file = sources.resolve(root.replace('.', '/')).resolve("PGConnection.java");
			Files.createDirectories(file.getParent());
			Files.writeString(file, "package " + root + "; public interface PGConnection {}\n");
			files.add(file);
		}
		ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();
		assertEquals(0, ToolProvider.getSystemJavaCompiler().run(null, diagnostics, diagnostics, "--release", "17", "-d", classes.toString(),
				files.get(0).toString(), files.get(1).toString()), diagnostics::toString);
		return classes;
	}

	private static Connection driver(ApiLoader loader, String apiName, AtomicReference<Object> state, Throwable failure,
			boolean hasReader) throws Exception {
		Class<?> api = loader.loadClass(apiName);
		return (Connection) Proxy.newProxyInstance(loader,
				hasReader ? new Class<?>[]{Connection.class, api, StateReader.class} : new Class<?>[]{Connection.class, api},
				(proxy, method, arguments) -> switch (method.getName()) {
					case "getTransactionState" -> { if (failure != null) throw failure; yield state.get(); }
					case "isWrapperFor" -> ((Class<?>) arguments[0]).isInstance(proxy);
					case "unwrap" -> ((Class<?>) arguments[0]).cast(proxy);
					default -> throw new AssertionError(method);
				});
	}

	@SuppressWarnings("unchecked")
	private static Object[] readerMetadata(Class<?> type) throws Exception {
		Field field = PostgresDriverSupport.class.getDeclaredField("TRANSACTION_STATE_READERS");
		field.setAccessible(true);
		return ((ClassValue<Object[]>) field.get(null)).get(type);
	}

	static final class ApiLoader extends URLClassLoader {
		ApiLoader(Path classes) throws Exception { super(new URL[]{classes.toUri().toURL()}, PostgresTransactionStateCacheTests.class.getClassLoader()); }
		@Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
			if (name.equals("org.postgresql.PGConnection") || name.equals("com.yugabyte.PGConnection")) {
				synchronized (getClassLoadingLock(name)) {
					Class<?> type = findLoadedClass(name);
					if (type == null) type = findClass(name);
					if (resolve) resolveClass(type);
					return type;
				}
			}
			return super.loadClass(name, resolve);
		}
	}
}
