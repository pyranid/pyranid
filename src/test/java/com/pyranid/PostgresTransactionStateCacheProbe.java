/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at
 * https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;

/** Isolates loader reclamation from the test runner's reflection and assertion caches. */
public final class PostgresTransactionStateCacheProbe {
	public interface StateReader { Object getTransactionState(); }
	public enum State { OPEN }

	public static void main(String[] arguments) throws Exception {
		Connection parentDriver = driver(PostgresTransactionStateCacheProbe.class.getClassLoader(),
				Class.forName("org.postgresql.PGConnection"));
		for (int deployment = 0; deployment < 3; deployment++) {
			WeakReference<ClassLoader> discarded = discardBridge(parentDriver);
			// Displace bounded JDK reflection/adaptation caches with a parent-owned invocation.
			checkUsable(PostgresDriverSupport.transactionState(parentDriver));
			awaitCollection(discarded, "bridge loader retained by a parent driver cache key");
		}
		Reference.reachabilityFence(parentDriver);
		for (int deployment = 0; deployment < 3; deployment++) {
			WeakReference<ClassLoader> discarded = discardDriver(Path.of(arguments[0]));
			checkUsable(PostgresDriverSupport.transactionState(parentDriver));
			awaitCollection(discarded, "driver loader retained by parent bridge cache");
		}
		Reference.reachabilityFence(parentDriver);
		System.out.println("PASS: transaction state cache loader reclamation");
	}

	private static WeakReference<ClassLoader> discardBridge(Connection driver) throws Exception {
		URL classes = PostgresDriverSupport.class.getProtectionDomain().getCodeSource().getLocation();
		try (URLClassLoader bridgeLoader = new URLClassLoader(new URL[]{classes}, PostgresTransactionStateCacheProbe.class.getClassLoader()) {
			@Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
				if (name.startsWith("com.pyranid.PostgresDriverSupport")) {
					synchronized (getClassLoadingLock(name)) {
						Class<?> type = findLoadedClass(name);
						if (type == null) type = findClass(name);
						if (resolve) resolveClass(type);
						return type;
					}
				}
				return super.loadClass(name, resolve);
			}
		}) {
			Class<?> bridge = bridgeLoader.loadClass("com.pyranid.PostgresDriverSupport");
			if (bridge.getClassLoader() != bridgeLoader) throw new AssertionError("Bridge control was not child-loaded");
			var stateRead = bridge.getDeclaredMethod("transactionState", Connection.class);
			stateRead.setAccessible(true);
			checkUsable(stateRead.invoke(null, driver));
			checkUsable(stateRead.invoke(null, driver));
			return new WeakReference<>(bridgeLoader);
		}
	}

	private static WeakReference<ClassLoader> discardDriver(Path apiClasses) throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (URLClassLoader driverLoader = new URLClassLoader(new URL[]{apiClasses.toUri().toURL()},
				PostgresTransactionStateCacheProbe.class.getClassLoader()) {
			@Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
				if (name.equals("org.postgresql.PGConnection")) {
					synchronized (getClassLoadingLock(name)) {
						Class<?> type = findLoadedClass(name);
						if (type == null) type = findClass(name);
						if (resolve) resolveClass(type);
						return type;
					}
				}
				return super.loadClass(name, resolve);
			}
		}) {
			Thread.currentThread().setContextClassLoader(driverLoader);
			Connection driver = driver(driverLoader, driverLoader.loadClass("org.postgresql.PGConnection"));
			if (driver.getClass().getClassLoader() != driverLoader) throw new AssertionError("Driver control was not child-loaded");
			checkUsable(PostgresDriverSupport.transactionState(driver));
			checkUsable(PostgresDriverSupport.transactionState(driver));
			return new WeakReference<>(driverLoader);
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	private static Connection driver(ClassLoader loader, Class<?> api) {
		return (Connection) Proxy.newProxyInstance(loader, new Class<?>[]{Connection.class, api, StateReader.class},
				(proxy, method, arguments) -> switch (method.getName()) {
					case "getTransactionState" -> State.OPEN;
					case "isWrapperFor" -> ((Class<?>) arguments[0]).isInstance(proxy);
					case "unwrap" -> ((Class<?>) arguments[0]).cast(proxy);
					default -> throw new AssertionError(method);
				});
	}

	private static void checkUsable(Object read) throws Exception {
		Field state = read.getClass().getDeclaredField("state");
		state.setAccessible(true);
		if (!"USABLE".equals(((Enum<?>) state.get(read)).name())) throw new AssertionError("State read did not reach the driver");
	}

	private static void awaitCollection(WeakReference<ClassLoader> loader, String description) throws InterruptedException {
		WeakReference<Object> ordinaryControl = new WeakReference<>(new byte[1024]);
		Object retained = new Object();
		WeakReference<Object> retainedControl = new WeakReference<>(retained);
		long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
		while (System.nanoTime() < deadline) {
			System.gc();
			if (loader.refersTo(null) && ordinaryControl.refersTo(null)) break;
			Thread.sleep(25);
		}
		if (!ordinaryControl.refersTo(null)) throw new AssertionError("Collection control did not run");
		if (retainedControl.refersTo(null)) throw new AssertionError("Retained control disappeared");
		if (!loader.refersTo(null)) throw new AssertionError(description);
		Reference.reachabilityFence(retained);
	}
}
