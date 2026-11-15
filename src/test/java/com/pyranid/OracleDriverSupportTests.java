/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class OracleDriverSupportTests {
	@AfterEach void clearInterruption() { Thread.interrupted(); }

	@Test
	void canonicalNativeTextRetainsFractionSignsAndNanosecondsWithoutDurationCodec() {
		Duration[] values = {Duration.ZERO, Duration.ofNanos(1), Duration.ofNanos(-1), Duration.ofSeconds(-1, 1),
				Duration.ofNanos(-1_000_000_001L), Duration.ofNanos(123_456_789), Duration.ofSeconds(-90, 123_456_789),
				Duration.ofDays(-2).plusNanos(123_456_789), Duration.ofDays(999_999_999).plusSeconds(86_399).plusNanos(999_999_999)};
		String[] expected = {"+0 00:00:00.000000000", "+0 00:00:00.000000001", "-0 00:00:00.000000001", "-0 00:00:00.999999999",
				"-0 00:00:01.000000001", "+0 00:00:00.123456789", "-0 00:01:29.876543211",
				"-1 23:59:59.876543211", "+999999999 23:59:59.999999999"};
		for (int index = 0; index < values.length; index++)
			assertEquals(expected[index], OracleDriverSupport.formatDaySecond(values[index]));
		for (Duration oversized : List.of(Duration.ofDays(1_000_000_000), Duration.ofDays(-1_000_000_000),
				Duration.ofSeconds(Long.MAX_VALUE), Duration.ofSeconds(Long.MIN_VALUE)))
			assertThrows(DateTimeException.class, () -> OracleDriverSupport.formatDaySecond(oversized));
	}

	@Test
	void nativeFactoryUsesTheActualDriverAndCreatesIndependentExactValues() throws Exception {
		try (var loader = OracleIntervalTestDriver.newLoader()) {
			Connection connection = driver(loader);
			Object value = OracleDriverSupport.newIntervalDaySecond(connection, Duration.ofNanos(-1));
			assertSame(loader, value.getClass().getClassLoader());
			assertEquals("-0 00:00:00.000000001", value.toString());
			Object next = OracleDriverSupport.newIntervalDaySecond(connection, Duration.ofNanos(1));
			assertNotSame(value, next);
			assertSame(value.getClass(), next.getClass());
			assertEquals("+0 00:00:00.000000001", next.toString());
		}
	}

	@Test
	void onePoolClassCanChangeDriversWithoutAConnectionClassDecisionCache() throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (var first = OracleIntervalTestDriver.newLoader(); var second = OracleIntervalTestDriver.newLoader()) {
			AtomicReference<Connection> actual = new AtomicReference<>(driver(first));
			Connection pool = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "unwrap" -> ((Class<?>) args[0]).cast(actual.get());
						case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(actual.get());
						default -> throw new AssertionError(method);
					});
			Thread.currentThread().setContextClassLoader(second);
			assertSame(first, OracleDriverSupport.newIntervalDaySecond(pool, Duration.ZERO).getClass().getClassLoader());
			actual.set(driver(second));
			Thread.currentThread().setContextClassLoader(first);
			assertSame(second, OracleDriverSupport.newIntervalDaySecond(pool, Duration.ZERO).getClass().getClassLoader());
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	@Test
	void constructorAndTerminalDiscoveryFailuresRetainTheirIdentity() throws Exception {
		try (var loader = OracleIntervalTestDriver.newLoader()) {
			Field field = loader.loadClass("oracle.sql.INTERVALDS").getField("failure");
			for (Throwable original : List.of(new SQLException("native constructor", "22015"), new IllegalStateException("native"), new AssertionError("fatal"))) {
				field.set(null, original);
				assertSame(original, assertThrows(Throwable.class,
						() -> OracleDriverSupport.newIntervalDaySecond(driver(loader), Duration.ofNanos(1))));
			}
			field.set(null, null);
		}
		for (Throwable original : List.of(new SQLException("cancelled", "57014"),
				new SQLException("interrupted unwrap", new InterruptedException("cancel")), new AssertionError("fatal unwrap"))) {
			Connection broken = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> { throw original; });
			assertSame(original, assertThrows(Throwable.class, () -> OracleDriverSupport.newIntervalDaySecond(broken, Duration.ZERO)));
		}
	}

	@Test
	void opaqueWrapperCannotGuessBetweenTwoVisibleDriverCopies() throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (var first = OracleIntervalTestDriver.newLoader(); var second = OracleIntervalTestDriver.newLoader()) {
			ClassLoader firstOnly = new ClassLoader(ClassLoader.getPlatformClassLoader()) {
				@Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
					if (name.equals("oracle.jdbc.OracleConnection") || name.equals("oracle.sql.INTERVALDS")) return first.loadClass(name);
					return super.loadClass(name, resolve);
				}
			};
			try (URLClassLoader isolated = new URLClassLoader(new URL[]{OracleDriverSupport.class.getProtectionDomain().getCodeSource().getLocation()}, firstOnly)) {
				Thread.currentThread().setContextClassLoader(second);
				Connection opaque = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
						(proxy, method, args) -> method.getName().equals("unwrap") ? proxy : false);
				InvocationTargetException failure = assertThrows(InvocationTargetException.class,
						() -> createMethod(isolated).invoke(null, opaque, Duration.ZERO));
				assertInstanceOf(SQLException.class, failure.getTargetException());
				assertTrue(failure.getTargetException().getMessage().contains("unambiguously"));
			}
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	@Test
	void identifiedDriverUsesItsApiLoaderWhenProxyLoaderHidesNativeValues() throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (var first = OracleIntervalTestDriver.newLoader(); var foreign = OracleIntervalTestDriver.newLoader();
				URLClassLoader isolated = bridgeLoader()) {
			Class<?> api = first.loadClass("oracle.jdbc.OracleConnection");
			ClassLoader proxyLoader = new ClassLoader(ClassLoader.getPlatformClassLoader()) {
				@Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
					if (name.equals("oracle.jdbc.OracleConnection")) return api;
					if (name.equals("oracle.sql.INTERVALDS")) throw new ClassNotFoundException(name);
					return super.loadClass(name, resolve);
				}
			};
			Thread.currentThread().setContextClassLoader(foreign);
			Object value = createMethod(isolated).invoke(null, driver(proxyLoader, api), Duration.ofNanos(-1));
			assertSame(first, value.getClass().getClassLoader());
			assertEquals("-0 00:00:00.000000001", value.toString());
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	@Test
	void identifiedDriverMissingNativeFactoryCannotUseForeignVisibleCopy() throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (var missingClass = OracleIntervalTestDriver.loaderWithoutDaySecond();
				var missingConstructor = OracleIntervalTestDriver.loaderWithoutDaySecondConstructor(); var foreign = OracleIntervalTestDriver.newLoader();
				URLClassLoader isolated = bridgeLoader()) {
			Thread.currentThread().setContextClassLoader(foreign);
			for (var owner : List.of(missingClass, missingConstructor)) {
				InvocationTargetException failure = assertThrows(InvocationTargetException.class,
						() -> createMethod(isolated).invoke(null, driver(owner), Duration.ZERO));
				assertInstanceOf(SQLException.class, failure.getTargetException());
				assertTrue(failure.getTargetException().getMessage().contains("INTERVALDS"));
			}
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	@Test
	void claimedOracleUnwrapMustReturnTheIdentifiedApi() throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (var owner = OracleIntervalTestDriver.newLoader(); URLClassLoader isolated = bridgeLoader()) {
			Thread.currentThread().setContextClassLoader(owner);
			Class<?> api = owner.loadClass("oracle.jdbc.OracleConnection");
			for (Object invalid : new Object[]{null, new Object()}) {
				Connection connection = (Connection) Proxy.newProxyInstance(owner, new Class<?>[]{Connection.class},
						(proxy, method, args) -> switch (method.getName()) {
							case "unwrap" -> args[0] == Connection.class ? proxy : invalid;
							case "isWrapperFor" -> args[0] == api;
							default -> throw new AssertionError(method);
						});
				InvocationTargetException failure = assertThrows(InvocationTargetException.class,
						() -> createMethod(isolated).invoke(null, connection, Duration.ZERO));
				assertInstanceOf(SQLException.class, failure.getTargetException());
				assertTrue(failure.getTargetException().getMessage().contains("claimed Oracle connection API"));
			}
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	@Test
	void claimedOracleUnwrapFailureCannotSelectAForeignFactoryAndRetainsIdentity() throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (var owner = OracleIntervalTestDriver.loaderWithoutDaySecond(); var foreign = OracleIntervalTestDriver.newLoader();
				URLClassLoader isolated = bridgeLoader()) {
			Thread.currentThread().setContextClassLoader(foreign);
			Class<?> api = owner.loadClass("oracle.jdbc.OracleConnection");
			for (Throwable original : List.of(new SQLException("claimed unwrap unsupported", "0A000"),
					new IllegalStateException("claimed unwrap failure"), new AbstractMethodError("claimed unwrap missing"))) {
				Connection connection = (Connection) Proxy.newProxyInstance(owner, new Class<?>[]{Connection.class},
						(proxy, method, args) -> switch (method.getName()) {
							case "unwrap" -> { if (args[0] == Connection.class) yield proxy; throw original; }
							case "isWrapperFor" -> args[0] == api;
							default -> throw new AssertionError(method);
						});
				InvocationTargetException failure = assertThrows(InvocationTargetException.class,
						() -> createMethod(isolated).invoke(null, connection, Duration.ZERO));
				assertSame(original, failure.getTargetException());
			}
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	@Test
	void conflictingConnectionAndApiNativeClassesAreRejected() throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (var first = OracleIntervalTestDriver.newLoader(); var foreign = OracleIntervalTestDriver.newLoader();
				URLClassLoader isolated = bridgeLoader()) {
			Class<?> api = first.loadClass("oracle.jdbc.OracleConnection");
			ClassLoader proxyLoader = new ClassLoader(ClassLoader.getPlatformClassLoader()) {
				@Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
					if (name.equals("oracle.jdbc.OracleConnection")) return api;
					if (name.equals("oracle.sql.INTERVALDS")) return foreign.loadClass(name);
					return super.loadClass(name, resolve);
				}
			};
			Thread.currentThread().setContextClassLoader(foreign);
			InvocationTargetException failure = assertThrows(InvocationTargetException.class,
					() -> createMethod(isolated).invoke(null, driver(proxyLoader, api), Duration.ZERO));
			assertInstanceOf(SQLException.class, failure.getTargetException());
			assertTrue(failure.getTargetException().getMessage().contains("conflicting INTERVALDS"));
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	@Test
	void loaderIdentityCannotHideOpaqueDriverAmbiguity() throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (var first = OracleIntervalTestDriver.newLoader(); var second = OracleIntervalTestDriver.newLoader()) {
			ClassLoader firstOnly = new ClassLoader(ClassLoader.getPlatformClassLoader()) {
				@Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
					if (name.startsWith("oracle.")) return first.loadClass(name);
					return super.loadClass(name, resolve);
				}
			};
			AtomicInteger equalityCalls = new AtomicInteger();
			ClassLoader misleading = new ClassLoader(ClassLoader.getPlatformClassLoader()) {
				@Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
					if (name.startsWith("oracle.")) return second.loadClass(name);
					return super.loadClass(name, resolve);
				}
				@Override public boolean equals(Object other) { equalityCalls.incrementAndGet(); return other instanceof ClassLoader; }
				@Override public int hashCode() { return 1; }
			};
			try (URLClassLoader isolated = new URLClassLoader(new URL[]{OracleDriverSupport.class.getProtectionDomain().getCodeSource().getLocation()}, firstOnly)) {
				Thread.currentThread().setContextClassLoader(misleading);
				Connection opaque = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
						(proxy, method, args) -> method.getName().equals("unwrap") ? proxy : false);
				InvocationTargetException failure = assertThrows(InvocationTargetException.class,
						() -> createMethod(isolated).invoke(null, opaque, Duration.ZERO));
				assertInstanceOf(SQLException.class, failure.getTargetException());
				assertTrue(failure.getTargetException().getMessage().contains("unambiguously"));
				assertEquals(0, equalityCalls.get(), "Loader deduplication must compare identity without executing user equality");
			}
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	@Test
	void absenceOfAnOracleDriverFailsClearlyAndPendingInterruptionIsNotConsumed() throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (URLClassLoader isolated = bridgeLoader()) {
			Thread.currentThread().setContextClassLoader(Connection.class.getClassLoader());
			Connection opaque = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> method.getName().equals("unwrap") ? proxy : false);
			Method create = createMethod(isolated);
			InvocationTargetException failure = assertThrows(InvocationTargetException.class, () -> create.invoke(null, opaque, Duration.ZERO));
			assertInstanceOf(SQLException.class, failure.getTargetException());
			assertTrue(failure.getTargetException().getMessage().contains("INTERVALDS"));
		} finally { Thread.currentThread().setContextClassLoader(previous); }
		Thread.currentThread().interrupt();
		assertDoesNotThrow(() -> OracleDriverSupport.newIntervalDaySecond(driver(OracleIntervalTestDriver.sharedLoader()), Duration.ZERO));
		assertTrue(Thread.currentThread().isInterrupted());
	}

	@Test
	void missingClassFailuresDoNotDiscardInterruptionOrFatalCauses() throws Exception {
		ClassLoader previous = Thread.currentThread().getContextClassLoader();
		try (URLClassLoader isolated = bridgeLoader()) {
			Method create = createMethod(isolated);
			Connection opaque = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> method.getName().equals("unwrap") ? proxy : false);
			for (Throwable cause : List.of(new InterruptedException("class loading interrupted"), new AssertionError("fatal loader"))) {
				ClassNotFoundException original = new ClassNotFoundException("oracle.jdbc.OracleConnection", cause);
				Thread.currentThread().setContextClassLoader(new ClassLoader(ClassLoader.getPlatformClassLoader()) {
					@Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
						if (name.startsWith("oracle.")) throw original;
						return super.loadClass(name, resolve);
					}
				});
				InvocationTargetException invocation = assertThrows(InvocationTargetException.class,
						() -> create.invoke(null, opaque, Duration.ZERO));
				assertInstanceOf(SQLException.class, invocation.getTargetException());
				assertSame(original, invocation.getTargetException().getCause());
				assertSame(cause, invocation.getTargetException().getCause().getCause());
				assertFalse(Thread.currentThread().isInterrupted(), "The boundary decides when retained interruption is published");
			}
		} finally { Thread.currentThread().setContextClassLoader(previous); }
	}

	@Test
	void parentDriverDoesNotRetainChildBridgeAndParentBridgeDoesNotRetainChildDriver() throws Exception {
		Retention bridge = installChildBridge();
		Retention driver = installChildDriver();
		System.gc();
		assertNotNull(bridge.loader.get()); assertNotNull(driver.loader.get());
		assertNotNull(bridge.strong); assertNotNull(driver.strong);
		bridge.strong = null; driver.strong = null;
		for (int attempt = 0; attempt < 60 && (bridge.loader.get() != null || driver.loader.get() != null); attempt++) {
			System.gc(); Thread.sleep(25);
		}
		assertNull(bridge.loader.get(), "A parent native-class key must not retain the child bridge through its cache value");
		assertNull(driver.loader.get(), "A parent bridge's ClassValue must not retain disposable driver classes");
	}

	private static Retention installChildBridge() throws Exception {
		URLClassLoader isolated = bridgeLoader();
		Method create = createMethod(isolated);
		create.invoke(null, driver(OracleIntervalTestDriver.sharedLoader()), Duration.ofNanos(-1));
		isolated.close();
		return new Retention(new WeakReference<>(isolated), create);
	}

	private static Retention installChildDriver() throws Exception {
		var loader = OracleIntervalTestDriver.newLoader();
		Object value = OracleDriverSupport.newIntervalDaySecond(driver(loader), Duration.ofNanos(1));
		loader.close();
		return new Retention(new WeakReference<>(loader), value);
	}

	private static URLClassLoader bridgeLoader() {
		return new URLClassLoader(new URL[]{OracleDriverSupport.class.getProtectionDomain().getCodeSource().getLocation()},
				ClassLoader.getPlatformClassLoader());
	}
	private static Method createMethod(ClassLoader loader) throws Exception {
		Method result = loader.loadClass(OracleDriverSupport.class.getName()).getDeclaredMethod("newIntervalDaySecond", Connection.class, Duration.class);
		result.setAccessible(true); return result;
	}
	private static Connection driver(ClassLoader loader) throws ClassNotFoundException {
		Class<?> api = loader.loadClass("oracle.jdbc.OracleConnection");
		return driver(loader, api);
	}
	private static Connection driver(ClassLoader loader, Class<?> api) {
		return (Connection) Proxy.newProxyInstance(loader, new Class<?>[]{Connection.class, api},
				(proxy, method, args) -> switch (method.getName()) {
					case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
					case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
					default -> throw new AssertionError(method);
				});
	}
	private static final class Retention {
		final WeakReference<ClassLoader> loader;
		Object strong;
		Retention(WeakReference<ClassLoader> loader, Object strong) { this.loader = loader; this.strong = strong; }
	}
}
