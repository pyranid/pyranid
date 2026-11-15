/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.jspecify.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Optional-driver bridge for Oracle's exact native DAY TO SECOND parameter representation. */
final class OracleDriverSupport {
	private static final String CONNECTION_API = "oracle.jdbc.OracleConnection";
	private static final String INTERVAL_CLASS = "oracle.sql.INTERVALDS";
	private static final BigInteger NANOS_PER_SECOND = BigInteger.valueOf(1_000_000_000L);
	private static final BigInteger NANOS_PER_DAY = NANOS_PER_SECOND.multiply(BigInteger.valueOf(86_400));
	private static final BigInteger MAX_DAYS = BigInteger.valueOf(999_999_999);
	// A JDK constructor keyed by the actual native value class does not retain this bridge's loader
	// when a parent driver outlives it. Neither a library holder nor a Throwable is stored as a value.
	private static final ClassValue<Constructor<?>> DAY_SECOND_FACTORIES = new ClassValue<>() {
		@Override protected @Nullable Constructor<?> computeValue(Class<?> type) {
			try { return type.getConstructor(String.class); }
			catch (NoSuchMethodException unavailable) { return null; }
		}
	};

	private OracleDriverSupport() {}

	static Object newIntervalDaySecond(Connection connection, Duration time) throws SQLException {
		String text = formatDaySecond(time);
		Class<?> type = resolveIntervalClass(connection);
		Constructor<?> constructor = type == null ? null : DAY_SECOND_FACTORIES.get(type);
		if (constructor == null)
			throw new SQLException("Unable to unambiguously locate the owning Oracle driver's INTERVALDS(String) constructor. "
					+ "SqlInterval DAY TO SECOND binding requires a supported Oracle driver or a CustomParameterBinder.");
		try {
			return constructor.newInstance(text);
		} catch (InvocationTargetException invocation) {
			Throwable original = invocation.getTargetException();
			if (original instanceof SQLException sqlFailure) throw sqlFailure;
			if (original instanceof RuntimeException runtimeFailure) throw runtimeFailure;
			if (original instanceof Error error) throw error;
			throw new SQLException("Unable to construct Oracle INTERVALDS", original);
		} catch (ReflectiveOperationException failure) {
			throw new SQLException("Unable to construct Oracle INTERVALDS", failure);
		}
	}

	static String formatDaySecond(Duration time) {
		BigInteger total = BigInteger.valueOf(time.getSeconds()).multiply(NANOS_PER_SECOND)
				.add(BigInteger.valueOf(time.getNano()));
		BigInteger[] parts = total.abs().divideAndRemainder(NANOS_PER_DAY);
		if (parts[0].compareTo(MAX_DAYS) > 0)
			throw new DateTimeException("Oracle DAY TO SECOND intervals cannot retain more than nine leading day digits");
		long remaining = parts[1].longValueExact();
		long seconds = remaining / 1_000_000_000L, nanos = remaining % 1_000_000_000L;
		return String.format(Locale.ROOT, "%s%s %02d:%02d:%02d.%09d", total.signum() < 0 ? "-" : "+", parts[0],
				seconds / 3600, seconds / 60 % 60, seconds % 60, nanos);
	}

	private static @Nullable Class<?> resolveIntervalClass(Connection original) throws SQLException {
		// The generic JDBC unwrap can reveal a driver loader hidden by a pool's loader, without
		// assuming a pool implementation or caching what a shared proxy class happens to wrap today.
		Connection connection = original;
		try {
			Connection unwrapped = original.unwrap(Connection.class);
			if (unwrapped != null) connection = unwrapped;
		} catch (SQLException | RuntimeException | AbstractMethodError unavailable) {
			if (JdbcFallbacks.isTerminal(unavailable)) rethrow(unavailable);
		}
		List<ClassLoader> loaders = candidateLoaders(original, connection);
		for (ClassLoader loader : loaders) {
			Class<?> api = load(CONNECTION_API, loader);
			if (api == null || !api.isInterface()) continue;
			Object actual;
			if (api.isInstance(connection)) actual = connection;
			else {
				boolean wraps;
				try { wraps = connection.isWrapperFor(api); }
				catch (SQLException | RuntimeException | AbstractMethodError unavailable) {
					if (JdbcFallbacks.isTerminal(unavailable)) rethrow(unavailable);
					continue;
				}
				if (!wraps) continue;
				// A positive wrapper claim establishes ownership even if its unwrap fails. Preserve
				// that failure instead of consulting a foreign visible driver's fallback factory.
				actual = connection.unwrap(api);
			}
			if (actual == null || !api.isInstance(actual))
				throw new SQLException("The connection did not unwrap to its claimed Oracle connection API");
			Class<?> actualValue = load(INTERVAL_CLASS, actual.getClass().getClassLoader());
			Class<?> apiValue = load(INTERVAL_CLASS, api.getClassLoader());
			if (actualValue != null && apiValue != null && actualValue != apiValue)
				throw new SQLException("The owning Oracle connection and API resolve conflicting INTERVALDS classes");
			Class<?> value = actualValue != null ? actualValue : apiValue;
			// Once ownership is established, a missing native class/constructor must not send us to
			// the opaque-wrapper fallback and another driver's factory from an unrelated loader.
			return value != null && DAY_SECOND_FACTORIES.get(value) != null ? value : null;
		}
		// Opaque wrappers are usable only when a single usable native class is visible. Different
		// driver copies may coexist: choosing by the pool class or the first loader would be unsafe.
		List<Class<?>> visible = new ArrayList<>();
		for (ClassLoader loader : loaders) {
			Class<?> value = load(INTERVAL_CLASS, loader);
			if (value != null && DAY_SECOND_FACTORIES.get(value) != null && !visible.contains(value)) visible.add(value);
		}
		return visible.size() == 1 ? visible.get(0) : null;
	}

	private static List<ClassLoader> candidateLoaders(Connection original, Connection actual) {
		List<ClassLoader> result = new ArrayList<>(4);
		ClassLoader context;
		try { context = Thread.currentThread().getContextClassLoader(); }
		catch (SecurityException unavailable) { context = null; }
		for (ClassLoader loader : new ClassLoader[]{actual.getClass().getClassLoader(), original.getClass().getClassLoader(),
				OracleDriverSupport.class.getClassLoader(), context})
			if (loader != null && result.stream().noneMatch(existing -> existing == loader)) result.add(loader);
		return result;
	}

	private static @Nullable Class<?> load(String name, @Nullable ClassLoader loader) throws SQLException {
		try { return Class.forName(name, false, loader); }
		catch (ClassNotFoundException unavailable) {
			// Class absence is optional; cancellation/fatal evidence carried by a foreign loader is not.
			if (unavailable.getCause() != null && JdbcFallbacks.isTerminal(unavailable))
				throw new SQLException("Unable to resolve the Oracle interval driver class", unavailable);
			return null;
		}
	}

	private static void rethrow(Throwable failure) throws SQLException {
		if (failure instanceof SQLException sqlFailure) throw sqlFailure;
		if (failure instanceof RuntimeException runtimeFailure) throw runtimeFailure;
		if (failure instanceof Error error) throw error;
		throw new SQLException("Unable to identify Oracle driver", failure);
	}
}
