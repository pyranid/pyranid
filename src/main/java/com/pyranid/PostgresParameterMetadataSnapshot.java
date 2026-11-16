/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.jspecify.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.InvocationTargetException;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Arrays;

/** Detaches the native driver's mutable parameter OIDs without unwrapping application metadata. */
final class PostgresParameterMetadataSnapshot {
	// Cache only JDK reflection objects, keyed by the actual native class. No connection, metadata,
	// throwable or bridge-owned holder is retained when a disposable driver or bridge loader ends.
	private static final ClassValue<Object[]> ACCESSORS = new ClassValue<>() {
		@Override protected Object[] computeValue(Class<?> type) {
			try {
				Field connection = type.getDeclaredField("connection"), oids = type.getDeclaredField("oids");
				String root = type.getName().startsWith("org.postgresql.") ? "org.postgresql" : "com.yugabyte";
				if (oids.getType() != int[].class || !connection.getType().getName().equals(root + ".core.BaseConnection"))
					return new Object[0];
				Constructor<?> constructor = type.getConstructor(connection.getType(), int[].class);
				if (!connection.trySetAccessible() || !oids.trySetAccessible()) return new Object[0];
				return new Object[]{connection, oids, constructor};
			} catch (ReflectiveOperationException | SecurityException | InaccessibleObjectException unavailable) {
				if (JdbcFallbacks.isTerminal(unavailable)) throw new ReflectionFailure(unavailable);
				return new Object[0];
			}
		}
	};

	private PostgresParameterMetadataSnapshot() {}

	record Snapshot(ParameterMetaData metadata, int[] builtinIndices) {}

	static @Nullable Snapshot freeze(ParameterMetaData metadata) throws SQLException {
		Class<?> type = metadata.getClass();
		String name = type.getName();
		if (!name.equals("org.postgresql.jdbc.PgParameterMetaData") && !name.equals("com.yugabyte.jdbc.PgParameterMetaData"))
			return null;
		try {
			Object[] accessors = ACCESSORS.get(type);
			if (accessors.length == 0) return null;
			Object connection = ((Field) accessors[0]).get(metadata), rawOids = ((Field) accessors[1]).get(metadata);
			if (connection == null || !(rawOids instanceof int[])) return null;
			int[] oids = ((int[]) rawOids).clone();
			ParameterMetaData detached = (ParameterMetaData) ((Constructor<?>) accessors[2]).newInstance(connection, oids);
			String root = name.startsWith("org.postgresql.") ? "org.postgresql" : "com.yugabyte";
			boolean nativeConnection = connection.getClass().getName().equals(root + ".jdbc.PgConnection")
					&& connection.getClass().getClassLoader() == type.getClassLoader();
			int[] builtinIndices = new int[oids.length];
			int count = 0;
			if (nativeConnection)
				for (int index = 0; index < oids.length; index++)
					if (locallyCachedBuiltin(oids[index], root.equals("org.postgresql"))) builtinIndices[count++] = index + 1;
			return new Snapshot(detached, Arrays.copyOf(builtinIndices, count));
		} catch (InvocationTargetException invocation) {
			rethrow(invocation.getTargetException());
			throw new AssertionError("Unreachable");
		} catch (ReflectionFailure unavailable) {
			rethrow(unavailable.getCause());
			throw new AssertionError("Unreachable");
		} catch (ReflectiveOperationException | SecurityException | InaccessibleObjectException unavailable) {
			if (JdbcFallbacks.isTerminal(unavailable)) rethrow(unavailable);
			return null;
		}
	}

	private static boolean locallyCachedBuiltin(int oid, boolean postgres) {
		// These scalar OIDs are installed locally by both drivers' TypeInfoCache constructors.
		// In particular, 0/unknown, domains, interval, UUID and extension types are not assumed cached.
		return switch (oid) {
			case 16, 17, 18, 19, 20, 21, 23, 25, 26, 114, 700, 701, 790, 1042, 1043,
					1082, 1083, 1114, 1184, 1266, 1560, 1562, 1700, 1790 -> true;
			case 3802 -> postgres; // The supported Yugabyte fork does not preinstall jsonb.
			default -> false;
		};
	}

	private static void rethrow(Throwable failure) throws SQLException {
		if (failure instanceof SQLException sqlFailure) throw sqlFailure;
		if (failure instanceof RuntimeException runtimeFailure) throw runtimeFailure;
		if (failure instanceof Error error) throw error;
		throw new SQLException("Unable to snapshot native PostgreSQL parameter metadata", failure);
	}

	private static final class ReflectionFailure extends RuntimeException {
		private ReflectionFailure(Throwable cause) { super(cause); }
	}
}
