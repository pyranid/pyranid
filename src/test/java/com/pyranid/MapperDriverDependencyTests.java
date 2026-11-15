/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at
 * https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.PGStatement;

import javax.sql.DataSource;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.net.URLClassLoader;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class MapperDriverDependencyTests {
	private static final String DRIVER_CLASS = "org/postgresql/PGStatement";

	@Test
	void mapperHasNoPostgresDriverClassConstantAndTheDetectorDistinguishesStrings() throws Exception {
		assertTrue(classReferences(DriverClassReference.class).contains(DRIVER_CLASS), "Positive control must detect a class edge");
		assertFalse(classReferences(DriverNameOnly.class).contains(DRIVER_CLASS), "A string is not a class dependency");
		assertFalse(classReferences(DefaultResultSetMapper.class).contains(DRIVER_CLASS),
				"Compile-time inlining can still leave a class reference visible to dependency tooling");
	}

	@Test
	void localInfinityLiteralsMatchTheActualOptionalDriversFields() throws Exception {
		for (String direction : new String[]{"POSITIVE", "NEGATIVE"}) {
			var local = DefaultResultSetMapper.class.getDeclaredField("POSTGRES_DATE_" + direction + "_INFINITY");
			local.setAccessible(true);
			long driver = PGStatement.class.getField("DATE_" + direction + "_INFINITY").getLong(null);
			assertEquals(driver, local.getLong(null));
		}
	}

	@Test
	void coreAlonePreservesInfinitySentinelsAndLegacyRangeGuardsWithoutAnyDriver() throws Exception {
		// Only production classes and platform modules are visible: no driver, annotation or test JARs.
		try (URLClassLoader isolated = new URLClassLoader(new java.net.URL[]{
				Database.class.getProtectionDomain().getCodeSource().getLocation()}, ClassLoader.getPlatformClassLoader())) {
			ClassLoader previous = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(isolated);
			try {
				assertThrows(ClassNotFoundException.class, () -> isolated.loadClass("org.postgresql.PGStatement"));
				assertThrows(ClassNotFoundException.class, () -> isolated.loadClass("org.jspecify.annotations.NonNull"));
				Class<?> mapperApi = isolated.loadClass("com.pyranid.ResultSetMapper");
				Object mapper = mapperApi.getMethod("withDefaultConfiguration").invoke(null);
				Object context = postgresContext(isolated);
				Class<?> contextType = isolated.loadClass("com.pyranid.StatementContext");
				Class<?> targetType = isolated.loadClass("com.pyranid.TargetType");
				Method target = targetType.getMethod("of", Type.class);
				Method convert = mapper.getClass().getDeclaredMethod("convertResultSetValueToTargetType", contextType, Object.class, targetType);
				Method normalize = mapper.getClass().getDeclaredMethod("normalizeContainerTemporalValue", contextType, Object.class, int.class);
				convert.setAccessible(true);
				normalize.setAccessible(true);
				for (boolean positive : new boolean[]{false, true}) {
					long sentinel = PGStatement.class.getField(positive ? "DATE_POSITIVE_INFINITY" : "DATE_NEGATIVE_INFINITY").getLong(null);
					for (int jdbcType : new int[]{Types.DATE, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE}) {
						Date raw = jdbcType == Types.DATE ? new java.sql.Date(sentinel) : new Timestamp(sentinel);
						Object expected = switch (jdbcType) {
							case Types.DATE -> positive ? LocalDate.MAX : LocalDate.MIN;
							case Types.TIMESTAMP -> positive ? LocalDateTime.MAX : LocalDateTime.MIN;
							default -> positive ? OffsetDateTime.MAX : OffsetDateTime.MIN;
						};
						assertEquals(expected, normalize.invoke(null, context, raw, jdbcType));
						for (Class<?> legacy : new Class<?>[]{Timestamp.class, java.sql.Date.class, Date.class}) {
							Object legacyTarget = target.invoke(null, legacy);
							InvocationTargetException failure = assertThrows(InvocationTargetException.class,
									() -> convert.invoke(mapper, context, raw, legacyTarget));
							assertEquals("com.pyranid.DatabaseException", failure.getCause().getClass().getName());
							assertTrue(failure.getCause().getMessage().contains("representable range"));
						}
						Optional<?> nativeValue = (Optional<?>) convert.invoke(mapper, context, raw, target.invoke(null, Object.class));
						assertSame(raw, nativeValue.orElseThrow());
					}
				}
			} finally {
				Thread.currentThread().setContextClassLoader(previous);
			}
		}
	}

	private static Object postgresContext(ClassLoader loader) throws Exception {
		DataSource unused = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> { throw new AssertionError("No JDBC driver should be consulted: " + method.getName()); });
		Class<?> databaseType = loader.loadClass("com.pyranid.DatabaseType");
		Class<?> databaseApi = loader.loadClass("com.pyranid.Database");
		Object builder = databaseApi.getMethod("with", DataSource.class, databaseType)
				.invoke(null, unused, databaseType.getField("POSTGRESQL").get(null));
		Object database = builder.getClass().getMethod("build").invoke(builder);
		Class<?> statementApi = loader.loadClass("com.pyranid.Statement");
		Object statement = statementApi.getMethod("of", Object.class, String.class).invoke(null, "driver-isolation", "SELECT value");
		Class<?> contextApi = loader.loadClass("com.pyranid.StatementContext");
		Object context = contextApi.getMethod("with", statementApi, databaseApi).invoke(null, statement, database);
		return context.getClass().getMethod("build").invoke(context);
	}

	/** Reads only CONSTANT_Class entries, so textual driver names do not become false dependencies. */
	private static Set<String> classReferences(Class<?> type) throws IOException {
		try (InputStream resource = type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
			assertNotNull(resource);
			DataInputStream input = new DataInputStream(resource);
			assertEquals(0xCAFEBABE, input.readInt());
			input.readUnsignedShort(); // minor version
			input.readUnsignedShort(); // major version
			int count = input.readUnsignedShort();
			String[] text = new String[count];
			Set<Integer> references = new HashSet<>();
			for (int index = 1; index < count; index++) {
				int tag = input.readUnsignedByte();
				switch (tag) {
					case 1 -> text[index] = input.readUTF();
					case 3, 4, 9, 10, 11, 12, 17, 18 -> input.skipNBytes(4);
					case 5, 6 -> { input.skipNBytes(8); index++; }
					case 7 -> references.add(input.readUnsignedShort());
					case 8, 16, 19, 20 -> input.skipNBytes(2);
					case 15 -> input.skipNBytes(3);
					default -> throw new IOException("Unknown constant-pool tag " + tag);
				}
			}
			Set<String> names = new HashSet<>();
			for (int index : references) names.add(text[index]);
			return names;
		}
	}

	private static final class DriverClassReference {
		static Class<?> type() { return PGStatement.class; }
	}

	private static final class DriverNameOnly {
		static String typeName() { return "org/postgresql/PGStatement"; }
	}
}
