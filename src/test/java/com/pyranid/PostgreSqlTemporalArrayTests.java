/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class PostgreSqlTemporalArrayTests {
	@Test
	void publicArrayBindingUsesConfiguredWallTimeForAllNormalizedInstantTypes() throws Exception {
		Instant instant = Instant.parse("2024-07-01T12:34:56.123456Z");
		OffsetDateTime offset = OffsetDateTime.parse("2024-07-01T18:19:56.123456+05:45");
		for (ZoneId zone : List.of(ZoneId.of("UTC"), ZoneId.of("America/New_York"), ZoneId.of("Asia/Kathmandu"))) {
			for (String type : List.of("timestamp", "TIMESTAMP", "pg_catalog.timestamp", "\"pg_catalog\".\"timestamp\"")) {
				AtomicReference<Object[]> captured = new AtomicReference<>();
				AtomicInteger frees = new AtomicInteger();
				Array array = proxy(Array.class, (method, arguments) -> {
					if (method.equals("free")) { frees.incrementAndGet(); return null; }
					throw new AssertionError(method);
				});
				Connection connection = proxy(Connection.class, (method, arguments) -> {
					assertEquals("createArrayOf", method);
					assertEquals(type, arguments[0]);
					captured.set((Object[]) arguments[1]);
					return array;
				});
				PreparedStatement statement = proxy(PreparedStatement.class, (method, arguments) -> {
					if (method.equals("getConnection")) return connection;
					assertEquals("setArray", method);
					assertSame(array, arguments[1]);
					return null;
				});
				JDBCDataSource source = new JDBCDataSource();
				source.setUrl("jdbc:hsqldb:mem:array_context");
				Database database = Database.with(source, DatabaseType.POSTGRESQL).timeZone(zone).build();
				StatementContext<?> context = StatementContext.with(Statement.of("array-temporal", "SELECT ?"), database).build();
				Object[] values = {instant, offset, Timestamp.from(instant), Date.from(instant),
						instant.atZone(ZoneId.of("Asia/Kathmandu")), Parameters.secure(Optional.of(instant)), null};
				PreparedStatementBinder.withDefaultConfiguration().bindParameter(context, statement, 1, Parameters.sqlArrayOf(type, values));
				LocalDateTime expected = LocalDateTime.ofInstant(instant, zone);
				for (int index : new int[]{0, 1, 2, 4, 5}) assertEquals(expected, captured.get()[index], type);
				assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(instant.toEpochMilli()), zone), captured.get()[3]);
				assertNull(captured.get()[6]);
				assertSame(instant, values[0]);
				assertEquals(0, frees.get());
				AutoCloseable cleanup;
				while ((cleanup = context.getCleanupOperations().poll()) != null) cleanup.close();
				assertEquals(1, frees.get());
			}
		}
	}

	@Test
	void zonedNonTemporalAndUnknownNamedTypesKeepTheirExistingRepresentation() {
		ZoneId zone = ZoneId.of("America/New_York");
		Instant instant = Instant.parse("2024-07-01T12:34:56.123456789Z");
		OffsetDateTime offset = instant.atZone(ZoneId.of("Asia/Kathmandu")).toOffsetDateTime();
		for (String type : List.of("timestamptz", "TIMESTAMP WITH TIME ZONE", "pg_catalog.timestamptz", "text",
				"timestamp_domain", "custom.timestamp", "\"TIMESTAMP\"", "\"PG_CATALOG\".timestamp")) {
			assertSame(instant, PostgresDialect.INSTANCE.normalizeSqlArrayElement(type, instant, zone), type);
			assertSame(offset, PostgresDialect.INSTANCE.normalizeSqlArrayElement(type, offset, zone), type);
		}
		LocalDateTime wall = LocalDateTime.parse("2024-07-01T12:34:56.123456789");
		assertSame(wall, PostgresDialect.INSTANCE.normalizeSqlArrayElement("timestamp", wall, zone));
	}

	@FunctionalInterface private interface Call { Object invoke(String method, Object[] arguments) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(instance, method, arguments) -> call.invoke(method.getName(), arguments)));
	}
}
