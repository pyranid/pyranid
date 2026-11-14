/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ResourceLock("java.util.TimeZone.default")
abstract class MySqlFamilyTemporalIntegrationSupport {
	protected abstract String jdbcUrl();
	protected abstract String username();
	protected abstract String password();
	protected abstract DatabaseType databaseType();
	protected abstract List<String> prepareModes();

	@Test
	void ambiguousInstantValuesUseDatabaseWallTimeAcrossPrepareModesAndRepeatedUpdates() throws Exception {
		TimeZone previous = TimeZone.getDefault();
		try {
			TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
			assertNotEquals(ZoneOffset.UTC, TimeZone.getDefault().toZoneId());
			for (String mode : prepareModes()) {
				for (boolean missingMetadata : new boolean[]{false, true}) {
					DataSource source = source(mode, missingMetadata);
					for (AmbiguousTimestampBindingStrategy strategy : AmbiguousTimestampBindingStrategy.values()) {
						Database database = Database.with(source, databaseType()).timeZone(ZoneOffset.UTC)
								.ambiguousTimestampBindingStrategy(strategy).build();
						String table = "temporal_" + UUID.randomUUID().toString().replace("-", "");
						database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, wall DATETIME(6), moment TIMESTAMP(6))").execute();
						try {
							Instant instant = Instant.parse("2024-07-01T12:34:56.123Z");
							List<Object> values = List.of(instant, instant.atOffset(ZoneOffset.ofHours(5)),
									Date.from(instant), Timestamp.from(instant));
							for (int id = 1; id <= values.size(); id++)
								database.query("INSERT INTO " + table + " VALUES (:id, :v, :v)").bind("id", id).bind("v", values.get(id - 1)).execute();
							assertEquals(List.of(instant, instant, instant, instant), database.query("SELECT wall FROM " + table + " ORDER BY id").fetchList(Instant.class), mode);
							assertEquals(List.of(instant, instant, instant, instant), database.query("SELECT moment FROM " + table + " ORDER BY id").fetchList(Instant.class), mode);
							assertEquals(List.of(LocalDateTime.ofInstant(instant, ZoneOffset.UTC), LocalDateTime.ofInstant(instant, ZoneOffset.UTC),
									LocalDateTime.ofInstant(instant, ZoneOffset.UTC), LocalDateTime.ofInstant(instant, ZoneOffset.UTC)),
									database.query("SELECT wall FROM " + table + " ORDER BY id").fetchList(LocalDateTime.class));
							assertEquals(4L, database.query("SELECT COUNT(*) FROM " + table + " WHERE wall=:v AND moment=:v")
									.bind("v", instant).fetchObject(Long.class).orElseThrow());
							assertEquals(4L, database.query("SELECT COUNT(*) FROM " + table + " WHERE wall>=:lo AND wall<:hi")
									.bind("lo", instant.minusMillis(1)).bind("hi", instant.plusMillis(1)).fetchObject(Long.class).orElseThrow());
							for (int repeat = 0; repeat < 3; repeat++) {
								Timestamp read = database.query("SELECT wall FROM " + table + " WHERE id=4").fetchObject(Timestamp.class).orElseThrow();
								assertEquals(instant, read.toInstant());
								assertEquals(1L, database.query("UPDATE " + table + " SET wall=:v, moment=:v WHERE id=4 AND wall=:v")
										.bind("v", read).execute());
							}
							// A future value must not become expired merely because the driver/JVM zone differs.
							LocalDateTime now = database.query("SELECT UTC_TIMESTAMP(6)").fetchObject(LocalDateTime.class).orElseThrow();
							Instant future = now.toInstant(ZoneOffset.UTC).plusSeconds(1800);
							database.query("INSERT INTO " + table + " VALUES (5, :future, :future)").bind("future", future).execute();
							assertEquals(0L, database.query("DELETE FROM " + table + " WHERE id=5 AND wall<=UTC_TIMESTAMP(6)").execute());
						} finally {
							database.query("DROP TABLE " + table).execute();
						}
					}
				}
			}
		} finally {
			TimeZone.setDefault(previous);
		}
	}

	private DataSource source(String options, boolean missingMetadata) {
		String url = jdbcUrl() + (jdbcUrl().contains("?") ? "&" : "?") + options;
		DataSource delegate = new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(url, username(), password());
		return DataSource.class.cast(Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
			Object value = invoke(delegate, method, args);
			if (!(value instanceof Connection connection)) return value;
			try (Statement settings = connection.createStatement()) { settings.execute("SET time_zone = '+00:00'"); }
			if (!missingMetadata) return connection;
			return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (wrapped, jdbc, parameters) -> {
				Object result = invoke(connection, jdbc, parameters);
				if (!(result instanceof PreparedStatement statement)) return result;
				return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class}, (prepared, operation, arguments) -> {
					if (operation.getName().equals("getParameterMetaData")) throw new SQLFeatureNotSupportedException("metadata disabled by pool");
					return invoke(statement, operation, arguments);
				});
			});
		}));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
