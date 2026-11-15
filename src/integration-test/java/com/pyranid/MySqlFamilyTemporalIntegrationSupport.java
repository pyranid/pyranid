/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import javax.sql.DataSource;
import java.math.BigDecimal;
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
import java.time.ZoneId;
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

	@Test
	void configuredDatabaseZoneIsIndependentOfDriverConnectionZoneAndJvmDefault() throws Exception {
		TimeZone previous = TimeZone.getDefault();
		try {
			TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
			Instant instant = Instant.parse("2024-07-01T12:34:56.123456Z");
			BigDecimal epoch = BigDecimal.valueOf(instant.getEpochSecond()).add(BigDecimal.valueOf(instant.getNano(), 9));
			for (String mode : prepareModes()) {
				for (String driverZone : List.of("America/Los_Angeles", "UTC")) {
					String options = mode + "&connectionTimeZone=" + driverZone
							+ "&forceConnectionTimeZoneToSession=false&preserveInstants=true";
					DataSource source = source(options, false);
					Database defaultZone = Database.with(source, databaseType()).build();
					assertEquals(ZoneId.of("America/New_York"), defaultZone.getTimeZone(), "Pyranid defaults to the JVM zone, not connectionTimeZone");
					Database database = Database.with(source, databaseType()).timeZone(ZoneOffset.UTC).build();
					String table = "explicit_zone_" + UUID.randomUUID().toString().replace("-", "");
					database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, wall DATETIME(6), moment TIMESTAMP(6))").execute();
					try {
						List<Object> values = List.of(instant, instant.atOffset(ZoneOffset.ofHours(5)), Timestamp.from(instant));
						for (int id = 1; id <= values.size(); id++)
							database.query("INSERT INTO " + table + " VALUES (:id, :value, :value)")
									.bind("id", id).bind("value", values.get(id - 1)).execute();
						assertEquals(List.of(instant, instant, instant), database.query("SELECT moment FROM " + table + " ORDER BY id").fetchList(Instant.class), options);
						assertEquals(List.of(instant, instant, instant), database.query("SELECT wall FROM " + table + " ORDER BY id").fetchList(Timestamp.class)
								.stream().map(Timestamp::toInstant).toList(), options);
						assertEquals(List.of(LocalDateTime.ofInstant(instant, ZoneOffset.UTC), LocalDateTime.ofInstant(instant, ZoneOffset.UTC),
								LocalDateTime.ofInstant(instant, ZoneOffset.UTC)), database.query("SELECT wall FROM " + table + " ORDER BY id").fetchList(LocalDateTime.class));
						for (BigDecimal stored : database.query("SELECT UNIX_TIMESTAMP(moment) FROM " + table + " ORDER BY id").fetchList(BigDecimal.class))
							assertEquals(0, epoch.compareTo(stored), "The server's actual instant must match, not just a symmetric Java round trip: " + options);
						assertEquals(3L, database.query("SELECT COUNT(*) FROM " + table + " WHERE wall=:value AND moment=:value")
								.bind("value", instant).fetchObject(Long.class).orElseThrow());
						try (Connection connection = source.getConnection(); Statement statement = connection.createStatement();
								var rows = statement.executeQuery("SELECT @@session.time_zone, moment FROM " + table + " WHERE id=1")) {
							assertTrue(rows.next());
							assertEquals("+00:00", rows.getString(1), "The driver property must not silently change the server session in this control");
							// Raw JDBC follows its driver's zone contract, independently of Pyranid's conversion policy.
							assertEquals(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).atZone(ZoneId.of(driverZone)).toInstant(),
									rows.getTimestamp(2).toInstant(), options);
						}
						// Leaving Pyranid at its JVM-zone default can hide a server mismatch behind a symmetric Java round trip.
						defaultZone.query("INSERT INTO " + table + " VALUES (9, :value, :value)").bind("value", instant).execute();
						assertEquals(instant, defaultZone.query("SELECT moment FROM " + table + " WHERE id=9").fetchObject(Instant.class).orElseThrow());
						Instant serverValue = LocalDateTime.ofInstant(instant, ZoneId.of("America/New_York")).toInstant(ZoneOffset.UTC);
						assertNotEquals(instant, serverValue);
						BigDecimal serverEpoch = BigDecimal.valueOf(serverValue.getEpochSecond()).add(BigDecimal.valueOf(serverValue.getNano(), 9));
						assertEquals(0, serverEpoch.compareTo(database.query("SELECT UNIX_TIMESTAMP(moment) FROM " + table + " WHERE id=9")
								.fetchObject(BigDecimal.class).orElseThrow()), options);
					} finally {
						database.query("DROP TABLE " + table).execute();
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
