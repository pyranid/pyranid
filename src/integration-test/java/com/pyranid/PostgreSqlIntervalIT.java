/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PGInterval;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.pyranid.SqlIntervalIntegrationSupport.*;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlIntervalIT {
	@Container private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_intervals").withUsername("pyranid").withPassword("pyranid");
	public record FanOut(@DatabaseColumn("amount") SqlInterval typed, @DatabaseColumn("amount") Object nativeValue) {}

	@Test
	void typedReadsAndWritesPreserveStoredFieldsAcrossStylesTransferModesAndMappingPlans() {
		for (boolean binary : new boolean[]{false, true}) for (boolean cached : new boolean[]{false, true}) {
			Database database = database(source(binary), cached);
			String table = name(); List<SqlInterval> values = values();
			database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, amount INTERVAL)").execute();
			try {
				database.transaction(() -> database.query("INSERT INTO " + table + " VALUES (:id, :amount)")
						.batchChunkSize(2).executeBatch(groups(values)));
				List<byte[]> fields = database.query("SELECT interval_send(amount) FROM " + table + " WHERE amount IS NOT NULL ORDER BY id").fetchList(byte[].class);
				assertEquals(values.size(), fields.size());
				for (int index = 0; index < values.size(); index++) assertArrayEquals(postgresBinary(values.get(index)), fields.get(index));
				for (String style : List.of("postgres", "postgres_verbose", "sql_standard", "iso_8601")) database.transaction(() -> {
					database.query("SET LOCAL IntervalStyle = '" + style + "'").execute();
					verifyRows(database, table, values);
				});
			} finally { database.query("DROP TABLE " + table).execute(); }
		}
	}

	@Test
	void databaseCoercionsAndSqlEqualityDoNotMasqueradeAsExactFieldRoundTrips() {
		Database database = database(source(false), true); String table = name();
		database.query("CREATE TABLE " + table + " (ym INTERVAL YEAR TO MONTH, precise INTERVAL SECOND(3))").execute();
		try {
			database.query("INSERT INTO " + table + " VALUES (:ym, :time)")
					.bind("ym", SqlInterval.of(Period.of(1, 2, 3), Duration.ofHours(4)))
					.bind("time", SqlInterval.of(Duration.ofNanos(1_234_567_000))).execute();
			assertEquals(SqlInterval.of(Period.ofMonths(14)), database.query("SELECT ym FROM " + table).fetchObject(SqlInterval.class).orElseThrow());
			assertEquals(SqlInterval.of(Duration.ofMillis(1235)), database.query("SELECT precise FROM " + table).fetchObject(SqlInterval.class).orElseThrow());
			assertTrue(database.query("SELECT INTERVAL '1 day' = INTERVAL '24 hours'").fetchObject(Boolean.class).orElseThrow());
			assertNotEquals(database.query("SELECT INTERVAL '1 day'").fetchObject(SqlInterval.class).orElseThrow(),
					database.query("SELECT INTERVAL '24 hours'").fetchObject(SqlInterval.class).orElseThrow());
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void infinityAndAdapterLossesFailWhileFiniteSignedBoundariesRemainReadable() {
		for (boolean binary : new boolean[]{false, true}) {
			Database database = database(source(binary), true);
			for (String infinity : List.of("infinity", "-infinity"))
				assertThrows(DatabaseException.class, () -> database.query("SELECT INTERVAL '" + infinity + "'").fetchObject(SqlInterval.class));
			for (SqlInterval value : List.of(SqlInterval.of(Duration.ofNanos(1)), SqlInterval.of(Duration.ofSeconds(Long.MAX_VALUE)),
					SqlInterval.of(Integer.MAX_VALUE, Integer.MAX_VALUE, fromMicros(Long.MAX_VALUE)),
					SqlInterval.of(Integer.MIN_VALUE, Integer.MIN_VALUE, fromMicros(Long.MIN_VALUE))))
				assertThrows(DatabaseException.class, () -> database.query("SELECT :amount").bind("amount", value).fetchObject(SqlInterval.class));
			assertEquals(SqlInterval.of(fromMicros(Long.MIN_VALUE)), database.query("SELECT :amount")
					.bind("amount", SqlInterval.of(fromMicros(Long.MIN_VALUE))).fetchObject(SqlInterval.class).orElseThrow());
		}
	}

	@Test
	void mixedIntervalBatchRetainsOneProtectedMetadataSnapshotAcrossRowsAndChunks() {
		AtomicInteger descriptions = new AtomicInteger(), savepoints = new AtomicInteger(), releases = new AtomicInteger();
		DataSource counted = proxy(DataSource.class, source(false), (method, result) -> {
			if (method.equals("getConnection") && result instanceof Connection connection) {
				Connection[] handle = new Connection[1];
				handle[0] = proxy(Connection.class, connection, (connectionMethod, connectionResult) -> {
					if (connectionMethod.equals("setSavepoint")) savepoints.incrementAndGet();
					if (connectionMethod.equals("releaseSavepoint")) releases.incrementAndGet();
					if (connectionMethod.equals("prepareStatement") && connectionResult instanceof PreparedStatement statement)
						return proxy(PreparedStatement.class, statement, (statementMethod, statementResult) -> {
							if (statementMethod.equals("getConnection")) return handle[0];
							if (statementMethod.equals("getParameterMetaData")) descriptions.incrementAndGet();
							return statementResult;
						});
					return connectionResult;
				});
				return handle[0];
			}
			return result;
		});
		Database database = database(counted, true); String table = name();
		database.query("CREATE TABLE " + table + " (id INT, amount INTERVAL, nullable TIMESTAMPTZ, moment TIMESTAMPTZ, local_value TIMESTAMPTZ)").execute();
		Instant moment = Instant.parse("2026-07-01T12:34:56.123456Z");
		try {
			List<Map<String, Object>> groups = new ArrayList<>();
			for (int index = 0; index < 3; index++) {
				Map<String, Object> row = new HashMap<>(); row.put("id", index); row.put("amount", SqlInterval.of(Duration.ofHours(index)));
				row.put("nullable", null); row.put("moment", moment); row.put("local", LocalDateTime.ofInstant(moment, ZoneOffset.UTC)); groups.add(row);
			}
			database.transaction(() -> database.query("INSERT INTO " + table + " VALUES (:id, :amount, :nullable, :moment, :local)")
					.batchChunkSize(1).executeBatch(groups));
			assertEquals(1, descriptions.get()); assertEquals(1, savepoints.get()); assertEquals(1, releases.get());
			assertEquals(List.of(moment, moment, moment), database.query("SELECT moment FROM " + table + " ORDER BY id").fetchList(Instant.class));
			assertEquals(List.of(moment, moment, moment), database.query("SELECT local_value FROM " + table + " ORDER BY id").fetchList(Instant.class));
			for (int index = 0; index < 3; index++) assertArrayEquals(postgresBinary(SqlInterval.of(Duration.ofHours(index))), database.query(
					"SELECT interval_send(amount) FROM " + table + " WHERE id=:id").bind("id", index).fetchObject(byte[].class).orElseThrow());
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void calendarDayArithmeticRetainsTheDocumentedDstDifferenceFromJava() {
		Database database = database(source(false), true); ZoneId ny = ZoneId.of("America/New_York");
		database.transaction(() -> {
			database.query("SET LOCAL TIME ZONE 'America/New_York'").execute();
			ZonedDateTime start = ZonedDateTime.of(2026, 10, 31, 1, 30, 0, 0, ny);
			Instant sql = database.query("SELECT TIMESTAMPTZ '2026-10-31 01:30:00-04' + :amount")
					.bind("amount", SqlInterval.of(Period.ofDays(1))).fetchObject(Instant.class).orElseThrow();
			assertEquals(Instant.parse("2026-11-01T06:30:00Z"), sql);
			assertEquals(Instant.parse("2026-11-01T05:30:00Z"), start.plus(Period.ofDays(1)).toInstant());
			assertNotEquals(start.plus(Period.ofDays(1)).toInstant(), sql);
			SqlInterval difference = database.query("SELECT TIMESTAMPTZ '2026-03-10 12:00' - TIMESTAMPTZ '2026-03-07 09:00'")
					.fetchObject(SqlInterval.class).orElseThrow();
			assertEquals(SqlInterval.of(0, 3, Duration.ofHours(2)), difference);
			assertThrows(DateTimeException.class, difference::toDuration);
		});
	}

	@Test
	void customMappersReceiveActualDriverValuesAndFallbackRereadsFieldsAcrossStylesAndFanOut() {
		SqlInterval expected = SqlInterval.of(14, 3, Duration.ofSeconds(14_706, 789_000_000));
		SqlInterval replacement = SqlInterval.of(Duration.ofHours(7));
		String select = "SELECT INTERVAL '1 year 2 mons 3 days 04:05:06.789' AS amount";
		for (boolean cached : new boolean[]{false, true}) for (boolean handled : new boolean[]{false, true}) {
			AtomicInteger calls = new AtomicInteger(); AtomicReference<Object> raw = new AtomicReference<>();
			CustomColumnMapper hook = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(SqlInterval.class); }
				@Override public MappingResult map(StatementContext<?> context, java.sql.ResultSet rows, Object value, TargetType target,
						Integer index, String label, InstanceProvider provider) {
					assertInstanceOf(PGInterval.class, value); raw.set(value); calls.incrementAndGet();
					return handled ? MappingResult.of(replacement) : MappingResult.fallback();
				}
			};
			Database database = hookedDatabase(false, cached, hook);
			for (String style : List.of("postgres", "sql_standard")) database.transaction(() -> {
				database.query("SET LOCAL IntervalStyle = '" + style + "'").execute();
				calls.set(0);
				for (int repeat = 0; repeat < 2; repeat++) {
					FanOut fanOut = database.query(select).fetchObject(FanOut.class).orElseThrow();
					assertEquals(handled ? replacement : expected, fanOut.typed());
					assertInstanceOf(PGInterval.class, fanOut.nativeValue());
					assertSame(raw.get(), fanOut.nativeValue(), "Fan-out must share the actual raw object, not a typed replacement");
					assertEquals(handled ? replacement : expected, database.query(select).fetchObject(SqlInterval.class).orElseThrow());
					assertEquals(new FanOut(null, null), database.query("SELECT NULL::interval AS amount").fetchObject(FanOut.class).orElseThrow());
					assertTrue(database.query("SELECT NULL::interval AS amount").fetchObject(SqlInterval.class).isEmpty());
					assertEquals((repeat + 1) * 2, calls.get(), "SQL NULL must not invoke a custom mapper");
				}
			});
		}
	}

	@Test
	void forcedBinaryTypedAndFallbackReadsUseBytesWhileHooksRetainActualRawDriverObjects() throws Exception {
		String select = "SELECT INTERVAL '1 mon -3 days 04:05:06.789' AS amount";
		SqlInterval expected = SqlInterval.of(1, -3, Duration.ofSeconds(14_706, 789_000_000));
		for (boolean cached : new boolean[]{false, true}) {
			PGSimpleDataSource binarySource = source(true);
			try (Connection connection = binarySource.getConnection(); PreparedStatement statement = connection.prepareStatement(select);
					java.sql.ResultSet rows = statement.executeQuery()) {
				assertTrue(rows.next()); assertNull(rows.getString(1)); assertFalse(rows.wasNull());
				byte[] bytes = rows.getBytes(1); assertEquals(16, bytes.length); assertFalse(rows.wasNull());
				assertArrayEquals(postgresBinary(expected), bytes, "This control must really receive the interval wire format");
				Object raw = rows.getObject(1); assertFalse(rows.wasNull());
				PGInterval interval = assertInstanceOf(PGInterval.class, raw);
				assertTrue(interval.isNull()); assertNull(interval.getValue());
				// pgjdbc returns a non-null object with unavailable fields, not a null reference.
				// Printing that object's toString() alone would conceal the distinction.
			}
			Database direct = database(binarySource, cached);
			assertEquals(expected, direct.query(select).fetchObject(SqlInterval.class).orElseThrow());
			FanOut directFanOut = direct.query(select).fetchObject(FanOut.class).orElseThrow();
			assertEquals(expected, directFanOut.typed());
			assertTrue(assertInstanceOf(PGInterval.class, directFanOut.nativeValue()).isNull());
			AtomicInteger calls = new AtomicInteger(); AtomicReference<Object> raw = new AtomicReference<>();
			CustomColumnMapper hook = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(SqlInterval.class); }
				@Override public MappingResult map(StatementContext<?> context, java.sql.ResultSet rows, Object value, TargetType target,
						Integer index, String label, InstanceProvider provider) {
					PGInterval interval = assertInstanceOf(PGInterval.class, value);
					assertTrue(interval.isNull()); assertNull(interval.getValue());
					raw.set(value); calls.incrementAndGet(); return MappingResult.fallback();
				}
			};
			Database custom = hookedDatabase(true, cached, hook);
			assertEquals(expected, custom.query(select).fetchObject(SqlInterval.class).orElseThrow());
			FanOut customFanOut = custom.query(select).fetchObject(FanOut.class).orElseThrow();
			assertEquals(expected, customFanOut.typed()); assertSame(raw.get(), customFanOut.nativeValue());
			assertEquals(new FanOut(null, null), custom.query("SELECT NULL::interval AS amount").fetchObject(FanOut.class).orElseThrow());
			assertTrue(custom.query("SELECT NULL::interval AS amount").fetchObject(SqlInterval.class).isEmpty());
			assertEquals(2, calls.get(), "Non-null raw driver objects reach callbacks; true SQL NULL does not");
		}
	}

	private static PGSimpleDataSource source(boolean binary) {
		PGSimpleDataSource source = new PGSimpleDataSource();
		String url = POSTGRES.getJdbcUrl();
		source.setUrl(url + (url.contains("?") ? "&" : "?") + (binary ? "prepareThreshold=-1&binaryTransferEnable=interval" : "prepareThreshold=0"));
		source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword()); return source;
	}
	private static Database database(DataSource source, boolean cached) {
		return Database.with(source, DatabaseType.POSTGRESQL).timeZone(ZoneOffset.UTC)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
	private static Database hookedDatabase(boolean binary, boolean cached, CustomColumnMapper mapper) {
		return Database.with(source(binary), DatabaseType.POSTGRESQL).timeZone(ZoneOffset.UTC)
				.resultSetMapper(ResultSetMapper.withCustomColumnMappers(List.of(mapper)).planCachingEnabled(cached).build()).build();
	}
	private static String name() { return "interval_" + UUID.randomUUID().toString().replace("-", ""); }
	private interface ResultObserver { Object observe(String method, Object result); }
	private static <T> T proxy(Class<T> type, T delegate, ResultObserver observer) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (instance, method, arguments) -> {
			try { return observer.observe(method.getName(), method.invoke(delegate, arguments)); }
			catch (InvocationTargetException failure) { throw failure.getCause(); }
		}));
	}
}
