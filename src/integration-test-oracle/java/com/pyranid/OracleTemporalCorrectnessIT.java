/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class OracleTemporalCorrectnessIT {
	@Container
	private static final OracleContainer ORACLE = new OracleContainer(DockerImageName.parse(
			System.getProperty("oracle.integration.image", "gvenzl/oracle-free:23-slim-faststart"))
			.asCompatibleSubstituteFor("gvenzl/oracle-free")).withUsername("pyranid").withPassword("pyranid");

	@Test
	void nativeLocalTimeKeepsNanosecondsAndWallClockThroughTimestampColumnsAndPredicates() {
		Database database = database();
		String table = "time_correctness_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, time_value TIMESTAMP(9), writes NUMBER(10) NOT NULL)").execute();
		try {
			int id = 0;
			for (LocalTime value : List.of(LocalTime.MIDNIGHT, LocalTime.of(10, 11, 12, 123456789),
					LocalTime.of(23, 59, 59, 998500000), LocalTime.MAX)) {
				id++;
				database.query("INSERT INTO " + table + " VALUES (:id, :value, 0)").bind("id", id).bind("value", value).execute();
				assertEquals(value, LocalTime.parse(database.query("SELECT TO_CHAR(time_value, 'HH24:MI:SS.FF9') FROM " + table + " WHERE id = :id")
						.bind("id", id).fetchObject(String.class).orElseThrow()));
				assertEquals(value, database.query("SELECT time_value FROM " + table + " WHERE id = :id")
						.bind("id", id).fetchObject(LocalTime.class).orElseThrow());
				assertEquals(1L, database.query("SELECT COUNT(*) FROM " + table + " WHERE id = :id AND time_value = :value")
						.bind("id", id).bind("value", value).fetchObject(Long.class).orElseThrow());
				assertEquals(1L, database.query("UPDATE " + table + " SET writes = writes + 1 WHERE id = :id AND time_value = :value")
						.bind("id", id).bind("value", value).execute());
			}
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	@Test
	void nativeLocalTimeIntoOracleDateRetainsItsClockAtTheColumnsSecondPrecision() {
		Database database = database();
		String table = "date_time_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (time_value DATE)").execute();
		try {
			LocalTime value = LocalTime.of(10, 11, 12, 123456789);
			database.query("INSERT INTO " + table + " VALUES (:value)").bind("value", value).execute();
			assertEquals("10:11:12", database.query("SELECT TO_CHAR(time_value, 'HH24:MI:SS') FROM " + table).fetchObject(String.class).orElseThrow());
			assertEquals(value.withNano(0), database.query("SELECT time_value FROM " + table).fetchObject(LocalTime.class).orElseThrow());
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	@Test
	void valuesAndExplicitCastShapesPreserveConfiguredLocalFieldsAndActualInstants() {
		for (ZoneId zone : List.of(ZoneId.of("UTC"), ZoneId.of("Asia/Tokyo"))) {
			Database database = database(zone); String table = tableName();
			database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, plain_value TIMESTAMP(9), zoned_value TIMESTAMP(9) WITH TIME ZONE)").execute();
			try {
				database.transaction(() -> {
					List<String> inserts = List.of(
							"INSERT INTO " + table + " VALUES (:id, :plain, :zoned)",
							"INSERT INTO " + table + " SELECT :id, CAST(:plain AS TIMESTAMP(9)), CAST(:zoned AS TIMESTAMP(9) WITH TIME ZONE) FROM dual",
							"MERGE INTO " + table + " t USING (SELECT :id id, CAST(:plain AS TIMESTAMP(9)) plain_value, "
									+ "CAST(:zoned AS TIMESTAMP(9) WITH TIME ZONE) zoned_value FROM dual) s ON (t.id=s.id) "
									+ "WHEN NOT MATCHED THEN INSERT (id, plain_value, zoned_value) VALUES (s.id,s.plain_value,s.zoned_value)");
					int id = 0;
					for (String insert : inserts) {
						for (Object value : temporalValues()) {
							int row = ++id;
							assertEquals(1L, database.query(insert).bind("id", row).bind("plain", value).bind("zoned", value).execute());
							String label = zone + ", " + value.getClass().getName() + ", " + insert;
							assertEquals(render(localValue(value, zone)), database.query("SELECT TO_CHAR(plain_value, 'YYYY-MM-DD HH24:MI:SS.FF9') FROM "
									+ table + " WHERE id=:id").bind("id", row).fetchObject(String.class).orElseThrow(), label);
							assertEquals(render(LocalDateTime.ofInstant(instantValue(value, zone), ZoneOffset.UTC)), database.query(
									"SELECT TO_CHAR(SYS_EXTRACT_UTC(zoned_value), 'YYYY-MM-DD HH24:MI:SS.FF9') FROM " + table + " WHERE id=:id")
									.bind("id", row).fetchObject(String.class).orElseThrow(), label);
						}
					}
				});
			} finally { database.query("DROP TABLE " + table).execute(); }
		}
	}

	@Test
	void parserUnavailableTemporalTargetsFailBeforeWritingEvenAfterAnEarlierNullCachedTheFallback() {
		Database database = database(ZoneId.of("UTC")); String table = tableName();
		database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, note VARCHAR2(30), plain_value TIMESTAMP(9))").execute();
		String insert = "INSERT INTO " + table + " (id,note,plain_value) SELECT :id,:note,:value FROM dual";
		try {
			database.transaction(() -> {
				database.query("INSERT INTO " + table + " (id,note) VALUES (0,'before')").execute();
				int id = 0;
				for (boolean earlierNull : new boolean[]{false, true}) {
					for (Object value : temporalValues()) {
						int row = ++id;
						DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query(insert)
								.bind("id", row).bind("note", earlierNull ? null : "nonnull").bind("value", value).execute(),
								value.getClass().getName() + ", earlierNull=" + earlierNull);
						assertTrue(failure.getMessage().contains("Oracle") && failure.getMessage().contains("temporal"), failure.getMessage());
						Throwable cause = failure;
						while (cause.getCause() != null) cause = cause.getCause();
						assertEquals(NullPointerException.class, cause.getClass(), "The parser failure remains the refusal cause");
					}
				}
				database.query("INSERT INTO " + table + " (id,note) VALUES (99,'after')").execute();
			});
			assertEquals(List.of(0, 99), database.query("SELECT id FROM " + table + " ORDER BY id").fetchList(Integer.class),
					"Handled metadata refusals must not discard surviving application work or write shifted temporal values");
			for (Integer chunk : java.util.Arrays.asList(null, 1, 2)) {
				List<Map<String, Object>> groups = List.of(Map.of("id", 20, "note", "first NULL", "value", Optional.empty()),
						Map.of("id", 21, "note", "then instant", "value", Instant.parse("2026-01-01T05:06:07.123456789Z")));
				DatabaseException failure = assertThrows(DatabaseException.class, () -> database.transaction(() -> {
					Query batch = database.query(insert);
					if (chunk != null) batch.batchChunkSize(chunk);
					batch.executeBatch(groups);
				}), "chunk=" + chunk);
				assertTrue(failure.getMessage().contains("Oracle") && failure.getMessage().contains("temporal"), failure.getMessage());
				assertEquals(List.of(0, 99), database.query("SELECT id FROM " + table + " ORDER BY id").fetchList(Integer.class),
						"An already executed NULL chunk must roll back when later temporal binding is refused: " + chunk);
			}
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void explicitAmbiguousStrategyAndHandledCustomBinderKeepTheirOracleEscapeHatches() {
		ZoneId zone = ZoneId.of("Asia/Tokyo"); Database inspection = database(zone); String table = tableName();
		inspection.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, note VARCHAR2(30), plain_value TIMESTAMP(9))").execute();
		try {
			Database explicit = Database.with(source(), DatabaseType.ORACLE).timeZone(zone)
					.ambiguousTimestampBindingStrategy(AmbiguousTimestampBindingStrategy.TIMESTAMP_WITHOUT_TIME_ZONE).build();
			String merge = "MERGE INTO " + table + " t USING (SELECT :id id,:value value FROM dual) s ON (t.id=s.id) "
					+ "WHEN NOT MATCHED THEN INSERT (id,plain_value) VALUES (s.id,s.value)";
			explicit.transaction(() -> {
				int id = 0;
				for (Object value : temporalValues()) {
					int row = ++id;
					assertEquals(1L, explicit.query(merge).bind("id", row).bind("value", value).execute());
					assertEquals(render(localValue(value, zone)), explicit.query("SELECT TO_CHAR(plain_value, 'YYYY-MM-DD HH24:MI:SS.FF9') FROM "
							+ table + " WHERE id=:id").bind("id", row).fetchObject(String.class).orElseThrow());
				}
			});
			AtomicInteger handled = new AtomicInteger();
			CustomParameterBinder timestamp = new CustomParameterBinder() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Instant.class); }
				@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws SQLException {
					handled.incrementAndGet();
					statement.setObject(index, LocalDateTime.ofInstant((Instant) value, context.getTimeZone()), Types.TIMESTAMP);
					return BindingResult.handled();
				}
			};
			Database custom = Database.with(source(), DatabaseType.ORACLE).timeZone(zone)
					.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(timestamp))).build();
			Instant value = Instant.parse("2026-01-01T05:06:07.123456789Z");
			custom.transaction(() -> {
				assertEquals(1L, custom.query("INSERT INTO " + table + " (id,note,plain_value) SELECT 99,:note,:value FROM dual")
						.bind("note", null).bind("value", value).execute());
				assertEquals(1, handled.get(), "A handled custom binder remains authoritative after an earlier NULL saw the parser failure");
				assertEquals(render(LocalDateTime.ofInstant(value, zone)), custom.query("SELECT TO_CHAR(plain_value, 'YYYY-MM-DD HH24:MI:SS.FF9') FROM "
						+ table + " WHERE id=99").fetchObject(String.class).orElseThrow());
			});
		} finally { inspection.query("DROP TABLE " + table).execute(); }
	}

	private static List<Object> temporalValues() {
		Instant instant = Instant.parse("2026-01-01T05:06:07.123456789Z");
		return List.of(instant, instant.atOffset(ZoneOffset.ofHoursMinutes(5, 30)), instant.atZone(ZoneId.of("Asia/Kathmandu")),
				java.sql.Timestamp.from(instant), java.util.Date.from(instant), LocalDateTime.of(2026, 1, 1, 5, 6, 7, 123456789),
				LocalDate.of(2026, 1, 1));
	}

	private static LocalDateTime localValue(Object value, ZoneId zone) {
		if (value instanceof LocalDate day) return day.atStartOfDay();
		if (value instanceof LocalDateTime local) return local;
		return LocalDateTime.ofInstant(instantValue(value, zone), zone);
	}

	private static Instant instantValue(Object value, ZoneId zone) {
		if (value instanceof Instant instant) return instant;
		if (value instanceof OffsetDateTime offset) return offset.toInstant();
		if (value instanceof ZonedDateTime zoned) return zoned.toInstant();
		if (value instanceof java.util.Date legacy) return legacy.toInstant();
		if (value instanceof LocalDateTime local) return local.atZone(zone).toInstant();
		if (value instanceof LocalDate day) return day.atStartOfDay(zone).toInstant();
		throw new AssertionError(value);
	}

	private static String render(LocalDateTime value) {
		return value.format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSSSS"));
	}

	private static String tableName() { return "temporal_" + UUID.randomUUID().toString().replace("-", ""); }

	private static Database database() {
		return database(ZoneId.of("Asia/Tokyo"));
	}

	private static Database database(ZoneId zone) {
		return Database.with(source(), DatabaseType.ORACLE).timeZone(zone).build();
	}

	private static DataSource source() {
		return new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(ORACLE.getJdbcUrl(), ORACLE.getUsername(), ORACLE.getPassword());
	}
}
