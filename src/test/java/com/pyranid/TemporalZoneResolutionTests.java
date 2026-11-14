/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ResourceLock("java.util.TimeZone.default")
class TemporalZoneResolutionTests {
	private static final String TEXT = "SELECT CAST(:value AS VARCHAR(80)) AS value FROM (VALUES(0))";
	private static final String TIME = "SELECT CAST(:value AS TIME(9)) AS value FROM (VALUES(0))";
	private static final String TIME_WITH_ZONE = "SELECT CAST(:value AS TIME(9) WITH TIME ZONE) AS value FROM (VALUES(0))";
	public record DateRow(OffsetDateTime value) {}
	public record DateList(List<OffsetDateTime> value) {}
	public record DateMap(Map<String, OffsetDateTime> value) {}
	public record TimeRow(OffsetTime value) {}
	public record TimeList(List<OffsetTime> value) {}
	public record TimeSet(Set<OffsetTime> value) {}
	public record TimeMap(Map<String, OffsetTime> value) {}
	public record NativeRow(Object value) {}
	public static class DateBean {
		private OffsetDateTime value;
		public void setValue(OffsetDateTime value) { this.value = value; }
		public OffsetDateTime getValue() { return this.value; }
	}
	public static class TimeBean {
		private OffsetTime value;
		public void setValue(OffsetTime value) { this.value = value; }
		public OffsetTime getValue() { return this.value; }
	}

	@Test
	void localTextUsesTheSameGapAndOverlapResolutionAcrossScalarRecordAndBeanTargets() {
		TimeZone previous = TimeZone.getDefault();
		try {
			for (String jvm : List.of("UTC", "America/New_York")) {
				TimeZone.setDefault(TimeZone.getTimeZone(jvm));
				for (boolean cached : new boolean[]{false, true}) {
					for (String[] scenario : scenarios()) {
						ZoneId zone = ZoneId.of(scenario[0]);
						LocalDateTime local = LocalDateTime.parse(scenario[1]);
						OffsetDateTime expected = local.atZone(zone).toOffsetDateTime();
						Database database = database(zone, cached);
						for (String text : List.of(local.toString(), local.toString().replace('T', ' '))) {
							for (int pass = 0; pass < 2; pass++) {
								assertEquals(expected, database.query(TEXT).bind("value", text).fetchObject(OffsetDateTime.class).orElseThrow());
								assertEquals(expected, database.query(TEXT).bind("value", text).fetchObject(DateRow.class).orElseThrow().value());
								assertEquals(expected, database.query(TEXT).bind("value", text).fetchObject(DateBean.class).orElseThrow().getValue());
								assertEquals(expected.toInstant(), database.query(TEXT).bind("value", text).fetchObject(Instant.class).orElseThrow());
								assertEquals(expected.toInstant(), database.query(TEXT).bind("value", text).fetchObject(ZonedDateTime.class).orElseThrow().toInstant());
								assertEquals(text, database.query(TEXT).bind("value", text).fetchObject(Object.class).orElseThrow());
							}
						}
					}
				}
			}
		} finally { TimeZone.setDefault(previous); }
	}

	@Test
	void typedArraysListsAndMapsUseTheResolvedDateTimeAndKeepExplicitOffsets() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ZoneId.of("America/New_York"), cached);
			LocalDateTime local = LocalDateTime.parse("2024-03-10T02:30:00.123456789");
			OffsetDateTime expected = local.atZone(database.getTimeZone()).toOffsetDateTime();
			assertArrayEquals(new OffsetDateTime[]{expected, null}, (OffsetDateTime[]) convert(database,
					new String[]{local.toString(), null}, OffsetDateTime[].class));
			assertEquals(Arrays.asList(expected, null), convert(database,
					Arrays.asList(local.toString(), null), component(DateList.class)));
			assertEquals(Map.of("gap", expected), convert(database, Map.of("gap", local.toString()), component(DateMap.class)));
			OffsetDateTime explicit = OffsetDateTime.parse("2024-03-10T02:30:00.123456789+05:45");
			for (String text : List.of(explicit.toString(), explicit.toString().replace('T', ' '))) {
				assertEquals(explicit, database.query(TEXT).bind("value", text).fetchObject(OffsetDateTime.class).orElseThrow());
				assertEquals(explicit, database.query(TEXT).bind("value", text).fetchObject(DateRow.class).orElseThrow().value());
				assertEquals(explicit, database.query(TEXT).bind("value", text).fetchObject(DateBean.class).orElseThrow().getValue());
				assertEquals(explicit.toInstant(), database.query(TEXT).bind("value", text).fetchObject(Instant.class).orElseThrow());
			}
		}
	}

	@Test
	void plainTimesUseOnlyFixedConfiguredOffsetsAcrossRowsAndContainers() {
		LocalTime local = LocalTime.parse("12:34:56.123456789");
		for (boolean cached : new boolean[]{false, true}) {
			for (ZoneId zone : List.of(ZoneOffset.UTC, ZoneOffset.ofHoursMinutes(5, 45), ZoneOffset.ofHours(-18),
					ZoneOffset.ofHours(18), ZoneId.of("Etc/GMT-9"), ZoneId.of("UTC+03:15"))) {
				Database database = database(zone, cached);
				OffsetTime expected = local.atOffset((ZoneOffset) zone.normalized());
				assertEquals(expected, database.query(TIME).bind("value", local).fetchObject(OffsetTime.class).orElseThrow());
				assertEquals(expected, database.query(TIME).bind("value", local).fetchObject(TimeRow.class).orElseThrow().value());
				assertEquals(expected, database.query(TIME).bind("value", local).fetchObject(TimeBean.class).orElseThrow().getValue());
				assertEquals(expected, database.query(TEXT).bind("value", local.toString()).fetchObject(OffsetTime.class).orElseThrow());
				assertEquals(expected, database.query(TEXT).bind("value", local.toString()).fetchObject(TimeRow.class).orElseThrow().value());
				assertArrayEquals(new OffsetTime[]{expected, null}, (OffsetTime[]) convert(database, new LocalTime[]{local, null}, OffsetTime[].class));
				assertEquals(Arrays.asList(expected, null), convert(database, Arrays.asList(local, null), component(TimeList.class)));
				assertEquals(Set.of(expected), convert(database, List.of(local), component(TimeSet.class)));
				assertEquals(Map.of("clock", expected), convert(database, Map.of("clock", local), component(TimeMap.class)));
				assertTrue(database.query("SELECT CAST(NULL AS TIME(9)) AS value FROM (VALUES(0))").fetchObject(OffsetTime.class).isEmpty());
				assertNull(database.query("SELECT CAST(NULL AS TIME(9)) AS value FROM (VALUES(0))").fetchObject(TimeRow.class).orElseThrow().value());
			}
		}
	}

	@Test
	void regionalZonesCannotInventAnOffsetForDateLessTimesAndNativeValuesRemainAvailable() {
		LocalTime local = LocalTime.parse("12:34:56.123456789");
		for (boolean cached : new boolean[]{false, true}) {
			for (String name : List.of("Europe/London", "Asia/Singapore", "Asia/Kathmandu", "America/New_York")) {
				Database database = database(ZoneId.of(name), cached);
				for (Class<?> target : List.of(OffsetTime.class, TimeRow.class, TimeBean.class))
					assertDateLessFailure(() -> database.query(TIME).bind("value", local).fetchObject(target));
				assertDateLessFailure(() -> convert(database, new LocalTime[]{local}, OffsetTime[].class));
				assertDateLessFailure(() -> convert(database, List.of(local), component(TimeList.class)));
				assertDateLessFailure(() -> convert(database, Map.of("clock", local), component(TimeMap.class)));
				assertEquals(local, database.query(TIME).bind("value", local).fetchObject(LocalTime.class).orElseThrow());
				assertEquals(local, database.query(TIME).bind("value", local).fetchObject(NativeRow.class).orElseThrow().value());
				assertInstanceOf(java.sql.Time.class, database.query(TIME).bind("value", local).fetchObject(Object.class).orElseThrow());
				assertTrue(database.query("SELECT CAST(NULL AS TIME(9)) AS value FROM (VALUES(0))").fetchObject(OffsetTime.class).isEmpty());
			}
		}
	}

	@Test
	void explicitOffsetTimeValuesAndTextKeepTheirOffsetsUnderRegionalConfiguration() {
		OffsetTime expected = OffsetTime.parse("23:59:59.123456789-11:30");
		for (boolean cached : new boolean[]{false, true}) {
			for (String zone : List.of("Europe/London", "Asia/Singapore", "America/New_York")) {
				Database database = database(ZoneId.of(zone), cached);
				assertEquals(expected, database.query(TIME_WITH_ZONE).bind("value", expected).fetchObject(OffsetTime.class).orElseThrow());
				assertEquals(expected, database.query(TIME_WITH_ZONE).bind("value", expected).fetchObject(TimeRow.class).orElseThrow().value());
				assertEquals(expected, database.query(TIME_WITH_ZONE).bind("value", expected).fetchObject(TimeBean.class).orElseThrow().getValue());
				assertEquals(expected, database.query(TEXT).bind("value", expected.toString()).fetchObject(OffsetTime.class).orElseThrow());
				assertEquals(expected, database.query(TEXT).bind("value", expected.toString()).fetchObject(TimeRow.class).orElseThrow().value());
				assertEquals(Map.of("clock", expected), convert(database, Map.of("clock", expected.toString()), component(TimeMap.class)));
			}
		}
	}

	@Test
	void customColumnMappersCanSupplyAnApplicationSpecificTimeOffset() {
		LocalTime local = LocalTime.parse("12:34:56.123456789");
		OffsetTime expected = local.atOffset(ZoneOffset.ofHours(8));
		for (boolean cached : new boolean[]{false, true}) {
			List<Object> received = new ArrayList<>();
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.getRawClass() == OffsetTime.class; }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object value, TargetType target,
						Integer column, String label, InstanceProvider provider) {
					received.add(value); return MappingResult.of(expected);
				}
			})).build();
			Database database = database(ZoneId.of("Asia/Singapore"), mapper);
			assertEquals(expected, database.query(TIME).bind("value", local).fetchObject(OffsetTime.class).orElseThrow());
			assertEquals(expected, database.query(TIME).bind("value", local).fetchObject(TimeRow.class).orElseThrow().value());
			assertEquals(List.of(local, local), received);
		}
	}

	@Test
	void fallbackReadersNeverReplaceAnExplicitOffsetAndRejectLostZonedOffsets() throws Exception {
		OffsetTime explicit = OffsetTime.parse("01:02:03.123456789+05:45");
		StatementContext<?> context = context(database(ZoneId.of("Asia/Singapore"), true));
		assertSame(explicit, DefaultResultSetMapper.TemporalReaders.asOffsetTime(rows(explicit), 1, context, true));
		assertEquals(explicit, DefaultResultSetMapper.TemporalReaders.asOffsetTime(rows(explicit.toString()), 1, context, true));
		assertSame(explicit, DefaultResultSetMapper.TemporalReaders.asOffsetTime(rows(explicit), 1, context, false));
		DatabaseException failure = assertThrows(DatabaseException.class, () -> DefaultResultSetMapper.TemporalReaders.asOffsetTime(
				rows(java.sql.Time.valueOf("01:02:03")), 1, context(database(ZoneOffset.UTC, true)), true));
		assertTrue(failure.getMessage().contains("without losing its offset"));
	}

	@Test
	void nonIsoTimeTextRetainsTheDriversConfiguredClassicGetterFallback() throws Exception {
		java.sql.Time legacy = java.sql.Time.valueOf("12:34:56");
		legacy.setTime(legacy.getTime() + 123);
		ResultSet rows = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getObject" -> { if (args.length == 2) throw new SQLFeatureNotSupportedException("typed getter unavailable"); yield "driver-specific format"; }
					case "getTime" -> legacy;
					default -> throw new AssertionError(method.getName());
				});
		assertEquals(OffsetTime.parse("12:34:56.123+05:45"), DefaultResultSetMapper.TemporalReaders.asOffsetTime(rows, 1,
				context(database(ZoneOffset.ofHoursMinutes(5, 45), true)), false));
	}

	@Test
	void malformedExplicitOffsetsCannotFallBackToLenientClassicGetters() throws Exception {
		Database database = database(ZoneOffset.UTC, true);
		StatementContext<?> context = context(database);
		for (String text : List.of("12:34:56+25:00", "12:34:56+05:99", "12:34:56Zjunk", "12:34+25:00")) {
			assertThrows(DatabaseException.class, () -> DefaultResultSetMapper.TemporalReaders.asOffsetTime(lenientRows(text), 1, context, false));
			assertThrows(DatabaseException.class, () -> convert(database, text, OffsetTime.class));
		}
		for (String text : List.of("2024-03-10T02:30:00+25:00", "2024-03-10 02:30:00+05:99", "2024-03-10T02:30:00Zjunk")) {
			assertThrows(DatabaseException.class, () -> DefaultResultSetMapper.TemporalReaders.asOffsetDateTime(lenientRows(text), 1, context, false));
			assertThrows(DatabaseException.class, () -> DefaultResultSetMapper.TemporalReaders.asInstant(lenientRows(text), 1, context, false));
			assertThrows(DatabaseException.class, () -> DefaultResultSetMapper.TemporalReaders.asLocalDateTime(lenientRows(text), 1, context, false));
			assertThrows(DatabaseException.class, () -> convert(database, text, OffsetDateTime.class));
		}
	}

	@Test
	void nonIsoTimestampTextRetainsTheDriversConfiguredClassicGetterFallback() throws Exception {
		StatementContext<?> context = context(database(ZoneOffset.ofHoursMinutes(5, 45), true));
		LocalDateTime local = LocalDateTime.parse("2024-07-01T12:34:56.123456789");
		assertEquals(local.atOffset(ZoneOffset.ofHoursMinutes(5, 45)),
				DefaultResultSetMapper.TemporalReaders.asOffsetDateTime(lenientRows("driver-specific format"), 1, context, false));
		assertEquals(local.atOffset(ZoneOffset.ofHoursMinutes(5, 45)).toInstant(),
				DefaultResultSetMapper.TemporalReaders.asInstant(lenientRows("driver-specific format"), 1, context, false));
	}

	private static List<String[]> scenarios() {
		return List.of(new String[]{"America/New_York", "2024-03-10T02:30:00.123456789"},
				new String[]{"America/New_York", "2024-11-03T01:30:00.123456789"},
				new String[]{"America/New_York", "2024-07-01T12:34:56.123456789"},
				new String[]{"Australia/Lord_Howe", "2024-10-06T02:15:00.123456789"},
				new String[]{"Pacific/Apia", "2011-12-30T12:34:56.123456789"});
	}
	private static void assertDateLessFailure(org.junit.jupiter.api.function.Executable operation) {
		DatabaseException failure = assertThrows(DatabaseException.class, operation);
		assertTrue(failure.getMessage().contains("date-less"), failure.getMessage());
		assertFalse(failure.getMessage().contains("12:34:56"));
	}
	private static Type component(Class<?> record) { return record.getRecordComponents()[0].getGenericType(); }
	private static Object convert(Database database, Object value, Type target) {
		return ((DefaultResultSetMapper) database.getResultSetMapper()).convertResultSetValueToTargetType(context(database), value, TargetType.of(target)).orElseThrow();
	}
	private static StatementContext<?> context(Database database) { return StatementContext.with(Statement.of("zone-resolution", "SELECT value"), database).build(); }
	private static Database database(ZoneId zone, boolean cached) { return database(zone, ResultSetMapper.withPlanCachingEnabled(cached).build()); }
	private static Database database(ZoneId zone, ResultSetMapper mapper) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:zone_resolution_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).timeZone(zone).resultSetMapper(mapper).build();
	}
	private static ResultSet rows(Object value) {
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getObject" -> { if (args.length == 2) throw new SQLFeatureNotSupportedException("typed getter unavailable"); yield value; }
					default -> throw new AssertionError(method.getName());
				});
	}
	private static ResultSet lenientRows(String text) {
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getObject" -> { if (args.length == 2) throw new SQLFeatureNotSupportedException("typed getter unavailable"); yield text; }
					case "getTime" -> throw new AssertionError("Malformed explicit-offset text must not use a lenient time getter");
					case "getTimestamp" -> java.sql.Timestamp.valueOf("2024-07-01 12:34:56.123456789");
					default -> throw new AssertionError(method.getName());
				});
	}
}
