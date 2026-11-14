/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.PGStatement;

import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LegacyTemporalRangeTests {
	public record Stamp(Timestamp value) {}
	public record Day(java.sql.Date value) {}
	public record Days(List<java.sql.Date> value) {}
	public record NestedDays(List<List<java.sql.Date>> value) {}
	public record DayMap(Map<String, java.sql.Date> value) {}
	public record LocalDays(List<LocalDate> value) {}
	public record LocalStamps(List<LocalDateTime> value) {}
	public record OffsetStamps(List<OffsetDateTime> value) {}
	public static class StampBean {
		private Timestamp value;
		public Timestamp getValue() { return value; }
		public void setValue(Timestamp value) { this.value = value; }
	}

	@Test
	void javaTimeExtremaRejectLegacyTargetsAndRetainNativeAndJavaTimeRepresentations() {
		for (Object value : List.of(LocalDate.MIN, LocalDate.MAX, LocalDateTime.MIN, LocalDateTime.MAX,
				OffsetDateTime.MIN, OffsetDateTime.MAX, Instant.MIN, Instant.MAX)) {
			for (Class<?> target : List.of(Timestamp.class, java.sql.Date.class, Date.class))
				assertRangeFailure(() -> convert(value, TargetType.of(target)), target);
			assertSame(value, convert(value, TargetType.of(value.getClass())));
			assertSame(value, convert(value, TargetType.of(Object.class)));
		}
	}

	@Test
	void timestampConversionChecksBothInstantAndMillisecondBounds() {
		Instant earliest = Instant.ofEpochSecond(Math.floorDiv(Long.MIN_VALUE, 1000L) + 1);
		Instant latest = Instant.ofEpochMilli(Long.MAX_VALUE).plusNanos(999_999);
		for (Instant instant : List.of(earliest, latest, Instant.parse("-0001-02-03T04:05:06.123456789Z"),
				Instant.parse("+200000-02-03T04:05:06.123456789Z"))) {
			Timestamp timestamp = (Timestamp) convert(instant, TargetType.of(Timestamp.class));
			assertEquals(instant, timestamp.toInstant());
			assertEquals(instant.toEpochMilli(), timestamp.getTime());
		}
		for (Instant instant : List.of(earliest.minusNanos(1), latest.plusNanos(1),
				Instant.ofEpochMilli(Long.MIN_VALUE), Instant.parse("+400000000-01-01T00:00:00Z")))
			assertRangeFailure(() -> convert(instant, TargetType.of(Timestamp.class)), Timestamp.class);
		// util.Date has millisecond precision and a wider negative boundary than Timestamp.from.
		for (long millis : new long[]{Long.MIN_VALUE, Long.MAX_VALUE})
			assertEquals(millis, ((Date) convert(Instant.ofEpochMilli(millis), TargetType.of(Date.class))).getTime());
		assertRangeFailure(() -> convert(Instant.ofEpochMilli(Long.MAX_VALUE).plusMillis(1), TargetType.of(Date.class)), Date.class);
	}

	@Test
	void sqlDateRequiresAnEraAndYearThatSurviveItsCivilDateAndJdbcTextConversions() {
		for (LocalDate date : List.of(LocalDate.of(1, 1, 1), LocalDate.of(1500, 2, 28),
				LocalDate.of(1582, 10, 4), LocalDate.of(1582, 10, 15), LocalDate.of(1600, 2, 29),
				LocalDate.of(2000, 2, 29), LocalDate.of(9999, 12, 31))) {
			java.sql.Date converted = (java.sql.Date) convert(date, TargetType.of(java.sql.Date.class));
			assertEquals(date, converted.toLocalDate());
			assertEquals(date.toString(), converted.toString());
		}
		for (LocalDate date : List.of(LocalDate.of(0, 1, 1), LocalDate.of(-1, 1, 1),
				LocalDate.of(10000, 1, 1), LocalDate.of(300000000, 1, 1), LocalDate.of(1582, 10, 10)))
			assertRangeFailure(() -> convert(date, TargetType.of(java.sql.Date.class)), java.sql.Date.class);
	}

	@Test
	void typedNestedContainersAndMapValuesUseTheSameTemporalGuards() throws Exception {
		assertRangeFailure(() -> convert(new Object[]{LocalDate.MAX}, TargetType.of(java.sql.Date[].class)), java.sql.Date.class);
		assertRangeFailure(() -> convert(new Object[]{new Object[]{LocalDate.MIN}},
				TargetType.of(NestedDays.class.getRecordComponents()[0].getGenericType())), java.sql.Date.class);
		assertRangeFailure(() -> convert(Map.of("day", LocalDate.MAX),
				TargetType.of(DayMap.class.getRecordComponents()[0].getGenericType())), java.sql.Date.class);
	}

	@Test
	void unavailableTypedGettersCannotTurnPostgresInfinityIntoALegacyGarbageYear() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (long sentinel : new long[]{PGStatement.DATE_NEGATIVE_INFINITY, PGStatement.DATE_POSITIVE_INFINITY}) {
				for (int jdbcType : new int[]{Types.DATE, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE}) {
					Object raw = jdbcType == Types.DATE ? new java.sql.Date(sentinel) : new Timestamp(sentinel);
					Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
					for (Class<?> target : List.of(Timestamp.class, java.sql.Date.class, Date.class, Stamp.class, StampBean.class, Day.class))
						assertThrows(DatabaseException.class, () -> map(database, rows(raw, jdbcType), target));
					assertSame(raw, map(database, rows(raw, jdbcType), Object.class));
					assertSame(raw, convert(database, raw, TargetType.of(Object.class)));
					assertRangeFailure(() -> convert(database, raw, TargetType.of(raw.getClass())), raw.getClass());
					if (jdbcType == Types.DATE)
						assertEquals(sentinel > 0 ? LocalDate.MAX : LocalDate.MIN, map(database, rows(raw, jdbcType), LocalDate.class));
					if (jdbcType == Types.TIMESTAMP)
						assertEquals(sentinel > 0 ? LocalDateTime.MAX : LocalDateTime.MIN, map(database, rows(raw, jdbcType), LocalDateTime.class));
					if (jdbcType == Types.TIMESTAMP_WITH_TIMEZONE)
						assertEquals(sentinel > 0 ? OffsetDateTime.MAX : OffsetDateTime.MIN, map(database, rows(raw, jdbcType), OffsetDateTime.class));
				}
			}
		}
	}

	@Test
	void sqlArrayInfinityKeepsJavaTimeSentinelsAndRejectsLegacyElements() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			for (int jdbcType : new int[]{Types.DATE, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE}) {
				for (long sentinel : new long[]{PGStatement.DATE_NEGATIVE_INFINITY, PGStatement.DATE_POSITIVE_INFINITY}) {
					Object value = jdbcType == Types.DATE ? new java.sql.Date(sentinel) : new Timestamp(sentinel);
					for (Class<?> target : List.of(java.sql.Date[].class, Timestamp[].class, Date[].class, Days.class))
						assertThrows(DatabaseException.class, () -> map(database, rows(array(value, jdbcType), Types.ARRAY), target));
					if (jdbcType == Types.DATE)
						assertEquals(List.of(sentinel > 0 ? LocalDate.MAX : LocalDate.MIN),
								map(database, rows(array(value, jdbcType), Types.ARRAY), LocalDays.class).value());
					else if (jdbcType == Types.TIMESTAMP)
						assertEquals(List.of(sentinel > 0 ? LocalDateTime.MAX : LocalDateTime.MIN),
								map(database, rows(array(value, jdbcType), Types.ARRAY), LocalStamps.class).value());
					else
						assertEquals(List.of(sentinel > 0 ? OffsetDateTime.MAX : OffsetDateTime.MIN),
								map(database, rows(array(value, jdbcType), Types.ARRAY), OffsetStamps.class).value());
					assertSame(value, map(database, rows(array(value, jdbcType), Types.ARRAY), Object[].class)[0]);
				}
			}
		}
	}

	@Test
	void customColumnMappersKeepAlreadyReadJavaTimeValuesAndTheirOwnLegacyResult() throws Exception {
		Timestamp sentinel = new Timestamp(PGStatement.DATE_POSITIVE_INFINITY);
		ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(true).customColumnMappers(List.of(new CustomColumnMapper() {
			@Override public Boolean appliesTo(TargetType target) { return target.getRawClass() == Timestamp.class; }
			@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object value,
					TargetType target, Integer column, String label, InstanceProvider provider) {
				assertEquals(LocalDateTime.MAX, value);
				return MappingResult.of(sentinel);
			}
		})).build();
		Database database = database(mapper);
		assertSame(sentinel, map(database, rows(sentinel, Types.TIMESTAMP), Timestamp.class));
		assertSame(sentinel, map(database, rows(sentinel, Types.TIMESTAMP), Stamp.class).value());
		assertSame(sentinel, map(database, rows(sentinel, Types.TIMESTAMP), StampBean.class).getValue());
	}

	private static Object convert(Object value, TargetType target) {
		return convert(database(ResultSetMapper.withDefaultConfiguration()), value, target);
	}
	private static Object convert(Database database, Object value, TargetType target) {
		StatementContext<?> context = StatementContext.with(Statement.of("legacy-range", "SELECT value"), database).build();
		return ((DefaultResultSetMapper) database.getResultSetMapper()).convertResultSetValueToTargetType(context, value, target).orElseThrow();
	}
	private static void assertRangeFailure(org.junit.jupiter.api.function.Executable action, Class<?> target) {
		DatabaseException failure = assertThrows(DatabaseException.class, action);
		assertTrue(failure.getMessage().contains("representable range"), failure.getMessage());
		assertTrue(failure.getMessage().contains(target.getTypeName()), failure.getMessage());
	}
	private static Database database(ResultSetMapper mapper) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:legacy_ranges");
		ZoneId zone = ZoneId.systemDefault().equals(ZoneId.of("UTC")) ? ZoneId.of("Asia/Kathmandu") : ZoneId.of("UTC");
		return Database.with(source, DatabaseType.POSTGRESQL).timeZone(zone).resultSetMapper(mapper).build();
	}
	private static <T> T map(Database database, ResultSet rows, Class<T> target) throws Exception {
		StatementContext<T> context = StatementContext.with(Statement.of("legacy-range-row", "SELECT value"), database).build();
		return database.getResultSetMapper().map(context, rows, target, new InstanceProvider() {}).orElseThrow();
	}
	private static Array array(Object value, int jdbcType) {
		return (Array) Proxy.newProxyInstance(Array.class.getClassLoader(), new Class<?>[]{Array.class}, (proxy, method, args) -> switch (method.getName()) {
			case "getArray" -> new Object[]{value};
			case "getBaseType" -> jdbcType;
			case "getBaseTypeName" -> typeName(jdbcType);
			case "free" -> null;
			default -> throw new AssertionError(method.getName());
		});
	}
	private static ResultSet rows(Object raw, int jdbcType) {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					case "getColumnCount" -> 1;
					case "getColumnType" -> jdbcType;
					case "getColumnTypeName" -> typeName(jdbcType);
					case "getColumnLabel", "getColumnName" -> "value";
					case "getColumnClassName" -> raw.getClass().getName();
					default -> throw new AssertionError(method.getName());
				});
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject" -> {
						if (args.length > 1) throw new SQLFeatureNotSupportedException("typed getter unavailable");
						yield raw;
					}
					case "getDate", "getTimestamp" -> raw;
					case "wasNull" -> false;
					default -> throw new AssertionError(method.getName());
				});
	}
	private static String typeName(int jdbcType) {
		return switch (jdbcType) {
			case Types.DATE -> "date";
			case Types.TIMESTAMP -> "timestamp";
			case Types.TIMESTAMP_WITH_TIMEZONE -> "timestamptz";
			default -> "_timestamp";
		};
	}
}
