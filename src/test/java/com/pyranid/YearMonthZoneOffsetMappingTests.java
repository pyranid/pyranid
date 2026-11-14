/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class YearMonthZoneOffsetMappingTests {
	private static final String SQL = "SELECT CAST(:value AS VARCHAR(128)) AS value FROM (VALUES (0))";
	public record Month(YearMonth value) {}
	public record Months(List<YearMonth> value) {}
	public record MonthSet(Set<YearMonth> value) {}
	public record MonthMap(Map<YearMonth, YearMonth> value) {}
	public record Offset(ZoneOffset value) {}
	public record Offsets(List<ZoneOffset> value) {}
	public record OffsetMap(Map<String, ZoneOffset> value) {}
	public static class GenericBean<T> {
		private T value;
		public T getValue() { return value; }
		public void setValue(T value) { this.value = value; }
	}
	public static class MonthBean extends GenericBean<YearMonth> {}
	public static class OffsetBean extends GenericBean<ZoneOffset> {}

	@Test
	void boundYearMonthsRoundTripTheirOwnTextAcrossScalarRecordAndInheritedBeanTargets() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(mapper(cached));
			for (int year : new int[]{-999999999, -10000, -1, 0, 1, 9999, 10000, 123456, 999999999}) {
				YearMonth value = YearMonth.of(year, 2);
				assertEquals(value, database.query(SQL).bind("value", value).fetchObject(YearMonth.class).orElseThrow());
				assertEquals(new Month(value), database.query(SQL).bind("value", value).fetchObject(Month.class).orElseThrow());
				assertEquals(value, database.query(SQL).bind("value", value).fetchObject(MonthBean.class).orElseThrow().getValue());
				assertEquals(value.toString(), database.query(SQL).bind("value", value).fetchObject(Object.class).orElseThrow());
			}
		}
	}

	@Test
	void yearMonthParsingKeepsCanonicalFormsStrictAndSupportsUnsignedExpandedPositiveYears() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(mapper(cached));
			for (String value : List.of("10000-12", "+10000-12", " 10000-12 "))
				assertEquals(YearMonth.of(10000, 12), database.query(SQL).bind("value", value).fetchObject(YearMonth.class).orElseThrow());
			for (String value : List.of("10000-00", "10000-13", "1000000000-01", "2024-1", "10000-1", "+2024-01", "00000-01", "10000-01-01", "invalid"))
				for (Class<?> target : List.of(YearMonth.class, Month.class, MonthBean.class))
					assertThrows(DatabaseException.class, () -> database.query(SQL).bind("value", value).fetchObject(target));
		}
	}

	@Test
	void expandedYearMonthsConvertRecursivelyThroughTypedContainers() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(mapper(cached));
			Object[] values = {"10000-02", null, "-10000-12"};
			YearMonth first = YearMonth.of(10000, 2), last = YearMonth.of(-10000, 12);
			assertArrayEquals(new YearMonth[]{first, null, last}, (YearMonth[]) convert(database, values, TargetType.of(YearMonth[].class)));
			assertEquals(Arrays.asList(first, null, last), convert(database, values, type(Months.class)));
			assertEquals(Set.of(first, last), convert(database, new Object[]{"10000-02", "-10000-12", "10000-02"}, type(MonthSet.class)));
			Map<Object, Object> source = new LinkedHashMap<>();
			source.put("10000-02", "999999999-12"); source.put("-10000-12", null);
			Map<YearMonth, YearMonth> expected = new LinkedHashMap<>();
			expected.put(first, YearMonth.of(999999999, 12)); expected.put(last, null);
			assertEquals(expected, convert(database, source, type(MonthMap.class)));
			assertThrows(DatabaseException.class, () -> convert(database, new Object[]{"1000000000-02"}, type(Months.class)));
		}
	}

	@Test
	void zoneOffsetScalarRecordBeanAndNativeTargetsAgreeForFixedOffsetsAndAliases() {
		Map<String, ZoneOffset> cases = new LinkedHashMap<>();
		cases.put("Z", ZoneOffset.UTC); cases.put("UTC", ZoneOffset.UTC); cases.put("GMT", ZoneOffset.UTC); cases.put("UT", ZoneOffset.UTC);
		cases.put("+02", ZoneOffset.ofHours(2)); cases.put("-0530", ZoneOffset.ofHoursMinutes(-5, -30));
		cases.put("+05:45", ZoneOffset.ofHoursMinutes(5, 45)); cases.put("-05:30:45", ZoneOffset.ofHoursMinutesSeconds(-5, -30, -45));
		cases.put("+18:00", ZoneOffset.MAX); cases.put("-18:00", ZoneOffset.MIN);
		cases.put("UTC+05:45", ZoneOffset.ofHoursMinutes(5, 45)); cases.put("Etc/GMT+5", ZoneOffset.ofHours(-5));
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(mapper(cached));
			for (Map.Entry<String, ZoneOffset> entry : cases.entrySet()) {
				assertEquals(entry.getValue(), database.query(SQL).bind("value", entry.getKey()).fetchObject(ZoneOffset.class).orElseThrow());
				assertEquals(new Offset(entry.getValue()), database.query(SQL).bind("value", entry.getKey()).fetchObject(Offset.class).orElseThrow());
				assertEquals(entry.getValue(), database.query(SQL).bind("value", entry.getKey()).fetchObject(OffsetBean.class).orElseThrow().getValue());
				assertEquals(entry.getKey(), database.query(SQL).bind("value", entry.getKey()).fetchObject(Object.class).orElseThrow());
			}
		}
	}

	@Test
	void zoneOffsetsRejectChangingRegionsAndInvalidOffsetsWithoutAssigningTheWrongSubtype() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(mapper(cached));
			for (String value : List.of("Europe/Paris", "America/New_York", "+18:01", "+19:00", "+00:00:60", "not-a-zone")) {
				for (Class<?> target : List.of(ZoneOffset.class, Offset.class, OffsetBean.class))
					assertThrows(DatabaseException.class, () -> database.query(SQL).bind("value", value).fetchObject(target));
				assertThrows(DatabaseException.class, () -> convert(database, new Object[]{value}, type(Offsets.class)));
			}
			assertEquals(ZoneId.of("Europe/Paris"), database.query(SQL).bind("value", "Europe/Paris").fetchObject(ZoneId.class).orElseThrow());
			assertEquals(Arrays.asList(ZoneOffset.UTC, null, ZoneOffset.ofHoursMinutes(5, 45)),
					convert(database, new Object[]{"UTC", null, "+05:45"}, type(Offsets.class)));
			assertEquals(Map.of("east", ZoneOffset.ofHours(2)), convert(database, Map.of("east", "+02:00"), type(OffsetMap.class)));
			ZoneOffset nativeOffset = ZoneOffset.ofHoursMinutes(5, 45);
			assertSame(nativeOffset, convert(database, nativeOffset, TargetType.of(ZoneOffset.class)));
		}
	}

	@Test
	void nullValuesAndOriginalCustomMapperInputsKeepTheirEstablishedContracts() {
		for (boolean cached : new boolean[]{false, true}) {
			List<Object> inputs = new ArrayList<>();
			CustomColumnMapper custom = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(ZoneOffset.class) || target.matchesClass(YearMonth.class); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
						Integer column, String label, InstanceProvider instances) {
					inputs.add(raw);
					return raw.equals("application-value") ? MappingResult.of(target.matchesClass(YearMonth.class) ? YearMonth.of(10000, 2) : ZoneOffset.UTC)
							: MappingResult.fallback();
				}
			};
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(custom)).build());
			assertEquals(ZoneOffset.UTC, database.query(SQL).bind("value", "application-value").fetchObject(ZoneOffset.class).orElseThrow());
			assertEquals(new Month(YearMonth.of(10000, 2)), database.query(SQL).bind("value", "application-value").fetchObject(Month.class).orElseThrow());
			assertEquals("application-value", inputs.get(0)); assertEquals("application-value", inputs.get(1));
			assertEquals(YearMonth.of(10000, 2), database.query(SQL).bind("value", "10000-02").fetchObject(YearMonth.class).orElseThrow());
			for (Class<?> scalar : List.of(YearMonth.class, ZoneOffset.class))
				assertTrue(database.query(SQL).bind("value", null).fetchObject(scalar).isEmpty());
			assertEquals(new Month(null), database.query(SQL).bind("value", null).fetchObject(Month.class).orElseThrow());
			assertEquals(new Offset(null), database.query(SQL).bind("value", null).fetchObject(Offset.class).orElseThrow());
		}
	}

	private static TargetType type(Class<?> record) { return TargetType.of(record.getRecordComponents()[0].getGenericType()); }
	private static Object convert(Database database, Object value, TargetType type) {
		StatementContext<?> context = StatementContext.with(Statement.of("small-temporal", "SELECT value"), database).build();
		return ((DefaultResultSetMapper) database.getResultSetMapper()).convertResultSetValueToTargetType(context, value, type).orElseThrow();
	}
	private static ResultSetMapper mapper(boolean cached) { return ResultSetMapper.withPlanCachingEnabled(cached).build(); }
	private static Database database(ResultSetMapper mapper) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:small_temporal_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).resultSetMapper(mapper).build();
	}
}
