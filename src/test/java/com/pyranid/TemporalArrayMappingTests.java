/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TemporalArrayMappingTests {
	public record Instants(List<Instant> value) {}
	public record InstantSet(Set<Instant> value) {}
	public record SqlDates(List<java.sql.Date> value) {}
	public record SqlTimestamps(List<Timestamp> value) {}
	public record LocalDateTimes(List<LocalDateTime> value) {}
	public record Dates(List<Date> value) {}
	public record LocalDates(List<LocalDate> value) {}
	public record NestedInstants(List<List<Instant>> value) {}
	public record OffsetTimes(List<OffsetTime> value) {}
	public record NestedOffsetTimes(List<List<OffsetTime>> value) {}
	public record LocalTimes(List<LocalTime> value) {}
	public record LocalTimeSet(Set<LocalTime> value) {}
	public static class InstantBean {
		private List<Instant> value;
		public List<Instant> getValue() { return this.value; }
		public void setValue(List<Instant> value) { this.value = value; }
	}

	@Test
	void hsqlDateAndTimestampArraysMatchScalarConversionsInConfiguredZones() {
		LocalDate date = LocalDate.parse("2024-01-02");
		LocalDateTime timestamp = LocalDateTime.parse("2024-01-02T12:34:56.123456789");
		for (boolean cached : new boolean[]{false, true}) {
			// At least one configured zone differs from the JVM default, without mutating it.
			for (ZoneId zone : List.of(ZoneId.of("UTC"), ZoneId.of("Asia/Kathmandu"))) {
				Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build(), zone);
				for (String literal : List.of("DATE '2024-01-02'", "TIMESTAMP '2024-01-02 12:34:56.123456789'")) {
					LocalDateTime local = literal.startsWith("DATE") ? date.atStartOfDay() : timestamp;
					Instant expected = local.atZone(zone).toInstant();
					String scalar = "SELECT " + literal + " AS value FROM (VALUES(0))";
					String array = "SELECT ARRAY[" + literal + "] AS value FROM (VALUES(0))";
					assertEquals(expected, database.query(scalar).fetchObject(Instant.class).orElseThrow());
					for (int execution = 0; execution < 2; execution++) {
						assertEquals(List.of(expected), database.query(array).fetchObject(Instants.class).orElseThrow().value());
						assertEquals(List.of(expected), database.query(array).fetchObject(InstantBean.class).orElseThrow().getValue());
						assertEquals(Set.of(expected), database.query(array).fetchObject(InstantSet.class).orElseThrow().value());
						assertArrayEquals(new Instant[]{expected}, database.query(array).fetchObject(Instant[].class).orElseThrow());
						assertEquals(List.of(java.sql.Date.valueOf(date)), database.query(array).fetchObject(SqlDates.class).orElseThrow().value());
						assertEquals(List.of(Timestamp.valueOf(local)), database.query(array).fetchObject(SqlTimestamps.class).orElseThrow().value());
						assertEquals(List.of(local), database.query(array).fetchObject(LocalDateTimes.class).orElseThrow().value());
						assertEquals(List.of(Date.from(expected)), database.query(array).fetchObject(Dates.class).orElseThrow().value());
					}
				}
				String nullable = "SELECT ARRAY[DATE '2024-01-02', CAST(NULL AS DATE)] AS value FROM (VALUES(0))";
				assertEquals(Arrays.asList(date.atStartOfDay(zone).toInstant(), null),
						database.query(nullable).fetchObject(Instants.class).orElseThrow().value());
			}
		}
	}

	@Test
	void timestampWithTimeZoneArrayElementsPreserveTheirInstant() throws Exception {
		OffsetDateTime source = OffsetDateTime.parse("2024-01-02T00:34:56.123456789+05:45");
		ZoneId zone = ZoneId.of("America/New_York");
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			Database database = database(mapper, zone);
			String sql = "SELECT ARRAY[TIMESTAMP '2024-01-02 00:34:56.123456789+05:45'] AS value FROM (VALUES(0))";
			assertEquals(List.of(source.toInstant()), database.query(sql).fetchObject(Instants.class).orElseThrow().value());
			assertEquals(List.of(java.sql.Date.valueOf(source.atZoneSameInstant(zone).toLocalDate())),
					database.query(sql).fetchObject(SqlDates.class).orElseThrow().value());
			// Some drivers expose TIMESTAMP WITH TIME ZONE elements as legacy Timestamp.
			assertEquals(List.of(source.atZoneSameInstant(zone).toLocalDate()), mapArray(mapper, zone,
					Types.TIMESTAMP_WITH_TIMEZONE, new Object[]{source}, LocalDates.class).value());
			Timestamp legacy = Timestamp.from(source.toInstant());
			assertEquals(List.of(source.toInstant()), mapArray(mapper, zone, Types.TIMESTAMP_WITH_TIMEZONE,
					new Object[]{legacy}, Instants.class).value());
			assertEquals(List.of(source.atZoneSameInstant(zone).toLocalDateTime()), mapArray(mapper, zone,
					Types.TIMESTAMP_WITH_TIMEZONE, new Object[]{legacy}, LocalDateTimes.class).value());
			assertEquals(List.of(java.sql.Date.valueOf(source.atZoneSameInstant(zone).toLocalDate())), mapArray(mapper, zone,
					Types.TIMESTAMP_WITH_TIMEZONE, new Object[]{legacy}, SqlDates.class).value());
		}
	}

	@Test
	void timestampArrayTypeNamesDistinguishInstantsFromWallClockValues() throws Exception {
		Instant instant = Instant.parse("2024-01-02T06:49:56.123456789Z");
		Timestamp raw = Timestamp.from(instant);
		ZoneId zone = ZoneId.systemDefault().equals(ZoneId.of("UTC")) ? ZoneId.of("Asia/Kathmandu") : ZoneId.of("UTC");
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			for (String name : List.of("timestamptz", "TIMESTAMP WITH TIME ZONE", "timestamp(9) with time zone", "TIMESTAMP WITH LOCAL TIME ZONE")) {
				assertEquals(List.of(instant), mapArray(mapper, zone, Types.TIMESTAMP, name,
						new Object[]{raw}, Instants.class).value());
				assertEquals(List.of(Arrays.asList(instant, null)), mapArray(mapper, zone, Types.TIMESTAMP, name,
						new Object[]{new Object[]{raw, null}}, NestedInstants.class).value());
			}
			assertEquals(List.of(raw.toLocalDateTime().atZone(zone).toInstant()), mapArray(mapper, zone,
					Types.TIMESTAMP, "timestamp", new Object[]{raw}, Instants.class).value());
		}
	}

	@Test
	void unavailableArrayTypeNameRetainsJdbcTypeAndFreesTheArray() throws Exception {
		Timestamp raw = Timestamp.valueOf("2024-01-02 12:34:56.123456789");
		ZoneId zone = ZoneId.systemDefault().equals(ZoneId.of("UTC")) ? ZoneId.of("Asia/Kathmandu") : ZoneId.of("UTC");
		for (Throwable unsupported : List.of(new SQLFeatureNotSupportedException("type name unavailable"),
				new UnsupportedOperationException("type name unavailable"), new AbstractMethodError("type name unavailable"))) {
			AtomicInteger frees = new AtomicInteger();
			Array array = (Array) Proxy.newProxyInstance(Array.class.getClassLoader(), new Class<?>[]{Array.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getArray" -> new Object[]{raw};
						case "getBaseType" -> Types.TIMESTAMP;
						case "getBaseTypeName" -> throw unsupported;
						case "free" -> { frees.incrementAndGet(); yield null; }
						default -> throw new AssertionError(method.getName());
					});
			assertEquals(List.of(raw.toLocalDateTime().atZone(zone).toInstant()),
					mapArray(ResultSetMapper.withDefaultConfiguration(), zone, array, Instants.class).value());
			assertEquals(1, frees.get());
		}
	}

	@Test
	void mappingResourceScopesReleaseContainerMetadataAfterEachRow() throws Exception {
		MappingResources resources = new MappingResources(null, null).acquire();
		Map<String, Object> attributes = Map.of("value", Timestamp.valueOf("2024-01-02 12:34:56"));
		Object[] values = {attributes};
		resources.registerStructType(attributes, "STRUCT(value TIMESTAMP)");
		resources.registerArrayType(values, Types.STRUCT);
		resources.registerArrayTimeValues(values, new Object[]{OffsetTime.parse("12:34:56+05:45")});
		assertEquals("STRUCT(value TIMESTAMP)", resources.structType(attributes));
		assertEquals(Types.STRUCT, resources.arrayType(values));
		resources.close();
		assertSame(resources, resources.acquire());
		assertNull(resources.structType(attributes));
		assertEquals(Types.OTHER, resources.arrayType(values));
		assertSame(values, resources.arrayTimeValues(values));
		resources.close();
	}

	@Test
	void customFallbackSeesOriginalArrayElementsAndRetainsBaseTypeForBuiltIns() throws Exception {
		ZoneId zone = ZoneId.systemDefault().equals(ZoneId.of("UTC")) ? ZoneId.of("Asia/Kathmandu") : ZoneId.of("UTC");
		Timestamp raw = Timestamp.valueOf("2024-01-02 12:34:56.123456789");
		Instant expected = raw.toLocalDateTime().atZone(zone).toInstant();
		for (boolean cached : new boolean[]{false, true}) {
			AtomicInteger calls = new AtomicInteger();
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) {
					return target.getRawClass() == List.class || target.getRawClass() == Instant[].class;
				}
				@Override public MappingResult map(StatementContext<?> context, ResultSet rs, Object value,
						TargetType target, Integer column, String label, InstanceProvider provider) {
					assertSame(raw, ((Object[]) value)[0]);
					calls.incrementAndGet();
					return MappingResult.fallback();
				}
			})).build();
			assertEquals(List.of(expected), mapArray(mapper, zone, Types.TIMESTAMP, new Object[]{raw}, Instants.class).value());
			assertArrayEquals(new Instant[]{expected}, mapArray(mapper, zone, Types.TIMESTAMP, new Object[]{raw}, Instant[].class));
			assertEquals(2, calls.get());
			Map<?, ?> nativeRow = mapArray(mapper, zone, Types.TIMESTAMP, new Object[]{raw}, Map.class);
			assertSame(raw, ((Object[]) nativeRow.get("value"))[0]);
		}
	}

	@Test
	void unavailableBaseTypePreservesTimestampInstantAndArrayCleanup() throws Exception {
		Timestamp raw = Timestamp.from(Instant.parse("2024-01-02T12:34:56Z"));
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			for (Integer unavailable : Arrays.asList(null, Integer.MIN_VALUE, Integer.MAX_VALUE))
				assertEquals(List.of(raw.toInstant()), mapArray(mapper, ZoneId.of("Asia/Kathmandu"), unavailable,
						new Object[]{raw}, Instants.class).value());
		}
		SQLException extractionFailure = new SQLException("array read failed");
		SQLException freeFailure = new SQLException("array free failed");
		Array failing = (Array) Proxy.newProxyInstance(Array.class.getClassLoader(), new Class<?>[]{Array.class},
				(proxy, method, args) -> {
					if (method.getName().equals("getArray")) throw extractionFailure;
					if (method.getName().equals("free")) throw freeFailure;
					throw new AssertionError(method.getName());
				});
		SQLException failure = assertThrows(SQLException.class, () -> mapArray(ResultSetMapper.withDefaultConfiguration(),
				ZoneId.of("UTC"), failing, Instants.class));
		assertSame(extractionFailure, failure);
		assertArrayEquals(new Throwable[]{freeFailure}, failure.getSuppressed());
		Array unavailableMetadata = (Array) Proxy.newProxyInstance(Array.class.getClassLoader(), new Class<?>[]{Array.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getArray" -> new Object[]{raw};
					case "getBaseType" -> throw new SQLFeatureNotSupportedException("base type unavailable");
					case "free" -> throw freeFailure;
					default -> throw new AssertionError(method.getName());
				});
		assertSame(freeFailure, assertThrows(SQLException.class, () -> mapArray(ResultSetMapper.withDefaultConfiguration(),
				ZoneId.of("UTC"), unavailableMetadata, Instants.class)));

	}

	@Test
	void assignableNestedTemporalArraysStillConvertEveryElement() throws Exception {
		Timestamp raw = Timestamp.valueOf("2024-01-02 12:34:56.123456789");
		ZoneId zone = ZoneId.systemDefault().equals(ZoneId.of("UTC")) ? ZoneId.of("Asia/Kathmandu") : ZoneId.of("UTC");
		Date expected = Date.from(raw.toLocalDateTime().atZone(zone).toInstant());
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			Timestamp[][] values = {new Timestamp[]{raw, null}, new Timestamp[0], null};
			Date[][] mapped = mapArray(mapper, zone, Types.TIMESTAMP, values, Date[][].class);
			assertArrayEquals(new Date[]{expected, null}, mapped[0]);
			assertEquals(Date[].class, mapped[0].getClass());
			assertArrayEquals(new Date[0], mapped[1]);
			assertNull(mapped[2]);
			Object[] nativeValues = mapArray(mapper, zone, Types.TIMESTAMP, values, Object[].class);
			assertSame(values[0], nativeValues[0]);
			assertSame(raw, ((Object[]) nativeValues[0])[0]);
		}
	}

	@Test
	void zonedTimeArrayReadersPreserveOffsetsFractionsAndNestedShape() throws Exception {
		OffsetTime expected = OffsetTime.parse("12:34:56.123456789+05:45");
		java.sql.Time raw = java.sql.Time.valueOf("01:49:56");
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			for (String name : List.of("timetz", "TIME WITH TIME ZONE", "time(9) with time zone")) {
				List<String> events = new ArrayList<>();
				OffsetTime[][] result = mapArray(mapper, ZoneId.of("UTC"), timeArray(
						new Object[]{new java.sql.Time[]{raw, null}}, new Object[]{new Object[]{expected, null}},
						Types.TIME, name, events, "root", null, null, null), OffsetTime[][].class);
				assertArrayEquals(new OffsetTime[]{expected, null}, result[0]);
				assertEquals(List.of("root.0:close", "root.0:free", "root:close", "root:free"), events);
			}
			List<String> events = new ArrayList<>();
			OffsetTimes result = mapArray(mapper, ZoneId.of("America/New_York"), timeArray(
					new java.sql.Time[]{raw}, new Object[]{expected}, Types.TIME_WITH_TIMEZONE, null,
					events, "root", null, null, null), OffsetTimes.class);
			assertEquals(List.of(expected), result.value());
			assertEquals(List.of("root:close", "root:free"), events);
		}
	}

	@Test
	void zonedTimeCustomFallbackKeepsRawInputsAndNativeTargets() throws Exception {
		OffsetTime expected = OffsetTime.parse("12:34:56.123456789+05:45");
		java.sql.Time raw = java.sql.Time.valueOf("01:49:56");
		for (boolean cached : new boolean[]{false, true}) {
			AtomicInteger calls = new AtomicInteger();
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.getRawClass() == List.class; }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object value,
						TargetType target, Integer column, String label, InstanceProvider provider) {
					assertSame(raw, ((Object[]) value)[0]);
					calls.incrementAndGet();
					return MappingResult.fallback();
				}
			})).build();
			assertEquals(List.of(expected), mapArray(mapper, ZoneId.of("UTC"), timeArray(
					new java.sql.Time[]{raw}, new Object[]{expected}, Types.TIME, "timetz",
					new ArrayList<>(), "root", null, null, null), OffsetTimes.class).value());
			assertEquals(1, calls.get());
			Map<?, ?> row = mapArray(mapper, ZoneId.of("UTC"), timeArray(new java.sql.Time[]{raw}, new Object[]{expected},
					Types.TIME, "timetz", new ArrayList<>(), "root", null, null, null), Map.class);
			assertSame(raw, ((Object[]) row.get("value"))[0]);
		}
	}

	@Test
	void unavailableZonedTimeReaderFailsTypedMappingButKeepsNativeValues() throws Exception {
		java.sql.Time raw = java.sql.Time.valueOf("01:49:56");
		for (Throwable unsupported : List.of(new SQLFeatureNotSupportedException("typed getter unavailable"),
				new SQLException("typed getter unavailable", "0A000"), new SQLException("typed getter unavailable", "0A001"),
				new SQLException("typed getter unavailable", "HYC00"),
				new UnsupportedOperationException("typed getter unavailable"), new AbstractMethodError("typed getter unavailable"))) {
			List<String> events = new ArrayList<>();
			DatabaseException failure = assertThrows(DatabaseException.class, () -> mapArray(ResultSetMapper.withDefaultConfiguration(),
					ZoneId.of("UTC"), timeArray(new java.sql.Time[]{raw}, new Object[]{null}, Types.TIME, "timetz",
					events, "root", unsupported, null, null), OffsetTime[].class));
			assertSame(unsupported, failure.getCause());
			assertEquals(List.of("root:close", "root:free"), events);
			Map<?, ?> nativeRow = mapArray(ResultSetMapper.withDefaultConfiguration(), ZoneId.of("UTC"), timeArray(
					new java.sql.Time[]{raw}, new Object[]{null}, Types.TIME, "timetz", new ArrayList<>(), "root",
					unsupported, null, null), Map.class);
			assertSame(raw, ((Object[]) nativeRow.get("value"))[0]);
			OffsetTime supplied = OffsetTime.parse("12:34:56.123456789+05:45");
			ResultSetMapper custom = ResultSetMapper.withPlanCachingEnabled(true).customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.getRawClass() == OffsetTime[].class; }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object value,
						TargetType target, Integer column, String label, InstanceProvider provider) {
					assertSame(raw, ((Object[]) value)[0]);
					return MappingResult.of(new OffsetTime[]{supplied});
				}
			})).build();
			assertArrayEquals(new OffsetTime[]{supplied}, mapArray(custom, ZoneId.of("UTC"), timeArray(
					new java.sql.Time[]{raw}, new Object[]{null}, Types.TIME, "timetz", new ArrayList<>(), "root",
					unsupported, null, null), OffsetTime[].class));
		}
	}

	@Test
	void zonedTimeReadFailuresAreNotHiddenForNativeTargets() {
		java.sql.Time raw = java.sql.Time.valueOf("01:49:56");
		SQLException unreadableState = new SQLException("classification unavailable") {
			@Override public String getSQLState() { throw new AssertionError("broken SQLState accessor"); }
		};
		SQLException connectionFailure = new SQLException("connection lost", "08006");
		connectionFailure.setNextException(new SQLException("unrelated unsupported operation", "0A000"));
		for (Throwable readFailure : List.of(connectionFailure, unreadableState,
				new IllegalStateException("driver failed"), new AssertionError("driver failed"))) {
			List<String> events = new ArrayList<>();
			Throwable thrown = assertThrows(Throwable.class, () -> mapArray(ResultSetMapper.withDefaultConfiguration(),
					ZoneId.of("UTC"), timeArray(new java.sql.Time[]{raw}, new Object[]{null}, Types.TIME, "timetz",
					events, "root", readFailure, null, null), Map.class));
			assertSame(readFailure, thrown);
			assertEquals(List.of("root:close", "root:free"), events);
		}
	}

	@Test
	void zonedTimeReaderCleanupPreservesPrimaryFailureAndReleasesAllHandles() throws Exception {
		SQLException readFailure = new SQLException("typed read failed");
		SQLException freeFailure = new SQLException("array free failed");
		java.sql.Time raw = java.sql.Time.valueOf("01:49:56");
		List<String> events = new ArrayList<>();
		// Reusing the same Throwable during read/close must not cause self-suppression.
		SQLException thrown = assertThrows(SQLException.class, () -> mapArray(ResultSetMapper.withDefaultConfiguration(),
				ZoneId.of("UTC"), timeArray(new Object[]{new java.sql.Time[]{raw}}, new Object[]{new Object[]{null}},
				Types.TIME, "timetz", events, "root", readFailure, readFailure, freeFailure), Map.class));
		assertSame(readFailure, thrown);
		assertTrue(Arrays.asList(thrown.getSuppressed()).contains(freeFailure));
		assertEquals(List.of("root.0:close", "root.0:free", "root:close", "root:free"), events);
	}

	@Test
	void plainTimeArrayReadersPreserveFractionsEndOfDayAndNestedShape() throws Exception {
		LocalTime fraction = LocalTime.parse("12:34:56.123456789");
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			for (LocalTime expected : List.of(fraction, LocalTime.MAX)) {
				java.sql.Time raw = java.sql.Time.valueOf(expected == LocalTime.MAX ? LocalTime.MIDNIGHT : expected);
				List<String> events = new ArrayList<>();
				LocalTime[][] nested = mapArray(mapper, ZoneId.of("UTC"), timeArray(
						new Object[]{new java.sql.Time[]{raw, null}, new java.sql.Time[0], null},
						new Object[]{new Object[]{expected, null}, new Object[0], null}, Types.TIME, "time",
						events, "root", null, null, null), LocalTime[][].class);
				assertArrayEquals(new LocalTime[]{expected, null}, nested[0]);
				assertArrayEquals(new LocalTime[0], nested[1]);
				assertNull(nested[2]);
				assertEquals(List.of("root.0:close", "root.0:free", "root.1:close", "root.1:free", "root:close", "root:free"), events);
				assertEquals(Arrays.asList(expected, null), mapArray(mapper, ZoneId.of("UTC"), timeArray(
						new java.sql.Time[]{raw, null}, new Object[]{expected, null}, Types.TIME, "time",
						new ArrayList<>(), "root", null, null, null), LocalTimes.class).value());
				assertEquals(Set.of(expected), mapArray(mapper, ZoneId.of("UTC"), timeArray(
						new java.sql.Time[]{raw, raw}, new Object[]{expected, expected}, Types.TIME, "time",
						new ArrayList<>(), "root", null, null, null), LocalTimeSet.class).value());
			}
		}
	}

	@Test
	void plainTimeCustomFallbackAndNativeTargetsKeepDriverInputs() throws Exception {
		LocalTime expected = LocalTime.parse("12:34:56.123456789");
		java.sql.Time raw = java.sql.Time.valueOf(expected);
		for (boolean cached : new boolean[]{false, true}) {
			AtomicInteger calls = new AtomicInteger();
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.getRawClass() == List.class; }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object value,
						TargetType target, Integer column, String label, InstanceProvider provider) {
					assertSame(raw, ((Object[]) value)[0]);
					calls.incrementAndGet();
					return MappingResult.fallback();
				}
			})).build();
			assertEquals(List.of(expected), mapArray(mapper, ZoneId.of("UTC"), timeArray(
					new java.sql.Time[]{raw}, new Object[]{expected}, Types.TIME, "time",
					new ArrayList<>(), "root", null, null, null), LocalTimes.class).value());
			assertEquals(1, calls.get());
			Map<?, ?> nativeRow = mapArray(mapper, ZoneId.of("UTC"), timeArray(
					new java.sql.Time[]{raw}, new Object[]{expected}, Types.TIME, "time",
					new ArrayList<>(), "root", null, null, null), Map.class);
			assertSame(raw, ((Object[]) nativeRow.get("value"))[0]);
		}
	}

	@Test
	void unsupportedPlainTimeReaderFailsBuiltInMappingWithoutChangingNativeOrCustomResults() throws Exception {
		java.sql.Time raw = java.sql.Time.valueOf("12:34:56");
		SQLException brokenMetadata = new SQLFeatureNotSupportedException("typed getter unavailable") {
			@Override public String getSQLState() { throw new IllegalStateException("unreadable SQLState"); }
		};
		for (Throwable unsupported : List.of(new SQLFeatureNotSupportedException("typed getter unavailable"),
				new SQLException("typed getter unavailable", "0A000"), new SQLException("typed getter unavailable", "HYC00"),
				new UnsupportedOperationException("typed getter unavailable"), new AbstractMethodError("typed getter unavailable"), brokenMetadata)) {
			List<String> events = new ArrayList<>();
			DatabaseException failure = assertThrows(DatabaseException.class, () -> mapArray(ResultSetMapper.withDefaultConfiguration(),
					ZoneId.of("UTC"), timeArray(new java.sql.Time[]{raw}, new Object[]{null}, Types.TIME, "time",
					events, "root", unsupported, null, null), LocalTime[].class));
			assertSame(unsupported, failure.getCause());
			assertEquals(List.of("root:close", "root:free"), events);
			Map<?, ?> nativeRow = mapArray(ResultSetMapper.withDefaultConfiguration(), ZoneId.of("UTC"), timeArray(
					new java.sql.Time[]{raw}, new Object[]{null}, Types.TIME, "time", new ArrayList<>(), "root",
					unsupported, null, null), Map.class);
			assertSame(raw, ((Object[]) nativeRow.get("value"))[0]);
			LocalTime supplied = LocalTime.parse("12:34:56.123456789");
			ResultSetMapper custom = ResultSetMapper.withPlanCachingEnabled(true).customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.getRawClass() == LocalTime[].class; }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object value,
						TargetType target, Integer column, String label, InstanceProvider provider) {
					assertSame(raw, ((Object[]) value)[0]);
					return MappingResult.of(new LocalTime[]{supplied});
				}
			})).build();
			assertArrayEquals(new LocalTime[]{supplied}, mapArray(custom, ZoneId.of("UTC"), timeArray(
					new java.sql.Time[]{raw}, new Object[]{null}, Types.TIME, "time", new ArrayList<>(), "root",
					unsupported, null, null), LocalTime[].class));
		}
	}

	@Test
	void plainTimeReaderFailureStillClosesEveryNestedHandle() {
		SQLException failure = new SQLException("connection lost", "08006");
		java.sql.Time raw = java.sql.Time.valueOf("12:34:56");
		List<String> events = new ArrayList<>();
		SQLException thrown = assertThrows(SQLException.class, () -> mapArray(ResultSetMapper.withDefaultConfiguration(),
				ZoneId.of("UTC"), timeArray(new Object[]{new java.sql.Time[]{raw}}, new Object[]{new Object[]{null}},
				Types.TIME, "time", events, "root", failure, failure, failure), Map.class));
		assertSame(failure, thrown);
		assertEquals(0, failure.getSuppressed().length);
		assertEquals(List.of("root.0:close", "root.0:free", "root:close", "root:free"), events);
	}

	@Test
	void scalarZonedLocalTimeUsesOffsetReaderAndRawOffsetFallback() throws Exception {
		OffsetTime expected = OffsetTime.parse("12:34:56.123456789+05:45");
		for (boolean cached : new boolean[]{false, true}) {
			for (boolean typed : new boolean[]{false, true}) {
				for (int jdbcType : new int[]{Types.TIME, Types.TIME_WITH_TIMEZONE}) {
					ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
					ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
							new Class<?>[]{ResultSetMetaData.class}, (proxy, method, args) -> switch (method.getName()) {
								case "getColumnCount" -> 1;
								case "getColumnLabel", "getColumnName" -> "value";
								case "getColumnType" -> jdbcType;
								case "getColumnTypeName" -> "timetz";
								default -> throw new AssertionError(method.getName());
							});
					ResultSet rows = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
							(proxy, method, args) -> switch (method.getName()) {
								case "getMetaData" -> metadata;
								case "getObject" -> {
									if (args.length > 1) {
										assertEquals(OffsetTime.class, args[1], "A zoned time must not use the driver's LocalTime conversion");
										if (!typed) throw new SQLFeatureNotSupportedException("typed getter unavailable");
									}
									yield expected;
								}
								case "wasNull" -> false;
								default -> throw new AssertionError(method.getName());
							});
					Database database = database(mapper, ZoneId.of("UTC"));
					StatementContext<LocalTime> context = StatementContext.<LocalTime>with(Statement.of("zoned_time", "SELECT value"), database)
							.resultSetRowType(LocalTime.class).build();
					assertEquals(expected.toLocalTime(), mapper.map(context, rows, LocalTime.class, new InstanceProvider() {}).orElseThrow());
				}
			}
		}
	}

	private static Array timeArray(Object[] raw, Object[] typed, int baseType, String name, List<String> events,
			String path, Throwable readFailure, Throwable closeFailure, Throwable freeFailure) {
		return (Array) Proxy.newProxyInstance(Array.class.getClassLoader(), new Class<?>[]{Array.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getArray" -> raw;
					case "getBaseType" -> baseType;
					case "getBaseTypeName" -> name;
					case "getResultSet" -> {
						AtomicInteger index = new AtomicInteger(-1);
						yield Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
								(row, operation, arguments) -> switch (operation.getName()) {
									case "next" -> index.incrementAndGet() < raw.length;
									case "getObject" -> {
										String normalizedName = name == null ? "" : name.toUpperCase(java.util.Locale.ROOT);
										boolean zoned = baseType == Types.TIME_WITH_TIMEZONE || normalizedName.equals("TIMETZ")
												|| normalizedName.contains("WITH TIME ZONE");
										assertEquals(zoned ? OffsetTime.class : LocalTime.class, arguments[1]);
										if (readFailure != null) throw readFailure;
										yield typed[index.get()];
									}
									case "getArray" -> timeArray((Object[]) raw[index.get()], (Object[]) typed[index.get()],
											baseType, name, events, path + "." + index.get(), readFailure, closeFailure, freeFailure);
									case "close" -> { events.add(path + ":close"); if (closeFailure != null) throw closeFailure; yield null; }
									default -> throw new AssertionError(operation.getName());
								});
					}
					case "free" -> { events.add(path + ":free"); if (freeFailure != null) throw freeFailure; yield null; }
					default -> throw new AssertionError(method.getName());
				});
	}

	private static Database database(ResultSetMapper mapper, ZoneId zone) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:temporal_array_mapping");
		source.setUser("sa");
		return Database.withDataSource(source).databaseType(DatabaseType.GENERIC).timeZone(zone).resultSetMapper(mapper).build();
	}

	private static <T> T mapArray(ResultSetMapper mapper, ZoneId zone, Integer baseType, Object[] values, Class<T> target) throws SQLException {
		return mapArray(mapper, zone, baseType, null, values, target);
	}

	private static <T> T mapArray(ResultSetMapper mapper, ZoneId zone, Integer baseType, String baseTypeName, Object[] values, Class<T> target) throws SQLException {
		AtomicInteger frees = new AtomicInteger();
		AtomicInteger reads = new AtomicInteger();
		Array array = (Array) Proxy.newProxyInstance(Array.class.getClassLoader(), new Class<?>[]{Array.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getBaseTypeName" -> baseTypeName;
					case "getBaseType" -> {
						if (baseType == null) throw new SQLFeatureNotSupportedException("base type unavailable");
						if (baseType == Integer.MIN_VALUE) throw new UnsupportedOperationException("base type unavailable");
						if (baseType == Integer.MAX_VALUE) throw new AbstractMethodError("base type unavailable");
						yield baseType;
					}
					case "getArray" -> { assertEquals(0, frees.get()); reads.incrementAndGet(); yield values; }
					case "free" -> { frees.incrementAndGet(); yield null; }
					default -> throw new AssertionError(method.getName());
				});
		T result = mapArray(mapper, zone, array, target);
		assertEquals(1, reads.get());
		assertEquals(1, frees.get());
		return result;
	}

	private static <T> T mapArray(ResultSetMapper mapper, ZoneId zone, Array array, Class<T> target) throws SQLException {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class}, (proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnLabel", "getColumnName" -> "value";
					case "getColumnType" -> Types.ARRAY;
					case "getColumnTypeName" -> "ARRAY";
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					default -> throw new AssertionError(method.getName());
				});
		ResultSet rs = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject" -> array;
					case "wasNull" -> false;
					default -> throw new AssertionError(method.getName());
				});
		StatementContext<T> context = StatementContext.<T>with(Statement.of("temporal_array", "SELECT value"), database(mapper, zone))
				.resultSetRowType(target).build();
		return mapper.map(context, rs, target, new InstanceProvider() {}).orElseThrow();
	}
}
