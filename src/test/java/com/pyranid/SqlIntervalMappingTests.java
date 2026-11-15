/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Period;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SqlIntervalMappingTests {
	private static final SqlInterval EXPECTED = SqlInterval.of(14, -3, Duration.ofSeconds(4, 123456000));
	public record IntervalRow(SqlInterval value) {}
	public record OptionalRow(Optional<SqlInterval> value) {}
	public record ArrayRow(SqlInterval[] value) {}
	public record ListRow(List<SqlInterval> value) {}
	public record StructRow(IntervalRow value) {}
	public record Recursive<T>(T value, Recursive<SqlInterval> next) {}
	public record RecursiveStructRow(Recursive<String> value) {}
	public record DurationRow(Duration value) {}
	public record PeriodRow(Period value) {}
	public record FanOut(@DatabaseColumn("value") SqlInterval interval, @DatabaseColumn("value") Object nativeValue) {}
	public record TwoIntervals(@DatabaseColumn("value") SqlInterval first, @DatabaseColumn("value") SqlInterval second) {}
	public static class IntervalBean {
		private SqlInterval value = EXPECTED;
		private int calls;
		public SqlInterval getValue() { return value; }
		public void setValue(SqlInterval value) { this.value = value; calls++; }
	}

	@Test
	void directTypedTargetsNeverExtractALossyOrThrowingRawInterval() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = mapper(cached, List.of());
			for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class, IntervalBean.class)) {
				Row row = new Row("1 year 2 mons -3 days 00:00:04.123456", new Object());
				row.rawFailure = new SQLException("PGInterval overflow", "22003");
				for (int repeat = 0; repeat < 2; repeat++) {
					Object value = map(mapper, row, shape).orElseThrow();
					assertEquals(EXPECTED, value instanceof IntervalRow r ? r.value()
							: value instanceof IntervalBean b ? b.getValue() : value);
				}
				assertEquals(0, row.rawReads.get());
				assertEquals(2, row.textReads.get());
			}
		}
	}

	@Test
	void binaryNonNullNullsUseTypedReadersAcrossAllRowShapes() throws Exception {
		SqlInterval minimum = SqlInterval.of(0, 0, SqlIntervalText.fromMicroseconds(Long.MIN_VALUE));
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = mapper(cached, List.of());
			for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class, IntervalBean.class)) {
				Row row = new Row(null, null);
				row.bytes = ByteBuffer.allocate(16).putLong(Long.MIN_VALUE).putInt(0).putInt(0).array();
				Object value = map(mapper, row, shape).orElseThrow();
				assertEquals(minimum, value instanceof IntervalRow r ? r.value()
						: value instanceof IntervalBean b ? b.getValue() : value);
				assertEquals(0, row.rawReads.get());
				assertEquals(1, row.byteReads.get());
			}
		}
	}

	@Test
	void trueSqlNullPreservesOuterOptionalNullablePropertiesAndSetterCalls() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = mapper(cached, List.of());
			Row row = new Row(null, null); row.sqlNull = true;
			assertTrue(map(mapper, row, SqlInterval.class).isEmpty());
			assertNull(map(mapper, row, IntervalRow.class).orElseThrow().value());
			IntervalBean bean = map(mapper, row, IntervalBean.class).orElseThrow();
			assertNull(bean.getValue()); assertEquals(1, bean.calls);
			assertEquals(0, row.rawReads.get());
			assertThrows(DatabaseException.class, () -> map(mapper, row, OptionalRow.class));
		}
	}

	@Test
	void hooksReceiveRawValuesAndFallbackRereadsTheOriginalInterval() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (boolean handled : new boolean[]{false, true}) {
				Object nativeValue = new Object();
				AtomicInteger calls = new AtomicInteger();
				ResultSetMapper mapper = mapper(cached, List.of(hook(nativeValue, calls, handled)));
				for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class, IntervalBean.class)) {
					Row row = new Row("1 year 2 mons -3 days 00:00:04.123456", nativeValue);
					Object value = map(mapper, row, shape).orElseThrow();
					assertEquals(handled ? SqlInterval.ZERO : EXPECTED, value instanceof IntervalRow r ? r.value()
							: value instanceof IntervalBean b ? b.getValue() : value);
					assertEquals(1, row.rawReads.get());
					assertEquals(handled ? 0 : 1, row.textReads.get());
				}
				assertEquals(3, calls.get());
			}
		}
	}

	@Test
	void fanOutDoesNotConfuseTypedAndRawRepresentationsOrFreeBorrowedHandles() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			AtomicInteger frees = new AtomicInteger();
			Blob nativeValue = (Blob) Proxy.newProxyInstance(Blob.class.getClassLoader(), new Class<?>[]{Blob.class},
					(proxy, method, args) -> { if (method.getName().equals("free")) { frees.incrementAndGet(); return null; }
						throw new AssertionError("Native borrowed handle must not be inspected: " + method); });
			for (boolean hook : new boolean[]{false, true}) {
				AtomicInteger calls = new AtomicInteger();
				ResultSetMapper mapper = mapper(cached, hook ? List.of(hook(nativeValue, calls, false)) : List.of());
				Row row = new Row("1 year 2 mons -3 days 00:00:04.123456", nativeValue);
				FanOut result = map(mapper, row, FanOut.class).orElseThrow();
				assertEquals(EXPECTED, result.interval());
				assertSame(nativeValue, result.nativeValue());
				assertEquals(1, row.rawReads.get()); assertEquals(1, row.textReads.get());
				assertEquals(hook ? 1 : 0, calls.get()); assertEquals(0, frees.get());
			}
			Row same = new Row("1 year 2 mons -3 days 00:00:04.123456", nativeValue);
			TwoIntervals result = map(mapper(cached, List.of()), same, TwoIntervals.class).orElseThrow();
			assertEquals(EXPECTED, result.first()); assertSame(result.first(), result.second());
			assertEquals(0, same.rawReads.get()); assertEquals(1, same.textReads.get());
		}
	}

	@Test
	void requiredRawFailuresAndNonNullNullsRemainFailuresOnlyInNewIntervalPlans() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Object raw = new Object(); AtomicInteger calls = new AtomicInteger();
			ResultSetMapper hooked = mapper(cached, List.of(hook(raw, calls, false)));
			for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class, FanOut.class)) {
				Row row = new Row("1 day", null);
				DatabaseException unavailable = assertThrows(DatabaseException.class, () -> map(hooked, row, shape));
				assertTrue(unavailable.getMessage().contains("raw"));
				SQLException failure = new SQLException("original driver failure", "22003");
				row.rawFailure = failure;
				assertSame(failure, assertThrows(SQLException.class, () -> map(hooked, row, shape)));
			}
			assertEquals(0, calls.get());
			Row rawNull = new Row(null, null);
			assertTrue(map(mapper(cached, List.of()), rawNull, Object.class).isEmpty());
			Map<?, ?> nativeMap = map(mapper(cached, List.of()), rawNull, Map.class).orElseThrow();
			assertTrue(nativeMap.containsKey("value")); assertNull(nativeMap.get("value"));
		}
	}

	@Test
	void nonIntervalColumnsAndUnsupportedContainersFailWithoutInventingARepresentation() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = mapper(cached, List.of());
			for (boolean sqlNull : new boolean[]{false, true}) {
				Row text = new Row(sqlNull ? null : "PT1H", sqlNull ? null : "PT1H");
				text.type = Types.VARCHAR; text.typeName = "varchar"; text.sqlNull = sqlNull;
				for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class))
					assertThrows(DatabaseException.class, () -> map(mapper, text, shape));
			}
			for (Class<?> shape : List.of(ArrayRow.class, ListRow.class, StructRow.class, SqlInterval[].class)) {
				Row array = new Row(null, new String[]{"1 day"}); array.type = Types.ARRAY; array.typeName = "interval[]";
				DatabaseException failure = assertThrows(DatabaseException.class, () -> map(mapper, array, shape));
				assertTrue(failure.getMessage().contains("SqlInterval"));
				assertEquals(0, array.rawReads.get(), "Unsupported declarations fail before lossy interval extraction");
			}
			Row struct = new Row(null, Map.of("value", "1 day")); struct.type = Types.STRUCT; struct.typeName = "interval_record";
			assertThrows(DatabaseException.class, () -> map(mapper, struct, IntervalRow.class));
			assertEquals(0, struct.rawReads.get());
		}
	}

	@Test
	void recursiveGenericStructDeclarationsCannotHideAnUnsupportedIntervalBehindTheRecursionGuard() throws Exception {
		for (boolean cached : new boolean[]{false, true}) for (boolean sqlNull : new boolean[]{false, true}) {
			ResultSetMapper mapper = mapper(cached, List.of());
			Row struct = new Row(null, sqlNull ? null : Map.of("value", "outer", "next", Map.of("value", EXPECTED)));
			struct.type = Types.STRUCT;
			struct.typeName = "recursive_interval_record";
			struct.sqlNull = sqlNull;
			struct.rawFailure = new SQLException("Unsupported declarations must fail before extraction", "22003");
			DatabaseException failure = assertThrows(DatabaseException.class,
					() -> map(mapper, struct, RecursiveStructRow.class));
			assertTrue(failure.getMessage().contains("SqlInterval"));
			assertEquals(0, struct.rawReads.get());
		}
	}

	@Test
	void trueSqlNullDoesNotInvokeCustomHooksOrTypedFallbackAndStillCallsNullableSetters() throws Exception {
		for (boolean cached : new boolean[]{false, true}) for (boolean handled : new boolean[]{false, true}) {
			AtomicInteger calls = new AtomicInteger();
			ResultSetMapper mapper = mapper(cached, List.of(hook(new Object(), calls, handled)));
			for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class, IntervalBean.class, FanOut.class)) {
				Row row = new Row(null, null);
				row.sqlNull = true;
				Optional<?> result = map(mapper, row, shape);
				if (shape == SqlInterval.class) assertTrue(result.isEmpty());
				else if (result.orElseThrow() instanceof IntervalRow record) assertNull(record.value());
				else if (result.orElseThrow() instanceof IntervalBean bean) {
					assertNull(bean.getValue());
					assertEquals(1, bean.calls);
				} else {
					FanOut fanOut = (FanOut) result.orElseThrow();
					assertNull(fanOut.interval());
					assertNull(fanOut.nativeValue());
				}
				assertEquals(1, row.rawReads.get());
				assertEquals(0, row.textReads.get());
			}
			assertEquals(0, calls.get());
		}
	}

	@Test
	void wholeContainerOverridesMayHandleButCannotFallbackIntoUnsupportedMapping() throws Exception {
		for (boolean cached : new boolean[]{false, true}) for (boolean handled : new boolean[]{false, true}) {
			AtomicInteger calls = new AtomicInteger();
			CustomColumnMapper hook = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(SqlInterval[].class); }
				@Override public MappingResult map(StatementContext<?> c, ResultSet rs, Object raw, TargetType target,
						Integer column, String label, InstanceProvider ip) {
					calls.incrementAndGet(); return handled ? MappingResult.of(new SqlInterval[]{SqlInterval.ZERO}) : MappingResult.fallback();
				}
			};
			ResultSetMapper mapper = mapper(cached, List.of(hook));
			for (Class<?> shape : List.of(SqlInterval[].class, ArrayRow.class)) {
				Row row = new Row(null, new String[]{"1 day"}); row.type = Types.ARRAY; row.typeName = "interval[]";
				if (handled) {
					Object result = map(mapper, row, shape).orElseThrow();
					assertArrayEquals(new SqlInterval[]{SqlInterval.ZERO}, result instanceof ArrayRow r ? r.value() : (SqlInterval[]) result);
				} else assertThrows(DatabaseException.class, () -> map(mapper, row, shape));
			}
			assertEquals(2, calls.get());
		}
	}

	@Test
	void nativeDurationPeriodAndNullPropertiesKeepTheirExistingIdentityBehavior() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = mapper(cached, List.of());
			Duration duration = Duration.ofNanos(1); Period period = Period.of(1, 2, 3);
			assertSame(duration, map(mapper, new Row(null, duration), DurationRow.class).orElseThrow().value());
			assertSame(period, map(mapper, new Row(null, period), PeriodRow.class).orElseThrow().value());
			Row nil = new Row(null, null); nil.sqlNull = true;
			assertNull(map(mapper, nil, DurationRow.class).orElseThrow().value());
			assertNull(map(mapper, nil, PeriodRow.class).orElseThrow().value());
		}
	}

	private static CustomColumnMapper hook(Object expected, AtomicInteger calls, boolean handled) {
		return new CustomColumnMapper() {
			@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(SqlInterval.class); }
			@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
					Integer column, String label, InstanceProvider instances) {
				assertSame(expected, raw); calls.incrementAndGet();
				return handled ? MappingResult.of(SqlInterval.ZERO) : MappingResult.fallback();
			}
		};
	}

	private static ResultSetMapper mapper(boolean cached, List<CustomColumnMapper> hooks) {
		return ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(hooks).build();
	}
	private static <T> Optional<T> map(ResultSetMapper mapper, Row row, Class<T> shape) throws SQLException {
		Database database = Database.with(new JDBCDataSource(), DatabaseType.POSTGRESQL).resultSetMapper(mapper).build();
		StatementContext<T> context = StatementContext.<T>with(Statement.of("interval-mapping", "SELECT value"), database).resultSetRowType(shape).build();
		return mapper.map(context, row.rows, shape, new InstanceProvider() {});
	}

	private static final class Row {
		final String text; final Object raw;
		boolean sqlNull, lastNull; int type = Types.OTHER; String typeName = "interval";
		byte[] bytes; SQLException rawFailure;
		final AtomicInteger rawReads = new AtomicInteger(), textReads = new AtomicInteger(), byteReads = new AtomicInteger();
		final ResultSet rows;
		Row(String text, Object raw) {
			this.text = text; this.raw = raw;
			ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getColumnCount" -> 1;
						case "getColumnLabel", "getColumnName" -> "value";
						case "getColumnType" -> type;
						case "getColumnTypeName" -> typeName;
						case "getColumnClassName" -> Object.class.getName();
						default -> method.getReturnType() == int.class ? 0 : method.getReturnType() == boolean.class ? false : "";
					});
			this.rows = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ResultSet.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getMetaData" -> metadata;
						case "getObject" -> { rawReads.incrementAndGet(); if (rawFailure != null) throw rawFailure;
							lastNull = raw == null && sqlNull; yield raw; }
						case "getString" -> { textReads.incrementAndGet(); lastNull = text == null && sqlNull; yield text; }
						case "getBytes" -> { byteReads.incrementAndGet(); lastNull = bytes == null && sqlNull; yield bytes; }
						case "wasNull" -> lastNull;
						case "close" -> null;
						case "hashCode" -> System.identityHashCode(proxy);
						case "equals" -> proxy == args[0];
						case "toString" -> "interval-result";
						default -> throw new AssertionError(method);
					});
		}
	}
}
