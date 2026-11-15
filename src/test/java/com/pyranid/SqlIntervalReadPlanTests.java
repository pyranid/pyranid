/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Period;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class SqlIntervalReadPlanTests {
	private static final SqlInterval EXPECTED = SqlInterval.of(14, -3, Duration.ofSeconds(4, 123_456_000));
	public record IntervalRow(SqlInterval value) {}
	public record FanOut(@DatabaseColumn("value") SqlInterval interval, @DatabaseColumn("value") Object nativeValue) {}
	public record TwoIntervals(@DatabaseColumn("value") SqlInterval first, @DatabaseColumn("value") SqlInterval second) {}
	public static class IntervalBean {
		private SqlInterval value;
		public SqlInterval getValue() { return this.value; }
		public void setValue(SqlInterval value) { this.value = value; }
	}

	@Test
	void cachedDirectRowPlansReadIntervalMetadataOnlyDuringPlanning() throws Exception {
		for (Class<?> shape : List.of(IntervalRow.class, IntervalBean.class, FanOut.class, TwoIntervals.class)) {
			DefaultResultSetMapper mapper = mapper(true, List.of());
			Row row = new Row(); StatementContext<?> context = context(mapper, DatabaseType.POSTGRESQL, shape);
			assertInterval(EXPECTED, map(mapper, context, row, shape));
			int types = row.types.get(), names = row.names.get(), metadata = row.metadataReads.get();
			for (int repeat = 0; repeat < 4; repeat++) assertInterval(EXPECTED, map(mapper, context, row, shape));
			assertEquals(types, row.types.get(), shape.getName());
			assertEquals(names, row.names.get(), shape.getName());
			assertEquals(metadata + 4, row.metadataReads.get(), "Only map's existing metadata acquisition remains per row");
			assertEquals(5, row.textReads.get(), "Fan-out still shares one typed read per row");
			assertEquals(shape == FanOut.class ? 5 : 0, row.rawReads.get());
		}
	}

	@Test
	void scalarAndRowFallbackCacheFactsAfterCallbacksWithoutAddingWarmMetadataReads() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class, IntervalBean.class, FanOut.class)) {
				Row row = new Row(); AtomicReference<int[]> afterHook = new AtomicReference<>();
				CustomColumnMapper fallback = hook((context, rows) -> {
					afterHook.set(new int[]{row.types.get(), row.names.get(), row.metadataReads.get()});
					return CustomColumnMapper.MappingResult.fallback();
				});
				DefaultResultSetMapper mapper = mapper(cached, List.of(fallback));
				StatementContext<?> context = context(mapper, DatabaseType.POSTGRESQL, shape);
				assertInterval(EXPECTED, map(mapper, context, row, shape));
				for (int repeat = 0; repeat < 3; repeat++) {
					assertInterval(EXPECTED, map(mapper, context, row, shape));
					assertArrayEquals(afterHook.get(), new int[]{row.types.get(), row.names.get(), row.metadataReads.get()},
							"Warm fallback adds no metadata demand after the hook");
				}
			}
			DefaultResultSetMapper scalar = mapper(cached, List.of()); Row row = new Row();
			StatementContext<?> context = context(scalar, DatabaseType.POSTGRESQL, SqlInterval.class);
			assertEquals(EXPECTED, map(scalar, context, row, SqlInterval.class));
			int types = row.types.get(), names = row.names.get(), metadata = row.metadataReads.get();
			for (int repeat = 0; repeat < 4; repeat++) assertEquals(EXPECTED, map(scalar, context, row, SqlInterval.class));
			assertEquals(types, row.types.get()); assertEquals(names, row.names.get());
			assertEquals(metadata + 4, row.metadataReads.get());
		}
	}

	@Test
	void handledHooksAndTrueNullsNeverDemandIntervalClassification() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class, IntervalBean.class, FanOut.class)) {
				Row row = new Row(); row.type = Types.INTEGER; row.typeName = "INTEGER";
				AtomicInteger calls = new AtomicInteger();
				CustomColumnMapper handled = hook((context, rows) -> {
					calls.incrementAndGet(); row.failNames = true;
					return CustomColumnMapper.MappingResult.of(SqlInterval.ZERO);
				});
				DefaultResultSetMapper mapper = mapper(cached, List.of(handled));
				StatementContext<?> context = context(mapper, DatabaseType.POSTGRESQL, shape);
				assertInterval(SqlInterval.ZERO, map(mapper, context, row, shape));
				assertEquals(1, calls.get()); assertEquals(0, row.textReads.get());
				row.failNames = false; row.sqlNull = true; row.raw = null;
				assertInterval(null, map(mapper, context, row, shape));
				assertEquals(1, calls.get(), "True SQL NULL still suppresses custom callbacks and fallback");
				assertEquals(0, row.textReads.get());
			}
		}
	}

	@Test
	void failedFallbackMetadataIsNotPublishedAndCanBeRetriedOnTheSameCursor() throws Exception {
		for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class)) {
			Row row = new Row(); AtomicBoolean fail = new AtomicBoolean(true);
			DefaultResultSetMapper mapper = mapper(true, List.of(hook((context, rows) -> {
				row.failNames = fail.get(); return CustomColumnMapper.MappingResult.fallback();
			})));
			StatementContext<?> context = context(mapper, DatabaseType.POSTGRESQL, shape);
			Throwable failure = assertThrows(Throwable.class, () -> map(mapper, context, row, shape));
			while (failure != row.metadataFailure && failure.getCause() != null) failure = failure.getCause();
			assertSame(row.metadataFailure, failure);
			assertEquals(0, row.textReads.get());
			fail.set(false); row.failNames = false;
			assertInterval(EXPECTED, map(mapper, context, row, shape));
			int types = row.types.get(), names = row.names.get();
			assertInterval(EXPECTED, map(mapper, context, row, shape));
			if (shape != SqlInterval.class) { assertEquals(types, row.types.get()); assertEquals(names, row.names.get()); }
		}
	}

	@Test
	void sharedSchemaPlansAndExecutionCachesUseTheCurrentDatabaseForEveryRead() throws Exception {
		for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class)) {
			DefaultResultSetMapper mapper = mapper(true, List.of()); Row row = new Row();
			assertInterval(EXPECTED, map(mapper, context(mapper, DatabaseType.POSTGRESQL, shape), row, shape));
			int reads = row.textReads.get();
			assertThrows(DatabaseException.class, () -> map(mapper, context(mapper, DatabaseType.GENERIC, shape), row, shape));
			assertEquals(reads, row.textReads.get(), "GENERIC still requires a native YEAR-MONTH or DAY-SECOND family");
			assertInterval(EXPECTED, map(mapper, context(mapper, DatabaseType.POSTGRESQL, shape), row, shape));

			Row vendor = new Row(); vendor.type = -103; vendor.typeName = ""; vendor.raw = Period.ofMonths(14);
			SqlInterval months = SqlInterval.of(Period.ofMonths(14));
			assertInterval(months, map(mapper, context(mapper, DatabaseType.ORACLE, shape), vendor, shape));
			assertThrows(DatabaseException.class, () -> map(mapper, context(mapper, DatabaseType.GENERIC, shape), vendor, shape));
			assertInterval(months, map(mapper, context(mapper, DatabaseType.ORACLE, shape), vendor, shape));
		}
	}

	@Test
	void cachedReadersKeepBinaryNullEvidenceAndRefreshForAnotherCursor() throws Exception {
		for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class, TwoIntervals.class)) {
			DefaultResultSetMapper mapper = mapper(true, List.of()); Row binary = new Row(); binary.text = null;
			binary.bytes = ByteBuffer.allocate(16).putLong(Long.MIN_VALUE).putInt(-3).putInt(14).array();
			SqlInterval expected = SqlInterval.of(14, -3, SqlIntervalText.fromMicroseconds(Long.MIN_VALUE));
			StatementContext<?> context = context(mapper, DatabaseType.POSTGRESQL, shape);
			assertInterval(expected, map(mapper, context, binary, shape));
			assertInterval(expected, map(mapper, context, binary, shape));
			assertEquals(2, binary.byteReads.get()); assertEquals(0, binary.rawReads.get());
			Row other = new Row(); other.type = Types.VARCHAR; other.typeName = "VARCHAR";
			assertThrows(DatabaseException.class, () -> map(mapper, context, other, shape));
			assertEquals(0, other.textReads.get());
			assertInterval(expected, map(mapper, context, binary, shape));
		}
	}

	@Test
	void reusableScopeKeepsOnlyWeakCursorIdentityAndEnumFactsAndRetriesFailedMetadata() throws Exception {
		MappingResources resources = new MappingResources(null, null); Row first = new Row(), second = new Row();
		SqlIntervalSupport.SourceColumn descriptor = resources.intervalColumn(first.rows, first.metadata, 1);
		resources.close();
		assertSame(descriptor, resources.intervalColumn(first.rows, null, 1));
		for (Field field : descriptor.getClass().getDeclaredFields()) {
			field.setAccessible(true); assertInstanceOf(Enum.class, field.get(descriptor));
		}
		Field cursorField = MappingResources.class.getDeclaredField("intervalResultSet"); cursorField.setAccessible(true);
		Object weak = cursorField.get(resources); assertSame(WeakReference.class, weak.getClass());
		assertSame(first.rows, ((WeakReference<?>) weak).get());
		second.failNames = true;
		assertSame(second.metadataFailure, assertThrows(SQLException.class, () -> resources.intervalColumn(second.rows, second.metadata, 1)));
		assertSame(weak, cursorField.get(resources), "Failed new-cursor metadata must not replace the successful entry");
		second.failNames = false;
		assertNotSame(descriptor, resources.intervalColumn(second.rows, second.metadata, 1));
		((WeakReference<?>) cursorField.get(resources)).clear();
		int reads = second.names.get(); resources.intervalColumn(second.rows, second.metadata, 1);
		assertEquals(reads + 1, second.names.get(), "A cleared weak cursor identity requires a fresh descriptor");
		Reference.reachabilityFence(first); Reference.reachabilityFence(second);
	}


	@Test
	void unsupportedFamilyValidationWaitsUntilTheBindingIsActuallyRead() throws Exception {
		AtomicInteger calls = new AtomicInteger();
		DefaultResultSetMapper mapper = mapper(false, List.of(new CustomColumnMapper() {
			@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Integer.class); }
			@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
					Integer index, String label, InstanceProvider instances) {
				calls.incrementAndGet(); return MappingResult.of(7);
			}
		}));
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class}, (proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> 2;
					case "getColumnLabel", "getColumnName" -> (int) args[0] == 1 ? "first" : "value";
					case "getColumnType" -> Types.VARCHAR;
					case "getColumnTypeName" -> "VARCHAR";
					default -> throw new AssertionError(method);
				});
		ResultSet rows = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject" -> "opaque";
					case "wasNull" -> false;
					default -> throw new AssertionError(method);
				});
		StatementContext<EarlierHook> context = context(mapper, DatabaseType.POSTGRESQL, EarlierHook.class);
		assertThrows(DatabaseException.class, () -> mapper.map(context, rows, EarlierHook.class, new InstanceProvider() {}));
		assertEquals(1, calls.get(), "Plan construction must not reject an interval family before an earlier column's hook");
	}
	public record EarlierHook(Integer first, SqlInterval value) {}

	@Test
	void intervalFastPathsKeepProtectedStandardTypeAndPlanOverridesInTheDispatch() throws Exception {
		class ObservingMapper extends DefaultResultSetMapper {
			int standardCalls, planCalls;
			ObservingMapper() { super(ResultSetMapper.withPlanCachingEnabled(true)); }
			@Override protected <T> StandardTypeResult<T> mapResultSetToStandardType(StatementContext<T> context, ResultSet rows,
					Class<T> type, InstanceProvider instances, ResultSetMetaData metadata, MappingResources resources) throws Exception {
				this.standardCalls++;
				return super.mapResultSetToStandardType(context, rows, type, instances, metadata, resources);
			}
			@Override protected <T> RowPlan<T> buildPlan(StatementContext<T> context, ResultSet rows, Class<T> type,
					InstanceProvider instances, ResultSetMetaData metadata, String signature) throws Exception {
				this.planCalls++;
				return super.buildPlan(context, rows, type, instances, metadata, signature);
			}
		}
		ObservingMapper mapper = new ObservingMapper(); Row row = new Row();
		for (Class<?> shape : List.of(SqlInterval.class, IntervalRow.class)) {
			StatementContext<?> context = context(mapper, DatabaseType.POSTGRESQL, shape);
			for (int repeat = 0; repeat < 2; repeat++) assertInterval(EXPECTED, map(mapper, context, row, shape));
		}
		assertEquals(4, mapper.standardCalls); assertEquals(2, mapper.planCalls);
	}

	private interface Hook { CustomColumnMapper.MappingResult map(StatementContext<?> context, ResultSet rows) throws SQLException; }
	private static CustomColumnMapper hook(Hook hook) {
		return new CustomColumnMapper() {
			@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(SqlInterval.class); }
			@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
					Integer index, String label, InstanceProvider instances) throws SQLException { return hook.map(context, rows); }
		};
	}
	private static DefaultResultSetMapper mapper(boolean cached, List<CustomColumnMapper> hooks) {
		return (DefaultResultSetMapper) ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(hooks).build();
	}
	private static <T> StatementContext<T> context(ResultSetMapper mapper, DatabaseType type, Class<T> shape) {
		Database database = Database.with(new JDBCDataSource(), type).resultSetMapper(mapper).build();
		return StatementContext.<T>with(Statement.of("interval-read-plan", "SELECT value"), database).resultSetRowType(shape).build();
	}
	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Object map(ResultSetMapper mapper, StatementContext<?> context, Row row, Class<?> shape) throws SQLException {
		return mapper.map((StatementContext) context, row.rows, shape, new InstanceProvider() {}).orElse(null);
	}
	private static void assertInterval(SqlInterval expected, Object result) {
		if (result instanceof IntervalRow row) assertEquals(expected, row.value());
		else if (result instanceof IntervalBean bean) assertEquals(expected, bean.getValue());
		else if (result instanceof FanOut row) assertEquals(expected, row.interval());
		else if (result instanceof TwoIntervals row) { assertEquals(expected, row.first()); assertSame(row.first(), row.second()); }
		else assertEquals(expected, result);
	}

	private static final class Row {
		String text = "1 year 2 mons -3 days 00:00:04.123456"; Object raw = new Object();
		int type = Types.OTHER; String typeName = "interval";
		boolean sqlNull, lastNull, failNames; byte[] bytes;
		final SQLException metadataFailure = new SQLException("interval metadata unavailable", "0A000");
		final AtomicInteger types = new AtomicInteger(), names = new AtomicInteger(), metadataReads = new AtomicInteger();
		final AtomicInteger rawReads = new AtomicInteger(), textReads = new AtomicInteger(), byteReads = new AtomicInteger();
		final ResultSet rows; final ResultSetMetaData metadata;
		Row() {
			this.metadata = (ResultSetMetaData) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getColumnCount" -> 1;
						case "getColumnLabel", "getColumnName" -> "value";
						case "getColumnType" -> { types.incrementAndGet(); yield type; }
						case "getColumnTypeName" -> { names.incrementAndGet(); if (failNames) throw metadataFailure; yield typeName; }
						case "hashCode" -> System.identityHashCode(proxy);
						case "equals" -> proxy == args[0];
						default -> method.getReturnType() == int.class ? 0 : method.getReturnType() == boolean.class ? false : "";
					});
			this.rows = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ResultSet.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getMetaData" -> { metadataReads.incrementAndGet(); yield metadata; }
						case "getObject" -> { rawReads.incrementAndGet(); lastNull = sqlNull; yield sqlNull ? null : raw; }
						case "getString" -> { textReads.incrementAndGet(); lastNull = sqlNull; yield sqlNull ? null : text; }
						case "getBytes" -> { byteReads.incrementAndGet(); lastNull = sqlNull; yield sqlNull ? null : bytes; }
						case "wasNull" -> lastNull;
						case "hashCode" -> System.identityHashCode(proxy);
						case "equals" -> proxy == args[0];
						case "close" -> null;
						default -> throw new AssertionError(method);
					});
		}
	}
}
