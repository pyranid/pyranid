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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SqlIntervalReadRetentionTests {
	public record IntervalRow(SqlInterval value) {}

	@Test
	void liveMapperContextPlanAndPopulatedScopeDoNotRetainIntervalCursorOrMetadata() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Retained owners = primeIntervalReaders(cached);
			try {
				System.gc();
				assertFalse(owners.cursor.refersTo(null), "The explicit strong cursor control must remain alive");
				assertFalse(owners.metadata.refersTo(null), "The explicit strong metadata control must remain alive");
				Arrays.fill(owners.controls, null);
				long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
				for (int attempt = 0; attempt < 100 && System.nanoTime() < deadline
						&& (!owners.cursor.refersTo(null) || !owners.metadata.refersTo(null)); attempt++) {
					System.gc(); Thread.sleep(25L);
				}
				assertTrue(owners.cursor.refersTo(null), "Interval reader cache retained its cursor; planCachingEnabled=" + cached);
				assertTrue(owners.metadata.refersTo(null), "Interval reader cache retained JDBC metadata; planCachingEnabled=" + cached);
			} finally {
				Reference.reachabilityFence(owners.mapper);
				Reference.reachabilityFence(owners.context);
				Reference.reachabilityFence(owners.plan);
				Reference.reachabilityFence(owners.resources);
			}
		}
	}

	/** Keep all strong cursor/metadata locals on a frame that returns before requesting GC. */
	private static Retained primeIntervalReaders(boolean cached) throws Exception {
		RecordingMapper mapper = new RecordingMapper(cached);
		Database database = Database.with(new JDBCDataSource(), DatabaseType.POSTGRESQL).resultSetMapper(mapper).build();
		StatementContext<IntervalRow> context = StatementContext.<IntervalRow>with(Statement.of("interval-retention", "SELECT value"), database)
				.resultSetRowType(IntervalRow.class).build();
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(SqlIntervalReadRetentionTests.class.getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class}, (proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnLabel", "getColumnName" -> "value";
					case "getColumnType" -> Types.OTHER;
					case "getColumnTypeName" -> "interval";
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					default -> throw new AssertionError(method);
				});
		ResultSet rows = (ResultSet) Proxy.newProxyInstance(SqlIntervalReadRetentionTests.class.getClassLoader(),
				new Class<?>[]{ResultSet.class}, (proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getString" -> "1 day 00:00:01.000001";
					case "wasNull" -> false;
					case "close" -> null;
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					default -> throw new AssertionError(method);
				});
		SqlInterval expected = SqlInterval.of(0, 1, Duration.ofSeconds(1, 1_000));
		try (rows) {
			assertEquals(new IntervalRow(expected), mapper.map(context, rows, IntervalRow.class, new InstanceProvider() {}).orElseThrow());
			StatementContext<SqlInterval> scalar = StatementContext.<SqlInterval>with(context.getStatement(), database)
					.resultSetRowType(SqlInterval.class).build();
			assertEquals(expected, mapper.map(scalar, rows, SqlInterval.class, new InstanceProvider() {}).orElseThrow());
		}
		Field scopes = DefaultResultSetMapper.class.getDeclaredField("mappingResources"); scopes.setAccessible(true);
		ThreadLocal<?> local = (ThreadLocal<?>) scopes.get(mapper);
		MappingResources resources = (MappingResources) ((WeakReference<?>) local.get()).get();
		assertNotNull(resources); assertNotNull(mapper.lastPlan);
		Field columns = MappingResources.class.getDeclaredField("intervalColumns"); columns.setAccessible(true);
		assertFalse(((Map<?, ?>) columns.get(resources)).isEmpty(), "Prime the new descriptor cache before checking reclamation");
		return new Retained(mapper, context, mapper.lastPlan, resources, new WeakReference<>(rows),
				new WeakReference<>(metadata), new Object[]{rows, metadata});
	}

	private record Retained(DefaultResultSetMapper mapper, StatementContext<?> context, Object plan,
			MappingResources resources, WeakReference<ResultSet> cursor, WeakReference<ResultSetMetaData> metadata,
			Object[] controls) {}

	private static final class RecordingMapper extends DefaultResultSetMapper {
		private RowPlan<?> lastPlan;
		RecordingMapper(boolean cached) { super(ResultSetMapper.withPlanCachingEnabled(cached)); }
		@Override protected <T> RowPlan<T> buildPlan(StatementContext<T> context, ResultSet rows, Class<T> type,
				InstanceProvider instances, ResultSetMetaData metadata, String signature) throws Exception {
			RowPlan<T> plan = super.buildPlan(context, rows, type, instances, metadata, signature);
			this.lastPlan = plan;
			return plan;
		}
	}
}
