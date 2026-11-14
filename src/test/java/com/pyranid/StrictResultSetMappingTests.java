/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class StrictResultSetMappingTests {
	public record Id(Integer id) {}
	public record Partial(Integer id, String absent) {}
	public record PrimitivePartial(Integer id, int absent) {}
	public record Value(String value) {}
	public record FanOut(@DatabaseColumn("selected") String first, @DatabaseColumn("selected") String second) {}
	public static class Bean {
		private Integer id;
		private String absent = "initial";
		public Integer getId() { return this.id; }
		public void setId(Integer id) { this.id = id; }
		public String getAbsent() { return this.absent; }
		public void setAbsent(String absent) { this.absent = absent; }
	}

	@Test
	void strictModeRejectsExtraSourceColumnsForBeansAndRecordsWithBothCacheModes() {
		for (boolean cached : new boolean[]{false, true}) {
			Database loose = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			Database strict = database(ResultSetMapper.withPlanCachingEnabled(cached).failOnUnmappedColumns(true).build());
			String sql = "SELECT CAST(7 AS INTEGER) AS id, CAST(:ignoredValue AS VARCHAR(40)) AS ignored FROM (VALUES (0))";
			assertEquals(new Id(7), loose.query(sql).bind("ignoredValue", Parameters.secure("private-row-value")).fetchObject(Id.class).orElseThrow());
			assertEquals(7, loose.query(sql).bind("ignoredValue", Parameters.secure("private-row-value")).fetchObject(Bean.class).orElseThrow().getId());
			for (int pass = 0; pass < 2; pass++) {
				for (Class<?> target : List.of(Id.class, Bean.class)) {
					DatabaseException failure = assertThrows(DatabaseException.class, () -> strict.query(sql).bind("ignoredValue", Parameters.secure("private-row-value")).fetchObject(target));
					assertTrue(failure.getMessage().contains("IGNORED"));
					assertTrue(failure.getMessage().contains(target.getName()));
					assertFalse(failure.getMessage().contains("private-row-value"));
				}
			}
			assertTrue(strict.query(sql + " WHERE 1=0").bind("ignoredValue", Parameters.secure("private-row-value")).fetchList(Id.class).isEmpty(),
					"An empty result never invokes the row mapper or its strict plan validation");
		}
	}

	@Test
	void strictValidationRunsBeforeReadingColumnValuesOrConstructingObjects() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).failOnUnmappedColumns(true).build();
			Database database = database(mapper);
			AtomicInteger reads = new AtomicInteger();
			ResultSet rows = rows(new String[]{"id", "ignored"}, Types.INTEGER, reads, null);
			InstanceProvider provider = new InstanceProvider() {
				@Override public <T> T provide(StatementContext<T> context, Class<T> type) { throw new AssertionError("No construction before validation"); }
				@Override public <T extends Record> T provideRecord(StatementContext<T> context, Class<T> type, Object... args) { throw new AssertionError("No construction before validation"); }
			};
			for (Class<?> target : List.of(Id.class, Bean.class)) {
				StatementContext context = StatementContext.with(Statement.of("strict", "SELECT id, ignored"), database).build();
				DatabaseException failure = assertThrows(DatabaseException.class, () -> mapper.map(context, rows, target, provider));
				assertTrue(failure.getMessage().contains("ignored"));
			}
			assertEquals(0, reads.get());
		}
	}

	@Test
	void selectedNullsAndUnselectedTargetMembersKeepExistingPartialMappingRules() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).failOnUnmappedColumns(true).build());
			String sql = "SELECT CAST(NULL AS INTEGER) AS id FROM (VALUES (0))";
			assertEquals(new Partial(null, null), database.query(sql).fetchObject(Partial.class).orElseThrow());
			Bean bean = database.query(sql).fetchObject(Bean.class).orElseThrow();
			assertNull(bean.getId());
			assertEquals("initial", bean.getAbsent());
			DatabaseException missingPrimitive = assertThrows(DatabaseException.class, () -> database.query(sql).fetchObject(PrimitivePartial.class));
			assertTrue(missingPrimitive.getMessage().contains("No column matches record component"));
		}
	}

	@Test
	void aliasesFanOutAndNativeRowsAreNotMistakenForUnmappedColumns() {
		Database database = database(ResultSetMapper.withPlanCachingEnabled(true).failOnUnmappedColumns(true).build());
		assertEquals(new FanOut("shared", "shared"), database.query("SELECT CAST('shared' AS VARCHAR(20)) AS selected FROM (VALUES (0))")
				.fetchObject(FanOut.class).orElseThrow());
		assertEquals(7, database.query("SELECT CAST(7 AS INTEGER) AS arbitrary FROM (VALUES (0))").fetchObject(Integer.class).orElseThrow());
		assertEquals(Map.of("arbitrary", 7, "other", 8), database.query("SELECT CAST(7 AS INTEGER) AS arbitrary, CAST(8 AS INTEGER) AS other FROM (VALUES (0))")
				.fetchObject(Query.mapRowType()).orElseThrow());
	}

	@Test
	void directSingleStructFallbackRemainsAvailableAndNamedMappingKeepsPrecedence() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).failOnUnmappedColumns(true).build();
			Database database = database(mapper);
			StatementContext<Value> context = StatementContext.<Value>with(Statement.of("struct", "SELECT payload"), database).build();
			assertEquals(new Value("nested"), mapper.map(context, rows(new String[]{"payload"}, Types.STRUCT, new AtomicInteger(), Map.of("value", "nested")),
					Value.class, new InstanceProvider() {}).orElseThrow());
			assertTrue(mapper.map(context, rows(new String[]{"payload"}, Types.STRUCT, new AtomicInteger(), null), Value.class, new InstanceProvider() {}).isEmpty());
			assertThrows(DatabaseException.class, () -> mapper.map(context,
					rows(new String[]{"value", "ignored"}, Types.STRUCT, new AtomicInteger(), Map.of("value", "nested")), Value.class, new InstanceProvider() {}));
		}
	}

	@Test
	void completeCustomRowOverridesRemainOpaqueButFallbackUsesStrictRules() {
		for (boolean cached : new boolean[]{false, true}) {
			CustomColumnMapper hook = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Value.class); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
						Integer index, String label, InstanceProvider provider) {
					return raw.equals("decline") ? MappingResult.fallback() : MappingResult.of(new Value(raw.toString()));
				}
			};
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).failOnUnmappedColumns(true).customColumnMappers(List.of(hook)).build());
			String sql = "SELECT CAST(:value AS VARCHAR(30)) AS payload FROM (VALUES (0))";
			assertEquals(new Value("manual"), database.query(sql).bind("value", "manual").fetchObject(Value.class).orElseThrow());
			assertTrue(database.query(sql).bind("value", null).fetchObject(Value.class).isEmpty());
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query(sql).bind("value", "decline").fetchObject(Value.class));
			assertTrue(failure.getMessage().contains("Unmapped source columns"));
		}
	}

	@Test
	void builderReuseDoesNotShareStrictnessOrCachedPlansBetweenMappers() {
		ResultSetMapper.Builder builder = ResultSetMapper.withPlanCachingEnabled(true);
		Database loose = database(builder.build());
		Database strict = database(builder.failOnUnmappedColumns(true).build());
		Database looseAgain = database(builder.failOnUnmappedColumns(false).build());
		String sql = "SELECT CAST(1 AS INTEGER) AS id, CAST(2 AS INTEGER) AS extra FROM (VALUES (0))";
		for (int pass = 0; pass < 2; pass++) {
			assertEquals(new Id(1), loose.query(sql).fetchObject(Id.class).orElseThrow());
			assertThrows(DatabaseException.class, () -> strict.query(sql).fetchObject(Id.class));
			assertEquals(new Id(1), looseAgain.query(sql).fetchObject(Id.class).orElseThrow());
		}
		assertThrows(NullPointerException.class, () -> builder.failOnUnmappedColumns(null));
	}

	private static Database database(ResultSetMapper mapper) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:strict_mapping_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).resultSetMapper(mapper).build();
	}

	private static ResultSet rows(String[] labels, int jdbcType, AtomicInteger reads, Object value) {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					case "getColumnCount" -> labels.length;
					case "getColumnLabel", "getColumnName" -> labels[(int) args[0] - 1];
					case "getColumnType" -> jdbcType;
					case "getColumnTypeName" -> jdbcType == Types.STRUCT ? "STRUCT" : "INTEGER";
					case "getColumnClassName" -> Object.class.getName();
					default -> throw new AssertionError(method.getName());
				});
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> {
					if (method.getName().equals("getMetaData")) return metadata;
					reads.incrementAndGet();
					if (method.getName().equals("getObject")) return value;
					throw new AssertionError("Unexpected column access: " + method.getName());
				});
	}
}
