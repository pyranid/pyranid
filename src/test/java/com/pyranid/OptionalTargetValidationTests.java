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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class OptionalTargetValidationTests {
	private static final String SQL = "SELECT CAST(:value AS VARCHAR(128)) AS value FROM (VALUES (0))";
	public record OptionalRow(Optional<String> value) {}
	public record Partial(int id, Optional<String> missing) {}
	public record OptionalList(List<Optional<String>> value) {}
	public record OptionalSet(Set<Optional<String>> value) {}
	public record OptionalMap(Map<String, Optional<String>> value) {}
	public record OptionalKeys(Map<Optional<String>, String> value) {}
	public record OptionalArray(Optional<String>[] value) {}
	public record NestedRecord(OptionalRow value) {}
	public record NestedList(List<OptionalRow> value) {}
	public record WildcardList(List<? extends Optional<String>> value) {}
	public record PlainNode(String label, PlainNode child) {}
	public record PlainCarrier(PlainNode value) {}
	public record GrowingPlain<T>(T label, GrowingPlain<List<T>> child) {}
	public record GrowingCarrier(GrowingPlain<String> value) {}
	public record GrowingOptional<T>(T label, GrowingOptional<Optional<T>> child) {}
	public record GrowingOptionalCarrier(GrowingOptional<String> value) {}
	public record NativeMap(Map<String, Object> value) {}
	public static class OptionalBean {
		private Optional<String> value = Optional.empty();
		public Optional<String> getValue() { return value; }
		public void setValue(Optional<String> value) { this.value = value; }
	}
	public static class GenericBean<T> {
		private Optional<T> value;
		public Optional<T> getValue() { return value; }
		public void setValue(Optional<T> value) { this.value = value; }
	}
	public static class InheritedOptionalBean extends GenericBean<String> {}
	public static class PartialBean {
		private int id;
		private Optional<String> missing = Optional.empty();
		public int getId() { return id; }
		public void setId(int id) { this.id = id; }
		public Optional<String> getMissing() { return missing; }
		public void setMissing(Optional<String> missing) { throw new AssertionError("An unselected property must not be written"); }
	}

	@Test
	void selectedOptionalComponentsAndBeanPropertiesRejectNullAndNonNullValuesInBothCacheModes() {
		String privateValue = "private-row-value-85127";
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			for (String value : Arrays.asList(null, privateValue))
				for (Class<?> target : List.of(OptionalRow.class, OptionalBean.class, InheritedOptionalBean.class)) {
					DatabaseException failure = unsupported(() -> database.query(SQL).bind("value", Parameters.secure(value)).fetchObject(target));
					assertFalse(failure.getMessage().contains(privateValue));
				}
		}
	}

	@Test
	void declaredOptionalTreesRejectEvenWhenTheSelectedValueIsNullOrContainerIsEmpty() {
		List<Class<?>> shapes = List.of(OptionalList.class, OptionalSet.class, OptionalMap.class, OptionalKeys.class,
				OptionalArray.class, NestedRecord.class, NestedList.class, WildcardList.class, GrowingOptionalCarrier.class);
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			for (Class<?> shape : shapes) {
				unsupported(() -> database.query(SQL).bind("value", null).fetchObject(shape));
				TargetType target = TargetType.of(shape.getRecordComponents()[0].getGenericType());
				unsupported(() -> convert(database, null, target));
				unsupported(() -> convert(database, List.of(), target));
			}
		}
	}

	@Test
	void unselectedOptionalPropertiesRetainExistingPartialMappingRules() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			String sql = "SELECT CAST(1 AS INTEGER) AS id FROM (VALUES (0))";
			assertEquals(new Partial(1, null), database.query(sql).fetchObject(Partial.class).orElseThrow());
			PartialBean bean = database.query(sql).fetchObject(PartialBean.class).orElseThrow();
			assertEquals(1, bean.getId());
			assertEquals(Optional.empty(), bean.getMissing());
		}
	}

	@Test
	void anOptionalPropertyColumnMapperCannotBypassTheNullContract() {
		for (boolean cached : new boolean[]{false, true}) {
			AtomicInteger calls = new AtomicInteger();
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Optional.class); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
						Integer column, String label, InstanceProvider instances) {
					calls.incrementAndGet(); return MappingResult.of(Optional.of(raw));
				}
			})).build();
			Database database = database(mapper);
			for (String value : Arrays.asList(null, "present"))
				unsupported(() -> database.query(SQL).bind("value", value).fetchObject(OptionalRow.class));
			assertEquals(0, calls.get(), "Unsupported Optional-valued properties fail before reading or invoking a column mapper");
		}
	}

	@Test
	void opaqueWholeRowColumnMappingAndCustomResultSetMappingKeepTheirExistingEscapeHatches() {
		for (boolean cached : new boolean[]{false, true}) {
			AtomicInteger calls = new AtomicInteger();
			ResultSetMapper opaqueMapper = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(OptionalRow.class); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
						Integer column, String label, InstanceProvider instances) {
					calls.incrementAndGet(); return MappingResult.of(new OptionalRow(Optional.of(raw.toString())));
				}
			})).build();
			Database opaque = database(opaqueMapper);
			String sql = "SELECT CAST(:value AS VARCHAR(128)) AS payload FROM (VALUES (0))";
			assertEquals(new OptionalRow(Optional.of("manual")), opaque.query(sql).bind("value", "manual").fetchObject(OptionalRow.class).orElseThrow());
			assertTrue(opaque.query(sql).bind("value", null).fetchObject(OptionalRow.class).isEmpty());
			assertEquals(1, calls.get());
		}
		Database manual = database(new ResultSetMapper() {
			@Override public <T> Optional<T> map(StatementContext<T> context, ResultSet rows, Class<T> target, InstanceProvider instances) throws SQLException {
				return Optional.of(target.cast(new OptionalRow(Optional.ofNullable(rows.getString(1)))));
			}
		});
		assertEquals(new OptionalRow(Optional.empty()), manual.query(SQL).bind("value", null).fetchObject(OptionalRow.class).orElseThrow());
		assertEquals(new OptionalRow(Optional.of("manual")), manual.query(SQL).bind("value", "manual").fetchObject(OptionalRow.class).orElseThrow());
	}

	@Test
	void wholeShapeColumnMappersMayConstructNestedOptionalsButCannotDeclineIntoAutomaticMapping() {
		for (boolean cached : new boolean[]{false, true}) {
			CustomColumnMapper custom = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) {
					return List.of(Optional.class, Map.class, List.class, OptionalRow.class, Optional[].class).contains(target.getRawClass());
				}
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
						Integer column, String label, InstanceProvider instances) {
					if (raw.equals("decline")) return MappingResult.fallback();
					Optional<String> value = Optional.of(raw.toString());
					Class<?> type = target.getRawClass();
					if (type == Map.class) return MappingResult.of(Map.of("entry", value));
					if (type == List.class) return MappingResult.of(List.of(value));
					if (type == OptionalRow.class) return MappingResult.of(new OptionalRow(value));
					if (type == Optional[].class) return MappingResult.of(new Optional<?>[]{value});
					return MappingResult.of(value);
				}
			};
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(custom)).build());
			assertEquals(new OptionalMap(Map.of("entry", Optional.of("manual"))), database.query(SQL).bind("value", "manual").fetchObject(OptionalMap.class).orElseThrow());
			assertEquals(new OptionalList(List.of(Optional.of("manual"))), database.query(SQL).bind("value", "manual").fetchObject(OptionalList.class).orElseThrow());
			assertEquals(new NestedRecord(new OptionalRow(Optional.of("manual"))), database.query(SQL).bind("value", "manual").fetchObject(NestedRecord.class).orElseThrow());
			assertArrayEquals(new Optional<?>[]{Optional.of("manual")}, database.query(SQL).bind("value", "manual").fetchObject(OptionalArray.class).orElseThrow().value());
			assertEquals(Optional.of("manual"), database.query(SQL).bind("value", "manual").fetchObject(Optional.class).orElseThrow());
			assertArrayEquals(new Optional<?>[]{Optional.of("manual")}, database.query(SQL).bind("value", "manual").fetchObject(Optional[].class).orElseThrow());
			assertEquals(new OptionalMap(null), database.query(SQL).bind("value", null).fetchObject(OptionalMap.class).orElseThrow());
			assertEquals(new OptionalList(null), database.query(SQL).bind("value", null).fetchObject(OptionalList.class).orElseThrow());
			assertEquals(new NestedRecord(null), database.query(SQL).bind("value", null).fetchObject(NestedRecord.class).orElseThrow());
			assertNull(database.query(SQL).bind("value", null).fetchObject(OptionalArray.class).orElseThrow().value());
			assertTrue(database.query(SQL).bind("value", null).fetchObject(Optional.class).isEmpty());
			assertTrue(database.query(SQL).bind("value", null).fetchObject(Optional[].class).isEmpty());
			for (Class<?> shape : List.of(OptionalMap.class, OptionalList.class, NestedRecord.class, OptionalArray.class, Optional.class, Optional[].class))
				unsupported(() -> database.query(SQL).bind("value", "decline").fetchObject(shape));
		}
	}

	@Test
	void unsupportedScalarAndMatchedPropertyTargetsFailBeforeColumnValueReadersRun() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			for (Class<?> target : List.of(Optional.class, Optional[].class, OptionalRow.class, OptionalList.class)) {
				StatementContext<?> context = StatementContext.with(Statement.of("optional-plan", "SELECT value"), database).build();
				unsupported(() -> database.getResultSetMapper().map((StatementContext) context, unreadableRows(), target, new InstanceProvider() {}));
			}
		}
	}

	@Test
	void recursiveRecordShapesTerminateAndNativeObjectLeavesRemainOpaque() {
		Database database = database(ResultSetMapper.withDefaultConfiguration());
		assertEquals(new PlainCarrier(null), database.query(SQL).bind("value", null).fetchObject(PlainCarrier.class).orElseThrow());
		assertEquals(new GrowingCarrier(null), database.query(SQL).bind("value", null).fetchObject(GrowingCarrier.class).orElseThrow());
		Optional<String> nativeValue = Optional.of("opaque");
		assertSame(nativeValue, convert(database, nativeValue, TargetType.of(Object.class)).orElseThrow());
		TargetType mapType = TargetType.of(NativeMap.class.getRecordComponents()[0].getGenericType());
		assertSame(nativeValue, ((Map<?, ?>) convert(database, Map.of("native", nativeValue), mapType).orElseThrow()).get("native"));
	}

	private static DatabaseException unsupported(org.junit.jupiter.api.function.Executable operation) {
		DatabaseException failure = assertThrows(DatabaseException.class, operation);
		assertTrue(failure.getMessage().contains("Optional-valued"), failure::getMessage);
		assertTrue(failure.getMessage().contains("nullable underlying type"), failure::getMessage);
		assertTrue(failure.getMessage().contains("custom ResultSetMapper"), failure::getMessage);
		return failure;
	}
	private static Optional<Object> convert(Database database, Object value, TargetType target) {
		StatementContext<?> context = StatementContext.with(Statement.of("optional-target", "SELECT value"), database).build();
		return ((DefaultResultSetMapper) database.getResultSetMapper()).convertResultSetValueToTargetType(context, value, target);
	}
	private static ResultSet unreadableRows() {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					case "getColumnCount" -> 1;
					case "getColumnType" -> Types.VARCHAR;
					case "getColumnTypeName" -> "VARCHAR";
					case "getColumnClassName" -> String.class.getName();
					case "getColumnLabel", "getColumnName" -> "value";
					default -> throw new AssertionError(method.getName());
				});
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> {
					if (method.getName().equals("getMetaData")) return metadata;
					throw new AssertionError("Unsupported target must fail before value access: " + method.getName());
				});
	}
	private static Database database(ResultSetMapper mapper) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:optional_targets_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).resultSetMapper(mapper).build();
	}
}
