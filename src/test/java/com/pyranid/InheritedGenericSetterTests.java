/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import com.pyranid.fixtures.GenericSetterFixtures.HiddenLongEntity;
import com.pyranid.fixtures.GenericSetterFixtures.PrivateOverloadEntity;
import com.pyranid.fixtures.GenericSetterFixtures.StaticOverloadEntity;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class InheritedGenericSetterTests {
	private static final UUID KEY = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

	public static class Entity<ID> {
		private ID id;
		private String label = "unchanged";
		public ID getId() { return this.id; }
		public void setId(ID id) { this.id = id; }
		public String getLabel() { return this.label; }
		public void setLabel(String label) { this.label = label; }
	}
	public static class LongEntity extends Entity<Long> {}
	public static class UuidEntity extends Entity<UUID> {}
	public static class MiddleEntity<V> extends Entity<V> {}
	public static class DeepLongEntity extends MiddleEntity<Long> {}
	public static class ExplicitObjectEntity extends Entity<Object> {}
	public static class ExplicitLongOverride extends Entity<Long> {
		@Override public void setId(Long id) { super.setId(id); }
	}
	public interface DefaultSetter<T> {
		void store(T value);
		default void setId(T id) { store(id); }
	}
	public static class InterfaceEntity implements DefaultSetter<Long> {
		private Long id;
		@Override public void store(Long id) { this.id = id; }
		public Long getId() { return this.id; }
	}
	public static class Owner<T> {
		public class Member<U> {
			private T id;
			private U key;
			public T getId() { return this.id; }
			public void setId(T id) { this.id = id; }
			public U getKey() { return this.key; }
			public void setKey(U key) { this.key = key; }
		}
	}
	public static class OwnedEntity extends Owner<Long>.Member<UUID> {
		public OwnedEntity() { new Owner<Long>().super(); }
	}
	public static class Containers<T> {
		private List<T> ids;
		private List<List<T>> groups;
		private T[] array;
		private List<T>[] listArray;
		private List<? extends T> upper;
		private List<? super T> lower;
		public List<T> getIds() { return this.ids; }
		public void setIds(List<T> ids) { this.ids = ids; }
		public List<List<T>> getGroups() { return this.groups; }
		public void setGroups(List<List<T>> groups) { this.groups = groups; }
		public T[] getArray() { return this.array; }
		public void setArray(T[] array) { this.array = array; }
		public List<T>[] getListArray() { return this.listArray; }
		public void setListArray(List<T>[] listArray) { this.listArray = listArray; }
		public List<? extends T> getUpper() { return this.upper; }
		public void setUpper(List<? extends T> upper) { this.upper = upper; }
		public List<? super T> getLower() { return this.lower; }
		public void setLower(List<? super T> lower) { this.lower = lower; }
	}
	public static class LongContainers extends Containers<Long> {}
	public static class UuidContainers extends Containers<UUID> {}
	public static class NativeProperties {
		private Object id;
		private List values;
		public Object getId() { return this.id; }
		public void setId(Object id) { this.id = id; }
		public List getValues() { return this.values; }
		public void setValues(List values) { this.values = values; }
	}
	public static class UnresolvedBound<T extends Number> extends Entity<T> {}
	public static class GenericMethod {
		public <V> void setId(V value) {}
	}
	private static class ExpectedTypes {
		List<List<Long>> nested;
		List<Long>[] listArray;
		List<? extends Long> upper;
		List<? super Long> lower;
	}

	@Test
	void inheritedScalarTypesConvertBeforeTheErasedSetterRuns() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, List.of());
			for (int invocation = 0; invocation < 2; invocation++) {
				assertEquals(Long.valueOf(7), database.query("SELECT 7 AS id FROM (VALUES (0))").fetchObject(LongEntity.class).orElseThrow().getId());
				assertEquals(KEY, database.query("SELECT :key AS id FROM (VALUES (0))").bind("key", KEY.toString())
						.fetchObject(UuidEntity.class).orElseThrow().getId());
				assertEquals(Long.valueOf(8), database.query("SELECT 8 AS id FROM (VALUES (0))").fetchObject(DeepLongEntity.class).orElseThrow().getId());
			}
		}
	}

	@Test
	void interfaceAndOwnerBindingsReachTheConcreteParameterTypes() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, List.of());
			assertEquals(Long.valueOf(9), mapped(database, InterfaceEntity.class, new String[]{"id"}, new Object[]{9}).getId());
			OwnedEntity owner = mapped(database, OwnedEntity.class, new String[]{"id", "key"}, new Object[]{10, KEY.toString()});
			assertEquals(Long.valueOf(10), owner.getId());
			assertEquals(KEY, owner.getKey());
		}
	}

	@Test
	void hiddenGenericVisibilityBridgesKeepAliasesAndResolvedMetadata() throws Exception {
		assertTrue(HiddenLongEntity.class.getMethod("setId", Object.class).isBridge());
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, List.of());
			HiddenLongEntity bean = database.query("SELECT 7 AS inherited_identifier FROM (VALUES (0))")
					.fetchObject(HiddenLongEntity.class).orElseThrow();
			assertEquals(Long.valueOf(7), bean.getId());
		}
	}

	@Test
	void concreteOverridesAndTheirNarrowingBridgesStillSelectTheRealSetter() throws Exception {
		assertTrue(ExplicitLongOverride.class.getMethod("setId", Object.class).isBridge());
		for (boolean cached : new boolean[]{false, true}) {
			assertEquals(Long.valueOf(11), mapped(database(cached, List.of()), ExplicitLongOverride.class,
					new String[]{"id"}, new Object[]{11}).getId());
		}
	}

	@Test
	void privateAndStaticOverloadsDoNotSuppressAnInheritedPublicVisibilityBridge() throws Exception {
		for (Class<?> type : List.of(PrivateOverloadEntity.class, StaticOverloadEntity.class)) {
			assertTrue(type.getMethod("setId", Object.class).isBridge());
			for (boolean cached : new boolean[]{false, true}) {
				Object bean = database(cached, List.of()).query("SELECT 7 AS inherited_identifier FROM (VALUES (0))")
						.fetchObject(type).orElseThrow();
				assertEquals(7L, type.getMethod("getId").invoke(bean));
			}
		}
	}

	@Test
	void nestedCollectionsAndGenericArraysRetainResolvedElementTypes() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, List.of());
			for (int invocation = 0; invocation < 2; invocation++) {
				LongContainers row = mapped(database, LongContainers.class,
						new String[]{"ids", "groups", "array", "list_array", "upper", "lower"},
						new Object[]{new Integer[]{1, 2}, new Integer[][]{{3}, {4}}, new Integer[]{5},
								new Integer[][]{{6}, {7}}, new Integer[]{8}, new Integer[]{9}});
				assertEquals(List.of(1L, 2L), row.getIds());
				assertEquals(List.of(List.of(3L), List.of(4L)), row.getGroups());
				assertArrayEquals(new Long[]{5L}, row.getArray());
				assertEquals(List.of(6L), row.getListArray()[0]);
				assertEquals(List.of(7L), row.getListArray()[1]);
				assertEquals(List.of(8L), row.getUpper());
				assertEquals(List.of(9), row.getLower(), "A lower-bounded wildcard keeps the existing Object upper-bound semantics");
			}
			UuidContainers uuid = mapped(database, UuidContainers.class, new String[]{"ids", "array"},
					new Object[]{new String[]{KEY.toString()}, new String[]{KEY.toString()}});
			assertEquals(List.of(KEY), uuid.getIds());
			assertArrayEquals(new UUID[]{KEY}, uuid.getArray());
		}
	}

	@Test
	void customMappersSeeResolvedTypesAndCannotPassAnErasedValidationCheck() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			List<Type> observed = new ArrayList<>();
			CustomColumnMapper callback = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Long.class) || target.isList(); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet result, Object value,
						TargetType target, Integer index, String label, InstanceProvider instances) {
					observed.add(target.getType());
					return MappingResult.fallback();
				}
			};
			Database database = database(cached, List.of(callback));
			assertEquals(Long.valueOf(12), mapped(database, LongEntity.class, new String[]{"id"}, new Object[]{12}).getId());
			assertEquals(List.of(List.of(13L)), mapped(database, LongContainers.class, new String[]{"groups"}, new Object[]{new Integer[][]{{13}}}).getGroups());
			assertEquals(List.of(Long.class, ExpectedTypes.class.getDeclaredField("nested").getGenericType()), observed);

			CustomColumnMapper wrongType = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Long.class); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet result, Object value,
						TargetType target, Integer index, String label, InstanceProvider instances) { return MappingResult.of("wrong"); }
			};
			DatabaseException rejected = assertThrows(DatabaseException.class, () -> mapped(database(cached, List.of(wrongType)),
					LongEntity.class, new String[]{"id"}, new Object[]{1}));
			assertTrue(rejected.getMessage().contains("java.lang.Long"));
		}
	}

	@Test
	void explicitObjectAndRawCollectionsRetainNativeValues() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, List.of());
			NativeProperties nativeRow = mapped(database, NativeProperties.class, new String[]{"id", "values"},
					new Object[]{14, new Integer[]{15}});
			assertEquals(14, nativeRow.getId());
			assertEquals(List.of(15), nativeRow.getValues());
			assertEquals(16, mapped(database, ExplicitObjectEntity.class, new String[]{"id"}, new Object[]{16}).getId());
		}
	}

	@Test
	void unboundVariablesFailOnlyWhenTheirPropertyIsSelected() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, List.of());
			for (Class<?> unresolved : List.of(Entity.class, UnresolvedBound.class, GenericMethod.class)) {
				DatabaseException failure = assertThrows(DatabaseException.class,
						() -> mapped(database, unresolved, new String[]{"id"}, new Object[]{17}));
				assertTrue(failure.getMessage().contains("Cannot resolve generic type"));
				assertTrue(failure.getMessage().contains("'id'"));
			}
			Entity<?> unaffected = mapped(database, Entity.class, new String[]{"label"}, new Object[]{"known"});
			assertEquals("known", unaffected.getLabel());
			assertNull(unaffected.getId());
		}
	}

	@Test
	void substitutedTypesObeyReflectionEqualityAndPreserveGenericArrayComponents() throws Exception {
		for (String property : List.of("groups", "listArray", "upper", "lower")) {
			Method setter = Containers.class.getMethod("set" + Character.toUpperCase(property.charAt(0)) + property.substring(1),
					property.equals("listArray") ? List[].class : List.class);
			Type resolved = GenericPropertyTypes.resolve(LongContainers.class, Containers.class, setter.getGenericParameterTypes()[0]);
			Type expected = ExpectedTypes.class.getDeclaredField(property.equals("groups") ? "nested" : property).getGenericType();
			assertEquals(expected, resolved);
			assertEquals(resolved, expected);
			assertEquals(expected.hashCode(), resolved.hashCode());
			assertEquals("match", Map.of(expected, "match").get(resolved));
		}
		Type array = ExpectedTypes.class.getDeclaredField("listArray").getGenericType();
		assertEquals(Long.class, TargetType.of(array).getArrayComponentType().orElseThrow().getListElementType().orElseThrow().getRawClass());
		assertEquals(Long.class, TargetType.of(Long[].class).getArrayComponentType().orElseThrow().getRawClass());
	}

	private static Database database(boolean cached, List<CustomColumnMapper> callbacks) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:generic_setters_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(callbacks).build()).build();
	}

	private static <T> T mapped(Database database, Class<T> type, String[] labels, Object[] values) throws Exception {
		StatementContext<T> context = StatementContext.<T>with(Statement.of("generic-setters", "SELECT projected values"), database)
				.resultSetRowType(type).build();
		try (ResultSet result = cursor(labels, values)) {
			return database.getResultSetMapper().map(context, result, type, database.getInstanceProvider()).orElseThrow();
		}
	}

	private static ResultSet cursor(String[] labels, Object[] values) {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class}, (proxy, method, arguments) -> switch (method.getName()) {
					case "getColumnCount" -> labels.length;
					case "getColumnLabel", "getColumnName" -> labels[(int) arguments[0] - 1];
					case "getColumnType" -> Types.OTHER;
					case "getColumnTypeName" -> "JAVA_OBJECT";
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == arguments[0];
					case "toString" -> "generic setter metadata";
					default -> throw new AssertionError("Unexpected metadata access: " + method);
				});
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, arguments) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject" -> values[(int) arguments[0] - 1];
					case "wasNull" -> false;
					case "close" -> null;
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == arguments[0];
					case "toString" -> "generic setter cursor";
					default -> throw new AssertionError("Unexpected cursor access: " + method);
				});
	}
}
