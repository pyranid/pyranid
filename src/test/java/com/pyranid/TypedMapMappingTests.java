/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.duckdb.FaultArray;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TypedMapMappingTests {
	public record Uuids(Map<UUID, Long> value) {}
	public record Longs(Map<Long, Long> value) {}
	public record NamedLongs(Map<String, Long> value) {}
	public record Item(Long count) {}
	public record Nested(Map<String, Map<Long, List<Item>>> value) {}
	public record MapList(List<Map<UUID, Long>> value) {}
	public record MapSet(Set<Map<UUID, Long>> value) {}
	public record MapArray(Map<UUID, Long>[] value) {}
	public record LongLists(Map<String, List<Long>> value) {}
	public record LongKeyLists(Map<Long, List<Long>> value) {}
	public record SharedNativeArray(Map<List<Long>, Object> value) {}
	public record SharedNativeKey(Map<Object, List<Long>> value) {}
	public record UuidLists(Map<String, List<UUID>> value) {}
	public record InstantLists(Map<String, List<Instant>> value) {}
	public record Instants(Map<String, Instant> value) {}
	public record Binary(Map<String, byte[]> value) {}
	public record HashMaps(HashMap<UUID, Long> value) {}
	public record LinkedMaps(LinkedHashMap<UUID, Long> value) {}
	public record SortedMaps(SortedMap<String, Long> value) {}
	public enum Key { FIRST }
	public record EnumMaps(EnumMap<Key, Long> value) {}
	public record ConcreteLists(Map<String, ArrayList<UUID>> value) {}
	public record ConcreteSets(Map<String, TreeSet<Long>> value) {}
	public record NestedConcreteCollections(Map<String, List<Map<String, TreeSet<UUID>>>> value) {}
	public record ConcreteCollectionsInMapList(List<Map<String, ArrayList<UUID>>> value) {}
	public record ConcreteCollectionsInMapArray(Map<String, ArrayList<UUID>>[] value) {}
	public record NativeConcreteLists(Map<String, ArrayList<Object>> value) {}
	public record WildcardMaps(Map<String, ? extends Map<Long, ? extends List<Long>>> value) {}
	public record WildcardMapList(List<? extends Map<String, ? extends List<Long>>> value) {}
	public record WildcardConcreteLists(Map<String, ? extends ArrayList<UUID>> value) {}
	@SuppressWarnings("rawtypes") public record RawMap(Map value) {}
	public record Objects(Map<Object, Object> value) {}
	public record NamedObjects(Map<String, Object> value) {}
	public record Native(Object value) {}
	public static class GenericBean<K, V> {
		private Map<K, V> value;
		public void setValue(Map<K, V> value) { this.value = value; }
		public Map<K, V> getValue() { return value; }
	}
	public static class UuidBean extends GenericBean<UUID, Long> {}

	@Test
	void recordAndInheritedGenericBeanMapsConvertKeysAndValuesWithEitherCacheMode() throws Exception {
		UUID first = UUID.randomUUID(), second = UUID.randomUUID();
		Map<String, Integer> source = new LinkedHashMap<>();
		source.put(second.toString(), 2);
		source.put(first.toString(), 1);
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = mapper(cached);
			for (int pass = 0; pass < 2; pass++) {
				Map<UUID, Long> actual = map(mapper, source, Uuids.class).value();
				assertEquals(List.of(second, first), new ArrayList<>(actual.keySet()));
				assertEquals(2L, actual.get(second));
				assertInstanceOf(Long.class, actual.get(first));
				assertEquals(actual, map(mapper, source, UuidBean.class).getValue());
				assertEquals(actual, map(mapper, source, HashMaps.class).value());
				assertEquals(actual, map(mapper, source, LinkedMaps.class).value());
			}
		}
		assertEquals(List.of(second.toString(), first.toString()), new ArrayList<>(source.keySet()));
		assertInstanceOf(Integer.class, source.get(first.toString()));
	}

	@Test
	void nestedMapsListsAndStructRecordsAreConvertedRecursively() throws Exception {
		Map<String, Object> item = Map.of("count", 7);
		for (boolean cached : new boolean[]{false, true}) {
			for (Object elements : List.of(new Object[]{item, null}, Arrays.asList(item, null))) {
				Map<String, Object> source = Map.of("group", Map.of(4, elements));
				Nested row = map(mapper(cached), source, Nested.class);
				assertEquals(Arrays.asList(new Item(7L), null), row.value().get("group").get(4L));
			}
		}
	}

	@Test
	void mapsInsideSqlArrayShapedAndAlreadyExtractedCollectionsDoNotBypassConversion() throws Exception {
		UUID key = UUID.randomUUID();
		Map<String, Integer> source = Map.of(key.toString(), 3);
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = mapper(cached);
			for (Object values : List.of(new Object[]{source}, List.of(source))) {
				Map<UUID, Long> mapped = map(mapper, values, MapList.class).value().get(0);
				assertEquals(3L, mapped.get(key));
				assertEquals(Set.of(mapped), map(mapper, values, MapSet.class).value());
			}
			assertEquals(3L, map(mapper, new Object[]{source}, MapArray.class).value()[0].get(key));
		}
	}

	@Test
	void nullKeysNullValuesAndEmptyMapsArePreserved() throws Exception {
		Map<Object, Object> source = new LinkedHashMap<>();
		source.put(null, 1); source.put(2, null); source.put(3, 4);
		for (boolean cached : new boolean[]{false, true}) {
			Map<Long, Long> result = map(mapper(cached), source, Longs.class).value();
			assertEquals(Arrays.asList(null, 2L, 3L), new ArrayList<>(result.keySet()));
			assertEquals(1L, result.get(null)); assertNull(result.get(2L)); assertEquals(4L, result.get(3L));
			assertTrue(map(mapper(cached), Map.of(), Uuids.class).value().isEmpty());
			assertNull(map(mapper(cached), null, Uuids.class).value());
		}
	}

	@Test
	void duplicateConvertedKeysCannotOverwriteEarlierEntriesIncludingNullValues() {
		Map<Object, Object> source = new LinkedHashMap<>();
		source.put(1, null); source.put(new BigDecimal("1.0"), 9);
		for (boolean cached : new boolean[]{false, true}) {
			DatabaseException failure = assertThrows(DatabaseException.class, () -> map(mapper(cached), source, Longs.class));
			assertTrue(failure.getMessage().contains("duplicate key after conversion"), failure::getMessage);
			assertEquals(2, source.size());
		}
	}

	@Test
	void invalidKeysValuesAndNarrowingFailDuringMapping() {
		for (boolean cached : new boolean[]{false, true}) {
			DatabaseException invalid = assertThrows(DatabaseException.class,
					() -> map(mapper(cached), Map.of("entry", "private-payload"), NamedLongs.class));
			assertTrue(invalid.getMessage().contains("SQL MAP entry 0 value"), invalid::getMessage);
			assertFalse(invalid.getMessage().contains("private-payload"));
			assertThrows(DatabaseException.class, () -> map(mapper(cached), Map.of(new Object(), 1), Longs.class));
			assertThrows(DatabaseException.class, () -> map(mapper(cached), Map.of(1, new BigDecimal("1.5")), Longs.class));
		}
	}

	@Test
	void unsupportedTypedMapTargetsFailEvenForEmptyInput() {
		for (boolean cached : new boolean[]{false, true}) {
			for (Class<?> type : List.of(SortedMaps.class, EnumMaps.class)) {
				DatabaseException failure = assertThrows(DatabaseException.class, () -> map(mapper(cached), Map.of(), type));
				assertTrue(failure.getMessage().contains("cannot receive an insertion-ordered LinkedHashMap"), failure::getMessage);
			}
		}
	}

	@Test
	void unsupportedTypedCollectionSubtypesInsideMapsFailBeforeEmptyOrNonemptyContainersCanHideThem() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (Class<?> type : List.of(ConcreteLists.class, ConcreteSets.class, NestedConcreteCollections.class, WildcardConcreteLists.class)) {
				for (Map<?, ?> source : List.of(Map.of(), Map.of("entry", new ArrayList<>()))) {
					DatabaseException failure = assertThrows(DatabaseException.class, () -> map(mapper(cached), source, type));
					assertTrue(failure.getMessage().contains("inside SQL MAP is unsupported"), failure::getMessage);
				}
			}
			assertThrows(DatabaseException.class, () -> map(mapper(cached), List.of(), ConcreteCollectionsInMapList.class));
			assertThrows(DatabaseException.class, () -> map(mapper(cached), new Object[0], ConcreteCollectionsInMapArray.class));
			ArrayList<Object> nativeValues = new ArrayList<>(List.of(1, "native"));
			assertSame(nativeValues, map(mapper(cached), Map.of("entry", nativeValues), NativeConcreteLists.class).value().get("entry"));
		}
	}

	@Test
	void parameterizedUpperBoundsStillConvertEveryNestedDeclaredType() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			WildcardMaps mapped = map(mapper(cached), Map.of("nested", Map.of(1, List.of(2))), WildcardMaps.class);
			assertEquals(2L, mapped.value().get("nested").get(1L).get(0));
			assertInstanceOf(Long.class, mapped.value().get("nested").get(1L).get(0));
			WildcardMapList list = map(mapper(cached), List.of(Map.of("nested", List.of(2))), WildcardMapList.class);
			assertEquals(2L, list.value().get(0).get("nested").get(0));
			assertInstanceOf(Long.class, list.value().get(0).get("nested").get(0));
		}
	}

	@Test
	void rawObjectMapsAndObjectValuedEntriesKeepNativeRepresentationsAndHandles() throws Exception {
		AtomicInteger frees = new AtomicInteger();
		Blob blob = blob(frees);
		Map<Object, Object> source = new LinkedHashMap<>();
		source.put(1, 2); source.put("blob", blob);
		for (boolean cached : new boolean[]{false, true}) {
			assertSame(source, map(mapper(cached), source, RawMap.class).value());
			assertSame(source, map(mapper(cached), source, Objects.class).value());
			assertSame(source, map(mapper(cached), source, Native.class).value());
			assertSame(blob, map(mapper(cached), Map.of("blob", blob), NamedObjects.class).value().get("blob"));
		}
		assertEquals(0, frees.get());
	}

	@Test
	void customColumnMappersSeeOriginalMapsAndTheirHandledResultsStayUntouched() throws Exception {
		UUID key = UUID.randomUUID();
		Map<String, Integer> source = Map.of(key.toString(), 1);
		Map<UUID, Long> replacement = Map.of(key, 8L);
		for (boolean cached : new boolean[]{false, true}) {
			AtomicInteger calls = new AtomicInteger();
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.isMap(); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet result, Object raw,
						TargetType target, Integer column, String label, InstanceProvider provider) {
					assertSame(source, raw); calls.incrementAndGet(); return MappingResult.of(replacement);
				}
			})).build();
			assertSame(replacement, map(mapper, source, Uuids.class).value());
			assertEquals(1, calls.get());
		}
	}

	@Test
	void nestedArraysAreReadAndFreedExactlyOnceOnSuccessAndConversionFailure() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (DatabaseType dialect : List.of(DatabaseType.GENERIC, DatabaseType.DUCK_DB)) {
				for (boolean fail : new boolean[]{false, true}) {
					AtomicInteger reads = new AtomicInteger(), frees = new AtomicInteger();
					Map<String, Object> source = Map.of("numbers", new FaultArray(null, null, reads, frees));
					if (fail) assertThrows(DatabaseException.class, () -> map(mapper(cached), source, UuidLists.class, dialect, "MAP(VARCHAR, INTEGER[])"));
					else assertEquals(List.of(7L), map(mapper(cached), source, LongLists.class, dialect, "MAP(VARCHAR, INTEGER[])").value().get("numbers"));
					assertEquals(1, reads.get()); assertEquals(1, frees.get());
				}
			}
		}
	}

	@Test
	void repeatedArrayHandlesAreDetachedOnceEvenWhenALaterMapKeyFails() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (DatabaseType dialect : List.of(DatabaseType.GENERIC, DatabaseType.DUCK_DB)) {
				for (boolean invalid : new boolean[]{false, true}) {
					AtomicInteger reads = new AtomicInteger(), frees = new AtomicInteger();
					Array array = new FaultArray(null, null, reads, frees);
					Map<Object, Object> source = new LinkedHashMap<>();
					source.put(1, array); source.put(2, array);
					if (invalid) source.put("invalid-key", array);
					if (invalid) assertThrows(DatabaseException.class, () -> map(mapper(cached), source, LongKeyLists.class, dialect, "MAP(INTEGER, INTEGER[])"));
					else assertEquals(Map.of(1L, List.of(7L), 2L, List.of(7L)),
							map(mapper(cached), source, LongKeyLists.class, dialect, "MAP(INTEGER, INTEGER[])").value());
					assertEquals(1, reads.get()); assertEquals(1, frees.get());
				}
			}
		}
	}

	@Test
	void nativeMapLegsBorrowSharedArrayHandlesBeforeTypedSiblingsAndDuckRetainsNativeNormalization() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (DatabaseType dialect : List.of(DatabaseType.GENERIC, DatabaseType.DUCK_DB)) {
				for (boolean nativeKey : new boolean[]{false, true}) {
					AtomicInteger reads = new AtomicInteger(), frees = new AtomicInteger();
					Array array = new FaultArray(null, null, reads, frees);
					Map<Object, Object> source = new LinkedHashMap<>(); source.put(array, array);
					Map<?, ?> result = nativeKey
							? map(mapper(cached), source, SharedNativeKey.class, dialect, "MAP(INTEGER[], INTEGER[])").value()
							: map(mapper(cached), source, SharedNativeArray.class, dialect, "MAP(INTEGER[], INTEGER[])").value();
					Object raw = nativeKey ? result.keySet().iterator().next() : result.values().iterator().next();
					if (dialect == DatabaseType.GENERIC) { assertSame(array, raw); assertEquals(0, frees.get()); }
					else { assertArrayEquals(new Object[]{7}, (Object[]) raw); assertEquals(1, frees.get()); }
					assertEquals(1, reads.get());
				}
				AtomicInteger reads = new AtomicInteger(), frees = new AtomicInteger();
				Array array = new FaultArray(null, null, reads, frees);
				Object raw = map(mapper(cached), Map.of("array", array), NamedObjects.class, dialect, "MAP(VARCHAR, INTEGER[])").value().get("array");
				if (dialect == DatabaseType.GENERIC) { assertSame(array, raw); assertEquals(0, reads.get()); assertEquals(0, frees.get()); }
				else { assertArrayEquals(new Object[]{7}, (Object[]) raw); assertEquals(1, reads.get()); assertEquals(1, frees.get()); }
			}
		}
	}

	@Test
	void failedArrayExtractionsAreMemoizedWithoutMaskingCleanupFailuresOrSelfSuppressing() throws Exception {
		for (boolean readFails : new boolean[]{false, true}) {
			AtomicInteger reads = new AtomicInteger(), frees = new AtomicInteger();
			SQLException read = new SQLException("array read"), cleanup = new SQLException("array free");
			Array array = new FaultArray(readFails ? read : null, cleanup, reads, frees);
			try (MappingResources resources = new MappingResources(null, null)) {
				for (int pass = 0; pass < 2; pass++) {
					SQLException failure = assertThrows(SQLException.class,
							() -> DefaultResultSetMapper.extractSqlArrayResultSetValue(array, resources));
					assertSame(readFails ? read : cleanup, failure);
					assertArrayEquals(readFails ? new Throwable[]{cleanup} : new Throwable[0], failure.getSuppressed());
				}
			}
			assertEquals(1, reads.get()); assertEquals(1, frees.get());
		}
	}

	@Test
	void sharedArrayMemoRetainsTemporalMetadataAcrossReentrantScopesAndResetsAtRowEnd() throws Exception {
		Timestamp stamp = Timestamp.valueOf("2024-07-01 12:34:56.123456789");
		AtomicInteger reads = new AtomicInteger(), frees = new AtomicInteger();
		Array array = (Array) Proxy.newProxyInstance(Array.class.getClassLoader(), new Class<?>[]{Array.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getArray" -> { reads.incrementAndGet(); yield new Object[]{stamp}; }
					case "getBaseType" -> Types.TIMESTAMP;
					case "getBaseTypeName" -> "TIMESTAMP";
					case "free" -> { frees.incrementAndGet(); yield null; }
					default -> throw new UnsupportedOperationException(method.getName());
				});
		MappingResources outer = new MappingResources(null, null).acquire();
		MappingResources inner = outer.acquire();
		Object values = DefaultResultSetMapper.extractSqlArrayResultSetValue(array, inner);
		inner.close();
		assertSame(values, DefaultResultSetMapper.extractSqlArrayResultSetValue(array, outer));
		assertEquals(Types.TIMESTAMP, outer.arrayType(values));
		assertEquals(1, reads.get()); assertEquals(1, frees.get());
		outer.close();
		assertNull(outer.sqlArrayExtraction(array));
	}

	@Test
	void alreadyExtractedListTimestampElementsRetainTheirSqlDeclaration() throws Exception {
		LocalDateTime local = LocalDateTime.parse("2024-07-01T12:34:56.123456789");
		ZoneId zone = ZoneId.of("Asia/Kathmandu");
		for (boolean cached : new boolean[]{false, true}) {
			Map<String, List<Timestamp>> source = Map.of("wall", Arrays.asList(Timestamp.valueOf(local), null));
			assertEquals(Arrays.asList(local.atZone(zone).toInstant(), null),
					map(mapper(cached), source, InstantLists.class, DatabaseType.DUCK_DB, "MAP(VARCHAR, TIMESTAMP[])", zone).value().get("wall"));
			DatabaseException missingMetadata = assertThrows(DatabaseException.class,
					() -> map(mapper(cached), Map.of("wall", Timestamp.valueOf(local)), Instants.class, DatabaseType.DUCK_DB, "MAP", zone));
			assertTrue(missingMetadata.getMessage().contains("requires declared TIMESTAMP or TIMESTAMPTZ metadata"));
		}
	}

	@Test
	void typedLobEntriesDetachAndFreeOnSuccessAndLaterFailureWhileCustomFallbackBorrows() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (boolean invalid : new boolean[]{false, true}) {
				for (boolean customFallback : new boolean[]{false, true}) {
					AtomicInteger frees = new AtomicInteger();
					Map<String, Object> source = new LinkedHashMap<>();
					source.put("binary", blob(frees));
					if (invalid) source.put("invalid", new Object());
					ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(customFallback ? List.of(new CustomColumnMapper() {
						@Override public Boolean appliesTo(TargetType target) { return target.isMap(); }
						@Override public MappingResult map(StatementContext<?> context, ResultSet result, Object raw,
								TargetType target, Integer column, String label, InstanceProvider provider) {
							assertSame(source, raw); return MappingResult.fallback();
						}
					}) : List.of()).build();
					if (invalid) assertThrows(DatabaseException.class, () -> map(mapper, source, Binary.class));
					else assertArrayEquals(new byte[]{1, 2}, map(mapper, source, Binary.class).value().get("binary"));
					assertEquals(customFallback ? 0 : 1, frees.get());
				}
			}
		}
	}

	private static Blob blob(AtomicInteger frees) {
		return (Blob) Proxy.newProxyInstance(Blob.class.getClassLoader(), new Class<?>[]{Blob.class}, (proxy, method, args) -> switch (method.getName()) {
			case "length" -> 2L;
			case "getBinaryStream" -> new ByteArrayInputStream(new byte[]{1, 2});
			case "free" -> { frees.incrementAndGet(); yield null; }
			default -> throw new UnsupportedOperationException(method.getName());
		});
	}
	private static ResultSetMapper mapper(boolean cached) { return ResultSetMapper.withPlanCachingEnabled(cached).build(); }
	private static <T> T map(ResultSetMapper mapper, Object source, Class<T> rowType) throws Exception {
		return map(mapper, source, rowType, DatabaseType.GENERIC, "MAP");
	}
	private static <T> T map(ResultSetMapper mapper, Object source, Class<T> rowType, DatabaseType dialect, String typeName) throws Exception {
		return map(mapper, source, rowType, dialect, typeName, ZoneId.systemDefault());
	}
	private static <T> T map(ResultSetMapper mapper, Object source, Class<T> rowType, DatabaseType dialect, String typeName, ZoneId zone) throws Exception {
		DataSource dataSource = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> { throw new UnsupportedOperationException(method.getName()); });
		Database database = Database.with(dataSource, dialect).timeZone(zone).build();
		StatementContext<T> context = StatementContext.<T>with(Statement.of("typed-map", "SELECT value"), database).resultSetRowType(rowType).build();
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnLabel", "getColumnName" -> "value";
					case "getColumnType" -> Types.JAVA_OBJECT;
					case "getColumnTypeName" -> typeName;
					default -> defaultValue(method.getReturnType());
				});
		ResultSet result = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject" -> source;
					case "wasNull" -> source == null;
					default -> defaultValue(method.getReturnType());
				});
		return mapper.map(context, result, rowType, new InstanceProvider() {}).orElseThrow();
	}
	private static Object defaultValue(Class<?> type) {
		if (!type.isPrimitive()) return null;
		if (type == boolean.class) return false;
		if (type == int.class) return 0;
		if (type == long.class) return 0L;
		return null;
	}
}
