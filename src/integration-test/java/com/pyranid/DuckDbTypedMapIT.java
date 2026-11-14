/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbTypedMapIT {
	public record Values(Map<UUID, Long> value) {}
	public record NamedValues(Map<String, Long> value) {}
	public record Item(Long count, List<Long> values) {}
	public record Items(Map<UUID, Item> value) {}
	public record MapLists(List<Map<UUID, Long>> value) {}
	public record Nested(Map<String, Map<UUID, Long>> value) {}
	public record BooleanKeys(Map<Boolean, Long> value) {}
	public record TemporalMap(Map<Instant, Instant> value) {}
	public record TemporalMapList(List<Map<Instant, Instant>> value) {}
	public record NestedTemporalMap(Map<String, List<Map<Instant, Instant>>> value) {}
	public record TemporalPayload(TemporalMap payload) {}
	public record TemporalFields(Map<String, Instant> value) {}
	public record NativeValues(Map<Object, Object> value) {}
	public record NativeFields(Map<String, Object> value) {}
	public record MixedTemporal(Map<Instant, Object> value) {}
	public record Binary(Map<String, byte[]> value) {}
	public static class GenericBean<K, V> {
		private Map<K, V> value;
		public void setValue(Map<K, V> value) { this.value = value; }
		public Map<K, V> getValue() { return value; }
	}
	public static class ValuesBean extends GenericBean<UUID, Long> {}

	@Test
	void realMapKeysAndValuesConvertForRecordsAndInheritedGenericBeanSetters() {
		UUID first = UUID.randomUUID(), second = UUID.randomUUID();
		String sql = "SELECT MAP(['" + second + "', '" + first + "'], [2, NULL]) AS value";
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, ZoneId.of("UTC"));
			for (int pass = 0; pass < 2; pass++) {
				Map<UUID, Long> values = database.query(sql).fetchObject(Values.class).orElseThrow().value();
				assertEquals(List.of(second, first), new ArrayList<>(values.keySet()));
				assertEquals(2L, values.get(second)); assertNull(values.get(first));
				assertEquals(values, database.query(sql).fetchObject(ValuesBean.class).orElseThrow().getValue());
			}
		}
	}

	@Test
	void realNestedMapsMapArraysAndStructValuesConvertRecursively() {
		UUID key = UUID.randomUUID();
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, ZoneId.of("UTC"));
			String sqlMap = "MAP(['" + key + "'], [3])";
			assertEquals(Arrays.asList(Map.of(key, 3L), null), database.query("SELECT [" + sqlMap + ", NULL] AS value")
					.fetchObject(MapLists.class).orElseThrow().value());
			assertEquals(Map.of("nested", Map.of(key, 3L)), database.query("SELECT MAP(['nested'], [" + sqlMap + "]) AS value")
					.fetchObject(Nested.class).orElseThrow().value());
			Item expected = new Item(7L, Arrays.asList(1L, null, 3L));
			assertEquals(Map.of(key, expected), database.query("SELECT MAP(['" + key + "'], [{'count': 7, 'values': [1, NULL, 3]}]) AS value")
					.fetchObject(Items.class).orElseThrow().value());
		}
	}

	@Test
	void structAttributesAlsoHonorTypedMapValuesAndNativeObjectValuesRemainNative() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, ZoneId.of("UTC"));
			String sql = "SELECT {'first': 1::INTEGER, 'second': 2::SMALLINT, 'missing': NULL::INTEGER} AS value";
			Map<String, Long> result = database.query(sql).fetchObject(NamedValues.class).orElseThrow().value();
			assertEquals(List.of("first", "second", "missing"), new ArrayList<>(result.keySet()));
			assertEquals(1L, result.get("first")); assertEquals(2L, result.get("second")); assertNull(result.get("missing"));
			Map<String, Object> raw = database.query(sql).fetchObject(NativeFields.class).orElseThrow().value();
			assertInstanceOf(Integer.class, raw.get("first")); assertInstanceOf(Short.class, raw.get("second"));
		}
	}

	@Test
	void distinctSqlKeysThatConvertToTheSameJavaKeyFailInsteadOfLosingEntries() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, ZoneId.of("UTC"));
			DatabaseException collision = assertThrows(DatabaseException.class,
					() -> database.query("SELECT MAP(['true', 't'], [NULL, 2]) AS value").fetchObject(BooleanKeys.class));
			assertTrue(collision.getMessage().contains("duplicate key after conversion"), collision::getMessage);
			assertThrows(DatabaseException.class, () -> database.query("SELECT MAP(['bad'], ['private-payload']) AS value").fetchObject(NamedValues.class));
		}
	}

	@Test
	void mapTimestampKeysAndValuesRespectSqlSemanticsInDifferentConfiguredZones() {
		LocalDateTime local = LocalDateTime.parse("2024-07-01T12:34:56.123456");
		Instant zoned = OffsetDateTime.parse("2024-07-01T12:34:56.123456+05:45").toInstant();
		String wall = "TIMESTAMP '2024-07-01 12:34:56.123456'", withZone = "TIMESTAMPTZ '2024-07-01 12:34:56.123456+05:45'";
		for (boolean cached : new boolean[]{false, true}) {
			for (ZoneId zone : List.of(ZoneId.of("UTC"), ZoneId.of("Asia/Kathmandu"))) {
				Database database = database(cached, zone);
				Instant wallInstant = local.atZone(zone).toInstant();
				for (int pass = 0; pass < 2; pass++) {
					String map = "MAP([" + wall + "], [" + withZone + "])";
					assertEquals(Map.of(wallInstant, zoned), database.query("SELECT " + map + " AS value").fetchObject(TemporalMap.class).orElseThrow().value());
					assertEquals(Map.of(zoned, wallInstant), database.query("SELECT MAP([" + withZone + "], [" + wall + "]) AS value")
							.fetchObject(TemporalMap.class).orElseThrow().value());
					assertEquals(Map.of(wallInstant, zoned), database.query("SELECT {'value': " + map + "} AS payload")
							.fetchObject(TemporalPayload.class).orElseThrow().payload().value());
					assertEquals(Arrays.asList(Map.of(wallInstant, zoned), null), database.query("SELECT [" + map + ", NULL] AS value")
							.fetchObject(TemporalMapList.class).orElseThrow().value());
					assertEquals(Map.of("nested", List.of(Map.of(wallInstant, zoned))), database.query("SELECT MAP(['nested'], [[" + map + "]]) AS value")
							.fetchObject(NestedTemporalMap.class).orElseThrow().value());
					assertEquals(Map.of("local", wallInstant, "zoned", zoned), database.query("SELECT {'local': " + wall + ", 'zoned': " + withZone + "} AS value")
							.fetchObject(TemporalFields.class).orElseThrow().value());
				}
			}
		}
	}

	@Test
	void nativeTemporalMapLegsRetainTheDriverRepresentation() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, ZoneId.of("Asia/Kathmandu"));
			String wall = "TIMESTAMP '2024-07-01 12:34:56.123456'";
			Map<Object, Object> raw = database.query("SELECT MAP([" + wall + "], [" + wall + "]) AS value")
					.fetchObject(NativeValues.class).orElseThrow().value();
			assertInstanceOf(Timestamp.class, raw.keySet().iterator().next());
			assertInstanceOf(Timestamp.class, raw.values().iterator().next());
			Map<Instant, Object> mixed = database.query("SELECT MAP([" + wall + "], [" + wall + "]) AS value")
					.fetchObject(MixedTemporal.class).orElseThrow().value();
			assertInstanceOf(Timestamp.class, mixed.values().iterator().next());
			assertEquals(LocalDateTime.parse("2024-07-01T12:34:56.123456").atZone(ZoneId.of("Asia/Kathmandu")).toInstant(), mixed.keySet().iterator().next());
			assertInstanceOf(Timestamp.class, database.query("SELECT MAP(['wall'], [" + wall + "]) AS value")
					.fetchObject(NativeFields.class).orElseThrow().value().get("wall"));
		}
	}

	@Test
	void typedMapBlobsUseTheConfiguredMaterializationLimitWhileNativeValuesKeepTheirContract() {
		String sql = "SELECT MAP(['blob'], [from_hex('0102')]) AS value";
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, ZoneId.of("UTC"));
			assertArrayEquals(new byte[]{1, 2}, database.query(sql).fetchObject(Binary.class).orElseThrow().value().get("blob"));
			Database limited = Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:duckdb:", null, null), DatabaseType.DUCK_DB)
					.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).maxLobBytes(1L).build()).build();
			assertThrows(DatabaseException.class, () -> limited.query(sql).fetchObject(Binary.class));
			assertArrayEquals(new byte[]{1, 2}, (byte[]) limited.query(sql).fetchObject(NativeFields.class).orElseThrow().value().get("blob"));
		}
	}

	private static Database database(boolean cached, ZoneId zone) {
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:duckdb:", null, null), DatabaseType.DUCK_DB)
				.timeZone(zone).resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
