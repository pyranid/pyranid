/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MappingOptionsIT {
	@TempDir Path directory;
	public record Person(Integer id, String name) {}
	public record ObjectRow(Object value) {}
	public record NativeMap(Map<String, Object> value) {}
	public record NativeList(List<Object> value) {}

	@Test
	void duckDbStrictColumnsRetainDirectStructFallbackAndMapScalarRules() {
		for (boolean cached : new boolean[]{false, true}) {
			Database strict = database(DatabaseType.DUCK_DB, cached, true, true, 5L);
			for (int pass = 0; pass < 2; pass++) {
				assertEquals(new Person(1, "Ada"), strict.query("SELECT 1 AS id, 'Ada' AS name").fetchObject(Person.class).orElseThrow());
				DatabaseException failure = assertThrows(DatabaseException.class,
						() -> strict.query("SELECT 1 AS id, 'Ada' AS name, 2 AS extra").fetchObject(Person.class));
				assertTrue(failure.getMessage().contains("extra"));
				assertEquals(new Person(1, "Ada"), strict.query("SELECT {'id': 1, 'name': 'Ada', 'extra': 2} AS payload").fetchObject(Person.class).orElseThrow());
				assertTrue(strict.query("SELECT CAST(NULL AS STRUCT(id INTEGER, name VARCHAR)) AS payload").fetchObject(Person.class).isEmpty());
				assertThrows(DatabaseException.class, () -> strict.query("SELECT {'id': 1, 'name': 'Ada'} AS payload, 2 AS extra").fetchObject(Person.class));
				assertEquals(Map.of("extra", 2), strict.query("SELECT 2 AS extra").fetchObject(Query.mapRowType()).orElseThrow());
				assertEquals(2, strict.query("SELECT 2 AS extra").fetchObject(Integer.class).orElseThrow());
			}
		}
	}

	@Test
	void duckDbOptInBoundsNativeBlobValuesAndObjectLeavesWithoutChangingDefaultNormalization() {
		byte[] expected = "hello".getBytes(StandardCharsets.UTF_8);
		for (boolean cached : new boolean[]{false, true}) {
			Database enough = database(DatabaseType.DUCK_DB, cached, false, true, 5L);
			Database small = database(DatabaseType.DUCK_DB, cached, false, true, 4L);
			Database legacy = database(DatabaseType.DUCK_DB, cached, false, false, 4L);
			assertArrayEquals(expected, (byte[]) enough.query("SELECT 'hello'::BLOB AS value").fetchObject(Object.class).orElseThrow());
			assertArrayEquals(expected, (byte[]) enough.query("SELECT 'hello'::BLOB AS value").fetchObject(ObjectRow.class).orElseThrow().value());
			assertArrayEquals(expected, (byte[]) enough.query("SELECT 'hello'::BLOB AS value").fetchObject(Query.mapRowType()).orElseThrow().get("value"));
			assertArrayEquals(expected, (byte[]) legacy.query("SELECT 'hello'::BLOB AS value").fetchObject(Query.mapRowType()).orElseThrow().get("value"));
			for (Class<?> target : List.of(Object.class, ObjectRow.class, Map.class))
				assertThrows(DatabaseException.class, () -> small.query("SELECT 'hello'::BLOB AS value").fetchObject(target));
			assertThrows(DatabaseException.class, () -> small.query("SELECT MAP(['blob'], ['hello'::BLOB]) AS value").fetchObject(NativeMap.class));
			assertThrows(DatabaseException.class, () -> small.query("SELECT ['hello'::BLOB] AS value").fetchObject(NativeList.class));
			assertThrows(DatabaseException.class, () -> legacy.query("SELECT ['hello'::BLOB] AS value").fetchObject(byte[][].class),
					"Typed BLOB arrays keep their limits even when native detachment is disabled");
			assertNull(enough.query("SELECT NULL::BLOB AS value").fetchObject(ObjectRow.class).orElseThrow().value());
		}
	}

	@Test
	void duckDbDetachedNativeStructAndListLobsSurviveTheQueryConnection() {
		Database database = database(DatabaseType.DUCK_DB, true, false, true, 5L);
		Map<?, ?> value = (Map<?, ?>) database.query("SELECT {'blob': 'hello'::BLOB, 'list': ['world'::BLOB, NULL]} AS value")
				.fetchObject(ObjectRow.class).orElseThrow().value();
		assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), (byte[]) value.get("blob"));
		Object[] list = (Object[]) value.get("list");
		assertArrayEquals("world".getBytes(StandardCharsets.UTF_8), (byte[]) list[0]);
		assertNull(list[1]);
		assertEquals(2, database.query("SELECT 2").fetchObject(Integer.class).orElseThrow());
	}

	@Test
	void sqliteStrictMappingRejectsExtrasAcrossFetchListStreamAndCachedPlans() {
		for (boolean cached : new boolean[]{false, true}) {
			Database strict = database(DatabaseType.SQLITE, cached, true, false, 5L);
			for (int pass = 0; pass < 2; pass++) {
				assertEquals(new Person(1, "Ada"), strict.query("SELECT 1 AS id, 'Ada' AS name").fetchObject(Person.class).orElseThrow());
				assertThrows(DatabaseException.class, () -> strict.query("SELECT 1 AS id, 'Ada' AS name, 2 AS extra").fetchList(Person.class));
				assertThrows(DatabaseException.class, () -> strict.query("SELECT 1 AS id, 'Ada' AS name, 2 AS extra").fetchStream(Person.class, rows -> rows.toList()));
				assertEquals(new Person(1, null), strict.query("SELECT 1 AS id").fetchObject(Person.class).orElseThrow());
			}
		}
	}

	private Database database(DatabaseType type, boolean cached, boolean strict, boolean detached, long bytes) {
		String url = type == DatabaseType.DUCK_DB ? "jdbc:duckdb:" : "jdbc:sqlite:" + directory.resolve("options_" + cached + ".sqlite");
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(url, null, null), type)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).failOnUnmappedColumns(strict).detachLobValues(detached)
						.maxLobBytes(bytes).build()).build();
	}
}
