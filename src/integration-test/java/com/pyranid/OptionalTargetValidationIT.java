/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class OptionalTargetValidationIT {
	@TempDir Path directory;
	public record OptionalRow(Optional<String> value) {}
	public record OptionalList(List<Optional<String>> value) {}
	public record OptionalMap(Map<String, Optional<String>> value) {}
	public record OptionalArray(Optional<String>[] value) {}
	public record NestedRecord(OptionalRow value) {}
	public record NestedRows(List<OptionalRow> value) {}
	public record Envelope(OptionalRow payload) {}
	public record Partial(int id, Optional<String> absent) {}
	public static class OptionalBean {
		public void setValue(Optional<String> value) { throw new AssertionError("Unsupported Optional targets cannot reach a setter"); }
	}

	@Test
	void sqliteSelectedOptionalsRejectBothNullAndNonNullRowsAndPreservePartialMapping() { verify(DatabaseType.SQLITE); }

	@Test
	void duckDbSelectedOptionalsRejectBothNullAndNonNullRowsAndPreservePartialMapping() { verify(DatabaseType.DUCK_DB); }

	@Test
	void duckDbStructFallbackAndNestedContainersRejectDeclaredOptionalShapesBeforeNullCanHideThem() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(DatabaseType.DUCK_DB, "struct_" + cached, cached, List.of());
			for (String sql : List.of("SELECT {'value': 'private-row-value'} AS payload", "SELECT {'value': NULL} AS payload",
					"SELECT CAST(NULL AS STRUCT(value VARCHAR)) AS payload")) {
				unsupported(() -> database.query(sql).fetchObject(OptionalRow.class));
				unsupported(() -> database.query(sql).fetchObject(Envelope.class));
			}
			for (String sql : List.of("SELECT ['present'] AS value", "SELECT [NULL] AS value", "SELECT [] AS value", "SELECT NULL AS value"))
				unsupported(() -> database.query(sql).fetchObject(OptionalList.class));
			for (String sql : List.of("SELECT MAP(['entry'], ['present']) AS value", "SELECT MAP(['entry'], [NULL]) AS value", "SELECT NULL AS value"))
				unsupported(() -> database.query(sql).fetchObject(OptionalMap.class));
			for (String sql : List.of("SELECT [{'value': 'present'}] AS value", "SELECT [] AS value", "SELECT NULL AS value"))
				unsupported(() -> database.query(sql).fetchObject(NestedRows.class));
		}
	}

	private void verify(DatabaseType type) {
		for (boolean cached : new boolean[]{false, true}) {
			AtomicInteger calls = new AtomicInteger();
			CustomColumnMapper custom = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Optional.class); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object value, TargetType target,
						Integer column, String label, InstanceProvider instances) {
					calls.incrementAndGet(); return MappingResult.of(Optional.of(value));
				}
			};
			Database database = database(type, "rows_" + cached, cached, List.of(custom));
			database.query("CREATE TABLE optional_values (id INT PRIMARY KEY, value VARCHAR(128))").execute();
			database.query("INSERT INTO optional_values VALUES (1, :value), (2, NULL)").bind("value", "private-row-value").execute();
			for (int id : List.of(1, 2)) {
				for (Class<?> target : List.of(OptionalRow.class, OptionalBean.class, Optional[].class)) {
					DatabaseException failure = unsupported(() -> database.query("SELECT value FROM optional_values WHERE id = :id")
							.bind("id", id).fetchObject(target));
					assertFalse(failure.getMessage().contains("private-row-value"));
				}
			}
			assertEquals(0, calls.get());
			assertEquals(Optional.of("private-row-value"), database.query("SELECT value FROM optional_values WHERE id = 1").fetchObject(Optional.class).orElseThrow());
			assertTrue(database.query("SELECT value FROM optional_values WHERE id = 2").fetchObject(Optional.class).isEmpty());
			assertEquals(1, calls.get(), "Only the non-null scalar source reaches the manual whole-row hook");
			assertEquals("private-row-value", database.query("SELECT value FROM optional_values WHERE id = 1").fetchObject(String.class).orElseThrow());
			assertTrue(database.query("SELECT value FROM optional_values WHERE id = 2").fetchObject(String.class).isEmpty());
			assertEquals(new Partial(1, null), database.query("SELECT id FROM optional_values WHERE id = 1").fetchObject(Partial.class).orElseThrow());
			assertEquals(2L, database.query("SELECT COUNT(*) FROM optional_values").fetchObject(Long.class).orElseThrow());

			CustomColumnMapper wholeShape = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) {
					return List.of(Map.class, List.class, OptionalRow.class, Optional[].class).contains(target.getRawClass());
				}
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
						Integer column, String label, InstanceProvider instances) {
					if (raw.equals("decline")) return MappingResult.fallback();
					Optional<String> value = Optional.of(raw.toString());
					Class<?> shape = target.getRawClass();
					if (shape == Map.class) return MappingResult.of(Map.of("entry", value));
					if (shape == List.class) return MappingResult.of(List.of(value));
					if (shape == OptionalRow.class) return MappingResult.of(new OptionalRow(value));
					return MappingResult.of(new Optional<?>[]{value});
				}
			};
			Database manual = database(type, "manual_" + cached, cached, List.of(wholeShape));
			String sql = "SELECT CAST(:source AS VARCHAR(128)) AS value";
			assertEquals(new OptionalMap(Map.of("entry", Optional.of("manual"))), manual.query(sql).bind("source", "manual").fetchObject(OptionalMap.class).orElseThrow());
			assertEquals(new OptionalList(List.of(Optional.of("manual"))), manual.query(sql).bind("source", "manual").fetchObject(OptionalList.class).orElseThrow());
			assertEquals(new NestedRecord(new OptionalRow(Optional.of("manual"))), manual.query(sql).bind("source", "manual").fetchObject(NestedRecord.class).orElseThrow());
			assertArrayEquals(new Optional<?>[]{Optional.of("manual")}, manual.query(sql).bind("source", "manual").fetchObject(OptionalArray.class).orElseThrow().value());
			assertArrayEquals(new Optional<?>[]{Optional.of("manual")}, manual.query(sql).bind("source", "manual").fetchObject(Optional[].class).orElseThrow());
			assertEquals(new OptionalMap(null), manual.query(sql).bind("source", null).fetchObject(OptionalMap.class).orElseThrow());
			assertEquals(new OptionalList(null), manual.query(sql).bind("source", null).fetchObject(OptionalList.class).orElseThrow());
			assertEquals(new NestedRecord(null), manual.query(sql).bind("source", null).fetchObject(NestedRecord.class).orElseThrow());
			assertNull(manual.query(sql).bind("source", null).fetchObject(OptionalArray.class).orElseThrow().value());
			assertTrue(manual.query(sql).bind("source", null).fetchObject(Optional[].class).isEmpty());
			for (Class<?> shape : List.of(OptionalMap.class, OptionalList.class, NestedRecord.class, OptionalArray.class, Optional[].class))
				unsupported(() -> manual.query(sql).bind("source", "decline").fetchObject(shape));
		}
	}

	private Database database(DatabaseType type, String name, boolean cached, List<CustomColumnMapper> mappers) {
		String url = (type == DatabaseType.SQLITE ? "jdbc:sqlite:" : "jdbc:duckdb:") + directory.resolve(type.name() + name + ".db");
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(url, null, null), type)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(mappers).build()).build();
	}
	private static DatabaseException unsupported(org.junit.jupiter.api.function.Executable operation) {
		DatabaseException failure = assertThrows(DatabaseException.class, operation);
		assertTrue(failure.getMessage().contains("Optional-valued"), failure::getMessage);
		assertTrue(failure.getMessage().contains("custom ResultSetMapper"), failure::getMessage);
		return failure;
	}
}
