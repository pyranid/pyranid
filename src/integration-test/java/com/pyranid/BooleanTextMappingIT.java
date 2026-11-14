/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BooleanTextMappingIT {
	@TempDir Path directory;
	public record Flag(Boolean flag) {}
	public record PrimitiveFlag(boolean flag) {}
	public record Flags(List<Boolean> flag) {}
	public record NestedFlags(List<List<Boolean>> flag) {}
	public record Payload(Flag payload) {}
	public static class FlagBean {
		private Boolean flag;
		public Boolean getFlag() { return flag; }
		public void setFlag(Boolean flag) { this.flag = flag; }
	}
	public static class PrimitiveFlagBean {
		private boolean flag;
		public boolean getFlag() { return flag; }
		public void setFlag(boolean flag) { this.flag = flag; }
	}

	@Test
	void sqliteTextBooleansUseTheSameStrictRuleAcrossScalarAndRowMappings() {
		verify(DatabaseType.SQLITE);
	}

	@Test
	void duckDbTextBooleansUseTheSameStrictRuleAcrossScalarAndRowMappings() {
		verify(DatabaseType.DUCK_DB);
	}

	@Test
	void duckDbTextInNestedArraysAndStructsUsesTheSameBooleanRule() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(DatabaseType.DUCK_DB, "nested_" + cached, cached);
			String array = "SELECT ['t', 'of', NULL, 'yes'] AS flag";
			assertArrayEquals(new Boolean[]{true, false, null, true}, database.query(array).fetchObject(Boolean[].class).orElseThrow());
			assertEquals(Arrays.asList(true, false, null, true), database.query(array).fetchObject(Flags.class).orElseThrow().flag());
			assertThrows(DatabaseException.class, () -> database.query(array).fetchObject(boolean[].class));
			assertEquals(List.of(List.of(true, false), Arrays.asList(true, null)), database.query(
					"SELECT [['on', 'OFF'], ['tr', NULL]] AS flag").fetchObject(NestedFlags.class).orElseThrow().flag());
			assertEquals(new Flag(true), database.query("SELECT {'flag': 'yes'} AS payload").fetchObject(Flag.class).orElseThrow());
			assertEquals(new Payload(new Flag(false)), database.query("SELECT {'flag': 'off'} AS payload").fetchObject(Payload.class).orElseThrow());
			assertThrows(DatabaseException.class, () -> database.query("SELECT ['true', 'o'] AS flag").fetchObject(Flags.class));
			assertThrows(DatabaseException.class, () -> database.query("SELECT {'flag': 'unknown'} AS payload").fetchObject(Flag.class));
		}
	}

	private void verify(DatabaseType type) {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(type, "rows_" + cached, cached);
			database.query("CREATE TABLE flags (id INT PRIMARY KEY, flag VARCHAR(64))").execute();
			List<String> values = List.of("true", "1", "t", "tr", "tru", "yes", "ye", "y", "on", " TrUe ",
					"false", "0", "f", "fa", "fal", "fals", "no", "n", "of", "off", "\tOFF\n");
			database.transaction(() -> {
				for (int id = 0; id < values.size(); id++) {
					String value = values.get(id);
					boolean expected = id < 10;
					database.query("INSERT INTO flags VALUES (:id, :flag)").bind("id", id).bind("flag", value).execute();
					String sql = "SELECT flag FROM flags WHERE id=:id";
					assertEquals(expected, database.query(sql).bind("id", id).fetchObject(Boolean.class).orElseThrow());
					assertEquals(expected, database.query(sql).bind("id", id).fetchObject(boolean.class).orElseThrow());
					assertEquals(new Flag(expected), database.query(sql).bind("id", id).fetchObject(Flag.class).orElseThrow());
					assertEquals(new PrimitiveFlag(expected), database.query(sql).bind("id", id).fetchObject(PrimitiveFlag.class).orElseThrow());
					assertEquals(expected, database.query(sql).bind("id", id).fetchObject(FlagBean.class).orElseThrow().getFlag());
					assertEquals(expected, database.query(sql).bind("id", id).fetchObject(PrimitiveFlagBean.class).orElseThrow().getFlag());
					assertEquals(value, database.query(sql).bind("id", id).fetchObject(Object.class).orElseThrow());
					assertEquals(Map.of("flag", value), database.query(sql).bind("id", id).fetchObject(Map.class).orElseThrow());
				}
				for (String invalid : List.of("o", "2", "", "unrecognized")) {
					database.query("INSERT INTO flags VALUES (100, :flag)").bind("flag", invalid).execute();
					String sql = "SELECT flag FROM flags WHERE id=100";
					for (Class<?> target : List.of(Boolean.class, boolean.class, Flag.class, PrimitiveFlag.class, FlagBean.class, PrimitiveFlagBean.class))
						assertThrows(DatabaseException.class, () -> database.query(sql).fetchObject(target));
					database.query("DELETE FROM flags WHERE id=100").execute();
				}
				database.query("INSERT INTO flags VALUES (101, NULL)").execute();
				assertTrue(database.query("SELECT flag FROM flags WHERE id=101").fetchObject(Boolean.class).isEmpty());
				assertEquals(new Flag(null), database.query("SELECT flag FROM flags WHERE id=101").fetchObject(Flag.class).orElseThrow());
				assertNull(database.query("SELECT flag FROM flags WHERE id=101").fetchObject(FlagBean.class).orElseThrow().getFlag());
			});
		}
	}

	private Database database(DatabaseType type, String name, boolean cached) {
		String url = (type == DatabaseType.SQLITE ? "jdbc:sqlite:" : "jdbc:duckdb:") + directory.resolve(type.name() + name + ".db");
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(url, null, null), type)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
