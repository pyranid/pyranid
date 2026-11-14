/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BeanFieldAnnotationIT {
	@TempDir Path directory;
	public static class Row {
		@DatabaseColumn("stored_active") private boolean isActive;
		@DatabaseColumn("stored_x") private int xCoordinate;
		@DatabaseColumn("stored_url") private String URL;
		public void setActive(boolean value) { isActive = value; }
		public void setXCoordinate(int value) { xCoordinate = value; }
		public void setURL(String value) { URL = value; }
	}
	public static class InvalidRow {
		@DatabaseColumn("stored_active") private boolean isActive;
		public boolean isActive() { return isActive; }
	}
	public record RecordRow(@DatabaseColumn("stored_url") String URL) {}

	@Test
	void sqliteProjectsAnnotatedFieldsIntoTheirWritableBeanProperties() { verify(DatabaseType.SQLITE); }

	@Test
	void duckDbProjectsAnnotatedFieldsIntoTheirWritableBeanProperties() { verify(DatabaseType.DUCK_DB); }

	private void verify(DatabaseType type) {
		for (boolean cached : new boolean[]{false, true}) {
			String file = type + "_" + cached;
			String url = type == DatabaseType.SQLITE ? "jdbc:sqlite:" + directory.resolve(file + ".sqlite")
					: "jdbc:duckdb:" + directory.resolve(file + ".duckdb");
			Database database = Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(url, null, null), type)
					.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
			database.query("CREATE TABLE work (id INT PRIMARY KEY, enabled BOOLEAN, coordinate INT, site VARCHAR(32))").execute();
			database.query("INSERT INTO work VALUES (1, :active, 7, :site), (2, :inactive, 8, :other)")
					.bind("active", true).bind("inactive", false).bind("site", "first").bind("other", "second").execute();
			String query = "SELECT enabled AS stored_active, coordinate AS stored_x, site AS stored_url FROM work ORDER BY id";
			for (int execution = 0; execution < 2; execution++) {
				List<Row> rows = database.query(query).fetchList(Row.class);
				assertEquals(2, rows.size());
				assertTrue(rows.get(0).isActive); assertEquals(7, rows.get(0).xCoordinate); assertEquals("first", rows.get(0).URL);
				assertFalse(rows.get(1).isActive); assertEquals(8, rows.get(1).xCoordinate); assertEquals("second", rows.get(1).URL);
				assertEquals(List.of("first"), database.query(query).fetchStream(Row.class, stream -> stream.limit(1).map(row -> row.URL).toList()));
				assertEquals(new RecordRow("first"), database.query("SELECT site AS stored_url FROM work WHERE id=1")
						.fetchObject(RecordRow.class).orElseThrow());
			}
			DatabaseException invalid = assertThrows(DatabaseException.class,
					() -> database.query("SELECT coordinate AS unrelated FROM work").fetchList(InvalidRow.class));
			assertTrue(invalid.getMessage().contains("no writable JavaBean property"));
			assertTrue(invalid.getMessage().contains("isActive"));
			assertEquals(2L, database.query("SELECT COUNT(*) FROM work").fetchObject(Long.class).orElseThrow());
		}
	}
}
