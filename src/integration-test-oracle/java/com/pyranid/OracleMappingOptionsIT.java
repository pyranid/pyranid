/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class OracleMappingOptionsIT {
	@Container private static final OracleContainer ORACLE = new OracleContainer(DockerImageName.parse(
			System.getProperty("oracle.integration.image", "gvenzl/oracle-free:23-slim-faststart"))
			.asCompatibleSubstituteFor("gvenzl/oracle-free")).withUsername("pyranid").withPassword("pyranid");
	public record ObjectRow(Object value) {}
	public record Id(Integer id) {}

	@Test
	void oracleBlobClobAndNclobNativeResultsDetachAndEnforceLimitsAcrossRowShapes() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, true, 5L, 4L);
			Database small = database(cached, true, 4L, 3L);
			Database legacy = database(cached, false, 1L, 1L);
			for (String expression : List.of("TO_CLOB('hello')", "TO_NCLOB('hello')")) {
				String sql = "SELECT " + expression + " AS value FROM dual";
				assertEquals("hello", database.query(sql).fetchObject(Object.class).orElseThrow());
				assertEquals("hello", database.query(sql).fetchObject(ObjectRow.class).orElseThrow().value());
				assertEquals("hello", database.query(sql).fetchObject(Query.mapRowType()).orElseThrow().get("value"));
				assertEquals(List.of("hello"), database.query(sql).fetchStream(Object.class, rows -> rows.toList()));
				assertInstanceOf(java.sql.Clob.class, legacy.query(sql).fetchObject(Object.class).orElseThrow());
				for (Class<?> target : List.of(Object.class, ObjectRow.class, Map.class))
					assertThrows(DatabaseException.class, () -> small.query(sql).fetchObject(target));
			}
			String binary = "SELECT TO_BLOB(HEXTORAW('0001FEFF')) AS value FROM dual";
			byte[] expected = {0, 1, (byte) 0xfe, (byte) 0xff};
			assertArrayEquals(expected, (byte[]) database.query(binary).fetchObject(Object.class).orElseThrow());
			assertArrayEquals(expected, (byte[]) database.query(binary).fetchObject(ObjectRow.class).orElseThrow().value());
			assertArrayEquals(expected, (byte[]) database.query(binary).fetchObject(Query.mapRowType()).orElseThrow().get("value"));
			assertInstanceOf(java.sql.Blob.class, legacy.query(binary).fetchObject(Object.class).orElseThrow());
			assertThrows(DatabaseException.class, () -> small.query(binary).fetchObject(Object.class));
			assertNull(database.query("SELECT TO_CLOB(NULL) AS value FROM dual").fetchObject(ObjectRow.class).orElseThrow().value());
			assertEquals(new Id(1), database.query("SELECT 1 AS id FROM dual").fetchObject(Id.class).orElseThrow());
			assertThrows(DatabaseException.class, () -> database.query("SELECT 1 AS id, 2 AS ignored FROM dual").fetchObject(Id.class));
		}
	}

	private static Database database(boolean cached, boolean detached, long characters, long bytes) {
		var source = new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(ORACLE.getJdbcUrl(), ORACLE.getUsername(), ORACLE.getPassword());
		return Database.with(source, DatabaseType.ORACLE).resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached)
				.failOnUnmappedColumns(true).detachLobValues(detached).maxLobCharacters(characters).maxLobBytes(bytes).build()).build();
	}
}
