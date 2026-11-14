/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlMappingOptionsIT {
	@Container private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("mapping_options").withUsername("pyranid").withPassword("pyranid");
	public record Id(Integer id) {}
	public record ObjectRow(Object value) {}

	@Test
	void strictMappingRejectsUnclaimedColumnsWhileMapRowsAndPartialTargetsRemainAvailable() {
		for (boolean cached : new boolean[]{false, true}) {
			Database strict = database(cached, true, true, 20L);
			assertEquals(new Id(1), strict.query("SELECT 1 AS id").fetchObject(Id.class).orElseThrow());
			assertThrows(DatabaseException.class, () -> strict.query("SELECT 1 AS id, 2 AS ignored").fetchObject(Id.class));
			assertThrows(DatabaseException.class, () -> strict.query("SELECT 1 AS id, 2 AS ignored").fetchStream(Id.class, rows -> rows.toList()));
			assertEquals(Map.of("id", 1, "ignored", 2), strict.query("SELECT 1 AS id, 2 AS ignored").fetchObject(Query.mapRowType()).orElseThrow());
		}
	}

	@Test
	void sqlXmlNativeValuesDetachWithBoundsAndOrdinaryByteaRemainsOrdinaryBinary() {
		String xml = "<root>hello</root>";
		String sql = "SELECT XMLPARSE(DOCUMENT CAST(:xml AS text)) AS value";
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, false, true, (long) xml.length());
			assertEquals(xml, database.query(sql).bind("xml", xml).fetchObject(Object.class).orElseThrow());
			assertEquals(xml, database.query(sql).bind("xml", xml).fetchObject(ObjectRow.class).orElseThrow().value());
			assertEquals(xml, database.query(sql).bind("xml", xml).fetchObject(Query.mapRowType()).orElseThrow().get("value"));
			assertEquals(List.of(xml), database.query(sql).bind("xml", xml).fetchStream(Object.class, rows -> rows.toList()));
			Database small = database(cached, false, true, (long) xml.length() - 1);
			for (Class<?> target : List.of(Object.class, ObjectRow.class, Map.class))
				assertThrows(DatabaseException.class, () -> small.query(sql).bind("xml", xml).fetchObject(target));
			assertTrue(database.query("SELECT NULL::xml AS value").fetchObject(Object.class).isEmpty());
			assertArrayEquals(new byte[]{0, 1, 2}, (byte[]) database.query("SELECT decode('000102', 'hex') AS value").fetchObject(Object.class).orElseThrow());
			byte[][] ordinary = database.query("SELECT ARRAY[decode('000102', 'hex'), NULL] AS value").fetchObject(byte[][].class).orElseThrow();
			assertArrayEquals(new byte[]{0, 1, 2}, ordinary[0]); assertNull(ordinary[1]);
		}
	}

	private static Database database(boolean cached, boolean strict, boolean detached, long characters) {
		PGSimpleDataSource source = new PGSimpleDataSource(); source.setUrl(POSTGRES.getJdbcUrl());
		source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return Database.with(source, DatabaseType.POSTGRESQL).resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached)
				.failOnUnmappedColumns(strict).detachLobValues(detached).maxLobBytes(1L).maxLobCharacters(characters).build()).build();
	}
}
