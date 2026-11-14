/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class LocalTemporalZoneIT {
	@Container private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("local_temporal").withUsername("pyranid").withPassword("pyranid");
	@TempDir Path directory;

	@Test
	void postgresqlLocalValuesRetainIdentityInZonedColumns() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		verify(source, DatabaseType.POSTGRESQL);
	}

	@Test
	void duckDbLocalValuesRetainIdentityInZonedColumns() {
		verify(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:duckdb:" + directory.resolve("local.duckdb"), null, null),
				DatabaseType.DUCK_DB);
	}

	private static void verify(DataSource source, DatabaseType type) {
		ZoneId zone = ZoneId.of("Asia/Kathmandu");
		Database database = Database.with(source, type).timeZone(zone).build();
		String table = "local_temporal_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, value TIMESTAMP WITH TIME ZONE)").execute();
		try {
			int id = 0;
			for (Object value : List.of(LocalDate.parse("2024-01-02"), LocalDateTime.parse("2024-07-02T12:34:56.123456"))) {
				LocalDateTime local = value instanceof LocalDate date ? date.atStartOfDay() : (LocalDateTime) value;
				database.query("INSERT INTO " + table + " VALUES (:id, :value)").bind("id", ++id).bind("value", value).execute();
				assertEquals(local.atZone(zone).toInstant(), database.query("SELECT value FROM " + table + " WHERE id=:id")
						.bind("id", id).fetchObject(Instant.class).orElseThrow());
				assertEquals(local, database.query("SELECT value FROM " + table + " WHERE id=:id")
						.bind("id", id).fetchObject(LocalDateTime.class).orElseThrow());
				assertEquals(1L, database.query("SELECT COUNT(*) FROM " + table + " WHERE value=:value").bind("value", value)
						.fetchObject(Long.class).orElseThrow());
				assertEquals(1L, database.query("UPDATE " + table + " SET value=:value WHERE id=:id AND value=:value")
						.bind("id", id).bind("value", value).execute());
			}
		} finally { database.query("DROP TABLE " + table).execute(); }
	}
}
