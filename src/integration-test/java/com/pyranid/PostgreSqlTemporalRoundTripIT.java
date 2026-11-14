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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlTemporalRoundTripIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_temporal_roundtrips").withUsername("pyranid").withPassword("pyranid");

	@Test
	void timestampArrayElementsMatchScalarBindingsAndRetainZonedInstants() {
		for (boolean cached : new boolean[]{false, true}) {
			for (ZoneId zone : List.of(ZoneId.of("America/New_York"), ZoneId.of("Asia/Kathmandu"))) {
				Database database = database(zone, cached);
				String table = "temporal_arrays_" + UUID.randomUUID().toString().replace("-", "");
				database.query("CREATE TABLE " + table + " (plain TIMESTAMP, zoned TIMESTAMPTZ, plain_array TIMESTAMP[], zoned_array TIMESTAMPTZ[])").execute();
				try {
					for (Instant instant : List.of(Instant.parse("2024-01-02T12:34:56.123456Z"), Instant.parse("2024-07-01T12:34:56.987654Z"))) {
						for (Object input : List.of(instant, instant.atZone(ZoneId.of("Asia/Kathmandu")).toOffsetDateTime(),
								Timestamp.from(instant), Date.from(instant))) {
							Instant expected = input instanceof Date && !(input instanceof Timestamp)
									? Instant.ofEpochMilli(instant.toEpochMilli()) : instant;
							database.query("DELETE FROM " + table).execute();
							database.query("INSERT INTO " + table + " VALUES (:scalar, :scalar, :plain, :zoned)")
									.bind("scalar", input)
									.bind("plain", Parameters.sqlArrayOf("pg_catalog.timestamp", new Object[]{input, null}))
									.bind("zoned", Parameters.sqlArrayOf("timestamptz", new Object[]{input, null})).execute();
							assertEquals(LocalDateTime.ofInstant(expected, zone), database.query("SELECT plain_array[1] FROM " + table).fetchObject(LocalDateTime.class).orElseThrow());
							for (String column : List.of("plain", "zoned", "plain_array[1]", "zoned_array[1]"))
								assertEquals(expected, database.query("SELECT " + column + " FROM " + table).fetchObject(Instant.class).orElseThrow(), zone + " " + column);
							for (String column : List.of("plain_array", "zoned_array")) {
								assertArrayEquals(new Instant[]{expected, null}, database.query("SELECT " + column + " FROM " + table).fetchObject(Instant[].class).orElseThrow());
								assertArrayEquals(new Timestamp[]{Timestamp.from(expected), null}, database.query("SELECT " + column + " FROM " + table).fetchObject(Timestamp[].class).orElseThrow());
							}
						}
					}
					// Explicit null/empty arrays and ordinary local values keep their existing meaning.
					assertTrue(database.query("SELECT :values IS NULL").bind("values", Parameters.sqlArrayOf("timestamp", (Object[]) null)).fetchObject(Boolean.class).orElseThrow());
					assertArrayEquals(new Instant[0], database.query("SELECT :values").bind("values", Parameters.sqlArrayOf("timestamp", new Object[0])).fetchObject(Instant[].class).orElseThrow());
					LocalDateTime wall = LocalDateTime.parse("2024-01-02T12:34:56.123456");
					assertEquals(wall, database.query("SELECT (:values)[1]").bind("values", Parameters.sqlArrayOf("\"pg_catalog\".\"timestamp\"", new Object[]{wall})).fetchObject(LocalDateTime.class).orElseThrow());
				} finally {
					database.query("DROP TABLE " + table).execute();
				}
			}
		}
	}

	@Test
	void timestampReadModifyWritePreservesInstantAcrossScalarRecordAndBeanMappings() {
		for (boolean cached : new boolean[]{false, true}) {
			for (ZoneId zone : List.of(ZoneId.of("UTC"), ZoneId.of("America/New_York"), ZoneId.of("Asia/Kathmandu"))) {
				Database database = database(zone, cached);
				TemporalTimestampIntegrationSupport.verify(database);
			}
		}
	}

	private static Database database(ZoneId zone, boolean cached) {
		PGSimpleDataSource source = new PGSimpleDataSource() {
			@Override public Connection getConnection() throws SQLException {
				Connection connection = super.getConnection();
				try (java.sql.Statement statement = connection.createStatement()) {
					statement.execute("SET TIME ZONE 'Pacific/Honolulu'");
				} catch (SQLException failure) { connection.close(); throw failure; }
				return connection;
			}
		};
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return Database.with(source, DatabaseType.POSTGRESQL).timeZone(zone)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
