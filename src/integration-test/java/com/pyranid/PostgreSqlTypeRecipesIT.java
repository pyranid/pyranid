/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Runs the public-API enum and session-zone recipes against PostgreSQL. */
@Testcontainers
@ResourceLock("java.util.TimeZone.default")
class PostgreSqlTypeRecipesIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("type_recipes").withUsername("pyranid").withPassword("pyranid");

	public enum Status {
		QUEUED { @Override public String toString() { return "Waiting for execution"; } }, RUNNING, DONE
	}
	public record EnumRow(int id, Status status) {}
	public record TemporalRow(LocalDateTime localValue, Instant instantValue) {}

	@Test
	void qualifiedEnumCastSupportsValuesNullsPredicatesUpdatesAndBatches() {
		Database database = Database.with(source(), DatabaseType.POSTGRESQL).build();
		String suffix = UUID.randomUUID().toString().replace("-", "");
		String enumType = "public.recipe_status_" + suffix;
		String table = "recipe_enum_" + suffix;
		database.query("CREATE TYPE " + enumType + " AS ENUM ('QUEUED', 'RUNNING', 'DONE')").execute();
		try {
			database.query("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, status " + enumType + ")").execute();
			String insert = "INSERT INTO " + table + " VALUES (:id, CAST(:status AS " + enumType + "))";
			assertEquals(1L, database.query(insert).bind("id", 1).bind("status", Status.QUEUED).execute());
			assertEquals(Status.QUEUED, database.query("SELECT status FROM " + table + " WHERE id=1").fetchObject(Status.class).orElseThrow());
			assertEquals("QUEUED", database.query("SELECT status::text FROM " + table + " WHERE id=1").fetchObject(String.class).orElseThrow());
			assertNotEquals(Status.QUEUED.name(), Status.QUEUED.toString());
			assertEquals(List.of(1L, 1L, 1L), database.query(insert).executeBatch(List.of(row(2, Status.RUNNING), row(3, Status.DONE), row(4, null))));
			assertEquals(List.of(new EnumRow(1, Status.QUEUED), new EnumRow(2, Status.RUNNING), new EnumRow(3, Status.DONE), new EnumRow(4, null)),
					database.query("SELECT id, status FROM " + table + " ORDER BY id").fetchList(EnumRow.class));
			assertEquals(List.of(1), database.query("SELECT id FROM " + table + " WHERE status=CAST(:status AS " + enumType + ")")
					.bind("status", Status.QUEUED).fetchList(Integer.class));
			assertEquals(1L, database.query("UPDATE " + table + " SET status=CAST(:status AS " + enumType + ") WHERE id=:id")
					.bind("status", Status.DONE).bind("id", 1).execute());
			assertEquals(Status.DONE, database.query("SELECT status FROM " + table + " WHERE id=1").fetchObject(Status.class).orElseThrow());
			assertEquals(1L, database.query("UPDATE " + table + " SET status=CAST(:status AS " + enumType + ") WHERE id=:id")
					.bind("status", null).bind("id", 2).execute());
			assertEquals(List.of(2, 4), database.query("SELECT id FROM " + table + " WHERE status IS NULL ORDER BY id").fetchList(Integer.class));
			DatabaseException uncast = assertThrows(DatabaseException.class, () -> database.query("INSERT INTO " + table + " VALUES (5, :status)")
					.bind("status", Status.QUEUED).execute());
			assertEquals("42804", uncast.getSqlState().orElseThrow(), "Pyranid's normal enum name is a VARCHAR, not a native enum parameter");
			assertEquals(4L, database.query("SELECT COUNT(*) FROM " + table).fetchObject(Long.class).orElseThrow());
		} finally {
			database.query("DROP TABLE IF EXISTS " + table).execute();
			database.query("DROP TYPE " + enumType).execute();
		}
	}

	@Test
	void configuredServerAndDatabaseUtcZonesRoundTripLocalAndInstantValuesUnderANewYorkJvm() throws Exception {
		TimeZone previous = TimeZone.getDefault();
		try {
			TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
			PGSimpleDataSource optionsOnly = source();
			optionsOnly.setOptions("-c TimeZone=UTC");
			assertEquals("America/New_York", Database.with(optionsOnly, DatabaseType.POSTGRESQL).build()
					.query("SHOW TimeZone").fetchObject(String.class).orElseThrow(),
					"pgjdbc sends its own JVM-derived startup TimeZone, overriding this options-only attempt");
			HikariConfig config = new HikariConfig();
			config.setDataSource(source());
			config.setMaximumPoolSize(2);
			config.setMinimumIdle(0);
			config.setConnectionInitSql("SET TIME ZONE 'UTC'");
			try (HikariDataSource pool = new HikariDataSource(config)) {
				// Hold both checkouts at once to prove initialization applies to distinct physical sessions.
				try (Connection first = pool.getConnection(); Connection second = pool.getConnection()) {
					assertEquals("UTC", string(first, "SHOW TimeZone"));
					assertEquals("UTC", string(second, "SHOW TimeZone"));
					assertNotEquals(string(first, "SELECT pg_backend_pid()"), string(second, "SELECT pg_backend_pid()"));
				}
				Database database = Database.with(pool, DatabaseType.POSTGRESQL).timeZone(ZoneOffset.UTC).build();
				assertEquals("UTC", database.query("SHOW TimeZone").fetchObject(String.class).orElseThrow());
				String table = "recipe_zone_" + UUID.randomUUID().toString().replace("-", "");
				database.query("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, local_value TIMESTAMP, instant_value TIMESTAMPTZ)").execute();
				try {
					LocalDateTime local = LocalDateTime.parse("2024-07-01T12:34:56.123456");
					Instant instant = Instant.parse("2024-07-01T12:34:56.123456Z");
					database.query("INSERT INTO " + table + " VALUES (1, :local, :instant)").bind("local", local).bind("instant", instant).execute();
					// In a matching UTC session the two SQL types can be compared or cast without an extra shift.
					assertEquals(true, database.query("SELECT local_value=instant_value::timestamp FROM " + table).fetchObject(Boolean.class).orElseThrow());
					assertEquals(new TemporalRow(local, instant), database.query("SELECT local_value, instant_value FROM " + table).fetchObject(TemporalRow.class).orElseThrow());
					assertEquals(local, database.query("SELECT instant_value::timestamp FROM " + table).fetchObject(LocalDateTime.class).orElseThrow());
					assertEquals(instant, database.query("SELECT local_value::timestamptz FROM " + table).fetchObject(Instant.class).orElseThrow());
					assertEquals(1L, database.query("UPDATE " + table + " SET local_value=:local WHERE instant_value=:instant")
							.bind("local", local).bind("instant", instant).execute());
				} finally { database.query("DROP TABLE " + table).execute(); }
			}
		} finally { TimeZone.setDefault(previous); }
	}

	private static String string(Connection connection, String sql) throws SQLException {
		try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
			assertTrue(rows.next()); return rows.getString(1);
		}
	}

	private static Map<String, Object> row(int id, Status status) {
		Map<String, Object> values = new HashMap<>(); values.put("id", id); values.put("status", status); return values;
	}
	private static PGSimpleDataSource source() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword()); return source;
	}
}
