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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class TemporalZoneResolutionIT {
	@Container private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("temporal_zone_resolution").withUsername("pyranid").withPassword("pyranid");
	@TempDir Path directory;
	public record DateRow(OffsetDateTime value) {}
	public record TimeRow(OffsetTime value) {}
	public record TimeList(List<OffsetTime> value) {}
	public static class DateBean {
		private OffsetDateTime value;
		public void setValue(OffsetDateTime value) { this.value = value; }
		public OffsetDateTime getValue() { return this.value; }
	}
	public static class TimeBean {
		private OffsetTime value;
		public void setValue(OffsetTime value) { this.value = value; }
		public OffsetTime getValue() { return this.value; }
	}

	@Test
	void postgresTextTimesAndZonedValuesUseTheDeclaredInformation() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		verify(source, DatabaseType.POSTGRESQL);
	}

	@Test
	void duckDbTextTimesAndZonedValuesUseTheDeclaredInformation() {
		verify(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:duckdb:" + directory.resolve("zones.duckdb"), null, null),
				DatabaseType.DUCK_DB);
	}

	@Test
	void sqliteTextTimesAndExplicitOffsetsUseTheDeclaredInformation() {
		verify(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:sqlite:" + directory.resolve("zones.db"), null, null),
				DatabaseType.SQLITE);
	}

	private static void verify(DataSource source, DatabaseType type) {
		for (boolean cached : new boolean[]{false, true}) {
			verifyText(source, type, cached);
			verifyTimes(source, type, cached);
		}
	}

	private static void verifyText(DataSource source, DatabaseType type, boolean cached) {
		Database setup = database(source, type, cached, ZoneOffset.UTC);
		String table = "zone_text_" + UUID.randomUUID().toString().replace("-", "");
		setup.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, value VARCHAR(80))").execute();
		try {
			int id = 0;
			for (String[] scenario : List.of(new String[]{"America/New_York", "2024-03-10T02:30:00.123456"},
					new String[]{"America/New_York", "2024-11-03T01:30:00.123456"},
					new String[]{"Australia/Lord_Howe", "2024-10-06T02:15:00.123456"},
					new String[]{"Pacific/Apia", "2011-12-30T12:34:56.123456"})) {
				ZoneId zone = ZoneId.of(scenario[0]);
				Database database = database(source, type, cached, zone);
				LocalDateTime local = LocalDateTime.parse(scenario[1]);
				OffsetDateTime expected = local.atZone(zone).toOffsetDateTime();
				for (String text : List.of(local.toString(), local.toString().replace('T', ' '))) {
					setup.query("INSERT INTO " + table + " VALUES (:id, :value)").bind("id", ++id).bind("value", text).execute();
					String sql = "SELECT value FROM " + table + " WHERE id=:id";
					for (int pass = 0; pass < 2; pass++) {
						assertEquals(expected, database.query(sql).bind("id", id).fetchObject(OffsetDateTime.class).orElseThrow());
						assertEquals(expected, database.query(sql).bind("id", id).fetchObject(DateRow.class).orElseThrow().value());
						assertEquals(expected, database.query(sql).bind("id", id).fetchObject(DateBean.class).orElseThrow().getValue());
						assertEquals(expected.toInstant(), database.query(sql).bind("id", id).fetchObject(Instant.class).orElseThrow());
						assertEquals(text, database.query(sql).bind("id", id).fetchObject(Object.class).orElseThrow());
					}
				}
			}
			Database regional = database(source, type, cached, ZoneId.of("America/New_York"));
			OffsetDateTime explicit = OffsetDateTime.parse("2024-03-10T02:30:00.123456+05:45");
			for (String text : List.of(explicit.toString(), explicit.toString().replace('T', ' '))) {
				setup.query("INSERT INTO " + table + " VALUES (:id, :value)").bind("id", ++id).bind("value", text).execute();
				String sql = "SELECT value FROM " + table + " WHERE id=:id";
				assertEquals(explicit, regional.query(sql).bind("id", id).fetchObject(OffsetDateTime.class).orElseThrow());
				assertEquals(explicit, regional.query(sql).bind("id", id).fetchObject(DateRow.class).orElseThrow().value());
				assertEquals(explicit, regional.query(sql).bind("id", id).fetchObject(DateBean.class).orElseThrow().getValue());
				assertEquals(explicit.toInstant(), regional.query(sql).bind("id", id).fetchObject(Instant.class).orElseThrow());
			}
			for (String text : List.of("2024-03-10T02:30:00+25:00", "2024-03-10 02:30:00+05:99", "2024-03-10T02:30:00Zjunk")) {
				int invalidId = ++id;
				setup.query("INSERT INTO " + table + " VALUES (:id, :value)").bind("id", invalidId).bind("value", text).execute();
				String sql = "SELECT value FROM " + table + " WHERE id=:id";
				assertThrows(DatabaseException.class, () -> regional.query(sql).bind("id", invalidId).fetchObject(OffsetDateTime.class));
				assertThrows(DatabaseException.class, () -> regional.query(sql).bind("id", invalidId).fetchObject(DateRow.class));
			}
		} finally { setup.query("DROP TABLE " + table).execute(); }
	}

	private static void verifyTimes(DataSource source, DatabaseType type, boolean cached) {
		ZoneOffset fixed = ZoneOffset.ofHoursMinutes(5, 45);
		Database database = database(source, type, cached, fixed);
		String table = "zone_time_" + UUID.randomUUID().toString().replace("-", "");
		String zonedTable = table + "_z";
		boolean sqlTime = type != DatabaseType.SQLITE;
		database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, value " + (sqlTime ? "TIME" : "TEXT") + ")").execute();
		database.query("CREATE TABLE " + zonedTable + " (id INT PRIMARY KEY, value " + (sqlTime ? "TIME WITH TIME ZONE" : "TEXT") + ")").execute();
		try {
			LocalTime local = LocalTime.parse("12:34:56.123456");
			OffsetTime expected = local.atOffset(fixed);
			database.query("INSERT INTO " + table + " VALUES (:id, :value)").bind("id", 1).bind("value", sqlTime ? local : local.toString()).execute();
			database.query("INSERT INTO " + table + " VALUES (:id, :value)").bind("id", 2).bind("value", null).execute();
			String sql = "SELECT value FROM " + table + " WHERE id=1";
			assertEquals(expected, database.query(sql).fetchObject(OffsetTime.class).orElseThrow());
			assertEquals(expected, database.query(sql).fetchObject(TimeRow.class).orElseThrow().value());
			assertEquals(expected, database.query(sql).fetchObject(TimeBean.class).orElseThrow().getValue());
			assertTrue(database.query("SELECT value FROM " + table + " WHERE id=2").fetchObject(OffsetTime.class).isEmpty());
			String arraySql = "SELECT " + (type == DatabaseType.POSTGRESQL ? "ARRAY[value, NULL]" : "[value, NULL]") + " AS value FROM " + table + " WHERE id=1";
			if (sqlTime)
				assertEquals(Arrays.asList(expected, null), database.query(arraySql).fetchObject(TimeList.class).orElseThrow().value());
			OffsetTime explicit = OffsetTime.parse("23:59:59.123456-11:30");
			database.query("INSERT INTO " + zonedTable + " VALUES (:id, :value)").bind("id", 1)
					.bind("value", sqlTime ? explicit : explicit.toString()).execute();
			for (String region : List.of("Europe/London", "Asia/Singapore")) {
				Database regional = database(source, type, cached, ZoneId.of(region));
				for (Class<?> target : List.of(OffsetTime.class, TimeRow.class, TimeBean.class)) {
					DatabaseException failure = assertThrows(DatabaseException.class, () -> regional.query(sql).fetchObject(target));
					assertTrue(failure.getMessage().contains("date-less"), failure.getMessage());
				}
				if (sqlTime)
					assertThrows(DatabaseException.class, () -> regional.query(arraySql).fetchObject(TimeList.class));
				assertEquals(local, regional.query(sql).fetchObject(LocalTime.class).orElseThrow());
				assertTrue(regional.query("SELECT value FROM " + table + " WHERE id=2").fetchObject(OffsetTime.class).isEmpty());
				String explicitSql = "SELECT value FROM " + zonedTable + " WHERE id=1";
				assertEquals(explicit, regional.query(explicitSql).fetchObject(OffsetTime.class).orElseThrow());
				assertEquals(explicit, regional.query(explicitSql).fetchObject(TimeRow.class).orElseThrow().value());
				assertEquals(explicit, regional.query(explicitSql).fetchObject(TimeBean.class).orElseThrow().getValue());
			}
		} finally {
			database.query("DROP TABLE " + zonedTable).execute();
			database.query("DROP TABLE " + table).execute();
		}
	}

	private static Database database(DataSource source, DatabaseType type, boolean cached, ZoneId zone) {
		return Database.with(source, type).timeZone(zone).resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
