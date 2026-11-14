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

import javax.sql.DataSource;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class OracleTemporalCorrectnessIT {
	@Container
	private static final OracleContainer ORACLE = new OracleContainer(DockerImageName.parse(
			System.getProperty("oracle.integration.image", "gvenzl/oracle-free:23-slim-faststart"))
			.asCompatibleSubstituteFor("gvenzl/oracle-free")).withUsername("pyranid").withPassword("pyranid");

	@Test
	void nativeLocalTimeKeepsNanosecondsAndWallClockThroughTimestampColumnsAndPredicates() {
		Database database = database();
		String table = "time_correctness_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, time_value TIMESTAMP(9), writes NUMBER(10) NOT NULL)").execute();
		try {
			int id = 0;
			for (LocalTime value : List.of(LocalTime.MIDNIGHT, LocalTime.of(10, 11, 12, 123456789),
					LocalTime.of(23, 59, 59, 998500000), LocalTime.MAX)) {
				id++;
				database.query("INSERT INTO " + table + " VALUES (:id, :value, 0)").bind("id", id).bind("value", value).execute();
				assertEquals(value, LocalTime.parse(database.query("SELECT TO_CHAR(time_value, 'HH24:MI:SS.FF9') FROM " + table + " WHERE id = :id")
						.bind("id", id).fetchObject(String.class).orElseThrow()));
				assertEquals(value, database.query("SELECT time_value FROM " + table + " WHERE id = :id")
						.bind("id", id).fetchObject(LocalTime.class).orElseThrow());
				assertEquals(1L, database.query("SELECT COUNT(*) FROM " + table + " WHERE id = :id AND time_value = :value")
						.bind("id", id).bind("value", value).fetchObject(Long.class).orElseThrow());
				assertEquals(1L, database.query("UPDATE " + table + " SET writes = writes + 1 WHERE id = :id AND time_value = :value")
						.bind("id", id).bind("value", value).execute());
			}
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	@Test
	void nativeLocalTimeIntoOracleDateRetainsItsClockAtTheColumnsSecondPrecision() {
		Database database = database();
		String table = "date_time_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (time_value DATE)").execute();
		try {
			LocalTime value = LocalTime.of(10, 11, 12, 123456789);
			database.query("INSERT INTO " + table + " VALUES (:value)").bind("value", value).execute();
			assertEquals("10:11:12", database.query("SELECT TO_CHAR(time_value, 'HH24:MI:SS') FROM " + table).fetchObject(String.class).orElseThrow());
			assertEquals(value.withNano(0), database.query("SELECT time_value FROM " + table).fetchObject(LocalTime.class).orElseThrow());
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	private static Database database() {
		DataSource source = new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
				ORACLE.getJdbcUrl(), ORACLE.getUsername(), ORACLE.getPassword());
		return Database.with(source, DatabaseType.ORACLE).timeZone(ZoneId.of("Asia/Tokyo")).build();
	}
}
