/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class SqlServerTemporalCorrectnessIT {
	@Container
	private static final MSSQLServerContainer SQL_SERVER = new MSSQLServerContainer(DockerImageName.parse(
			System.getProperty("sqlserver.integration.image", "mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04"))
			.asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server")).acceptLicense();

	@Test
	void timeParametersRetainPrecisionAndWorkInPredicatesWithDefaultAndExplicitDriverModes() {
		for (String settings : List.of("", ";sendTimeAsDatetime=true", ";sendTimeAsDatetime=false")) {
			Database database = database(settings);
			String table = "time_correctness_" + UUID.randomUUID().toString().replace("-", "");
			database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, time_value TIME(7), writes INT NOT NULL)").execute();
			try {
				int id = 0;
				for (LocalTime value : List.of(LocalTime.MIDNIGHT, LocalTime.of(10, 11, 12, 123456700),
						LocalTime.of(23, 59, 59, 998500000), LocalTime.MAX)) {
					id++;
					database.query("INSERT INTO " + table + " VALUES (:id, :value, 0)").bind("id", id).bind("value", value).execute();
					LocalTime expected = value.withNano(value.getNano() / 100 * 100); // SQL Server TIME(7), 100 ns precision.
					assertEquals(expected, database.query("SELECT time_value FROM " + table + " WHERE id = :id")
							.bind("id", id).fetchObject(LocalTime.class).orElseThrow(), settings + " " + value);
					assertEquals(expected, LocalTime.parse(database.query("SELECT CONVERT(VARCHAR(20), time_value) FROM " + table + " WHERE id = :id")
							.bind("id", id).fetchObject(String.class).orElseThrow()));
					assertEquals(1L, database.query("SELECT COUNT_BIG(*) FROM " + table + " WHERE id = :id AND time_value = :value")
							.bind("id", id).bind("value", value).fetchObject(Long.class).orElseThrow());
					assertEquals(1L, database.query("UPDATE " + table + " SET writes = writes + 1 WHERE id = :id AND time_value = :value")
							.bind("id", id).bind("value", value).execute());
				}
			} finally {
				database.query("DROP TABLE " + table).execute();
			}
		}
	}

	@Test
	void legacyDatetimeEqualityUsesTheDocumentedDatetimeParameterTypeSetting() {
		Database defaults = database("");
		Database legacy = database(";datetimeParameterType=datetime");
		String table = "legacy_datetime_" + UUID.randomUUID().toString().replace("-", "");
		defaults.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, occurred_at DATETIME NOT NULL, writes INT NOT NULL)").execute();
		try {
			LocalDateTime written = LocalDateTime.of(2024, 3, 10, 12, 13, 14, 123000000);
			defaults.query("INSERT INTO " + table + " VALUES (1, :value, 0)").bind("value", written).execute();
			LocalDateTime read = legacy.query("SELECT occurred_at FROM " + table).fetchObject(LocalDateTime.class).orElseThrow();
			assertEquals(written, read);
			// DATETIME's 1/300 s storage differs from the default DATETIME2 parameter's .123 value.
			assertEquals(0L, defaults.query("SELECT COUNT_BIG(*) FROM " + table + " WHERE occurred_at = :value")
					.bind("value", read).fetchObject(Long.class).orElseThrow());
			assertEquals(1L, legacy.query("SELECT COUNT_BIG(*) FROM " + table + " WHERE occurred_at = :value")
					.bind("value", read).fetchObject(Long.class).orElseThrow());
			assertEquals(1L, legacy.query("UPDATE " + table + " SET writes = writes + 1 WHERE occurred_at = :oldValue")
					.bind("oldValue", read).execute());
			assertEquals(1, legacy.query("SELECT writes FROM " + table).fetchObject(Integer.class).orElseThrow());
		} finally {
			defaults.query("DROP TABLE " + table).execute();
		}
	}

	private static Database database(String settings) {
		DataSource source = new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
				SQL_SERVER.getJdbcUrl() + settings, SQL_SERVER.getUsername(), SQL_SERVER.getPassword());
		return Database.with(source, DatabaseType.SQL_SERVER).timeZone(ZoneId.of("Asia/Tokyo")).build();
	}
}
