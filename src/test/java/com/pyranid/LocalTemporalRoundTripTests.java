/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ResourceLock("java.util.TimeZone.default")
class LocalTemporalRoundTripTests {
	@Test
	void localValuesUseDatabaseZoneForZonedTargetsAndKeepTheirComparisonIdentity() {
		TimeZone previous = TimeZone.getDefault();
		try {
			for (String jvmZone : List.of("UTC", "America/New_York")) {
				TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));
				ZoneId zone = ZoneId.of("Asia/Kathmandu");
				Database database = database(zone);
				database.query("CREATE TABLE work (id INT PRIMARY KEY, value TIMESTAMP(9) WITH TIME ZONE)").execute();
				int id = 0;
				for (Object value : List.of(LocalDate.parse("2024-01-02"), LocalDateTime.parse("2024-07-02T12:34:56.123456789"))) {
					LocalDateTime local = value instanceof LocalDate date ? date.atStartOfDay() : (LocalDateTime) value;
					database.query("INSERT INTO work VALUES (:id, :value)").bind("id", ++id).bind("value", value).execute();
					assertEquals(local.atZone(zone).toInstant(), database.query("SELECT value FROM work WHERE id=:id")
							.bind("id", id).fetchObject(java.time.Instant.class).orElseThrow());
					assertEquals(local, database.query("SELECT value FROM work WHERE id=:id")
							.bind("id", id).fetchObject(LocalDateTime.class).orElseThrow());
					assertEquals(1L, database.query("SELECT COUNT(*) FROM work WHERE value=:value").bind("value", value)
							.fetchObject(Long.class).orElseThrow());
					assertEquals(1L, database.query("UPDATE work SET value=:value WHERE id=:id AND value=:value")
							.bind("id", id).bind("value", value).execute());
				}
			}
		} finally { TimeZone.setDefault(previous); }
	}

	@Test
	void legacyTimeFractionsSurviveStorageAndEqualityPredicates() {
		TimeZone previous = TimeZone.getDefault();
		try {
			for (String jvmZone : List.of("UTC", "America/New_York")) {
				TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));
				Database database = database(ZoneId.of("Asia/Kathmandu"));
				database.query("CREATE TABLE work (id INT PRIMARY KEY, value TIME(9))").execute();
				int id = 0;
				for (long epoch : new long[]{1, 123, 999, -1}) {
					java.sql.Time value = new java.sql.Time(epoch);
					LocalTime expected = value.toLocalTime().withNano((int) Math.floorMod(epoch, 1000L) * 1_000_000);
					database.query("INSERT INTO work VALUES (:id, :value)").bind("id", ++id).bind("value", value).execute();
					assertEquals(expected, database.query("SELECT value FROM work WHERE id=:id").bind("id", id)
							.fetchObject(LocalTime.class).orElseThrow());
					assertEquals(1L, database.query("SELECT COUNT(*) FROM work WHERE id=:id AND value=:value")
							.bind("id", id).bind("value", value).fetchObject(Long.class).orElseThrow());
				}
			}
		} finally { TimeZone.setDefault(previous); }
	}

	private static Database database(ZoneId zone) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:local_temporal_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).timeZone(zone).build();
	}
}
