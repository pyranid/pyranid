/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class YearMonthZoneOffsetMappingIT {
	@TempDir Path directory;
	public record Row(YearMonth monthValue, ZoneOffset offsetValue) {}
	public record Months(List<YearMonth> value) {}
	public record MonthMap(Map<YearMonth, YearMonth> value) {}
	public record Offsets(List<ZoneOffset> value) {}
	public record Payload(Row payload) {}

	@Test
	void sqliteStoredYearMonthsAndZoneOffsetsRoundTripAcrossScalarAndRecordTargets() { verify(DatabaseType.SQLITE); }

	@Test
	void duckDbStoredYearMonthsAndZoneOffsetsRoundTripAcrossScalarAndRecordTargets() { verify(DatabaseType.DUCK_DB); }

	@Test
	void duckDbListsMapsAndStructsReuseTheSameYearMonthAndFixedOffsetRules() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(DatabaseType.DUCK_DB, "nested_" + cached, cached);
			assertEquals(Arrays.asList(YearMonth.of(10000, 2), null, YearMonth.of(-10000, 12)),
					database.query("SELECT ['10000-02', NULL, '-10000-12'] AS value").fetchObject(Months.class).orElseThrow().value());
			assertEquals(Map.of(YearMonth.of(10000, 2), YearMonth.of(999999999, 12)),
					database.query("SELECT MAP(['10000-02'], ['999999999-12']) AS value").fetchObject(MonthMap.class).orElseThrow().value());
			assertEquals(Arrays.asList(ZoneOffset.UTC, null, ZoneOffset.ofHoursMinutes(5, 45)),
					database.query("SELECT ['UTC', NULL, '+05:45'] AS value").fetchObject(Offsets.class).orElseThrow().value());
			Row expected = new Row(YearMonth.of(10000, 2), ZoneOffset.ofHoursMinutes(5, 45));
			assertEquals(expected, database.query("SELECT {'month_value': '10000-02', 'offset_value': 'UTC+05:45'} AS payload")
					.fetchObject(Row.class).orElseThrow());
			assertEquals(new Payload(expected), database.query("SELECT {'month_value': '10000-02', 'offset_value': 'UTC+05:45'} AS payload")
					.fetchObject(Payload.class).orElseThrow());
			assertThrows(DatabaseException.class, () -> database.query("SELECT ['Europe/Paris'] AS value").fetchObject(Offsets.class));
		}
	}

	private void verify(DatabaseType type) {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(type, "stored_" + cached, cached);
			database.query("CREATE TABLE temporal_values (id INT PRIMARY KEY, month_value VARCHAR(32), offset_value VARCHAR(64))").execute();
			int id = 0;
			for (int year : new int[]{-999999999, -10000, -1, 0, 9999, 10000, 999999999}) {
				YearMonth month = YearMonth.of(year, 2);
				ZoneOffset offset = id % 2 == 0 ? ZoneOffset.UTC : ZoneOffset.ofHoursMinutesSeconds(-5, -30, -45);
				database.query("INSERT INTO temporal_values VALUES (:id, :month, :offset)")
						.bind("id", id).bind("month", month).bind("offset", offset).execute();
				assertEquals(month.toString(), database.query("SELECT month_value FROM temporal_values WHERE id = :id")
						.bind("id", id).fetchObject(Object.class).orElseThrow());
				assertEquals(month, database.query("SELECT month_value FROM temporal_values WHERE id = :id")
						.bind("id", id).fetchObject(YearMonth.class).orElseThrow());
				assertEquals(offset, database.query("SELECT offset_value FROM temporal_values WHERE id = :id")
						.bind("id", id).fetchObject(ZoneOffset.class).orElseThrow());
				assertEquals(new Row(month, offset), database.query("SELECT month_value, offset_value FROM temporal_values WHERE id = :id")
						.bind("id", id).fetchObject(Row.class).orElseThrow());
				id++;
			}
			database.query("INSERT INTO temporal_values VALUES (100, NULL, NULL)").execute();
			assertTrue(database.query("SELECT month_value FROM temporal_values WHERE id = 100").fetchObject(YearMonth.class).isEmpty());
			assertTrue(database.query("SELECT offset_value FROM temporal_values WHERE id = 100").fetchObject(ZoneOffset.class).isEmpty());
			assertEquals(new Row(null, null), database.query("SELECT month_value, offset_value FROM temporal_values WHERE id = 100")
					.fetchObject(Row.class).orElseThrow());
			for (String value : List.of("UTC", "GMT", "UTC+05:45", "Etc/GMT+5")) {
				ZoneOffset expected = switch (value) {
					case "UTC", "GMT" -> ZoneOffset.UTC;
					case "UTC+05:45" -> ZoneOffset.ofHoursMinutes(5, 45);
					default -> ZoneOffset.ofHours(-5);
				};
				assertEquals(expected, database.query("SELECT CAST(:value AS VARCHAR(64))")
						.bind("value", value).fetchObject(ZoneOffset.class).orElseThrow());
			}
			assertEquals(ZoneId.of("Europe/Paris"), database.query("SELECT CAST(:value AS VARCHAR(64))")
					.bind("value", "Europe/Paris").fetchObject(ZoneId.class).orElseThrow());
			assertThrows(DatabaseException.class, () -> database.query("SELECT CAST(:value AS VARCHAR(64))")
					.bind("value", "Europe/Paris").fetchObject(ZoneOffset.class));
			assertThrows(DatabaseException.class, () -> database.query("SELECT CAST(:value AS VARCHAR(64))")
					.bind("value", "1000000000-02").fetchObject(YearMonth.class));
		}
	}

	private Database database(DatabaseType type, String name, boolean cached) {
		String url = (type == DatabaseType.SQLITE ? "jdbc:sqlite:" : "jdbc:duckdb:") + directory.resolve(type.name() + name + ".db");
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(url, null, null), type)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
