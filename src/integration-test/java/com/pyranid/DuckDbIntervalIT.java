/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.List;
import java.util.UUID;

import static com.pyranid.SqlIntervalIntegrationSupport.*;
import static org.junit.jupiter.api.Assertions.*;

class DuckDbIntervalIT {
	@TempDir Path directory;
	public record Fields(long months, long days, BigInteger micros) {}

	@Test
	void mixedSignedFieldsAndBoundariesRoundTripThroughBatchesAndTypedRows() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached); List<SqlInterval> values = values();
			database.query("CREATE TABLE intervals (id INT PRIMARY KEY, amount INTERVAL)").execute();
			database.transaction(() -> database.query("INSERT INTO intervals VALUES (:id, :amount)").batchChunkSize(2).executeBatch(groups(values)));
			verifyRows(database, "intervals", values);
			List<Fields> fields = database.query("SELECT date_part('year', amount) * 12 + date_part('month', amount) AS months,"
					+ " date_part('day', amount) AS days, CAST(date_part('hour', amount) AS HUGEINT) * 3600000000"
					+ " + CAST(date_part('minute', amount) AS HUGEINT) * 60000000 + date_part('microsecond', amount) AS micros"
					+ " FROM intervals WHERE amount IS NOT NULL ORDER BY id").fetchList(Fields.class);
			assertEquals(values.size(), fields.size());
			for (int index = 0; index < values.size(); index++) assertEquals(new Fields(values.get(index).getMonths(), values.get(index).getDays(),
					BigInteger.valueOf(micros(values.get(index).getTime()))), fields.get(index));
		}
	}

	@Test
	void castsMakeOperatorTargetsExplicitAndEqualityDoesNotDefineFieldIdentity() {
		Database database = database(true); SqlInterval day = SqlInterval.of(Period.ofDays(1));
		assertEquals(LocalDateTime.of(2026, 1, 2, 0, 0), database.query("SELECT DATE '2026-01-01' + CAST(:amount AS INTERVAL)")
				.bind("amount", day).fetchObject(LocalDateTime.class).orElseThrow());
		assertEquals(LocalDateTime.of(2025, 12, 31, 23, 0), database.query("SELECT TIMESTAMP '2026-01-01 00:00:00' - CAST(:amount AS INTERVAL)")
				.bind("amount", SqlInterval.of(Duration.ofHours(1))).fetchObject(LocalDateTime.class).orElseThrow());
		assertThrows(DatabaseException.class, () -> database.query("SELECT DATE '2026-01-01' + :amount")
				.bind("amount", day).fetchObject(LocalDateTime.class));
		assertTrue(database.query("SELECT INTERVAL '1 day' = INTERVAL '24 hours'").fetchObject(Boolean.class).orElseThrow());
		assertNotEquals(database.query("SELECT INTERVAL '1 day'").fetchObject(SqlInterval.class).orElseThrow(),
				database.query("SELECT INTERVAL '24 hours'").fetchObject(SqlInterval.class).orElseThrow());
		assertThrows(DatabaseException.class, () -> database.query("SELECT CAST(:amount AS INTERVAL)")
				.bind("amount", SqlInterval.of(Duration.ofNanos(1))).fetchObject(SqlInterval.class));
		assertThrows(DatabaseException.class, () -> database.query("SELECT 'P1D'").fetchObject(SqlInterval.class));
	}

	@Test
	void calendarSubtractionIsNotSilentlyConvertedToElapsedDuration() {
		Database database = database(false);
		database.transaction(() -> {
			database.query("SET TimeZone = 'America/New_York'").execute();
			SqlInterval difference = database.query("SELECT TIMESTAMPTZ '2026-03-10 12:00' - TIMESTAMPTZ '2026-03-07 09:00'")
					.fetchObject(SqlInterval.class).orElseThrow();
			assertEquals(SqlInterval.of(0, 3, Duration.ofHours(3)), difference);
			assertThrows(DateTimeException.class, difference::toDuration);
		});
	}

	private Database database(boolean cached) {
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
				"jdbc:duckdb:" + this.directory.resolve("interval_" + UUID.randomUUID() + ".duckdb"), null, null), DatabaseType.DUCK_DB)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
