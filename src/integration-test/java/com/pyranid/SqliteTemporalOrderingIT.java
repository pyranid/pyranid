/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

@ResourceLock("java.util.TimeZone.default")
class SqliteTemporalOrderingIT {
	@TempDir Path directory;

	@Test
	void instantTextSortsAndFiltersChronologicallyAcrossOffsetsAndFractionWidths() {
		TimeZone previous = TimeZone.getDefault();
		try {
			TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
			for (String affinity : List.of("TEXT", "TIMESTAMP", "DATETIME"))
				for (boolean idFirst : new boolean[]{false, true}) assertInstantOrdering(affinity, idFirst);
		} finally {
			TimeZone.setDefault(previous);
		}
	}

	private void assertInstantOrdering(String affinity, boolean idFirst) {
		Database database = database("ordered_" + affinity + "_" + idFirst, AmbiguousTimestampBindingStrategy.TIMESTAMP_WITH_TIME_ZONE);
		database.query("CREATE TABLE work (id INT PRIMARY KEY, value " + affinity + ")").execute();
		Instant first = Instant.parse("2024-07-01T12:34:00Z");
		List<Object> values = List.of(first, first.plusNanos(1), first.plusMillis(500), first.plusSeconds(1),
				first.atOffset(ZoneOffset.ofHours(8)), Timestamp.from(first.plusNanos(999_999_999)),
				Date.from(first.plusMillis(123)), first.plusMillis(250).atZone(ZoneId.of("Europe/Paris")));
		for (int id = values.size(); id > 0; id--)
			database.query(idFirst ? "INSERT INTO work (id, value) VALUES (:id, :v)" : "INSERT INTO work (value, id) VALUES (:v, :id)").bind("id", id).bind("v", values.get(id - 1)).execute();
		assertEquals(List.of(1, 5, 2, 7, 8, 3, 6, 4), database.query("SELECT id FROM work ORDER BY value, id").fetchList(Integer.class));
		assertEquals(List.of(1, 2, 3, 5, 6, 7, 8), database.query("SELECT id FROM work WHERE value >= :lower AND value < :upper ORDER BY id")
				.bind("lower", first).bind("upper", first.plusSeconds(1)).fetchList(Integer.class));
		assertEquals(List.of(1, 5), database.query("SELECT id FROM work WHERE value = :v ORDER BY id")
				.bind("v", first.atOffset(ZoneOffset.ofHours(-4))).fetchList(Integer.class));
		assertEquals(List.of(first, first.plusNanos(1), first.plusMillis(500), first.plusSeconds(1), first,
				first.plusNanos(999_999_999), first.plusMillis(123), first.plusMillis(250)),
				database.query("SELECT value FROM work ORDER BY id").fetchList(Instant.class));
		assertEquals("2024-07-01T12:34:00.000000000Z", database.query("SELECT CAST(value AS TEXT) FROM work WHERE id=1").fetchObject(String.class).orElseThrow());
		assertEquals("2024-07-01T12:34:00.999999999Z", database.query("SELECT CAST(value AS TEXT) FROM work WHERE id=6").fetchObject(String.class).orElseThrow());
	}

	@Test
	void unzonedOptInUsesOneWallClockRepresentationRegardlessOfParameterPosition() {
		Database database = database("local", AmbiguousTimestampBindingStrategy.TIMESTAMP_WITHOUT_TIME_ZONE);
		database.query("CREATE TABLE work (id INT PRIMARY KEY, value TEXT)").execute();
		Instant instant = Instant.parse("2024-07-01T12:34:56.123Z");
		database.query("INSERT INTO work (id, value) VALUES (:id, :v)").bind("id", 1).bind("v", instant).execute();
		database.query("INSERT INTO work (value, id) VALUES (:v, :id)").bind("v", instant.atOffset(ZoneOffset.ofHours(-4))).bind("id", 2).execute();
		assertEquals(List.of(LocalDateTime.of(2024, 7, 1, 21, 34, 56, 123_000_000),
				LocalDateTime.of(2024, 7, 1, 21, 34, 56, 123_000_000)), database.query("SELECT value FROM work ORDER BY id").fetchList(LocalDateTime.class));
		assertEquals(List.of(instant, instant), database.query("SELECT value FROM work ORDER BY id").fetchList(Instant.class));
		assertEquals(List.of(1, 2), database.query("SELECT id FROM work WHERE value=:v ORDER BY id").bind("v", instant).fetchList(Integer.class));
		List<String> text = database.query("SELECT CAST(value AS TEXT) FROM work ORDER BY id").fetchList(String.class);
		assertEquals(text.get(0), text.get(1));
		assertFalse(text.get(0).endsWith("Z"));
		assertEquals("2024-07-01T21:34:56.123000000", text.get(0));
		LocalDateTime localMinute = LocalDateTime.of(2024, 7, 1, 21, 35);
		Instant minute = localMinute.atZone(ZoneId.of("Asia/Tokyo")).toInstant();
		database.query("INSERT INTO work (value, id) VALUES (:v, 3)").bind("v", minute).execute();
		database.query("INSERT INTO work (id, value) VALUES (4, :v)").bind("v", minute.plusNanos(1)).execute();
		database.query("INSERT INTO work (id, value) VALUES (5, :v)").bind("v", localMinute.plusNanos(500_000_000)).execute();
		database.query("INSERT INTO work (id, value) VALUES (6, :v)").bind("v", localMinute.plusSeconds(1)).execute();
		assertEquals(List.of(1, 2, 3, 4, 5, 6), database.query("SELECT id FROM work ORDER BY value, id").fetchList(Integer.class));
		assertEquals(List.of(3, 4, 5), database.query("SELECT id FROM work WHERE value>=:lo AND value<:hi ORDER BY id")
				.bind("lo", minute).bind("hi", localMinute.plusSeconds(1)).fetchList(Integer.class));
		assertEquals(minute.plusNanos(500_000_000), database.query("SELECT value FROM work WHERE id=5").fetchObject(Instant.class).orElseThrow());
		assertEquals("2024-07-01T21:35:00.000000000", database.query("SELECT CAST(value AS TEXT) FROM work WHERE id=3").fetchObject(String.class).orElseThrow());
		assertEquals("2024-07-01T21:35:00.000000001", database.query("SELECT CAST(value AS TEXT) FROM work WHERE id=4").fetchObject(String.class).orElseThrow());
	}

	@Test
	void existingVariableWidthAndOffsetTimestampTextStillReadsAsTheOriginalInstant() {
		Database database = database("legacy", AmbiguousTimestampBindingStrategy.TIMESTAMP_WITH_TIME_ZONE);
		database.query("CREATE TABLE work (id INT PRIMARY KEY, value TEXT)").execute();
		database.query("INSERT INTO work VALUES (1, '2024-07-01T12:34Z'), (2, '2024-07-01T12:34:00.500Z'), (3, '2024-07-01T08:34:00-04:00')").execute();
		Instant first = Instant.parse("2024-07-01T12:34:00Z");
		assertEquals(List.of(first, first.plusMillis(500), first), database.query("SELECT value FROM work ORDER BY id").fetchList(Instant.class));
	}

	private Database database(String name, AmbiguousTimestampBindingStrategy strategy) {
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
				"jdbc:sqlite:" + this.directory.resolve(name + ".sqlite"), null, null), DatabaseType.SQLITE)
				.timeZone(ZoneId.of("Asia/Tokyo")).ambiguousTimestampBindingStrategy(strategy).build();
	}
}
