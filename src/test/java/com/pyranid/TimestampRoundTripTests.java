/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TimestampRoundTripTests {
	public record Stamp(Timestamp stamp) {}
	public static class StampBean {
		private Timestamp stamp;
		public Timestamp getStamp() { return this.stamp; }
		public void setStamp(Timestamp stamp) { this.stamp = stamp; }
	}

	@Test
	void timestampReadModifyWriteKeepsItsInstantAcrossScalarRecordAndBeanPaths() {
		for (boolean cached : new boolean[]{false, true}) {
			for (ZoneId zone : List.of(ZoneId.of("UTC"), ZoneId.of("America/New_York"), ZoneId.of("Asia/Kathmandu"))) {
				Database database = database(zone, cached);
				database.query("CREATE TABLE work (id INT PRIMARY KEY, stamp TIMESTAMP(9), zoned TIMESTAMP(9) WITH TIME ZONE)").execute();
				for (Instant instant : List.of(Instant.parse("2024-01-02T12:34:56.123456789Z"), Instant.parse("2024-07-01T12:34:56.987654321Z"))) {
					database.query("DELETE FROM work").execute();
					database.query("INSERT INTO work VALUES (1, :stamp, :stamp)").bind("stamp", Timestamp.from(instant)).execute();
					for (String column : List.of("stamp", "zoned")) {
						String sql = "SELECT " + column + " AS stamp FROM work WHERE id = 1";
						for (int pass = 0; pass < 3; pass++) {
							Timestamp scalar = database.query(sql).fetchObject(Timestamp.class).orElseThrow();
							Timestamp record = database.query(sql).fetchObject(Stamp.class).orElseThrow().stamp();
							Timestamp bean = database.query(sql).fetchObject(StampBean.class).orElseThrow().getStamp();
							for (Timestamp value : List.of(scalar, record, bean)) {
								assertEquals(instant, value.toInstant(), zone + " " + column + " pass=" + pass);
								assertEquals(instant.getNano(), value.getNanos());
							}
							Timestamp rewrite = pass == 0 ? scalar : pass == 1 ? record : bean;
							database.query("UPDATE work SET " + column + " = :stamp WHERE id = 1").bind("stamp", rewrite).execute();
						}
						assertEquals(instant, database.query(sql).fetchObject(Instant.class).orElseThrow());
					}
				}
			}
		}
	}

	@Test
	void dateToTimestampUsesTheConfiguredStartOfDayAndKeepsJdbcDateWallSemantics() {
		for (ZoneId zone : List.of(ZoneId.of("America/New_York"), ZoneId.of("Asia/Kathmandu"))) {
			Database database = database(zone, true);
			LocalDate date = LocalDate.of(2024, 3, 10);
			String sql = "SELECT DATE '2024-03-10' AS stamp FROM (VALUES(0))";
			assertEquals(date.atStartOfDay(zone).toInstant(), database.query(sql).fetchObject(Timestamp.class).orElseThrow().toInstant());
			assertEquals(date.atStartOfDay(zone).toInstant(), database.query(sql).fetchObject(Stamp.class).orElseThrow().stamp().toInstant());
			assertEquals(date.atStartOfDay(zone).toInstant(), database.query(sql).fetchObject(StampBean.class).orElseThrow().getStamp().toInstant());
			assertEquals(java.sql.Date.valueOf(date), database.query(sql).fetchObject(java.sql.Date.class).orElseThrow());
		}
	}

	@Test
	void objectTargetsKeepTheDriverTimestampWhileTypedTargetsUseTheDatabaseZone() {
		ZoneId zone = ZoneId.systemDefault().equals(ZoneId.of("UTC")) ? ZoneId.of("Asia/Kathmandu") : ZoneId.of("UTC");
		Database database = database(zone, true);
		String sql = "SELECT TIMESTAMP '2024-01-02 12:34:56.123456789' AS stamp FROM (VALUES(0))";
		Timestamp raw = assertInstanceOf(Timestamp.class, database.query(sql).fetchObject(Object.class).orElseThrow());
		LocalDateTime local = LocalDateTime.parse("2024-01-02T12:34:56.123456789");
		assertEquals(local, raw.toLocalDateTime());
		assertEquals(local.atZone(zone).toInstant(), database.query(sql).fetchObject(Timestamp.class).orElseThrow().toInstant());
	}

	private static Database database(ZoneId zone, boolean cached) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:timestamp_roundtrip_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).timeZone(zone)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
