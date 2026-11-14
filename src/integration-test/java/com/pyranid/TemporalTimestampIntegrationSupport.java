/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class TemporalTimestampIntegrationSupport {
	private TemporalTimestampIntegrationSupport() {}
	public record Stamp(Timestamp stamp) {}
	public static class StampBean {
		private Timestamp stamp;
		public Timestamp getStamp() { return this.stamp; }
		public void setStamp(Timestamp stamp) { this.stamp = stamp; }
	}

	static void verify(Database database) {
		String table = "timestamp_roundtrip_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (stamp TIMESTAMP, zoned TIMESTAMPTZ)").execute();
		try {
			for (Instant instant : List.of(Instant.parse("2024-01-02T12:34:56.123456Z"), Instant.parse("2024-07-01T12:34:56.987654Z"))) {
				database.query("DELETE FROM " + table).execute();
				database.query("INSERT INTO " + table + " VALUES (:stamp, :stamp)").bind("stamp", Timestamp.from(instant)).execute();
				for (String column : List.of("stamp", "zoned")) {
					String sql = "SELECT " + column + " AS stamp FROM " + table;
					for (int pass = 0; pass < 3; pass++) {
						Timestamp scalar = database.query(sql).fetchObject(Timestamp.class).orElseThrow();
						Timestamp record = database.query(sql).fetchObject(Stamp.class).orElseThrow().stamp();
						Timestamp bean = database.query(sql).fetchObject(StampBean.class).orElseThrow().getStamp();
						assertEquals(instant, scalar.toInstant(), database.getTimeZone() + " " + column);
						assertEquals(scalar, record);
						assertEquals(scalar, bean);
						database.query("UPDATE " + table + " SET " + column + " = :stamp").bind("stamp", pass == 0 ? scalar : pass == 1 ? record : bean).execute();
					}
					assertEquals(instant, database.query(sql).fetchObject(Instant.class).orElseThrow());
				}
			}
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}
}
