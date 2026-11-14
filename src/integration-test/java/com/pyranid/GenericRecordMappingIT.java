/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class GenericRecordMappingIT {
	public record Box<T>(T value) {}
	public record Direct(Box<UUID> value) {}
	public record ListRow(List<Box<UUID>> value) {}
	public record MapRow(Map<String, Box<UUID>> value) {}
	public record Nested(Box<Map<UUID, List<Box<Long>>>> value) {}
	public record InstantRow(Box<Instant> value) {}
	public record LocalRow(Box<LocalDateTime> value) {}
	public record OffsetRow(Box<OffsetDateTime> value) {}
	public record ConcreteOffset(OffsetDateTime value) {}
	public record ConcreteOffsetRow(ConcreteOffset value) {}
	public static class GenericBean<T> {
		private Box<T> value;
		public void setValue(Box<T> value) { this.value = value; }
		public Box<T> getValue() { return value; }
	}
	public static class UuidBean extends GenericBean<UUID> {}

	@Test
	void duckDbStructsInsideDirectListMapAndInheritedPropertiesKeepGenericArguments() {
		UUID uuid = UUID.randomUUID();
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (int pass = 0; pass < 2; pass++) {
				assertEquals(uuid, database.query("SELECT {'value': CAST(:id AS VARCHAR)} AS value").bind("id", uuid)
						.fetchObject(Direct.class).orElseThrow().value().value());
				assertEquals(Arrays.asList(new Box<>(uuid), null), database.query("SELECT [{'value': CAST(:id AS VARCHAR)}, NULL] AS value")
						.bind("id", uuid).fetchObject(ListRow.class).orElseThrow().value());
				Map<String, Box<UUID>> values = database.query("SELECT MAP(['first', 'missing'], [{'value': CAST(:id AS VARCHAR)}, NULL]) AS value")
						.bind("id", uuid).fetchObject(MapRow.class).orElseThrow().value();
				assertEquals(new Box<>(uuid), values.get("first")); assertNull(values.get("missing"));
				assertEquals(uuid, database.query("SELECT {'value': CAST(:id AS VARCHAR)} AS value").bind("id", uuid)
						.fetchObject(UuidBean.class).orElseThrow().getValue().value());
				assertEquals(7L, database.query("SELECT {'value': MAP([CAST(:id AS VARCHAR)], [[{'value': 7}]])} AS value").bind("id", uuid)
						.fetchObject(Nested.class).orElseThrow().value().value().get(uuid).get(0).value());
				assertNull(database.query("SELECT NULL::STRUCT(value VARCHAR) AS value").fetchObject(Direct.class).orElseThrow().value());
				assertNull(database.query("SELECT {'value': NULL::VARCHAR} AS value").fetchObject(Direct.class).orElseThrow().value().value());
			}
		}
	}

	@Test
	void duckDbGenericTemporalRecordsKeepTimestampAndTimestampWithTimeZoneSemantics() {
		ZoneId zone = ZoneId.of("Asia/Tokyo");
		LocalDateTime wall = LocalDateTime.of(2024, 1, 2, 3, 4, 5);
		Instant instant = Instant.parse("2024-01-02T03:04:05Z");
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached, zone);
			for (int pass = 0; pass < 2; pass++) {
				for (boolean zoned : new boolean[]{false, true}) {
					String sql = "SELECT {'value': " + (zoned ? "TIMESTAMPTZ '2024-01-02 03:04:05+00'" : "TIMESTAMP '2024-01-02 03:04:05'") + "} AS value";
					assertEquals(zoned ? instant : wall.atZone(zone).toInstant(),
							database.query(sql).fetchObject(InstantRow.class).orElseThrow().value().value());
					assertEquals(zoned ? instant.atZone(zone).toLocalDateTime() : wall,
							database.query(sql).fetchObject(LocalRow.class).orElseThrow().value().value());
					OffsetDateTime offset = database.query(sql).fetchObject(OffsetRow.class).orElseThrow().value().value();
					assertEquals(zoned ? instant : wall.atZone(zone).toInstant(), offset.toInstant());
					// A native OffsetDateTime keeps the driver's offset, matching concrete record targets.
					assertEquals(database.query(sql).fetchObject(ConcreteOffsetRow.class).orElseThrow().value().value(), offset);
				}
			}
		}
	}

	private static Database database(boolean cached) {
		return database(cached, ZoneId.systemDefault());
	}

	private static Database database(boolean cached, ZoneId zone) {
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:duckdb:", null, null), DatabaseType.DUCK_DB)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).timeZone(zone).build();
	}
}
