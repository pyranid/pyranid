/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.time.ZoneId;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TemporalArrayMappingIntegrationIT {
	public record Values(List<Instant> values) {}
	public record Payload(Values payload) {}
	public record NestedValues(List<List<Instant>> values) {}
	public record Stamp(Instant value) {}
	public record StampRow(Stamp payload) {}
	public record StampList(List<Stamp> values) {}
	public record NativeStamp(Object value) {}
	public record TextStamp(String value) {}
	public record NativeMapRow(Map<String, Object> payload) {}
	public record TemporalFields(Instant local, Instant zoned, LocalDateTime localWall, LocalDateTime zonedWall,
			Timestamp sqlTimestamp, java.sql.Date sqlDate, Instant date, LocalTime time,
			@DatabaseColumn("quoted, \"value\"") Instant alias, Stamp nested) {}

	@Test
	void duckDbStructTimestampsMatchScalarAndArrayConversionsWithoutChangingNativeValues() {
		LocalDateTime local = LocalDateTime.parse("2024-01-02T12:34:56.123456");
		OffsetDateTime zoned = OffsetDateTime.parse("2024-01-02T12:34:56.123456+05:45");
		for (boolean cached : new boolean[]{false, true}) {
			for (ZoneId zone : List.of(ZoneId.of("UTC"), ZoneId.of("Asia/Kathmandu"))) {
				Database database = Database.withDataSource(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:duckdb:", null, null))
						.databaseType(DatabaseType.DUCK_DB).timeZone(zone)
						.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
				for (boolean withZone : new boolean[]{false, true}) {
					String literal = withZone ? "TIMESTAMPTZ '2024-01-02 12:34:56.123456+05:45'" : "TIMESTAMP '2024-01-02 12:34:56.123456'";
					Instant expected = withZone ? zoned.toInstant() : local.atZone(zone).toInstant();
					for (int execution = 0; execution < 2; execution++) {
						assertEquals(expected, database.query("SELECT " + literal).fetchObject(Instant.class).orElseThrow());
						assertEquals(expected, database.query("SELECT {'value': " + literal + "} AS payload").fetchObject(Stamp.class).orElseThrow().value());
						assertEquals(expected, database.query("SELECT {'value': " + literal + "} AS payload").fetchObject(StampRow.class).orElseThrow().payload().value());
						assertEquals(Arrays.asList(new Stamp(expected), null), database.query("SELECT [{'value': " + literal + "}, NULL] AS values")
								.fetchObject(StampList.class).orElseThrow().values());
						Object raw = database.query("SELECT " + literal + " AS value").fetchObject(Object.class).orElseThrow();
						assertEquals(raw, database.query("SELECT {'value': " + literal + "} AS payload").fetchObject(NativeStamp.class).orElseThrow().value());
						DatabaseException unsupportedText = assertThrows(DatabaseException.class, () -> database.query("SELECT {'value': " + literal + "} AS payload").fetchObject(TextStamp.class));
						assertTrue(unsupportedText.getMessage().contains(raw.getClass().getName()));
						assertEquals(raw, database.query("SELECT {'value': " + literal + "} AS payload").fetchObject(NativeMapRow.class).orElseThrow().payload().get("value"));
					}
				}
				String literal = "TIMESTAMP '2024-01-02 12:34:56.123456'";
				String zonedLiteral = "TIMESTAMPTZ '2024-01-02 12:34:56.123456+05:45'";
				String sql = "SELECT {'nested': {'value': " + literal + "}, 'local': " + literal + ", 'zoned': " + zonedLiteral
						+ ", 'local_wall': " + literal + ", 'zoned_wall': " + zonedLiteral
						+ ", 'sql_timestamp': " + literal + ", 'sql_date': " + literal
						+ ", 'date': DATE '2024-01-02', 'time': TIME '12:34:56', 'quoted, \"value\"': " + literal + "} AS payload";
				TemporalFields expected = new TemporalFields(local.atZone(zone).toInstant(), zoned.toInstant(), local,
						zoned.atZoneSameInstant(zone).toLocalDateTime(), Timestamp.valueOf(local), java.sql.Date.valueOf(local.toLocalDate()),
						LocalDate.parse("2024-01-02").atStartOfDay(zone).toInstant(), LocalTime.parse("12:34:56"),
						local.atZone(zone).toInstant(), new Stamp(local.atZone(zone).toInstant()));
				assertEquals(expected, database.query(sql).fetchObject(TemporalFields.class).orElseThrow());
			}
		}
	}

	@Test
	void duckDbNestedArraysRetainTimestampSemanticsThroughDialectUnwrapping() {
		LocalDateTime local = LocalDateTime.parse("2024-07-01T12:30:00.123456");
		OffsetDateTime zoned = OffsetDateTime.parse("2024-07-01T12:30:00.123456+05:45");
		for (boolean cached : new boolean[]{false, true}) {
			for (ZoneId zone : List.of(ZoneId.of("UTC"), ZoneId.of("America/New_York"))) {
				Database database = Database.withDataSource(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:duckdb:", null, null))
						.databaseType(DatabaseType.DUCK_DB).timeZone(zone)
						.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
				for (boolean withZone : new boolean[]{false, true}) {
					String literal = withZone ? "TIMESTAMPTZ '2024-07-01 12:30:00.123456+05:45'" : "TIMESTAMP '2024-07-01 12:30:00.123456'";
					Instant expected = withZone ? zoned.toInstant() : local.atZone(zone).toInstant();
					for (int execution = 0; execution < 2; execution++) {
						assertEquals(expected, database.query("SELECT " + literal).fetchObject(Instant.class).orElseThrow());
						assertEquals(List.of(expected), database.query("SELECT [" + literal + "] AS values")
								.fetchObject(Values.class).orElseThrow().values());
						assertEquals(List.of(expected), database.query("SELECT {'values': [" + literal + "]} AS payload")
								.fetchObject(Payload.class).orElseThrow().payload().values());
						assertEquals(List.of(List.of(expected)), database.query("SELECT [[" + literal + "]] AS values")
								.fetchObject(NestedValues.class).orElseThrow().values());
					}
				}
			}
		}
	}
}
