/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;

/**
 * Keeps temporal scalar, record, and JavaBean mapping consistent across both mapping paths.
 */
public class TemporalResultSetMappingTests {
	private static final ZoneId DATABASE_ZONE = ZoneId.of("America/New_York");
	private static final String TIMESTAMP_SQL = "SELECT CAST(:value AS TIMESTAMP(9)) AS value FROM (VALUES (0))";
	private static final String ZONED_TIMESTAMP_SQL = "SELECT CAST(:value AS TIMESTAMP(9) WITH TIME ZONE) AS value FROM (VALUES (0))";
	private static final String TIME_ZONE_SQL = "SELECT CAST(:value AS VARCHAR(64)) AS value FROM (VALUES (0))";

	public record OffsetRow(OffsetDateTime value) {}
	public record TimestampRow(Timestamp value) {}
	public record SqlDateRow(java.sql.Date value) {}
	public record DateRow(Date value) {}
	public record TimeZoneRow(TimeZone value) {}

	public static class OffsetBean {
		private OffsetDateTime value;
		public OffsetDateTime getValue() { return this.value; }
		public void setValue(OffsetDateTime value) { this.value = value; }
	}

	public static class TimestampBean {
		private Timestamp value;
		public Timestamp getValue() { return this.value; }
		public void setValue(Timestamp value) { this.value = value; }
	}

	public static class SqlDateBean {
		private java.sql.Date value;
		public java.sql.Date getValue() { return this.value; }
		public void setValue(java.sql.Date value) { this.value = value; }
	}

	public static class TimeZoneBean {
		private TimeZone value;
		public TimeZone getValue() { return this.value; }
		public void setValue(TimeZone value) { this.value = value; }
	}

	@Test
	public void zoneLessTimestampsUseConfiguredZoneAcrossRowShapes() {
		for (boolean planCachingEnabled : new boolean[]{true, false}) {
			Database database = database("zone_less", planCachingEnabled);
			for (LocalDateTime local : new LocalDateTime[]{
					LocalDateTime.parse("2020-01-02T03:04:05.123456789"),
					LocalDateTime.parse("2020-07-02T03:04:05.123456789"),
					LocalDateTime.parse("2024-03-10T02:30:00.123456789")}) {
				OffsetDateTime expected = local.atZone(DATABASE_ZONE).toOffsetDateTime();
				Assertions.assertEquals(expected, database.query(TIMESTAMP_SQL).bind("value", local)
						.fetchObject(OffsetDateTime.class).orElseThrow());
				Assertions.assertEquals(expected, database.query(TIMESTAMP_SQL).bind("value", local)
						.fetchObject(OffsetRow.class).orElseThrow().value());
				Assertions.assertEquals(expected, database.query(TIMESTAMP_SQL).bind("value", local)
						.fetchObject(OffsetBean.class).orElseThrow().getValue());
				Assertions.assertEquals(expected.toInstant(), database.query(TIMESTAMP_SQL).bind("value", local)
						.fetchObject(Instant.class).orElseThrow());
			}
		}
	}

	@Test
	public void zonedTimestampsPreserveTheirOffsetAcrossRowShapes() {
		OffsetDateTime expected = OffsetDateTime.parse("2020-01-02T03:04:05.123456789+05:45");
		for (boolean planCachingEnabled : new boolean[]{true, false}) {
			Database database = database("zoned", planCachingEnabled);
			Assertions.assertEquals(expected, database.query(ZONED_TIMESTAMP_SQL).bind("value", expected)
					.fetchObject(OffsetDateTime.class).orElseThrow());
			Assertions.assertEquals(expected, database.query(ZONED_TIMESTAMP_SQL).bind("value", expected)
					.fetchObject(OffsetRow.class).orElseThrow().value());
			Assertions.assertEquals(expected, database.query(ZONED_TIMESTAMP_SQL).bind("value", expected)
					.fetchObject(OffsetBean.class).orElseThrow().getValue());
		}
	}

	@Test
	public void jdbcTimestampTargetsPreserveTypeAndNanosecondsAcrossRowShapes() {
		LocalDateTime local = LocalDateTime.parse("2020-01-02T03:04:05.123456789");
		OffsetDateTime zoned = OffsetDateTime.parse("2020-01-02T03:04:05.123456789+05:45");
		for (boolean planCachingEnabled : new boolean[]{true, false}) {
			Database database = database("jdbc_timestamp", planCachingEnabled);
			assertTimestampTargets(database, TIMESTAMP_SQL, local, Timestamp.valueOf(local));
			assertTimestampTargets(database, ZONED_TIMESTAMP_SQL, zoned, Timestamp.from(zoned.toInstant()));
			Assertions.assertEquals(Date.from(local.atZone(DATABASE_ZONE).toInstant()),
					database.query(TIMESTAMP_SQL).bind("value", local).fetchObject(DateRow.class).orElseThrow().value());
		}
	}

	@Test
	public void jdbcDateTargetsRetainTheirSubtypeAcrossRowShapes() {
		LocalDateTime local = LocalDateTime.parse("2020-01-02T03:04:05.123456789");
		java.sql.Date expected = java.sql.Date.valueOf(local.toLocalDate());
		for (boolean planCachingEnabled : new boolean[]{true, false}) {
			Database database = database("jdbc_date", planCachingEnabled);
			Assertions.assertEquals(expected, database.query(TIMESTAMP_SQL).bind("value", local)
					.fetchObject(java.sql.Date.class).orElseThrow());
			Assertions.assertEquals(expected, database.query(TIMESTAMP_SQL).bind("value", local)
					.fetchObject(SqlDateRow.class).orElseThrow().value());
			Assertions.assertEquals(expected, database.query(TIMESTAMP_SQL).bind("value", local)
					.fetchObject(SqlDateBean.class).orElseThrow().getValue());
		}
	}

	@Test
	public void zonedTimestampsUseConfiguredDateForJdbcDateTargets() {
		// The source wall date is January 2, while the configured New York date is January 1.
		OffsetDateTime zoned = OffsetDateTime.parse("2020-01-02T03:04:05.123456789+05:45");
		java.sql.Date expected = java.sql.Date.valueOf("2020-01-01");
		for (boolean planCachingEnabled : new boolean[]{true, false}) {
			Database database = database("zoned_jdbc_date", planCachingEnabled);
			Assertions.assertEquals(expected, database.query(ZONED_TIMESTAMP_SQL).bind("value", zoned)
					.fetchObject(java.sql.Date.class).orElseThrow());
			Assertions.assertEquals(expected, database.query(ZONED_TIMESTAMP_SQL).bind("value", zoned)
					.fetchObject(SqlDateRow.class).orElseThrow().value());
			Assertions.assertEquals(expected, database.query(ZONED_TIMESTAMP_SQL).bind("value", zoned)
					.fetchObject(SqlDateBean.class).orElseThrow().getValue());
		}
	}

	@Test
	public void instantConversionKeepsJdbcDateSubtypes() {
		Database database = database("instant_subtypes", true);
		DefaultResultSetMapper mapper = (DefaultResultSetMapper) database.getResultSetMapper();
		StatementContext<Object> context = StatementContext.with(Statement.of("instant_subtypes", "SELECT 1"), database).build();
		Instant instant = Instant.parse("2020-01-02T03:04:05.123456789Z");
		Assertions.assertEquals(Timestamp.from(instant), mapper.convertResultSetValueToTargetType(
				context, instant, TargetType.of(Timestamp.class)).orElseThrow());
		Assertions.assertEquals(java.sql.Date.valueOf(instant.atZone(DATABASE_ZONE).toLocalDate()),
				mapper.convertResultSetValueToTargetType(context, instant, TargetType.of(java.sql.Date.class)).orElseThrow());
		Assertions.assertEquals(Date.from(instant), mapper.convertResultSetValueToTargetType(
				context, instant, TargetType.of(Date.class)).orElseThrow());
	}

	@Test
	public void utcAndUtTimeZoneOffsetsSurviveMappingAcrossRowShapes() {
		for (boolean planCachingEnabled : new boolean[]{true, false}) {
			Database database = database("time_zone_offsets", planCachingEnabled);
			for (String zoneId : new String[]{"UTC+02:00", "UT-03:30", "GMT+02:00", "UTC", "UT", "America/New_York"}) {
				TimeZone expected = TimeZone.getTimeZone(ZoneId.of(zoneId));
				assertTimeZoneRules(expected, database.query(TIME_ZONE_SQL).bind("value", zoneId)
						.fetchObject(TimeZone.class).orElseThrow());
				assertTimeZoneRules(expected, database.query(TIME_ZONE_SQL).bind("value", zoneId)
						.fetchObject(TimeZoneRow.class).orElseThrow().value());
				assertTimeZoneRules(expected, database.query(TIME_ZONE_SQL).bind("value", zoneId)
						.fetchObject(TimeZoneBean.class).orElseThrow().getValue());
			}
			Assertions.assertThrows(DatabaseException.class, () -> database.query(TIME_ZONE_SQL)
					.bind("value", "UTC+25:00").fetchObject(TimeZone.class));
		}
	}

	private void assertTimestampTargets(Database database, String sql, Object value, Timestamp expected) {
		Assertions.assertEquals(expected, database.query(sql).bind("value", value)
				.fetchObject(Timestamp.class).orElseThrow());
		Assertions.assertEquals(expected, database.query(sql).bind("value", value)
				.fetchObject(TimestampRow.class).orElseThrow().value());
		Assertions.assertEquals(expected, database.query(sql).bind("value", value)
				.fetchObject(TimestampBean.class).orElseThrow().getValue());
	}

	private void assertTimeZoneRules(TimeZone expected, TimeZone actual) {
		Assertions.assertTrue(expected.hasSameRules(actual),
				() -> "Expected rules for " + expected.getID() + " but received " + actual.getID());
	}

	private Database database(String name, boolean planCachingEnabled) {
		// Force a driver/session zone different from Pyranid's configured zone without mutating JVM defaults.
		JDBCDataSource dataSource = new JDBCDataSource() {
			@Override
			public Connection getConnection() throws SQLException {
				Connection connection = super.getConnection();
				try (java.sql.Statement statement = connection.createStatement()) {
					statement.execute("SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE");
				} catch (SQLException e) {
					connection.close();
					throw e;
				}
				return connection;
			}
		};
		dataSource.setUrl("jdbc:hsqldb:mem:temporal_mapping_" + name + "_" + planCachingEnabled);
		dataSource.setUser("sa");
		dataSource.setPassword("");
		return Database.withDataSource(dataSource)
				.timeZone(DATABASE_ZONE)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(planCachingEnabled).build())
				.build();
	}
}
