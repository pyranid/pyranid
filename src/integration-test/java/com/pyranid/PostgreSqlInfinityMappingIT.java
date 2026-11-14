/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.PGStatement;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlInfinityMappingIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_temporal_infinity").withUsername("pyranid").withPassword("pyranid");
	public record Stamp(Timestamp value) {}
	public record Day(java.sql.Date value) {}
	public record LegacyDate(Date value) {}
	public record Stamps(List<Timestamp> value) {}
	public record Days(List<java.sql.Date> value) {}
	public record NestedStamps(List<List<Timestamp>> value) {}
	public record LocalDays(List<LocalDate> value) {}
	public record LocalStamps(List<LocalDateTime> value) {}
	public record OffsetStamps(List<OffsetDateTime> value) {}
	public static class StampBean {
		private Timestamp value;
		public Timestamp getValue() { return value; }
		public void setValue(Timestamp value) { this.value = value; }
	}

	@Test
	void infinitiesRejectLegacyScalarRecordBeanAndContainerTargets() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (String type : List.of("date", "timestamp", "timestamptz")) {
				for (String infinity : List.of("infinity", "-infinity")) {
					String literal = "'" + infinity + "'::" + type;
					String scalar = "SELECT " + literal + " AS value";
					for (Class<?> target : List.of(Timestamp.class, java.sql.Date.class, Date.class,
							Stamp.class, Day.class, LegacyDate.class, StampBean.class))
						assertThrows(DatabaseException.class, () -> database.query(scalar).fetchObject(target), scalar + " " + target);
					String array = "SELECT ARRAY[" + literal + ", NULL] AS value";
					for (Class<?> target : List.of(Timestamp[].class, java.sql.Date[].class, Date[].class, Stamps.class, Days.class))
						assertThrows(DatabaseException.class, () -> database.query(array).fetchObject(target), array + " " + target);
					assertThrows(DatabaseException.class, () -> database.query("SELECT ARRAY[ARRAY[" + literal + "]] AS value")
							.fetchObject(NestedStamps.class));
				}
			}
		}
	}

	@Test
	void javaTimeInfinitySentinelsAndNativeDriverValuesRemainAvailable() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (boolean positive : new boolean[]{false, true}) {
				String infinity = positive ? "infinity" : "-infinity";
				long millis = positive ? PGStatement.DATE_POSITIVE_INFINITY : PGStatement.DATE_NEGATIVE_INFINITY;
				LocalDate day = positive ? LocalDate.MAX : LocalDate.MIN;
				LocalDateTime stamp = positive ? LocalDateTime.MAX : LocalDateTime.MIN;
				OffsetDateTime offset = positive ? OffsetDateTime.MAX : OffsetDateTime.MIN;
				assertEquals(day, database.query("SELECT '" + infinity + "'::date").fetchObject(LocalDate.class).orElseThrow());
				assertEquals(stamp, database.query("SELECT '" + infinity + "'::timestamp").fetchObject(LocalDateTime.class).orElseThrow());
				assertEquals(offset, database.query("SELECT '" + infinity + "'::timestamptz").fetchObject(OffsetDateTime.class).orElseThrow());
				assertEquals(List.of(day), database.query("SELECT ARRAY['" + infinity + "'::date] AS value").fetchObject(LocalDays.class).orElseThrow().value());
				assertEquals(List.of(stamp), database.query("SELECT ARRAY['" + infinity + "'::timestamp] AS value").fetchObject(LocalStamps.class).orElseThrow().value());
				assertEquals(List.of(offset), database.query("SELECT ARRAY['" + infinity + "'::timestamptz] AS value").fetchObject(OffsetStamps.class).orElseThrow().value());
				for (String type : List.of("date", "timestamp", "timestamptz")) {
					String literal = "'" + infinity + "'::" + type;
					assertEquals(millis, ((Date) database.query("SELECT " + literal).fetchObject(Object.class).orElseThrow()).getTime());
					Map<?, ?> row = database.query("SELECT " + literal + " AS value").fetchObject(Map.class).orElseThrow();
					Object rowValue = type.equals("date") ? day : type.equals("timestamp") ? stamp : offset;
					assertEquals(rowValue, row.get("value"));
					Object[] values = database.query("SELECT ARRAY[" + literal + "]").fetchObject(Object[].class).orElseThrow();
					assertEquals(millis, ((Date) values[0]).getTime());
				}
			}
		}
	}

	@Test
	void finiteWideAndBceValuesRetainTimestampInstantsButSqlDatesRequireFaithfulCivilDates() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (String literal : List.of("TIMESTAMP '200000-02-03 04:05:06.123456'", "TIMESTAMP '0001-02-03 04:05:06.123456 BC'",
					"DATE '1000000-01-01'")) {
				String sql = "SELECT " + literal + " AS value";
				LocalDateTime wall = database.query(sql).fetchObject(LocalDateTime.class).orElseThrow();
				assertEquals(wall.atZone(database.getTimeZone()).toInstant(), database.query(sql).fetchObject(Timestamp.class).orElseThrow().toInstant());
				assertEquals(wall.atZone(database.getTimeZone()).toInstant(), database.query(sql).fetchObject(Stamp.class).orElseThrow().value().toInstant());
				assertThrows(DatabaseException.class, () -> database.query(sql).fetchObject(java.sql.Date.class));
			}
			for (String day : List.of("0001-01-01", "1582-10-04", "1582-10-15", "1600-02-29", "2000-02-29", "9999-12-31")) {
				String sql = "SELECT DATE '" + day + "' AS value";
				assertEquals(LocalDate.parse(day), database.query(sql).fetchObject(java.sql.Date.class).orElseThrow().toLocalDate());
				assertEquals(LocalDate.parse(day), database.query(sql).fetchObject(Day.class).orElseThrow().value().toLocalDate());
			}
			for (String day : List.of("0001-01-01 BC", "10000-01-01", "1582-10-10"))
				assertThrows(DatabaseException.class, () -> database.query("SELECT DATE '" + day + "'").fetchObject(java.sql.Date.class));
		}
	}

	private static Database database(boolean cached) {
		PGSimpleDataSource source = new PGSimpleDataSource() {
			@Override public Connection getConnection() throws SQLException {
				Connection connection = super.getConnection();
				try (java.sql.Statement statement = connection.createStatement()) {
					statement.execute("SET TIME ZONE 'Pacific/Honolulu'");
				} catch (SQLException failure) { connection.close(); throw failure; }
				return connection;
			}
		};
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		ZoneId zone = ZoneId.systemDefault().equals(ZoneId.of("Asia/Kathmandu")) ? ZoneId.of("UTC") : ZoneId.of("Asia/Kathmandu");
		return Database.with(source, DatabaseType.POSTGRESQL).timeZone(zone)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
