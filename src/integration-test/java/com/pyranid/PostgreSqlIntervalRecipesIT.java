/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Fixed elapsed-time intervals use numeric projections, never driver interval text parsing. */
@Testcontainers
class PostgreSqlIntervalRecipesIT {

	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("interval_recipes").withUsername("pyranid").withPassword("pyranid");

	public record ElapsedRow(int id, Duration elapsed) {}

	@Test
	void durationCastAndNumericProjectionWorkWithPostgresIntervalStyle() { verifyIntervalStyle("postgres"); }

	@Test
	void durationCastAndNumericProjectionWorkWithVerboseIntervalStyle() { verifyIntervalStyle("postgres_verbose"); }

	@Test
	void durationCastAndNumericProjectionWorkWithSqlStandardIntervalStyle() { verifyIntervalStyle("sql_standard"); }

	@Test
	void durationCastAndNumericProjectionWorkWithIsoIntervalStyle() { verifyIntervalStyle("iso_8601"); }

	private void verifyIntervalStyle(String style) {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = durationDatabase(cached);
			String table = "elapsed_recipe_" + UUID.randomUUID().toString().replace("-", "");
			// Calendar days/months cannot safely be treated as fixed elapsed seconds, especially across DST.
			database.query("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, elapsed INTERVAL CHECK ("
					+ "EXTRACT(YEAR FROM elapsed)=0 AND EXTRACT(MONTH FROM elapsed)=0 AND EXTRACT(DAY FROM elapsed)=0))").execute();
			try {
				database.transaction(() -> {
					assertEquals(style, database.query("SELECT set_config('IntervalStyle', :style, true)")
							.bind("style", style).fetchObject(String.class).orElseThrow());
					List<Duration> durations = new ArrayList<>();
					durations.add(Duration.ZERO);
					durations.add(Duration.ofSeconds(1, 123_456_000));
					durations.add(Duration.ofNanos(-1_000));
					durations.add(Duration.ofHours(-26).minusNanos(123_456_000));
					durations.add(Duration.ofHours(49));
					durations.add(null);
					String insert = "INSERT INTO " + table + " VALUES (:id, CAST(:duration AS INTERVAL))";
					List<Map<String, Object>> groups = new ArrayList<>();
					for (int index = 0; index < durations.size(); index++) {
						Map<String, Object> group = new HashMap<>();
						group.put("id", index); group.put("duration", durations.get(index)); groups.add(group);
					}
					assertEquals(List.of(1L, 1L, 1L, 1L, 1L, 1L), database.query(insert).executeBatch(groups));
					List<ElapsedRow> expected = new ArrayList<>();
					for (int index = 0; index < durations.size(); index++) expected.add(new ElapsedRow(index, durations.get(index)));
					assertEquals(expected, database.query("SELECT id, EXTRACT(EPOCH FROM elapsed) AS elapsed FROM " + table + " ORDER BY id")
							.fetchList(ElapsedRow.class));
					assertEquals(Duration.ofHours(49), database.query("SELECT EXTRACT(EPOCH FROM elapsed) FROM " + table + " WHERE id=4")
							.fetchObject(Duration.class).orElseThrow());
					assertTrue(database.query("SELECT EXTRACT(EPOCH FROM elapsed) FROM " + table + " WHERE id=5").fetchObject(Duration.class).isEmpty());
					assertEquals(1L, database.query("UPDATE " + table + " SET elapsed=CAST(:duration AS INTERVAL) WHERE id=0")
							.bind("duration", Duration.ofMinutes(90)).execute());
					assertEquals(List.of(0), database.query("SELECT id FROM " + table + " WHERE elapsed=CAST(:duration AS INTERVAL)")
							.bind("duration", Duration.ofMinutes(90)).fetchList(Integer.class));
				});
				for (String calendar : List.of("1 month", "1 year", "1 day", "-1 month", "1 day -24 hours")) {
					DatabaseException rejected = assertThrows(DatabaseException.class, () -> database.query(insertSql(table))
							.bind("id", 100).bind("duration", calendar).execute());
					assertEquals("23514", rejected.getSqlState().orElseThrow());
				}
			} finally { database.query("DROP TABLE " + table).execute(); }
		}
	}

	@Test
	void precisionAndProjectionRequirementsFailExplicitlyInsteadOfSilentlyLosingSemantics() {
		Database database = durationDatabase(true);
		DatabaseException precision = assertThrows(DatabaseException.class, () -> database.query("SELECT CAST(:duration AS INTERVAL)")
				.bind("duration", Duration.ofNanos(1)).fetchObject(Object.class));
		assertEquals("22003", precision.getSqlState().orElseThrow());
		for (String expression : List.of("INTERVAL '1 month'", "INTERVAL '1 day'", "INTERVAL '1 hour'")) {
			DatabaseException direct = assertThrows(DatabaseException.class, () -> database.query("SELECT " + expression).fetchObject(Duration.class));
			assertEquals("22023", direct.getSqlState().orElseThrow());
		}
		DatabaseException nonNumeric = assertThrows(DatabaseException.class, () -> database.query("SELECT '123'::text").fetchObject(Duration.class));
		assertEquals("22023", nonNumeric.getSqlState().orElseThrow());
		DatabaseException overflow = assertThrows(DatabaseException.class, () -> database.query("SELECT 1e100::numeric").fetchObject(Duration.class));
		assertEquals("22003", overflow.getSqlState().orElseThrow());
	}

	@Test
	void aCalendarPeriodDayAndTwentyFourHourDurationHaveDifferentTimestampArithmetic() {
		Database database = durationDatabase(false);
		database.transaction(() -> {
			database.query("SET LOCAL TIME ZONE 'America/New_York'").execute();
			// This is a semantic distinction, not an attempt to supply a general Period binder.
			BigDecimal calendarSeconds = database.query("SELECT EXTRACT(EPOCH FROM ((TIMESTAMPTZ '2024-03-09 12:00:00 America/New_York'"
					+ " + CAST(:period AS INTERVAL)) - TIMESTAMPTZ '2024-03-09 12:00:00 America/New_York'))")
					.bind("period", Period.ofDays(1).toString()).fetchObject(BigDecimal.class).orElseThrow();
			assertEquals(0, calendarSeconds.compareTo(BigDecimal.valueOf(23 * 3600)));
			Duration fixedElapsed = database.query("SELECT EXTRACT(EPOCH FROM ((TIMESTAMPTZ '2024-03-09 12:00:00 America/New_York'"
					+ " + CAST(:duration AS INTERVAL)) - TIMESTAMPTZ '2024-03-09 12:00:00 America/New_York'))")
					.bind("duration", Duration.ofHours(24)).fetchObject(Duration.class).orElseThrow();
			assertEquals(Duration.ofHours(24), fixedElapsed);
		});
	}

	private static String insertSql(String table) { return "INSERT INTO " + table + " VALUES (:id, CAST(:duration AS INTERVAL))"; }

	private static Database durationDatabase(boolean cached) {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return Database.with(source, DatabaseType.POSTGRESQL)
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(new DurationBinder())))
				.resultSetMapper(ResultSetMapper.withCustomColumnMappers(List.of(new DurationSecondsMapper())).planCachingEnabled(cached).build()).build();
	}

	/** Application extension: CAST(:duration AS INTERVAL), fixed-time values with microsecond precision. */
	static final class DurationBinder implements CustomParameterBinder {
		@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(Duration.class); }
		@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object parameter) throws SQLException {
			Duration duration = (Duration) parameter;
			if (duration.getNano() % 1_000 != 0) throw new SQLException("PostgreSQL INTERVAL supports microseconds; round explicitly before binding Duration", "22003");
			BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds()).add(BigDecimal.valueOf(duration.getNano(), 9));
			statement.setString(index, seconds.toPlainString() + " seconds");
			return BindingResult.handled();
		}
	}

	/** Only numeric seconds from a fixed-time interval column; never EXTRACT(EPOCH) from arbitrary calendar intervals. */
	static final class DurationSecondsMapper implements CustomColumnMapper {
		@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(Duration.class); }
		@Override public MappingResult map(StatementContext<?> context, ResultSet resultSet, Object value, TargetType target,
				Integer index, String label, InstanceProvider provider) throws SQLException {
			if (!(value instanceof BigDecimal seconds)) throw new SQLException("Duration requires numeric EXTRACT(EPOCH FROM fixed_interval); calendar intervals need separate Period semantics", "22023");
			try {
				BigDecimal[] parts = seconds.divideAndRemainder(BigDecimal.ONE);
				return MappingResult.of(Duration.ofSeconds(parts[0].longValueExact(), parts[1].movePointRight(9).longValueExact()));
			} catch (ArithmeticException exception) {
				throw new SQLException("Numeric seconds cannot be represented exactly as Duration", "22003", exception);
			}
		}
	}
}
