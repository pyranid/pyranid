/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class OracleIntervalIT {
	@Container private static final OracleContainer ORACLE = new OracleContainer(DockerImageName.parse(
			System.getProperty("oracle.integration.image", "gvenzl/oracle-free:23-slim-faststart"))
			.asCompatibleSubstituteFor("gvenzl/oracle-free")).withUsername("pyranid").withPassword("pyranid");

	public record Pair(SqlInterval ym, SqlInterval ds) {}
	public record GraceRow(int id, SqlInterval grace, BigDecimal seconds, int missing) {}
	public record MetadataRow(int id, String note, SqlInterval amount) {}

	@Test
	void bothNativeFamiliesBindZeroAndRetainTheirSupportedPrecision() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached); String table = name();
			database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, ym INTERVAL YEAR(9) TO MONTH, ds INTERVAL DAY(9) TO SECOND(9))").execute();
			try {
				database.transaction(() -> {
					database.query("INSERT INTO " + table + " VALUES (0, :ym, :ds)").bind("ym", SqlInterval.ZERO).bind("ds", SqlInterval.ZERO).execute();
					database.query("INSERT INTO " + table + " VALUES (1, :ym, :ds)")
							.bind("ym", SqlInterval.of(Period.ofMonths(-14))).bind("ds", SqlInterval.of(Duration.ofSeconds(-1, 1))).execute();
					database.query("INSERT INTO " + table + " VALUES (2, NULL, NULL)").execute();
					database.query("INSERT INTO " + table + " VALUES (3, :ym, NULL)")
							.bind("ym", SqlInterval.of(Integer.MIN_VALUE, 0, Duration.ZERO)).execute();
					database.query("INSERT INTO " + table + " VALUES (4, :ym, NULL)")
							.bind("ym", SqlInterval.of(Integer.MAX_VALUE, 0, Duration.ZERO)).execute();
				});
				assertEquals(List.of(new Pair(SqlInterval.ZERO, SqlInterval.ZERO), new Pair(SqlInterval.of(Period.ofMonths(-14)),
						SqlInterval.of(Duration.ofSeconds(-1, 1))), new Pair(null, null), new Pair(SqlInterval.of(Integer.MIN_VALUE, 0, Duration.ZERO), null),
						new Pair(SqlInterval.of(Integer.MAX_VALUE, 0, Duration.ZERO), null)), database.query("SELECT ym, ds FROM " + table + " ORDER BY id").fetchList(Pair.class));
				assertEquals(SqlInterval.ZERO, database.query("SELECT ym FROM " + table + " WHERE id=0").fetchObject(SqlInterval.class).orElseThrow());
				assertEquals(SqlInterval.of(Duration.ofSeconds(-1, 1)), database.query("SELECT ds FROM " + table + " WHERE id=1").fetchObject(SqlInterval.class).orElseThrow());
				assertTrue(database.query("SELECT ds FROM " + table + " WHERE id=2").fetchObject(SqlInterval.class).isEmpty());
				for (int id = 3; id <= 4; id++) {
					int expected = id == 3 ? Integer.MIN_VALUE : Integer.MAX_VALUE;
					assertEquals(expected, database.query("SELECT EXTRACT(YEAR FROM ym)*12 + EXTRACT(MONTH FROM ym) FROM " + table + " WHERE id=:id")
							.bind("id", id).fetchObject(BigDecimal.class).orElseThrow().intValueExact());
				}
				assertThrows(DatabaseException.class, () -> database.query("INSERT INTO " + table + " (id, ds) VALUES (99, :amount)")
						.bind("amount", SqlInterval.of(Period.ofDays(1))).execute());
				assertThrows(DatabaseException.class, () -> database.query("INSERT INTO " + table + " (id, ds) VALUES (99, :amount)")
						.bind("amount", SqlInterval.of(Period.ofMonths(1), Duration.ofHours(1))).execute());
			} finally { database.query("DROP TABLE " + table).execute(); }
		}
	}

	@Test
	void defaultColumnPrecisionAndLeadingYearRangeDoNotBecomeAdapterGuarantees() {
		Database database = database(true); String table = name();
		database.query("CREATE TABLE " + table + " (amount INTERVAL DAY TO SECOND)").execute();
		try {
			database.query("INSERT INTO " + table + " VALUES (:amount)").bind("amount", SqlInterval.of(Duration.ofNanos(1_234_567_891))).execute();
			assertEquals(SqlInterval.of(Duration.ofNanos(1_234_568_000)), database.query("SELECT amount FROM " + table).fetchObject(SqlInterval.class).orElseThrow());
			assertThrows(DatabaseException.class, () -> database.query("INSERT INTO " + table + " VALUES (:amount)")
					.bind("amount", SqlInterval.of(Duration.ofDays(100))).execute());
			assertThrows(DatabaseException.class, () -> database.query("SELECT INTERVAL '999999999-11' YEAR(9) TO MONTH FROM dual").fetchObject(SqlInterval.class));
			assertThrows(DatabaseException.class, () -> database.query("SELECT 'P1D' FROM dual").fetchObject(SqlInterval.class));
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void elapsedFractionSignsPreserveActualServerFieldsAcrossSecondAndDayBoundaries() {
		Database database = database(true); String table = name();
		database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, amount INTERVAL DAY(9) TO SECOND(9))").execute();
		try {
			database.transaction(() -> {
				List<Duration> values = List.of(Duration.ofNanos(1), Duration.ofNanos(-1), Duration.ofNanos(123_456_789),
						Duration.ofNanos(-123_456_789), Duration.ofNanos(-999_999_999), Duration.ofSeconds(-2, 999_999_999),
						Duration.ofHours(-27).minusNanos(1), Duration.ofHours(-27).plusNanos(123_456_789),
						Duration.ofHours(27).plusNanos(123_456_789));
				for (int id = 0; id < values.size(); id++) database.query("INSERT INTO " + table + " VALUES (:id, :amount)")
						.bind("id", id).bind("amount", SqlInterval.of(values.get(id))).execute();
				for (int id = 0; id < values.size(); id++) {
					Duration expected = values.get(id);
					assertEquals(SqlInterval.of(expected), database.query("SELECT amount FROM " + table + " WHERE id=:id")
							.bind("id", id).fetchObject(SqlInterval.class).orElseThrow());
					BigDecimal seconds = BigDecimal.valueOf(expected.getSeconds()).add(BigDecimal.valueOf(expected.getNano(), 9));
					BigDecimal server = database.query("SELECT EXTRACT(DAY FROM amount)*86400 + EXTRACT(HOUR FROM amount)*3600"
							+ " + EXTRACT(MINUTE FROM amount)*60 + EXTRACT(SECOND FROM amount) FROM " + table + " WHERE id=:id")
							.bind("id", id).fetchObject(BigDecimal.class).orElseThrow();
					assertEquals(0, seconds.compareTo(server), "Actual Oracle fields must match, not merely a symmetric Java round trip");
					String rendered = database.query("SELECT TO_CHAR(amount) FROM " + table + " WHERE id=:id")
							.bind("id", id).fetchObject(String.class).orElseThrow();
					assertEquals(expected.isNegative(), rendered.trim().startsWith("-"), rendered);
				}
			});
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void existingNativePassThroughStillAcceptsDurationAndPeriod() {
		Database database = database(false); String table = name();
		database.query("CREATE TABLE " + table + " (ym INTERVAL YEAR(9) TO MONTH, ds INTERVAL DAY(9) TO SECOND(9))").execute();
		try {
			database.query("INSERT INTO " + table + " VALUES (:ym, :ds)").bind("ym", Period.ofMonths(14))
					.bind("ds", Duration.ofNanos(1_234_567_891)).execute();
			assertEquals(new Pair(SqlInterval.of(Period.ofMonths(14)), SqlInterval.of(Duration.ofNanos(1_234_567_891))),
					database.query("SELECT ym, ds FROM " + table).fetchObject(Pair.class).orElseThrow());
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void expressionBatchesUseExplicitIntervalNullsAndKeepNonzeroValuesExactAcrossChunksAndReuse() {
		SqlInterval fraction = SqlInterval.of(Duration.ofNanos(-123_456_789));
		SqlInterval hour = SqlInterval.of(Duration.ofHours(2));
		SqlInterval dayFraction = SqlInterval.of(Duration.ofHours(-27).plusNanos(123_456_789));
		List<List<SqlInterval>> sequences = List.of(Arrays.asList(null, fraction, null, hour, dayFraction),
				Arrays.asList(hour, null, dayFraction, fraction, null));
		TypedParameter intervalNull = new TypedParameter() {
			@Override public Type getExplicitType() { return SqlInterval.class; }
			@Override public Optional<Object> getValue() { return Optional.empty(); }
		};
		for (boolean cached : new boolean[]{false, true}) {
			AtomicInteger nativeNulls = new AtomicInteger(), timestampHints = new AtomicInteger();
			CustomParameterBinder daySecondNull = new CustomParameterBinder() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(SqlInterval.class); }
				@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index,
						Object parameter) { return BindingResult.fallback(); }
				@Override public BindingResult bindNull(StatementContext<?> context, PreparedStatement statement, Integer index,
						TargetType target, Integer sqlType) throws SQLException {
					if (sqlType == -101) timestampHints.incrementAndGet(); // Oracle TIMESTAMP WITH TIME ZONE
					nativeNulls.incrementAndGet();
					statement.setNull(index, -104); // Explicit Oracle INTERVAL DAY TO SECOND, not a guessed default
					return BindingResult.handled();
				}
			};
			Database database = Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(ORACLE.getJdbcUrl(),
					ORACLE.getUsername(), ORACLE.getPassword()), DatabaseType.ORACLE)
					.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(daySecondNull)))
					.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
			String table = name();
			database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, captured TIMESTAMP(9) WITH TIME ZONE,"
					+ " due TIMESTAMP(9) WITH TIME ZONE)").execute();
			try {
				database.transaction(() -> {
					for (int id = 0; id < 5; id++) database.query("INSERT INTO " + table + " (id) VALUES (:id)").bind("id", id).execute();
				});
				String update = "UPDATE " + table + " SET captured = SYSTIMESTAMP, due = SYSTIMESTAMP + :grace WHERE id = :id";
				String fields = "SELECT id, grace, EXTRACT(DAY FROM grace)*86400 + EXTRACT(HOUR FROM grace)*3600"
						+ " + EXTRACT(MINUTE FROM grace)*60 + EXTRACT(SECOND FROM grace) AS seconds, missing"
						+ " FROM (SELECT id, due - captured AS grace, CASE WHEN due IS NULL THEN 1 ELSE 0 END AS missing FROM " + table + ") ORDER BY id";
				for (Integer chunk : Arrays.asList(null, 1, 2)) {
					Query batch = database.query(update).batchChunkSize(chunk);
					for (List<SqlInterval> values : sequences) {
						List<Map<String, Object>> groups = new ArrayList<>();
						for (int id = 0; id < values.size(); id++) {
							Map<String, Object> group = new HashMap<>();
							group.put("id", id); group.put("grace", values.get(id) == null ? intervalNull : values.get(id)); groups.add(group);
						}
						String label = "cached=" + cached + ", chunk=" + chunk + ", values=" + values;
						// Oracle describes :grace as TIMESTAMP WITH TIME ZONE even though the callback
						// explicitly binds native interval NULL. Cached expression metadata must not
						// reject subsequent nonzero values, which retain built-in exact binding.
						assertEquals(values.size(), batch.executeBatch(groups).size(), label);
						database.transaction(() -> {
							assertEquals(0L, database.query("SELECT COUNT(*) FROM " + table + " WHERE captured IS NULL")
									.fetchObject(Long.class).orElseThrow(), label);
							List<GraceRow> actual = database.query(fields).fetchList(GraceRow.class);
							assertEquals(values.size(), actual.size(), label);
							for (int id = 0; id < values.size(); id++) {
								SqlInterval expected = values.get(id); GraceRow row = actual.get(id);
								assertEquals(id, row.id(), label); assertEquals(expected, row.grace(), label);
								assertEquals(expected == null ? 1 : 0, row.missing(), label);
								if (expected == null) assertNull(row.seconds(), label);
								else {
									Duration time = expected.getTime();
									BigDecimal seconds = BigDecimal.valueOf(time.getSeconds()).add(BigDecimal.valueOf(time.getNano(), 9));
									assertEquals(0, seconds.compareTo(row.seconds()), label);
								}
							}
						});
					}
				}
				assertEquals(12, nativeNulls.get());
				assertTrue(timestampHints.get() > 0, "The regression must encounter cached Oracle timestamp metadata");
			} finally { database.query("DROP TABLE " + table).execute(); }
		}
	}

	@Test
	void untypedNullInTimestampAdditionRetainsTheExistingNativeDurationFailure() {
		Database database = database(false); String table = name();
		database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, due TIMESTAMP(9) WITH TIME ZONE)").execute();
		try {
			database.query("INSERT INTO " + table + " (id) VALUES (0)").execute();
			database.query("INSERT INTO " + table + " (id) VALUES (1)").execute();
			String update = "UPDATE " + table + " SET due = SYSTIMESTAMP + :grace WHERE id = :id";
			assertEquals(2, database.query(update).executeBatch(List.of(Map.of("id", 0, "grace", Duration.ofHours(1)),
					Map.of("id", 1, "grace", Duration.ofHours(2)))).size());
			Map<String, Object> nullRow = new HashMap<>(); nullRow.put("id", 0); nullRow.put("grace", null);
			// Also reproduced on released 4.8.0: Oracle's datetime metadata types an untyped NULL
			// as a second datetime operand. This is separate from SqlInterval's nonzero-family guard.
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query(update).executeBatch(
					List.of(nullRow, Map.of("id", 1, "grace", Duration.ofHours(1)))));
			assertEquals(30087, failure.getErrorCode().orElseThrow());
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void zeroFailsWithoutFamilyMetadataWhileNonzeroBindsAndAZeroOnlyBinderSelectsTheFamily() {
		Database database = database(false); String table = name();
		database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, ds INTERVAL DAY(9) TO SECOND(9))").execute();
		try {
			String expression = "SELECT COUNT(*) FROM dual WHERE SYSTIMESTAMP + :amount > SYSTIMESTAMP - 1";
			String merge = "MERGE INTO " + table + " t USING (SELECT :id id, :amount amount FROM dual) s ON (t.id = s.id) "
					+ "WHEN MATCHED THEN UPDATE SET t.ds = s.amount WHEN NOT MATCHED THEN INSERT (id, ds) VALUES (s.id, s.amount)";
			// Oracle reports no interval family for these parameters, so zero fails while nonzero values bind.
			// The documented limitation is pinned here so a driver metadata change is noticed.
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query(expression)
					.bind("amount", SqlInterval.ZERO).fetchObject(Long.class));
			assertTrue(failure.getMessage().contains("SqlInterval.ZERO"), failure.getMessage());
			assertThrows(DatabaseException.class, () -> database.query(merge).bind("id", 0).bind("amount", SqlInterval.ZERO).execute());
			assertEquals(1L, database.query(expression).bind("amount", SqlInterval.of(Duration.ofSeconds(1)))
					.fetchObject(Long.class).orElseThrow());
			database.query(merge).bind("id", 1).bind("amount", SqlInterval.of(Duration.ofSeconds(1))).execute();

			// The documented escape hatch handles only ZERO; nonzero values keep the built-in exact binding.
			CustomParameterBinder daySecondZero = new CustomParameterBinder() {
				@Override public Boolean appliesTo(TargetType targetType) { return targetType.matchesClass(SqlInterval.class); }
				@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index,
						Object parameter) throws SQLException {
					if (!SqlInterval.ZERO.equals(parameter)) return BindingResult.fallback();
					statement.setObject(index, Duration.ZERO);
					return BindingResult.handled();
				}
			};
			Database custom = Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(ORACLE.getJdbcUrl(),
					ORACLE.getUsername(), ORACLE.getPassword()), DatabaseType.ORACLE)
					.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(daySecondZero))).build();
			assertEquals(1L, custom.query(expression).bind("amount", SqlInterval.ZERO).fetchObject(Long.class).orElseThrow());
			custom.query(merge).bind("id", 2).bind("amount", SqlInterval.of(Duration.between(java.time.Instant.EPOCH, java.time.Instant.EPOCH))).execute();
			custom.query(merge).bind("id", 3).bind("amount", SqlInterval.of(Duration.ofMillis(-1500))).execute();
			assertEquals(List.of(SqlInterval.of(Duration.ofSeconds(1)), SqlInterval.ZERO, SqlInterval.of(Duration.ofMillis(-1500))),
					database.query("SELECT ds FROM " + table + " ORDER BY id").fetchList(SqlInterval.class));
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void explicitIntervalCastsExposeZeroFamiliesAcrossExpressionsSelectsAndMergeShapes() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached); String table = name();
			database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, ym INTERVAL YEAR(9) TO MONTH, "
					+ "ds INTERVAL DAY(9) TO SECOND(9), due TIMESTAMP(9))").execute();
			String ym = "CAST(:ym AS INTERVAL YEAR(9) TO MONTH)";
			String ds = "CAST(:ds AS INTERVAL DAY(9) TO SECOND(9))";
			try {
				database.transaction(() -> {
					assertEquals(new Pair(SqlInterval.ZERO, SqlInterval.ZERO), database.query("SELECT " + ym + " AS ym, " + ds + " AS ds FROM dual")
							.bind("ym", SqlInterval.ZERO).bind("ds", SqlInterval.ZERO).fetchObject(Pair.class).orElseThrow());
					assertEquals(1L, database.query("SELECT COUNT(*) FROM dual WHERE TIMESTAMP '2026-01-01 00:00:00' + " + ym
							+ " = TIMESTAMP '2026-01-01 00:00:00' AND TIMESTAMP '2026-01-01 00:00:00' + " + ds
							+ " = TIMESTAMP '2026-01-01 00:00:00'")
							.bind("ym", SqlInterval.ZERO).bind("ds", SqlInterval.ZERO).fetchObject(Long.class).orElseThrow());
					assertEquals(1L, database.query("INSERT INTO " + table + " (id,ym,ds) SELECT 1," + ym + "," + ds + " FROM dual")
							.bind("ym", SqlInterval.ZERO).bind("ds", SqlInterval.ZERO).execute());
					String mergeUsing = "MERGE INTO " + table + " t USING (SELECT 2 id," + ym + " ym," + ds
							+ " ds FROM dual) s ON (t.id=s.id) WHEN NOT MATCHED THEN INSERT (id,ym,ds) VALUES (s.id,s.ym,s.ds)";
					assertEquals(1L, database.query(mergeUsing).bind("ym", SqlInterval.ZERO).bind("ds", SqlInterval.ZERO).execute());
					String mergeUpdate = "MERGE INTO " + table + " t USING (SELECT 2 id FROM dual) s ON (t.id=s.id) "
							+ "WHEN MATCHED THEN UPDATE SET t.ym=" + ym + ",t.ds=" + ds;
					assertEquals(1L, database.query(mergeUpdate).bind("ym", SqlInterval.ZERO).bind("ds", SqlInterval.ZERO).execute());
					database.query("INSERT INTO " + table + " (id) VALUES (3)").execute();
					assertEquals(1L, database.query("UPDATE " + table + " SET ym=COALESCE(ym," + ym + "),ds=COALESCE(ds,"
							+ ds + "),due=TIMESTAMP '2026-01-01 00:00:00' + " + ds + " WHERE id=3")
							.bind("ym", SqlInterval.ZERO).bind("ds", SqlInterval.ZERO).execute());
					assertEquals(List.of(new Pair(SqlInterval.ZERO, SqlInterval.ZERO), new Pair(SqlInterval.ZERO, SqlInterval.ZERO),
							new Pair(SqlInterval.ZERO, SqlInterval.ZERO)), database.query("SELECT ym,ds FROM " + table + " ORDER BY id").fetchList(Pair.class));
					assertEquals("2026-01-01 00:00:00.000000000", database.query("SELECT TO_CHAR(due,'YYYY-MM-DD HH24:MI:SS.FF9') FROM "
							+ table + " WHERE id=3").fetchObject(String.class).orElseThrow());
					// Selecting the DAY-SECOND bind family is not a conversion into a YEAR-MONTH column.
					DatabaseException wrongFamily = assertThrows(DatabaseException.class,
							() -> database.query("INSERT INTO " + table + " (id,ym) SELECT 99," + ds + " FROM dual")
									.bind("ds", SqlInterval.ZERO).execute());
					assertEquals(932, wrongFamily.getErrorCode().orElseThrow());
					assertEquals(0L, database.query("SELECT COUNT(*) FROM " + table + " WHERE id=99").fetchObject(Long.class).orElseThrow());
				});
			} finally { database.query("DROP TABLE " + table).execute(); }
		}
	}

	@Test
	void insertSelectParserMetadataFailureFallsBackForNullsAndKeepsZeroAmbiguous() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached); String table = name();
			database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, note VARCHAR2(30), amount INTERVAL DAY(9) TO SECOND(9))").execute();
			String insert = "INSERT INTO " + table + " (id, note, amount) SELECT :id, :note, :amount FROM dual";
			SqlInterval fraction = SqlInterval.of(Duration.ofNanos(-123_456_789));
			try {
				// ojdbc's descriptor is acquired successfully, but its first type getter throws
				// the known internal parser NPE. NULL retains the ordinary Types.NULL fallback.
				assertEquals(1L, database.query(insert).bind("id", 0).bind("note", null).bind("amount", null).execute());
				assertEquals(1L, database.query(insert).bind("id", 1).bind("note", null).bind("amount", fraction).execute());
				List<Map<String, Object>> groups = new ArrayList<>();
				for (int id = 2; id <= 4; id++) {
					Map<String, Object> group = new HashMap<>();
					group.put("id", id); group.put("note", null); group.put("amount", id == 3 ? fraction : null); groups.add(group);
				}
				assertEquals(3, database.query(insert).batchChunkSize(1).executeBatch(groups).size());
				DatabaseException zero = assertThrows(DatabaseException.class, () -> database.query(insert)
						.bind("id", 99).bind("note", "ambiguous zero").bind("amount", SqlInterval.ZERO).execute());
				assertTrue(zero.getMessage().contains("Cannot choose the SQL interval family for SqlInterval.ZERO"), zero.getMessage());
				for (Throwable cause = zero; cause != null; cause = cause.getCause()) assertFalse(cause instanceof NullPointerException);

				AtomicInteger nativeNulls = new AtomicInteger();
				CustomParameterBinder daySecondNull = new CustomParameterBinder() {
					@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(SqlInterval.class); }
					@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index,
							Object parameter) { return BindingResult.fallback(); }
					@Override public BindingResult bindNull(StatementContext<?> context, PreparedStatement statement, Integer index,
							TargetType target, Integer sqlType) throws SQLException {
						assertEquals(java.sql.Types.NULL, sqlType);
						nativeNulls.incrementAndGet(); statement.setNull(index, -104);
						return BindingResult.handled();
					}
				};
				TypedParameter intervalNull = new TypedParameter() {
					@Override public Type getExplicitType() { return SqlInterval.class; }
					@Override public Optional<Object> getValue() { return Optional.empty(); }
				};
				Database custom = Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(ORACLE.getJdbcUrl(),
						ORACLE.getUsername(), ORACLE.getPassword()), DatabaseType.ORACLE)
						.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(daySecondNull))).build();
				assertEquals(1L, custom.query(insert).bind("id", 5).bind("note", "explicit native null")
						.bind("amount", intervalNull).execute());
				assertEquals(1, nativeNulls.get());
				assertEquals(List.of(new MetadataRow(0, null, null), new MetadataRow(1, null, fraction), new MetadataRow(2, null, null),
						new MetadataRow(3, null, fraction), new MetadataRow(4, null, null), new MetadataRow(5, "explicit native null", null)),
						database.query("SELECT id, note, amount FROM " + table + " ORDER BY id").fetchList(MetadataRow.class));
			} finally { database.query("DROP TABLE " + table).execute(); }
		}
	}

	@Test
	void repeatedInsertSelectNullBindingSurvivesWarmDriverParserFailuresAndKeepsZeroAmbiguous() {
		Database database = database(true); String table = name();
		database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, amount INTERVAL DAY(9) TO SECOND(9))").execute();
		String insert = "INSERT INTO " + table + " (id, amount) SELECT :id, :amount FROM dual";
		int iterations = 12_000;
		try {
			database.transaction(() -> {
				for (int id = 0; id < iterations; id++) {
					// Repeated failures at ojdbc's parser site become stackless fast-throw NPEs in
					// a warm HotSpot JVM. The ordinary NULL fallback must survive that transition.
					assertEquals(1L, database.query(insert).bind("id", id).bind("amount", null).execute(), "id=" + id);
				}
				DatabaseException zero = assertThrows(DatabaseException.class,
						() -> database.query(insert).bind("id", -1).bind("amount", SqlInterval.ZERO).execute());
				assertTrue(zero.getMessage().contains("Cannot choose the SQL interval family for SqlInterval.ZERO"), zero.getMessage());
				for (Throwable cause = zero; cause != null; cause = cause.getCause())
					assertFalse(cause instanceof NullPointerException, "A warm parser failure must retain the interval-family error");
			});
			assertEquals((long) iterations, database.query("SELECT COUNT(*) FROM " + table + " WHERE amount IS NULL")
					.fetchObject(Long.class).orElseThrow());
			assertEquals(0L, database.query("SELECT COUNT(*) FROM " + table + " WHERE id=-1").fetchObject(Long.class).orElseThrow());
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	private static Database database(boolean cached) {
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(ORACLE.getJdbcUrl(),
				ORACLE.getUsername(), ORACLE.getPassword()), DatabaseType.ORACLE)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
	private static String name() { return "interval_" + UUID.randomUUID().toString().replace("-", ""); }
}
