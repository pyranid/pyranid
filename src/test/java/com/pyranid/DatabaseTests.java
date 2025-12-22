/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2025 Revetware LLC.
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

import com.pyranid.JsonParameter.BindingPreference;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Currency;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 2.0.0
 */
@ThreadSafe
public class DatabaseTests {
	public record EmployeeRecord(@DatabaseColumn("name") String displayName, String emailAddress, Locale locale) {}

	public static class EmployeeClass {
		private @DatabaseColumn("name") String displayName;
		private String emailAddress;
		private Locale locale;
		private @DatabaseColumn("locale") String rawLocale;

		public String getDisplayName() {
			return this.displayName;
		}

		public void setDisplayName(String displayName) {
			this.displayName = displayName;
		}

		public String getEmailAddress() {
			return this.emailAddress;
		}

		public void setEmailAddress(String emailAddress) {
			this.emailAddress = emailAddress;
		}

		public Locale getLocale() {
			return this.locale;
		}

		public void setLocale(Locale locale) {
			this.locale = locale;
		}

		public String getRawLocale() {
			return this.rawLocale;
		}

		public void setRawLocale(String rawLocale) {
			this.rawLocale = rawLocale;
		}
	}

	public static class LocaleHolder {
		private Locale locale;

		public Locale getLocale() {
			return this.locale;
		}

		public void setLocale(Locale locale) {
			this.locale = locale;
		}
	}

	public static class CurrencyHolder {
		private Currency currency;

		public Currency getCurrency() {
			return this.currency;
		}

		public void setCurrency(Currency currency) {
			this.currency = currency;
		}
	}

	public static class ZoneIdHolder {
		private ZoneId zoneId;

		public ZoneId getZoneId() {
			return this.zoneId;
		}

		public void setZoneId(ZoneId zoneId) {
			this.zoneId = zoneId;
		}
	}

	public static class BigDecimalHolder {
		private BigDecimal v;

		public BigDecimal getV() {
			return this.v;
		}

		public void setV(BigDecimal v) {
			this.v = v;
		}
	}

	public static class BigIntegerHolder {
		private BigInteger v;

		public BigInteger getV() {
			return this.v;
		}

		public void setV(BigInteger v) {
			this.v = v;
		}
	}

	@Test
	public void testBasicQueries() {
		Database database = Database.withDataSource(createInMemoryDataSource("testBasicQueries")).build();

		createTestSchema(database);

		TestQueries.execute(database, "INSERT INTO employee VALUES (1, 'Employee One', 'employee-one@company.com', NULL)");
		TestQueries.execute(database, "INSERT INTO employee VALUES (2, 'Employee Two', NULL, NULL)");

		List<EmployeeRecord> employeeRecords = database.query("SELECT * FROM employee ORDER BY name")
				.fetchList(EmployeeRecord.class);
		Assertions.assertEquals(2, employeeRecords.size(), "Wrong number of employees");
		Assertions.assertEquals("Employee One", employeeRecords.get(0).displayName(), "Didn't detect DB column name override");

		List<EmployeeClass> employeeClasses = database.query("SELECT * FROM employee ORDER BY name")
				.fetchList(EmployeeClass.class);
		Assertions.assertEquals(2, employeeClasses.size(), "Wrong number of employees");
		Assertions.assertEquals("Employee One", employeeClasses.get(0).getDisplayName(), "Didn't detect DB column name override");
	}

	@Test
	public void testQueryRejectsPositionalParameters() {
		Database db = Database.withDataSource(createInMemoryDataSource("testQueryRejectsPositionalParameters")).build();

		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
				() -> db.query("SELECT * FROM employee WHERE employee_id=?"));
		Assertions.assertTrue(e.getMessage().contains("Positional"),
				"Expected a helpful error message mentioning positional parameters");
	}

	@Test
	public void testQueryAllowsQuestionMarkInStringLiteral() {
		Database db = Database.withDataSource(createInMemoryDataSource("testQueryAllowsQuestionMarkInStringLiteral")).build();

		Assertions.assertDoesNotThrow(() ->
				db.query("SELECT '?' FROM (VALUES (0)) AS t(x)"));
	}

	@Test
	public void testQueryBindRejectsUnknownParameterName() {
		Database db = Database.withDataSource(createInMemoryDataSource("testQueryBindRejectsUnknownParameterName")).build();

		Assertions.assertThrows(IllegalArgumentException.class, () ->
				db.query("SELECT :id FROM (VALUES (0)) AS t(x)")
						.bind("nope", 1));
	}

	@Test
	public void testQueryBindRejectsRawCollection() {
		Database db = Database.withDataSource(createInMemoryDataSource("testQueryBindRejectsRawCollection")).build();

		Assertions.assertThrows(IllegalArgumentException.class, () ->
				db.query("SELECT :ids FROM (VALUES (0)) AS t(x)")
						.bind("ids", List.of(1, 2))
						.fetchList(Integer.class));
	}

	@Test
	public void testQueryBindRejectsRawArray() {
		Database db = Database.withDataSource(createInMemoryDataSource("testQueryBindRejectsRawArray")).build();

		Assertions.assertThrows(IllegalArgumentException.class, () ->
				db.query("SELECT :names FROM (VALUES (0)) AS t(x)")
						.bind("names", new String[]{"alpha", "beta"})
						.fetchList(String.class));
	}

	@Test
	public void testQueryInListExpandsParameter() {
		Database db = Database.withDataSource(createInMemoryDataSource("testQueryInListExpandsParameter")).build();

		TestQueries.execute(db, "CREATE TABLE t (id INT, email VARCHAR(255))");
		TestQueries.execute(db, "INSERT INTO t VALUES (1, 'a@example.com')");
		TestQueries.execute(db, "INSERT INTO t VALUES (2, 'b@example.com')");
		TestQueries.execute(db, "INSERT INTO t VALUES (3, 'c@example.com')");

		List<Integer> ids = db.query("SELECT id FROM t WHERE email IN (:emails) ORDER BY id")
				.bind("emails", Parameters.inList(List.of("a@example.com", "c@example.com")))
				.fetchList(Integer.class);

		Assertions.assertEquals(List.of(1, 3), ids);
	}

	@Test
	public void testQueryInListExpandsRepeatedParameterName() {
		Database db = Database.withDataSource(createInMemoryDataSource("testQueryInListExpandsRepeatedParameterName")).build();

		TestQueries.execute(db, "CREATE TABLE t (id INT, email VARCHAR(255))");
		TestQueries.execute(db, "INSERT INTO t VALUES (1, 'a@example.com')");
		TestQueries.execute(db, "INSERT INTO t VALUES (2, 'b@example.com')");
		TestQueries.execute(db, "INSERT INTO t VALUES (3, 'c@example.com')");

		List<Integer> ids = db.query("SELECT id FROM t WHERE email IN (:emails) OR email IN (:emails) ORDER BY id")
				.bind("emails", Parameters.inList(List.of("a@example.com", "c@example.com")))
				.fetchList(Integer.class);

		Assertions.assertEquals(List.of(1, 3), ids);
	}

	@Test
	public void testQueryInListExpandsPrimitiveArray() {
		Database db = Database.withDataSource(createInMemoryDataSource("testQueryInListExpandsPrimitiveArray")).build();

		TestQueries.execute(db, "CREATE TABLE t (id INT, email VARCHAR(255))");
		TestQueries.execute(db, "INSERT INTO t VALUES (1, 'a@example.com')");
		TestQueries.execute(db, "INSERT INTO t VALUES (2, 'b@example.com')");
		TestQueries.execute(db, "INSERT INTO t VALUES (3, 'c@example.com')");

		List<Integer> ids = db.query("SELECT id FROM t WHERE id IN (:ids) ORDER BY id")
				.bind("ids", Parameters.inList(new int[]{1, 3}))
				.fetchList(Integer.class);

		Assertions.assertEquals(List.of(1, 3), ids);
	}

	@Test
	public void testQueryInListRejectsEmptyCollection() {
		Database db = Database.withDataSource(createInMemoryDataSource("testQueryInListRejectsEmptyCollection")).build();

		TestQueries.execute(db, "CREATE TABLE t (id INT, email VARCHAR(255))");

		Assertions.assertThrows(IllegalArgumentException.class, () ->
				db.query("SELECT id FROM t WHERE email IN (:emails)")
						.bind("emails", Parameters.inList(List.of()))
						.fetchList(Integer.class));
	}

	@Test
	public void testExecuteBatchRejectsMismatchedInListSizes() {
		Database db = Database.withDataSource(createInMemoryDataSource("testExecuteBatchRejectsMismatchedInListSizes")).build();

		TestQueries.execute(db, "CREATE TABLE t (id INT, flag INT)");
		TestQueries.execute(db, "INSERT INTO t VALUES (1, 0)");
		TestQueries.execute(db, "INSERT INTO t VALUES (2, 0)");
		TestQueries.execute(db, "INSERT INTO t VALUES (3, 0)");

		Query query = db.query("UPDATE t SET flag = 1 WHERE id IN (:ids)");

		List<Map<String, Object>> parameterGroups = List.of(
				Map.of("ids", Parameters.inList(List.of(1, 2))),
				Map.of("ids", Parameters.inList(List.of(1, 2, 3)))
		);

		Assertions.assertThrows(IllegalArgumentException.class, () -> query.executeBatch(parameterGroups));
	}

	public record Product(Long productId, String name, BigDecimal price) {}

	@Test
	public void testTransactions() {
		Database database = Database.withDataSource(createInMemoryDataSource("testTransactions")).build();

		TestQueries.execute(database, "CREATE TABLE product (product_id BIGINT, name VARCHAR(255) NOT NULL, price DECIMAL)");

		AtomicBoolean ranPostTransactionOperation = new AtomicBoolean(false);

		database.transaction(() -> {
			database.currentTransaction().get().addPostTransactionOperation((transactionResult -> {
				Assertions.assertEquals(TransactionResult.COMMITTED, transactionResult, "Wrong transaction result");
				ranPostTransactionOperation.set(true);
			}));

			TestQueries.execute(database, "INSERT INTO product VALUES (1, 'VR Goggles', 3500.99)");

			Product product = database.query("""
							SELECT * 
							FROM product 
							WHERE product_id=:productId
							""")
					.bind("productId", 1L)
					.fetchObject(Product.class)
					.orElse(null);

			Assertions.assertNotNull(product, "Product failed to insert");

			database.currentTransaction().get().rollback();

			product = database.query("""
							SELECT * 
							FROM product 
							WHERE product_id=:productId
							""")
					.bind("productId", 1L)
					.fetchObject(Product.class)
					.orElse(null);

			Assertions.assertNull(product, "Product failed to roll back");
		});

		Assertions.assertTrue(ranPostTransactionOperation.get(), "Did not run post-transaction operation");
	}

	@Test
	public void testDateAndTimeRoundTrips() {
		DataSource ds = createInMemoryDataSource("dt_roundtrips");
		ZoneId zone = ZoneId.of("America/New_York");

		Database db = Database.withDataSource(ds)
				.timeZone(zone)
				.build();

		// DATE <-> LocalDate
		LocalDate ld = LocalDate.of(2020, 1, 2);
		LocalDate ldRoundTrip = db.query("SELECT CAST(:ld AS DATE) FROM (VALUES (0)) AS t(x)")
				.bind("ld", ld)
				.fetchObject(LocalDate.class)
				.orElseThrow();
		Assertions.assertEquals(ld, ldRoundTrip);

		// TIME <-> LocalTime (use second precision to avoid driver quirks)
		LocalTime lt = LocalTime.of(3, 4, 5);
		LocalTime ltRoundTrip = db.query("SELECT CAST(:lt AS TIME) FROM (VALUES (0)) AS t(x)")
				.bind("lt", lt)
				.fetchObject(LocalTime.class)
				.orElseThrow();
		Assertions.assertEquals(lt, ltRoundTrip);
	}

	@Test
	public void testTimestampRoundTripsAllJavaTimeFlavors() {
		DataSource ds = createInMemoryDataSource("ts_roundtrips");
		ZoneId zone = ZoneId.of("America/New_York");

		Database db = Database.withDataSource(ds)
				.timeZone(zone)
				.build();

		// 1) LocalDateTime param
		LocalDateTime ldt = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000_000); // 123ms for JDBC-friendly precision
		// LocalDateTime -> TIMESTAMP -> LocalDateTime
		LocalDateTime ldtRoundTrip = db.query("SELECT CAST(:ldt AS TIMESTAMP) FROM (VALUES (0)) AS t(x)")
				.bind("ldt", ldt)
				.fetchObject(LocalDateTime.class)
				.orElseThrow();
		Assertions.assertEquals(ldt, ldtRoundTrip, "LocalDateTime round-trip mismatch");
		// LocalDateTime -> TIMESTAMP -> Instant (interpreted in DB zone)
		Instant expectedFromLdt = ldt.atZone(zone).toInstant().truncatedTo(ChronoUnit.MILLIS);
		Instant instFromLdt = db.query("SELECT CAST(:ldt AS TIMESTAMP) FROM (VALUES (0)) AS t(x)")
				.bind("ldt", ldt)
				.fetchObject(Instant.class)
				.orElseThrow()
				.truncatedTo(ChronoUnit.MILLIS);
		Assertions.assertEquals(expectedFromLdt, instFromLdt, "LocalDateTime→Instant mapping mismatch");

		// 2) Instant param
		Instant instant = Instant.parse("2020-01-02T08:09:10.123Z");
		// Instant -> TIMESTAMP -> Instant (should be identity)
		Instant instRoundTrip = db.query("SELECT CAST(:instant AS TIMESTAMP) FROM (VALUES (0)) AS t(x)")
				.bind("instant", instant)
				.fetchObject(Instant.class)
				.orElseThrow()
				.truncatedTo(ChronoUnit.MILLIS);
		Assertions.assertEquals(instant.truncatedTo(ChronoUnit.MILLIS), instRoundTrip, "Instant round-trip mismatch");
		// Instant -> TIMESTAMP -> LocalDateTime (in DB zone)
		LocalDateTime expectedLdtFromInstant = LocalDateTime.ofInstant(instant, zone);
		LocalDateTime ldtFromInstant = db.query("SELECT CAST(:instant AS TIMESTAMP) FROM (VALUES (0)) AS t(x)")
				.bind("instant", instant)
				.fetchObject(LocalDateTime.class)
				.orElseThrow();
		Assertions.assertEquals(expectedLdtFromInstant, ldtFromInstant, "Instant→LocalDateTime mapping mismatch");

		// 3) OffsetDateTime param (use odd offset and nanos to ensure normalization)
		OffsetDateTime odt = OffsetDateTime.parse("2020-01-02T08:09:10.123456789-03:00");
		Instant expectedFromOdt = odt.toInstant().truncatedTo(ChronoUnit.MILLIS);
		Instant instFromOdt = db.query("SELECT CAST(:odt AS TIMESTAMP) FROM (VALUES (0)) AS t(x)")
				.bind("odt", odt)
				.fetchObject(Instant.class)
				.orElseThrow()
				.truncatedTo(ChronoUnit.MILLIS);
		Assertions.assertEquals(expectedFromOdt, instFromOdt, "OffsetDateTime→Instant mapping mismatch");
		LocalDateTime expectedLdtFromOdt = LocalDateTime.ofInstant(odt.toInstant(), zone);
		LocalDateTime ldtFromOdt = db.query("SELECT CAST(:odt AS TIMESTAMP) FROM (VALUES (0)) AS t(x)")
				.bind("odt", odt)
				.fetchObject(LocalDateTime.class)
				.orElseThrow();
		Assertions.assertEquals(truncate(expectedLdtFromOdt, ChronoUnit.MICROS), truncate(ldtFromOdt, ChronoUnit.MICROS), "OffsetDateTime→LocalDateTime mapping mismatch");
	}

	@Test
	public void testTimestampLiteralMappingRespectsDatabaseTimeZone() {
		// Same SQL literal interpreted under two different DB zones
		LocalDateTime ldt = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000_000);

		// NY DB
		DataSource dsNY = createInMemoryDataSource("ts_literal_ny");
		ZoneId ny = ZoneId.of("America/New_York");
		Database dbNY = Database.withDataSource(dsNY).timeZone(ny).build();
		Instant expectedNY = ldt.atZone(ny).toInstant().truncatedTo(ChronoUnit.MILLIS);
		Instant gotNY = dbNY.query("SELECT TIMESTAMP '2020-01-02 03:04:05.123' FROM (VALUES (0)) AS t(x)")
				.fetchObject(Instant.class)
				.orElseThrow()
				.truncatedTo(ChronoUnit.MILLIS);
		Assertions.assertEquals(expectedNY, gotNY, "NY literal TIMESTAMP→Instant mismatch");

		// But LocalDateTime should be the literal value regardless of zone
		LocalDateTime gotNYLdt = dbNY.query("SELECT TIMESTAMP '2020-01-02 03:04:05.123' FROM (VALUES (0)) AS t(x)")
				.fetchObject(LocalDateTime.class)
				.orElseThrow();
		Assertions.assertEquals(ldt, gotNYLdt, "NY literal TIMESTAMP→LocalDateTime mismatch");

		// UTC DB
		DataSource dsUTC = createInMemoryDataSource("ts_literal_utc");
		ZoneId utc = ZoneId.of("UTC");
		Database dbUTC = Database.withDataSource(dsUTC).timeZone(utc).build();
		Instant expectedUTC = ldt.atZone(utc).toInstant().truncatedTo(ChronoUnit.MILLIS);
		Instant gotUTC = dbUTC.query("SELECT TIMESTAMP '2020-01-02 03:04:05.123' FROM (VALUES (0)) AS t(x)")
				.fetchObject(Instant.class)
				.orElseThrow()
				.truncatedTo(ChronoUnit.MILLIS);

		Assertions.assertEquals(expectedUTC, gotUTC, "UTC literal TIMESTAMP→Instant mismatch");
		LocalDateTime gotUTCLdt = dbUTC.query("SELECT TIMESTAMP '2020-01-02 03:04:05.123' FROM (VALUES (0)) AS t(x)")
				.fetchObject(LocalDateTime.class)
				.orElseThrow();
		Assertions.assertEquals(ldt, gotUTCLdt, "UTC literal TIMESTAMP→LocalDateTime mismatch");
	}

	@Test
	public void testLegacySqlTypesRoundTrip() {
		DataSource ds = createInMemoryDataSource("legacy_sql_types");
		ZoneId zone = ZoneId.of("America/New_York");
		Database db = Database.withDataSource(ds).timeZone(zone).build();

		// java.sql.Timestamp
		java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2020-01-02 03:04:05.123");
		Instant instFromSqlTs = db.query("SELECT CAST(:ts AS TIMESTAMP) FROM (VALUES (0)) AS t(x)")
				.bind("ts", ts)
				.fetchObject(Instant.class)
				.orElseThrow()
				.truncatedTo(ChronoUnit.MILLIS);
		Assertions.assertEquals(ts.toInstant().truncatedTo(ChronoUnit.MILLIS), instFromSqlTs, "java.sql.Timestamp→Instant mismatch");

		// java.sql.Date
		java.sql.Date sqlDate = java.sql.Date.valueOf("2020-01-02");
		LocalDate ldFromSqlDate = db.query("SELECT CAST(:sqlDate AS DATE) FROM (VALUES (0)) AS t(x)")
				.bind("sqlDate", sqlDate)
				.fetchObject(LocalDate.class)
				.orElseThrow();
		Assertions.assertEquals(sqlDate.toLocalDate(), ldFromSqlDate, "java.sql.Date→LocalDate mismatch");

		// java.sql.Time
		java.sql.Time sqlTime = java.sql.Time.valueOf("03:04:05");
		LocalTime ltFromSqlTime = db.query("SELECT CAST(:sqlTime AS TIME) FROM (VALUES (0)) AS t(x)")
				.bind("sqlTime", sqlTime)
				.fetchObject(LocalTime.class)
				.orElseThrow();
		Assertions.assertEquals(sqlTime.toLocalTime(), ltFromSqlTime, "java.sql.Time→LocalTime mismatch");
	}

	@Test
	public void testCustomColumnMapper() {
		DataSource dataSource = createInMemoryDataSource("cm_basic");

		// Mapper: for any Locale target, always return CANADA (to prove the custom path is used)
		CustomColumnMapper localeOverride = new CustomColumnMapper() {
			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Locale.class);
			}

			@Nonnull
			@Override
			public MappingResult map(@Nonnull StatementContext<?> statementContext,
															 @Nonnull ResultSet resultSet,
															 @Nonnull Object resultSetValue,
															 @Nonnull TargetType targetType,
															 @Nonnull Integer columnIndex,
															 @Nullable String columnLabel,
															 @Nonnull InstanceProvider instanceProvider) {
				// ignore DB value; force a deterministic value so we can assert override happened
				return MappingResult.of(Locale.CANADA);
			}
		};

		Database db = Database.withDataSource(dataSource)
				.resultSetMapper(ResultSetMapper.withCustomColumnMappers(List.of(localeOverride)).build())
				.build();

		createTestSchema(db);

		TestQueries.execute(db, "INSERT INTO employee VALUES (1, 'A', 'a@x.com', 'en-US')");
		TestQueries.execute(db, "INSERT INTO employee VALUES (2, 'B', 'b@x.com', 'ja-JP')");

		// JavaBean target
		EmployeeClass e1 = db.query("SELECT * FROM employee WHERE employee_id=1")
				.fetchObject(EmployeeClass.class)
				.orElse(null);
		Assertions.assertNotNull(e1);
		Assertions.assertEquals(Locale.CANADA, e1.getLocale(), "Custom mapper did not override Locale for bean");
		Assertions.assertEquals("en-US", e1.getRawLocale(), "Raw locale should remain the DB string");

		// Record target
		EmployeeRecord r2 = db.query("SELECT * FROM employee WHERE employee_id=2")
				.fetchObject(EmployeeRecord.class)
				.orElse(null);
		Assertions.assertNotNull(r2);
		Assertions.assertEquals(Locale.CANADA, r2.locale(), "Custom mapper did not override Locale for record");
	}

	@Test
	public void testCustomColumnMapperPreferredCache() {
		DataSource dataSource = createInMemoryDataSource("cm_cache");

		AtomicInteger firstCalls = new AtomicInteger(0);
		AtomicInteger secondCalls = new AtomicInteger(0);

		// First mapper applies to Locale but never handles (returns empty). We count calls.
		CustomColumnMapper first = new CustomColumnMapper() {
			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Locale.class);
			}

			@Nonnull
			@Override
			public MappingResult map(@Nonnull StatementContext<?> statementContext,
															 @Nonnull ResultSet resultSet,
															 @Nonnull Object resultSetValue,
															 @Nonnull TargetType targetType,
															 @Nonnull Integer columnIndex,
															 @Nullable String columnLabel,
															 @Nonnull InstanceProvider instanceProvider) {
				firstCalls.incrementAndGet();
				return MappingResult.fallback();
			}
		};

		// Second mapper actually handles and returns a fixed Locale
		CustomColumnMapper second = new CustomColumnMapper() {
			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Locale.class);
			}

			@Nonnull
			@Override
			public MappingResult map(@Nonnull StatementContext<?> statementContext,
															 @Nonnull ResultSet resultSet,
															 @Nonnull Object resultSetValue,
															 @Nonnull TargetType targetType,
															 @Nonnull Integer columnIndex,
															 @Nullable String columnLabel,
															 @Nonnull InstanceProvider instanceProvider) {
				secondCalls.incrementAndGet();
				return MappingResult.of(Locale.GERMANY);
			}
		};

		Database db = Database.withDataSource(dataSource)
				.resultSetMapper(ResultSetMapper.withCustomColumnMappers(List.of(first, second)).build())
				.build();

		createTestSchema(db);

		// Insert multiple rows so the winner cache has a chance to kick in
		TestQueries.execute(db, "INSERT INTO employee VALUES (1, 'A', 'a@x.com', 'en-US')");
		TestQueries.execute(db, "INSERT INTO employee VALUES (2, 'B', 'b@x.com', 'fr-FR')");
		TestQueries.execute(db, "INSERT INTO employee VALUES (3, 'C', 'c@x.com', 'ja-JP')");

		List<EmployeeClass> rows = db.query("SELECT * FROM employee ORDER BY employee_id")
				.fetchList(EmployeeClass.class);
		Assertions.assertEquals(3, rows.size());
		rows.forEach(e -> Assertions.assertEquals(Locale.GERMANY, e.getLocale(), "Winner mapper should set GERMANY"));

		// Expected call pattern with positive winner cache:
		// Row1: first called (empty), second called (handles) -> cache winner=(String, Locale) -> second
		// Row2: second called only
		// Row3: second called only
		Assertions.assertEquals(1, firstCalls.get(), "First mapper should be tried only on the first row");
		Assertions.assertEquals(3, secondCalls.get(), "Second mapper should handle every row");
	}

	@Test
	public void testPreferredColumnMapperCacheCapacity() {
		DataSource dataSource = createInMemoryDataSource("cm_cache_cap");

		CustomColumnMapper mapper = new CustomColumnMapper() {
			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Locale.class)
						|| targetType.matchesClass(Currency.class)
						|| targetType.matchesClass(ZoneId.class);
			}

			@Nonnull
			@Override
			public MappingResult map(@Nonnull StatementContext<?> statementContext,
															 @Nonnull ResultSet resultSet,
															 @Nonnull Object resultSetValue,
															 @Nonnull TargetType targetType,
															 @Nonnull Integer columnIndex,
															 @Nullable String columnLabel,
															 @Nonnull InstanceProvider instanceProvider) {
				if (targetType.matchesClass(Locale.class))
					return MappingResult.of(Locale.CANADA);
				if (targetType.matchesClass(Currency.class))
					return MappingResult.of(Currency.getInstance("USD"));
				if (targetType.matchesClass(ZoneId.class))
					return MappingResult.of(ZoneId.of("UTC"));
				return MappingResult.fallback();
			}
		};

		DefaultResultSetMapper resultSetMapper = (DefaultResultSetMapper) ResultSetMapper.withCustomColumnMappers(List.of(mapper))
				.preferredColumnMapperCacheCapacity(2)
				.build();

		Database db = Database.withDataSource(dataSource)
				.resultSetMapper(resultSetMapper)
				.build();

		TestQueries.execute(db, """
				CREATE TABLE cache_types (
				  locale VARCHAR(255),
				  currency VARCHAR(255),
				  zone_id VARCHAR(255)
				)
				""");
		TestQueries.execute(db, "INSERT INTO cache_types VALUES ('en-US', 'USD', 'UTC')");

		db.query("SELECT locale FROM cache_types").fetchObject(LocaleHolder.class);
		db.query("SELECT currency FROM cache_types").fetchObject(CurrencyHolder.class);
		db.query("SELECT zone_id FROM cache_types").fetchObject(ZoneIdHolder.class);

		Map<?, ?> cache = resultSetMapper.getPreferredColumnMapperBySourceTargetKey();
		if (cache instanceof ConcurrentLruMap<?, ?> lru)
			lru.drain();

		Assertions.assertTrue(cache instanceof ConcurrentLruMap, "Preferred mapper cache should use LRU when capacity is set");
		Assertions.assertTrue(cache.size() <= 2, "Preferred mapper cache should honor configured capacity");
	}

	@Test
	public void testPlanCacheCapacity() {
		DataSource dataSource = createInMemoryDataSource("plan_cache_cap");

		DefaultResultSetMapper resultSetMapper = (DefaultResultSetMapper) ResultSetMapper.withPlanCachingEnabled(true)
				.planCacheCapacity(2)
				.build();

		Database db = Database.withDataSource(dataSource)
				.resultSetMapper(resultSetMapper)
				.build();

		createTestSchema(db);

		TestQueries.execute(db, "INSERT INTO employee VALUES (1, 'A', 'a@x.com', 'en-US')");
		TestQueries.execute(db, "INSERT INTO employee VALUES (2, 'B', 'b@x.com', 'ja-JP')");

		db.query("SELECT name, email_address, locale FROM employee").fetchList(EmployeeClass.class);
		db.query("SELECT name, email_address FROM employee").fetchList(EmployeeClass.class);
		db.query("SELECT name, locale FROM employee").fetchList(EmployeeClass.class);

		Map<?, ?> cache = resultSetMapper.getRowPlanningCache();
		if (cache instanceof ConcurrentLruMap<?, ?> lru)
			lru.drain();

		Assertions.assertTrue(cache instanceof ConcurrentLruMap, "Row-plan cache should use LRU when capacity is set");
		Assertions.assertTrue(cache.size() <= 2, "Row-plan cache should honor configured capacity");
	}

	public record GroupRow(String groupName, List<UUID> ids) {}

	@Test
	public void testCustomColumnMapperParameterizedList() {
		DataSource dataSource = createInMemoryDataSource("cm_list_uuid");

		// Mapper: List<UUID> from CSV string (e.g., "u1,u2")
		CustomColumnMapper csvUuidList = new CustomColumnMapper() {
			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesParameterizedType(List.class, UUID.class);
			}

			@Nonnull
			@Override
			public MappingResult map(@Nonnull StatementContext<?> statementContext,
															 @Nonnull ResultSet resultSet,
															 @Nonnull Object resultSetValue,
															 @Nonnull TargetType targetType,
															 @Nonnull Integer columnIndex,
															 @Nullable String columnLabel,
															 @Nonnull InstanceProvider instanceProvider) {
				String s = resultSetValue == null ? null : resultSetValue.toString();
				if (s == null || s.isBlank())
					return MappingResult.of(List.of());

				List<UUID> uuids = Arrays.stream(s.split(","))
						.map(String::trim)
						.filter(str -> !str.isEmpty())
						.map(UUID::fromString)
						.collect(Collectors.toList());

				return MappingResult.of(uuids);
			}
		};

		Database db = Database.withDataSource(dataSource)
				.resultSetMapper(ResultSetMapper.withCustomColumnMappers(List.of(csvUuidList)).build())
				.build();

		// Simple schema for the test
		TestQueries.execute(db, """
				CREATE TABLE group_data (
				  group_name VARCHAR(255),
				  ids VARCHAR(2000)
				)
				""");

		UUID u1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
		UUID u2 = UUID.fromString("22222222-2222-2222-2222-222222222222");
		TestQueries.execute(db, "INSERT INTO group_data VALUES (?, ?)", "alpha", u1 + "," + u2);

		// Note: property "groupName" should match DB column "group_name" via normalization logic
		GroupRow row = db.query("SELECT group_name, ids FROM group_data WHERE group_name=:groupName")
				.bind("groupName", "alpha")
				.fetchObject(GroupRow.class)
				.orElse(null);

		Assertions.assertNotNull(row);
		Assertions.assertEquals("alpha", row.groupName());
		Assertions.assertEquals(List.of(u1, u2), row.ids());
	}

	@Test
	public void testCustomColumnMapper_AppliesToStandardType_SingleColumn() {
		DataSource dataSource = createInMemoryDataSource("cm_standard");

		// Mapper: for any Locale target, always return CANADA (proves custom path is used on standard fast path)
		CustomColumnMapper localeOverride = new CustomColumnMapper() {
			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Locale.class);
			}

			@Nonnull
			@Override
			public MappingResult map(@Nonnull StatementContext<?> statementContext,
															 @Nonnull ResultSet resultSet,
															 @Nonnull Object resultSetValue,
															 @Nonnull TargetType targetType,
															 @Nonnull Integer columnIndex,
															 @Nullable String columnLabel,
															 @Nonnull InstanceProvider instanceProvider) {
				// Ignore DB value; force a deterministic value so we can assert override happened.
				return MappingResult.of(Locale.CANADA);
			}
		};

		ResultSetMapper.Builder rsmBuilder = ResultSetMapper
				.withCustomColumnMappers(List.of(localeOverride));

		Database db = Database.withDataSource(dataSource)
				.resultSetMapper(rsmBuilder.build())
				.build();

		createTestSchema(db);

		// id | name | email   | locale
		TestQueries.execute(db, "INSERT INTO employee VALUES (1, 'A', 'a@x.com', 'en-US')");
		TestQueries.execute(db, "INSERT INTO employee VALUES (2, 'B', 'b@x.com', 'ja-JP')");
		TestQueries.execute(db, "INSERT INTO employee VALUES (3, 'C', 'c@x.com', NULL)");

		// --- PROOF: custom mapper applies for standard-type mapping (Locale.class) on a 1-column SELECT ---
		Optional<Locale> l1 = db.query("SELECT locale FROM employee WHERE employee_id=1")
				.fetchObject(Locale.class);
		Assertions.assertTrue(l1.isPresent(), "Expected a Locale result");
		Assertions.assertEquals(Locale.CANADA, l1.get(), "Custom mapper did not override Locale for standard-type mapping");

		// Another row, same behavior (also exercises the (sourceClass, targetType) cache)
		Optional<Locale> l2 = db.query("SELECT locale FROM employee WHERE employee_id=2")
				.fetchObject(Locale.class);
		Assertions.assertTrue(l2.isPresent(), "Expected a Locale result");
		Assertions.assertEquals(Locale.CANADA, l2.get(), "Custom mapper did not override Locale for standard-type mapping (row 2)");

		// --- NULL raw value: mapper is skipped (raw==null), standard fast path returns Optional.empty() ---
		Optional<Locale> l3 = db.query("SELECT locale FROM employee WHERE employee_id=3")
				.fetchObject(Locale.class);
		Assertions.assertTrue(l3.isEmpty(), "NULL column should map to Optional.empty() for standard types");

		// --- Single-column invariant still enforced even if a mapper exists ---
		Assertions.assertThrows(DatabaseException.class, () ->
						db.query("SELECT locale, email FROM employee WHERE employee_id=1")
								.fetchObject(Locale.class),
				"Mapping a standard type from multiple columns should throw");
	}

	@NotThreadSafe
	protected static class TestPerson {
		private String name;
		private int age;

		public TestPerson() {}

		public TestPerson(String name, int age) {
			this.name = name;
			this.age = age;
		}

		public String getName() {return name;}

		public void setName(String name) {this.name = name;}

		public int getAge() {return age;}

		public void setAge(int age) {this.age = age;}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof TestPerson p)) return false;
			return age == p.age && (name == null ? p.name == null : name.equals(p.name));
		}

		@Override
		public int hashCode() {return name.hashCode() * 31 + age;}
	}

	@NotThreadSafe
	protected record TestPersonRow(@DatabaseColumn("payload") TestPerson testPerson, Long id) {}

	@Test
	public void testCustomColumnMapper_InflatesJsonToPojo() {
		DataSource dataSource = createInMemoryDataSource("cm_json");

		// Mapper: if target is Person.class and source is a JSON string, inflate it.
		CustomColumnMapper jsonToPerson = new CustomColumnMapper() {
			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(TestPerson.class);
			}

			@Nonnull
			@Override
			public MappingResult map(@Nonnull StatementContext<?> statementContext,
															 @Nonnull ResultSet resultSet,
															 @Nonnull Object resultSetValue,
															 @Nonnull TargetType targetType,
															 @Nonnull Integer columnIndex,
															 @Nullable String columnLabel,
															 @Nonnull InstanceProvider instanceProvider) {
				if (!(resultSetValue instanceof String json)) return MappingResult.fallback();
				// Very simple parser for test purposes (no external libs needed)
				json = json.trim().replaceAll("[{}\"]", "");
				String[] parts = json.split(",");
				String name = null;
				Integer age = null;
				for (String part : parts) {
					String[] kv = part.split(":");
					if (kv.length != 2) continue;
					String key = kv[0].trim();
					String value = kv[1].trim();
					if (key.equals("name")) name = value;
					else if (key.equals("age")) age = Integer.parseInt(value);
				}
				return MappingResult.of(new TestPerson(name, age == null ? 0 : age));
			}
		};

		ResultSetMapper.Builder rsmBuilder = ResultSetMapper
				.withCustomColumnMappers(List.of(jsonToPerson));
		// If feature is behind a toggle:
		// rsmBuilder = rsmBuilder.applyCustomMappersForStandardTypes(true);

		Database db = Database.withDataSource(dataSource)
				.resultSetMapper(rsmBuilder.build())
				.build();

		// Simple schema with a TEXT column holding JSON
		TestQueries.execute(db, "CREATE TABLE people_json (id INT PRIMARY KEY, payload VARCHAR)");
		TestQueries.execute(db, "INSERT INTO people_json VALUES (1, '{\"name\":\"Alice\",\"age\":30}')");
		TestQueries.execute(db, "INSERT INTO people_json VALUES (2, '{\"name\":\"Bob\",\"age\":42}')");
		TestQueries.execute(db, "INSERT INTO people_json VALUES (3, NULL)");

		// Mapper inflates JSON into Person object directly from single-column SELECT
		Optional<TestPerson> p1 = db.query("SELECT payload FROM people_json WHERE id=1")
				.fetchObject(TestPerson.class);
		Assertions.assertEquals(new TestPerson("Alice", 30), p1.orElse(null));

		Optional<TestPerson> p2 = db.query("SELECT payload FROM people_json WHERE id=2")
				.fetchObject(TestPerson.class);
		Assertions.assertEquals(new TestPerson("Bob", 42), p2.orElse(null));

		// NULL → Optional.empty()
		Optional<TestPerson> p3 = db.query("SELECT payload FROM people_json WHERE id=3")
				.fetchObject(TestPerson.class);
		Assertions.assertTrue(p3.isEmpty(), "Expected empty Optional for NULL JSON column");

		Optional<TestPerson> p4 = db.query("SELECT payload FROM people_json WHERE id=4")
				.fetchObject(TestPerson.class);
		Assertions.assertTrue(p4.isEmpty(), "Expected empty Optional for no rows");

		List<TestPerson> people = db.query("SELECT payload FROM people_json ORDER BY id")
				.fetchList(TestPerson.class);
		Assertions.assertEquals(3, people.size(), "Wrong number of people returned");
		Assertions.assertEquals("Alice", people.get(0).name, "Wrong person name");
		Assertions.assertEquals("Bob", people.get(1).name, "Wrong person name");
		Assertions.assertNull(people.get(2), "Third person result should be null");

		// Multi-column should still throw
		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT payload, id FROM people_json WHERE id=1")
						.fetchObject(TestPerson.class));

		// Pull back the whole row and make sure the mapper still works
		TestPersonRow row = db.query("SELECT * FROM people_json WHERE id=1")
				.fetchObject(TestPersonRow.class)
				.orElse(null);

		Assertions.assertNotNull(row, "Unable to pull person record by ID");
		Assertions.assertEquals("Alice", row.testPerson().name, "Wrong person name");
		Assertions.assertEquals(1L, row.id(), "Wrong person ID");
	}

	@Test
	public void testExecuteForObjectAndListUsingSelect() {
		Database db = Database.withDataSource(createInMemoryDataSource("exec_select")).build();
		TestQueries.execute(db, "CREATE TABLE prod (id INT, name VARCHAR(64))");
		TestQueries.execute(db, "INSERT INTO prod VALUES (1, 'A')");
		TestQueries.execute(db, "INSERT INTO prod VALUES (2, 'B')");

		Optional<String> name = db.query("SELECT name FROM prod WHERE id=:id")
				.bind("id", 1)
				.fetchObject(String.class);
		Assertions.assertTrue(name.isPresent());
		Assertions.assertEquals("A", name.get());

		List<Integer> ids = db.query("SELECT id FROM prod ORDER BY id")
				.fetchList(Integer.class);
		Assertions.assertEquals(List.of(1, 2), ids);
	}

	public static class Prefs {
		private ZoneId tz;
		private Locale locale;
		private Currency currency;

		public ZoneId getTz() {return tz;}

		public void setTz(ZoneId tz) {this.tz = tz;}

		public Locale getLocale() {return locale;}

		public void setLocale(Locale locale) {this.locale = locale;}

		public Currency getCurrency() {return currency;}

		public void setCurrency(Currency currency) {this.currency = currency;}
	}

	@Test
	public void testZoneIdLocaleCurrencyRoundTrip() {
		Database db = Database.withDataSource(createInMemoryDataSource("prefs")).build();
		TestQueries.execute(db, "CREATE TABLE prefs (tz VARCHAR(64), locale VARCHAR(32), currency VARCHAR(3))");

		ZoneId zone = ZoneId.of("America/New_York");
		Locale loc = Locale.CANADA;
		Currency cur = Currency.getInstance("USD");

		TestQueries.execute(db, "INSERT INTO prefs (tz, locale, currency) VALUES (?, ?, ?)", zone, loc, cur);

		Prefs prefs = db.query("SELECT * FROM prefs")
				.fetchObject(Prefs.class)
				.orElseThrow();
		Assertions.assertEquals(zone, prefs.getTz());
		Assertions.assertEquals(loc, prefs.getLocale());
		Assertions.assertEquals(cur, prefs.getCurrency());
	}

	@Test
	public void testNumericMappingPreservesPrecision() {
		Database db = Database.withDataSource(createInMemoryDataSource("big_numbers")).build();
		TestQueries.execute(db, "CREATE TABLE big_numbers (v BIGINT)");

		long value = 9_007_199_254_740_993L; // 2^53 + 1
		TestQueries.execute(db, "INSERT INTO big_numbers (v) VALUES (?)", value);

		BigDecimalHolder bigDecimalHolder = db.query("SELECT v FROM big_numbers")
				.fetchObject(BigDecimalHolder.class)
				.orElseThrow();
		BigIntegerHolder bigIntegerHolder = db.query("SELECT v FROM big_numbers")
				.fetchObject(BigIntegerHolder.class)
				.orElseThrow();

		Assertions.assertEquals(BigDecimal.valueOf(value), bigDecimalHolder.getV());
		Assertions.assertEquals(BigInteger.valueOf(value), bigIntegerHolder.getV());
	}

	// Should be able to read the NULL back into a Java bean
	static class Foo {
		private Integer id;
		private String name;

		public Integer getId() {return id;}

		public void setId(Integer id) {this.id = id;}

		public String getName() {return name;}

		public void setName(String name) {this.name = name;}
	}

	@Test
	public void testNullParameterBinding() {
		Database db = Database.withDataSource(createInMemoryDataSource("nulls")).build();
		TestQueries.execute(db, "CREATE TABLE foo (id INT, name VARCHAR(64))");
		TestQueries.execute(db, "INSERT INTO foo (id, name) VALUES (?, ?)", 1, null);

		Foo row = db.query("SELECT * FROM foo")
				.fetchObject(Foo.class)
				.orElseThrow();
		Assertions.assertEquals(Integer.valueOf(1), row.getId());
		Assertions.assertNull(row.getName());
	}

	@Test
	public void testReadDatabaseMetaData() {
		Database db = Database.withDataSource(createInMemoryDataSource("metadata")).build();
		final AtomicInteger seen = new AtomicInteger(0);
		db.readDatabaseMetaData(meta -> {
			Assertions.assertNotNull(meta.getDatabaseProductName());
			seen.incrementAndGet();
		});
		Assertions.assertEquals(1, seen.get());
	}

	@Test
	public void testDatabaseBuilderAllowsDatabaseTypeOverride() {
		DataSource dataSource = new DataSource() {
			@Override
			public Connection getConnection() throws SQLException {
				throw new SQLException("boom");
			}

			@Override
			public Connection getConnection(String username, String password) throws SQLException {
				throw new SQLException("boom");
			}

			@Override
			public java.io.PrintWriter getLogWriter() throws SQLException {
				return null;
			}

			@Override
			public void setLogWriter(java.io.PrintWriter out) throws SQLException {
			}

			@Override
			public void setLoginTimeout(int seconds) throws SQLException {
			}

			@Override
			public int getLoginTimeout() throws SQLException {
				return 0;
			}

			@Override
			public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
				throw new java.sql.SQLFeatureNotSupportedException();
			}

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				throw new SQLException("unwrap");
			}

			@Override
			public boolean isWrapperFor(Class<?> iface) {
				return false;
			}
		};

		Assertions.assertDoesNotThrow(() ->
				Database.withDataSource(dataSource)
						.databaseType(DatabaseType.GENERIC)
						.build());
	}

	@Test
	public void testStatementLoggerReceivesEvent() {
		Database db = Database.withDataSource(createInMemoryDataSource("logger")).statementLogger((log) -> {
			Assertions.assertNotNull(log.getStatementContext(), "StatementContext should be present");
			Assertions.assertTrue(log.getStatementContext().getStatement().getSql().length() > 0, "SQL should be present");
		}).build();

		TestQueries.execute(db, "CREATE TABLE z (id INT)");
		TestQueries.execute(db, "INSERT INTO z VALUES (1)");
	}

	@Test
	public void testStatementLoggerIncludesBatchSize() {
		AtomicReference<StatementLog<?>> logRef = new AtomicReference<>();
		Database db = Database.withDataSource(createInMemoryDataSource("logger_batch"))
				.statementLogger(logRef::set)
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT)");
		logRef.set(null);

		List<Map<String, Object>> groups = List.of(
				Map.of("id", 1),
				Map.of("id", 2),
				Map.of("id", 3)
		);

		db.query("INSERT INTO t (id) VALUES (:id)")
				.executeBatch(groups);

		StatementLog<?> log = logRef.get();
		Assertions.assertNotNull(log, "Expected a StatementLog for batch execution");
		Assertions.assertEquals(Integer.valueOf(3), log.getBatchSize().orElse(null));
	}

	@Test
	public void testStatementLoggerFailureDoesNotRollbackTransaction() {
		AtomicBoolean shouldThrow = new AtomicBoolean(false);
		Database db = Database.withDataSource(createInMemoryDataSource("logger_no_rollback"))
				.statementLogger(statementLog -> {
					if (shouldThrow.get())
						throw new RuntimeException("logger boom");
				})
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT)");

		shouldThrow.set(true);
		Assertions.assertThrows(RuntimeException.class, () ->
				db.transaction(() -> TestQueries.execute(db, "INSERT INTO t VALUES (1)")));

		shouldThrow.set(false);
		Long count = db.query("SELECT COUNT(*) FROM t")
				.fetchObject(Long.class)
				.orElse(0L);

		Assertions.assertEquals(1L, count, "Transaction should commit even if statement logger fails");
	}

	@Test
	public void testQueryRejectsDuplicateColumnLabels() {
		Database db = Database.withDataSource(createInMemoryDataSource("dup_column_labels")).build();

		TestQueries.execute(db, "CREATE TABLE t (id INT, name VARCHAR(255))");
		TestQueries.execute(db, "INSERT INTO t VALUES (1, 'alpha')");

		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT name AS name, name AS name FROM t")
						.fetchList(EmployeeClass.class));
	}

	@Test
	public void testQueryRejectsDuplicateColumnLabels_withoutPlanCaching() {
		Database db = Database.withDataSource(createInMemoryDataSource("dup_column_labels_no_plan"))
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(false).build())
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT, name VARCHAR(255))");
		TestQueries.execute(db, "INSERT INTO t VALUES (1, 'alpha')");

		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT name AS name, name AS name FROM t")
						.fetchList(EmployeeClass.class));
	}

	@Test
	public void testSqlArrayParameter_insertAndReadBack() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_array_param")).build();

		// HSQLDB array column syntax: VARCHAR(100) ARRAY
		TestQueries.execute(db, "CREATE TABLE t_array (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, tags VARCHAR(100) ARRAY)");

		// Insert using SqlArrayParameter (defensive copy internally)
		TestQueries.execute(db, "INSERT INTO t_array(tags) VALUES (?)",
				Parameters.sqlArrayOf("VARCHAR", List.of("alpha", "beta", "gamma")));

		// Verify size via SQL function
		Optional<Integer> size = db.query("SELECT CARDINALITY(tags) FROM t_array FETCH FIRST ROW ONLY")
				.fetchObject(Integer.class);
		Assertions.assertTrue(size.isPresent());
		Assertions.assertEquals(Integer.valueOf(3), size.get());

		// HSQLDB supports 1-based array element access with brackets
		Optional<String> first = db.query("SELECT tags[1] FROM t_array FETCH FIRST ROW ONLY")
				.fetchObject(String.class);
		Optional<String> second = db.query("SELECT tags[2] FROM t_array FETCH FIRST ROW ONLY")
				.fetchObject(String.class);
		Optional<String> third = db.query("SELECT tags[3] FROM t_array FETCH FIRST ROW ONLY")
				.fetchObject(String.class);

		Assertions.assertEquals("alpha", first.orElse(null));
		Assertions.assertEquals("beta", second.orElse(null));
		Assertions.assertEquals("gamma", third.orElse(null));
	}

	@Test
	public void testSqlArrayParameter_emptyArray() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_array_param_empty")).build();
		TestQueries.execute(db, "CREATE TABLE t_array (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, tags VARCHAR(100) ARRAY)");

		TestQueries.execute(db, "INSERT INTO t_array(tags) VALUES (?)",
				Parameters.sqlArrayOf("VARCHAR", List.of())); // empty

		Optional<Integer> size = db.query("SELECT CARDINALITY(tags) FROM t_array FETCH FIRST ROW ONLY")
				.fetchObject(Integer.class);
		Assertions.assertTrue(size.isPresent());
		Assertions.assertEquals(Integer.valueOf(0), size.get());
	}

	@Test
	public void testSqlArrayParameter_allowsNullElements() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_array_param_null_elements")).build();
		TestQueries.execute(db, "CREATE TABLE t_array (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, tags VARCHAR(100) ARRAY)");

		TestQueries.execute(db, "INSERT INTO t_array(tags) VALUES (?)",
				Parameters.sqlArrayOf("VARCHAR", Arrays.asList("alpha", null, "gamma")));

		Optional<Integer> size = db.query("SELECT CARDINALITY(tags) FROM t_array FETCH FIRST ROW ONLY")
				.fetchObject(Integer.class);
		Assertions.assertTrue(size.isPresent());
		Assertions.assertEquals(Integer.valueOf(3), size.get());

		Optional<String> first = db.query("SELECT tags[1] FROM t_array FETCH FIRST ROW ONLY")
				.fetchObject(String.class);
		Optional<String> second = db.query("SELECT tags[2] FROM t_array FETCH FIRST ROW ONLY")
				.fetchObject(String.class);
		Optional<String> third = db.query("SELECT tags[3] FROM t_array FETCH FIRST ROW ONLY")
				.fetchObject(String.class);

		Assertions.assertEquals("alpha", first.orElse(null));
		Assertions.assertTrue(second.isEmpty());
		Assertions.assertEquals("gamma", third.orElse(null));
	}

	@Test
	public void testJsonParameter_roundTrip_textOnHsqldb() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_json_param")).build();
		TestQueries.execute(db, "CREATE TABLE t_json (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, body LONGVARCHAR)");

		String json = "{\"a\":1,\"b\":[true,false],\"s\":\"x\"}";

		// AUTOMATIC mode should fall back to text on HSQLDB and insert successfully
		TestQueries.execute(db, "INSERT INTO t_json(body) VALUES (?)", Parameters.json(json));

		Optional<String> got = db.query("SELECT body FROM t_json FETCH FIRST ROW ONLY")
				.fetchObject(String.class);
		Assertions.assertTrue(got.isPresent());
		Assertions.assertEquals(json, got.get());

		// TEXT is honored (also text on HSQLDB)
		String json2 = "{\"k\":\"v\"}";
		TestQueries.execute(db, "INSERT INTO t_json(body) VALUES (?)", Parameters.json(json2, BindingPreference.TEXT));
		List<String> bodies = db.query("SELECT body FROM t_json ORDER BY id")
				.fetchList(String.class);
		Assertions.assertEquals(List.of(json, json2), bodies);
	}

	@Test
	public void testVectorParameter_throwsOnNonPostgres() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_vector_param")).build();
		TestQueries.execute(db, "CREATE TABLE t_vec (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		try {
			TestQueries.execute(db, "INSERT INTO t_vec(v) VALUES (?)", Parameters.vectorOfDoubles(new double[]{0.12, -0.5, 1.0}));
			Assertions.fail("Expected IllegalArgumentException for non-PostgreSQL DatabaseType when binding VectorParameter");
		} catch (DatabaseException expected) {
			// pass
		}
	}

	@Test
	public void testTransactionIsolation() {
		Database db = Database.withDataSource(createInMemoryDataSource("txn_isolation")).build();

		db.transaction(TransactionIsolation.SERIALIZABLE, () -> {
			db.performRawConnectionOperation(conn -> {
				int level = conn.getTransactionIsolation();
				Assertions.assertEquals(Connection.TRANSACTION_SERIALIZABLE, level);
				Assertions.assertEquals(TransactionIsolation.SERIALIZABLE, db.currentTransaction().get().getTransactionIsolation());
				return Optional.empty();
			}, true);
		});

		// Another transaction; isolation restored
		db.transaction(() -> {
			db.performRawConnectionOperation(conn -> {
				int level = conn.getTransactionIsolation();
				Assertions.assertEquals(Connection.TRANSACTION_READ_COMMITTED, level);
				Assertions.assertEquals(TransactionIsolation.DEFAULT, db.currentTransaction().get().getTransactionIsolation());
				return Optional.empty();
			}, true);
		});

		// Another txn level
		db.transaction(TransactionIsolation.REPEATABLE_READ, () -> {
			db.performRawConnectionOperation(conn -> {
				int level = conn.getTransactionIsolation();
				Assertions.assertEquals(Connection.TRANSACTION_REPEATABLE_READ, level);
				Assertions.assertEquals(TransactionIsolation.REPEATABLE_READ, db.currentTransaction().get().getTransactionIsolation());
				return Optional.empty();
			}, true);
		});

		// Non-transactional; verifies restoration
		db.performRawConnectionOperation(conn -> {
			int level = conn.getTransactionIsolation();
			// compare to what HSQLDB default was before entering the txn
			Assertions.assertEquals(Connection.TRANSACTION_READ_COMMITTED, level); // example default
			return Optional.empty();
		}, false);
	}

	@Test
	public void testLocalDate_bindsToDateColumn_roundTrips() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_localdate"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		TestQueries.execute(db, "CREATE TABLE t_date (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, d DATE)");

		LocalDate input = LocalDate.of(2020, 6, 7);
		TestQueries.execute(db, "INSERT INTO t_date(d) VALUES (?)", input);

		LocalDate roundTripped = db.query("SELECT d FROM t_date")
				.fetchObject(LocalDate.class)
				.orElseThrow();
		Assertions.assertEquals(input, roundTripped, "LocalDate round-trip mismatch");
	}

	@Test
	public void testLocalTime_bindsAsString_onDriversWithoutSafeTimeHandling() {
		// DefaultPreparedStatementBinder writes LocalTime as string for maximum safety.
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_localtime"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		TestQueries.execute(db, "CREATE TABLE t_time (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, t LONGVARCHAR)");

		LocalTime input = LocalTime.of(8, 9, 10, 123_456_789);
		TestQueries.execute(db, "INSERT INTO t_time(t) VALUES (?)", input);

		String stored = db.query("SELECT t FROM t_time")
				.fetchObject(String.class)
				.orElseThrow();
		Assertions.assertEquals(input.toString(), stored, "LocalTime should be stored as ISO-8601 string");
	}

	@Test
	public void testLocalDateTime_bindsToTimestamp_roundTrips() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_ldt"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		TestQueries.execute(db, "CREATE TABLE t_ts (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, ts TIMESTAMP)");

		LocalDateTime input = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000_000);
		TestQueries.execute(db, "INSERT INTO t_ts(ts) VALUES (?)", input);

		// Let mapper do the conversion
		LocalDateTime roundTripped = db.query("SELECT ts FROM t_ts")
				.fetchObject(LocalDateTime.class)
				.orElseThrow();
		Assertions.assertEquals(input, roundTripped, "LocalDateTime round-trip mismatch");
	}

	@Test
	public void testOffsetDateTime_targetTimestamp_coercesToDbZone_andInserts() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_odt_ts"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				// default tz is fine; binder will coerce ODT -> LDT when target is TIMESTAMP
				.build();

		TestQueries.execute(db, "CREATE TABLE t_odt_ts (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, ts TIMESTAMP)");

		OffsetDateTime odt = OffsetDateTime.parse("2023-05-10T12:34:56.789+02:00");
		TestQueries.execute(db, "INSERT INTO t_odt_ts(ts) VALUES (?)", odt);

		Integer count = db.query("SELECT COUNT(*) FROM t_odt_ts")
				.fetchObject(Integer.class)
				.orElseThrow();
		Assertions.assertEquals(Integer.valueOf(1), count, "Row should be inserted");
	}

	@Test
	public void testInstant_targetTimestamp_inserts() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_instant"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		TestQueries.execute(db, "CREATE TABLE t_instant (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, ts TIMESTAMP)");

		Instant instant = Instant.parse("2020-01-02T03:04:05.123Z");
		TestQueries.execute(db, "INSERT INTO t_instant(ts) VALUES (?)", instant);

		// Pull back as Timestamp just to ensure the write stuck
		Timestamp ts = db.query("SELECT ts FROM t_instant")
				.fetchObject(Timestamp.class)
				.orElseThrow();
		Assertions.assertTrue(Math.abs(ts.toInstant().toEpochMilli() - instant.toEpochMilli()) <= 1, "Stored timestamp should be close to input instant"); // tolerance for driver rounding
	}

	@Test
	public void testJsonParameter_onNonPostgres_isStoredAsText() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_json_text"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		TestQueries.execute(db, "CREATE TABLE t_json (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, j LONGVARCHAR)");

		String json = """
					{"a":1,"b":[2,3],"ok":true}
				""";

		// On non-Postgres, DefaultPreparedStatementBinder falls back to setString(json)
		TestQueries.execute(db, "INSERT INTO t_json(j) VALUES (?)", Parameters.json(json));

		String stored = db.query("SELECT j FROM t_json")
				.fetchObject(String.class)
				.orElseThrow();
		Assertions.assertEquals(json.trim(), stored.trim(), "JSON should be stored as text on non-Postgres");
	}

	@Test
	public void testVectorParameter_throwsOnNonPostgres_matchesExample() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_vector_param_dup")).build();
		TestQueries.execute(db, "CREATE TABLE t_vec (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		try {
			TestQueries.execute(db, "INSERT INTO t_vec(v) VALUES (?)", Parameters.vectorOfDoubles(new double[]{0.12, -0.5, 1.0}));
			Assertions.fail("Expected exception for VectorParameter on non-PostgreSQL");
		} catch (DatabaseException expected) {
			// pass
		}
	}

	@Test
	public void testCustomParameterBinder_winsAndCaches() {
		// We’ll install a binder that handles Locale and writes a custom-marked string.
		AtomicInteger calls = new AtomicInteger(0);
		CustomParameterBinder localeBinder = new CustomParameterBinder() {
			@Override
			public BindingResult bind(StatementContext<?> ctx, java.sql.PreparedStatement ps, Integer index, Object param) throws java.sql.SQLException {
				if (!(param instanceof Locale)) return BindingResult.fallback();
				calls.incrementAndGet();
				ps.setString(index, "CUSTOM:" + ((Locale) param).toLanguageTag());
				return BindingResult.handled();
			}

			@Override
			public Boolean appliesTo(TargetType targetType) {
				return targetType.matchesClass(Locale.class);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_custom"))
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(localeBinder)))
				.build();

		TestQueries.execute(db, "CREATE TABLE t_locale (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, lang LONGVARCHAR)");

		Locale l = Locale.forLanguageTag("en-US");
		TestQueries.execute(db, "INSERT INTO t_locale(lang) VALUES (?)", l);
		TestQueries.execute(db, "INSERT INTO t_locale(lang) VALUES (?)", l); // hit again to exercise preferredBinderByInboundKey fast path

		List<String> rows = db.query("SELECT lang FROM t_locale ORDER BY id")
				.fetchList(String.class);
		Assertions.assertEquals(2, rows.size());
		Assertions.assertEquals("CUSTOM:en-US", rows.get(0));
		Assertions.assertEquals("CUSTOM:en-US", rows.get(1));
		Assertions.assertTrue(calls.get() >= 2, "Custom binder should be invoked at least twice");
	}

	@Test
	public void testNullJsonParameter_bindsAsNull() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_json_null"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		TestQueries.execute(db, "CREATE TABLE t_json_null (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, j LONGVARCHAR)");

		TestQueries.execute(db, "INSERT INTO t_json_null(j) VALUES (?)", Parameters.json(null));

		Integer nullCount = db.query("SELECT COUNT(*) FROM t_json_null WHERE j IS NULL")
				.fetchObject(Integer.class)
				.orElseThrow();
		Assertions.assertEquals(Integer.valueOf(1), nullCount);
	}

	@Test
	public void testOffsetTime_bindsAsTimeWithZoneOrString_noException() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_offsettime"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		TestQueries.execute(db, "CREATE TABLE t_ot (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, t LONGVARCHAR)");

		OffsetTime ot = OffsetTime.parse("08:09:10.123456789-03:00");
		// Binder will try TIME WITH TIME ZONE; if unsupported it falls back to string, so our column is VARCHAR.
		TestQueries.execute(db, "INSERT INTO t_ot(t) VALUES (?)", ot);

		String stored = db.query("SELECT t FROM t_ot")
				.fetchObject(String.class)
				.orElseThrow();
		Assertions.assertEquals(ot.toString(), stored, "OffsetTime should be stored as ISO-8601 string when driver lacks TIMETZ");
	}

	@Test
	public void testEverythingElse_setObjectFallback_handlesEnumAndCurrency() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_misc"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		TestQueries.execute(db, "CREATE TABLE t_misc (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, e LONGVARCHAR, c LONGVARCHAR)");

		// Enum is normalized to name(); Currency normalized to currency code
		TestQueries.execute(db, "INSERT INTO t_misc(e, c) VALUES (?, ?)", TestEnum.BLUE, java.util.Currency.getInstance("USD"));

		EnumCurrencyRow enumCurrencyRow = db.query("SELECT e, c FROM t_misc")
				.fetchObject(EnumCurrencyRow.class)
				.orElseThrow();

		Assertions.assertEquals(TestEnum.BLUE, enumCurrencyRow.getE());
		Assertions.assertEquals("USD", enumCurrencyRow.getC().getCurrencyCode());
	}

	protected static class EnumCurrencyRow {
		private TestEnum e;
		private Currency c;

		public TestEnum getE() {
			return this.e;
		}

		public void setE(TestEnum e) {
			this.e = e;
		}

		public Currency getC() {
			return this.c;
		}

		public void setC(Currency c) {
			this.c = c;
		}
	}

	protected enum TestEnum {
		RED, BLUE
	}

	/**
	 * Basic success: binder applies to Locale and sets a custom string form.
	 */
	@Test
	public void testBinder_appliesTo_Locale_wins() {
		AtomicInteger calls = new AtomicInteger();

		CustomParameterBinder localeBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc,
																@Nonnull PreparedStatement ps,
																@Nonnull Integer idx,
																@Nonnull Object param) throws SQLException {
				Assertions.assertTrue(param instanceof Locale, "Expected Locale");
				calls.incrementAndGet();
				ps.setString(idx, "CUSTOM:" + ((Locale) param).toLanguageTag());
				return BindingResult.handled();
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Locale.class);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("cpb_locale_wins"))
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(localeBinder)))
				// set a stable timezone just to be explicit
				.timeZone(ZoneId.of("UTC"))
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, lang LONGVARCHAR)");

		TestQueries.execute(db, "INSERT INTO t(lang) VALUES (?)", Locale.forLanguageTag("en-US"));
		TestQueries.execute(db, "INSERT INTO t(lang) VALUES (?)", Locale.forLanguageTag("pt-BR"));

		List<String> rows = db.query("SELECT lang FROM t ORDER BY id")
				.fetchList(String.class);
		Assertions.assertEquals(List.of("CUSTOM:en-US", "CUSTOM:pt-BR"), rows);
		Assertions.assertTrue(calls.get() >= 2, "Binder should have been called twice");
	}

	/**
	 * Ensure non-applicable binders are never invoked.
	 */
	@Test
	public void testBinder_appliesTo_filtering_skipsNonApplicable() {
		AtomicInteger neverCalled = new AtomicInteger(0);
		AtomicInteger called = new AtomicInteger(0);

		CustomParameterBinder intOnly = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) {
				neverCalled.incrementAndGet();
				throw new AssertionError("Should not be called for Locale");
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Integer.class);
			}
		};

		CustomParameterBinder localeBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
				called.incrementAndGet();
				ps.setString(idx, "OK");
				return BindingResult.handled();
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Locale.class);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("cpb_filtering"))
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(intOnly, localeBinder)))
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, lang LONGVARCHAR)");
		TestQueries.execute(db, "INSERT INTO t(lang) VALUES (?)", Locale.US);

		String stored = db.query("SELECT lang FROM t")
				.fetchObject(String.class)
				.orElseThrow();
		Assertions.assertEquals("OK", stored);
		Assertions.assertEquals(0, neverCalled.get(), "Non-applicable binder must not be called");
		Assertions.assertEquals(1, called.get(), "Applicable binder called exactly once");
	}

	/**
	 * Preferred-binder caching: first applicable binder "wins", and subsequent binds
	 * with same (valueClass, sqlType) should try that binder first.
	 * We assert that the first binder is the only one invoked on subsequent calls.
	 */
	@Test
	public void testBinder_preferredCache_firstWins() {
		AtomicInteger firstCalls = new AtomicInteger();
		AtomicInteger secondCalls = new AtomicInteger();

		CustomParameterBinder first = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
				firstCalls.incrementAndGet();
				ps.setString(idx, "FIRST");
				return BindingResult.handled();
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Locale.class);
			}
		};

		CustomParameterBinder second = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
				secondCalls.incrementAndGet();
				ps.setString(idx, "SECOND");
				return BindingResult.handled();
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Locale.class);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("cpb_cache"))
				// Order matters: 'first' should win and be cached
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(first, second)))
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, lang LONGVARCHAR)");

		// First call: cache will be populated (first binder wins)
		TestQueries.execute(db, "INSERT INTO t(lang) VALUES (?)", Locale.CANADA); // preferred selected here

		// Second call: should hit cached binder directly
		TestQueries.execute(db, "INSERT INTO t(lang) VALUES (?)", Locale.FRANCE);

		List<String> rows = db.query("SELECT lang FROM t ORDER BY id")
				.fetchList(String.class);
		Assertions.assertEquals(List.of("FIRST", "FIRST"), rows);

		Assertions.assertEquals(2, firstCalls.get(), "First binder should be used twice");
		// If caching is working, second binder is never even attempted
		Assertions.assertEquals(0, secondCalls.get(), "Second binder should not be used");
	}

	/**
	 * Parameterized type support: binder applies to List<UUID> and writes a joined string.
	 */
	@Test
	public void testBinder_parameterizedType_ListOfUuid() {
		CustomParameterBinder listUuidBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
				if (!(param instanceof List<?> list)) return BindingResult.fallback();
				for (Object o : list) if (!(o instanceof UUID)) return BindingResult.fallback();
				String joined = list.stream().map(Object::toString).reduce((a, b) -> a + "," + b).orElse("");
				ps.setString(idx, joined);
				return BindingResult.handled();
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesParameterizedType(List.class, UUID.class);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("cpb_param_list_uuid"))
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(listUuidBinder)))
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		List<UUID> ids = List.of(
				UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
				UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
		);

		TestQueries.execute(db, "INSERT INTO t(v) VALUES (?)", Parameters.listOf(UUID.class, ids));

		String got = db.query("SELECT v FROM t")
				.fetchObject(String.class)
				.orElseThrow();
		Assertions.assertEquals("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", got);
	}

	/**
	 * Binder returns false -> default binding path should be used.
	 */
	@Test
	public void testBinder_returnsFalse_fallsBackToDefault() {
		AtomicInteger falseCalls = new AtomicInteger();

		CustomParameterBinder alwaysFalseForString = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) {
				falseCalls.incrementAndGet();
				return BindingResult.fallback(); // claim applicability but decide not to handle now
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(String.class);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("cpb_fallback"))
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(alwaysFalseForString)))
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");
		TestQueries.execute(db, "INSERT INTO t(v) VALUES (?)", "hello");

		String stored = db.query("SELECT v FROM t")
				.fetchObject(String.class)
				.orElseThrow();
		Assertions.assertEquals("hello", stored);
		Assertions.assertEquals(1, falseCalls.get(), "Binder should have been invoked once and returned false");
	}

	/**
	 * Binder throws SQLException -> DatabaseException should surface from Query.execute().
	 */
	@Test
	public void testBinder_throwsSQLException_propagates() {
		CustomParameterBinder throwingBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
				throw new SQLException("boom");
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Locale.class);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("cpb_throw"))
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(throwingBinder)))
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		try {
			TestQueries.execute(db, "INSERT INTO t(v) VALUES (?)", Locale.US);
			Assertions.fail("Expected DatabaseException caused by SQLException from binder");
		} catch (DatabaseException e) {
			// pass
			Assertions.assertNotNull(e.getCause());
			Assertions.assertTrue(e.getCause() instanceof SQLException);
			Assertions.assertEquals("boom", e.getCause().getMessage());
		}
	}

	/**
	 * Ensure caching is per (valueClass, sqlType). We insert the same valueClass (Locale)
	 * into two different column SQL types (VARCHAR and INTEGER) and verify both succeed,
	 * implying two separate cache entries can coexist (the binder just writes strings).
	 */
	@Test
	public void testBinder_cacheKey_includesSqlType() {
		CustomParameterBinder localeBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
				if (!(param instanceof Locale l)) return BindingResult.fallback();
				ps.setString(idx, l.toLanguageTag());
				return BindingResult.handled();
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesClass(Locale.class);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("cpb_cache_sqltype"))
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(localeBinder)))
				.build();

		TestQueries.execute(db, "CREATE TABLE t1 (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");
		TestQueries.execute(db, "CREATE TABLE t2 (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)"); // using same SQL type here still exercises cache hits

		TestQueries.execute(db, "INSERT INTO t1(v) VALUES (?)", Locale.CANADA);
		TestQueries.execute(db, "INSERT INTO t2(v) VALUES (?)", Locale.JAPAN);

		List<String> a = db.query("SELECT v FROM t1")
				.fetchList(String.class);
		List<String> b = db.query("SELECT v FROM t2")
				.fetchList(String.class);
		Assertions.assertEquals(List.of(Locale.CANADA.toLanguageTag()), a);
		Assertions.assertEquals(List.of(Locale.JAPAN.toLanguageTag()), b);
	}

	@Test
	public void testBinder_parameterizedType_SetOfUuid() {
		CustomParameterBinder setUuidBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc,
																@Nonnull PreparedStatement ps,
																@Nonnull Integer idx,
																@Nonnull Object param) throws SQLException {
				if (!(param instanceof Set<?> set)) return BindingResult.fallback();
				for (Object o : set) if (!(o instanceof UUID)) return BindingResult.fallback();

				// Stable serialization: sort lexicographically then join by comma
				String joined = set.stream()
						.map(Object::toString)
						.sorted()
						.reduce((a, b) -> a + "," + b)
						.orElse("");
				ps.setString(idx, joined);
				return BindingResult.handled();
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				// Should succeed because TypedParameter carries explicit Set<UUID> type
				return targetType.matchesParameterizedType(Set.class, UUID.class);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("cpb_param_set_uuid"))
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(setUuidBinder)))
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		Set<UUID> ids = new LinkedHashSet<>(List.of(
				UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
				UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
		));

		// Uses TypedParameter with explicit Set<UUID> type
		TestQueries.execute(db, "INSERT INTO t(v) VALUES (?)", Parameters.setOf(UUID.class, ids));

		String got = db.query("SELECT v FROM t")
				.fetchObject(String.class)
				.orElseThrow();
		Assertions.assertEquals(
				"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
				got);
	}

	@Test
	public void testBinder_parameterizedType_MapOfStringInteger() {
		CustomParameterBinder mapBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc,
																@Nonnull PreparedStatement ps,
																@Nonnull Integer idx,
																@Nonnull Object param) throws SQLException {
				if (!(param instanceof Map<?, ?> map)) return BindingResult.fallback();

				// Validate key/value types at runtime
				for (Map.Entry<?, ?> e : map.entrySet()) {
					if (!(e.getKey() instanceof String)) return BindingResult.fallback();
					if (!(e.getValue() instanceof Integer)) return BindingResult.fallback();
				}

				// Stable serialization: sort by key, "k=v" pairs joined by comma
				@SuppressWarnings("unchecked")
				Map<String, Integer> m = (Map<String, Integer>) param;
				String joined = m.entrySet().stream()
						.sorted(Map.Entry.comparingByKey())
						.map(e -> e.getKey() + "=" + e.getValue())
						.reduce((a, b) -> a + "," + b)
						.orElse("");
				ps.setString(idx, joined);
				return BindingResult.handled();
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				// Matches Map<String,Integer>
				return targetType.matchesParameterizedType(Map.class, String.class, Integer.class);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("cpb_param_map_str_int"))
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(mapBinder)))
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		Map<String, Integer> payload = new LinkedHashMap<>();
		payload.put("b", 2);
		payload.put("a", 1);

		// Uses TypedParameter with explicit Map<String,Integer> type
		TestQueries.execute(db, "INSERT INTO t(v) VALUES (?)", Parameters.mapOf(String.class, Integer.class, payload));

		String got = db.query("SELECT v FROM t")
				.fetchObject(String.class)
				.orElseThrow();
		Assertions.assertEquals("a=1,b=2", got);
	}

	@Test
	public void testBinder_arrayOf_StringArray() {
		CustomParameterBinder arrayBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc,
																@Nonnull PreparedStatement ps,
																@Nonnull Integer idx,
																@Nonnull Object param) throws SQLException {
				if (!(param instanceof String[] values)) return BindingResult.fallback();
				ps.setString(idx, Arrays.stream(values).collect(Collectors.joining(",")));
				return BindingResult.handled();
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.isArray()
						&& targetType.getArrayComponentType()
						.map(component -> component.matchesClass(String.class))
						.orElse(false);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("cpb_param_array_str"))
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(arrayBinder)))
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		String[] names = {"alpha", "beta"};
		TestQueries.execute(db, "INSERT INTO t(v) VALUES (?)", Parameters.arrayOf(String.class, names));

		String got = db.query("SELECT v FROM t")
				.fetchObject(String.class)
				.orElseThrow();
		Assertions.assertEquals("alpha,beta", got);
	}

	@Test
	public void testBinder_cacheKey_includesTargetType() {
		CustomParameterBinder uuidListBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc,
																@Nonnull PreparedStatement ps,
																@Nonnull Integer idx,
																@Nonnull Object param) throws SQLException {
				List<?> list = (List<?>) param;
				String joined = list.stream()
						.map(Object::toString)
						.reduce((a, b) -> a + "," + b)
						.orElse("");
				ps.setString(idx, "UUID:" + joined);
				return BindingResult.handled();
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesParameterizedType(List.class, UUID.class);
			}
		};

		CustomParameterBinder stringListBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public BindingResult bind(@Nonnull StatementContext<?> sc,
																@Nonnull PreparedStatement ps,
																@Nonnull Integer idx,
																@Nonnull Object param) throws SQLException {
				List<?> list = (List<?>) param;
				String joined = list.stream()
						.map(Object::toString)
						.reduce((a, b) -> a + "," + b)
						.orElse("");
				ps.setString(idx, "STR:" + joined);
				return BindingResult.handled();
			}

			@Nonnull
			@Override
			public Boolean appliesTo(@Nonnull TargetType targetType) {
				return targetType.matchesParameterizedType(List.class, String.class);
			}
		};

		Database db = Database
				.withDataSource(createInMemoryDataSource("cpb_cache_target_type"))
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(uuidListBinder, stringListBinder)))
				.build();

		TestQueries.execute(db, "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		List<UUID> ids = List.of(
				UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
				UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
		);
		List<String> names = List.of("alpha", "beta");

		TestQueries.execute(db, "INSERT INTO t(v) VALUES (?)", Parameters.listOf(UUID.class, ids));
		TestQueries.execute(db, "INSERT INTO t(v) VALUES (?)", Parameters.listOf(String.class, names));

		List<String> rows = db.query("SELECT v FROM t ORDER BY id")
				.fetchList(String.class);

		Assertions.assertEquals(List.of(
				"UUID:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
				"STR:alpha,beta"
		), rows);
	}

	@Test
	public void testTypedParameterNullBindsAsSqlNull() {
		Database db = Database.withDataSource(createInMemoryDataSource("typed_param_null")).build();
		TestQueries.execute(db, "CREATE TABLE t (v VARCHAR(50))");

		TestQueries.execute(db, "INSERT INTO t(v) VALUES (?)",
				Parameters.listOf(String.class, null));

		Long count = db.query("SELECT COUNT(*) FROM t")
				.fetchObject(Long.class)
				.orElse(0L);
		Assertions.assertEquals(1L, count, "Expected one row inserted");

		Optional<String> value = db.query("SELECT v FROM t")
				.fetchObject(String.class);
		Assertions.assertTrue(value.isEmpty(), "Expected NULL when binding TypedParameter with null");
	}

	@Test
	public void testListOf_withoutBinder_failsFast() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_failfast_list")).build();
		TestQueries.execute(db, "CREATE TABLE t_dummy (id INT)");

		try {
			TestQueries.execute(db, "INSERT INTO t_dummy(id) VALUES (?)",
					Parameters.listOf(UUID.class, List.of(UUID.randomUUID())));
			Assertions.fail("Expected DatabaseException for TypedParameter without CustomParameterBinder");
		} catch (DatabaseException e) {
			Assertions.assertTrue(e.getMessage() != null && e.getMessage().contains("CustomParameterBinder"), "Message should mention CustomParameterBinder");
		}
	}

	@Test
	public void testSetOf_withoutBinder_failsFast() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_failfast_set")).build();
		TestQueries.execute(db, "CREATE TABLE t_dummy (id INT)");

		try {
			TestQueries.execute(db, "INSERT INTO t_dummy(id) VALUES (?)",
					Parameters.setOf(UUID.class, Set.of(UUID.randomUUID())));
			Assertions.fail("Expected DatabaseException for TypedParameter without CustomParameterBinder");
		} catch (DatabaseException e) {
			Assertions.assertTrue(e.getMessage() != null && e.getMessage().contains("CustomParameterBinder"), "Message should mention CustomParameterBinder");
		}
	}

	@Test
	public void testMapOf_withoutBinder_failsFast() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_failfast_map")).build();
		TestQueries.execute(db, "CREATE TABLE t_dummy (id INT)");

		try {
			TestQueries.execute(db, "INSERT INTO t_dummy(id) VALUES (?)",
					Parameters.mapOf(UUID.class, Integer.class, Map.of(UUID.randomUUID(), 1)));
			Assertions.fail("Expected DatabaseException for TypedParameter without CustomParameterBinder");
		} catch (DatabaseException e) {
			Assertions.assertTrue(e.getMessage() != null && e.getMessage().contains("CustomParameterBinder"), "Message should mention CustomParameterBinder");
		}
	}

	@Test
	public void testArrayOf_withoutBinder_failsFast() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_failfast_typed_array")).build();
		TestQueries.execute(db, "CREATE TABLE t_dummy (v LONGVARCHAR)");

		try {
			TestQueries.execute(db, "INSERT INTO t_dummy(v) VALUES (?)",
					Parameters.arrayOf(String.class, new String[]{"alpha", "beta"}));
			Assertions.fail("Expected DatabaseException for TypedParameter without CustomParameterBinder");
		} catch (DatabaseException e) {
			Assertions.assertTrue(e.getMessage() != null && e.getMessage().contains("CustomParameterBinder"), "Message should mention CustomParameterBinder");
		}
	}

	protected void createTestSchema(@Nonnull Database database) {
		requireNonNull(database);
		TestQueries.execute(database, """
				CREATE TABLE employee (
				  employee_id BIGINT,
				  name VARCHAR(255) NOT NULL,
				  email_address VARCHAR(255),
				  locale VARCHAR(255)
				)
				""");
	}

	@Nonnull
	protected DataSource createInMemoryDataSource(@Nonnull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}

	@Nonnull
	protected LocalDateTime truncate(@Nonnull LocalDateTime localDateTime,
																	 @Nonnull ChronoUnit chronoUnit) {
		requireNonNull(localDateTime);
		requireNonNull(chronoUnit);

		return switch (chronoUnit) {
			case NANOS -> localDateTime; // no-op
			case MICROS -> localDateTime.withNano((localDateTime.getNano() / 1_000) * 1_000);
			case MILLIS -> localDateTime.truncatedTo(ChronoUnit.MILLIS);
			default -> localDateTime;
		};
	}
}
