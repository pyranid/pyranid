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

import org.hsqldb.jdbc.JDBCDataSource;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Currency;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

	@Test
	public void testBasicQueries() {
		Database database = Database.forDataSource(createInMemoryDataSource("testBasicQueries")).build();

		createTestSchema(database);

		database.execute("INSERT INTO employee VALUES (1, 'Employee One', 'employee-one@company.com', NULL)");
		database.execute("INSERT INTO employee VALUES (2, 'Employee Two', NULL, NULL)");

		List<EmployeeRecord> employeeRecords = database.queryForList("SELECT * FROM employee ORDER BY name", EmployeeRecord.class);
		Assert.assertEquals("Wrong number of employees", 2, employeeRecords.size());
		Assert.assertEquals("Didn't detect DB column name override", "Employee One", employeeRecords.get(0).displayName());

		List<EmployeeClass> employeeClasses = database.queryForList("SELECT * FROM employee ORDER BY name", EmployeeClass.class);
		Assert.assertEquals("Wrong number of employees", 2, employeeClasses.size());
		Assert.assertEquals("Didn't detect DB column name override", "Employee One", employeeClasses.get(0).getDisplayName());
	}

	public record Product(Long productId, String name, BigDecimal price) {}

	@Test
	public void testTransactions() {
		Database database = Database.forDataSource(createInMemoryDataSource("testTransactions")).build();

		database.execute("CREATE TABLE product (product_id BIGINT, name VARCHAR(255) NOT NULL, price DECIMAL)");

		AtomicBoolean ranPostTransactionOperation = new AtomicBoolean(false);

		database.transaction(() -> {
			database.currentTransaction().get().addPostTransactionOperation((transactionResult -> {
				Assert.assertEquals("Wrong transaction result", TransactionResult.COMMITTED, transactionResult);
				ranPostTransactionOperation.set(true);
			}));

			database.execute("INSERT INTO product VALUES (1, 'VR Goggles', 3500.99)");

			Product product = database.queryForObject("""
					SELECT * 
					FROM product 
					WHERE product_id=?
					""", Product.class, 1L).orElse(null);

			Assert.assertNotNull("Product failed to insert", product);

			database.currentTransaction().get().rollback();

			product = database.queryForObject("""
					SELECT * 
					FROM product 
					WHERE product_id=?
					""", Product.class, 1L).orElse(null);

			Assert.assertNull("Product failed to roll back", product);
		});

		Assert.assertTrue("Did not run post-transaction operation", ranPostTransactionOperation.get());
	}

	@Test
	public void testDateAndTimeRoundTrips() {
		DataSource ds = createInMemoryDataSource("dt_roundtrips");
		ZoneId zone = ZoneId.of("America/New_York");

		Database db = Database.forDataSource(ds)
				.timeZone(zone)
				.build();

		// DATE <-> LocalDate
		LocalDate ld = LocalDate.of(2020, 1, 2);
		LocalDate ldRoundTrip = db.queryForObject("SELECT CAST(? AS DATE) FROM (VALUES (0)) AS t(x)", LocalDate.class, ld).get();
		Assert.assertEquals(ld, ldRoundTrip);

		// TIME <-> LocalTime (use second precision to avoid driver quirks)
		LocalTime lt = LocalTime.of(3, 4, 5);
		LocalTime ltRoundTrip = db.queryForObject("SELECT CAST(? AS TIME) FROM (VALUES (0)) AS t(x)", LocalTime.class, lt).get();
		Assert.assertEquals(lt, ltRoundTrip);
	}

	@Test
	public void testTimestampRoundTripsAllJavaTimeFlavors() {
		DataSource ds = createInMemoryDataSource("ts_roundtrips");
		ZoneId zone = ZoneId.of("America/New_York");

		Database db = Database.forDataSource(ds)
				.timeZone(zone)
				.build();

		// 1) LocalDateTime param
		LocalDateTime ldt = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000_000); // 123ms for JDBC-friendly precision
		// LocalDateTime -> TIMESTAMP -> LocalDateTime
		LocalDateTime ldtRoundTrip = db.queryForObject("SELECT CAST(? AS TIMESTAMP) FROM (VALUES (0)) AS t(x)", LocalDateTime.class, ldt).get();
		Assert.assertEquals("LocalDateTime round-trip mismatch", ldt, ldtRoundTrip);
		// LocalDateTime -> TIMESTAMP -> Instant (interpreted in DB zone)
		Instant expectedFromLdt = ldt.atZone(zone).toInstant().truncatedTo(ChronoUnit.MILLIS);
		Instant instFromLdt = db.queryForObject("SELECT CAST(? AS TIMESTAMP) FROM (VALUES (0)) AS t(x)", Instant.class, ldt).get().truncatedTo(ChronoUnit.MILLIS);
		Assert.assertEquals("LocalDateTime→Instant mapping mismatch", expectedFromLdt, instFromLdt);

		// 2) Instant param
		Instant instant = Instant.parse("2020-01-02T08:09:10.123Z");
		// Instant -> TIMESTAMP -> Instant (should be identity)
		Instant instRoundTrip = db.queryForObject("SELECT CAST(? AS TIMESTAMP) FROM (VALUES (0)) AS t(x)", Instant.class, instant).get().truncatedTo(ChronoUnit.MILLIS);
		Assert.assertEquals("Instant round-trip mismatch", instant.truncatedTo(ChronoUnit.MILLIS), instRoundTrip);
		// Instant -> TIMESTAMP -> LocalDateTime (in DB zone)
		LocalDateTime expectedLdtFromInstant = LocalDateTime.ofInstant(instant, zone);
		LocalDateTime ldtFromInstant = db.queryForObject("SELECT CAST(? AS TIMESTAMP) FROM (VALUES (0)) AS t(x)", LocalDateTime.class, instant).get();
		Assert.assertEquals("Instant→LocalDateTime mapping mismatch", expectedLdtFromInstant, ldtFromInstant);

		// 3) OffsetDateTime param (use odd offset and nanos to ensure normalization)
		OffsetDateTime odt = OffsetDateTime.parse("2020-01-02T08:09:10.123456789-03:00");
		Instant expectedFromOdt = odt.toInstant().truncatedTo(ChronoUnit.MILLIS);
		Instant instFromOdt = db.queryForObject("SELECT CAST(? AS TIMESTAMP) FROM (VALUES (0)) AS t(x)", Instant.class, odt).get().truncatedTo(ChronoUnit.MILLIS);
		Assert.assertEquals("OffsetDateTime→Instant mapping mismatch", expectedFromOdt, instFromOdt);
		LocalDateTime expectedLdtFromOdt = LocalDateTime.ofInstant(odt.toInstant(), zone);
		LocalDateTime ldtFromOdt = db.queryForObject("SELECT CAST(? AS TIMESTAMP) FROM (VALUES (0)) AS t(x)", LocalDateTime.class, odt).get();
		Assert.assertEquals("OffsetDateTime→LocalDateTime mapping mismatch", truncate(expectedLdtFromOdt, ChronoUnit.MICROS), truncate(ldtFromOdt, ChronoUnit.MICROS));
	}

	@Test
	public void testTimestampLiteralMappingRespectsDatabaseTimeZone() {
		// Same SQL literal interpreted under two different DB zones
		LocalDateTime ldt = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000_000);

		// NY DB
		DataSource dsNY = createInMemoryDataSource("ts_literal_ny");
		ZoneId ny = ZoneId.of("America/New_York");
		Database dbNY = Database.forDataSource(dsNY).timeZone(ny).build();
		Instant expectedNY = ldt.atZone(ny).toInstant().truncatedTo(ChronoUnit.MILLIS);
		Instant gotNY = dbNY.queryForObject("SELECT TIMESTAMP '2020-01-02 03:04:05.123' FROM (VALUES (0)) AS t(x)", Instant.class).get()
				.truncatedTo(ChronoUnit.MILLIS);
		Assert.assertEquals("NY literal TIMESTAMP→Instant mismatch", expectedNY, gotNY);

		// But LocalDateTime should be the literal value regardless of zone
		LocalDateTime gotNYLdt = dbNY.queryForObject("SELECT TIMESTAMP '2020-01-02 03:04:05.123' FROM (VALUES (0)) AS t(x)", LocalDateTime.class).get();
		Assert.assertEquals("NY literal TIMESTAMP→LocalDateTime mismatch", ldt, gotNYLdt);

		// UTC DB
		DataSource dsUTC = createInMemoryDataSource("ts_literal_utc");
		ZoneId utc = ZoneId.of("UTC");
		Database dbUTC = Database.forDataSource(dsUTC).timeZone(utc).build();
		Instant expectedUTC = ldt.atZone(utc).toInstant().truncatedTo(ChronoUnit.MILLIS);
		Instant gotUTC = dbUTC.queryForObject("SELECT TIMESTAMP '2020-01-02 03:04:05.123' FROM (VALUES (0)) AS t(x)", Instant.class).get()
				.truncatedTo(ChronoUnit.MILLIS);

		Assert.assertEquals("UTC literal TIMESTAMP→Instant mismatch", expectedUTC, gotUTC);
		LocalDateTime gotUTCLdt = dbUTC.queryForObject("SELECT TIMESTAMP '2020-01-02 03:04:05.123' FROM (VALUES (0)) AS t(x)", LocalDateTime.class).get();
		Assert.assertEquals("UTC literal TIMESTAMP→LocalDateTime mismatch", ldt, gotUTCLdt);
	}

	@Test
	public void testLegacySqlTypesRoundTrip() {
		DataSource ds = createInMemoryDataSource("legacy_sql_types");
		ZoneId zone = ZoneId.of("America/New_York");
		Database db = Database.forDataSource(ds).timeZone(zone).build();

		// java.sql.Timestamp
		java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2020-01-02 03:04:05.123");
		Instant instFromSqlTs = db.queryForObject("SELECT CAST(? AS TIMESTAMP) FROM (VALUES (0)) AS t(x)", Instant.class, ts).get().truncatedTo(ChronoUnit.MILLIS);
		Assert.assertEquals("java.sql.Timestamp→Instant mismatch",
				ts.toInstant().truncatedTo(ChronoUnit.MILLIS), instFromSqlTs);

		// java.sql.Date
		java.sql.Date sqlDate = java.sql.Date.valueOf("2020-01-02");
		LocalDate ldFromSqlDate = db.queryForObject("SELECT CAST(? AS DATE) FROM (VALUES (0)) AS t(x)", LocalDate.class, sqlDate).get();
		Assert.assertEquals("java.sql.Date→LocalDate mismatch", sqlDate.toLocalDate(), ldFromSqlDate);

		// java.sql.Time
		java.sql.Time sqlTime = java.sql.Time.valueOf("03:04:05");
		LocalTime ltFromSqlTime = db.queryForObject("SELECT CAST(? AS TIME) FROM (VALUES (0)) AS t(x)", LocalTime.class, sqlTime).get();
		Assert.assertEquals("java.sql.Time→LocalTime mismatch", sqlTime.toLocalTime(), ltFromSqlTime);
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
			public Optional<?> map(@Nonnull StatementContext<?> statementContext,
														 @Nonnull ResultSet resultSet,
														 @Nonnull Object resultSetValue,
														 @Nonnull TargetType targetType,
														 @Nullable Integer columnIndex,
														 @Nullable String columnLabel,
														 @Nonnull InstanceProvider instanceProvider) {
				// ignore DB value; force a deterministic value so we can assert override happened
				return Optional.of(Locale.CANADA);
			}
		};

		Database db = Database.forDataSource(dataSource)
				.resultSetMapper(ResultSetMapper.withCustomColumnMappers(List.of(localeOverride)).build())
				.build();

		createTestSchema(db);

		db.execute("INSERT INTO employee VALUES (1, 'A', 'a@x.com', 'en-US')");
		db.execute("INSERT INTO employee VALUES (2, 'B', 'b@x.com', 'ja-JP')");

		// JavaBean target
		EmployeeClass e1 = db.queryForObject("SELECT * FROM employee WHERE employee_id=1", EmployeeClass.class).orElse(null);
		Assert.assertNotNull(e1);
		Assert.assertEquals("Custom mapper did not override Locale for bean", Locale.CANADA, e1.getLocale());
		Assert.assertEquals("Raw locale should remain the DB string", "en-US", e1.getRawLocale());

		// Record target
		EmployeeRecord r2 = db.queryForObject("SELECT * FROM employee WHERE employee_id=2", EmployeeRecord.class).orElse(null);
		Assert.assertNotNull(r2);
		Assert.assertEquals("Custom mapper did not override Locale for record", Locale.CANADA, r2.locale());
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
			public Optional<?> map(@Nonnull StatementContext<?> statementContext,
														 @Nonnull ResultSet resultSet,
														 @Nonnull Object resultSetValue,
														 @Nonnull TargetType targetType,
														 @Nullable Integer columnIndex,
														 @Nullable String columnLabel,
														 @Nonnull InstanceProvider instanceProvider) {
				firstCalls.incrementAndGet();
				return Optional.empty();
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
			public Optional<?> map(@Nonnull StatementContext<?> statementContext,
														 @Nonnull ResultSet resultSet,
														 @Nonnull Object resultSetValue,
														 @Nonnull TargetType targetType,
														 @Nullable Integer columnIndex,
														 @Nullable String columnLabel,
														 @Nonnull InstanceProvider instanceProvider) {
				secondCalls.incrementAndGet();
				return Optional.of(Locale.GERMANY);
			}
		};

		Database db = Database.forDataSource(dataSource)
				.resultSetMapper(ResultSetMapper.withCustomColumnMappers(List.of(first, second)).build())
				.build();

		createTestSchema(db);

		// Insert multiple rows so the winner cache has a chance to kick in
		db.execute("INSERT INTO employee VALUES (1, 'A', 'a@x.com', 'en-US')");
		db.execute("INSERT INTO employee VALUES (2, 'B', 'b@x.com', 'fr-FR')");
		db.execute("INSERT INTO employee VALUES (3, 'C', 'c@x.com', 'ja-JP')");

		List<EmployeeClass> rows = db.queryForList("SELECT * FROM employee ORDER BY employee_id", EmployeeClass.class);
		Assert.assertEquals(3, rows.size());
		rows.forEach(e -> Assert.assertEquals("Winner mapper should set GERMANY", Locale.GERMANY, e.getLocale()));

		// Expected call pattern with positive winner cache:
		// Row1: first called (empty), second called (handles) -> cache winner=(String, Locale) -> second
		// Row2: second called only
		// Row3: second called only
		Assert.assertEquals("First mapper should be tried only on the first row", 1, firstCalls.get());
		Assert.assertEquals("Second mapper should handle every row", 3, secondCalls.get());
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
			public Optional<?> map(@Nonnull StatementContext<?> statementContext,
														 @Nonnull ResultSet resultSet,
														 @Nonnull Object resultSetValue,
														 @Nonnull TargetType targetType,
														 @Nullable Integer columnIndex,
														 @Nullable String columnLabel,
														 @Nonnull InstanceProvider instanceProvider) {
				String s = resultSetValue == null ? null : resultSetValue.toString();
				if (s == null || s.isBlank()) return Optional.of(List.of());

				List<UUID> uuids = Arrays.stream(s.split(","))
						.map(String::trim)
						.filter(str -> !str.isEmpty())
						.map(UUID::fromString)
						.collect(Collectors.toList());

				return Optional.of(uuids);
			}
		};

		Database db = Database.forDataSource(dataSource)
				.resultSetMapper(ResultSetMapper.withCustomColumnMappers(List.of(csvUuidList)).build())
				.build();

		// Simple schema for the test
		db.execute("""
				CREATE TABLE group_data (
				  group_name VARCHAR(255),
				  ids VARCHAR(2000)
				)
				""");

		UUID u1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
		UUID u2 = UUID.fromString("22222222-2222-2222-2222-222222222222");
		db.execute("INSERT INTO group_data VALUES (?, ?)", "alpha", u1 + "," + u2);

		// Note: property "groupName" should match DB column "group_name" via normalization logic
		GroupRow row = db.queryForObject("SELECT group_name, ids FROM group_data WHERE group_name=?", GroupRow.class, "alpha").orElse(null);

		Assert.assertNotNull(row);
		Assert.assertEquals("alpha", row.groupName());
		Assert.assertEquals(List.of(u1, u2), row.ids());
	}

	@Test
	public void testExecuteForObjectAndListUsingSelect() {
		Database db = Database.forDataSource(createInMemoryDataSource("exec_select")).build();
		db.execute("CREATE TABLE prod (id INT, name VARCHAR(64))");
		db.execute("INSERT INTO prod VALUES (1, 'A')");
		db.execute("INSERT INTO prod VALUES (2, 'B')");

		// Although executeForObject is meant for DML with RETURNING, it currently delegates to queryForObject.
		Optional<String> name = db.executeForObject("SELECT name FROM prod WHERE id = ?", String.class, 1);
		Assert.assertTrue(name.isPresent());
		Assert.assertEquals("A", name.get());

		List<Integer> ids = db.executeForList("SELECT id FROM prod ORDER BY id", Integer.class);
		Assert.assertEquals(List.of(1, 2), ids);
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
		Database db = Database.forDataSource(createInMemoryDataSource("prefs")).build();
		db.execute("CREATE TABLE prefs (tz VARCHAR(64), locale VARCHAR(32), currency VARCHAR(3))");

		ZoneId zone = ZoneId.of("America/New_York");
		Locale loc = Locale.CANADA;
		Currency cur = Currency.getInstance("USD");

		db.execute("INSERT INTO prefs (tz, locale, currency) VALUES (?, ?, ?)", zone, loc, cur);

		Prefs prefs = db.queryForObject("SELECT * FROM prefs", Prefs.class).orElseThrow();
		Assert.assertEquals(zone, prefs.getTz());
		Assert.assertEquals(loc, prefs.getLocale());
		Assert.assertEquals(cur, prefs.getCurrency());
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
		Database db = Database.forDataSource(createInMemoryDataSource("nulls")).build();
		db.execute("CREATE TABLE foo (id INT, name VARCHAR(64))");
		db.execute("INSERT INTO foo (id, name) VALUES (?, ?)", 1, null);

		Foo row = db.queryForObject("SELECT * FROM foo", Foo.class).orElseThrow();
		Assert.assertEquals(Integer.valueOf(1), row.getId());
		Assert.assertNull(row.getName());
	}

	@Test
	public void testExamineDatabaseMetaData() {
		Database db = Database.forDataSource(createInMemoryDataSource("metadata")).build();
		final AtomicInteger seen = new AtomicInteger(0);
		db.examineDatabaseMetaData(meta -> {
			Assert.assertNotNull(meta.getDatabaseProductName());
			seen.incrementAndGet();
		});
		Assert.assertEquals(1, seen.get());
	}

	@Test
	public void testStatementLoggerReceivesEvent() {
		Database db = Database.forDataSource(createInMemoryDataSource("logger")).statementLogger((log) -> {
			Assert.assertNotNull("StatementContext should be present", log.getStatementContext());
			Assert.assertTrue("SQL should be present", log.getStatementContext().getStatement().getSql().length() > 0);
		}).build();

		db.execute("CREATE TABLE z (id INT)");
		db.execute("INSERT INTO z VALUES (1)");
	}

	protected void createTestSchema(@Nonnull Database database) {
		requireNonNull(database);
		database.execute("""
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
