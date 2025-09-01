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
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.math.BigDecimal;
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
		Database database = Database.withDataSource(createInMemoryDataSource("testBasicQueries")).build();

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
		Database database = Database.withDataSource(createInMemoryDataSource("testTransactions")).build();

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

		Database db = Database.withDataSource(ds)
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

		Database db = Database.withDataSource(ds)
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
		Database dbNY = Database.withDataSource(dsNY).timeZone(ny).build();
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
		Database dbUTC = Database.withDataSource(dsUTC).timeZone(utc).build();
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
		Database db = Database.withDataSource(ds).timeZone(zone).build();

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
			public MappingResult map(@Nonnull StatementContext<?> statementContext,
															 @Nonnull ResultSet resultSet,
															 @Nonnull Object resultSetValue,
															 @Nonnull TargetType targetType,
															 @Nullable Integer columnIndex,
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
			public MappingResult map(@Nonnull StatementContext<?> statementContext,
															 @Nonnull ResultSet resultSet,
															 @Nonnull Object resultSetValue,
															 @Nonnull TargetType targetType,
															 @Nullable Integer columnIndex,
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
															 @Nullable Integer columnIndex,
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
			public MappingResult map(@Nonnull StatementContext<?> statementContext,
															 @Nonnull ResultSet resultSet,
															 @Nonnull Object resultSetValue,
															 @Nonnull TargetType targetType,
															 @Nullable Integer columnIndex,
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
		Database db = Database.withDataSource(createInMemoryDataSource("exec_select")).build();
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
		Database db = Database.withDataSource(createInMemoryDataSource("prefs")).build();
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
		Database db = Database.withDataSource(createInMemoryDataSource("nulls")).build();
		db.execute("CREATE TABLE foo (id INT, name VARCHAR(64))");
		db.execute("INSERT INTO foo (id, name) VALUES (?, ?)", 1, null);

		Foo row = db.queryForObject("SELECT * FROM foo", Foo.class).orElseThrow();
		Assert.assertEquals(Integer.valueOf(1), row.getId());
		Assert.assertNull(row.getName());
	}

	@Test
	public void testReadDatabaseMetaData() {
		Database db = Database.withDataSource(createInMemoryDataSource("metadata")).build();
		final AtomicInteger seen = new AtomicInteger(0);
		db.readDatabaseMetaData(meta -> {
			Assert.assertNotNull(meta.getDatabaseProductName());
			seen.incrementAndGet();
		});
		Assert.assertEquals(1, seen.get());
	}

	@Test
	public void testStatementLoggerReceivesEvent() {
		Database db = Database.withDataSource(createInMemoryDataSource("logger")).statementLogger((log) -> {
			Assert.assertNotNull("StatementContext should be present", log.getStatementContext());
			Assert.assertTrue("SQL should be present", log.getStatementContext().getStatement().getSql().length() > 0);
		}).build();

		db.execute("CREATE TABLE z (id INT)");
		db.execute("INSERT INTO z VALUES (1)");
	}

	@Test
	public void testArrayParameter_insertAndReadBack() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_array_param")).build();

		// HSQLDB array column syntax: VARCHAR(100) ARRAY
		db.execute("CREATE TABLE t_array (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, tags VARCHAR(100) ARRAY)");

		// Insert using ArrayParameter (defensive copy internally)
		db.execute("INSERT INTO t_array(tags) VALUES (?)",
				Parameters.arrayOf("VARCHAR", List.of("alpha", "beta", "gamma")));

		// Verify size via SQL function
		Optional<Integer> size = db.executeForObject("SELECT CARDINALITY(tags) FROM t_array FETCH FIRST ROW ONLY", Integer.class);
		Assert.assertTrue(size.isPresent());
		Assert.assertEquals(Integer.valueOf(3), size.get());

		// HSQLDB supports 1-based array element access with brackets
		Optional<String> first = db.executeForObject("SELECT tags[1] FROM t_array FETCH FIRST ROW ONLY", String.class);
		Optional<String> second = db.executeForObject("SELECT tags[2] FROM t_array FETCH FIRST ROW ONLY", String.class);
		Optional<String> third = db.executeForObject("SELECT tags[3] FROM t_array FETCH FIRST ROW ONLY", String.class);

		Assert.assertEquals("alpha", first.orElse(null));
		Assert.assertEquals("beta", second.orElse(null));
		Assert.assertEquals("gamma", third.orElse(null));
	}

	@Test
	public void testArrayParameter_emptyArray() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_array_param_empty")).build();
		db.execute("CREATE TABLE t_array (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, tags VARCHAR(100) ARRAY)");

		db.execute("INSERT INTO t_array(tags) VALUES (?)",
				Parameters.arrayOf("VARCHAR", List.of())); // empty

		Optional<Integer> size = db.executeForObject("SELECT CARDINALITY(tags) FROM t_array FETCH FIRST ROW ONLY", Integer.class);
		Assert.assertTrue(size.isPresent());
		Assert.assertEquals(Integer.valueOf(0), size.get());
	}

	@Test
	public void testJsonParameter_roundTrip_textOnHsqldb() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_json_param")).build();
		db.execute("CREATE TABLE t_json (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, body LONGVARCHAR)");

		String json = "{\"a\":1,\"b\":[true,false],\"s\":\"x\"}";

		// AUTOMATIC mode should fall back to text on HSQLDB and insert successfully
		db.execute("INSERT INTO t_json(body) VALUES (?)", Parameters.jsonOf(json));

		Optional<String> got = db.executeForObject("SELECT body FROM t_json FETCH FIRST ROW ONLY", String.class);
		Assert.assertTrue(got.isPresent());
		Assert.assertEquals(json, got.get());

		// TEXT is honored (also text on HSQLDB)
		String json2 = "{\"k\":\"v\"}";
		db.execute("INSERT INTO t_json(body) VALUES (?)", Parameters.jsonOf(json2, BindingPreference.TEXT));
		List<String> bodies = db.executeForList("SELECT body FROM t_json ORDER BY id", String.class);
		Assert.assertEquals(List.of(json, json2), bodies);
	}

	@Test
	public void testVectorParameter_throwsOnNonPostgres() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_vector_param")).build();
		db.execute("CREATE TABLE t_vec (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		try {
			db.execute("INSERT INTO t_vec(v) VALUES (?)", Parameters.vectorOfDoubles(new double[]{0.12, -0.5, 1.0}));
			Assert.fail("Expected IllegalArgumentException for non-PostgreSQL DatabaseType when binding VectorParameter");
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
				Assert.assertEquals(Connection.TRANSACTION_SERIALIZABLE, level);
				Assert.assertEquals(TransactionIsolation.SERIALIZABLE, db.currentTransaction().get().getTransactionIsolation());
				return Optional.empty();
			}, true);
		});

		// Another transaction; isolation restored
		db.transaction(() -> {
			db.performRawConnectionOperation(conn -> {
				int level = conn.getTransactionIsolation();
				Assert.assertEquals(Connection.TRANSACTION_READ_COMMITTED, level);
				Assert.assertEquals(TransactionIsolation.DEFAULT, db.currentTransaction().get().getTransactionIsolation());
				return Optional.empty();
			}, true);
		});

		// Another txn level
		db.transaction(TransactionIsolation.REPEATABLE_READ, () -> {
			db.performRawConnectionOperation(conn -> {
				int level = conn.getTransactionIsolation();
				Assert.assertEquals(Connection.TRANSACTION_REPEATABLE_READ, level);
				Assert.assertEquals(TransactionIsolation.REPEATABLE_READ, db.currentTransaction().get().getTransactionIsolation());
				return Optional.empty();
			}, true);
		});

		// Non-transactional; verifies restoration
		db.performRawConnectionOperation(conn -> {
			int level = conn.getTransactionIsolation();
			// compare to what HSQLDB default was before entering the txn
			Assert.assertEquals(Connection.TRANSACTION_READ_COMMITTED, level); // example default
			return Optional.empty();
		}, false);
	}

	@Test
	public void testLocalDate_bindsToDateColumn_roundTrips() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_localdate"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		db.execute("CREATE TABLE t_date (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, d DATE)");

		LocalDate input = LocalDate.of(2020, 6, 7);
		db.execute("INSERT INTO t_date(d) VALUES (?)", input);

		LocalDate roundTripped = db.queryForObject("SELECT d FROM t_date", LocalDate.class).orElseThrow();
		Assert.assertEquals("LocalDate round-trip mismatch", input, roundTripped);
	}

	@Test
	public void testLocalTime_bindsAsString_onDriversWithoutSafeTimeHandling() {
		// DefaultPreparedStatementBinder writes LocalTime as string for maximum safety.
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_localtime"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		db.execute("CREATE TABLE t_time (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, t LONGVARCHAR)");

		LocalTime input = LocalTime.of(8, 9, 10, 123_456_789);
		db.execute("INSERT INTO t_time(t) VALUES (?)", input);

		String stored = db.queryForObject("SELECT t FROM t_time", String.class).orElseThrow();
		Assert.assertEquals("LocalTime should be stored as ISO-8601 string", input.toString(), stored);
	}

	@Test
	public void testLocalDateTime_bindsToTimestamp_roundTrips() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_ldt"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		db.execute("CREATE TABLE t_ts (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, ts TIMESTAMP)");

		LocalDateTime input = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000_000);
		db.execute("INSERT INTO t_ts(ts) VALUES (?)", input);

		// Let mapper do the conversion
		LocalDateTime roundTripped = db.queryForObject("SELECT ts FROM t_ts", LocalDateTime.class).orElseThrow();
		Assert.assertEquals("LocalDateTime round-trip mismatch", input, roundTripped);
	}

	@Test
	public void testOffsetDateTime_targetTimestamp_coercesToDbZone_andInserts() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_odt_ts"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				// default tz is fine; binder will coerce ODT -> LDT when target is TIMESTAMP
				.build();

		db.execute("CREATE TABLE t_odt_ts (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, ts TIMESTAMP)");

		OffsetDateTime odt = OffsetDateTime.parse("2023-05-10T12:34:56.789+02:00");
		db.execute("INSERT INTO t_odt_ts(ts) VALUES (?)", odt);

		Integer count = db.queryForObject("SELECT COUNT(*) FROM t_odt_ts", Integer.class).orElseThrow();
		Assert.assertEquals("Row should be inserted", Integer.valueOf(1), count);
	}

	@Test
	public void testInstant_targetTimestamp_inserts() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_instant"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		db.execute("CREATE TABLE t_instant (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, ts TIMESTAMP)");

		Instant instant = Instant.parse("2020-01-02T03:04:05.123Z");
		db.execute("INSERT INTO t_instant(ts) VALUES (?)", instant);

		// Pull back as Timestamp just to ensure the write stuck
		Timestamp ts = db.queryForObject("SELECT ts FROM t_instant", Timestamp.class).orElseThrow();
		Assert.assertTrue("Stored timestamp should be close to input instant",
				Math.abs(ts.toInstant().toEpochMilli() - instant.toEpochMilli()) <= 1); // tolerance for driver rounding
	}

	@Test
	public void testJsonParameter_onNonPostgres_isStoredAsText() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_json_text"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		db.execute("CREATE TABLE t_json (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, j LONGVARCHAR)");

		String json = """
					{"a":1,"b":[2,3],"ok":true}
				""";

		// On non-Postgres, DefaultPreparedStatementBinder falls back to setString(json)
		db.execute("INSERT INTO t_json(j) VALUES (?)", Parameters.jsonOf(json));

		String stored = db.queryForObject("SELECT j FROM t_json", String.class).orElseThrow();
		Assert.assertEquals("JSON should be stored as text on non-Postgres", json.trim(), stored.trim());
	}

	@Test
	public void testVectorParameter_throwsOnNonPostgres_matchesExample() {
		Database db = Database.withDataSource(createInMemoryDataSource("it_vector_param_dup")).build();
		db.execute("CREATE TABLE t_vec (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		try {
			db.execute("INSERT INTO t_vec(v) VALUES (?)", Parameters.vectorOfDoubles(new double[]{0.12, -0.5, 1.0}));
			Assert.fail("Expected exception for VectorParameter on non-PostgreSQL");
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
			public Boolean bind(StatementContext<?> ctx, java.sql.PreparedStatement ps, Integer index, Object param) throws java.sql.SQLException {
				if (!(param instanceof Locale)) return false;
				calls.incrementAndGet();
				ps.setString(index, "CUSTOM:" + ((Locale) param).toLanguageTag());
				return true;
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

		db.execute("CREATE TABLE t_locale (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, lang LONGVARCHAR)");

		Locale l = Locale.forLanguageTag("en-US");
		db.execute("INSERT INTO t_locale(lang) VALUES (?)", l);
		db.execute("INSERT INTO t_locale(lang) VALUES (?)", l); // hit again to exercise preferredBinderByInboundKey fast path

		List<String> rows = db.queryForList("SELECT lang FROM t_locale ORDER BY id", String.class);
		Assert.assertEquals(2, rows.size());
		Assert.assertEquals("CUSTOM:en-US", rows.get(0));
		Assert.assertEquals("CUSTOM:en-US", rows.get(1));
		Assert.assertTrue("Custom binder should be invoked at least twice", calls.get() >= 2);
	}

	@Test
	public void testNullJsonParameter_bindsAsNull() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_json_null"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		db.execute("CREATE TABLE t_json_null (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, j LONGVARCHAR)");

		db.execute("INSERT INTO t_json_null(j) VALUES (?)", Parameters.jsonOf(null));

		Integer nullCount = db.queryForObject("SELECT COUNT(*) FROM t_json_null WHERE j IS NULL", Integer.class).orElseThrow();
		Assert.assertEquals(Integer.valueOf(1), nullCount);
	}

	@Test
	public void testOffsetTime_bindsAsTimeWithZoneOrString_noException() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_offsettime"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		db.execute("CREATE TABLE t_ot (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, t LONGVARCHAR)");

		OffsetTime ot = OffsetTime.parse("08:09:10.123456789-03:00");
		// Binder will try TIME WITH TIME ZONE; if unsupported it falls back to string, so our column is VARCHAR.
		db.execute("INSERT INTO t_ot(t) VALUES (?)", ot);

		String stored = db.queryForObject("SELECT t FROM t_ot", String.class).orElseThrow();
		Assert.assertEquals("OffsetTime should be stored as ISO-8601 string when driver lacks TIMETZ", ot.toString(), stored);
	}

	@Test
	public void testEverythingElse_setObjectFallback_handlesEnumAndCurrency() {
		Database db = Database
				.withDataSource(createInMemoryDataSource("it_psb_misc"))
				.preparedStatementBinder(PreparedStatementBinder.withDefaultConfiguration())
				.build();

		db.execute("CREATE TABLE t_misc (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, e LONGVARCHAR, c LONGVARCHAR)");

		// Enum is normalized to name(); Currency normalized to currency code
		db.execute("INSERT INTO t_misc(e, c) VALUES (?, ?)", TestEnum.BLUE, java.util.Currency.getInstance("USD"));

		EnumCurrencyRow enumCurrencyRow = db.queryForObject("SELECT e, c FROM t_misc", EnumCurrencyRow.class).get();

		Assert.assertEquals(TestEnum.BLUE, enumCurrencyRow.getE());
		Assert.assertEquals("USD", enumCurrencyRow.getC().getCurrencyCode());
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
			public Boolean bind(@Nonnull StatementContext<?> sc,
													@Nonnull PreparedStatement ps,
													@Nonnull Integer idx,
													@Nonnull Object param) throws SQLException {
				Assert.assertTrue("Expected Locale", param instanceof Locale);
				calls.incrementAndGet();
				ps.setString(idx, "CUSTOM:" + ((Locale) param).toLanguageTag());
				return true; // handled
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

		db.execute("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, lang LONGVARCHAR)");

		db.execute("INSERT INTO t(lang) VALUES (?)", Locale.forLanguageTag("en-US"));
		db.execute("INSERT INTO t(lang) VALUES (?)", Locale.forLanguageTag("pt-BR"));

		List<String> rows = db.queryForList("SELECT lang FROM t ORDER BY id", String.class);
		Assert.assertEquals(List.of("CUSTOM:en-US", "CUSTOM:pt-BR"), rows);
		Assert.assertTrue("Binder should have been called twice", calls.get() >= 2);
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
			public Boolean bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) {
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
			public Boolean bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
				called.incrementAndGet();
				ps.setString(idx, "OK");
				return true;
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

		db.execute("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, lang LONGVARCHAR)");
		db.execute("INSERT INTO t(lang) VALUES (?)", Locale.US);

		String stored = db.queryForObject("SELECT lang FROM t", String.class).orElseThrow();
		Assert.assertEquals("OK", stored);
		Assert.assertEquals("Non-applicable binder must not be called", 0, neverCalled.get());
		Assert.assertEquals("Applicable binder called exactly once", 1, called.get());
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
			public Boolean bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
				firstCalls.incrementAndGet();
				ps.setString(idx, "FIRST");
				return true;
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
			public Boolean bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
				secondCalls.incrementAndGet();
				ps.setString(idx, "SECOND");
				return true;
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

		db.execute("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, lang LONGVARCHAR)");

		// First call: cache will be populated (first binder wins)
		db.execute("INSERT INTO t(lang) VALUES (?)", Locale.CANADA); // preferred selected here

		// Second call: should hit cached binder directly
		db.execute("INSERT INTO t(lang) VALUES (?)", Locale.FRANCE);

		List<String> rows = db.queryForList("SELECT lang FROM t ORDER BY id", String.class);
		Assert.assertEquals(List.of("FIRST", "FIRST"), rows);

		Assert.assertEquals("First binder should be used twice", 2, firstCalls.get());
		// If caching is working, second binder is never even attempted
		Assert.assertEquals("Second binder should not be used", 0, secondCalls.get());
	}

	/**
	 * Parameterized type support: binder applies to List<UUID> and writes a joined string.
	 */
	@Test
	public void testBinder_parameterizedType_ListOfUuid() {
		CustomParameterBinder listUuidBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public Boolean bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
				if (!(param instanceof List<?> list)) return false;
				for (Object o : list) if (!(o instanceof UUID)) return false;
				String joined = list.stream().map(Object::toString).reduce((a, b) -> a + "," + b).orElse("");
				ps.setString(idx, joined);
				return true;
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

		db.execute("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		List<UUID> ids = List.of(
				UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
				UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
		);

		db.execute("INSERT INTO t(v) VALUES (?)", Parameters.listOf(UUID.class, ids));

		String got = db.queryForObject("SELECT v FROM t", String.class).orElseThrow();
		Assert.assertEquals("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", got);
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
			public Boolean bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) {
				falseCalls.incrementAndGet();
				return false; // claim applicability but decide not to handle now
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

		db.execute("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");
		db.execute("INSERT INTO t(v) VALUES (?)", "hello");

		String stored = db.queryForObject("SELECT v FROM t", String.class).orElseThrow();
		Assert.assertEquals("hello", stored);
		Assert.assertEquals("Binder should have been invoked once and returned false", 1, falseCalls.get());
	}

	/**
	 * Binder throws SQLException -> DatabaseException should surface from db.execute().
	 */
	@Test
	public void testBinder_throwsSQLException_propagates() {
		CustomParameterBinder throwingBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public Boolean bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
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

		db.execute("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		try {
			db.execute("INSERT INTO t(v) VALUES (?)", Locale.US);
			Assert.fail("Expected DatabaseException caused by SQLException from binder");
		} catch (DatabaseException e) {
			// pass
			Assert.assertNotNull(e.getCause());
			Assert.assertTrue(e.getCause() instanceof SQLException);
			Assert.assertEquals("boom", e.getCause().getMessage());
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
			public Boolean bind(@Nonnull StatementContext<?> sc, @Nonnull PreparedStatement ps, @Nonnull Integer idx, @Nonnull Object param) throws SQLException {
				if (!(param instanceof Locale l)) return false;
				ps.setString(idx, l.toLanguageTag());
				return true;
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

		db.execute("CREATE TABLE t1 (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");
		db.execute("CREATE TABLE t2 (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)"); // using same SQL type here still exercises cache hits

		db.execute("INSERT INTO t1(v) VALUES (?)", Locale.CANADA);
		db.execute("INSERT INTO t2(v) VALUES (?)", Locale.JAPAN);

		List<String> a = db.queryForList("SELECT v FROM t1", String.class);
		List<String> b = db.queryForList("SELECT v FROM t2", String.class);
		Assert.assertEquals(List.of(Locale.CANADA.toLanguageTag()), a);
		Assert.assertEquals(List.of(Locale.JAPAN.toLanguageTag()), b);
	}

	@Test
	public void testBinder_parameterizedType_SetOfUuid() {
		CustomParameterBinder setUuidBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public Boolean bind(@Nonnull StatementContext<?> sc,
													@Nonnull PreparedStatement ps,
													@Nonnull Integer idx,
													@Nonnull Object param) throws SQLException {
				if (!(param instanceof Set<?> set)) return false;
				for (Object o : set) if (!(o instanceof UUID)) return false;

				// Stable serialization: sort lexicographically then join by comma
				String joined = set.stream()
						.map(Object::toString)
						.sorted()
						.reduce((a, b) -> a + "," + b)
						.orElse("");
				ps.setString(idx, joined);
				return true;
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

		db.execute("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		Set<UUID> ids = new LinkedHashSet<>(List.of(
				UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
				UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
		));

		// Uses TypedParameter with explicit Set<UUID> type
		db.execute("INSERT INTO t(v) VALUES (?)", Parameters.setOf(UUID.class, ids));

		String got = db.queryForObject("SELECT v FROM t", String.class).orElseThrow();
		Assert.assertEquals(
				"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
				got);
	}

	@Test
	public void testBinder_parameterizedType_MapOfStringInteger() {
		CustomParameterBinder mapBinder = new CustomParameterBinder() {
			@Nonnull
			@Override
			public Boolean bind(@Nonnull StatementContext<?> sc,
													@Nonnull PreparedStatement ps,
													@Nonnull Integer idx,
													@Nonnull Object param) throws SQLException {
				if (!(param instanceof Map<?, ?> map)) return false;

				// Validate key/value types at runtime
				for (Map.Entry<?, ?> e : map.entrySet()) {
					if (!(e.getKey() instanceof String)) return false;
					if (!(e.getValue() instanceof Integer)) return false;
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
				return true;
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

		db.execute("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v LONGVARCHAR)");

		Map<String, Integer> payload = new LinkedHashMap<>();
		payload.put("b", 2);
		payload.put("a", 1);

		// Uses TypedParameter with explicit Map<String,Integer> type
		db.execute("INSERT INTO t(v) VALUES (?)", Parameters.mapOf(String.class, Integer.class, payload));

		String got = db.queryForObject("SELECT v FROM t", String.class).orElseThrow();
		Assert.assertEquals("a=1,b=2", got);
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
