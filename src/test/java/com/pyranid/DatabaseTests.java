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

import com.pyranid.DefaultResultSetMapper.CustomColumnMapper;
import org.hsqldb.jdbc.JDBCDataSource;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 2.0.0
 */
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
	public void testCustomDatabase() {
		DataSource dataSource = createInMemoryDataSource("testCustomDatabase");

		// Override JVM default timezone
		ZoneId timeZone = ZoneId.of("UTC");

		InstanceProvider instanceProvider = new DefaultInstanceProvider() {
			@Override
			@Nonnull
			public <T> T provide(@Nonnull StatementContext<T> statementContext,
													 @Nonnull Class<T> instanceType) {
				if (Objects.equals("employee-query", statementContext.getStatement().getId()))
					System.out.printf("Creating instance of %s for Employee Query: %s\n",
							instanceType.getSimpleName(), statementContext);

				return super.provide(statementContext, instanceType);
			}
		};

		ResultSetMapper resultSetMapper = new DefaultResultSetMapper() {
			@Nonnull
			@Override
			public <T> Optional<T> map(@Nonnull StatementContext<T> statementContext,
																 @Nonnull ResultSet resultSet,
																 @Nonnull Class<T> resultSetRowType,
																 @Nonnull InstanceProvider instanceProvider) {
				if (Objects.equals("employee-query", statementContext.getStatement().getId()))
					System.out.printf("Mapping ResultSet for Employee Query: %s\n", statementContext);

				return super.map(statementContext, resultSet, resultSetRowType, instanceProvider);
			}
		};

		PreparedStatementBinder preparedStatementBinder = new DefaultPreparedStatementBinder() {
			@Override
			public <T> void bindParameter(@Nonnull StatementContext<T> statementContext,
																		@Nonnull PreparedStatement preparedStatement,
																		@Nonnull Object parameter,
																		@Nonnull Integer parameterIndex) throws SQLException {
				if (Objects.equals("employee-query", statementContext.getStatement().getId()))
					System.out.printf("Binding Employee Query parameter %d (%s): Statement context was %s\n",
							parameterIndex, parameter, statementContext);

				super.bindParameter(statementContext, preparedStatement, parameter, parameterIndex);
			}
		};

		StatementLogger statementLogger = new StatementLogger() {
			@Override
			public void log(StatementLog statementLog) {
				// Send log to whatever output sink you'd like
				if (Objects.equals("employee-query", statementLog.getStatementContext().getStatement().getId()))
					System.out.printf("Completed Employee Query: %s\n", statementLog);
			}
		};

		Database customDatabase = Database.forDataSource(dataSource)
				.timeZone(timeZone)
				.instanceProvider(instanceProvider)
				.resultSetMapper(resultSetMapper)
				.preparedStatementBinder(preparedStatementBinder)
				.statementLogger(statementLogger)
				.build();

		createTestSchema(customDatabase);

		customDatabase.execute("INSERT INTO employee VALUES (?, 'Employee One', 'employee-one@company.com', NULL)", 1);
		customDatabase.execute("INSERT INTO employee VALUES (2, 'Employee Two', NULL, NULL)");

		List<EmployeeRecord> employeeRecords = customDatabase.queryForList("SELECT * FROM employee ORDER BY name", EmployeeRecord.class);
		Assert.assertEquals("Wrong number of employees", 2, employeeRecords.size());
		Assert.assertEquals("Didn't detect DB column name override", "Employee One", employeeRecords.get(0).displayName());

		List<EmployeeClass> employeeClasses = customDatabase.queryForList("SELECT * FROM employee ORDER BY name", EmployeeClass.class);
		Assert.assertEquals("Wrong number of employees", 2, employeeClasses.size());
		Assert.assertEquals("Didn't detect DB column name override", "Employee One", employeeClasses.get(0).getDisplayName());

		EmployeeClass employee = customDatabase.queryForObject(Statement.of("employee-query", """
				SELECT *
				FROM employee
				WHERE email_address=?
				"""), EmployeeClass.class, "employee-one@company.com").orElse(null);

		Assert.assertNotNull("Could not find employee", employee);

		List<List<Object>> parameterGroups = List.of(
				List.of(3, "Employee Three", "employee-three@company.com", Locale.US),
				List.of(4, "Employee Four", "employee-four@company.com", Locale.JAPAN)
		);

		List<Long> updateCounts = customDatabase.executeBatch("INSERT INTO employee VALUES (?,?,?,?)", parameterGroups);

		Assert.assertEquals("Wrong number of update counts", 2, updateCounts.size());
		Assert.assertEquals("Wrong update count 1", 1, (long) updateCounts.get(0));
		Assert.assertEquals("Wrong update count 2", 1, (long) updateCounts.get(1));

		EmployeeClass employeeWithLocale = customDatabase.queryForObject("""
				SELECT *
				FROM employee
				WHERE email_address=?
				""", EmployeeClass.class, "employee-three@company.com").orElse(null);

		Assert.assertNotNull("Unable to fetch employee with locale", employeeWithLocale);
		Assert.assertEquals("Locale mismatch", Locale.US, employeeWithLocale.getLocale());
		Assert.assertEquals("Raw locale mismatch", "en-US", employeeWithLocale.getRawLocale());
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
		DataSource dataSource = createInMemoryDataSource("testCustomColumnMapper");

		List<CustomColumnMapper> customColumnMappers = List.of(
				new CustomColumnMapper() {
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
																 @Nonnull InstanceProvider instanceProvider) throws SQLException {
						// TODO: implement tests
						return Optional.empty();
					}
				}
		);

		Database customDatabase = Database.forDataSource(dataSource)
				.resultSetMapper(new DefaultResultSetMapper(customColumnMappers))
				.build();

		createTestSchema(customDatabase);
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
