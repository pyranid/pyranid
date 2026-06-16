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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

abstract class AbstractPortableJdbcIntegrationTests {
	public record PersonRow(Long personId, String name, String emailAddress, Locale locale) {}
	public record NullableRow(Long itemId, String note) {}
	public record NumericRow(Integer intValue, Long longValue, BigDecimal decimalValue, Double doubleValue) {}
	public record TemporalRow(LocalDate eventDate, LocalTime eventTime, LocalDateTime eventTimestamp) {}

	public static class PersonBean {
		private Long personId;
		private String name;
		private String emailAddress;
		private Locale locale;

		public Long getPersonId() {
			return this.personId;
		}

		public void setPersonId(Long personId) {
			this.personId = personId;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
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
	}

	@NonNull
	protected abstract DataSource dataSource();

	@NonNull
	protected abstract String generatedKeyTableSql(@NonNull String tableName);

	@Test
	public void testDatabaseTypeDetectionUsesExpectedType() {
		Database db = database();

		Assertions.assertEquals(expectedDatabaseType(), db.getDatabaseType());
	}

	@Test
	public void testNamedParametersAndResultMapping() {
		Database db = database();
		String table = "pyranid_portable_people";
		createPeopleTable(db, table);

		db.query("INSERT INTO " + table + " (person_id, name, email_address, locale) VALUES (:personId, :name, :emailAddress, :locale)")
				.bind("personId", 1L)
				.bind("name", "Ada")
				.bind("emailAddress", "ada@example.com")
				.bind("locale", Locale.forLanguageTag("en-US"))
				.execute();
		db.query("INSERT INTO " + table + " (person_id, name, email_address, locale) VALUES (:personId, :name, :emailAddress, :locale)")
				.bindAll(Map.of(
						"personId", 2L,
						"name", "Grace",
						"emailAddress", "grace@example.com",
						"locale", Locale.forLanguageTag("en-GB")))
				.execute();

		String email = db.query("SELECT email_address FROM " + table + " WHERE person_id = :personId")
				.bind("personId", 2L)
				.fetchObject(String.class)
				.orElseThrow();
		List<String> names = db.query("SELECT name FROM " + table + " WHERE person_id IN (:ids) ORDER BY person_id")
				.bind("ids", Parameters.inList(List.of(1L, 2L)))
				.fetchList(String.class);
		PersonRow record = db.query("SELECT person_id, name, email_address, locale FROM " + table + " WHERE person_id = :personId")
				.bind("personId", 1L)
				.fetchObject(PersonRow.class)
				.orElseThrow();
		PersonBean bean = db.query("SELECT person_id, name, email_address, locale FROM " + table + " WHERE person_id = :personId")
				.bind("personId", 2L)
				.fetchObject(PersonBean.class)
				.orElseThrow();

		Assertions.assertEquals("grace@example.com", email);
		Assertions.assertEquals(List.of("Ada", "Grace"), names);
		Assertions.assertEquals(Long.valueOf(1L), record.personId());
		Assertions.assertEquals("Ada", record.name());
		Assertions.assertEquals(Locale.forLanguageTag("en-US"), record.locale());
		Assertions.assertEquals(Long.valueOf(2L), bean.getPersonId());
		Assertions.assertEquals("Grace", bean.getName());
		Assertions.assertEquals("grace@example.com", bean.getEmailAddress());
		Assertions.assertEquals(Locale.forLanguageTag("en-GB"), bean.getLocale());
	}

	@Test
	public void testNullBindingAndResultMapping() {
		Database db = database();
		String table = "pyranid_nullable_items";
		recreateTable(db, table, "CREATE TABLE " + table + " (item_id BIGINT PRIMARY KEY, note VARCHAR(100) NULL)");

		db.query("INSERT INTO " + table + " (item_id, note) VALUES (:itemId, :note)")
				.bind("itemId", 1L)
				.bind("note", null)
				.execute();

		NullableRow row = db.query("SELECT item_id, note FROM " + table + " WHERE note IS NULL")
				.fetchObject(NullableRow.class)
				.orElseThrow();

		Assertions.assertEquals(Long.valueOf(1L), row.itemId());
		Assertions.assertNull(row.note());
	}

	@Test
	public void testRepeatedParametersAndInListExpansion() {
		Database db = database();
		String table = "pyranid_parameter_items";
		recreateTable(db, table, "CREATE TABLE " + table + " (item_id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)");

		for (long i = 1L; i <= 4L; ++i)
			db.query("INSERT INTO " + table + " (item_id, name) VALUES (:itemId, :name)")
					.bind("itemId", i)
					.bind("name", "item-" + i)
					.execute();

		List<String> names = db.query("SELECT name FROM " + table
						+ " WHERE item_id = :selectedId OR item_id IN (:otherIds) OR item_id = :selectedId"
						+ " ORDER BY item_id")
				.bind("selectedId", 2L)
				.bind("otherIds", Parameters.inList(List.of(1L, 3L)))
				.fetchList(String.class);

		Assertions.assertEquals(List.of("item-1", "item-2", "item-3"), names);
	}

	@Test
	public void testNumericConversions() {
		Database db = database();
		String table = "pyranid_numeric_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "item_id BIGINT PRIMARY KEY, "
				+ "int_value INTEGER NOT NULL, "
				+ "long_value BIGINT NOT NULL, "
				+ "decimal_value DECIMAL(19, 4) NOT NULL, "
				+ "double_value DOUBLE PRECISION NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table
						+ " (item_id, int_value, long_value, decimal_value, double_value)"
						+ " VALUES (:itemId, :intValue, :longValue, :decimalValue, :doubleValue)")
				.bind("itemId", 1L)
				.bind("intValue", 42)
				.bind("longValue", 4_000_000_000L)
				.bind("decimalValue", new BigDecimal("1234.5000"))
				.bind("doubleValue", 9.25D)
				.execute();

		NumericRow row = db.query("SELECT int_value, long_value, decimal_value, double_value FROM " + table)
				.fetchObject(NumericRow.class)
				.orElseThrow();

		Assertions.assertEquals(Integer.valueOf(42), row.intValue());
		Assertions.assertEquals(Long.valueOf(4_000_000_000L), row.longValue());
		Assertions.assertEquals(0, row.decimalValue().compareTo(new BigDecimal("1234.5")));
		Assertions.assertEquals(9.25D, row.doubleValue(), 0.000001D);
	}

	@Test
	public void testTemporalRoundTrip() {
		Database db = database();
		String table = "pyranid_temporal_items";
		LocalDate eventDate = LocalDate.of(2020, 1, 2);
		LocalTime eventTime = LocalTime.of(3, 4, 5);
		LocalDateTime eventTimestamp = LocalDateTime.of(2020, 1, 2, 3, 4, 5);
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "item_id BIGINT PRIMARY KEY, "
				+ "event_date DATE NOT NULL, "
				+ "event_time TIME NOT NULL, "
				+ "event_timestamp TIMESTAMP NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table
						+ " (item_id, event_date, event_time, event_timestamp)"
						+ " VALUES (:itemId, :eventDate, :eventTime, :eventTimestamp)")
				.bind("itemId", 1L)
				.bind("eventDate", eventDate)
				.bind("eventTime", eventTime)
				.bind("eventTimestamp", eventTimestamp)
				.execute();

		TemporalRow row = db.query("SELECT event_date, event_time, event_timestamp FROM " + table)
				.fetchObject(TemporalRow.class)
				.orElseThrow();

		Assertions.assertEquals(eventDate, row.eventDate());
		Assertions.assertEquals(eventTime, row.eventTime());
		Assertions.assertEquals(eventTimestamp, row.eventTimestamp());
	}

	@Test
	public void testGeneratedKeyRoundTrip() {
		Database db = database();
		String table = "pyranid_generated_key_items";
		recreateTable(db, table, generatedKeyTableSql(table));

		Long id = db.query("INSERT INTO " + table + " (name) VALUES (:name)")
				.bind("name", "generated")
				.executeReturningGeneratedKey(Long.class, "id")
				.orElseThrow();
		String name = db.query("SELECT name FROM " + table + " WHERE id = :id")
				.bind("id", id)
				.fetchObject(String.class)
				.orElseThrow();

		Assertions.assertTrue(id > 0L, "Generated key should be positive");
		Assertions.assertEquals("generated", name);
	}

	@Test
	public void testTransactionCommitAndRollback() {
		Database db = database();
		String table = "pyranid_transaction_items";
		recreateTable(db, table, "CREATE TABLE " + table + " (item_id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)");

		db.transaction(() ->
				db.query("INSERT INTO " + table + " (item_id, name) VALUES (:itemId, :name)")
						.bind("itemId", 1L)
						.bind("name", "committed")
						.execute());

		Assertions.assertEquals(1L, countRows(db, table));

		Assertions.assertThrows(IllegalStateException.class, () ->
				db.transaction(() -> {
					db.query("INSERT INTO " + table + " (item_id, name) VALUES (:itemId, :name)")
							.bind("itemId", 2L)
							.bind("name", "rolled back")
							.execute();
					throw new IllegalStateException("rollback");
				}));

		Assertions.assertEquals(1L, countRows(db, table));
		Assertions.assertEquals(0L, db.query("SELECT COUNT(*) FROM " + table + " WHERE item_id = :itemId")
				.bind("itemId", 2L)
				.fetchObject(Long.class)
				.orElseThrow());
	}

	@Test
	public void testTransactionReadOnlyOption() {
		Assumptions.assumeTrue(supportsReadOnlyTransactions());

		Database db = database();

		db.transaction(TransactionOptions.withReadOnly(true).build(), () ->
				db.useRawConnection(connection -> {
					Assertions.assertTrue(connection.isReadOnly());
					return Optional.empty();
				}));
	}

	@Test
	public void testTransactionIsolationOption() {
		Assumptions.assumeTrue(supportsTransactionIsolationOptions());

		Database db = database();

		db.transaction(TransactionOptions.withIsolation(TransactionIsolation.READ_COMMITTED).build(), () ->
				db.useRawConnection(connection -> {
					Assertions.assertEquals(Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());
					return Optional.empty();
				}));
	}

	@Test
	public void testBatchChunkingExecutesAllParameterGroups() {
		Database db = database();
		String table = "pyranid_batch_items";
		recreateTable(db, table, "CREATE TABLE " + table + " (item_id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)");
		List<Map<@NonNull String, @Nullable Object>> rows = List.of(
				Map.of("itemId", 1L, "name", "one"),
				Map.of("itemId", 2L, "name", "two"),
				Map.of("itemId", 3L, "name", "three"),
				Map.of("itemId", 4L, "name", "four"),
				Map.of("itemId", 5L, "name", "five")
		);

		List<Long> updateCounts = db.query("INSERT INTO " + table + " (item_id, name) VALUES (:itemId, :name)")
				.batchChunkSize(2)
				.executeBatch(rows);
		List<String> names = db.query("SELECT name FROM " + table + " ORDER BY item_id")
				.fetchList(String.class);

		Assertions.assertEquals(5, updateCounts.size());
		Assertions.assertEquals(List.of("one", "two", "three", "four", "five"), names);
	}

	@Test
	public void testFetchStreamConsumesRowsWithinCallback() {
		Database db = database();
		String table = "pyranid_stream_items";
		recreateTable(db, table, "CREATE TABLE " + table + " (item_id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)");

		for (long i = 1L; i <= 4L; ++i)
			db.query("INSERT INTO " + table + " (item_id, name) VALUES (:itemId, :name)")
					.bind("itemId", i)
					.bind("name", "item-" + i)
					.execute();

		List<String> names = db.query("SELECT name FROM " + table + " ORDER BY item_id")
				.fetchStream(String.class, stream -> stream.toList());

		Assertions.assertEquals(List.of("item-1", "item-2", "item-3", "item-4"), names);
	}

	@Test
	public void testRawConnectionAccessUsesGuardedConnection() {
		Database db = database();

		Optional<String> productName = db.useRawConnection(connection -> {
			Assertions.assertThrows(IllegalStateException.class, connection::close);
			Assertions.assertThrows(IllegalStateException.class, connection::commit);
			Assertions.assertThrows(IllegalStateException.class, () -> connection.setAutoCommit(false));

			try (PreparedStatement statement = connection.prepareStatement("SELECT 1");
					 ResultSet resultSet = statement.executeQuery()) {
				Assertions.assertTrue(resultSet.next());
				Assertions.assertEquals(1, resultSet.getInt(1));
			}

			return Optional.of(connection.getMetaData().getDatabaseProductName());
		});

		Assertions.assertTrue(productName.isPresent());
	}

	@Test
	public void testMaxRowsSettingLimitsResultSet() {
		Database db = database();
		String table = "pyranid_max_rows_items";
		recreateTable(db, table, "CREATE TABLE " + table + " (item_id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)");

		for (long i = 1L; i <= 3L; ++i)
			db.query("INSERT INTO " + table + " (item_id, name) VALUES (:itemId, :name)")
					.bind("itemId", i)
					.bind("name", "item-" + i)
					.execute();

		List<String> names = db.query("SELECT name FROM " + table + " ORDER BY item_id")
				.maxRows(2)
				.fetchList(String.class);

		Assertions.assertEquals(List.of("item-1", "item-2"), names);
	}

	@Test
	public void testHealthCheckUsesStandardJdbcValidation() {
		Database db = database();

		db.performHealthCheck(Duration.ofSeconds(1));
	}

	@Test
	public void testDatabaseExceptionWrapsDuplicateKeyWithStatementContext() {
		Database db = database();
		String table = "pyranid_unique_items";
		recreateTable(db, table, "CREATE TABLE " + table + " (email VARCHAR(100) NOT NULL UNIQUE)");

		db.query("INSERT INTO " + table + " (email) VALUES (:email)")
				.bind("email", "secret@example.com")
				.execute();

		DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO " + table + " (email) VALUES (:email)")
						.bind("email", "secret@example.com")
						.execute());

		Assertions.assertInstanceOf(SQLException.class, exception.getCause());
		Assertions.assertTrue(exception.getMessage().contains("sql=INSERT INTO " + table + " (email) VALUES (?)"));
		Assertions.assertTrue(exception.getMessage().contains("parameterCount=1"));
	}

	protected void createPeopleTable(@NonNull Database db,
																	 @NonNull String tableName) {
		requireNonNull(db);
		requireNonNull(tableName);

		recreateTable(db, tableName, "CREATE TABLE " + tableName + " ("
				+ "person_id BIGINT PRIMARY KEY, "
				+ "name VARCHAR(100) NOT NULL, "
				+ "email_address VARCHAR(100) NOT NULL, "
				+ "locale VARCHAR(32) NOT NULL"
				+ ")");
	}

	protected void recreateTable(@NonNull Database db,
															 @NonNull String tableName,
															 @NonNull String createTableSql) {
		requireNonNull(db);
		requireNonNull(tableName);
		requireNonNull(createTableSql);

		db.query("DROP TABLE IF EXISTS " + tableName).execute();
		db.query(createTableSql).execute();
	}

	protected long countRows(@NonNull Database db,
													 @NonNull String tableName) {
		requireNonNull(db);
		requireNonNull(tableName);

		return db.query("SELECT COUNT(*) FROM " + tableName)
				.fetchObject(Long.class)
				.orElseThrow();
	}

	@NonNull
	protected Database database() {
		return Database.withDataSource(dataSource()).build();
	}

	@NonNull
	protected DatabaseType expectedDatabaseType() {
		return DatabaseType.GENERIC;
	}

	protected boolean supportsReadOnlyTransactions() {
		return true;
	}

	protected boolean supportsTransactionIsolationOptions() {
		return true;
	}

	protected static final class DriverManagerDataSource implements DataSource {
		@NonNull
		private final String url;
		@Nullable
		private final String username;
		@Nullable
		private final String password;

		public DriverManagerDataSource(@NonNull String url,
																	 @Nullable String username,
																	 @Nullable String password) {
			this.url = requireNonNull(url);
			this.username = username;
			this.password = password;
		}

		@Override
		public Connection getConnection() throws SQLException {
			if (this.username == null)
				return DriverManager.getConnection(this.url);

			return DriverManager.getConnection(this.url, this.username, this.password == null ? "" : this.password);
		}

		@Override
		public Connection getConnection(String username,
																		String password) throws SQLException {
			return DriverManager.getConnection(this.url, username, password);
		}

		@Override
		public PrintWriter getLogWriter() {
			return DriverManager.getLogWriter();
		}

		@Override
		public void setLogWriter(PrintWriter out) {
			DriverManager.setLogWriter(out);
		}

		@Override
		public void setLoginTimeout(int seconds) {
			DriverManager.setLoginTimeout(seconds);
		}

		@Override
		public int getLoginTimeout() {
			return DriverManager.getLoginTimeout();
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			throw new SQLFeatureNotSupportedException();
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			if (iface.isInstance(this))
				return iface.cast(this);

			throw new SQLException("Not a wrapper for " + iface.getName());
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) {
			return iface.isInstance(this);
		}
	}
}
