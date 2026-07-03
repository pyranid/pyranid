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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
@Testcontainers
public class MySqlIntegrationIT extends AbstractPortableJdbcIntegrationTests {
	public record UnsignedBigIntRow(BigInteger unsignedValue) {}

	private static final String MYSQL_IMAGE_NAME =
			System.getProperty("mysql.integration.image", "mysql:8.4");
	private static final DockerImageName MYSQL_IMAGE = DockerImageName.parse(MYSQL_IMAGE_NAME)
			.asCompatibleSubstituteFor("mysql");

	@Container
	private static final MySQLContainer<?> MYSQL = new MySQLContainer<>(MYSQL_IMAGE)
			.withDatabaseName("pyranid")
			.withUsername("pyranid")
			.withPassword("pyranid");

	@Test
	public void testSecureParameterValueScrubbedFromDuplicateEntryDiagnostics() {
		// MySQL echoes the offending value in duplicate-key errors ("Duplicate entry 'x' for key ...").
		// A SecureParameter value must be scrubbed from Pyranid-rendered diagnostics while the raw driver
		// exception remains intact as the cause.
		String secret = "mysql-leak-secret@example.com";
		Database db = database();
		String table = "pyranid_mysql_secure_leak";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
				+ "email VARCHAR(100) NOT NULL UNIQUE"
				+ ")");

		db.query("INSERT INTO " + table + " (email) VALUES (:email)")
				.bind("email", Parameters.secure(secret))
				.execute();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO " + table + " (email) VALUES (:email)")
						.bind("email", Parameters.secure(secret))
						.execute());

		Assertions.assertTrue(ex.isUniqueConstraintViolation(),
				"Expected a recognized unique-constraint violation");
		Assertions.assertFalse(ex.getMessage().contains(secret),
				"Expected the secure value absent from DatabaseException.getMessage()");
		Assertions.assertFalse(ex.toString().contains(secret),
				"Expected the secure value absent from DatabaseException.toString()");
		Assertions.assertTrue(ex.getMessage().contains("<redacted>"),
				"Expected the mask present in the scrubbed message");

		Throwable cause = ex.getCause();
		Assertions.assertNotNull(cause);
		Assertions.assertTrue(String.valueOf(cause.getMessage()).contains(secret),
				"Expected the raw driver exception (the cause) to remain unsanitized");
	}

	@Test
	public void testOnDuplicateKeyUpdateGeneratedKeyBehavior() {
		Database db = database();
		String table = "pyranid_mysql_upsert_keys";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
				+ "email VARCHAR(100) NOT NULL UNIQUE, "
				+ "name VARCHAR(100) NOT NULL"
				+ ")");

		Long insertedId = db.query("INSERT INTO " + table + " (email, name) VALUES (:email, :name)")
				.bind("email", "ada@example.com")
				.bind("name", "Ada")
				.executeReturningGeneratedKey(Long.class)
				.orElseThrow();

		DatabaseException duplicateUpdateException = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO " + table + " (email, name) VALUES (:email, :name) "
								+ "ON DUPLICATE KEY UPDATE name = :updatedName")
						.bind("email", "ada@example.com")
						.bind("name", "Ignored")
						.bind("updatedName", "Ada Updated")
						.executeReturningGeneratedKey(Long.class));

		Assertions.assertTrue(duplicateUpdateException.getMessage().contains("Expected 1 generated-key row but got more than 1 instead"),
				"MySQL ON DUPLICATE KEY UPDATE does not behave like a normal single generated-key insert");
		Assertions.assertEquals("Ada Updated", db.query("SELECT name FROM " + table + " WHERE id = :id")
				.bind("id", insertedId)
				.fetchObject(String.class)
				.orElseThrow());

		List<Long> recoveredIds = db.query("INSERT INTO " + table + " (email, name) VALUES (:email, :name) "
						+ "ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id), name = :updatedName")
				.bind("email", "ada@example.com")
				.bind("name", "Ignored Again")
				.bind("updatedName", "Ada Recovered")
				.executeReturningGeneratedKeys(Long.class);

		Assertions.assertTrue(recoveredIds.contains(insertedId),
				"MySQL LAST_INSERT_ID(id) should include the updated row's existing id in generated keys");
		Assertions.assertEquals("Ada Recovered", db.query("SELECT name FROM " + table + " WHERE id = :id")
				.bind("id", insertedId)
				.fetchObject(String.class)
				.orElseThrow());
	}

	@Test
	public void testMySqlUuidStringRoundTrip() {
		Database db = database();
		String table = "pyranid_mysql_uuid_items";
		UUID id = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id CHAR(36) PRIMARY KEY, "
				+ "name VARCHAR(64) NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (id, name) VALUES (:id, :name)")
				.bind("id", id)
				.bind("name", "text uuid")
				.execute();

		Assertions.assertEquals(id, db.query("SELECT id FROM " + table)
				.fetchObject(UUID.class)
				.orElseThrow());
		Assertions.assertEquals(id.toString(), db.query("SELECT id FROM " + table)
				.fetchObject(String.class)
				.orElseThrow());
	}

	@Test
	public void testLockWaitTimeoutClassifiedAsTimeout() throws Exception {
		Database db = database();
		String table = "pyranid_mysql_lock_timeout";
		db.query("DROP TABLE IF EXISTS " + table).execute();
		db.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, val INT NOT NULL) ENGINE=InnoDB").execute();
		db.query("INSERT INTO " + table + " (id, val) VALUES (1, 0)").execute();

		CountDownLatch holderLocked = new CountDownLatch(1);
		CountDownLatch releaseHolder = new CountDownLatch(1);
		ExecutorService executor = Executors.newSingleThreadExecutor();

		try {
			// Lock holder: take a row lock and keep the transaction open until the contending update has timed out.
			Future<?> holder = executor.submit(() -> {
				db.transaction(() -> {
					db.query("SELECT val FROM " + table + " WHERE id = 1 FOR UPDATE")
							.fetchObject(Integer.class)
							.orElseThrow();
					holderLocked.countDown();
					awaitLatch(releaseHolder);
				});
				return null;
			});

			awaitLatch(holderLocked);

			DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
					db.transaction(() -> {
						db.query("SET innodb_lock_wait_timeout = 1").execute();
						db.query("UPDATE " + table + " SET val = 5 WHERE id = 1").execute();
					}));

			releaseHolder.countDown();
			holder.get(30, TimeUnit.SECONDS);

			Assertions.assertTrue(exception.isTimeout(),
					"InnoDB lock wait timeout (1205) should classify as a timeout");
			Assertions.assertEquals(Integer.valueOf(1205), exception.getErrorCode().orElse(null));
			// MySQL reports 1205 with SQLState 40001, but Pyranid excludes the timeout codes (1205/3024) from
			// serialization classification, so a lock-wait timeout is a timeout only and is not retried by the
			// default serializationFailureOrDeadlock() condition.
			Assertions.assertFalse(exception.isSerializationFailure(),
					"Lock-wait timeout should not be classified as a serialization failure");
		} finally {
			executor.shutdownNow();
		}
	}

	@Test
	public void testMySqlUnsignedBigIntMapsToBigInteger() {
		Database db = database();
		String table = "pyranid_mysql_unsigned_items";
		BigInteger unsignedValue = new BigInteger("9223372036854775808");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
				+ "unsigned_value BIGINT UNSIGNED NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (unsigned_value) VALUES (" + unsignedValue + ")")
				.execute();

		Assertions.assertEquals(unsignedValue, db.query("SELECT unsigned_value FROM " + table)
				.fetchObject(BigInteger.class)
				.orElseThrow());
		Assertions.assertEquals(unsignedValue, db.query("SELECT unsigned_value FROM " + table)
				.fetchObject(UnsignedBigIntRow.class)
				.orElseThrow()
				.unsignedValue());
	}

	@Test
	public void testMySqlJsonParameterBindsAsTextForNativeJsonColumn() {
		Database db = database();
		String table = "pyranid_mysql_json_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
				+ "payload JSON NULL"
				+ ")");

		Long id = db.query("INSERT INTO " + table + " (payload) VALUES (:payload)")
				.bind("payload", Parameters.json("{\"kind\":\"integration\",\"count\":3,\"tags\":[\"alpha\",\"beta\"]}"))
				.executeReturningGeneratedKey(Long.class)
				.orElseThrow();
		db.query("INSERT INTO " + table + " (payload) VALUES (:payload)")
				.bind("payload", Parameters.json(null))
				.execute();

		Assertions.assertEquals("integration", db.query("SELECT JSON_UNQUOTE(JSON_EXTRACT(payload, '$.kind')) FROM " + table + " WHERE id = :id")
				.bind("id", id)
				.fetchObject(String.class)
				.orElseThrow());
		Assertions.assertEquals(Integer.valueOf(3), db.query("SELECT CAST(JSON_EXTRACT(payload, '$.count') AS UNSIGNED) FROM " + table + " WHERE id = :id")
				.bind("id", id)
				.fetchObject(Integer.class)
				.orElseThrow());
		Assertions.assertEquals("beta", db.query("SELECT JSON_UNQUOTE(JSON_EXTRACT(payload, '$.tags[1]')) FROM " + table + " WHERE id = :id")
				.bind("id", id)
				.fetchObject(String.class)
				.orElseThrow());
		Assertions.assertEquals(Long.valueOf(1L), db.query("SELECT COUNT(*) FROM " + table + " WHERE payload IS NULL")
				.fetchObject(Long.class)
				.orElseThrow());
	}

	@Test
	public void testMySqlStreamingResultSetBlocksAdditionalStatementsUntilClosed() {
		Database db = database();
		String table = "pyranid_mysql_streaming_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "item_id BIGINT PRIMARY KEY, "
				+ "name VARCHAR(64) NOT NULL"
				+ ")");

		for (long i = 1L; i <= 5L; ++i)
			db.query("INSERT INTO " + table + " (item_id, name) VALUES (:itemId, :name)")
					.bind("itemId", i)
					.bind("name", "item-" + i)
					.execute();

		Optional<String> firstName = db.transaction(() -> {
			String first = db.query("SELECT name FROM " + table + " ORDER BY item_id")
					.fetchStream(String.class, stream -> {
						String firstStreamedName = stream.findFirst().orElseThrow();

						DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
								db.query("SELECT COUNT(*) FROM " + table)
										.fetchObject(Long.class)
										.orElseThrow());
						Assertions.assertTrue(hasStreamingResultSetStillActiveMessage(exception),
								() -> "Expected Connector/J to reject a second statement while the streaming result set is open; got "
										+ exception);

						return firstStreamedName;
					});

			Assertions.assertEquals(Long.valueOf(5L), db.query("SELECT COUNT(*) FROM " + table)
					.fetchObject(Long.class)
					.orElseThrow());

			return Optional.of(first);
		});

		Assertions.assertEquals("item-1", firstName.orElseThrow());
	}

	@NonNull
	@Override
	protected DataSource dataSource() {
		return new DriverManagerDataSource(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
	}

	@NonNull
	@Override
	protected DatabaseType expectedDatabaseType() {
		return DatabaseType.MYSQL;
	}

	@NonNull
	@Override
	protected DialectProfile dialectProfile() {
		return new DialectProfile() {
			@NonNull
			@Override
			String autoIncrementPrimaryKey(@NonNull String columnName) {
				return columnName + " BIGINT AUTO_INCREMENT PRIMARY KEY";
			}

			@NonNull
			@Override
			String timestampWithFractionalSeconds() {
				return "DATETIME(6)";
			}
		};
	}

	@NonNull
	@Override
	protected CapabilityFlags capabilityFlags() {
		return CapabilityFlags.builder()
				.supportsServerSideStreaming(true)
				.supportsNativeJson(true)
				.build();
	}

	private static boolean hasStreamingResultSetStillActiveMessage(@NonNull Throwable throwable) {
		requireNonNull(throwable);

		for (Throwable current = throwable; current != null; current = current.getCause()) {
			String message = current.getMessage();
			if (message == null)
				continue;

			String messageLowercase = message.toLowerCase(Locale.ROOT);
			if (messageLowercase.contains("streaming result set") && messageLowercase.contains("still active"))
				return true;
		}

		return false;
	}
}
