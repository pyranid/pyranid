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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.3.0
 */
@Testcontainers
public class OracleIntegrationIT extends AbstractPortableJdbcIntegrationTests {
	public record OracleNumberRow(BigInteger bigValue, Integer intValue, Long longValue) {}

	private static final String ORACLE_IMAGE_NAME =
			System.getProperty("oracle.integration.image", "gvenzl/oracle-free:23-slim-faststart");
	private static final DockerImageName ORACLE_IMAGE = DockerImageName.parse(ORACLE_IMAGE_NAME)
			.asCompatibleSubstituteFor("gvenzl/oracle-free");

	@Container
	private static final OracleContainer ORACLE = new OracleContainer(ORACLE_IMAGE)
			.withUsername("pyranid")
			.withPassword("pyranid");

	@Test
	public void testOracleGeneratedKeysRejectNoColumnNameBeforeRowIdTrap() {
		Database db = database();
		String table = "pyr_oracle_key_guard";
		recreateTable(db, table, dialectProfile().generatedKeyTableSql(table));

		DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO " + table + " (name) VALUES (:name)")
						.bind("name", "Ada")
						.executeReturningGeneratedKey(Long.class));

		Assertions.assertInstanceOf(IllegalArgumentException.class, exception.getCause());
		Assertions.assertTrue(exception.getMessage().contains("Oracle generated-key retrieval requires explicit key column names"));
		Assertions.assertEquals(0L, countRows(db, table),
				"Oracle no-column generated-key guard should reject before executing the insert");
	}

	@Test
	public void testOracleRawUuidRoundTrip() {
		Database db = database();
		String table = "pyr_oracle_uuid_items";
		UUID id = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id RAW(16) PRIMARY KEY, "
				+ "name VARCHAR2(64) NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (id, name) VALUES (:id, :name)")
				.bind("id", id)
				.bind("name", "raw uuid")
				.execute();

		Assertions.assertEquals(id, db.query("SELECT id FROM " + table)
				.fetchObject(UUID.class)
				.orElseThrow());
		Assertions.assertArrayEquals(uuidBytes(id), db.query("SELECT id FROM " + table)
				.fetchObject(byte[].class)
				.orElseThrow());
	}

	@Test
	public void testOracleTimestampWithTimeZoneRoundTrip() {
		Database db = Database.withDataSource(dataSource())
				.timeZone(ZoneId.of("UTC"))
				.build();
		String table = "pyr_oracle_tstz_items";
		OffsetDateTime eventAt = OffsetDateTime.parse("2020-11-01T01:30:15.123456-04:00");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id NUMBER(10,0) PRIMARY KEY, "
				+ "event_at TIMESTAMP(6) WITH TIME ZONE NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (id, event_at) VALUES (:id, :eventAt)")
				.bind("id", 1)
				.bind("eventAt", eventAt)
				.execute();

		Assertions.assertEquals(eventAt.toInstant(), db.query("SELECT event_at FROM " + table)
				.fetchObject(OffsetDateTime.class)
				.orElseThrow()
				.toInstant());
		Assertions.assertEquals(eventAt.toInstant(), db.query("SELECT event_at FROM " + table)
				.fetchObject(Instant.class)
				.orElseThrow());
	}

	@Test
	public void testOracleTimestampWithLocalTimeZoneMapsStoredInstant() {
		Database db = Database.withDataSource(dataSource())
				.timeZone(ZoneId.of("UTC"))
				.build();
		String table = "pyr_oracle_ltz_items";
		OffsetDateTime eventAt = OffsetDateTime.parse("2020-11-01T01:30:15.123456-04:00");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id NUMBER(10,0) PRIMARY KEY, "
				+ "event_at TIMESTAMP(6) WITH LOCAL TIME ZONE NOT NULL"
				+ ")");

		db.transaction(() -> {
			db.query("ALTER SESSION SET TIME_ZONE = '+00:00'").execute();
			db.query("INSERT INTO " + table + " (id, event_at) VALUES (1, "
							+ "TO_TIMESTAMP_TZ('2020-11-01 01:30:15.123456 -04:00', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'))")
					.execute();

			Assertions.assertEquals(eventAt.toInstant(), db.query("SELECT event_at FROM " + table)
					.fetchObject(OffsetDateTime.class)
					.orElseThrow()
					.toInstant());
			Assertions.assertEquals(eventAt.toInstant(), db.query("SELECT event_at FROM " + table)
					.fetchObject(Instant.class)
					.orElseThrow());
		});
	}

	@Test
	public void testOracleEmptyStringIsStoredAsNull() {
		Database db = database();
		String table = "pyr_oracle_empty_text";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id NUMBER(10,0) PRIMARY KEY, "
				+ "value VARCHAR2(64) NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (id, value) VALUES (:id, :value)")
				.bind("id", 1)
				.bind("value", "")
				.execute();

		Assertions.assertEquals(1L, db.query("SELECT COUNT(*) FROM " + table + " WHERE value IS NULL")
				.fetchObject(Long.class)
				.orElseThrow());
		Assertions.assertTrue(db.query("SELECT value FROM " + table + " WHERE id = 1")
				.fetchObject(String.class)
				.isEmpty());
	}

	@Test
	public void testOracleNumberMapsToBigIntegerAndNarrowsExactly() {
		Database db = database();
		String table = "pyr_oracle_number_items";
		BigInteger bigValue = new BigInteger("12345678901234567890123456789012345678");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id NUMBER(10,0) PRIMARY KEY, "
				+ "big_value NUMBER(38,0) NOT NULL, "
				+ "int_value NUMBER(10,0) NOT NULL, "
				+ "long_value NUMBER(19,0) NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (id, big_value, int_value, long_value) VALUES (1, "
						+ bigValue + ", 42, 4000000000)")
				.execute();

		Assertions.assertEquals(bigValue, db.query("SELECT big_value FROM " + table)
				.fetchObject(BigInteger.class)
				.orElseThrow());
		OracleNumberRow row = db.query("SELECT big_value, int_value, long_value FROM " + table)
				.fetchObject(OracleNumberRow.class)
				.orElseThrow();
		Assertions.assertEquals(bigValue, row.bigValue());
		Assertions.assertEquals(Integer.valueOf(42), row.intValue());
		Assertions.assertEquals(Long.valueOf(4_000_000_000L), row.longValue());
	}

	@NonNull
	@Override
	protected DataSource dataSource() {
		return new DriverManagerDataSource(ORACLE.getJdbcUrl(), ORACLE.getUsername(), ORACLE.getPassword());
	}

	@NonNull
	@Override
	protected DatabaseType expectedDatabaseType() {
		return DatabaseType.ORACLE;
	}

	@NonNull
	@Override
	protected DialectProfile dialectProfile() {
		return new DialectProfile() {
			@NonNull
			@Override
			String bigIntPrimaryKey() {
				return "NUMBER(19,0) PRIMARY KEY";
			}

			@NonNull
			@Override
			String bigInt() {
				return "NUMBER(19,0)";
			}

			@NonNull
			@Override
			String integer() {
				return "NUMBER(10,0)";
			}

			@NonNull
			@Override
			String varchar(int length) {
				return "VARCHAR2(" + length + ")";
			}

			@NonNull
			@Override
			String doublePrecision() {
				return "BINARY_DOUBLE";
			}

			@NonNull
			@Override
			String autoIncrementPrimaryKey(@NonNull String columnName) {
				return columnName + " NUMBER(19,0) GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY";
			}

			@NonNull
			@Override
			String generatedKeyColumnName() {
				return "ID";
			}

			@NonNull
			@Override
			String dropTableSql(@NonNull String tableName) {
				return "BEGIN EXECUTE IMMEDIATE 'DROP TABLE " + tableName
						+ "'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
			}

			@NonNull
			@Override
			String validationQuery() {
				return "SELECT 1 FROM DUAL";
			}
		};
	}

	@NonNull
	@Override
	protected CapabilityFlags capabilityFlags() {
		return CapabilityFlags.builder()
				.supportsReadOnlyTransactions(false)
				.supportsTemporalRoundTrip(false)
				.hasNativeBoolean(false)
				.build();
	}

	private static byte[] uuidBytes(UUID uuid) {
		ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
		byteBuffer.putLong(uuid.getMostSignificantBits());
		byteBuffer.putLong(uuid.getLeastSignificantBits());
		return byteBuffer.array();
	}
}
