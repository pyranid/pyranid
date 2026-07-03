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
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.3.0
 */
@Testcontainers
public class MariaDbIntegrationIT extends AbstractPortableJdbcIntegrationTests {
	public record UnsignedBigIntRow(BigInteger unsignedValue) {}

	private static final String MARIA_DB_IMAGE_NAME =
			System.getProperty("mariadb.integration.image", "mariadb:11.4");
	private static final DockerImageName MARIA_DB_IMAGE = DockerImageName.parse(MARIA_DB_IMAGE_NAME)
			.asCompatibleSubstituteFor("mariadb");

	@Container
	private static final MariaDBContainer<?> MARIA_DB = new MariaDBContainer<>(MARIA_DB_IMAGE)
			.withDatabaseName("pyranid")
			.withUsername("pyranid")
			.withPassword("pyranid");

	@Test
	public void testMariaDbUuidStringRoundTrip() {
		Database db = database();
		String table = "pyranid_mariadb_uuid_items";
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
	public void testMariaDbUnsignedBigIntMapsToBigInteger() {
		Database db = database();
		String table = "pyranid_mariadb_unsigned_items";
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
	public void testMariaDbJsonParameterBindsAsTextForJsonAliasColumn() {
		Database db = database();
		String table = "pyranid_mariadb_json_items";
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
		Assertions.assertEquals(Integer.valueOf(3), db.query("SELECT CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.count')) AS UNSIGNED) FROM " + table + " WHERE id = :id")
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
	public void testMariaDbInsertReturningMapsGeneratedRows() {
		Database db = database();
		String table = "pyranid_mariadb_returning_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
				+ "name VARCHAR(64) NOT NULL"
				+ ")");

		Long firstId = db.query("INSERT INTO " + table + " (name) VALUES (:name) RETURNING id")
				.bind("name", "one")
				.executeForObject(Long.class)
				.orElseThrow();
		List<Long> ids = db.query("INSERT INTO " + table + " (name) VALUES (:secondName), (:thirdName) RETURNING id")
				.bind("secondName", "two")
				.bind("thirdName", "three")
				.executeForList(Long.class);

		Assertions.assertEquals(Long.valueOf(1L), firstId);
		Assertions.assertEquals(List.of(2L, 3L), ids);
		Assertions.assertEquals(List.of("one", "two", "three"), db.query("SELECT name FROM " + table + " ORDER BY id")
				.fetchList(String.class));
	}

	@NonNull
	@Override
	protected DataSource dataSource() {
		return new DriverManagerDataSource(MARIA_DB.getJdbcUrl(), MARIA_DB.getUsername(), MARIA_DB.getPassword());
	}

	@NonNull
	@Override
	protected DatabaseType expectedDatabaseType() {
		return DatabaseType.MARIA_DB;
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
				.supportsNativeJson(true)
				.supportsReturningClause(true)
				.build();
	}
}
