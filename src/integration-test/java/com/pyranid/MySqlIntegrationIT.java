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
import java.util.List;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
@Testcontainers
public class MySqlIntegrationIT extends AbstractPortableJdbcIntegrationTests {
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
}
