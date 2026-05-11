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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.util.List;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.1.0
 */
@Testcontainers
public class PostgreSQLIntegrationIT {
	private static final String POSTGRES_IMAGE_NAME =
			System.getProperty("postgres.integration.image", "postgres:17-alpine");
	private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse(POSTGRES_IMAGE_NAME)
			.asCompatibleSubstituteFor("postgres");

	@Container
	private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(POSTGRES_IMAGE)
			.withDatabaseName("pyranid")
			.withUsername("pyranid")
			.withPassword("pyranid");

	@Test
	public void testPostgreSqlDatabaseTypeDetection() {
		Database db = Database.withDataSource(dataSource()).build();

		Assertions.assertEquals(DatabaseType.POSTGRESQL, db.getDatabaseType());
	}

	@Test
	public void testJsonbParameterAndReturningRoundTrip() {
		Database db = Database.withDataSource(dataSource()).build();

		db.query("CREATE TABLE IF NOT EXISTS pyranid_jsonb_test (id BIGSERIAL PRIMARY KEY, payload JSONB NOT NULL)")
				.execute();

		String kind = db.query("INSERT INTO pyranid_jsonb_test(payload) VALUES (:payload) RETURNING payload->>'kind'")
				.bind("payload", Parameters.json("{\"kind\":\"integration\",\"count\":3}"))
				.fetchObject(String.class)
				.orElseThrow();

		Assertions.assertEquals("integration", kind);
	}

	@Test
	public void testTextArrayParameterAndReturningRoundTrip() {
		Database db = Database.withDataSource(dataSource()).build();

		db.query("CREATE TABLE IF NOT EXISTS pyranid_array_test (id BIGSERIAL PRIMARY KEY, tags TEXT[] NOT NULL)")
				.execute();

		String secondTag = db.query("INSERT INTO pyranid_array_test(tags) VALUES (:tags) RETURNING tags[2]")
				.bind("tags", Parameters.sqlArrayOf("text", List.of("alpha", "beta", "gamma")))
				.fetchObject(String.class)
				.orElseThrow();

		Assertions.assertEquals("beta", secondTag);
	}

	@Test
	public void testPostgreSqlExceptionMetadataExtraction() {
		Database db = Database.withDataSource(dataSource()).build();

		db.query("CREATE TABLE IF NOT EXISTS pyranid_unique_test (id BIGSERIAL PRIMARY KEY, email TEXT UNIQUE)")
				.execute();
		db.query("TRUNCATE TABLE pyranid_unique_test").execute();
		db.query("INSERT INTO pyranid_unique_test(email) VALUES (:email)")
				.bind("email", "ada@example.com")
				.execute();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO pyranid_unique_test(email) VALUES (:email)")
						.bind("email", "ada@example.com")
						.execute());

		Assertions.assertEquals("23505", ex.getSqlState().orElse(null));
		Assertions.assertTrue(ex.getConstraint().orElse("").contains("email"),
				"Expected PostgreSQL constraint metadata to be extracted");
	}

	private DataSource dataSource() {
		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setUrl(POSTGRES.getJdbcUrl());
		dataSource.setUser(POSTGRES.getUsername());
		dataSource.setPassword(POSTGRES.getPassword());
		return dataSource;
	}
}
