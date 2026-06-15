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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
@Testcontainers
public class MySQLIntegrationIT extends AbstractPortableJdbcIntegrationTests {
	private static final String MYSQL_IMAGE_NAME =
			System.getProperty("mysql.integration.image", "mysql:8.4");
	private static final DockerImageName MYSQL_IMAGE = DockerImageName.parse(MYSQL_IMAGE_NAME)
			.asCompatibleSubstituteFor("mysql");

	@Container
	private static final MySQLContainer<?> MYSQL = new MySQLContainer<>(MYSQL_IMAGE)
			.withDatabaseName("pyranid")
			.withUsername("pyranid")
			.withPassword("pyranid");

	@NonNull
	@Override
	protected DataSource dataSource() {
		return new DriverManagerDataSource(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
	}

	@NonNull
	@Override
	protected String generatedKeyTableSql(@NonNull String tableName) {
		return "CREATE TABLE " + tableName + " (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64) NOT NULL)";
	}
}
