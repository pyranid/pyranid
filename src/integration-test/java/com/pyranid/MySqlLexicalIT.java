/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;

@Testcontainers
class MySqlLexicalIT {
	@Container private static final MySQLContainer DATABASE = new MySQLContainer(DockerImageName.parse(
			System.getProperty("mysql.integration.image", "mysql:8.4")).asCompatibleSubstituteFor("mysql"))
			.withDatabaseName("pyranid_lexical").withUsername("pyranid").withPassword("pyranid");

	@Test
	void flatCommentsExposeFollowingNamedParameters() { SqlCompilerIntegrationSupport.verifyFlatComments(source(), DatabaseType.MYSQL); }

	@Test
	void defaultDoubleQuotedStringsPreserveEscapesAndLiteralParameterText() { SqlCompilerIntegrationSupport.verifyMySqlQuotes(source(), DatabaseType.MYSQL); }

	private static DataSource source() {
		return new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
	}
}
