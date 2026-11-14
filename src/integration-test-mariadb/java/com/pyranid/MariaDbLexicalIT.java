/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;

@Testcontainers
class MariaDbLexicalIT {
	@Container private static final MariaDBContainer DATABASE = new MariaDBContainer(DockerImageName.parse(
			System.getProperty("mariadb.integration.image", "mariadb:11.4")).asCompatibleSubstituteFor("mariadb"))
			.withDatabaseName("pyranid_lexical").withUsername("pyranid").withPassword("pyranid");

	@Test
	void flatCommentsExposeFollowingNamedParameters() { SqlCompilerIntegrationSupport.verifyFlatComments(source(), DatabaseType.MARIA_DB); }

	@Test
	void defaultDoubleQuotedStringsPreserveEscapesAndLiteralParameterText() { SqlCompilerIntegrationSupport.verifyMySqlQuotes(source(), DatabaseType.MARIA_DB); }

	private static DataSource source() {
		return new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
	}
}
