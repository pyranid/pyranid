/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;

@Testcontainers
class OracleLexicalIT {
	@Container private static final OracleContainer DATABASE = new OracleContainer(DockerImageName.parse(
			System.getProperty("oracle.integration.image", "gvenzl/oracle-free:23-slim-faststart")).asCompatibleSubstituteFor("gvenzl/oracle-free"))
			.withUsername("pyranid").withPassword("pyranid");

	@Test
	void flatCommentsExposeFollowingNamedParameters() { SqlCompilerIntegrationSupport.verifyFlatComments(source(), DatabaseType.ORACLE); }

	@Test
	void nationalAlternativeQuotesProtectApostrophesAndLiteralParameterText() { SqlCompilerIntegrationSupport.verifyOracleNationalQuotes(source()); }

	private static DataSource source() {
		return new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
	}
}
