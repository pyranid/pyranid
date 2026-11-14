/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;

@Testcontainers
class SqlServerLexicalIT {
	@Container private static final MSSQLServerContainer DATABASE = new MSSQLServerContainer(DockerImageName.parse(
			System.getProperty("sqlserver.integration.image", "mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04")).asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server"))
			.acceptLicense().withPassword("Pyranid_Test_123!");

	@Test
	void nestedCommentsProtectInnerParameters() { SqlCompilerIntegrationSupport.verifyNestedComments(source(), DatabaseType.SQL_SERVER); }

	private static DataSource source() {
		return new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
	}
}
