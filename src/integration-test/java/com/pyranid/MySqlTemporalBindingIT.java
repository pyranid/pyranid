/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

@Testcontainers
class MySqlTemporalBindingIT extends MySqlFamilyTemporalIntegrationSupport {
	@Container private static final MySQLContainer MYSQL = new MySQLContainer(DockerImageName.parse(
			System.getProperty("mysql.integration.image", "mysql:8.4")).asCompatibleSubstituteFor("mysql"))
			.withDatabaseName("pyranid_temporal").withUsername("pyranid").withPassword("pyranid");
	@Override protected String jdbcUrl() { return MYSQL.getJdbcUrl(); }
	@Override protected String username() { return MYSQL.getUsername(); }
	@Override protected String password() { return MYSQL.getPassword(); }
	@Override protected DatabaseType databaseType() { return DatabaseType.MYSQL; }
	@Override protected List<String> prepareModes() {
		return List.of("useServerPrepStmts=false&generateSimpleParameterMetadata=false", "useServerPrepStmts=false&generateSimpleParameterMetadata=true",
				"useServerPrepStmts=true&generateSimpleParameterMetadata=false", "useServerPrepStmts=true&generateSimpleParameterMetadata=true");
	}
}
