/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

@Testcontainers
class MariaDbTemporalBindingIT extends MySqlFamilyTemporalIntegrationSupport {
	@Container private static final MariaDBContainer MARIA_DB = new MariaDBContainer(DockerImageName.parse(
			System.getProperty("mariadb.integration.image", "mariadb:11.4")).asCompatibleSubstituteFor("mariadb"))
			.withDatabaseName("pyranid_temporal").withUsername("pyranid").withPassword("pyranid");
	@Override protected String jdbcUrl() { return MARIA_DB.getJdbcUrl(); }
	@Override protected String username() { return MARIA_DB.getUsername(); }
	@Override protected String password() { return MARIA_DB.getPassword(); }
	@Override protected DatabaseType databaseType() { return DatabaseType.MARIA_DB; }
	@Override protected List<String> prepareModes() { return List.of("useServerPrepStmts=false", "useServerPrepStmts=true"); }
}
