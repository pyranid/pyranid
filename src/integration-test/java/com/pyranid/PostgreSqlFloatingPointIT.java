/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class PostgreSqlFloatingPointIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_floating").withUsername("pyranid").withPassword("pyranid");

	@Test
	void writtenSpecialValuesRoundTripThroughFloatingScalarAndRowTargets() {
		for (boolean cached : new boolean[]{false, true}) FloatingPointIntegrationSupport.verifyStoredScalars(database(cached));
	}

	@Test
	void typedArraysKeepSpecialValuesAndStillRejectFiniteOverflow() {
		for (boolean cached : new boolean[]{false, true}) FloatingPointIntegrationSupport.verifyTypedArraysAndOverflow(database(cached));
	}

	private static Database database(boolean cached) {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return Database.with(source, DatabaseType.POSTGRESQL).resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
