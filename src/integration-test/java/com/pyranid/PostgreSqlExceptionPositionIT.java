/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PSQLException;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.BatchUpdateException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlExceptionPositionIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_error_positions").withUsername("pyranid").withPassword("pyranid");

	@Test
	void absentPositionsInConstraintErrorsStayAbsentThroughDirectAndBatchDiagnostics() {
		Database database = database();
		String table = "position_unique_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY)").execute();
		try {
			database.query("INSERT INTO " + table + " VALUES (1)").execute();
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query("INSERT INTO " + table + " VALUES (1)").execute());
			assertEquals("23505", failure.getSqlState().orElseThrow());
			assertTrue(failure.isUniqueConstraintViolation());
			assertTrue(failure.getPosition().isEmpty());
			assertTrue(failure.getInternalPosition().isEmpty());
			PSQLException driver = assertInstanceOf(PSQLException.class, failure.getCause());
			assertEquals(0, driver.getServerErrorMessage().getPosition());
			assertEquals(0, driver.getServerErrorMessage().getInternalPosition());
			assertWrapperPositionsMatch(failure);
			assertEquals(List.of(1), database.query("SELECT id FROM " + table).fetchList(Integer.class));
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	@Test
	void outerQueryPositionKeepsItsExactOneBasedCharacterOffset() {
		String sql = "SELECT 'é😀', missing_outer_column";
		DatabaseException failure = assertThrows(DatabaseException.class, () -> database().query(sql).fetchObject(Object.class));
		assertEquals("42703", failure.getSqlState().orElseThrow());
		int expected = sql.codePointCount(0, sql.indexOf("missing_outer_column")) + 1;
		assertEquals(expected, failure.getPosition().orElseThrow());
		assertTrue(failure.getInternalPosition().isEmpty());
		assertWrapperPositionsMatch(failure);
	}

	@Test
	void dynamicFunctionErrorKeepsInternalPositionWithoutInventingOuterPosition() {
		String internalQuery = "SELECT missing_inner_column";
		DatabaseException failure = assertThrows(DatabaseException.class, () -> database().query(
				"DO $$ BEGIN EXECUTE '" + internalQuery + "'; END; $$").execute());
		assertEquals("42703", failure.getSqlState().orElseThrow());
		assertTrue(failure.getPosition().isEmpty());
		assertEquals(internalQuery, failure.getInternalQuery().orElseThrow());
		assertEquals(internalQuery.indexOf("missing_inner_column") + 1, failure.getInternalPosition().orElseThrow());
		assertWrapperPositionsMatch(failure);
	}

	private static void assertWrapperPositionsMatch(DatabaseException original) {
		for (boolean throughCause : new boolean[]{false, true}) {
			BatchUpdateException wrapper = new BatchUpdateException("batch wrapper", "HY000", 71, new int[]{1});
			if (throughCause) wrapper.initCause(original.getCause());
			else wrapper.setNextException(assertInstanceOf(PSQLException.class, original.getCause()));
			DatabaseException recovered = new DatabaseException("failed", wrapper, PostgresDialect.INSTANCE);
			assertEquals(original.getPosition(), recovered.getPosition());
			assertEquals(original.getInternalPosition(), recovered.getInternalPosition());
			assertEquals("HY000", recovered.getSqlState().orElseThrow());
			assertEquals(71, recovered.getErrorCode().orElseThrow());
		}
	}

	private static Database database() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return Database.with(source, DatabaseType.POSTGRESQL).build();
	}
}
