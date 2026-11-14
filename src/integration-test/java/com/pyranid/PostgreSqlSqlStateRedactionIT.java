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

import java.sql.BatchUpdateException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlSqlStateRedactionIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_sqlstate_redaction").withUsername("pyranid").withPassword("pyranid");

	@Test
	void directUniqueViolationsKeepTheirOperationalCodeWhenTheSecureValueOverlapsIt() {
		AtomicReference<StatementLog<?>> logged = new AtomicReference<>();
		Database database = database(logged);
		String table = "direct_state_overlap";
		database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, secret TEXT UNIQUE)").execute();
		try {
			int id = 0;
			for (String secret : List.of("2350", "23505")) {
				int original = ++id;
				database.query("INSERT INTO " + table + " VALUES (:id, :secret)").bind("id", original)
						.bind("secret", Parameters.secure(secret)).execute();
				DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query("INSERT INTO " + table + " VALUES (:id, :secret)")
						.bind("id", original + 10).bind("secret", Optional.of(Parameters.secure(Optional.of(secret), "<hidden>"))).execute());
				verify(failure, secret);
				assertEquals("23505", assertInstanceOf(DatabaseException.class, logged.get().getException().orElseThrow()).getSqlState().orElseThrow());
			}
			assertEquals(List.of("2350", "23505"), database.query("SELECT secret FROM " + table + " ORDER BY id").fetchList(String.class));
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	@Test
	void batchFailurePreservesSqlStateWhileScrubbingItsRecoveredDetailsAndRollingBackEarlierRows() {
		Database database = database(new AtomicReference<>());
		String table = "batch_state_overlap";
		database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, secret TEXT UNIQUE)").execute();
		try {
			for (String secret : List.of("2350", "23505")) {
				database.query("DELETE FROM " + table).execute();
				database.query("INSERT INTO " + table + " VALUES (1, :secret)").bind("secret", Parameters.secure(secret)).execute();
				DatabaseException failure = assertThrows(DatabaseException.class, () -> database.transaction(() -> database.query(
						"INSERT INTO " + table + " VALUES (:id, :secret)").executeBatch(List.of(
						Map.of("id", 2, "secret", Parameters.secure("safe-new-row")),
						Map.of("id", 3, "secret", Parameters.secure(secret, "<hidden>"))))));
				assertInstanceOf(BatchUpdateException.class, failure.getCause());
				verify(failure, secret);
				assertEquals(List.of(1), database.query("SELECT id FROM " + table + " ORDER BY id").fetchList(Integer.class));
				assertEquals(secret, database.query("SELECT secret FROM " + table).fetchObject(String.class).orElseThrow());
			}
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	private static void verify(DatabaseException failure, String secret) {
		assertEquals("23505", failure.getSqlState().orElseThrow());
		assertTrue(failure.isUniqueConstraintViolation());
		assertTrue(failure.getSqlState().map("23505"::equals).orElse(false), "Application SQLState policy must remain effective");
		assertTrue(failure.getDetail().orElseThrow().contains("<hidden>"));
		assertFalse(failure.getDetail().orElseThrow().contains(secret));
		assertFalse(failure.getMessage().contains("(" + secret + ")"));
		assertTrue(failure.getCause().getMessage().contains(secret), "Trusted raw causes retain original driver text");
	}

	private static Database database(AtomicReference<StatementLog<?>> logged) {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return Database.with(source, DatabaseType.POSTGRESQL).parameterRedactor(ParameterRedactor.redactAll()).statementLogger(logged::set).build();
	}
}
