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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlBatchMetadataIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_batch_metadata").withUsername("pyranid").withPassword("pyranid");

	@Test
	void batchUniqueViolationsExposeTheNamedConstraintAndTableAndRollBackEarlierRows() {
		Database database = database();
		String table = identifier("batch_unique"), constraint = identifier("unique_email");
		database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, email TEXT, CONSTRAINT " + constraint + " UNIQUE(email))").execute();
		try {
			database.query("INSERT INTO " + table + " VALUES (1, 'present@example.test')").execute();
			AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.transaction(
					TransactionOptions.withReadOnly(null).completionObserver(completion::set).build(), () -> database.query(
							"INSERT INTO " + table + " VALUES (:id, :email)").executeBatch(List.of(
							Map.of("id", 2, "email", "new@example.test"), Map.of("id", 3, "email", "present@example.test")))));
			assertInstanceOf(BatchUpdateException.class, failure.getCause());
			assertEquals("23505", failure.getSqlState().orElseThrow());
			assertEquals(constraint, failure.getConstraint().orElseThrow());
			assertEquals(table, failure.getTable().orElseThrow());
			assertEquals("public", failure.getSchema().orElseThrow());
			assertTrue(failure.getDetail().orElseThrow().contains("present@example.test"));
			assertTrue(failure.isUniqueConstraintViolation());
			assertEquals(TransactionResult.ROLLED_BACK, completion.get().getResult());
			assertEquals(List.of(1), database.query("SELECT id FROM " + table + " ORDER BY id").fetchList(Integer.class));
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	@Test
	void batchForeignKeyViolationsExposeTheForeignKeyMetadataAndKeepTheirClassifier() {
		Database database = database();
		String parent = identifier("batch_parent"), child = identifier("batch_child"), constraint = identifier("fk_parent");
		database.query("CREATE TABLE " + parent + " (id INT PRIMARY KEY)").execute();
		database.query("CREATE TABLE " + child + " (id INT PRIMARY KEY, parent_id INT, CONSTRAINT " + constraint
				+ " FOREIGN KEY(parent_id) REFERENCES " + parent + "(id))").execute();
		try {
			database.query("INSERT INTO " + parent + " VALUES (1)").execute();
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.transaction(() -> database.query(
					"INSERT INTO " + child + " VALUES (:id, :parent)").executeBatch(List.of(
					Map.of("id", 1, "parent", 1), Map.of("id", 2, "parent", 999)))));
			assertInstanceOf(BatchUpdateException.class, failure.getCause());
			assertEquals("23503", failure.getSqlState().orElseThrow());
			assertEquals(constraint, failure.getConstraint().orElseThrow());
			assertEquals(child, failure.getTable().orElseThrow());
			assertEquals("public", failure.getSchema().orElseThrow());
			assertTrue(failure.getDetail().orElseThrow().contains("999"));
			assertTrue(failure.isForeignKeyViolation());
			assertFalse(failure.isUniqueConstraintViolation());
			assertEquals(0L, database.query("SELECT COUNT(*) FROM " + child).fetchObject(Long.class).orElseThrow());
		} finally {
			database.query("DROP TABLE " + child).execute();
			database.query("DROP TABLE " + parent).execute();
		}
	}

	@Test
	void newlyRecoveredBatchServerFieldsRetainSecureParameterScrubbing() {
		Database database = database();
		String table = identifier("batch_secure"), constraint = identifier("unique_secret");
		String secret = "secure-bulk-value-" + UUID.randomUUID();
		database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, secret TEXT, CONSTRAINT " + constraint + " UNIQUE(secret))").execute();
		try {
			database.query("INSERT INTO " + table + " VALUES (1, :secret)").bind("secret", Parameters.secure(secret)).execute();
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.transaction(() -> database.query(
					"INSERT INTO " + table + " VALUES (:id, :secret)").executeBatch(List.of(
					Map.of("id", 2, "secret", Parameters.secure(secret))))));
			assertEquals(constraint, failure.getConstraint().orElseThrow());
			assertEquals(table, failure.getTable().orElseThrow());
			assertTrue(failure.getDetail().orElseThrow().contains("<redacted>"));
			assertFalse(failure.getDetail().orElseThrow().contains(secret));
			assertFalse(failure.getMessage().contains(secret));
			assertFalse(failure.toString().contains(secret));
			assertTrue(failure.isUniqueConstraintViolation());
			// Driver causes intentionally retain their original diagnostics for trusted inspection.
			assertTrue(failure.getCause().getMessage().contains(secret));
			assertEquals(1L, database.query("SELECT COUNT(*) FROM " + table).fetchObject(Long.class).orElseThrow());
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	private static Database database() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return Database.with(source, DatabaseType.POSTGRESQL).build();
	}
	private static String identifier(String prefix) { return prefix + "_" + UUID.randomUUID().toString().replace("-", ""); }
}
