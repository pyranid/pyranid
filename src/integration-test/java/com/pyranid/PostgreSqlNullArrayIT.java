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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlNullArrayIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_null_arrays").withUsername("pyranid").withPassword("pyranid");

	@Test
	void typedNullArraysWorkWithColumnContextAndContextFreeTypeInspection() {
		Database database = database();
		for (String[] type : List.of(new String[]{"text", "text[]"}, new String[]{"int4", "integer[]"},
				new String[]{"integer", "integer[][]"}, new String[]{"uuid", "uuid[]"})) {
			String table = identifier("typed_null_array");
			database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, payload " + type[1] + ")").execute();
			try {
				TransactionOutcome<Void> outcome = database.transactionWithOutcome(() -> {
					database.query("INSERT INTO " + table + " VALUES (1, NULL)").execute();
					database.query("INSERT INTO " + table + " VALUES (2, :payload)")
							.bind("payload", Parameters.sqlArrayOf(type[0], (Object[]) null)).execute();
					assertType(database, type[0], type[1]);
				});
				assertEquals(TransactionResult.COMMITTED, outcome.getResult());
				assertTrue(outcome.getFailure().isEmpty());
				assertEquals(2L, database.query("SELECT COUNT(*) FROM " + table + " WHERE payload IS NULL")
						.fetchObject(Long.class).orElseThrow());
			} finally {
				database.query("DROP TABLE " + table).execute();
			}
		}
	}

	@Test
	void nullAndEmptyArraysRemainDistinctAcrossRepeatedAndBatchBindings() {
		Database database = database();
		String table = identifier("null_empty_array");
		database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, payload TEXT[])").execute();
		try {
			database.transaction(() -> {
				database.query("INSERT INTO " + table + " VALUES (1, :payload)")
						.bind("payload", Parameters.sqlArrayOf("text", (List<String>) null)).execute();
				database.query("INSERT INTO " + table + " VALUES (2, :payload)")
						.bind("payload", Parameters.sqlArrayOf("text", List.of())).execute();
				database.query("INSERT INTO " + table + " VALUES (:id, :payload)").batchChunkSize(1).executeBatch(List.of(
						java.util.Map.of("id", 3, "payload", Parameters.sqlArrayOf("text", (List<String>) null)),
						java.util.Map.of("id", 4, "payload", Parameters.sqlArrayOf("text", List.of("present")))));
			});
			assertEquals(List.of(1, 3), database.query("SELECT id FROM " + table + " WHERE payload IS NULL ORDER BY id")
					.fetchList(Integer.class));
			assertEquals(0, database.query("SELECT cardinality(payload) FROM " + table + " WHERE id = 2")
					.fetchObject(Integer.class).orElseThrow());
			assertEquals(List.of("present"), database.query("SELECT payload FROM " + table + " WHERE id = 4")
					.fetchObject(String[].class).map(java.util.Arrays::asList).orElseThrow());
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	@Test
	void quotedSchemaEnumAndElementDomainsKeepTheirActualArrayTypes() {
		Database database = database();
		String schema = "Array.Schema " + UUID.randomUUID();
		String qualifiedSchema = quoted(schema);
		String enumeration = qualifiedSchema + "." + quoted("Enum.Type Name");
		String domain = qualifiedSchema + "." + quoted("Element.Domain Name");
		String table = qualifiedSchema + "." + quoted("payloads");
		database.query("CREATE SCHEMA " + qualifiedSchema).execute();
		try {
			database.query("CREATE TYPE " + enumeration + " AS ENUM ('first', 'second')").execute();
			database.query("CREATE DOMAIN " + domain + " AS text CHECK (VALUE <> '')").execute();
			database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, enums " + enumeration + "[], domains " + domain + "[])").execute();
			database.transaction(() -> {
				database.query("INSERT INTO " + table + " VALUES (1, :enums, :domains)")
						.bind("enums", Parameters.sqlArrayOf(enumeration, (Object[]) null))
						.bind("domains", Parameters.sqlArrayOf(domain, (Object[]) null)).execute();
				assertType(database, enumeration, enumeration + "[]");
				assertType(database, domain, domain + "[]");
			});
			assertTrue(database.query("SELECT enums IS NULL AND domains IS NULL FROM " + table)
					.fetchObject(Boolean.class).orElseThrow());
		} finally {
			database.query("DROP SCHEMA " + qualifiedSchema + " CASCADE").execute();
		}
	}

	@Test
	void invalidTypeNamesPropagateOriginalDriverDiagnostics() {
		Database database = database();
		String missing = identifier("missing_element");
		DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query("SELECT :payload")
				.bind("payload", Parameters.sqlArrayOf(missing, (Object[]) null)).fetchObject(Object.class));
		PSQLException driver = assertInstanceOf(PSQLException.class, failure.getCause());
		assertEquals(driver.getSQLState(), failure.getSqlState().orElseThrow());
		assertTrue(failure.getMessage().contains(missing));
		assertTrue(failure.getMessage().contains("sql=SELECT"));
	}

	@Test
	void guardedRawBindersRetainTypeWithoutExposingTransactionControls() {
		Database database = database();
		StatementContext<?> context = StatementContext.with(Statement.of("raw-null-array",
				"SELECT pg_typeof(?) = 'text[]'::regtype"), database).build();
		TransactionOutcome<Void> outcome = database.transactionWithOutcome(() -> {
			database.useRawConnection(connection -> {
				assertThrows(IllegalStateException.class, connection::commit);
				try (PreparedStatement statement = connection.prepareStatement("SELECT pg_typeof(?) = 'text[]'::regtype")) {
					try {
						PreparedStatementBinder.withDefaultConfiguration().bindParameter(context, statement, 1,
								Parameters.sqlArrayOf("text", (Object[]) null));
						try (ResultSet result = statement.executeQuery()) {
							assertTrue(result.next());
							assertTrue(result.getBoolean(1));
						}
					} finally {
						// A direct binder's caller owns its context cleanup within the raw callback lifetime.
						AutoCloseable cleanup;
						while ((cleanup = context.getCleanupOperations().poll()) != null) cleanup.close();
					}
				}
				return Optional.empty();
			});
		});
		assertEquals(TransactionResult.COMMITTED, outcome.getResult());
		assertTrue(outcome.getFailure().isEmpty());
		assertTrue(context.getCleanupOperations().isEmpty());
	}

	private static void assertType(Database database, String elementType, String expectedArrayType) {
		assertTrue(database.query("SELECT pg_typeof(:payload) = CAST(:expected AS regtype)")
				.bind("payload", Parameters.sqlArrayOf(elementType, (Object[]) null))
				.bind("expected", expectedArrayType).fetchObject(Boolean.class).orElseThrow());
	}

	private static Database database() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return Database.with(source, DatabaseType.POSTGRESQL).build();
	}
	private static String identifier(String prefix) { return prefix + "_" + UUID.randomUUID().toString().replace("-", ""); }
	private static String quoted(String value) { return "\"" + value.replace("\"", "\"\"") + "\""; }
}
