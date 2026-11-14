/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class ParameterNameRedactionIT {
	@Container private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("parameter_names").withUsername("pyranid").withPassword("pyranid");
	@TempDir Path directory;

	@Test
	void postgresKeepsNamesThroughExpansionStreamingBatchAndDriverFailures() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		verify(source, DatabaseType.POSTGRESQL);
	}

	@Test
	void sqliteKeepsNamesThroughExpansionStreamingBatchAndDriverFailures() {
		verify(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:sqlite:" + this.directory.resolve("names.sqlite"), null, null), DatabaseType.SQLITE);
	}

	@Test
	void duckDbKeepsNamesThroughExpansionStreamingBatchAndDriverFailures() {
		verify(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:duckdb:" + this.directory.resolve("names.duckdb"), null, null), DatabaseType.DUCK_DB);
	}

	private static void verify(DataSource source, DatabaseType type) {
		for (int capacity : new int[]{0, 16}) {
			AtomicReference<StatementContext<?>> captured = new AtomicReference<>();
			List<String> redactedNames = new ArrayList<>();
			ParameterRedactor redactor = new ParameterRedactor() {
				@Override public Optional<Object> redactParameter(StatementContext<?> context, Integer index, Object value) { throw new AssertionError("Use named overload"); }
				@Override public Optional<Object> redactParameter(StatementContext<?> context, Integer index, String name, Object value) {
					redactedNames.add(name);
					return Optional.ofNullable("password".equals(name) ? "<password>" : value);
				}
			};
			Database database = Database.with(source, type).parsedSqlCacheCapacity(capacity).parameterRedactor(redactor)
					.statementLogger(log -> captured.set(log.getStatementContext())).build();
			String sql = "SELECT CAST(:visible AS VARCHAR(80)) AS value WHERE :visible=:password OR 1 IN (:ids)";
			assertEquals(List.of("public"), database.query(sql).bind("password", Optional.of("private-password-81927"))
					.bind("ids", Parameters.inList(new int[]{1, 2})).bind("visible", "public").fetchStream(String.class, rows -> rows.toList()));
			StatementContext<?> ordinary = captured.get();
			assertEquals(List.of("visible", "visible", "password", "ids", "ids"), ordinary.getParameterNames());
			assertEquals(List.of("public", "public", "<password>", 1, 2), ordinary.getRedactedParameters());
			assertEquals(ordinary.getParameterNames(), redactedNames);
			assertThrows(UnsupportedOperationException.class, () -> ordinary.getParameterNames().set(0, "replacement"));

			String table = "named_batch_" + UUID.randomUUID().toString().replace("-", "");
			database.query("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, payload VARCHAR(80))").execute();
			try {
				redactedNames.clear();
				assertEquals(List.of(1L, 1L), database.query("INSERT INTO " + table + " VALUES (:id, :password)").batchChunkSize(1)
						.executeBatch(List.of(Map.of("id", 1, "password", "first-private"), Map.of("id", 2, "password", "second-private"))));
				StatementContext<?> batch = captured.get();
				assertEquals(List.of("id", "password"), batch.getParameterNames());
				assertEquals(List.of("<batch: 2 groups x 2 parameters>"), batch.getRedactedParameters());
				assertTrue(redactedNames.isEmpty());
				assertEquals(List.of("first-private", "second-private"), database.query("SELECT payload FROM " + table + " WHERE id IN(:ids) ORDER BY id")
						.bind("ids", Parameters.secure(Parameters.inList(new int[]{1, 2}), "<ids>"))
						.fetchList(String.class));
				assertEquals(List.of("ids", "ids"), captured.get().getParameterNames());
				assertEquals(List.of("<ids>", "<ids>"), captured.get().getRedactedParameters());
				assertTrue(redactedNames.isEmpty(), "Secure masks bypass both overloads");
				DatabaseException failure = assertThrows(DatabaseException.class,
						() -> database.query("SELECT CAST(:password AS VARCHAR(80)) FROM absent_named_table_" + UUID.randomUUID().toString().replace("-", ""))
								.bind("password", "private-password-81927").fetchList(String.class));
				assertFalse(failure.getMessage().contains("private-password-81927"));
				assertEquals(List.of("password"), captured.get().getParameterNames());
				assertEquals(List.of("<password>"), captured.get().getRedactedParameters());
				assertEquals(2L, database.query("SELECT COUNT(*) FROM " + table).fetchObject(Long.class).orElseThrow());
			} finally { database.query("DROP TABLE " + table).execute(); }
		}
	}
}
