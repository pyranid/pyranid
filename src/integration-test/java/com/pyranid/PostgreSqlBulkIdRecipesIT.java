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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlBulkIdRecipesIT {
	@Container private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("bulk_id_recipes").withUsername("pyranid").withPassword("pyranid");
	public record Inserted(long id, String label) {}

	@Test
	void unnestAndReturningReturnActualIdsWithLabelsForCorrelationIncludingEmptyInput() {
		Database database = database();
		String table = createTable(database);
		try {
			List<String> labels = List.of("Ada's item", "雪", "last");
			String sql = "INSERT INTO " + table + " (label) SELECT input.label FROM unnest(:labels::text[]) AS input(label) RETURNING id, label";
			List<Inserted> returned = database.query(sql).bind("labels", Parameters.sqlArrayOf("text", labels)).executeForList(Inserted.class);
			assertActualRows(database, table, labels, returned);
			assertEquals(List.of(), database.query(sql).bind("labels", Parameters.sqlArrayOf("text", List.of())).executeForList(Inserted.class));
			assertEquals(3L, database.query("SELECT COUNT(*) FROM " + table).fetchObject(Long.class).orElseThrow());
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void aSingleMultiValuesReturningStatementReturnsEveryActualKeyWithoutInventingARange() {
		Database database = database();
		String table = createTable(database);
		try {
			List<String> labels = List.of("first", "second", "third");
			List<Inserted> returned = database.query("INSERT INTO " + table + " (label) VALUES (:first), (:second), (:third) RETURNING id, label")
					.bind("first", labels.get(0)).bind("second", labels.get(1)).bind("third", labels.get(2)).executeForList(Inserted.class);
			assertActualRows(database, table, labels, returned);
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void guardedRawJdbcCanExecuteAPostgresGeneratedKeyBatchOnOneTransactionConnection() {
		Database database = database();
		String table = createTable(database);
		try {
			List<String> labels = List.of("raw first", "raw 'quoted'", "raw 雪");
			List<Long> returned = database.transaction(() -> Optional.of(rawGeneratedKeys(database, table, labels))).orElseThrow();
			List<Inserted> stored = database.query("SELECT id, label FROM " + table).fetchList(Inserted.class);
			assertEquals(labels.size(), returned.size());
			assertEquals(new HashSet<>(returned), new HashSet<>(stored.stream().map(Inserted::id).toList()));
			assertEquals(new HashSet<>(labels), new HashSet<>(stored.stream().map(Inserted::label).toList()));
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void returnedBulkIdsDoNotEstablishThatTheirSurroundingTransactionCommitted() {
		Database database = database();
		String table = createTable(database);
		try {
			TransactionOutcome<List<Long>> result = database.transactionWithOutcome(() -> {
				List<Long> keys = rawGeneratedKeys(database, table, List.of("tentative one", "tentative two"));
				database.currentTransaction().orElseThrow().setRollbackOnly(true);
				return Optional.of(keys);
			});
			assertEquals(TransactionResult.ROLLED_BACK, result.getResult());
			assertEquals(2, result.getAttemptedValue().orElseThrow().size());
			assertTrue(result.getFailure().isEmpty());
			assertEquals(0L, database.query("SELECT COUNT(*) FROM " + table).fetchObject(Long.class).orElseThrow());
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	private static List<Long> rawGeneratedKeys(Database database, String table, List<String> labels) {
		return database.useRawConnection(connection -> {
			try (PreparedStatement statement = connection.prepareStatement("INSERT INTO " + table + " (label) VALUES (?)", new String[]{"id"})) {
				for (String label : labels) {
					statement.setString(1, label);
					statement.addBatch();
				}
				int[] counts = statement.executeBatch();
				assertEquals(labels.size(), counts.length);
				for (int count : counts) assertEquals(1, count);
				List<Long> keys = new ArrayList<>();
				try (ResultSet rows = statement.getGeneratedKeys()) {
					assertEquals(1, rows.getMetaData().getColumnCount());
					while (rows.next()) keys.add(rows.getLong(1));
				}
				return Optional.of(List.copyOf(keys));
			}
		}).orElseThrow();
	}

	private static void assertActualRows(Database database, String table, List<String> labels, List<Inserted> returned) {
		assertEquals(labels.size(), returned.size());
		assertEquals(returned.size(), new HashSet<>(returned.stream().map(Inserted::id).toList()).size());
		assertEquals(new HashSet<>(labels), new HashSet<>(returned.stream().map(Inserted::label).toList()));
		assertEquals(new HashSet<>(returned), new HashSet<>(database.query("SELECT id, label FROM " + table).fetchList(Inserted.class)));
		for (Inserted row : returned)
			assertEquals(row.label(), database.query("SELECT label FROM " + table + " WHERE id=:id").bind("id", row.id()).fetchObject(String.class).orElseThrow());
	}
	private static String createTable(Database database) {
		String table = "bulk_id_recipe_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, label TEXT NOT NULL)").execute();
		return table;
	}
	private static Database database() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return Database.with(source, DatabaseType.POSTGRESQL).build();
	}
}
