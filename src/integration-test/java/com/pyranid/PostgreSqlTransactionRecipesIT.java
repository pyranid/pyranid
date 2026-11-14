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

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlTransactionRecipesIT {
	@Container private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("transaction_recipes").withUsername("pyranid").withPassword("pyranid");

	@Test
	void aCaughtJoinedFailureStillRollsBackTheOwnerAndItsNormallyReturnedValue() {
		Database database = Database.with(source(), DatabaseType.POSTGRESQL).build();
		String table = createTable(database);
		try {
			IllegalStateException rejection = new IllegalStateException("joined failure");
			TransactionOutcome<String> result = database.transactionWithOutcome(() -> {
				insert(database, table, 1);
				assertSame(rejection, assertThrows(IllegalStateException.class, () -> joinOrBegin(database, () -> {
					insert(database, table, 2);
					throw rejection;
				})));
				assertTrue(database.currentTransaction().orElseThrow().isRollbackOnly());
				insert(database, table, 3);
				return Optional.of("handled");
			});
			assertEquals(TransactionResult.ROLLED_BACK, result.getResult());
			assertEquals(Optional.of("handled"), result.getAttemptedValue());
			assertTrue(result.getFailure().isEmpty());
			assertEquals(List.of(), ids(database, table));
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void savepointOrBeginRecoversPostgresAbortedStatementStateWithoutLosingOuterWork() {
		Database database = Database.with(source(), DatabaseType.POSTGRESQL).build();
		String table = createTable(database);
		try {
			assertEquals(Optional.of(1), savepointOrBegin(database, () -> {
				insert(database, table, 1);
				return Optional.of(1);
			}));
			TransactionOutcome<String> result = database.transactionWithOutcome(() -> {
				insert(database, table, 2);
				DatabaseException failure = assertThrows(DatabaseException.class, () -> savepointOrBegin(database, () -> {
					insert(database, table, 3);
					insert(database, table, 1);
					return Optional.empty();
				}));
				assertEquals("23505", failure.getSqlState().orElseThrow());
				assertFalse(database.currentTransaction().orElseThrow().isRollbackOnly());
				insert(database, table, 4);
				return Optional.of("continued");
			});
			assertEquals(TransactionResult.COMMITTED, result.getResult());
			assertEquals(Optional.of("continued"), result.getAttemptedValue());
			assertTrue(result.getFailure().isEmpty());
			assertEquals(List.of(1, 2, 4), ids(database, table));
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	@Test
	void reusedAmbientConnectionFailsBeforeInnerWorkAndTheOuterOwnerCanStillComplete() throws Exception {
		PGSimpleDataSource source = source();
		Database inspection = Database.with(source, DatabaseType.POSTGRESQL).build();
		String table = createTable(inspection);
		try {
			for (boolean commitOuter : new boolean[]{false, true}) {
				inspection.query("DELETE FROM " + table).execute();
				try (Connection physical = source.getConnection()) {
					AtomicInteger returns = new AtomicInteger(), aborts = new AtomicInteger();
					Connection borrowed = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
							(proxy, method, args) -> {
								if (method.getName().equals("close")) { returns.incrementAndGet(); return null; }
								if (method.getName().equals("abort")) aborts.incrementAndGet();
								try { return method.invoke(physical, args); }
								catch (InvocationTargetException failure) { throw failure.getCause(); }
							});
					DataSource singleConnection = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
							(proxy, method, args) -> {
								if (method.getName().equals("getConnection")) return borrowed;
								throw new AssertionError(method.getName());
							});
					Database database = Database.with(singleConnection, DatabaseType.POSTGRESQL).build();
					TransactionOutcome<Void> result = database.transactionWithOutcome(() -> {
						insert(database, table, 1);
						TransactionOutcome<Void> rejected = database.transactionWithOutcome(() -> { insert(database, table, 2); });
						assertEquals(TransactionResult.ROLLED_BACK, rejected.getResult());
						assertTrue(rejected.getFailure().orElseThrow().getMessage().contains("already owned by an enclosing transaction"));
						assertEquals(0, returns.get());
						assertEquals(0, aborts.get());
						assertFalse(database.currentTransaction().orElseThrow().isRollbackOnly());
						insert(database, table, 3);
						if (!commitOuter) database.currentTransaction().orElseThrow().setRollbackOnly(true);
					});
					assertEquals(commitOuter ? TransactionResult.COMMITTED : TransactionResult.ROLLED_BACK, result.getResult());
					assertTrue(result.getFailure().isEmpty());
					assertEquals(commitOuter ? List.of(1, 3) : List.of(), ids(inspection, table));
					assertEquals(1, returns.get());
					assertEquals(0, aborts.get());
					assertFalse(physical.isClosed());
				}
			}
		} finally { inspection.query("DROP TABLE " + table).execute(); }
	}

	static <T> Optional<T> joinOrBegin(Database database, ReturningTransactionalOperation<T> operation) {
		Transaction current = database.currentTransaction().orElse(null);
		return current == null ? database.transaction(operation) : database.participate(current, operation);
	}

	static <T> Optional<T> savepointOrBegin(Database database, ReturningTransactionalOperation<T> operation) {
		Transaction current = database.currentTransaction().orElse(null);
		return current == null ? database.transaction(operation) : current.withSavepoint(operation);
	}

	private static void insert(Database database, String table, int id) {
		database.query("INSERT INTO " + table + " VALUES (:id)").bind("id", id).execute();
	}
	private static List<Integer> ids(Database database, String table) {
		return database.query("SELECT id FROM " + table + " ORDER BY id").fetchList(Integer.class);
	}
	private static String createTable(Database database) {
		String table = "transaction_recipe_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY)").execute();
		return table;
	}
	private static PGSimpleDataSource source() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword()); return source;
	}
}
