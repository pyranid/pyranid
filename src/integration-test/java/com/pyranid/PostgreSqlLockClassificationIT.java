/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlLockClassificationIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_locks").withUsername("pyranid").withPassword("pyranid");

	@Test
	@Timeout(30)
	void lockTimeoutAndNowaitRetryOnlyAfterTheFailedBodyHasRolledBack() throws SQLException {
		for (boolean nowait : new boolean[]{false, true}) {
			PGSimpleDataSource source = source();
			Database inspection = Database.with(source, DatabaseType.POSTGRESQL).build();
			Database database = Database.with(source, DatabaseType.POSTGRESQL).build();
			String table = "lock_classification_" + UUID.randomUUID().toString().replace("-", "");
			String attemptsTable = table + "_attempts";
			inspection.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, amount INT NOT NULL)").execute();
			inspection.query("CREATE TABLE " + attemptsTable + " (attempt INT PRIMARY KEY)").execute();
			inspection.query("INSERT INTO " + table + " VALUES (1, 10)").execute();
			try (Connection blocker = source.getConnection()) {
				blocker.setAutoCommit(false);
				try (Statement lock = blocker.createStatement()) {
					assertEquals(1, lock.executeUpdate("UPDATE " + table + " SET amount = 99 WHERE id = 1"));
				}
				// The completed UPDATE acknowledgment proves the other connection owns the row lock before the attempt starts.
				AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger();
				List<TransactionCompletion> completions = new ArrayList<>();
				RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
					policies.incrementAndGet();
					assertEquals("55P03", failure.getSqlState().orElseThrow());
					assertTrue(failure.isTimeout());
					assertTrue(failure.isTransient());
					assertFalse(failure.isSerializationFailure());
					assertFalse(failure.isDeadlock());
					assertEquals(TransactionResult.ROLLED_BACK, completions.get(0).getResult());
					assertEquals(0L, inspection.query("SELECT COUNT(*) FROM " + attemptsTable).fetchObject(Long.class).orElseThrow());
					try { blocker.rollback(); }
					catch (SQLException cleanup) { throw new IllegalStateException("Unable to release test lock", cleanup); }
					return RetryPolicy.Condition.timeout().shouldRetry(failure);
				});
				TransactionRetryResult<String> result = database.transactionWithRetry(policy,
						TransactionOptions.withReadOnly(null).completionObserver(completions::add).build(), () -> {
					int attempt = attempts.incrementAndGet();
					database.query("INSERT INTO " + attemptsTable + " VALUES (:attempt)").bind("attempt", attempt).execute();
					database.query("SET LOCAL statement_timeout = '5s'").execute();
					if (!nowait) database.query("SET LOCAL lock_timeout = '50ms'").execute();
					assertEquals(1, database.query("SELECT id FROM " + table + " WHERE id = 1 FOR UPDATE" + (nowait ? " NOWAIT" : ""))
							.fetchObject(Integer.class).orElseThrow());
					database.query("UPDATE " + table + " SET amount = amount + 1 WHERE id = 1").execute();
					return Optional.of("committed");
				});
				assertEquals(Optional.of("committed"), result.getValue());
				assertEquals(2, result.getAttemptCount());
				assertEquals(2, attempts.get());
				assertEquals(1, policies.get());
				assertEquals(1, result.getFailures().size());
				assertEquals(List.of(TransactionResult.ROLLED_BACK, TransactionResult.COMMITTED),
						completions.stream().map(TransactionCompletion::getResult).toList());
				assertEquals(List.of(2), inspection.query("SELECT attempt FROM " + attemptsTable).fetchList(Integer.class));
				assertEquals(11, inspection.query("SELECT amount FROM " + table + " WHERE id = 1").fetchObject(Integer.class).orElseThrow());
			} finally {
				inspection.query("DROP TABLE " + attemptsTable).execute();
				inspection.query("DROP TABLE " + table).execute();
			}
		}
	}

	@Test
	@Timeout(20)
	void aHandledNowaitFailureStillRefusesCommitAndPreservesItsDiagnostics() throws SQLException {
		PGSimpleDataSource source = source();
		Database database = Database.with(source, DatabaseType.POSTGRESQL).build();
		String table = "handled_lock_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY)").execute();
		database.query("INSERT INTO " + table + " VALUES (1)").execute();
		try (Connection blocker = source.getConnection()) {
			blocker.setAutoCommit(false);
			try (Statement lock = blocker.createStatement()) {
				assertEquals(1, lock.executeUpdate("UPDATE " + table + " SET id = id WHERE id = 1"));
			}
			DatabaseException[] handled = new DatabaseException[1];
			TransactionOutcome<String> outcome = database.transactionWithOutcome(() -> {
				database.query("INSERT INTO " + table + " VALUES (2)").execute();
				handled[0] = assertThrows(DatabaseException.class, () -> database.query(
						"SELECT id FROM " + table + " WHERE id = 1 FOR UPDATE NOWAIT").fetchObject(Integer.class));
				return Optional.of("attempted");
			});
			assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
			assertEquals(Optional.of("attempted"), outcome.getAttemptedValue());
			DatabaseException refusal = assertInstanceOf(DatabaseException.class, outcome.getFailure().orElseThrow());
			assertSame(handled[0], refusal.getCause());
			assertTrue(refusal.isTimeout());
			assertTrue(refusal.isTransient());
			assertEquals(List.of(1), database.query("SELECT id FROM " + table + " ORDER BY id").fetchList(Integer.class));
			blocker.rollback();
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	private static PGSimpleDataSource source() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return source;
	}
}
