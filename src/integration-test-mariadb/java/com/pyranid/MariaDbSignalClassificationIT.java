/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class MariaDbSignalClassificationIT {
	@Container
	private static final MariaDBContainer MARIA_DB = new MariaDBContainer(DockerImageName.parse(
			System.getProperty("mariadb.integration.image", "mariadb:11.4")).asCompatibleSubstituteFor("mariadb"))
			.withDatabaseName("pyranid_signals").withUsername("pyranid").withPassword("pyranid");

	@Test
	void procedureAndTriggerSignalsAreNotTransientAndDoNotRepeatTheBoundary() {
		for (boolean trigger : new boolean[]{false, true}) {
			try (Fixture fixture = new Fixture()) {
				AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger();
				List<TransactionResult> completions = new ArrayList<>();
				RetryPolicy policy = RetryPolicy.ofMaxAttempts(3, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
					policies.incrementAndGet();
					return failure.isTransient();
				});
				DatabaseException failure = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(
						policy, TransactionOptions.withIsolation(TransactionIsolation.DEFAULT)
								.completionObserver(completion -> completions.add(completion.getResult())).build(), () -> {
							attempts.incrementAndGet();
							fixture.database.query("INSERT INTO " + fixture.table + " VALUES (2, 0)").execute();
							if (trigger) fixture.database.query("INSERT INTO " + fixture.table + " VALUES (9, 0)").execute();
							else fixture.database.query("CALL " + fixture.procedure + "()").execute();
						}));
				SQLTransientConnectionException driver = assertInstanceOf(SQLTransientConnectionException.class, failure.getCause());
				assertEquals("45000", driver.getSQLState());
				assertEquals("45000", failure.getSqlState().orElseThrow());
				assertEquals(1644, failure.getErrorCode().orElseThrow());
				assertFalse(failure.isTransient());
				assertFalse(failure.isTimeout());
				assertFalse(failure.isSerializationFailure());
				assertEquals(1, attempts.get());
				assertEquals(1, policies.get());
				assertEquals(List.of(TransactionResult.ROLLED_BACK), completions);
				assertEquals(List.of(1), fixture.ids());
			}
		}
	}

	@Test
	void aRealLockWaitTimeoutRemainsTransientAndCanRetryAfterRollback() throws Exception {
		try (Fixture fixture = new Fixture(); Connection owner = fixture.source.getConnection(); Statement lock = owner.createStatement()) {
			owner.setAutoCommit(false);
			lock.executeUpdate("UPDATE " + fixture.table + " SET counter = counter + 1 WHERE id = 1");
			AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger();
			List<TransactionResult> completions = new ArrayList<>();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				policies.incrementAndGet();
				assertEquals(1205, failure.getErrorCode().orElseThrow());
				assertTrue(failure.isTimeout());
				assertTrue(failure.isTransient());
				try { owner.rollback(); }
				catch (SQLException cleanup) { throw new AssertionError("Could not release the held lock", cleanup); }
				return failure.isTransient();
			});
			TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(policy,
					TransactionOptions.withIsolation(TransactionIsolation.DEFAULT)
							.completionObserver(completion -> completions.add(completion.getResult())).build(), () -> {
						attempts.incrementAndGet();
						fixture.database.useRawConnection(connection -> {
							try (Statement settings = connection.createStatement()) {
								settings.execute("SET SESSION innodb_lock_wait_timeout = 1");
							}
							return Optional.empty();
						});
						fixture.database.query("INSERT INTO " + fixture.table + " VALUES (2, 0)").execute();
						fixture.database.query("UPDATE " + fixture.table + " SET counter = counter + 1 WHERE id = 1").execute();
					});
			assertEquals(2, result.getAttemptCount());
			assertEquals(2, attempts.get());
			assertEquals(1, policies.get());
			assertEquals(List.of(TransactionResult.ROLLED_BACK, TransactionResult.COMMITTED), completions);
			assertEquals(List.of(1, 2), fixture.ids());
			assertEquals(1, fixture.database.query("SELECT counter FROM " + fixture.table + " WHERE id = 1")
					.fetchObject(Integer.class).orElseThrow());
		}
	}

	private static final class Fixture implements AutoCloseable {
		final DataSource source = new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
				MARIA_DB.getJdbcUrl(), MARIA_DB.getUsername(), MARIA_DB.getPassword());
		final Database database = Database.with(this.source, DatabaseType.MARIA_DB).build();
		final String suffix = UUID.randomUUID().toString().replace("-", "");
		final String table = "signal_work_" + this.suffix;
		final String procedure = "fail_business_" + this.suffix;

		Fixture() {
			this.database.query("CREATE TABLE " + this.table + " (id INT PRIMARY KEY, counter INT NOT NULL) ENGINE=InnoDB").execute();
			this.database.query("INSERT INTO " + this.table + " VALUES (1, 0)").execute();
			this.database.query("CREATE PROCEDURE " + this.procedure
					+ "() BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Business rule'; END").execute();
			this.database.query("CREATE TRIGGER reject_signal_" + this.suffix + " BEFORE INSERT ON " + this.table
					+ " FOR EACH ROW BEGIN IF NEW.id = 9 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Business rule'; END IF; END").execute();
		}

		List<Integer> ids() { return this.database.query("SELECT id FROM " + this.table + " ORDER BY id").fetchList(Integer.class); }
		@Override public void close() {
			try { this.database.query("DROP PROCEDURE " + this.procedure).execute(); }
			finally { this.database.query("DROP TABLE " + this.table).execute(); }
		}
	}
}
