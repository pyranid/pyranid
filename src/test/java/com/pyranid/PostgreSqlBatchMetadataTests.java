/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import com.pyranid.testdriver.util.PSQLException;
import com.pyranid.testdriver.util.ServerErrorMessage;
import org.junit.jupiter.api.Test;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class PostgreSqlBatchMetadataTests {
	@Test
	void jdbcWrappersFindServerMetadataThroughCauseAndNextWithoutLosingPrimaryDiagnostics() {
		for (boolean throughCause : new boolean[]{false, true}) {
			PSQLException driver = driver(new ServerErrorMessage());
			BatchUpdateException batch = new BatchUpdateException("batch", "23505", 37, new int[]{1});
			if (throughCause) batch.initCause(new IllegalArgumentException("intermediate", driver));
			else batch.setNextException(driver);
			DatabaseException failure = new DatabaseException("batch failed", batch, PostgresDialect.INSTANCE);
			assertSame(batch, failure.getCause());
			assertEquals("23505", failure.getSqlState().orElseThrow());
			assertEquals(37, failure.getErrorCode().orElseThrow());
			assertEquals("employee_email_key", failure.getConstraint().orElseThrow());
			assertEquals("employee", failure.getTable().orElseThrow());
			assertEquals("public", failure.getSchema().orElseThrow());
			assertEquals("email", failure.getColumn().orElseThrow());
			assertEquals("Key (email)=(a@b.c) already exists.", failure.getDetail().orElseThrow());
			assertTrue(failure.isUniqueConstraintViolation());
			assertEquals(0, batch.getSuppressed().length);
			assertEquals(0, driver.getSuppressed().length);
		}
	}

	@Test
	void nullServerMessagesAndCyclicSharedNodesDoNotHideTheFirstUsefulMessage() {
		BatchUpdateException batch = new BatchUpdateException("batch", null, 12, new int[0]);
		PSQLException empty = driver(null);
		PSQLException rich = driver(new ServerErrorMessage());
		batch.initCause(empty);
		batch.setNextException(empty);
		empty.setNextException(rich);
		rich.setNextException(batch);
		DatabaseExceptionMetadata metadata = PostgresDialect.INSTANCE.databaseExceptionMetadata(batch);
		assertEquals("23505", metadata.sqlState);
		assertEquals(12, metadata.errorCode);
		assertEquals("employee_email_key", metadata.constraint);
	}

	@Test
	void primaryStateWinsForJdbcShellsWhileDirectDriverStateKeepsItsEstablishedMeaning() {
		SQLException transport = new SQLException("transport", "08006", 91);
		transport.setNextException(driver(new ServerErrorMessage()));
		DatabaseExceptionMetadata metadata = PostgresDialect.INSTANCE.databaseExceptionMetadata(transport);
		assertEquals("08006", metadata.sqlState);
		assertEquals(91, metadata.errorCode);
		assertEquals("employee_email_key", metadata.constraint);
		PSQLException direct = new PSQLException("driver", "HY000", new ServerErrorMessage());
		assertEquals("23505", PostgresDialect.INSTANCE.databaseExceptionMetadata(direct).sqlState);
	}

	@Test
	void nonJdbcRootsKeepEmptyMetadataEvenWhenTheirCausesHaveServerDiagnostics() {
		for (Throwable root : List.of(new IllegalArgumentException("runtime", driver(new ServerErrorMessage())),
				new AssertionError("error", driver(new ServerErrorMessage())))) {
			DatabaseException failure = new DatabaseException("wrapped", root, PostgresDialect.INSTANCE);
			assertTrue(failure.getSqlState().isEmpty());
			assertTrue(failure.getErrorCode().isEmpty());
			assertTrue(failure.getConstraint().isEmpty());
			assertTrue(failure.getDetail().isEmpty());
			assertTrue(failure.isUniqueConstraintViolation());
			assertSame(root, failure.getCause());
		}
	}

	@Test
	void causePrecedenceAndSuppressedCleanupIsolationSelectTheIntendedFailure() {
		ServerErrorMessage caused = new ServerErrorMessage() {
			@Override public String getConstraint() { return "cause_constraint"; }
		};
		SQLException root = new SQLException("wrapper", "23505", driver(caused));
		root.setNextException(driver(new ServerErrorMessage()));
		assertEquals("cause_constraint", PostgresDialect.INSTANCE.databaseExceptionMetadata(root).constraint);
		SQLException cleanupOnly = new SQLException("ordinary failure", "HY000");
		cleanupOnly.addSuppressed(driver(new ServerErrorMessage()));
		DatabaseExceptionMetadata metadata = PostgresDialect.INSTANCE.databaseExceptionMetadata(cleanupOnly);
		assertNull(metadata.constraint);
		assertNull(metadata.detail);
		assertEquals("HY000", metadata.sqlState);
	}

	@Test
	void unreadableEdgesAreIndependentAndMetadataSearchDoesNotMutateTheOriginalFailure() {
		for (boolean unreadableCause : new boolean[]{false, true}) {
			IllegalStateException diagnostic = new IllegalStateException("unreadable diagnostic edge");
			SQLException shell = new SQLException("wrapper", "23505") {
				@Override public Throwable getCause() {
					if (unreadableCause) throw diagnostic;
					return super.getCause();
				}
				@Override public SQLException getNextException() {
					if (!unreadableCause) throw diagnostic;
					return super.getNextException();
				}
			};
			PSQLException driver = driver(new ServerErrorMessage());
			if (unreadableCause) shell.setNextException(driver); else shell.initCause(driver);
			assertEquals("employee_email_key", PostgresDialect.INSTANCE.databaseExceptionMetadata(shell).constraint);
			assertEquals(0, shell.getSuppressed().length);
			assertEquals(0, driver.getSuppressed().length);
		}
	}

	@Test
	void newlyVisibleServerMetadataStillUsesTheExistingStatementScrubber() {
		SQLException batch = new BatchUpdateException("batch", "23505", 0, new int[0]);
		batch.setNextException(driver(new ServerErrorMessage()));
		DatabaseException failure = DatabaseException.wrapSafely("scrubbed statement", batch, PostgresDialect.INSTANCE,
				value -> value.replace("a@b.c", "<redacted>"));
		assertEquals("Key (email)=(<redacted>) already exists.", failure.getDetail().orElseThrow());
		assertSame(batch, failure.getCause());
		assertEquals("Key (email)=(a@b.c) already exists.",
				PostgresDialect.INSTANCE.databaseExceptionMetadata(batch).detail);
	}

	@Test
	void mixedCommitEvidenceRemainsInDoubtAndCannotReplayPersistedWork() {
		ServerErrorMessage serialization = new ServerErrorMessage() {
			@Override public String getSQLState() { return "40001"; }
			@Override public String getMessage() { return "serialization detail"; }
		};
		SQLException driver = new SQLException("commit acknowledgment lost", "08006", 17);
		driver.setNextException(new PSQLException("serialization detail", "40001", serialization));
		TransactionCommitHardeningTests.Fixture fixture = new TransactionCommitHardeningTests.Fixture(DatabaseType.POSTGRESQL, driver, true);
		AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger();
		AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
		DatabaseException failure = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(
				RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), ignored -> { policies.incrementAndGet(); return true; }),
				TransactionOptions.withReadOnly(null).completionObserver(completion::set).build(), () -> {
					attempts.incrementAndGet(); fixture.insert(1);
				}));
		assertSame(driver, failure.getCause());
		assertEquals("08006", failure.getSqlState().orElseThrow());
		assertEquals(17, failure.getErrorCode().orElseThrow());
		assertEquals("employee_email_key", failure.getConstraint().orElseThrow());
		assertTrue(failure.isSerializationFailure());
		assertEquals(TransactionResult.IN_DOUBT, completion.get().getResult());
		assertEquals(1, attempts.get()); assertEquals(0, policies.get());
		assertEquals(List.of(1), fixture.ids());
	}

	private static PSQLException driver(ServerErrorMessage message) { return new PSQLException("driver failure", "23505", message); }
}
