/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbCommitClassificationTests {
	private static final String COMMIT_DUPLICATE =
			"TransactionContext Error: Failed to commit: PRIMARY KEY or UNIQUE constraint violation: duplicate key";

	@Test
	void commitDuplicatesAreUniqueViolationsWithoutBecomingSerializationFailures() {
		for (String edge : List.of("primary", "cause", "next", "cycle")) {
			SQLException duplicate = new SQLException(COMMIT_DUPLICATE);
			Throwable root;
			if (edge.equals("primary")) root = duplicate;
			else if (edge.equals("cause")) root = new IllegalStateException("operation wrapper", duplicate);
			else {
				SQLException wrapper = new SQLException("batch wrapper");
				wrapper.setNextException(duplicate);
				if (edge.equals("cycle")) duplicate.setNextException(wrapper);
				root = wrapper;
			}
			DatabaseException failure = new DatabaseException("Unable to commit", root, DuckDbDialect.INSTANCE);
			assertTrue(failure.isUniqueConstraintViolation(), edge);
			assertFalse(failure.isSerializationFailure(), edge);
			assertFalse(failure.isTransient(), edge);
			assertFalse(failure.isForeignKeyViolation(), edge);
			assertFalse(failure.isDeadlock(), edge);
			assertFalse(failure.isTimeout(), edge);
		}
	}

	@Test
	void matchingRemainsSpecificAndExistingConflictDiagnosticsRemainIndependent() {
		for (String message : List.of(
				"PRIMARY KEY or UNIQUE constraint violation",
				"Failed to commit: PRIMARY KEY or UNIQUE constraint violation",
				"TransactionContext Error: Failed to commit: CHECK constraint violation",
				"TransactionContext Error: Failed to commit: FOREIGN KEY constraint violation",
				"TransactionContext Error: another transaction failed a unique operation")) {
			assertFalse(new DatabaseException("operation failed", new SQLException(message), DuckDbDialect.INSTANCE)
					.isUniqueConstraintViolation(), message);
		}
		assertTrue(new DatabaseException("operation failed", new SQLException(COMMIT_DUPLICATE.toUpperCase(java.util.Locale.ROOT)),
				DuckDbDialect.INSTANCE).isUniqueConstraintViolation());
		assertTrue(new DatabaseException("operation failed", new SQLException("Constraint Error: Duplicate key violates primary key constraint"),
				DuckDbDialect.INSTANCE).isUniqueConstraintViolation());
		DatabaseException conflict = new DatabaseException("operation failed", new SQLException(
				"TransactionContext Error: Conflict on update!"), DuckDbDialect.INSTANCE);
		assertFalse(conflict.isUniqueConstraintViolation());
		assertTrue(conflict.isSerializationFailure());
		assertTrue(conflict.isTransient());
		DatabaseException duplicate = new DatabaseException("operation failed", new SQLException(
				COMMIT_DUPLICATE + ": key contains conflict"), DuckDbDialect.INSTANCE);
		assertTrue(duplicate.isUniqueConstraintViolation());
		assertFalse(duplicate.isSerializationFailure());
		assertFalse(duplicate.isTransient());
		SQLException separateConflict = new SQLException(COMMIT_DUPLICATE);
		separateConflict.setNextException(new SQLException("TransactionContext Error: Conflict on update!"));
		assertTrue(new DatabaseException("operation failed", separateConflict, DuckDbDialect.INSTANCE).isSerializationFailure());
	}

	@Test
	void uniqueCommitDiagnosticsCannotAuthorizeReplayEvenIfAcknowledgmentWasMisleading() {
		for (boolean actuallyCommitted : new boolean[]{false, true}) {
			SQLException driver = new SQLException(COMMIT_DUPLICATE);
			var fixture = new TransactionCommitHardeningTests.Fixture(DatabaseType.DUCK_DB, driver, actuallyCommitted);
			AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger();
			AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				policies.incrementAndGet();
				return failure.isUniqueConstraintViolation();
			});
			DatabaseException failure = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(policy,
					TransactionOptions.withReadOnly(null).completionObserver(completion::set).build(), () -> {
						attempts.incrementAndGet();
						fixture.insert(1);
					}));
			assertSame(driver, failure.getCause());
			assertTrue(failure.isUniqueConstraintViolation());
			assertFalse(failure.isSerializationFailure());
			assertEquals(TransactionResult.IN_DOUBT, completion.get().getResult());
			assertSame(failure, completion.get().getFailure().orElseThrow());
			assertEquals(1, attempts.get());
			assertEquals(0, policies.get());
			assertEquals(actuallyCommitted ? List.of(1) : List.of(), fixture.ids());
		}
	}

	@Test
	void outcomeRetainsValueAndUniqueFailureWithoutClaimingAProvenCommitAbort() {
		var fixture = new TransactionCommitHardeningTests.Fixture(DatabaseType.DUCK_DB, new SQLException(COMMIT_DUPLICATE), false);
		TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(1);
			return Optional.of("attempted receipt");
		});
		assertEquals(TransactionResult.IN_DOUBT, outcome.getResult());
		assertEquals(Optional.of("attempted receipt"), outcome.getAttemptedValue());
		assertTrue(((DatabaseException) outcome.getFailure().orElseThrow()).isUniqueConstraintViolation());
		assertFalse(TransactionCommitEvidence.provesSerializationAbort(new SQLException(COMMIT_DUPLICATE), DuckDbDialect.INSTANCE));
	}
}
