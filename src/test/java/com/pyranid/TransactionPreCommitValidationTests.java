/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */

package com.pyranid;


import org.junit.jupiter.api.Test;
import org.postgresql.core.TransactionState;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class TransactionPreCommitValidationTests {
	@Test
	void readablePostgresStateOverridesSuspectAndCustomAccessWithoutAQuery() {
		var fixture =
				new TransactionCommitHardeningTests.Fixture(DatabaseType.POSTGRESQL, null, false);
		fixture.validationFailure = new SQLException("must not query", "08006");
		DatabaseException handled =
				new DatabaseException("handled", new SQLException("constraint", "23505"));
		fixture.database.transaction(
				() -> {
					fixture.insert(1);
					record(fixture, handled, true);
				});
		assertEquals(0, fixture.validations.get());
		assertEquals(1, fixture.commits.get());
		assertEquals(List.of(1), fixture.ids());
	}

	@Test
	void failedPostgresStateRefusesCommitWithoutFabricatingAnOriginalFailure() {
		var fixture =
				new TransactionCommitHardeningTests.Fixture(DatabaseType.POSTGRESQL, null, false);
		fixture.state = TransactionState.FAILED;
		AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
		DatabaseException failure =
				assertThrows(
						DatabaseException.class,
						() ->
								fixture.database.transaction(
										TransactionOptions.withReadOnly(null)
												.completionObserver(completion::set)
												.build(),
										() -> fixture.insert(1)));
		assertNull(failure.getCause());
		assertTrue(failure.getSqlState().isEmpty());
		assertEquals(TransactionResult.ROLLED_BACK, completion.get().getResult());
		assertSame(failure, completion.get().getFailure().orElseThrow());
		assertEquals(0, fixture.validations.get());
		assertEquals(0, fixture.commits.get());
		assertEquals(1, fixture.rollbacks.get());
		assertEquals(List.of(), fixture.ids());
	}

	@Test
	void suspectFailureAndAccessFlagCoalesceIntoOneSuccessfulValidation() {
		for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.DUCK_DB)) {
			var fixture = new TransactionCommitHardeningTests.Fixture(type, null, false);
			fixture.state = null;
			fixture.database.transaction(
					() -> {
						fixture.insert(1);
						record(fixture, new IllegalStateException("handled"), true);
						record(
								fixture,
								new IllegalArgumentException("another handled failure"),
								true);
					});
			assertEquals(1, fixture.validations.get());
			assertEquals(1, fixture.commits.get());
			assertEquals(List.of(1), fixture.ids());
		}
	}

	@Test
	void failedValidationCopiesRecordedDiagnosticsWithoutMutatingTheHandledFailure() {
		for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.DUCK_DB)) {
			var fixture = new TransactionCommitHardeningTests.Fixture(type, null, false);
			fixture.state = null;
			SQLException validation = new SQLException("aborted", "25P02");
			fixture.validationFailure = validation;
			DatabaseException handled =
					new DatabaseException(
							"handled constraint", new SQLException("constraint", "23505"));
			DatabaseException refusal =
					assertThrows(
							DatabaseException.class,
							() ->
									fixture.database.transaction(
											() -> {
												fixture.insert(1);
												record(fixture, handled, true);
											}));
			assertNotSame(handled, refusal);
			assertSame(handled, refusal.getCause());
			assertEquals("23505", refusal.getSqlState().orElseThrow());
			assertTrue(refusal.isUniqueConstraintViolation());
			assertTrue(List.of(refusal.getSuppressed()).contains(validation));
			assertEquals(0, handled.getSuppressed().length);
			assertEquals(1, fixture.validations.get());
			assertEquals(0, fixture.commits.get());
			assertEquals(1, fixture.rollbacks.get());
			assertEquals(List.of(), fixture.ids());
		}
	}

	@Test
	void accessTriggeredValidationFailureHasNoInventedCauseOrClassification() {
		var fixture =
				new TransactionCommitHardeningTests.Fixture(DatabaseType.DUCK_DB, null, false);
		SQLException validation = new SQLException("validation transport", "08006");
		fixture.validationFailure = validation;
		DatabaseException refusal =
				assertThrows(
						DatabaseException.class,
						() ->
								fixture.database.transaction(
										() -> {
											fixture.insert(1);
											record(fixture, null, true);
										}));
		assertNull(refusal.getCause());
		assertTrue(refusal.getSqlState().isEmpty());
		assertFalse(refusal.isSerializationFailure());
		assertTrue(List.of(refusal.getSuppressed()).contains(validation));
		assertEquals(1, fixture.validations.get());
		assertEquals(0, fixture.commits.get());
		assertEquals(List.of(), fixture.ids());
	}

	@Test
	void runtimeCauseRetainsClassifierFlagsWithoutInventingSqlOrVendorMetadata() {
		var fixture =
				new TransactionCommitHardeningTests.Fixture(DatabaseType.POSTGRESQL, null, false);
		fixture.state = null;
		fixture.validationFailure = new SQLException("aborted", "25P02");
		RuntimeException handled =
				new IllegalStateException(
						"mapper failure", new SQLException("serialization", "40001", 17));
		DatabaseException refusal =
				assertThrows(
						DatabaseException.class,
						() ->
								fixture.database.transaction(
										() -> {
											fixture.insert(1);
											record(fixture, handled, false);
										}));
		assertSame(handled, refusal.getCause());
		assertTrue(refusal.getSqlState().isEmpty());
		assertTrue(refusal.getErrorCode().isEmpty());
		assertTrue(refusal.isSerializationFailure());
		assertEquals(0, handled.getSuppressed().length);
		assertEquals(0, fixture.commits.get());
		assertEquals(List.of(), fixture.ids());
	}

	@Test
	void refusalDiagnosticFailuresAreAttachedToTheRefusalRatherThanTheHandledRuntimeFailure() {
		var fixture =
				new TransactionCommitHardeningTests.Fixture(DatabaseType.POSTGRESQL, null, false);
		fixture.state = null;
		fixture.validationFailure = new SQLException("aborted", "25P02");
		IllegalStateException diagnostic = new IllegalStateException("unreadable SQLState");
		SQLException hostile =
				new SQLException("driver failure", "40001") {
					@Override
					public String getSQLState() {
						throw diagnostic;
					}
				};
		RuntimeException handled = new IllegalArgumentException("mapper failure", hostile);
		DatabaseException refusal =
				assertThrows(
						DatabaseException.class,
						() ->
								fixture.database.transaction(
										() -> {
											fixture.insert(1);
											record(fixture, handled, false);
										}));
		assertSame(handled, refusal.getCause());
		assertTrue(List.of(refusal.getSuppressed()).contains(diagnostic));
		assertEquals(0, handled.getSuppressed().length);
		assertEquals(0, hostile.getSuppressed().length);
		assertEquals(0, fixture.commits.get());
		assertEquals(List.of(), fixture.ids());
	}

	@Test
	void successfulSavepointRollbackClearsSuspicionButPreservesTheAccessTrigger() {
		for (boolean sticky : new boolean[] {false, true}) {
			var fixture =
					new TransactionCommitHardeningTests.Fixture(
							DatabaseType.POSTGRESQL, null, false);
			fixture.state = null;
			RuntimeException body = new IllegalStateException("savepoint body");
			fixture.database.transaction(
					() -> {
						fixture.insert(1);
						assertSame(
								body,
								assertThrows(
										IllegalStateException.class,
										() ->
												fixture.database
														.currentTransaction()
														.orElseThrow()
														.withSavepoint(
																() -> {
																	record(
																			fixture,
																			new DatabaseException(
																					"handled"
																						+ " statement"),
																			sticky);
																	throw body;
																})));
						fixture.insert(2);
					});
			assertEquals(sticky ? 1 : 0, fixture.validations.get());
			assertEquals(1, fixture.commits.get());
			assertEquals(List.of(1, 2), fixture.ids());
		}
	}

	@Test
	void suppressedSavepointReleaseFailureIsRecordedAfterRollbackClearedTheBodyFailure() {
		var fixture =
				new TransactionCommitHardeningTests.Fixture(DatabaseType.POSTGRESQL, null, false);
		fixture.state = null;
		fixture.releaseFailure = new SQLException("release failed", "HY000");
		RuntimeException body = new IllegalStateException("savepoint body");
		fixture.database.transaction(
				() -> {
					fixture.insert(1);
					assertSame(
							body,
							assertThrows(
									IllegalStateException.class,
									() ->
											fixture.database
													.currentTransaction()
													.orElseThrow()
													.withSavepoint(
															() -> {
																throw body;
															})));
				});
		assertEquals(1, body.getSuppressed().length);
		assertSame(fixture.releaseFailure, body.getSuppressed()[0].getCause());
		assertEquals(1, fixture.validations.get());
		assertEquals(1, fixture.commits.get());
		assertEquals(List.of(1), fixture.ids());
	}

	@Test
	void aFinalValidationErrorEscapesUnchangedAfterRollback() {
		var fixture =
				new TransactionCommitHardeningTests.Fixture(DatabaseType.DUCK_DB, null, false);
		AssertionError failure = new AssertionError("validation failed");
		fixture.validationFailure = failure;
		assertSame(
				failure,
				assertThrows(
						AssertionError.class,
						() ->
								fixture.database.transaction(
										() -> {
											fixture.insert(1);
											record(fixture, null, true);
										})));
		assertEquals(0, fixture.commits.get());
		assertEquals(1, fixture.rollbacks.get());
		assertEquals(List.of(), fixture.ids());
	}

	@Test
	void everyInvalidResultAndValidationResourceFailureRefusesCommit() {
		for (String phase :
				List.of(
						"empty",
						"value",
						"null",
						"extra-row",
						"fetch",
						"result-close",
						"statement-close")) {
			var fixture =
					new TransactionCommitHardeningTests.Fixture(DatabaseType.DUCK_DB, null, false);
			SQLException validation = new SQLException("validation " + phase, "HY000");
			switch (phase) {
				case "empty" -> fixture.validationEmpty = true;
				case "value" -> fixture.validationValue = 2;
				case "null" -> fixture.validationNull = true;
				case "extra-row" -> fixture.validationExtraRow = true;
				case "fetch" -> fixture.validationFetchFailure = validation;
				case "result-close" -> fixture.validationResultCloseFailure = validation;
				case "statement-close" -> fixture.validationStatementCloseFailure = validation;
				default -> throw new AssertionError(phase);
			}
			DatabaseException handled =
					new DatabaseException(
							"handled statement", new SQLException("serialization", "40001"));
			TransactionOutcome<String> outcome =
					fixture.database.transactionWithOutcome(
							() -> {
								fixture.insert(1);
								record(fixture, handled, false);
								return Optional.of("attempted");
							});
			assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult(), phase);
			assertEquals(Optional.of("attempted"), outcome.getAttemptedValue());
			DatabaseException refusal = (DatabaseException) outcome.getFailure().orElseThrow();
			assertSame(handled, refusal.getCause());
			assertTrue(refusal.getMessage().contains("validation"));
			if (List.of("fetch", "result-close", "statement-close").contains(phase))
				assertTrue(List.of(refusal.getSuppressed()).contains(validation));
			else assertInstanceOf(SQLException.class, refusal.getSuppressed()[0]);
			assertEquals(0, handled.getSuppressed().length);
			assertEquals(1, fixture.validations.get());
			assertEquals(1, fixture.validationResultCloses.get());
			assertEquals(1, fixture.validationStatementCloses.get());
			assertEquals(0, fixture.commits.get());
			assertEquals(1, fixture.rollbacks.get());
			assertEquals(List.of(), fixture.ids());
		}
	}

	@Test
	void validationAndCleanupFailuresAllRemainOnTheRefusalWithoutChangingTheRecordedFailure() {
		var fixture =
				new TransactionCommitHardeningTests.Fixture(DatabaseType.DUCK_DB, null, false);
		SQLException execution = new SQLException("fetch failed", "HY000");
		SQLException resultClose = new SQLException("result close failed", "HY000");
		SQLException statementClose = new SQLException("statement close failed", "HY000");
		fixture.validationFetchFailure = execution;
		fixture.validationResultCloseFailure = resultClose;
		fixture.validationStatementCloseFailure = statementClose;
		IllegalStateException handled = new IllegalStateException("handled mapper failure");
		DatabaseException refusal =
				assertThrows(
						DatabaseException.class,
						() ->
								fixture.database.transaction(
										() -> {
											fixture.insert(1);
											record(fixture, handled, false);
										}));
		assertSame(handled, refusal.getCause());
		assertEquals(
				List.of(execution, resultClose, statementClose), List.of(refusal.getSuppressed()));
		assertEquals(0, handled.getSuppressed().length);
		assertEquals(0, execution.getSuppressed().length);
		assertEquals(1, fixture.validationResultCloses.get());
		assertEquals(1, fixture.validationStatementCloses.get());
		assertEquals(0, fixture.commits.get());
		assertEquals(List.of(), fixture.ids());
	}

	@Test
	void normallyReturnedCallbacksCanRetryAnInheritedSerializationRefusalAfterAcknowledgedRollback() {
		for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.DUCK_DB)) {
			var fixture = new TransactionCommitHardeningTests.Fixture(type, null, false);
			SQLException driverFailure = new SQLException("handled serialization failure", "40001");
			DatabaseException handled = new DatabaseException("handled", driverFailure, type.dialect());
			SQLException validationFailure = new SQLException("transaction aborted", "25P02");
			AtomicInteger attempts = new AtomicInteger(), normalReturns = new AtomicInteger(), policies = new AtomicInteger();
			AtomicReference<DatabaseException> observedRefusal = new AtomicReference<>();
			List<TransactionCompletion> completions = new ArrayList<>();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				policies.incrementAndGet();
				observedRefusal.set(failure);
				assertEquals(1, normalReturns.get(), "The first callback returned before completion refused it");
				assertNotSame(handled, failure);
				assertSame(handled, failure.getCause());
				assertEquals(Optional.of("40001"), failure.getSqlState());
				assertTrue(failure.isSerializationFailure());
				assertFalse(failure.isTransactionOutcomeIndeterminate());
				assertFalse(failure.isTransactionOutcomeCommitted());
				assertEquals(1, fixture.rollbacks.get());
				assertEquals(0, fixture.commits.get());
				assertTrue(fixture.ids().isEmpty(), "The refused attempt must be rolled back before policy admission");
				assertEquals(1, completions.size());
				assertEquals(TransactionResult.ROLLED_BACK, completions.get(0).getResult());
				return failure.isSerializationFailure();
			});
			TransactionRetryResult<String> result = fixture.database.transactionWithRetry(policy,
					TransactionOptions.withReadOnly(null).completionObserver(completions::add).build(), () -> {
						int attempt = attempts.incrementAndGet();
						fixture.state = type == DatabaseType.POSTGRESQL ? TransactionState.OPEN : null;
						fixture.validationFailure = null;
						fixture.insert(attempt);
						if (attempt == 1) {
							if (type == DatabaseType.POSTGRESQL) fixture.state = TransactionState.FAILED;
							else fixture.validationFailure = validationFailure;
							record(fixture, handled, true);
						}
						normalReturns.incrementAndGet();
						return Optional.of("receipt-" + attempt);
					});
			assertEquals(2, attempts.get());
			assertEquals(2, normalReturns.get());
			assertEquals(1, policies.get());
			assertEquals(2, result.getAttemptCount());
			assertTrue(result.wasRetried());
			assertEquals(Optional.of("receipt-2"), result.getValue());
			assertEquals(List.of(observedRefusal.get()), result.getFailures());
			assertEquals(List.of(TransactionResult.ROLLED_BACK, TransactionResult.COMMITTED),
					completions.stream().map(TransactionCompletion::getResult).toList());
			assertSame(observedRefusal.get(), completions.get(0).getFailure().orElseThrow());
			assertTrue(completions.get(1).getFailure().isEmpty());
			assertEquals(type == DatabaseType.POSTGRESQL ? 0 : 1, fixture.validations.get());
			if (type == DatabaseType.DUCK_DB)
				assertTrue(List.of(observedRefusal.get().getSuppressed()).contains(validationFailure));
			assertSame(driverFailure, handled.getCause());
			assertEquals(0, handled.getSuppressed().length);
			assertEquals(0, driverFailure.getSuppressed().length);
			assertNull(driverFailure.getNextException());
			assertEquals(1, fixture.commits.get());
			assertEquals(1, fixture.rollbacks.get());
			assertEquals(List.of(2), fixture.ids());
		}
	}

	@Test
	void failedRollbackAfterValidationRefusalIsInDoubtAndCannotReachTheRetryPolicy() {
		for (boolean outcomeStyle : new boolean[] {false, true}) {
			var fixture =
					new TransactionCommitHardeningTests.Fixture(DatabaseType.DUCK_DB, null, false);
			fixture.validationFailure = new SQLException("transaction aborted", "25P02");
			fixture.rollbackFailure = new SQLException("rollback connection lost", "08006");
			AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger();
			ReturningTransactionalOperation<String> work =
					() -> {
						attempts.incrementAndGet();
						fixture.insert(1);
						record(fixture, null, true);
						return Optional.of("attempted");
					};
			DatabaseException failure;
			if (outcomeStyle) {
				TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(work);
				assertEquals(TransactionResult.IN_DOUBT, outcome.getResult());
				assertEquals(Optional.of("attempted"), outcome.getAttemptedValue());
				failure = (DatabaseException) outcome.getFailure().orElseThrow();
			} else {
				RetryPolicy policy =
						RetryPolicy.ofMaxAttempts(
								2,
								RetryPolicy.Backoff.fixed(Duration.ZERO),
								ignored -> {
									policies.incrementAndGet();
									return true;
								});
				failure =
						assertThrows(
								DatabaseException.class,
								() -> fixture.database.transactionWithRetry(policy, work));
			}
			assertTrue(failure.isTransactionOutcomeIndeterminate());
			assertTrue(
					ThrowableGraph.inspect(
									failure,
									current -> current == fixture.rollbackFailure ? 1 : 0,
									0)
							!= 0);
			assertEquals(1, attempts.get());
			assertEquals(0, policies.get());
			assertEquals(1, fixture.validations.get());
			assertEquals(0, fixture.commits.get());
			assertEquals(1, fixture.rollbacks.get());
			assertEquals(List.of(), fixture.ids());
		}
	}

	@Test
	void untriggeredAndUnaffectedTransactionsDoNotRunTheValidityQuery() {
		for (DatabaseType type :
				List.of(DatabaseType.DUCK_DB, DatabaseType.POSTGRESQL, DatabaseType.GENERIC)) {
			var fixture = new TransactionCommitHardeningTests.Fixture(type, null, false);
			fixture.state = null;
			fixture.database.transaction(() -> fixture.insert(1));
			assertEquals(0, fixture.validations.get());
			assertEquals(List.of(1), fixture.ids());
		}
	}

	private static void record(
			TransactionCommitHardeningTests.Fixture fixture, Throwable failure, boolean required) {
		Transaction transaction = fixture.database.currentTransaction().orElseThrow();
		transaction.getConnectionLock().lock();
		try {
			if (failure != null) transaction.recordSuspectFailure(failure);
			if (required) transaction.requireCompletionValidation();
		} finally {
			transaction.getConnectionLock().unlock();
		}
	}
}
