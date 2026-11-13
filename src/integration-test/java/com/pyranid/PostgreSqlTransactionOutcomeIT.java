/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.jdbc.AutoSave;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlTransactionOutcomeIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_outcomes").withUsername("pyranid").withPassword("pyranid");

	@Test
	void swallowedConstraintFailuresAreRefusedByBothTransactionApis() {
		for (boolean outcome : new boolean[]{false, true}) fixture().assertHandledStatementAbort(outcome);
	}

	@Test
	void rawAndLastRowMapperHandledAbortsAreDetectedWithoutEscapedFailures() {
		for (boolean escapes : new boolean[]{false, true}) fixture().assertRawHandledAbort(escapes);
		fixture().assertLastRowMapperHandledAbort();
	}

	@Test
	void successfulSavepointRecoveryCommitsSurvivingRows() {
		for (boolean manual : new boolean[]{false, true}) {
			var fixture = fixture();
			TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(fixture.options, () -> {
				fixture.insert(2);
				Transaction transaction = fixture.database.currentTransaction().orElseThrow();
				if (manual) {
					java.sql.Savepoint savepoint = transaction.createSavepoint();
					assertThrows(DatabaseException.class, () -> fixture.insert(1));
					transaction.rollback(savepoint);
					transaction.releaseSavepoint(savepoint);
				} else assertThrows(DatabaseException.class, () -> transaction.withSavepoint(() -> {
					fixture.insert(3); fixture.insert(1);
				}));
				fixture.insert(4);
				return Optional.of("surviving receipt");
			});
			assertEquals(TransactionResult.COMMITTED, outcome.getResult());
			assertTrue(outcome.getFailure().isEmpty());
			assertEquals(List.of(1, 2, 4), fixture.ids());
			assertEquals(1, fixture.commits.get());
			assertEquals(0, fixture.validations.get());
		}
	}

	@Test
	void autosaveAlwaysAllowsARecoveredStatementFailureToCommit() {
		PGSimpleDataSource source = source();
		source.setAutosave(AutoSave.ALWAYS);
		var fixture = new TransactionValidationIntegrationSupport.Fixture(source, DatabaseType.POSTGRESQL);
		TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(2);
			assertThrows(DatabaseException.class, () -> fixture.insert(1));
			return Optional.of("receipt");
		});
		assertEquals(TransactionResult.COMMITTED, outcome.getResult());
		assertTrue(outcome.getFailure().isEmpty());
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(0, fixture.validations.get());
	}

	@Test
	void unavailableDriverStateUsesOneConditionalValidationForRawHandledAbort() {
		var fixture = new TransactionValidationIntegrationSupport.Fixture(source(), DatabaseType.POSTGRESQL, true, null);
		fixture.assertRawHandledAbort(false);
		assertEquals(1, fixture.validations.get());
	}

	@Test
	void aSwallowedSerializationAbortRetriesTheWholeBoundaryWithoutIssuingItsCommit() {
		var fixture = fixture();
		AtomicInteger attempts = new AtomicInteger();
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), DatabaseException::isSerializationFailure);
		TransactionRetryResult<String> result = fixture.database.transactionWithRetry(policy, fixture.options, () -> {
			fixture.insert(2);
			if (attempts.incrementAndGet() == 1)
				assertThrows(DatabaseException.class, () -> fixture.database.query(
						"DO $$ BEGIN RAISE EXCEPTION 'forced serialization abort' USING ERRCODE = '40001'; END $$").execute());
			return Optional.of("receipt");
		});
		assertEquals(2, result.getAttemptCount());
		assertEquals(2, attempts.get());
		assertEquals(1, fixture.commits.get());
		assertEquals(1, fixture.rollbacks.get());
		assertEquals(List.of(1, 2), fixture.ids());
	}

	private static TransactionValidationIntegrationSupport.Fixture fixture() {
		return new TransactionValidationIntegrationSupport.Fixture(source(), DatabaseType.POSTGRESQL);
	}
	private static PGSimpleDataSource source() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return source;
	}
}
