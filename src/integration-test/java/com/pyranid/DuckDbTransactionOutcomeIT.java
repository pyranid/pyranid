/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbTransactionOutcomeIT {
	@TempDir Path directory;

	@Test
	void swallowedConstraintFailuresAreRefusedByBothTransactionApis() {
		for (boolean outcome : new boolean[]{false, true}) fixture(null).assertHandledStatementAbort(outcome);
	}

	@Test
	void rawAndLastRowMapperHandledAbortsAreDetectedWithoutEscapedFailures() {
		for (boolean escapes : new boolean[]{false, true}) fixture(null).assertRawHandledAbort(escapes);
		fixture(null).assertLastRowMapperHandledAbort();
	}

	@Test
	void preparationFailuresAreValidatedAccordingToTheDriversInvalidationPolicy() {
		for (boolean legacy : new boolean[]{false, true}) {
			var fixture = fixture(legacy ? "SYNTACTIC_ERRORS_DO_NOT_INVALIDATE" : null);
			AtomicReference<DatabaseException> handled = new AtomicReference<>();
			TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(fixture.options, () -> {
				fixture.insert(2);
				handled.set(assertThrows(DatabaseException.class,
						() -> fixture.database.query("INSERT INTO missing_outcome_table VALUES (1)").execute()));
				return Optional.of("receipt");
			});
			assertEquals(legacy ? TransactionResult.COMMITTED : TransactionResult.ROLLED_BACK, outcome.getResult());
			assertEquals(Optional.of("receipt"), outcome.getAttemptedValue());
			assertEquals(legacy, outcome.getFailure().isEmpty());
			if (!legacy) assertSame(handled.get(), outcome.getFailure().orElseThrow().getCause());
			assertEquals(legacy ? List.of(1, 2) : List.of(1), fixture.ids());
			assertEquals(1, fixture.validations.get());
			assertEquals(legacy ? 1 : 0, fixture.commits.get());
		}
	}

	@Test
	void aLocalExecuteFailureOnAUsableTransactionDoesNotForceRollback() {
		var fixture = fixture(null);
		TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(2);
			assertThrows(DatabaseException.class, () -> fixture.database.useRawConnection(connection -> {
				try (PreparedStatement statement = connection.prepareStatement("INSERT INTO " + fixture.table + " VALUES (?)")) {
					statement.executeUpdate();
				}
				return Optional.empty();
			}));
			return Optional.of("receipt");
		});
		assertEquals(TransactionResult.COMMITTED, outcome.getResult());
		assertTrue(outcome.getFailure().isEmpty());
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(1, fixture.validations.get());
	}

	@Test
	void healthyBuiltInWorkHasNoProbeButRepeatedRawAccessCoalescesIntoOneProbe() {
		for (boolean raw : new boolean[]{false, true}) {
			var fixture = fixture(null);
			TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
				fixture.insert(2);
				if (raw) for (int index = 0; index < 3; index++) fixture.database.useRawConnection(connection -> Optional.empty());
			});
			assertEquals(TransactionResult.COMMITTED, outcome.getResult());
			assertTrue(outcome.getFailure().isEmpty());
			assertEquals(raw ? 1 : 0, fixture.validations.get());
			assertEquals(List.of(1, 2), fixture.ids());
		}
	}

	private TransactionValidationIntegrationSupport.Fixture fixture(String policy) {
		DataSource source = new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
				"jdbc:duckdb:" + this.directory.resolve("outcome_" + UUID.randomUUID() + ".duckdb"), null, null);
		return new TransactionValidationIntegrationSupport.Fixture(source, DatabaseType.DUCK_DB, false, policy);
	}
}
