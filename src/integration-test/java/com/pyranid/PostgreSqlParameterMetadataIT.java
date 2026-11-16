/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.jdbc.AutoSave;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlParameterMetadataIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_metadata").withUsername("pyranid").withPassword("pyranid");
	private static final Instant NOW = Instant.parse("2024-06-15T12:34:56.123456Z");

	@Test
	void validTemporalArithmeticWorksBeforeAndAfterEarlierWorkWithDriverAutosaveModes() {
		for (AutoSave autosave : List.of(AutoSave.NEVER, AutoSave.ALWAYS, AutoSave.CONSERVATIVE)) {
			for (boolean precedingWrite : new boolean[]{false, true}) {
				Fixture fixture = new Fixture(autosave, ZoneId.of("UTC"));
				TransactionOutcome<Long> outcome = fixture.database.transactionWithOutcome(() -> {
					if (precedingWrite) fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (2)").execute();
					return Optional.of(fixture.database.query("UPDATE " + fixture.table
							+ " SET zoned = :now WHERE id = 1 AND last_login < :cutoff - interval '30 days'")
							.bind("now", NOW).bind("cutoff", NOW).execute());
				});
				assertEquals(TransactionResult.COMMITTED, outcome.getResult(), autosave + " preceding=" + precedingWrite);
				assertTrue(outcome.getFailure().isEmpty());
				assertEquals(Optional.of(1L), outcome.getAttemptedValue());
				assertEquals(NOW, fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id = 1")
						.fetchObject(Instant.class).orElseThrow());
				assertEquals(precedingWrite ? List.of(1, 2) : List.of(1), fixture.ids());
				assertEquals(1, fixture.descriptions.get());
				assertEquals(1, fixture.savepoints.get());
				assertEquals(1, fixture.rollbacks.get());
				assertEquals(1, fixture.releases.get());
			}
		}
	}

	@Test
	void timestampInferenceStillUsesTheConfiguredZoneAndPreservesTypedNulls() {
		ZoneId zone = ZoneId.of("America/Los_Angeles");
		Fixture fixture = new Fixture(AutoSave.NEVER, zone);
		fixture.database.transaction(() -> {
			fixture.database.query("UPDATE " + fixture.table + " SET last_login = last_login WHERE id = 1").execute();
			fixture.database.query("INSERT INTO " + fixture.table + " (id, plain, zoned, nullable_integer) VALUES (:id, :plain, :zoned, :nil)")
					.bind("id", 2).bind("plain", NOW).bind("zoned", NOW).bind("nil", Optional.empty()).execute();
		});
		assertEquals(LocalDateTime.ofInstant(NOW, zone), fixture.inspection.query("SELECT plain FROM " + fixture.table + " WHERE id = 2")
				.fetchObject(LocalDateTime.class).orElseThrow());
		assertEquals(NOW, fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id = 2")
				.fetchObject(Instant.class).orElseThrow());
		assertTrue(fixture.inspection.query("SELECT nullable_integer IS NULL FROM " + fixture.table + " WHERE id = 2")
				.fetchObject(Boolean.class).orElseThrow());
		assertEquals(1, fixture.descriptions.get());
		assertEquals(1, fixture.savepoints.get());
		assertEquals(0, fixture.rollbacks.get());
		assertEquals(1, fixture.releases.get());
	}

	@Test
	void metadataAndProtectionCallsAreSharedAcrossRequiredParametersBatchRowsAndChunks() {
		for (Integer chunkSize : new Integer[]{null, 4}) {
			Fixture fixture = new Fixture(AutoSave.NEVER, ZoneId.of("UTC"));
			List<Map<String, Object>> rows = new ArrayList<>();
			for (int index = 2; index <= 26; index++)
				rows.add(Map.of("id", index, "plain", Optional.empty(), "zoned", NOW));
			fixture.database.transaction(() -> {
				Query query = fixture.database.query("INSERT INTO " + fixture.table + " (id, plain, zoned) VALUES (:id, :plain, :zoned)");
				if (chunkSize != null) query.batchChunkSize(chunkSize);
				assertEquals(25, query.executeBatch(rows).size());
			});
			assertEquals(26, fixture.ids().size());
			assertEquals(1, fixture.descriptions.get());
			assertEquals(1, fixture.savepoints.get());
			assertEquals(0, fixture.rollbacks.get());
			assertEquals(1, fixture.releases.get());
		}
	}

	@Test
	void aRecoveredApplicationSavepointDoesNotRetainAnInternalMetadataRollbackFence() {
		for (boolean useOutcome : new boolean[]{false, true}) {
			for (boolean metadataParameter : new boolean[]{false, true}) {
				for (String failureState : List.of("23505", "P0001", "21000", "U0001", "57014", "40001")) {
					Fixture fixture = new Fixture(AutoSave.NEVER, ZoneId.of("UTC"));
					List<TransactionCompletion> completions = new ArrayList<>();
					TransactionOptions options = TransactionOptions.withIsolation(TransactionIsolation.DEFAULT)
							.completionObserver(completions::add).build();
					ReturningTransactionalOperation<String> operation = () -> {
						Transaction transaction = fixture.database.currentTransaction().orElseThrow();
						fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (3)").execute();
						DatabaseException failedScope = assertThrows(DatabaseException.class, () -> transaction.withSavepoint(() -> {
							DatabaseException initialFailure = assertThrows(DatabaseException.class, () -> forceTransactionFailure(fixture, failureState));
							assertEquals(Optional.of(failureState), initialFailure.getSqlState());
							if (metadataParameter) {
								fixture.database.query("INSERT INTO " + fixture.table + " (id, zoned) VALUES (4, :at)")
										.bind("at", NOW).execute();
							} else fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (4)").execute();
						}));
						assertEquals(Optional.of("25P02"), failedScope.getSqlState());
						assertFalse(transaction.isRollbackOnly(), () -> "Acknowledged application recovery must restore the enclosing transaction: "
								+ failureShape(failedScope));
						fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (5)").execute();
						return Optional.of("recovered receipt");
					};
					if (useOutcome) {
						TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(options, operation);
						assertEquals(TransactionResult.COMMITTED, outcome.getResult());
						assertEquals(Optional.of("recovered receipt"), outcome.getAttemptedValue());
						assertTrue(outcome.getFailure().isEmpty());
						assertEquals(1, completions.size());
						assertSame(completions.get(0), outcome.getCompletion());
					} else assertEquals(Optional.of("recovered receipt"), fixture.database.transaction(options, operation));
					assertEquals(1, completions.size());
					assertEquals(TransactionResult.COMMITTED, completions.get(0).getResult());
					assertTrue(completions.get(0).getFailure().isEmpty());
					assertEquals(List.of(1, 3, 5), fixture.ids());
				}
			}
		}
	}

	@Test
	void anUnrecoveredMetadataProtectionFailureStillRefusesCommit() {
		for (boolean useOutcome : new boolean[]{false, true}) {
			for (boolean metadataParameter : new boolean[]{false, true}) {
				for (String failureState : List.of("23505", "P0001", "21000", "U0001", "57014", "40001")) {
					Fixture fixture = new Fixture(AutoSave.NEVER, ZoneId.of("UTC"));
					List<TransactionCompletion> completions = new ArrayList<>();
					TransactionOptions options = TransactionOptions.withIsolation(TransactionIsolation.DEFAULT)
							.completionObserver(completions::add).build();
					ReturningTransactionalOperation<String> operation = () -> {
						fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (2)").execute();
						DatabaseException initial = assertThrows(DatabaseException.class, () -> forceTransactionFailure(fixture, failureState));
						assertEquals(Optional.of(failureState), initial.getSqlState());
						DatabaseException failedBind = assertThrows(DatabaseException.class, () -> {
							if (metadataParameter) fixture.database.query("INSERT INTO " + fixture.table + " (id, zoned) VALUES (3, :at)").bind("at", NOW).execute();
							else fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (3)").execute();
						});
						assertEquals(Optional.of("25P02"), failedBind.getSqlState());
						return Optional.of("uncommitted receipt");
					};
					if (useOutcome) {
						TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(options, operation);
						assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
						assertEquals(Optional.of("uncommitted receipt"), outcome.getAttemptedValue());
						assertTrue(outcome.getFailure().isPresent());
						assertEquals(1, completions.size());
						assertSame(completions.get(0), outcome.getCompletion());
					} else assertThrows(DatabaseException.class, () -> fixture.database.transaction(options, operation));
					assertEquals(1, completions.size());
					assertEquals(TransactionResult.ROLLED_BACK, completions.get(0).getResult());
					assertTrue(completions.get(0).getFailure().isPresent());
					assertEquals(List.of(1), fixture.ids());
				}
			}
		}
	}

	@Test
	void fiveMetadataDemandingParametersShareOneProtectedSnapshotWithoutLosingTemporalSemantics() {
		ZoneId zone = ZoneId.of("America/Los_Angeles");
		Fixture fixture = new Fixture(AutoSave.NEVER, zone);
		fixture.inspection.query("ALTER TABLE " + fixture.table + " ADD nullable_text TEXT").execute();
		LocalDate day = LocalDate.of(2024, 3, 10);
		LocalDateTime plain = LocalDateTime.of(2024, 6, 15, 1, 2, 3, 123_456_000);
		fixture.requireProtectedMetadata = true;
		fixture.database.transaction(() -> fixture.database.query("INSERT INTO " + fixture.table
				+ " (id, zoned, plain, last_login, nullable_integer, nullable_text) VALUES (:id, :instant, :local, :day, :nil, :text)")
				.bind("id", 2).bind("instant", NOW).bind("local", plain).bind("day", day)
				.bind("nil", Optional.empty()).bind("text", Optional.empty()).execute());
		assertEquals(NOW, fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id=2").fetchObject(Instant.class).orElseThrow());
		assertEquals(plain, fixture.inspection.query("SELECT plain FROM " + fixture.table + " WHERE id=2").fetchObject(LocalDateTime.class).orElseThrow());
		assertEquals(day.atStartOfDay(zone).toInstant(), fixture.inspection.query("SELECT last_login FROM " + fixture.table + " WHERE id=2")
				.fetchObject(Instant.class).orElseThrow());
		assertTrue(fixture.inspection.query("SELECT nullable_integer IS NULL AND nullable_text IS NULL FROM " + fixture.table + " WHERE id=2")
				.fetchObject(Boolean.class).orElseThrow());
		assertEquals(1, fixture.descriptions.get());
		assertEquals(1, fixture.savepoints.get());
		assertEquals(1, fixture.releases.get());
		assertEquals(0, fixture.rollbacks.get());
		assertEquals(5, fixture.typeReads.get());
		assertEquals(5, fixture.nameReads.get());
	}

	@Test
	void metadataDemandChangingAcrossBatchGroupsAndChunksUsesTheSameProtectedSnapshot() {
		for (Integer chunkSize : new Integer[]{null, 1, 2}) {
			ZoneId zone = ZoneId.of("America/Los_Angeles");
			Fixture fixture = new Fixture(AutoSave.NEVER, zone);
			LocalDateTime local = LocalDateTime.of(2024, 6, 15, 1, 2, 3);
			List<Map<String, Object>> groups = List.of(
					Map.of("id", 2, "plain", "2024-06-15T01:02:03", "zoned", "2024-06-15T01:02:03Z", "nil", 1),
					Map.of("id", 3, "plain", NOW, "zoned", local, "nil", 2),
					Map.of("id", 4, "plain", Optional.empty(), "zoned", NOW, "nil", Optional.empty()));
			fixture.requireProtectedMetadata = true;
			fixture.database.transaction(() -> {
				Query query = fixture.database.query("INSERT INTO " + fixture.table
						+ " (id, plain, zoned, nullable_integer) VALUES (:id, CAST(:plain AS TIMESTAMP), CAST(:zoned AS TIMESTAMPTZ), :nil)");
				if (chunkSize != null) query.batchChunkSize(chunkSize);
				assertEquals(3, query.executeBatch(groups).size());
			});
			assertEquals(List.of(1, 2, 3, 4), fixture.ids());
			assertEquals(LocalDateTime.ofInstant(NOW, zone), fixture.inspection.query("SELECT plain FROM " + fixture.table + " WHERE id=3")
					.fetchObject(LocalDateTime.class).orElseThrow());
			assertEquals(local.atZone(zone).toInstant(), fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id=3")
					.fetchObject(Instant.class).orElseThrow());
			assertTrue(fixture.inspection.query("SELECT plain IS NULL AND nullable_integer IS NULL FROM " + fixture.table + " WHERE id=4")
					.fetchObject(Boolean.class).orElseThrow());
			assertEquals(1, fixture.descriptions.get());
			assertEquals(1, fixture.savepoints.get());
			assertEquals(1, fixture.releases.get());
			assertEquals(0, fixture.rollbacks.get());
			assertEquals(3, fixture.typeReads.get());
			assertEquals(3, fixture.nameReads.get());
		}
	}

	@Test
	void customBinderSqlRunsOutsideMetadataProtectionAndSurvivesOptionalDescribeRecovery() {
		Fixture fixture = new Fixture(AutoSave.NEVER, ZoneId.of("UTC"));
		String audit = fixture.table + "_audit";
		fixture.inspection.query("CREATE TABLE " + audit + " (id INTEGER PRIMARY KEY)").execute();
		AtomicInteger callbacks = new AtomicInteger();
		CustomParameterBinder callback = new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Instant.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws SQLException {
				assertEquals(fixture.savepoints.get(), fixture.releases.get(), "Custom SQL must not be inside a speculative metadata window");
				try (var sql = statement.getConnection().createStatement()) {
					assertEquals(1, sql.executeUpdate("INSERT INTO " + audit + " VALUES (" + callbacks.incrementAndGet() + ")"));
				}
				return BindingResult.fallback();
			}
		};
		Database database = Database.with(fixture.counted, DatabaseType.POSTGRESQL)
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(callback))).build();
		database.transaction(() -> database.query("UPDATE " + fixture.table
				+ " SET zoned = :now WHERE id = 1 AND last_login < :cutoff - interval '30 days'")
				.bind("now", NOW).bind("cutoff", NOW).execute());
		assertEquals(2, callbacks.get());
		assertEquals(List.of(1, 2), fixture.inspection.query("SELECT id FROM " + audit + " ORDER BY id").fetchList(Integer.class));
		assertTrue(fixture.rollbacks.get() > 0, "The ambiguous describe must actually exercise rollback recovery");
		assertEquals(fixture.savepoints.get(), fixture.releases.get());
		assertEquals(NOW, fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id=1").fetchObject(Instant.class).orElseThrow());
	}

	@Test
	void aLateCustomOidTypeNameReadRemainsProtectedAfterMetadataInitializationAndCachesRecoveredFallback() {
		Fixture fixture = new Fixture(AutoSave.NEVER, ZoneId.of("UTC"));
		String enumType = fixture.table + "_kind";
		fixture.inspection.query("CREATE TYPE " + enumType + " AS ENUM ('ready')").execute();
		AtomicBoolean failed = new AtomicBoolean();
		fixture.requireProtectedMetadata = true;
		fixture.nameReadHook = (connection, index, name) -> {
			if (index != 2) return;
			assertEquals(enumType, name);
			if (failed.compareAndSet(false, true)) {
				// Type/name getters for a previously unseen OID can execute SQL even after the describe.
				try (var statement = connection.createStatement()) { statement.execute("SELECT 1 / 0"); }
			}
		};
		fixture.database.transaction(() -> {
			fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (2)").execute();
			fixture.database.useRawConnection(connection -> {
				String sql = "SELECT CAST(? AS INTEGER), CAST(? AS " + enumType + ")";
				try (PreparedStatement statement = connection.prepareStatement(sql)) {
					StatementContext<?> context = StatementContext.with(Statement.of("late-custom-oid", sql), fixture.database).build();
					var resolver = new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(context, statement);
					assertEquals(Types.INTEGER, resolver.determineParameterSqlType(1).orElseThrow().getSqlType());
					assertTrue(resolver.determineParameterSqlType(2).isPresent());
					assertTrue(resolver.determineParameterSqlType(2).isPresent());
				}
				return Optional.empty();
			});
			fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (3)").execute();
		});
		assertTrue(failed.get());
		assertEquals(List.of(1, 2, 3), fixture.ids());
		assertEquals(1, fixture.descriptions.get());
		assertEquals(2, fixture.savepoints.get());
		assertEquals(2, fixture.releases.get());
		assertEquals(1, fixture.rollbacks.get());
		assertEquals(2, fixture.typeReads.get());
		assertEquals(2, fixture.nameReads.get());
	}

	@Test
	void rawBinderRecoversDescribeFailureWithoutExposingRawTransactionControls() {
		Fixture fixture = new Fixture(AutoSave.NEVER, ZoneId.of("UTC"));
		String sql = "UPDATE " + fixture.table + " SET zoned = ? WHERE id = 1 AND last_login < ? - interval '30 days'";
		StatementContext<?> context = StatementContext.with(Statement.of("raw-described-update", sql), fixture.database).build();
		fixture.database.transaction(() -> {
			fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (2)").execute();
			fixture.database.useRawConnection(connection -> {
				assertThrows(IllegalStateException.class, connection::setSavepoint);
				try (PreparedStatement statement = connection.prepareStatement(sql)) {
					PreparedStatementBinder binder = PreparedStatementBinder.withDefaultConfiguration();
					binder.bindParameter(context, statement, 1, NOW);
					binder.bindParameter(context, statement, 2, NOW);
					assertEquals(1, statement.executeUpdate());
				}
				return Optional.empty();
			});
		});
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(NOW, fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id = 1")
				.fetchObject(Instant.class).orElseThrow());
		// Direct binder calls remain independently scoped; both ambiguous describes are safely recovered.
		assertEquals(2, fixture.descriptions.get());
		assertEquals(2, fixture.savepoints.get());
		assertEquals(2, fixture.rollbacks.get());
		assertEquals(2, fixture.releases.get());
	}

	@Test
	void autocommitDescribeFallbackStillExecutesTheValidTemporalStatement() {
		Fixture fixture = new Fixture(AutoSave.NEVER, ZoneId.of("UTC"));
		assertEquals(1L, fixture.database.query("UPDATE " + fixture.table
				+ " SET zoned = :now WHERE id = 1 AND last_login < :cutoff - interval '30 days'")
				.bind("now", NOW).bind("cutoff", NOW).execute());
		assertEquals(1, fixture.descriptions.get());
		assertEquals(0, fixture.savepoints.get());
		assertEquals(NOW, fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id = 1")
				.fetchObject(Instant.class).orElseThrow());
	}

	@Test
	void opaqueAndDecoratingBindersShareOneFenceWithoutCrossingCallbackSql() {
		for (String mode : List.of("unrelated", "fallback", "handled", "json", "secure", "decorating", "decorating-custom")) {
			ZoneId zone = ZoneId.of("Asia/Tokyo"); Fixture fixture = new Fixture(AutoSave.NEVER, zone);
			fixture.wrapMetadata = false;
			fixture.inspection.query("ALTER TABLE " + fixture.table + " ADD nullable_text TEXT, ADD payload JSONB").execute();
			AtomicInteger callbacks = new AtomicInteger(), decorated = new AtomicInteger();
			CustomParameterBinder callback = new CustomParameterBinder() {
				@Override public Boolean appliesTo(TargetType target) {
					return target.matchesClass(mode.equals("unrelated") || mode.equals("decorating-custom") ? UUID.class : Instant.class);
				}
				@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws SQLException {
					assertEquals(fixture.savepoints.get(), fixture.releases.get(), "Callbacks must run after protection closes: " + mode);
					callbacks.incrementAndGet();
					if (!mode.equals("handled")) return BindingResult.fallback();
					statement.setObject(index, ((Instant) value).atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE);
					return BindingResult.handled();
				}
			};
			PreparedStatementBinder delegate = mode.equals("json") || mode.equals("secure") || mode.equals("decorating")
					? PreparedStatementBinder.withDefaultConfiguration() : PreparedStatementBinder.withCustomParameterBinders(List.of(callback));
			PreparedStatementBinder binder = mode.startsWith("decorating") ? decorating(delegate, decorated) : delegate;
			Database database = Database.with(fixture.counted, DatabaseType.POSTGRESQL).timeZone(zone).preparedStatementBinder(binder).build();
			LocalDateTime local = LocalDateTime.of(2024, 6, 15, 1, 2, 3, 123_456_000); LocalDate day = LocalDate.of(2024, 3, 10);
			String sql = "INSERT INTO " + fixture.table + " (id, zoned, plain, last_login, nullable_integer, nullable_text, payload)"
					+ " VALUES (2, :instant, :local, :day, :nil, :text, " + (mode.equals("json") ? ":json" : "NULL") + ")";
			database.transaction(() -> {
				database.query("UPDATE " + fixture.table + " SET id=id WHERE id=1").execute();
				Query query = database.query(sql).bind("instant", mode.equals("secure") ? Parameters.secure(NOW) : NOW)
						.bind("local", local).bind("day", day).bind("nil", Optional.empty()).bind("text", Optional.empty());
				if (mode.equals("json")) query.bind("json", Parameters.json("{\"kind\":\"kept\"}"));
				assertEquals(1L, query.execute());
			});
			assertEquals(1, fixture.descriptions.get(), mode);
			assertEquals(1, fixture.savepoints.get(), mode); assertEquals(1, fixture.releases.get(), mode);
			assertEquals(0, fixture.rollbacks.get(), mode);
			assertEquals(mode.equals("fallback") || mode.equals("handled") ? 1 : 0, callbacks.get(), mode);
			if (mode.startsWith("decorating")) assertEquals(3, decorated.get(), "Only nonnull values invoke the binder SPI: " + mode);
			assertEquals(local, fixture.inspection.query("SELECT plain FROM " + fixture.table + " WHERE id=2").fetchObject(LocalDateTime.class).orElseThrow(), mode);
			assertEquals(NOW, fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id=2").fetchObject(Instant.class).orElseThrow(), mode);
			assertEquals(day.atStartOfDay(zone).toInstant(), fixture.inspection.query("SELECT last_login FROM " + fixture.table + " WHERE id=2").fetchObject(Instant.class).orElseThrow(), mode);
			assertTrue(fixture.inspection.query("SELECT nullable_integer IS NULL AND nullable_text IS NULL FROM " + fixture.table + " WHERE id=2").fetchObject(Boolean.class).orElseThrow(), mode);
			if (mode.equals("json")) assertEquals("kept", fixture.inspection.query("SELECT payload->>'kind' FROM " + fixture.table + " WHERE id=2").fetchObject(String.class).orElseThrow());
		}
	}

	@Test
	void heterogeneousUntrustedBatchesKeepDeclaredTimestampFamiliesAcrossRowsAndChunks() {
		ZoneId zone = ZoneId.of("Asia/Tokyo"); LocalDateTime local = LocalDateTime.of(2024, 6, 15, 1, 2, 3, 123_456_000);
		for (String mode : List.of("default", "unrelated", "decorating")) {
			for (boolean casts : new boolean[]{false, true}) {
				for (Integer chunk : new Integer[]{null, 1, 2}) {
					Fixture fixture = new Fixture(AutoSave.NEVER, zone, true); fixture.wrapMetadata = false;
					CustomParameterBinder unrelated = new CustomParameterBinder() {
						@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(UUID.class); }
						@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) { throw new AssertionError("Unrelated binder must not run"); }
					};
					PreparedStatementBinder delegate = mode.equals("default") ? PreparedStatementBinder.withDefaultConfiguration()
							: PreparedStatementBinder.withCustomParameterBinders(List.of(unrelated));
					Database database = Database.with(fixture.counted, DatabaseType.POSTGRESQL).timeZone(zone)
							.preparedStatementBinder(mode.equals("decorating") ? decorating(delegate, new AtomicInteger()) : delegate).build();
					String sql = "INSERT INTO " + fixture.table + " (id, last_login, plain, zoned) VALUES (:id, :anchor, "
							+ (casts ? "CAST(:plain AS TIMESTAMP), CAST(:zoned AS TIMESTAMPTZ)" : ":plain, :zoned") + ")";
					List<Map<String, Object>> rows = List.of(Map.of("id", 2, "anchor", NOW, "plain", "2024-06-15T01:02:03.123456", "zoned", "2024-06-15T12:34:56.123456Z"),
							Map.of("id", 3, "anchor", NOW, "plain", NOW, "zoned", local),
							Map.of("id", 4, "anchor", NOW, "plain", local, "zoned", NOW),
							Map.of("id", 5, "anchor", NOW, "plain", Optional.empty(), "zoned", Optional.empty()));
					database.transaction(() -> {
						database.query("UPDATE " + fixture.table + " SET id=id WHERE id=1").execute();
						assertEquals(4, database.query(sql).batchChunkSize(chunk).executeBatch(rows).size());
						database.query(sql).bind("id", 30).bind("anchor", NOW).bind("plain", NOW).bind("zoned", local).execute();
						database.query(sql).bind("id", 40).bind("anchor", NOW).bind("plain", local).bind("zoned", NOW).execute();
					});
					String label = mode + ", casts=" + casts + ", chunk=" + chunk;
					DateTimeFormatter format = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS");
					List<ServerFields> fields = fixture.inspection.query("SELECT id, to_char(plain, 'YYYY-MM-DD\"T\"HH24:MI:SS.US') AS plain,"
							+ " to_char(zoned AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US') AS zoned FROM " + fixture.table + " WHERE id>1 ORDER BY id").fetchList(ServerFields.class);
					ServerFields firstMixed = new ServerFields(3, LocalDateTime.ofInstant(NOW, zone).format(format), local.atZone(zone).withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime().format(format));
					ServerFields secondMixed = new ServerFields(4, local.format(format), LocalDateTime.ofInstant(NOW, ZoneOffset.UTC).format(format));
					assertEquals(List.of(new ServerFields(2, local.format(format), LocalDateTime.ofInstant(NOW, ZoneOffset.UTC).format(format)), firstMixed, secondMixed,
							new ServerFields(5, null, null), new ServerFields(30, firstMixed.plain(), firstMixed.zoned()), new ServerFields(40, secondMixed.plain(), secondMixed.zoned())), fields, label);
					assertEquals(3, fixture.descriptions.get(), label); assertEquals(3, fixture.savepoints.get(), label);
					assertEquals(3, fixture.releases.get(), label); assertEquals(0, fixture.rollbacks.get(), label);
				}
			}
		}
	}
	public record ServerFields(int id, String plain, String zoned) {}

	@Test
	void aLateFirstNativeDescribeCannotTrustEarlierBoundInputTypesAsSqlTargets() {
		ZoneId zone = ZoneId.of("Asia/Tokyo"); LocalDateTime local = LocalDateTime.of(2024, 6, 15, 1, 2, 3, 123_456_000);
		for (String mode : List.of("default", "unrelated", "decorating")) {
			for (Integer chunk : new Integer[]{null, 1, 2}) {
				Fixture fixture = new Fixture(AutoSave.NEVER, zone); fixture.wrapMetadata = false;
				CustomParameterBinder unrelated = new CustomParameterBinder() {
					@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(UUID.class); }
					@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) { throw new AssertionError("Unrelated binder must not run"); }
				};
				PreparedStatementBinder delegate = mode.equals("default") ? PreparedStatementBinder.withDefaultConfiguration()
						: PreparedStatementBinder.withCustomParameterBinders(List.of(unrelated));
				Database database = Database.with(fixture.counted, DatabaseType.POSTGRESQL).timeZone(zone)
						.preparedStatementBinder(mode.equals("decorating") ? decorating(delegate, new AtomicInteger()) : delegate).build();
				// Row 1 binds VARCHAR at position 2 before position 3 first asks for metadata. CAST
				// keeps that row valid, but a copied VARCHAR input OID is not a TIMESTAMP target fact.
				List<Map<String, Object>> rows = List.of(
						Map.of("id", 2, "plain", "2024-06-15T01:02:03.123456", "zoned", NOW),
						Map.of("id", 3, "plain", NOW, "zoned", local), Map.of("id", 4, "plain", local, "zoned", NOW));
				TransactionOutcome<Integer> outcome = database.transactionWithOutcome(() -> Optional.of(database.query("INSERT INTO " + fixture.table
						+ " (id, plain, zoned) VALUES (:id, CAST(:plain AS TIMESTAMP), CAST(:zoned AS TIMESTAMPTZ))")
						.batchChunkSize(chunk).executeBatch(rows).size()));
				String label = mode + ", chunk=" + chunk;
				if (mode.equals("default")) assertEquals(TransactionResult.COMMITTED, outcome.getResult(), label);
				if (outcome.getResult() == TransactionResult.COMMITTED) {
					assertTrue(outcome.getFailure().isEmpty(), label);
					assertEquals(List.of(new ServerFields(2, "2024-06-15T01:02:03.123456", "2024-06-15T12:34:56.123456"),
							new ServerFields(3, "2024-06-15T21:34:56.123456", "2024-06-14T16:02:03.123456"),
							new ServerFields(4, "2024-06-15T01:02:03.123456", "2024-06-15T12:34:56.123456")),
							fixture.inspection.query("SELECT id, to_char(plain, 'YYYY-MM-DD\"T\"HH24:MI:SS.US') AS plain,"
									+ " to_char(zoned AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US') AS zoned FROM " + fixture.table + " WHERE id>1 ORDER BY id").fetchList(ServerFields.class), label);
				} else {
					assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult(), label); assertTrue(outcome.getAttemptedValue().isEmpty(), label);
					DatabaseException failure = assertInstanceOf(DatabaseException.class, outcome.getFailure().orElseThrow());
					assertEquals(Optional.of("0A000"), failure.getSqlState(), label); assertEquals(List.of(1), fixture.ids(), label);
				}
			}
		}
	}

	@Test
	void opaqueMetadataRefusesLateFirstLookupsWithoutCommittingEarlierChunks() {
		ZoneId zone = ZoneId.of("Asia/Tokyo"); LocalDateTime local = LocalDateTime.of(2024, 6, 15, 1, 2, 3, 123_456_000);
		for (Integer chunk : new Integer[]{null, 1, 2}) {
			Fixture fixture = new Fixture(AutoSave.NEVER, zone, true); // Keep the foreign PMD proxy opaque.
			CustomParameterBinder unrelated = new CustomParameterBinder() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(UUID.class); }
				@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) { throw new AssertionError("Unrelated binder must not run"); }
			};
			Database database = Database.with(fixture.counted, DatabaseType.POSTGRESQL).timeZone(zone)
					.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(unrelated))).build();
			List<Map<String, Object>> rows = List.of(
					Map.of("id", 2, "anchor", NOW, "plain", "2024-06-15T01:02:03.123456", "zoned", "2024-06-15T12:34:56.123456Z"),
					Map.of("id", 3, "anchor", NOW, "plain", NOW, "zoned", local),
					Map.of("id", 4, "anchor", NOW, "plain", local, "zoned", NOW));
			TransactionOutcome<Integer> outcome = database.transactionWithOutcome(() -> Optional.of(database.query("INSERT INTO " + fixture.table
					+ " (id, last_login, plain, zoned) VALUES (:id, :anchor, :plain, :zoned)").batchChunkSize(chunk).executeBatch(rows).size()));
			assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
			assertTrue(outcome.getAttemptedValue().isEmpty());
			DatabaseException failure = assertInstanceOf(DatabaseException.class, outcome.getFailure().orElseThrow());
			assertEquals(Optional.of("0A000"), failure.getSqlState());
			assertEquals(List.of(1), fixture.ids(), "Opaque metadata refusal must roll back every earlier chunk; chunk=" + chunk);
		}
	}

	@Test
	void internalMetadataSavepointsDoNotConsumeTheDriverCounterOrInvalidateApplicationRecovery() {
		Fixture fixture = new Fixture(AutoSave.NEVER, ZoneId.of("UTC")); fixture.initialSavepointId = Integer.MAX_VALUE;
		fixture.database.transaction(() -> {
			Transaction transaction = fixture.database.currentTransaction().orElseThrow();
			Savepoint application = transaction.createSavepoint(); // MAX_VALUE succeeds, then pgjdbc's counter wraps.
			fixture.database.query("INSERT INTO " + fixture.table + " (id, zoned) VALUES (2, :at)").bind("at", NOW).execute();
			fixture.database.query("INSERT INTO " + fixture.table + " (id, zoned) VALUES (3, :at)").bind("at", NOW).execute();
			transaction.rollback(application); transaction.releaseSavepoint(application);
			fixture.database.query("INSERT INTO " + fixture.table + " (id, zoned) VALUES (4, :at)").bind("at", NOW).execute();
		});
		assertEquals(List.of(1, 4), fixture.ids());
		assertEquals(Integer.MIN_VALUE, fixture.counterAfterSavepoint.get());
		assertEquals(1L, fixture.savepointNames.stream().filter("<unnamed>"::equals).count());
		assertEquals(3L, fixture.savepointNames.stream().filter(name -> !name.equals("<unnamed>")).distinct().count(),
				"Each internal scope must use an independent named marker");
		assertEquals(4, fixture.savepoints.get()); assertEquals(4, fixture.releases.get()); assertEquals(1, fixture.rollbacks.get());
		assertEquals(3, fixture.descriptions.get());
	}

	private static PreparedStatementBinder decorating(PreparedStatementBinder delegate, AtomicInteger calls) {
		return new PreparedStatementBinder() {
			@Override public <T> void bindParameter(StatementContext<T> context, PreparedStatement statement, Integer index, Object parameter) throws SQLException {
				calls.incrementAndGet(); delegate.bindParameter(context, statement, index, parameter);
			}
		};
	}

	private static final class Fixture {
		final Database database, inspection;
		final DataSource counted;
		final String table = "metadata_" + UUID.randomUUID().toString().replace("-", "");
		final AtomicInteger descriptions = new AtomicInteger(), savepoints = new AtomicInteger(),
				rollbacks = new AtomicInteger(), releases = new AtomicInteger(), typeReads = new AtomicInteger(), nameReads = new AtomicInteger();
		boolean requireProtectedMetadata;
		boolean wrapMetadata = true;
		NameReadHook nameReadHook;
		Integer initialSavepointId;
		final AtomicInteger counterAfterSavepoint = new AtomicInteger();
		final List<String> savepointNames = new ArrayList<>();

		Fixture(AutoSave autoSave, ZoneId zone) { this(autoSave, zone, false); }
		Fixture(AutoSave autoSave, ZoneId zone, boolean unspecifiedStrings) {
			PGSimpleDataSource source = new PGSimpleDataSource();
			source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
			source.setOptions("-c TimeZone=UTC"); source.setAutosave(autoSave);
			if (unspecifiedStrings) source.setStringType("unspecified");
			this.inspection = Database.with(source, DatabaseType.POSTGRESQL).build();
			this.inspection.query("CREATE TABLE " + this.table
					+ " (id INT PRIMARY KEY, last_login TIMESTAMPTZ, plain TIMESTAMP, zoned TIMESTAMPTZ, nullable_integer INT)").execute();
			this.inspection.query("INSERT INTO " + this.table + " (id, last_login) VALUES (1, TIMESTAMPTZ '2020-01-02T00:00:00Z')").execute();
			this.counted = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(source, method, args);
				if (!(result instanceof Connection connection)) return result;
				// pgjdbc sends its own startup TimeZone after startup options. Set the session
				// explicitly so temporal controls do not depend on the test JVM's default zone.
				try (var session = connection.createStatement()) { session.execute("SET TIME ZONE 'UTC'"); }
				if (this.initialSavepointId != null) {
					Field counter = connection.getClass().getDeclaredField("savepointId"); counter.setAccessible(true); counter.setInt(connection, this.initialSavepointId);
				}
				Connection[] handle = new Connection[1];
				handle[0] = proxy(Connection.class, (jdbc, values) -> {
					String name = jdbc.getName();
					if (name.equals("setSavepoint")) {
						this.savepoints.incrementAndGet(); this.savepointNames.add(values == null || values.length == 0 ? "<unnamed>" : (String) values[0]);
					}
					if (name.equals("rollback") && values != null && values.length == 1) this.rollbacks.incrementAndGet();
					if (name.equals("releaseSavepoint")) this.releases.incrementAndGet();
					Object value = invoke(connection, jdbc, values);
					if (name.equals("setSavepoint") && this.initialSavepointId != null) {
						Field counter = connection.getClass().getDeclaredField("savepointId"); counter.setAccessible(true); this.counterAfterSavepoint.set(counter.getInt(connection));
					}
					if (!(value instanceof PreparedStatement statement)) return value;
					return proxy(PreparedStatement.class, (call, parameters) -> {
						if (call.getName().equals("getConnection")) return handle[0];
						if (!call.getName().equals("getParameterMetaData")) return invoke(statement, call, parameters);
						this.descriptions.incrementAndGet();
						ParameterMetaData metadata = (ParameterMetaData) invoke(statement, call, parameters);
						if (!this.wrapMetadata) return metadata;
						return proxy(ParameterMetaData.class, (getter, indexes) -> {
							boolean typeRead = getter.getName().equals("getParameterType"), nameRead = getter.getName().equals("getParameterTypeName");
							if (typeRead || nameRead) {
								if (this.requireProtectedMetadata) assertTrue(this.savepoints.get() > this.releases.get(),
										"Both type and type-name reads must remain inside protection");
								if (typeRead) this.typeReads.incrementAndGet();
								else this.nameReads.incrementAndGet();
							}
							Object resolved = invoke(metadata, getter, indexes);
							if (nameRead && this.nameReadHook != null)
								this.nameReadHook.read(connection, (Integer) indexes[0], (String) resolved);
							return resolved;
						});
					});
				});
				return handle[0];
			});
			this.database = Database.with(this.counted, DatabaseType.POSTGRESQL).timeZone(zone).build();
		}

		List<Integer> ids() { return this.inspection.query("SELECT id FROM " + this.table + " ORDER BY id").fetchList(Integer.class); }
	}
	@FunctionalInterface private interface NameReadHook { void read(Connection connection, int index, String name) throws SQLException; }

	private static void forceTransactionFailure(Fixture fixture, String state) {
		if (state.equals("23505")) fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (1)").execute();
		else if (state.equals("21000")) fixture.database.query("SELECT (SELECT generate_series(1, 2))").fetchObject(Integer.class);
		else fixture.database.query("DO $$ BEGIN RAISE EXCEPTION 'forced transaction abort' USING ERRCODE = '" + state + "'; END $$").execute();
	}

	private static String failureShape(Throwable failure) {
		StringBuilder shape = new StringBuilder();
		for (int depth = 0; failure != null && depth < 8; depth++, failure = failure.getCause()) {
			if (depth != 0) shape.append(" -> ");
			shape.append(failure.getClass().getName()).append("[suppressed=").append(failure.getSuppressed().length);
			if (failure instanceof SQLException sql)
				shape.append(", state=").append(sql.getSQLState()).append(", next=")
						.append(sql.getNextException() == null ? "none" : sql.getNextException().getClass().getName());
			shape.append(']');
		}
		return shape.toString();
	}

	@FunctionalInterface private interface Call { Object run(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> call.run(method, args)));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
