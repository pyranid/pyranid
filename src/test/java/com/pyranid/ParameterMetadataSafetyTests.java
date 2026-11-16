/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ParameterMetadataSafetyTests {
	@AfterEach
	void clearInterruption() { Thread.interrupted(); }

	@Test
	void metadataAndResolvedTypesAreReusedAcrossRowsAndChunksButNotAcrossStatements() {
		for (Integer chunkSize : new Integer[]{null, 3}) {
			for (String failureSite : List.of("none", "metadata", "type", "name")) {
				JDBCDataSource source = new JDBCDataSource();
				source.setUrl("jdbc:hsqldb:mem:metadata_batch_" + UUID.randomUUID());
				source.setUser("sa"); source.setPassword("");
				Database inspection = Database.with(source, DatabaseType.GENERIC).build();
				inspection.query("CREATE TABLE work (id INT, value_integer INT, value_instant TIMESTAMP WITH TIME ZONE)").execute();
				AtomicInteger descriptions = new AtomicInteger(), types = new AtomicInteger(), names = new AtomicInteger();
				DataSource counted = proxy(DataSource.class, (method, args) -> {
					Object result = invoke(source, method, args);
					if (!(result instanceof Connection connection)) return result;
					return proxy(Connection.class, (call, parameters) -> {
						Object value = invoke(connection, call, parameters);
						if (!(value instanceof PreparedStatement statement)) return value;
						return proxy(PreparedStatement.class, (jdbc, values) -> {
							if (!jdbc.getName().equals("getParameterMetaData")) return invoke(statement, jdbc, values);
							descriptions.incrementAndGet();
							if (failureSite.equals("metadata")) throw new SQLException("optional describe unavailable", "0A000");
							ParameterMetaData metadata = statement.getParameterMetaData();
							return proxy(ParameterMetaData.class, (getter, indexes) -> {
								if (getter.getName().equals("getParameterType")) {
									types.incrementAndGet();
									if (failureSite.equals("type")) throw new SQLException("optional type unavailable", "HY000");
								}
								if (getter.getName().equals("getParameterTypeName")) {
									names.incrementAndGet();
									if (failureSite.equals("name")) throw new SQLException("optional name unavailable", "HY000");
								}
								return invoke(metadata, getter, indexes);
							});
						});
					});
				});
				Database database = Database.with(counted, DatabaseType.GENERIC).build();
				List<Map<String, Object>> rows = new ArrayList<>();
				for (int index = 0; index < 13; index++) rows.add(Map.of("id", index, "value", Optional.empty(), "created", Instant.parse("2020-01-02T03:04:05Z")));
				for (int execution = 1; execution <= 2; execution++) {
					Query query = database.query("INSERT INTO work VALUES (:id, :value, :created)");
					if (chunkSize != null) query.batchChunkSize(chunkSize);
					query.executeBatch(rows);
					assertEquals(execution, descriptions.get(), failureSite);
					assertEquals(failureSite.equals("metadata") ? 0 : 2 * execution, types.get(), failureSite);
					assertEquals(List.of("metadata", "type").contains(failureSite) ? 0 : 2 * execution, names.get(), failureSite);
				}
				assertEquals(26, inspection.query("SELECT COUNT(*) FROM work").fetchObject(Integer.class).orElseThrow());
			}
		}
	}

	@Test
	void optionalPostgresDescribeFailureIsRecoveredOnceAndCached() throws Exception {
		Probe probe = new Probe(DatabaseType.POSTGRESQL);
		probe.metadataFailure = new SQLException("ambiguous unknown parameter", "42883");
		var resolver = probe.resolver();
		resolver.registerParameterIndices(List.of(1, 2, 3));
		assertTrue(resolver.determineParameterSqlType(1).isEmpty());
		assertTrue(resolver.determineParameterSqlType(1).isEmpty());
		assertTrue(resolver.determineParameterSqlType(2).isEmpty());
		assertEquals(List.of("savepoint", "metadata", "rollback", "release"), probe.events);
		assertFalse(probe.aborted);
	}

	@Test
	void declaredMetadataPositionsShareOneProtectionWindowAndLateDemandsStayProtected() throws Exception {
		Probe probe = new Probe(DatabaseType.POSTGRESQL);
		probe.requireProtectedGetters = true;
		var resolver = probe.resolver();
		resolver.registerParameterIndices(List.of(2, 4, 5, 2));
		assertTrue(probe.events.isEmpty(), "Declaring demands must not perform JDBC work");
		for (int index : new int[]{4, 2, 5, 4})
			assertEquals(Types.TIMESTAMP, resolver.determineParameterSqlType(index).orElseThrow().getSqlType());
		assertEquals(Set.of(2, 4, 5), Set.copyOf(probe.typeReads));
		assertEquals(3, probe.typeReads.size());
		assertEquals(3, probe.nameReads.size());
		assertEquals(1, probe.descriptions);
		assertEquals(1, probe.savepoints);
		assertEquals(1, probe.releases);
		assertEquals(0, probe.rollbacks);
		assertEquals(0, probe.activeSavepoints);

		resolver.registerParameterIndices(List.of(3));
		assertEquals(Types.TIMESTAMP, resolver.determineParameterSqlType(4).orElseThrow().getSqlType());
		assertEquals(1, probe.savepoints, "A cached demand must not trigger speculative late JDBC work");
		assertEquals(Types.TIMESTAMP, resolver.determineParameterSqlType(3).orElseThrow().getSqlType());
		assertEquals(Set.of(2, 3, 4, 5), Set.copyOf(probe.typeReads));
		assertEquals(4, probe.typeReads.size());
		assertEquals(1, probe.descriptions);
		assertEquals(2, probe.savepoints);
		assertEquals(2, probe.releases);
		assertEquals(0, probe.activeSavepoints);
	}

	@Test
	void optionalPositionFailuresRetainIndependentFallbacksAndProtectRemainingGetters() throws Exception {
		for (boolean nameFails : new boolean[]{false, true}) {
			Probe probe = new Probe(DatabaseType.POSTGRESQL);
			probe.requireProtectedGetters = true;
			probe.failureIndex = 2;
			if (nameFails) probe.nameFailure = new SQLException("name lookup unavailable", "HY000");
			else probe.typeFailure = new SQLException("type lookup unavailable", "HY000");
			var resolver = probe.resolver();
			resolver.registerParameterIndices(List.of(1, 2, 3));
			assertEquals(Types.TIMESTAMP, resolver.determineParameterSqlType(1).orElseThrow().getSqlType());
			assertEquals(Types.TIMESTAMP, resolver.determineParameterSqlType(3).orElseThrow().getSqlType());
			assertEquals(nameFails, resolver.determineParameterSqlType(2).isPresent());
			if (nameFails)
				assertEquals(Types.TIMESTAMP, resolver.determineParameterSqlType(2).orElseThrow().getSqlType());
			assertEquals(3, probe.typeReads.size(), "An optional failure must not cause repeated type reads");
			assertEquals(nameFails ? 3 : 2, probe.nameReads.size());
			assertEquals(1, probe.descriptions);
			assertEquals(2, probe.savepoints, "Later getters need fresh protection after recovery");
			assertEquals(1, probe.rollbacks);
			assertEquals(2, probe.releases);
			assertFalse(probe.aborted);
			assertEquals(0, probe.activeSavepoints);
		}
	}

	@Test
	void terminalFailureInALaterDeclaredPositionPoisonsEvenAlreadyResolvedDemands() {
		for (Throwable terminal : List.of(new SQLException("connection lost", "08006"),
				new AssertionError("fatal metadata read"))) {
			Probe probe = new Probe(DatabaseType.POSTGRESQL);
			probe.requireProtectedGetters = true;
			probe.failureIndex = 3;
			probe.typeFailure = terminal;
			var resolver = probe.resolver();
			resolver.registerParameterIndices(List.of(1, 2, 3));
			assertSame(terminal, assertThrows(Throwable.class, () -> resolver.determineParameterSqlType(1)));
			assertSame(terminal, assertThrows(Throwable.class, () -> resolver.determineParameterSqlType(2)));
			assertEquals(3, probe.typeReads.size());
			assertEquals(2, probe.nameReads.size());
			assertEquals(1, probe.savepoints);
			assertEquals(1, probe.rollbacks);
			assertEquals(1, probe.releases);
			assertEquals(0, probe.activeSavepoints);
		}
	}

	@Test
	void localTemporalBindingAvoidsUnnecessaryMetadataOnKnownUnzonedDialects() throws Exception {
		for (DatabaseType type : List.of(DatabaseType.MYSQL, DatabaseType.MARIA_DB, DatabaseType.SQLITE)) {
			for (Object value : List.of(LocalDate.of(2024, 7, 2), LocalDateTime.of(2024, 7, 2, 12, 34, 56))) {
				Probe probe = new Probe(type);
				probe.metadataFailure = new AssertionError("This dialect has no zoned target requiring local-date metadata");
				new DefaultPreparedStatementBinder().bindParameter(probe.context, probe.statement, 1, value);
				assertEquals(0, probe.descriptions, type.name());
				assertTrue(probe.events.isEmpty(), type.name());
			}
		}
	}

	@Test
	void plannedDemandsDoNotCrossOpaqueAccessorsOrCustomBinderCallbacks() throws Exception {
		SecureParameter secure = new SecureParameter() {
			@Override public Optional<Object> getValue() { throw new AssertionError("Speculative secure getter"); }
			@Override public String getMask() { throw new AssertionError("Speculative mask getter"); }
		};
		TypedParameter typed = new TypedParameter() {
			@Override public java.lang.reflect.Type getExplicitType() { throw new AssertionError("Speculative type getter"); }
			@Override public Optional<Object> getValue() { throw new AssertionError("Speculative typed getter"); }
		};
		CustomParameterBinder callback = new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType target) { throw new AssertionError("Speculative applicability callback"); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement,
					Integer index, Object value) { throw new AssertionError("Speculative binding callback"); }
		};
		for (Object barrier : List.of(secure, typed, new java.math.BigDecimal("1") {}, new java.util.Date() {})) {
			Probe probe = new Probe(DatabaseType.POSTGRESQL);
			probe.requireProtectedGetters = true;
			var resolver = probe.resolver();
			new DefaultPreparedStatementBinder().registerParameterSqlTypeDemands(probe.context, resolver,
					List.of(List.of(Instant.EPOCH, barrier, Optional.empty())));
			assertTrue(probe.events.isEmpty());
			resolver.determineParameterSqlType(1);
			assertEquals(List.of(1), probe.typeReads, "Future metadata must not cross an application-code barrier");
			resolver.determineParameterSqlType(3);
			assertEquals(List.of(1, 3), probe.typeReads);
			assertEquals(2, probe.savepoints);
		}
		Probe probe = new Probe(DatabaseType.POSTGRESQL);
		var resolver = probe.resolver();
		new DefaultPreparedStatementBinder(List.of(callback)).registerParameterSqlTypeDemands(probe.context, resolver,
				List.of(List.of(Instant.EPOCH, Optional.empty())));
		assertTrue(probe.events.isEmpty());
		resolver.determineParameterSqlType(1);
		assertEquals(List.of(1), probe.typeReads);
	}

	@Test
	void concreteJsonAndSecureWrappersKeepCallbackFreeDemandsInOneProtectionWindow() throws Exception {
		Probe probe = new Probe(DatabaseType.POSTGRESQL);
		probe.requireProtectedGetters = true;
		var resolver = probe.resolver();
		new DefaultPreparedStatementBinder().registerParameterSqlTypeDemands(probe.context, resolver,
				List.of(List.of(Instant.EPOCH, Parameters.json("{}"),
						Parameters.secure(Optional.of(Parameters.secure(LocalDateTime.of(2026, 1, 1, 0, 0)))),
						Parameters.secure(Optional.empty()))));
		assertTrue(probe.events.isEmpty());
		resolver.beginParameterGroup();
		resolver.prefetchRegisteredParameterSqlTypes();
		assertEquals(List.of(1, 3, 4), probe.typeReads);
		assertEquals(1, probe.savepoints);
		assertEquals(1, probe.releases);
	}

	@Test
	void opaqueMetadataCannotFirstResolveALateBatchPositionFromEarlierInputTypes() throws Exception {
		for (boolean firstGroupDescribed : new boolean[]{false, true}) {
			Probe probe = new Probe(DatabaseType.POSTGRESQL);
			var resolver = probe.resolver();
			resolver.beginParameterGroup();
			if (firstGroupDescribed) resolver.determineParameterSqlType(1);
			int descriptions = probe.descriptions, savepoints = probe.savepoints;
			resolver.beginParameterGroup();
			SQLException failure = assertThrows(SQLException.class, () -> resolver.determineParameterSqlType(2));
			assertEquals("0A000", failure.getSQLState());
			assertTrue(failure.getMessage().contains("previously uncached PostgreSQL batch parameter 2"));
			assertSame(failure, assertThrows(SQLException.class, () -> resolver.determineParameterSqlType(1)));
			assertEquals(descriptions, probe.descriptions, "Do not describe already-contaminated input OIDs");
			assertEquals(savepoints, probe.savepoints);
		}
	}

	@Test
	void lateSqlNullUsesUntypedBindingWithoutAuthorizingLaterTemporalCoercion() throws Exception {
		for (boolean firstGroupDescribed : new boolean[]{false, true}) {
			Probe probe = new Probe(DatabaseType.POSTGRESQL);
			var resolver = probe.resolver();
			resolver.beginParameterGroup(); resolver.noteParameterBound(1);
			if (firstGroupDescribed) resolver.determineParameterSqlType(2);
			int descriptions = probe.descriptions, savepoints = probe.savepoints;
			resolver.beginParameterGroup();
			assertTrue(resolver.determineNullParameterSqlType(1).isEmpty());
			new DefaultPreparedStatementBinder().bindParameter(probe.context, probe.statement, 1, Optional.empty(), resolver);
			assertEquals(descriptions, probe.descriptions);
			assertEquals(savepoints, probe.savepoints);
			SQLException unsafe = assertThrows(SQLException.class, () -> resolver.determineParameterSqlType(1));
			assertEquals("0A000", unsafe.getSQLState(), "A late NULL must not freeze unavailable target metadata");
			assertSame(unsafe, assertThrows(SQLException.class, () -> resolver.determineNullParameterSqlType(1)));
		}
	}

	@Test
	void aTrustedOpaqueMetadataPlanFreezesAllDemandedPositionsBeforeAnyGroupIsBound() throws Exception {
		Probe probe = new Probe(DatabaseType.POSTGRESQL);
		probe.requireProtectedGetters = true;
		var resolver = probe.resolver();
		new DefaultPreparedStatementBinder().registerParameterSqlTypeDemands(probe.context, resolver,
				List.of(List.of(Instant.EPOCH, "2026-01-01"), List.of(Instant.EPOCH, LocalDateTime.of(2026, 1, 1, 0, 0))));
		resolver.prefetchRegisteredParameterSqlTypes();
		resolver.beginParameterGroup();
		resolver.beginParameterGroup();
		assertEquals(Types.TIMESTAMP, resolver.determineParameterSqlType(2).orElseThrow().getSqlType());
		assertEquals(List.of(1, 2), probe.typeReads);
		assertEquals(1, probe.savepoints);
	}

	@Test
	void successfulMetadataAndTypeNameFallbackRemainCachedPerIndex() throws Exception {
		for (boolean missingName : new boolean[]{false, true}) {
			Probe probe = new Probe(DatabaseType.POSTGRESQL);
			if (missingName) probe.nameFailure = new SQLException("optional name unavailable", "HY000");
			var resolver = probe.resolver();
			for (int index : new int[]{1, 1, 2, 2})
				assertEquals(Types.TIMESTAMP, resolver.determineParameterSqlType(index).orElseThrow().getSqlType());
			assertEquals(1, probe.descriptions);
			assertEquals(2, probe.savepoints);
			assertEquals(missingName ? 2 : 0, probe.rollbacks);
			assertEquals(2, probe.releases);
			assertFalse(probe.aborted);
		}
	}

	@Test
	void anUnprotectedOrAutocommitDescribeKeepsItsExistingCompatibilityBehavior() throws Exception {
		for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.GENERIC)) {
			Probe probe = new Probe(type);
			probe.autoCommit = type == DatabaseType.POSTGRESQL;
			probe.metadataFailure = new SQLException("optional metadata unavailable", "0A000");
			assertTrue(probe.resolver().determineParameterSqlType(1).isEmpty());
			assertEquals(1, probe.descriptions);
			assertEquals(0, probe.savepoints);
			assertEquals(0, probe.rollbacks);
			assertEquals(0, probe.releases);
		}
	}

	@Test
	void failedProtectionNeverRunsTheDescribeAndFailedRecoveryNeverFallsBack() {
		for (String phase : List.of("savepoint", "rollback", "release")) {
			Probe probe = new Probe(DatabaseType.POSTGRESQL);
			SQLException describe = new SQLException("ambiguous describe", "42883");
			SQLException cleanup = new SQLException("failed " + phase, "HY000");
			probe.metadataFailure = describe;
			if (phase.equals("savepoint")) probe.savepointFailure = cleanup;
			if (phase.equals("rollback")) probe.rollbackFailure = cleanup;
			if (phase.equals("release")) probe.releaseFailure = cleanup;
			SQLException thrown = assertThrows(SQLException.class, () -> probe.resolver().determineParameterSqlType(1));
			assertSame(phase.equals("savepoint") ? cleanup : describe, thrown);
			assertEquals(phase.equals("savepoint") ? 0 : 1, probe.descriptions);
			if (!phase.equals("savepoint")) assertTrue(List.of(thrown.getSuppressed()).contains(cleanup));
		}
	}

	@Test
	void terminalDescribeFailuresRemainTerminalAfterSuccessfulRecovery() {
		for (Throwable failure : List.of(new SQLException("serialization", "40001"),
				new SQLException("cancelled", "57014", new InterruptedException("cancelled")),
				new AssertionError("fatal metadata failure"))) {
			Probe probe = new Probe(DatabaseType.POSTGRESQL);
			probe.metadataFailure = failure;
			assertSame(failure, assertThrows(failure.getClass(), () -> probe.resolver().determineParameterSqlType(1)));
			assertEquals(1, probe.rollbacks);
			assertEquals(1, probe.releases);
			assertFalse(probe.aborted);
		}
	}

	@Test
	void successfulDescribeDoesNotHideAReleaseFailureOrAnUnsupportedProtectionMethod() {
		Probe probe = new Probe(DatabaseType.POSTGRESQL);
		SQLException release = new SQLException("release failed", "HY000");
		probe.releaseFailure = release;
		assertSame(release, assertThrows(SQLException.class, () -> probe.resolver().determineParameterSqlType(1)));
		Probe legacy = new Probe(DatabaseType.POSTGRESQL);
		AbstractMethodError unsupported = new AbstractMethodError("savepoint unavailable");
		legacy.savepointFailure = unsupported;
		assertSame(unsupported, assertThrows(AbstractMethodError.class, () -> legacy.resolver().determineParameterSqlType(1)));
		assertEquals(0, legacy.descriptions);
		Probe missing = new Probe(DatabaseType.POSTGRESQL);
		missing.missingSavepoint = true;
		assertThrows(NullPointerException.class, () -> missing.resolver().determineParameterSqlType(1));
		assertEquals(0, missing.descriptions, "A null savepoint cannot authorize an unprotected describe");
	}

	@Test
	void rawBinderUsesInternalRecoveryWithoutOpeningPublicSavepointAccess() throws Exception {
		Probe probe = new Probe(DatabaseType.POSTGRESQL);
		probe.metadataFailure = new SQLException("ambiguous unknown parameter", "42883");
		PyranidRawConnection raw = new PyranidRawConnection(probe.connection);
		PreparedStatement guarded = raw.prepareStatement("SELECT ?");
		new DefaultPreparedStatementBinder().bindParameter(probe.context, guarded, 1, Optional.empty());
		assertEquals(1, probe.rollbacks);
		assertFalse(probe.aborted);
		assertThrows(IllegalStateException.class, raw::setSavepoint);
		CompletableFuture.runAsync(() -> assertThrows(IllegalStateException.class, raw::metadataConnection)).join();
		raw.release();
		assertThrows(IllegalStateException.class, raw::metadataConnection);
		assertThrows(IllegalStateException.class, guarded::getConnection);
	}

	@Test
	void handledMetadataProtectionFailuresFenceThePhysicalOwnerUnderANestedBoundary() {
		for (String phase : List.of("savepoint", "rollback", "release")) {
			for (String state : phase.equals("savepoint") ? List.of("HY000") : List.of("HY000", "25P02"))
				assertMetadataProtectionOwnerFence(phase, new SQLException("failed internal " + phase, state));
		}
	}

	@Test
	void abortedEstablishmentEvidenceWithConflictsOrUnreadableStateRetainsTheOwnerFence() {
		SQLException cause = new SQLException("aborted with connection failure", "25P02", new SQLException("lost connection", "08006"));
		SQLException next = new SQLException("aborted with chained failure", "25P02");
		next.setNextException(new SQLException("lost connection", "08006"));
		SQLException suppressed = new SQLException("aborted with cleanup failure", "25P02");
		suppressed.addSuppressed(new AssertionError("unknown cleanup"));
		SQLException unreadable = new SQLException("unreadable state") {
			@Override public String getSQLState() { throw new AssertionError("state inspection failed"); }
		};
		for (SQLException failure : List.of(cause, next, suppressed, unreadable))
			assertMetadataProtectionOwnerFence("savepoint", failure);
		for (String state : new String[]{"40001", "57014", "HY000", null})
			assertMetadataProtectionOwnerFence("savepoint", new SQLException("aborted with conflicting history", "25P02",
					new SQLException("uncertain earlier failure", state)));
		assertMetadataProtectionOwnerFence("savepoint", new SQLException("aborted with opaque history", "25P02",
				new IllegalStateException("unknown callback progress")));
		assertMetadataProtectionOwnerFence("savepoint", new SQLException("aborted with transient connection history", "25P02",
				new java.sql.SQLTransientConnectionException("lost connection", "23505")));
	}

	private void assertMetadataProtectionOwnerFence(String phase, SQLException failure) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:metadata_owner_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		Database inspection = Database.with(source, DatabaseType.GENERIC).build();
		inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
		inspection.query("INSERT INTO work VALUES (1)").execute();
		DataSource wrapped = proxy(DataSource.class, (method, args) -> {
			Object result = invoke(source, method, args);
			if (!(result instanceof Connection connection)) return result;
			Connection[] handle = new Connection[1];
			handle[0] = proxy(Connection.class, (call, values) -> {
				if ((phase.equals("savepoint") && call.getName().equals("setSavepoint"))
						|| (phase.equals("rollback") && call.getName().equals("rollback") && values != null && values.length == 1)
						|| (phase.equals("release") && call.getName().equals("releaseSavepoint"))) {
					// Model a failed control operation that has already discarded the surrounding work.
					connection.rollback();
					throw failure;
				}
				Object value = invoke(connection, call, values);
				if (!(value instanceof PreparedStatement prepared)) return value;
				return proxy(PreparedStatement.class, (jdbc, parameters) -> {
					if (jdbc.getName().equals("getConnection")) return handle[0];
					if (phase.equals("rollback") && jdbc.getName().equals("getParameterMetaData"))
						throw new SQLException("ambiguous metadata probe", "42883");
					return invoke(prepared, jdbc, parameters);
				});
			});
			return handle[0];
		});
		Database database = Database.with(wrapped, DatabaseType.GENERIC).build();
		Database selectedPostgres = Database.with(source, DatabaseType.POSTGRESQL).build();
		StatementContext<?> context = StatementContext.with(Statement.of("raw-metadata", "VALUES(?)"), selectedPostgres).build();
		TransactionOutcome<String> outcome = database.transactionWithOutcome(() -> {
			database.query("INSERT INTO work VALUES (2)").execute();
			database.useRawConnection(connection -> {
				try (PreparedStatement statement = connection.prepareStatement("VALUES(CAST(? AS INTEGER))")) {
					database.transaction(() -> {
						assertThrows(SQLException.class,
								() -> new DefaultPreparedStatementBinder().bindParameter(context, statement, 1, Optional.empty()));
					});
				}
				return Optional.empty();
			});
			assertFalse(database.currentTransaction().orElseThrow().isRollbackOnly(), phase);
			database.query("INSERT INTO work VALUES (3)").execute();
			return Optional.of("attempted receipt");
		});
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult(), phase);
		assertTrue(outcome.getFailure().isPresent(), "Uncertain protection must refuse completion visibly: " + phase);
		assertEquals(Optional.of("attempted receipt"), outcome.getAttemptedValue());
		assertEquals(List.of(1), inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class));
	}

	@Test
	void aForeignStatementContextCannotFenceAnUnrelatedTransaction() {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:metadata_foreign_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		Database database = Database.with(source, DatabaseType.GENERIC).build();
		database.query("CREATE TABLE work (id INT)").execute();
		Probe foreign = new Probe(DatabaseType.POSTGRESQL);
		foreign.savepointFailure = new SQLException("foreign savepoint unavailable", "HY000");
		TransactionOutcome<Void> outcome = database.transactionWithOutcome(() -> {
			database.query("INSERT INTO work VALUES (1)").execute();
			database.useRawConnection(connection -> {
				assertThrows(SQLException.class,
						() -> new DefaultPreparedStatementBinder().bindParameter(foreign.context, foreign.statement, 1, Optional.empty()));
				assertFalse(database.currentTransaction().orElseThrow().isRollbackOnly());
				return Optional.empty();
			});
		});
		assertEquals(TransactionResult.COMMITTED, outcome.getResult());
		assertTrue(outcome.getFailure().isEmpty());
		assertEquals(List.of(1), database.query("SELECT id FROM work").fetchList(Integer.class));
	}

	private static final class Probe {
		final List<String> events = new ArrayList<>();
		final StatementContext<?> context;
		final Connection connection;
		final PreparedStatement statement;
		boolean autoCommit, aborted, missingSavepoint, requireProtectedGetters;
		int descriptions, savepoints, rollbacks, releases, activeSavepoints;
		int failureIndex = -1;
		final List<Integer> typeReads = new ArrayList<>(), nameReads = new ArrayList<>();
		Throwable metadataFailure, typeFailure, nameFailure, savepointFailure, rollbackFailure, releaseFailure;

		Probe(DatabaseType type) {
			Database database = Database.with(new JDBCDataSource(), type).build();
			this.context = StatementContext.with(Statement.of("metadata-safety", "SELECT ?"), database).build();
			PreparedStatement[] prepared = new PreparedStatement[1];
			ParameterMetaData metadata = proxy(ParameterMetaData.class, (method, args) -> {
				if (method.getName().equals("getParameterType")) {
					assertProtectedGetter();
					this.typeReads.add((Integer) args[0]);
					if (this.typeFailure != null && (this.failureIndex < 0 || this.failureIndex == (int) args[0])) {
						this.aborted = true; throw this.typeFailure;
					}
					return Types.TIMESTAMP;
				}
				if (method.getName().equals("getParameterTypeName")) {
					assertProtectedGetter();
					this.nameReads.add((Integer) args[0]);
					if (this.nameFailure != null && (this.failureIndex < 0 || this.failureIndex == (int) args[0])) {
						this.aborted = true; throw this.nameFailure;
					}
					return "timestamp";
				}
				return null;
			});
			this.connection = proxy(Connection.class, (method, args) -> {
				switch (method.getName()) {
					case "getAutoCommit": return this.autoCommit;
					case "prepareStatement": return prepared[0];
					case "setSavepoint":
						this.events.add("savepoint"); this.savepoints++;
						if (this.savepointFailure != null) throw this.savepointFailure;
						if (this.missingSavepoint) return null;
						this.activeSavepoints++;
						return proxy(Savepoint.class, (ignored, values) -> 1);
					case "rollback":
						this.events.add("rollback"); this.rollbacks++;
						if (this.rollbackFailure != null) throw this.rollbackFailure;
						this.aborted = false; return null;
					case "releaseSavepoint":
						this.events.add("release"); this.releases++;
						if (this.releaseFailure != null) throw this.releaseFailure;
						this.activeSavepoints--;
						return null;
					default: return null;
				}
			});
			this.statement = prepared[0] = proxy(PreparedStatement.class, (method, args) -> {
				if (method.getName().equals("getConnection")) return this.connection;
				if (method.getName().equals("getParameterMetaData")) {
					this.events.add("metadata"); this.descriptions++;
					if (this.metadataFailure != null) { this.aborted = true; throw this.metadataFailure; }
					return metadata;
				}
				if (method.getName().equals("setNull") && this.aborted) throw new SQLException("transaction aborted", "25P02");
				return null;
			});
		}

		DefaultPreparedStatementBinder.ParameterSqlTypeResolver resolver() {
			return new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(this.context, this.statement);
		}

		private void assertProtectedGetter() {
			if (this.requireProtectedGetters) {
				assertEquals(1, this.activeSavepoints, "Lazy type/name lookups can issue server queries too");
				assertFalse(this.aborted, "An optional failure must be recovered before another getter");
			}
		}
	}

	@FunctionalInterface interface Call { Object run(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> call.run(method, args)));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
