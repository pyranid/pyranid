/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import com.pyranid.testdriver.util.PSQLException;
import com.pyranid.testdriver.util.ServerErrorMessage;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.*;

class SqlStateDiagnosticRedactionTests {
	@Test
	void operationalCodesNeverReachTheDiagnosticTextRedactor() {
		for (String state : Arrays.asList("23505", "40001", "08006", "57P05", "57014", "HY000", null)) {
			AtomicInteger calls = new AtomicInteger();
			SQLException driver = new SQLException("driver text", state, 19);
			DatabaseException failure = DatabaseException.wrapSafely("safe diagnostic", driver, GenericDialect.INSTANCE, value -> {
				calls.incrementAndGet();
				throw new AssertionError("Generic metadata has no driver-text fields to scrub");
			});
			assertEquals(Optional.ofNullable(state), failure.getSqlState());
			assertEquals(19, failure.getErrorCode().orElseThrow());
			assertSame(driver, failure.getCause());
			assertEquals(0, calls.get());
			assertEquals(0, driver.getSuppressed().length);
		}
	}

	@Test
	void fullAndPartialCodeOverlapsStillScrubDirectAndCopiedServerText() {
		for (String needle : List.of("2350", "23505")) {
			for (boolean copy : new boolean[]{false, true}) {
				PSQLException driver = driver("23505", needle);
				UnaryOperator<String> scrub = scrubber(needle);
				List<String> seen = new ArrayList<>();
				UnaryOperator<String> guarded = value -> {
					seen.add(value);
					if (value.equals("23505")) throw new AssertionError("SQLState is not driver text");
					return scrub.apply(value);
				};
				DatabaseException failure = copy
						? DatabaseException.wrapSafely("safe diagnostic", new DatabaseException("original", driver, PostgresDialect.INSTANCE), PostgresDialect.INSTANCE, guarded)
						: DatabaseException.wrapSafely("safe diagnostic", driver, PostgresDialect.INSTANCE, guarded);
				assertEquals("23505", failure.getSqlState().orElseThrow());
				assertEquals("detail=<hidden>", failure.getDetail().orElseThrow());
				assertEquals("hint=<hidden>", failure.getHint().orElseThrow());
				assertEquals("constraint_<hidden>", failure.getConstraint().orElseThrow());
				assertEquals("table_<hidden>", failure.getTable().orElseThrow());
				assertEquals("message=<hidden>", failure.getDbmsMessage().orElseThrow());
				assertTrue(failure.isUniqueConstraintViolation());
				assertFalse(seen.contains("23505"));
				assertTrue(seen.contains("detail=" + needle));
				assertTrue(failure.toString().contains("sqlState=23505"));
				assertEquals("detail=" + needle, PostgresDialect.INSTANCE.databaseExceptionMetadata(driver).detail);
				assertSame(driver, copy ? failure.getCause().getCause() : failure.getCause());
			}
		}
	}

	@Test
	void jdbcShellKeepsItsOwnStateWhileRecoveredServerFieldsRemainScrubbed() {
		BatchUpdateException batch = new BatchUpdateException("batch bound=0800", "08006", 91, new int[]{1});
		batch.setNextException(driver("23505", "0800"));
		DatabaseException failure = DatabaseException.wrapSafely("safe batch diagnostic", batch, PostgresDialect.INSTANCE, scrubber("0800"));
		assertEquals("08006", failure.getSqlState().orElseThrow());
		assertEquals(91, failure.getErrorCode().orElseThrow());
		assertEquals("detail=<hidden>", failure.getDetail().orElseThrow());
		assertEquals("constraint_<hidden>", failure.getConstraint().orElseThrow());
		assertTrue(failure.getSqlState().map(state -> state.startsWith("08")).orElse(false));
		assertTrue(failure.isTransient());
		assertTrue(failure.isUniqueConstraintViolation());
		assertSame(batch, failure.getCause());
		assertEquals("detail=0800", PostgresDialect.INSTANCE.databaseExceptionMetadata(batch).detail);
	}

	@Test
	void parameterDisplayFailuresDoNotReplaceTheStateOrUnmaskDriverEchoes() {
		RuntimeException displayFailure = new IllegalStateException("parameter display unavailable");
		Database database = Database.with(source(), DatabaseType.GENERIC).parameterRedactor((context, index, value) -> {
			throw displayFailure;
		}).build();
		StatementContext<?> context = StatementContext.with(Statement.of("state-overlap", "SELECT ?, ?"), database)
				.parameters(Optional.of(Parameters.secure("2350", "<hidden>")), "plain").build();
		SQLException driver = new SQLException("bound=2350 rejected", "23505");
		DatabaseException failure = Database.databaseExceptionWithStatementContext(context, driver.getMessage(), driver);
		assertEquals("23505", failure.getSqlState().orElseThrow());
		assertTrue(failure.getMessage().contains("bound=<hidden>"));
		assertFalse(failure.getMessage().contains("bound=2350"));
		assertTrue(Arrays.asList(failure.getSuppressed()).contains(displayFailure));
		assertTrue(failure.isUniqueConstraintViolation());
		assertSame(driver, failure.getCause());
	}

	@Test
	void realConstraintFailuresKeepTheirStateInThrownAndLoggedDiagnostics() {
		for (String value : List.of("2350", "23505")) {
			AtomicReference<StatementLog<?>> logged = new AtomicReference<>();
			Database database = Database.with(source(), DatabaseType.GENERIC).statementLogger(logged::set).build();
			database.query("CREATE TABLE secure_values (value VARCHAR(20) UNIQUE)").execute();
			database.query("INSERT INTO secure_values VALUES (:value)").bind("value", Parameters.secure(value)).execute();
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query("INSERT INTO secure_values VALUES (:value)")
					.bind("value", Optional.of(Parameters.secure(Optional.of(value), "<hidden>"))).execute());
			assertEquals("23505", failure.getSqlState().orElseThrow());
			assertTrue(failure.isUniqueConstraintViolation());
			assertTrue(failure.getMessage().contains("<hidden>"));
			DatabaseException loggedFailure = assertInstanceOf(DatabaseException.class, logged.get().getException().orElseThrow());
			assertEquals("23505", loggedFailure.getSqlState().orElseThrow());
			assertEquals(1L, database.query("SELECT COUNT(*) FROM secure_values").fetchObject(Long.class).orElseThrow());
		}
	}

	@Test
	void alreadyWrappedMapperFailureCanStillDriveAnExactSqlStateRetryPolicy() {
		AtomicInteger maps = new AtomicInteger();
		AtomicReference<DatabaseException> retried = new AtomicReference<>();
		DatabaseException mapperFailure = new DatabaseException("mapper bound=4000", new SQLException("raw bound=4000", "40001"), GenericDialect.INSTANCE);
		ResultSetMapper fallback = ResultSetMapper.withDefaultConfiguration();
		ResultSetMapper mapper = new ResultSetMapper() {
			@Override public <T> Optional<T> map(StatementContext<T> context, ResultSet rows, Class<T> type, InstanceProvider provider) throws SQLException {
				if (maps.incrementAndGet() == 1) throw mapperFailure;
				return fallback.map(context, rows, type, provider);
			}
		};
		Database database = Database.with(source(), DatabaseType.GENERIC).resultSetMapper(mapper).build();
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			retried.set(failure);
			return failure.getSqlState().map("40001"::equals).orElse(false);
		});
		TransactionRetryResult<String> result = database.transactionWithRetry(policy, () -> database.query("VALUES (CAST(:value AS VARCHAR(16)))")
				.bind("value", Parameters.secure("4000", "<hidden>")).fetchObject(String.class));
		assertEquals(2, result.getAttemptCount());
		assertEquals("4000", result.getValue().orElseThrow());
		assertEquals("40001", retried.get().getSqlState().orElseThrow());
		assertTrue(retried.get().getMessage().contains("mapper bound=<hidden>"));
		assertSame(mapperFailure, retried.get().getCause());
		assertEquals("raw bound=4000", mapperFailure.getCause().getMessage());
	}

	private static UnaryOperator<String> scrubber(String value) {
		return SecureParameterSupport.diagnosticScrubForParameters(List.of(Parameters.secure(value, "<hidden>"))).redactor();
	}
	private static PSQLException driver(String state, String value) {
		return new PSQLException("driver bound=" + value, state, new ServerErrorMessage() {
			@Override public String getSQLState() { return state; }
			@Override public String getDetail() { return "detail=" + value; }
			@Override public String getHint() { return "hint=" + value; }
			@Override public String getConstraint() { return "constraint_" + value; }
			@Override public String getTable() { return "table_" + value; }
			@Override public String getMessage() { return "message=" + value; }
		});
	}
	private static JDBCDataSource source() {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:sqlstate_redaction_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		return source;
	}
}
