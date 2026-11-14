/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */

package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static com.pyranid.TransactionCommitHardeningTests.invoke;
import static com.pyranid.TransactionCommitHardeningTests.proxy;
import static org.junit.jupiter.api.Assertions.*;

class TransactionCompletionDiagnosticTests {
	@Test
	void acknowledgedRollbackRetainsItsOutcomeWhenSuccessLoggingFails() {
		for (Throwable diagnostic : diagnostics()) {
			Fixture fixture = new Fixture(null);
			AtomicReference<TransactionCompletion> observed = new AtomicReference<>();
			try (var ignored = failingLogger("Transaction rolled back.", diagnostic)) {
				if (diagnostic instanceof Error) {
					assertSame(diagnostic, assertThrows(Error.class, () -> fixture.database.transactionWithOutcome(
							options(observed), () -> fixture.insertAndMarkRollbackOnly())));
				} else {
					TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(options(observed),
							() -> fixture.insertAndMarkRollbackOnly());
					assertSame(observed.get(), outcome.getCompletion());
					assertEquals(Optional.of("attempted"), outcome.getAttemptedValue());
				}
				assertTrue(Thread.currentThread().isInterrupted());
			} finally {
				Thread.interrupted();
			}
			assertEquals(TransactionResult.ROLLED_BACK, observed.get().getResult());
			assertSame(diagnostic, observed.get().getFailure().orElseThrow());
			assertEquals(0, fixture.commits.get());
			assertEquals(1, fixture.rollbacks.get());
			assertEquals(1, fixture.rollbackSuccesses.get());
			assertEquals(0, fixture.rollbackFailures.get());
			assertEquals(1, fixture.aborts.get());
			assertEquals(List.of(), fixture.ids());
		}
	}

	@Test
	void bodyFailureRetainsPrimaryIdentityAfterAcknowledgedRollbackDiagnosticsFail() {
		for (Throwable diagnostic : diagnostics()) {
			Fixture fixture = new Fixture(null);
			RuntimeException body = new IllegalArgumentException("body failure");
			TransactionOutcome<String> outcome;
			try (var ignored = failingLogger("Transaction rolled back.", diagnostic)) {
				outcome = fixture.database.transactionWithOutcome(TransactionOptions.withReadOnly(null).build(), () -> {
					fixture.insert();
					throw body;
				});
				assertTrue(Thread.currentThread().isInterrupted());
			} finally {
				Thread.interrupted();
			}
			assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
			assertSame(body, outcome.getFailure().orElseThrow());
			assertArrayEquals(new Throwable[] {diagnostic}, body.getSuppressed());
			assertEquals(1, fixture.rollbacks.get());
			assertEquals(1, fixture.aborts.get());
			assertEquals(List.of(), fixture.ids());
		}
	}

	@Test
	void serializationAbortRemainsKnownAfterAcknowledgedRollbackDiagnosticsFail() {
		Fixture fixture = new Fixture(new SQLException("serialization abort", "40001"));
		Throwable diagnostic = new IllegalStateException("rollback diagnostic");
		TransactionOutcome<String> outcome;
		try (var ignored = failingLogger("Transaction rolled back.", diagnostic)) {
			outcome = fixture.database.transactionWithOutcome(TransactionOptions.withReadOnly(null).build(), () -> {
				fixture.insert();
				return Optional.of("attempted");
			});
		}
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		DatabaseException failure = assertInstanceOf(DatabaseException.class, outcome.getFailure().orElseThrow());
		assertFalse(failure.isTransactionOutcomeIndeterminate());
		assertArrayEquals(new Throwable[] {diagnostic}, failure.getSuppressed());
		assertEquals(1, fixture.commits.get());
		assertEquals(1, fixture.rollbacks.get());
		assertEquals(1, fixture.aborts.get());
		assertEquals(List.of(), fixture.ids());
	}

	@Test
	void preDispatchCommitLoggingFailureDoesNotInventAnIssuedCommit() {
		for (Throwable diagnostic : diagnostics()) {
			Fixture fixture = new Fixture(null);
			AtomicReference<TransactionCompletion> observed = new AtomicReference<>();
			try (var ignored = failingLogger("Committing transaction...", diagnostic)) {
				if (diagnostic instanceof Error) {
					assertSame(diagnostic, assertThrows(Error.class, () -> fixture.database.transactionWithOutcome(
							options(observed), () -> {
								fixture.insert();
								return Optional.of("attempted");
							})));
				} else {
					TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(options(observed), () -> {
						fixture.insert();
						return Optional.of("attempted");
					});
					assertEquals(Optional.of("attempted"), outcome.getAttemptedValue());
				}
				assertTrue(Thread.currentThread().isInterrupted());
			} finally {
				Thread.interrupted();
			}
			assertEquals(TransactionResult.ROLLED_BACK, observed.get().getResult());
			assertSame(diagnostic, observed.get().getFailure().orElseThrow());
			assertEquals(0, fixture.commits.get());
			assertEquals(0, fixture.commitFailures.get());
			assertEquals(1, fixture.rollbacks.get());
			assertEquals(List.of(), fixture.ids());
		}
	}

	@Test
	void aDriverFailureAfterCommitDispatchRemainsUncertain() {
		for (Throwable failure : List.of(new IllegalStateException("driver commit"), new AssertionError("driver commit"))) {
			Fixture fixture = new Fixture(failure);
			AtomicReference<TransactionCompletion> observed = new AtomicReference<>();
			if (failure instanceof Error) {
				assertSame(failure, assertThrows(Error.class, () -> fixture.database.transactionWithOutcome(options(observed), () -> {
					fixture.insert();
					return Optional.of("attempted");
				})));
			} else {
				TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(options(observed), () -> {
					fixture.insert();
					return Optional.of("attempted");
				});
				assertEquals(Optional.of("attempted"), outcome.getAttemptedValue());
			}
			assertEquals(TransactionResult.IN_DOUBT, observed.get().getResult());
			assertSame(failure, observed.get().getFailure().orElseThrow());
			assertEquals(1, fixture.commits.get());
			assertEquals(1, fixture.commitFailures.get());
			assertEquals(1, fixture.rollbacks.get());
			assertEquals(1, fixture.aborts.get());
		}
	}

	private static TransactionOptions options(AtomicReference<TransactionCompletion> observed) {
		return TransactionOptions.withReadOnly(null).completionObserver(observed::set).build();
	}

	private static List<Throwable> diagnostics() {
		return List.of(new IllegalStateException("diagnostic", new InterruptedException("cancelled")),
				new AssertionError("diagnostic", new InterruptedException("cancelled")));
	}

	private interface LoggerScope extends AutoCloseable {
		@Override void close();
	}

	private static LoggerScope failingLogger(String message, Throwable diagnostic) {
		Logger logger = Logger.getLogger(Transaction.class.getName());
		Level previous = logger.getLevel();
		Handler handler = new Handler() {
			@Override public void publish(LogRecord record) {
				if (!message.equals(record.getMessage())) return;
				if (diagnostic instanceof Error error) throw error;
				throw (RuntimeException) diagnostic;
			}
			@Override public void flush() {}
			@Override public void close() {}
		};
		logger.addHandler(handler);
		logger.setLevel(Level.FINER);
		return () -> {
			logger.removeHandler(handler);
			logger.setLevel(previous);
		};
	}

	private static final class Fixture {
		final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger(), aborts = new AtomicInteger();
		final AtomicInteger rollbackSuccesses = new AtomicInteger(), rollbackFailures = new AtomicInteger(),
				commitFailures = new AtomicInteger();
		final Database database, inspection;

		Fixture(Throwable commitFailure) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:completion_diagnostic_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			inspection = Database.with(source, DatabaseType.GENERIC).build();
			inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object value = invoke(source, method, args);
				if (!(value instanceof Connection connection)) return value;
				return proxy(Connection.class, (jdbc, parameters) -> {
					if (jdbc.getName().equals("commit")) {
						commits.incrementAndGet();
						if (commitFailure != null) throw commitFailure;
					}
					if (jdbc.getName().equals("rollback") && (parameters == null || parameters.length == 0))
						rollbacks.incrementAndGet();
					if (jdbc.getName().equals("abort")) aborts.incrementAndGet();
					return invoke(connection, jdbc, parameters);
				});
			});
			database = Database.with(wrapped, DatabaseType.GENERIC).metricsCollector(new MetricsCollector() {
				@Override public void didRollbackPhysicalTransaction(Transaction transaction, DatabaseType databaseType,
						Duration duration) { rollbackSuccesses.incrementAndGet(); }
				@Override public void didFailToRollbackPhysicalTransaction(Transaction transaction, DatabaseType databaseType,
						Duration duration, Throwable failure) { rollbackFailures.incrementAndGet(); }
				@Override public void didFailToCommitPhysicalTransaction(Transaction transaction, DatabaseType databaseType,
						Duration duration, Throwable failure) { commitFailures.incrementAndGet(); }
			}).build();
		}

		void insert() { database.query("INSERT INTO work VALUES (1)").execute(); }
		Optional<String> insertAndMarkRollbackOnly() {
			insert();
			database.currentTransaction().orElseThrow().setRollbackOnly(true);
			return Optional.of("attempted");
		}
		List<Integer> ids() { return inspection.query("SELECT id FROM work").fetchList(Integer.class); }
	}
}
