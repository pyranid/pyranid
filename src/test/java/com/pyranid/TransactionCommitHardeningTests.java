/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */

package com.pyranid;


import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.TransactionState;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;

class TransactionCommitHardeningTests {
	@Test
	void commitAbortProofRequiresAnUnambiguousPrimaryAndEveryGraphEdge() {
		List<SQLException> uncertain = new ArrayList<>();
		for (String primary : new String[] {null, "", "08006", "08007", "40003", "HY000"}) {
			SQLException failure = new SQLException("primary", primary);
			failure.setNextException(new SQLException("serialization", "40001"));
			uncertain.add(failure);
		}
		for (String state : new String[] {null, "", "08006", "40003", "23505"}) {
			for (String edge : List.of("cause", "next", "suppressed")) {
				SQLException root = new SQLException("serialization", "40001");
				SQLException detail = new SQLException("uncertain", state);
				if (edge.equals("cause")) root.initCause(detail);
				else if (edge.equals("next")) root.setNextException(detail);
				else root.addSuppressed(detail);
				uncertain.add(root);
			}
		}
		uncertain.add(new SQLRecoverableException("transport", "40001"));
		uncertain.add(new SQLTransientConnectionException("transport", "40001"));
		uncertain.add(new SQLException("serialization", "40001", new IOException("transport")));
		for (SQLException failure : uncertain)
			assertFalse(
					TransactionCommitEvidence.provesSerializationAbort(
							failure, GenericDialect.INSTANCE));

		SQLException clean = new SQLException("serialization", "40001");
		SQLException shared = new SQLException("shared", "40001");
		clean.setNextException(shared);
		clean.initCause(shared);
		shared.setNextException(clean);
		assertTrue(
				TransactionCommitEvidence.provesSerializationAbort(clean, GenericDialect.INSTANCE));
		assertTrue(
				TransactionCommitEvidence.provesSerializationAbort(
						clean, PostgresDialect.INSTANCE));
		assertFalse(
				TransactionCommitEvidence.provesSerializationAbort(
						new SQLException("deadlock", "40001", 1205), SqlServerDialect.INSTANCE));
		assertFalse(
				TransactionCommitEvidence.provesSerializationAbort(
						new SQLException("serialization", "72000", 8177), OracleDialect.INSTANCE));
	}

	@Test
	void unreadableCommitEvidenceRetainsTheDriverFailureAndNeverProvesAbort() {
		for (String accessor : List.of("state", "cause", "next")) {
			IllegalStateException diagnostic = new IllegalStateException("unreadable " + accessor);
			SQLException failure =
					new SQLException("serialization", "40001") {
						@Override
						public String getSQLState() {
							if (accessor.equals("state")) throw diagnostic;
							return super.getSQLState();
						}

						@Override
						public Throwable getCause() {
							if (accessor.equals("cause")) throw diagnostic;
							return super.getCause();
						}

						@Override
						public SQLException getNextException() {
							if (accessor.equals("next")) throw diagnostic;
							return super.getNextException();
						}
					};
			assertFalse(
					TransactionCommitEvidence.provesSerializationAbort(
							failure, GenericDialect.INSTANCE));
			assertTrue(List.of(failure.getSuppressed()).contains(diagnostic));
		}
	}

	@Test
	void mixedCommitAcknowledgmentCannotReplayWorkThatActuallyPersisted() {
		for (DatabaseType type : List.of(DatabaseType.GENERIC, DatabaseType.POSTGRESQL)) {
			for (String primary : List.of("08006", "40003", "40001")) {
				SQLException failure = new SQLException("uncertain acknowledgment", primary);
				failure.setNextException(
						new SQLException("detail", primary.equals("40001") ? "08007" : "40001"));
				Fixture fixture = new Fixture(type, failure, true);
				AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger();
				AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
				RetryPolicy policy =
						RetryPolicy.ofMaxAttempts(
								2,
								RetryPolicy.Backoff.fixed(Duration.ZERO),
								ignored -> {
									policies.incrementAndGet();
									return true;
								});
				DatabaseException thrown =
						assertThrows(
								DatabaseException.class,
								() ->
										fixture.database.transactionWithRetry(
												policy,
												TransactionOptions.withReadOnly(null)
														.completionObserver(completion::set)
														.build(),
												() -> {
													attempts.incrementAndGet();
													fixture.insert(1);
												}));
				assertSame(failure, thrown.getCause());
				assertTrue(thrown.isSerializationFailure());
				assertEquals(TransactionResult.IN_DOUBT, completion.get().getResult());
				assertSame(thrown, completion.get().getFailure().orElseThrow());
				assertEquals(1, attempts.get());
				assertEquals(0, policies.get());
				assertEquals(1, fixture.commits.get());
				assertEquals(List.of(1), fixture.ids());
			}
		}
	}

	@Test
	void cleanSerializationAbortStillRetriesAfterSuccessfulRollback() {
		Fixture fixture =
				new Fixture(
						DatabaseType.GENERIC, new SQLException("serialization", "40001"), false);
		AtomicInteger attempts = new AtomicInteger();
		fixture.database.transactionWithRetry(
				RetryPolicy.ofMaxAttempts(
						2,
						RetryPolicy.Backoff.fixed(Duration.ZERO),
						DatabaseException::isSerializationFailure),
				() -> {
					attempts.incrementAndGet();
					fixture.insert(1);
				});
		assertEquals(2, attempts.get());
		assertEquals(2, fixture.commits.get());
		assertEquals(1, fixture.rollbacks.get());
		assertEquals(List.of(1), fixture.ids());
	}

	@Test
	void successfulPhysicalCommitSurvivesAFailingSuccessLogger() {
		Fixture fixture = new Fixture(DatabaseType.GENERIC, null, false);
		AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
		RuntimeException diagnostic = new IllegalStateException("success logger failed");
		Logger logger = Logger.getLogger(Transaction.class.getName());
		Level previous = logger.getLevel();
		Handler handler =
				new Handler() {
					@Override
					public void publish(LogRecord record) {
						if ("Transaction committed.".equals(record.getMessage())) throw diagnostic;
					}

					@Override
					public void flush() {}

					@Override
					public void close() {}
				};
		logger.addHandler(handler);
		logger.setLevel(Level.FINER);
		try {
			assertSame(
					diagnostic,
					assertThrows(
							IllegalStateException.class,
							() ->
									fixture.database.transaction(
											TransactionOptions.withReadOnly(null)
													.completionObserver(completion::set)
													.build(),
											() -> fixture.insert(1))));
		} finally {
			logger.removeHandler(handler);
			logger.setLevel(previous);
		}
		assertEquals(TransactionResult.COMMITTED, completion.get().getResult());
		assertSame(diagnostic, completion.get().getFailure().orElseThrow());
		assertEquals(1, fixture.commits.get());
		assertEquals(0, fixture.rollbacks.get());
		assertEquals(List.of(1), fixture.ids());
	}

	@Test
	void aSneakySerializationExceptionFromSuccessLoggingCannotChangeTheCommittedOutcome() {
		Fixture fixture = new Fixture(DatabaseType.GENERIC, null, false);
		AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
		AtomicInteger policies = new AtomicInteger();
		SQLException diagnostic = new SQLException("success logger diagnostic", "40001");
		Logger logger = Logger.getLogger(Transaction.class.getName());
		Level previous = logger.getLevel();
		Handler handler =
				new Handler() {
					@Override
					public void publish(LogRecord record) {
						if ("Transaction committed.".equals(record.getMessage()))
							TransactionCommitHardeningTests.<RuntimeException>raise(diagnostic);
					}

					@Override
					public void flush() {}

					@Override
					public void close() {}
				};
		logger.addHandler(handler);
		logger.setLevel(Level.FINER);
		RuntimeException thrown;
		try {
			thrown =
					assertThrows(
							RuntimeException.class,
							() ->
									fixture.database.transactionWithRetry(
											RetryPolicy.ofMaxAttempts(
													2,
													RetryPolicy.Backoff.fixed(Duration.ZERO),
													ignored -> {
														policies.incrementAndGet();
														return true;
													}),
											TransactionOptions.withReadOnly(null)
													.completionObserver(completion::set)
													.build(),
											() -> fixture.insert(1)));
		} finally {
			logger.removeHandler(handler);
			logger.setLevel(previous);
		}
		assertSame(diagnostic, thrown.getCause());
		assertEquals(0, policies.get());
		assertEquals(TransactionResult.COMMITTED, completion.get().getResult());
		assertSame(thrown, completion.get().getFailure().orElseThrow());
		assertEquals(1, fixture.commits.get());
		assertEquals(0, fixture.rollbacks.get());
		assertEquals(List.of(1), fixture.ids());
	}

	@SuppressWarnings("unchecked")
	private static <E extends Throwable> void raise(Throwable failure) throws E {
		throw (E) failure;
	}

	static final class Fixture {
		final Database database, inspection;
		final AtomicInteger commits = new AtomicInteger(),
				rollbacks = new AtomicInteger(),
				validations = new AtomicInteger();
		final AtomicInteger validationResultCloses = new AtomicInteger(),
				validationStatementCloses = new AtomicInteger();
		Throwable validationFailure,
				validationFetchFailure,
				validationResultCloseFailure,
				validationStatementCloseFailure;
		boolean validationEmpty, validationExtraRow, validationNull;
		Integer validationValue;
		SQLException rollbackFailure, releaseFailure;
		TransactionState state;
		private final AtomicBoolean armed = new AtomicBoolean(true);
		private final DatabaseType type;
		private final SQLException commitFailure;
		private final boolean persistBeforeFailure;

		Fixture(DatabaseType type, SQLException commitFailure, boolean persistBeforeFailure) {
			this.type = type;
			this.commitFailure = commitFailure;
			this.persistBeforeFailure = persistBeforeFailure;
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:commit_hardening_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			inspection = Database.with(source, DatabaseType.GENERIC).build();
			inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
			if (type == DatabaseType.POSTGRESQL) state = TransactionState.OPEN;
			DataSource wrapped =
					proxy(
							DataSource.class,
							(method, args) -> {
								Object result = invoke(source, method, args);
								return result instanceof Connection connection
										? wrapConnection(connection)
										: result;
							});
			database = Database.with(wrapped, type).build();
		}

		private Connection wrapConnection(Connection connection) {
			Class<? extends Connection> api =
					state == null ? Connection.class : BaseConnection.class;
			AtomicBoolean rolledBackSavepoint = new AtomicBoolean();
			return proxy(
					api,
					(call, values) -> {
						String name = call.getName();
						if (name.equals("getTransactionState")) return state;
						if (name.equals("commit")) {
							commits.incrementAndGet();
							if (commitFailure != null && armed.compareAndSet(true, false)) {
								if (persistBeforeFailure) invoke(connection, call, values);
								throw commitFailure;
							}
						}
						if (name.equals("rollback") && (values == null || values.length == 0)) {
							rollbacks.incrementAndGet();
							if (rollbackFailure != null) throw rollbackFailure;
						}
						if (name.equals("releaseSavepoint") && releaseFailure != null)
							throw releaseFailure;
						// PostgreSQL retains a savepoint after ROLLBACK TO; HSQLDB invalidates its
						// JDBC savepoint.
						if (type == DatabaseType.POSTGRESQL
								&& name.equals("releaseSavepoint")
								&& rolledBackSavepoint.getAndSet(false)) return null;
						Object value = invoke(connection, call, values);
						if (name.equals("rollback") && values != null && values.length == 1)
							rolledBackSavepoint.set(true);
						return name.equals("createStatement")
										&& value instanceof Statement statement
								? wrapStatement(statement)
								: value;
					});
		}

		private Statement wrapStatement(Statement statement) {
			AtomicBoolean validating = new AtomicBoolean();
			return proxy(
					Statement.class,
					(jdbc, parameters) -> {
						if (jdbc.getName().equals("executeQuery")
								&& "SELECT 1".equals(parameters[0])) {
							validating.set(true);
							validations.incrementAndGet();
							if (validationFailure != null) throw validationFailure;
							Object[] hsqlParameters = parameters.clone();
							hsqlParameters[0] = "VALUES(1)";
							return wrapValidationResult(
									(ResultSet) invoke(statement, jdbc, hsqlParameters));
						}
						if (jdbc.getName().equals("close") && validating.get()) {
							validationStatementCloses.incrementAndGet();
							Object closed = invoke(statement, jdbc, parameters);
							if (validationStatementCloseFailure != null)
								throw validationStatementCloseFailure;
							return closed;
						}
						return invoke(statement, jdbc, parameters);
					});
		}

		private ResultSet wrapValidationResult(ResultSet resultSet) {
			AtomicInteger nextCalls = new AtomicInteger();
			return proxy(
					ResultSet.class,
					(read, columns) -> {
						String method = read.getName();
						if (method.equals("next")) {
							if (validationFetchFailure != null) throw validationFetchFailure;
							if (validationEmpty) return false;
							if (nextCalls.incrementAndGet() == 2 && validationExtraRow) return true;
						}
						if (method.equals("getInt") && validationValue != null)
							return validationValue;
						if (method.equals("wasNull") && validationNull) return true;
						if (method.equals("close")) {
							validationResultCloses.incrementAndGet();
							Object closed = invoke(resultSet, read, columns);
							if (validationResultCloseFailure != null)
								throw validationResultCloseFailure;
							return closed;
						}
						return invoke(resultSet, read, columns);
					});
		}

		void insert(int id) {
			database.query("INSERT INTO work VALUES (:id)").bind("id", id).execute();
		}

		List<Integer> ids() {
			return inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class);
		}
	}

	@FunctionalInterface
	interface Call {
		Object run(Method method, Object[] args) throws Throwable;
	}

	static <T> T proxy(Class<T> type, Call call) {
		return type.cast(
				Proxy.newProxyInstance(
						type.getClassLoader(),
						new Class<?>[] {type},
						(proxy, method, args) -> call.run(method, args)));
	}

	static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try {
			return method.invoke(target, args);
		} catch (InvocationTargetException failure) {
			throw failure.getCause();
		}
	}
}
