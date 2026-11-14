/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class MetricsLifecycleRegressionTests {
	@Test
	void lazyAcquisitionAndBeginFailuresRetainTheirStatementAcquisitionWindow() {
		for (boolean beginFailure : new boolean[]{false, true}) {
			AtomicLong clock = new AtomicLong();
			SQLException original = new SQLException("lazy acquisition/begin failed", "HY000");
			Connection connection = proxy(Connection.class, (method, args) -> switch (method.getName()) {
				case "getAutoCommit" -> true;
				case "getTransactionIsolation" -> Connection.TRANSACTION_READ_COMMITTED;
				case "isReadOnly" -> false;
				case "setAutoCommit" -> { clock.set(150); throw original; }
				default -> throw new AssertionError(method.getName());
			});
			DataSource source = proxy(DataSource.class, (method, args) -> {
				if (method.getName().equals("getConnection")) {
					clock.set(100);
					if (!beginFailure) throw original;
					return connection;
				}
				throw new AssertionError(method.getName());
			});
			Database database = Database.with(source, DatabaseType.GENERIC).build();
			Transaction transaction = new Transaction(source, TransactionOptions.withReadOnly(null).build(),
					new MetricsCollectorDispatcher(null), DatabaseType.GENERIC);
			StatementConnections connections = new StatementConnections(source, () -> Optional.of(transaction), value -> fail("A transaction owns its connection"),
					DatabaseType.GENERIC, GenericDialect.INSTANCE);
			StatementContext<Void> context = StatementContext.with(Statement.of("lazy-timing", "VALUES (1)"), database).build();
			StatementExecution lease = new StatementExecution(context, connections, new MetricsCollectorDispatcher(null), clock::get);
			DatabaseException failure = assertThrows(DatabaseException.class, () -> lease.acquire("test lazy timing"));
			assertSame(original, failure.getCause());
			assertEquals(Duration.ofNanos(beginFailure ? 150 : 100), lease.acquisitionDuration());
			clock.set(200);
			assertNull(lease.close(null));
			assertEquals(Duration.ofNanos(200), lease.elapsedDuration());
			assertFalse(transaction.getConnectionLock().isLocked());
		}
	}

	@Test
	void publicLazyQueryAndStreamFailuresIncludeAcquisitionInTheirLogs() {
		for (String stage : List.of("acquire", "connection.setAutoCommit"))
			for (boolean stream : new boolean[]{false, true}) {
				Fixture fixture = new Fixture(DatabaseType.GENERIC);
				fixture.failAt = stage;
				fixture.failure = new SQLException("lazy begin failed", "HY000");
				assertThrows(DatabaseException.class, () -> fixture.database.transaction(() -> {
					if (stream) fixture.query().fetchStream(Integer.class, values -> values.toList());
					else fixture.query().fetchObject(Integer.class);
				}));
				assertEquals(1, fixture.logs.size());
				StatementLog<?> log = fixture.logs.get(0);
				Duration acquisition = log.getConnectionAcquisitionDuration().orElseThrow();
				assertEquals(acquisition, log.getTotalDuration());
				assertTrue(log.getElapsedDuration().orElseThrow().compareTo(acquisition) >= 0);
				assertTrue(log.getPreparationDuration().isEmpty());
				fixture.collector.assertPairs(1, 0, 1);
				assertEquals(0, fixture.collector.statementAcquisitionFailures);
				assertEquals(stage.equals("acquire") ? 1 : 0, fixture.collector.transactionAcquisitionFailures);
				assertEquals(1, fixture.collector.beginPhases.size());
			}
	}

	@Test
	void everyPhysicalBeginPhaseReportsSqlRuntimeAndErrorFailuresExactlyOnce() {
		for (MetricsCollector.PhysicalTransactionBeginFailurePhase phase : MetricsCollector.PhysicalTransactionBeginFailurePhase.values())
			for (Throwable original : List.of(new SQLException("begin SQL failure", "HY000"),
					new IllegalStateException("begin runtime failure"), new AssertionError("begin error"))) {
				Collector collector = new Collector();
				AtomicInteger acquisitions = new AtomicInteger();
				Connection connection = proxy(Connection.class, (method, args) -> {
					String selected = switch (phase) {
						case ACQUIRE_CONNECTION -> "never";
						case READ_INITIAL_AUTOCOMMIT -> "getAutoCommit";
						case READ_INITIAL_ISOLATION -> "getTransactionIsolation";
						case READ_INITIAL_READ_ONLY -> "isReadOnly";
						case SET_READ_ONLY -> "setReadOnly";
						case SET_AUTOCOMMIT_FALSE -> "setAutoCommit";
						case SET_ISOLATION -> "setTransactionIsolation";
					};
					if (method.getName().equals(selected)) throw original;
					return switch (method.getName()) {
						case "getAutoCommit" -> true;
						case "getTransactionIsolation" -> Connection.TRANSACTION_READ_COMMITTED;
						case "isReadOnly" -> false;
						case "setReadOnly", "setAutoCommit", "setTransactionIsolation" -> null;
						default -> throw new AssertionError(method.getName());
					};
				});
				DataSource source = proxy(DataSource.class, (method, args) -> {
					if (method.getName().equals("getConnection")) {
						acquisitions.incrementAndGet();
						if (phase == MetricsCollector.PhysicalTransactionBeginFailurePhase.ACQUIRE_CONNECTION) throw original;
						return connection;
					}
					throw new AssertionError(method.getName());
				});
				Transaction transaction = new Transaction(source, TransactionOptions.withIsolation(TransactionIsolation.SERIALIZABLE).readOnly(true).build(),
						new MetricsCollectorDispatcher(collector), DatabaseType.GENERIC);
				Throwable thrown = assertThrows(Throwable.class, transaction::getConnection);
				if (original instanceof Error) assertSame(original, thrown);
				else assertTrue(contains(thrown, original));
				assertSame(thrown, assertThrows(Throwable.class, transaction::getConnection), "A failed begin remains fenced");
				assertEquals(List.of(phase), collector.beginPhases);
				assertTrue(contains(collector.beginFailures.get(0), original));
				assertEquals(phase == MetricsCollector.PhysicalTransactionBeginFailurePhase.ACQUIRE_CONNECTION ? 1 : 0,
						collector.transactionAcquisitionFailures);
				assertEquals(1, acquisitions.get());
				assertTrue(transaction.didPhysicalTransactionBeginFail());
				assertFalse(transaction.didPhysicalTransactionBeginSuccessfully());
				assertFalse(transaction.getConnectionLock().isLocked());
			}
	}

	@Test
	void standaloneAcquisitionErrorsKeepTheirIdentityAndReportBothTerminalMetrics() {
		for (boolean stream : new boolean[]{false, true}) {
			Fixture fixture = new Fixture(DatabaseType.GENERIC);
			AssertionError original = new AssertionError("checkout failed");
			fixture.failAt = "acquire"; fixture.failure = original;
			assertSame(original, assertThrows(AssertionError.class, () -> {
				if (stream) fixture.query().fetchStream(Integer.class, values -> values.toList());
				else fixture.query().fetchObject(Integer.class);
			}));
			fixture.collector.assertPairs(1, 0, 1);
			assertEquals(1, fixture.collector.statementAcquisitionFailures);
			assertSame(original, fixture.collector.statementFailures.get(0));
			assertSame(original, fixture.logs.get(0).getException().orElseThrow().getCause());
			assertTrue(fixture.logs.get(0).getConnectionAcquisitionDuration().isPresent());
		}
	}

	@Test
	void queryAndStreamPairsStayBalancedForPreExecutionExecutionAndMappingFailures() {
		for (String stage : List.of("acquire", "connection.prepareStatement", "customize", "bind", "statement.executeQuery", "result.next", "map"))
			for (boolean stream : new boolean[]{false, true})
				for (Throwable original : List.of(new SQLException("operation failed", "HY000"), new AssertionError("operation error"))) {
					Fixture fixture = new Fixture(DatabaseType.GENERIC);
					fixture.failAt = stage; fixture.failure = original;
					Throwable thrown = assertThrows(Throwable.class, () -> {
						if (stream) fixture.query().fetchStream(Integer.class, values -> values.toList());
						else fixture.query().fetchList(Integer.class);
					});
					if (original instanceof Error) assertSame(original, thrown);
					else assertTrue(contains(thrown, original));
					fixture.collector.assertPairs(1, 0, 1);
					assertEquals(1, fixture.logs.size());
					assertEquals(1, fixture.collector.statementFailures.size());
				}
	}

	@Test
	void streamStatementPairsEndOnceAtExhaustionOrEarlyClosureAndCallbackFailuresStaySeparate() {
		for (String mode : List.of("exhaust", "early", "doubleClose", "callbackBeforeExhaustion", "callbackAfterExhaustion")) {
			Fixture fixture = new Fixture(DatabaseType.GENERIC);
			IllegalStateException callback = new IllegalStateException("callback failed");
			org.junit.jupiter.api.function.Executable operation = () -> fixture.query().fetchStream(Integer.class, values -> {
				assertEquals(1, fixture.collector.active.size());
				if (mode.equals("callbackBeforeExhaustion")) throw callback;
				if (mode.equals("doubleClose")) { values.close(); values.close(); }
				else if (mode.equals("early")) assertEquals(Optional.of(1), values.findFirst());
				else assertEquals(List.of(1), values.toList());
				if (!mode.equals("early")) assertEquals(0, fixture.collector.active.size());
				if (mode.equals("callbackAfterExhaustion")) throw callback;
				return null;
			});
			if (mode.startsWith("callback")) assertSame(callback, assertThrows(IllegalStateException.class, operation));
			else assertDoesNotThrow(operation);
			fixture.collector.assertPairs(1, 1, 0);
			assertEquals(1, fixture.collector.streamOutcomes.size());
			assertEquals(mode.startsWith("callback") ? MetricsCollector.StreamTerminalOutcome.CALLBACK_FAILURE
					: mode.equals("exhaust") ? MetricsCollector.StreamTerminalOutcome.COMPLETED_NORMALLY : MetricsCollector.StreamTerminalOutcome.EARLY_CLOSE,
					fixture.collector.streamOutcomes.get(0));
		}
	}

	@Test
	void retryAttemptsAndJoinedTransactionStatementsEachHaveOneBalancedPair() {
		Fixture retry = new Fixture(DatabaseType.GENERIC);
		retry.failAt = "statement.executeQuery"; retry.failure = new SQLException("retryable", "40001"); retry.failOnce = true;
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> true);
		TransactionRetryResult<Integer> retried = retry.database.transactionWithRetry(policy, () -> retry.query().fetchObject(Integer.class));
		assertEquals(Optional.of(1), retried.getValue());
		assertEquals(2, retried.getAttemptCount());
		assertEquals(1, retried.getFailures().size());
		retry.collector.assertPairs(2, 1, 1);

		Fixture joined = new Fixture(DatabaseType.GENERIC);
		joined.database.transaction(() -> {
			Transaction transaction = joined.database.currentTransaction().orElseThrow();
			joined.database.participate(transaction, () -> { assertEquals(Optional.of(1), joined.query().fetchObject(Integer.class)); });
			assertEquals(List.of(1), joined.query().fetchStream(Integer.class, values -> values.toList()));
		});
		joined.collector.assertPairs(2, 2, 0);
		assertEquals(1, joined.acquisitions);
		assertEquals(1, joined.commits);
		assertEquals(0, joined.rollbacks);
	}

	@Test
	void streamCleanupAndCommitFailuresAreContextualAndReportOneFailedStatement() {
		for (String stage : List.of("result.close", "statement.close", "parameter.cleanup", "connection.commit", "connection.setAutoCommit", "connection.close"))
			for (boolean exhaust : new boolean[]{false, true}) {
				Fixture fixture = new Fixture(DatabaseType.POSTGRESQL);
				SQLException original = new SQLException("private-stream-value-62978", "08006");
				fixture.failAt = stage; fixture.failure = original;
				// A restoration failure must occur after opening has successfully disabled autocommit.
				fixture.failAutoCommitRestoreOnly = true;
				DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.query("VALUES (CAST(:value AS VARCHAR(80)))")
						.bind("value", Parameters.secure("private-stream-value-62978")).fetchStream(String.class, values -> {
							if (exhaust) values.toList();
							return null;
						}));
				assertTrue(contains(thrown, original), stage);
				assertFalse(thrown.getMessage().contains("private-stream-value-62978"), stage);
				assertTrue(thrown.getMessage().contains("VALUES"), stage);
				fixture.collector.assertPairs(1, 0, 1);
				assertSame(thrown, fixture.logs.get(0).getException().orElseThrow(), stage);
				assertSame(thrown, fixture.collector.statementFailures.get(0), stage);
				assertEquals(1, fixture.closes, stage);
			}
	}

	@Test
	void streamCleanupErrorsRemainErrorsAndCallbackFailuresRetainPrimaryIdentity() {
		for (boolean exhaust : new boolean[]{false, true}) {
			Fixture fixture = new Fixture(DatabaseType.POSTGRESQL);
			AssertionError original = new AssertionError("commit error");
			fixture.failAt = "connection.commit"; fixture.failure = original;
			assertSame(original, assertThrows(AssertionError.class, () -> fixture.query().fetchStream(Integer.class, values -> {
				if (exhaust) values.toList();
				return null;
			})));
			fixture.collector.assertPairs(1, 0, 1);
			assertSame(original, fixture.collector.statementFailures.get(0));
			assertSame(original, fixture.logs.get(0).getException().orElseThrow().getCause());
		}
		for (Throwable callback : List.of(new IllegalStateException("callback"), new AssertionError("callback"))) {
			Fixture fixture = new Fixture(DatabaseType.GENERIC);
			SQLException cleanup = new SQLException("cleanup", "HY000");
			fixture.failAt = "result.close"; fixture.failure = cleanup;
			assertSame(callback, assertThrows(Throwable.class, () -> fixture.query().fetchStream(Integer.class, values -> {
				if (callback instanceof Error error) throw error;
				throw (RuntimeException) callback;
			})));
			assertArrayEquals(new Throwable[]{cleanup}, callback.getSuppressed());
			fixture.collector.assertPairs(1, 1, 0);
			assertEquals(List.of(MetricsCollector.StreamTerminalOutcome.CALLBACK_FAILURE), fixture.collector.streamOutcomes);
		}
	}

	@Test
	void aTransactionalStreamPublishesTheNormalizedCleanupFailureInItsOutcome() {
		Fixture fixture = new Fixture(DatabaseType.GENERIC);
		SQLException cleanup = new SQLException("cursor close failed", "HY000");
		fixture.failAt = "result.close"; fixture.failure = cleanup;
		TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.query().fetchStream(Integer.class, values -> values.toList());
			return Optional.empty();
		});
		RuntimeException thrown = outcome.getFailure().orElseThrow();
		assertInstanceOf(DatabaseException.class, thrown);
		assertSame(cleanup, thrown.getCause());
		assertSame(thrown, fixture.logs.get(0).getException().orElseThrow());
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		assertEquals(0, fixture.commits);
		assertEquals(1, fixture.rollbacks);
		fixture.collector.assertPairs(1, 0, 1);
	}

	@Test
	void statementLoggerFailureAfterClosureCannotEmitASecondTerminalStatementEvent() {
		for (boolean stream : new boolean[]{false, true}) {
			Fixture fixture = new Fixture(DatabaseType.GENERIC);
			IllegalStateException original = new IllegalStateException("logger failed");
			fixture.loggerFailure = original;
			Throwable thrown = assertThrows(Throwable.class, () -> {
				if (stream) fixture.query().fetchStream(Integer.class, values -> values.toList());
				else fixture.query().fetchList(Integer.class);
			});
			assertTrue(contains(thrown, original));
			fixture.collector.assertPairs(1, 1, 0);
		}
	}

	private static final class Collector implements MetricsCollector {
		private final Map<StatementContext<?>, Boolean> active = new IdentityHashMap<>();
		private final List<String> violations = new ArrayList<>();
		private final List<Throwable> statementFailures = new ArrayList<>();
		private final List<PhysicalTransactionBeginFailurePhase> beginPhases = new ArrayList<>();
		private final List<Throwable> beginFailures = new ArrayList<>();
		private final List<StreamTerminalOutcome> streamOutcomes = new ArrayList<>();
		private int attempts;
		private int successes;
		private int failures;
		private int statementAcquisitionFailures;
		private int transactionAcquisitionFailures;

		@Override public void willExecuteStatement(StatementContext<?> context) {
			if (this.active.put(context, true) != null) this.violations.add("Duplicate will");
			this.attempts++;
		}
		@Override public void didExecuteStatement(StatementContext<?> context, StatementLog<?> log, StatementResult result) {
			if (this.active.remove(context) == null) this.violations.add("Success without will");
			this.successes++;
		}
		@Override public void didFailToExecuteStatement(StatementContext<?> context, StatementLog<?> log, DatabaseType type, Throwable failure) {
			if (this.active.remove(context) == null) this.violations.add("Failure without will");
			this.failures++; this.statementFailures.add(failure);
		}
		@Override public void didFailToAcquireStatementConnection(StatementContext<?> context, DatabaseType type, Duration duration, Throwable failure) {
			this.statementAcquisitionFailures++;
		}
		@Override public void didFailToAcquireTransactionConnection(Transaction transaction, DatabaseType type, Duration duration, Throwable failure) {
			this.transactionAcquisitionFailures++;
		}
		@Override public void didFailToBeginPhysicalTransaction(Transaction transaction, TransactionIsolation isolation,
				PhysicalTransactionBeginFailurePhase phase, DatabaseType type, Throwable failure) {
			this.beginPhases.add(phase); this.beginFailures.add(failure);
		}
		@Override public void didCloseStream(StatementContext<?> context, StreamTerminalOutcome outcome, Long rows, Duration duration, Throwable failure) {
			this.streamOutcomes.add(outcome);
		}
		private void assertPairs(int attempts, int successes, int failures) {
			assertEquals(List.of(), this.violations);
			assertEquals(0, this.active.size());
			assertEquals(attempts, this.attempts);
			assertEquals(successes, this.successes);
			assertEquals(failures, this.failures);
		}
	}

	private static final class Fixture {
		private final Collector collector = new Collector();
		private final List<StatementLog<?>> logs = new ArrayList<>();
		private final Database database;
		private String failAt = "none";
		private Throwable failure;
		private Throwable loggerFailure;
		private boolean failOnce;
		private boolean failed;
		private boolean failAutoCommitRestoreOnly;
		private int acquisitions;
		private int closes;
		private int commits;
		private int rollbacks;

		private Fixture(DatabaseType type) {
			JDBCDataSource nativeSource = new JDBCDataSource();
			nativeSource.setUrl("jdbc:hsqldb:mem:metrics_lifecycle_" + UUID.randomUUID()); nativeSource.setUser("sa"); nativeSource.setPassword("");
			DataSource source = proxy(DataSource.class, (method, args) -> {
				if (method.getName().equals("getConnection")) {
					this.acquisitions++; failIf("acquire");
					return wrapConnection((Connection) invoke(nativeSource, method, args));
				}
				return invoke(nativeSource, method, args);
			});
			ResultSetMapper defaultMapper = ResultSetMapper.withDefaultConfiguration();
			PreparedStatementBinder defaultBinder = PreparedStatementBinder.withDefaultConfiguration();
			this.database = Database.with(source, type).metricsCollector(this.collector).statementLogger(log -> {
				this.logs.add(log);
				if (this.loggerFailure instanceof RuntimeException runtime) throw runtime;
				if (this.loggerFailure instanceof Error error) throw error;
			}).resultSetMapper(new ResultSetMapper() {
				@Override public <T> Optional<T> map(StatementContext<T> context, ResultSet rows, Class<T> target, InstanceProvider instances) throws SQLException {
					failChecked("map"); return defaultMapper.map(context, rows, target, instances);
				}
			}).preparedStatementBinder(new PreparedStatementBinder() {
				@Override public <T> void bindParameter(StatementContext<T> context, PreparedStatement statement, Integer index, Object parameter) throws SQLException {
					failChecked("bind"); defaultBinder.bindParameter(context, statement, index, parameter);
				}
			}).build();
		}
		private Query query() { return query("VALUES (CAST(:value AS INTEGER))").bind("value", 1); }
		private Query query(String sql) {
			return this.database.query(sql).customize((context, statement) -> {
				failChecked("customize");
				context.addCleanupOperation(() -> failChecked("parameter.cleanup"));
			});
		}
		private Connection wrapConnection(Connection connection) {
			return proxy(Connection.class, (method, args) -> {
				if (method.getName().equals("close")) {
					this.closes++;
					try { failIf("connection.close"); } finally { connection.close(); }
					return null;
				}
				if (method.getName().equals("abort")) return null;
				if (method.getName().equals("commit")) this.commits++;
				if (method.getName().equals("rollback")) this.rollbacks++;
				if (!(method.getName().equals("setAutoCommit") && this.failAutoCommitRestoreOnly && Boolean.FALSE.equals(args[0])))
					failIf("connection." + method.getName());
				Object result = invoke(connection, method, args);
				return result instanceof PreparedStatement statement ? wrapStatement(statement) : result;
			});
		}
		private PreparedStatement wrapStatement(PreparedStatement statement) {
			return proxy(PreparedStatement.class, (method, args) -> {
				if (method.getName().equals("close")) {
					try { failIf("statement.close"); } finally { statement.close(); }
					return null;
				}
				failIf("statement." + method.getName());
				Object result = invoke(statement, method, args);
				return result instanceof ResultSet rows ? wrapRows(rows) : result;
			});
		}
		private ResultSet wrapRows(ResultSet rows) {
			return proxy(ResultSet.class, (method, args) -> {
				if (method.getName().equals("close")) {
					try { failIf("result.close"); } finally { rows.close(); }
					return null;
				}
				failIf("result." + method.getName());
				return invoke(rows, method, args);
			});
		}
		private void failIf(String stage) throws Throwable {
			if (this.failAt.equals(stage) && (!this.failOnce || !this.failed)) {
				this.failed = true; throw this.failure;
			}
		}
		private void failChecked(String stage) throws SQLException {
			try { failIf(stage); }
			catch (SQLException | RuntimeException | Error failure) { throw failure; }
			catch (Throwable failure) { throw new AssertionError(failure); }
		}
	}

	private interface Call { Object run(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> call.run(method, args)));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
	private static boolean contains(Throwable root, Throwable value) {
		if (root == value) return true;
		if (root == null) return false;
		if (contains(root.getCause(), value)) return true;
		for (Throwable suppressed : root.getSuppressed()) if (contains(suppressed, value)) return true;
		return false;
	}
}
