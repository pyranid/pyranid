/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
@ThreadSafe
public class MetricsCollectorTests {
	@Test
	public void defaultAndNullCollectorsAreDisabled() {
		Database defaultDatabase = Database.withDataSource(createInMemoryDataSource("metrics_default")).build();
		Database nullDatabase = Database.withDataSource(createInMemoryDataSource("metrics_null"))
				.metricsCollector(null)
				.build();

		Assertions.assertSame(MetricsCollector.disabledInstance(), defaultDatabase.getMetricsCollector());
		Assertions.assertSame(MetricsCollector.disabledInstance(), nullDatabase.getMetricsCollector());
	}

	@Test
	public void statementResultsValidateAndBatchSentinelsAreNotSummed() {
		Assertions.assertSame(StatementResult.empty(), StatementResult.empty());
		Assertions.assertEquals(3L, StatementResult.ofRowsReturned(3L).rowsReturned());
		Assertions.assertEquals(5L, StatementResult.ofRowsAffected(5L).rowsAffected());
		Assertions.assertThrows(IllegalArgumentException.class, () -> new StatementResult(-1L, null));
		Assertions.assertThrows(IllegalArgumentException.class, () -> new StatementResult(null, -1L));

		Assertions.assertEquals(6L, Database.sumBatchUpdateCounts(List.of(1L, 2L, 3L)));
		Assertions.assertNull(Database.sumBatchUpdateCounts(List.of(1L, (long) Statement.SUCCESS_NO_INFO)));
		Assertions.assertNull(Database.sumBatchUpdateCounts(List.of(1L, (long) Statement.EXECUTE_FAILED)));
		Assertions.assertNull(Database.sumBatchUpdateCounts(List.of(Long.MAX_VALUE, 1L)));
	}

	@Test
	public void inMemoryCollectorCountsStatementsAndTransactionOutcomes() {
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = Database.withDataSource(createInMemoryDataSource("metrics_snapshot"))
				.metricsCollector(metricsCollector)
				.build();

		database.query("CREATE TABLE t (id INT)").execute();

		metricsCollector.reset();
		database.query("INSERT INTO t VALUES (1)").execute();

		MetricsCollector.Snapshot snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.statementsExecuted());
		Assertions.assertEquals(1L, snapshot.connectionsAcquiredStatementScope());
		Assertions.assertEquals(0L, snapshot.connectionReleaseFailuresStatementScope());
		Assertions.assertEquals(0L, snapshot.physicalTransactionsBegun());

		metricsCollector.reset();
		database.transaction(() -> database.query("INSERT INTO t VALUES (2)").execute());

		snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.transactionClosuresEntered());
		Assertions.assertEquals(1L, snapshot.transactionClosuresExited());
		Assertions.assertEquals(1L, snapshot.transactionClosuresCommitted());
		Assertions.assertEquals(0L, snapshot.transactionClosuresRolledBack());
		Assertions.assertEquals(1L, snapshot.physicalTransactionsBegun());
		Assertions.assertEquals(1L, snapshot.physicalTransactionsCommitted());
		Assertions.assertEquals(1L, snapshot.connectionsAcquiredTransactionScope());

		metricsCollector.reset();
		database.transaction(() -> {
			// No database work, so no physical transaction starts.
		});

		snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.transactionClosuresNoPhysical());
		Assertions.assertEquals(0L, snapshot.physicalTransactionsBegun());

		metricsCollector.reset();
		Assertions.assertThrows(RuntimeException.class, () -> database.transaction(() -> {
			database.query("INSERT INTO t VALUES (3)").execute();
			throw new RuntimeException("rollback");
		}));

		snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.transactionClosuresRolledBack());
		Assertions.assertEquals(1L, snapshot.physicalTransactionsRolledBack());
	}

	@Test
	public void streamingTerminalOutcomesAreDeferredUntilCallbackCompletion() {
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = Database.withDataSource(createInMemoryDataSource("metrics_stream"))
				.metricsCollector(metricsCollector)
				.build();

		database.query("CREATE TABLE t (id INT)").execute();
		database.query("INSERT INTO t VALUES (1)").execute();
		database.query("INSERT INTO t VALUES (2)").execute();

		metricsCollector.reset();
		List<Integer> rows = database.query("SELECT id FROM t ORDER BY id")
				.fetchStream(Integer.class, stream -> stream.collect(Collectors.toList()));

		Assertions.assertEquals(List.of(1, 2), rows);
		MetricsCollector.Snapshot snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.streamsOpened());
		Assertions.assertEquals(1L, snapshot.streamsClosedNormally());
		Assertions.assertEquals(0L, snapshot.streamsEarlyClosed());

		metricsCollector.reset();
		rows = database.query("SELECT id FROM t ORDER BY id")
				.fetchStream(Integer.class, stream -> stream.limit(1).collect(Collectors.toList()));

		Assertions.assertEquals(List.of(1), rows);
		snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.streamsEarlyClosed());
		Assertions.assertEquals(0L, snapshot.streamsClosedNormally());

		metricsCollector.reset();
		Assertions.assertThrows(IllegalStateException.class, () -> database.query("SELECT id FROM t ORDER BY id")
				.fetchStream(Integer.class, stream -> {
					stream.findFirst();
					throw new IllegalStateException("callback failure");
				}));

		snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.streamsCallbackFailed());
		Assertions.assertEquals(0L, snapshot.streamsEarlyClosed());
	}

	@Test
	public void openFailureCloseOutcomeDoesNotDoubleCountInMemoryOpenFailures() {
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = Database.withDataSource(createInMemoryDataSource("metrics_open_failure_counter")).build();
		StatementContext<?> statementContext = StatementContext.with(com.pyranid.Statement.of("test", "SELECT 1"), database).build();

		metricsCollector.didFailToOpenStream(statementContext, Duration.ZERO, new RuntimeException("open"));
		metricsCollector.didCloseStream(statementContext, MetricsCollector.StreamTerminalOutcome.OPEN_FAILURE, 0L, Duration.ZERO, null);

		Assertions.assertEquals(1L, snapshot(metricsCollector).streamsOpenFailures());
	}

	@Test
	public void iterationFailureStreamOutcomeIsRecordedWhenResultSetThrows() {
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		AtomicInteger nextCalls = new AtomicInteger();
		Database database = Database.withDataSource(iterationFailingDataSource("metrics_iteration_failure", nextCalls))
				.metricsCollector(metricsCollector)
				.build();

		database.query("CREATE TABLE t (id INT)").execute();
		database.query("INSERT INTO t VALUES (1)").execute();
		metricsCollector.reset();

		Assertions.assertThrows(DatabaseException.class, () -> database.query("SELECT id FROM t")
				.fetchStream(Integer.class, stream -> stream.collect(Collectors.toList())));

		MetricsCollector.Snapshot snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(1, nextCalls.get());
		Assertions.assertEquals(1L, snapshot.streamsOpened());
		Assertions.assertEquals(1L, snapshot.streamsIterationFailed());
		Assertions.assertEquals(0L, snapshot.streamsCallbackFailed());
		Assertions.assertEquals(0L, snapshot.streamsClosedNormally());
		Assertions.assertEquals(0L, snapshot.streamsEarlyClosed());
		Assertions.assertEquals(1L, snapshot.statementsFailed());
	}

	@Test
	public void mapperFailureStreamOutcomeIsRecordedAsIterationFailure() {
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		AtomicReference<StatementLog<?>> logRef = new AtomicReference<>();
		RuntimeException mapperFailure = new RuntimeException("mapper failure");
		ResultSetMapper throwingMapper = new ResultSetMapper() {
			@NonNull
			@Override
			public <T> Optional<T> map(@NonNull StatementContext<T> statementContext,
																 @NonNull ResultSet resultSet,
																 @NonNull Class<T> resultSetRowType,
																 @NonNull InstanceProvider instanceProvider) {
				throw mapperFailure;
			}
		};
		Database database = Database.withDataSource(createInMemoryDataSource("metrics_mapper_iteration_failure"))
				.metricsCollector(metricsCollector)
				.statementLogger(logRef::set)
				.resultSetMapper(throwingMapper)
				.build();

		database.query("CREATE TABLE t (id INT)").execute();
		database.query("INSERT INTO t VALUES (1)").execute();
		metricsCollector.reset();
		logRef.set(null);

		RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, () -> database.query("SELECT id FROM t")
				.fetchStream(Integer.class, stream -> stream.collect(Collectors.toList())));

		Assertions.assertSame(mapperFailure, thrown);
		MetricsCollector.Snapshot snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.streamsOpened());
		Assertions.assertEquals(1L, snapshot.streamsIterationFailed());
		Assertions.assertEquals(0L, snapshot.streamsCallbackFailed());
		Assertions.assertEquals(0L, snapshot.streamsClosedNormally());
		Assertions.assertEquals(0L, snapshot.streamsEarlyClosed());
		Assertions.assertEquals(1L, snapshot.statementsFailed());

		StatementLog<?> statementLog = logRef.get();
		Assertions.assertNotNull(statementLog, "Expected statement logger to receive the failed stream statement");
		Assertions.assertSame(mapperFailure, statementLog.getException().orElse(null));
	}

	@Test
	public void collectorFailuresDoNotAffectQueriesTransactionsOrStreams() {
		Database database = Database.withDataSource(createInMemoryDataSource("metrics_failure_containment"))
				.metricsCollector(throwingMetricsCollector())
				.build();

		Assertions.assertDoesNotThrow(() -> {
			database.query("CREATE TABLE t (id INT)").execute();
			database.transaction(() -> database.query("INSERT INTO t VALUES (1)").execute());
			List<Integer> rows = database.query("SELECT id FROM t")
					.fetchStream(Integer.class, stream -> stream.collect(Collectors.toList()));
			Assertions.assertEquals(List.of(1), rows);
		});
	}

	@Test
	public void databaseTypeWarmingFailureDoesNotFailSuccessfulStatement() {
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = Database.withDataSource(metadataFailingDataSource("metrics_warm_type_statement"))
				.metricsCollector(metricsCollector)
				.build();

		Integer value = database.query("SELECT 1 FROM (VALUES (0)) AS t(x)")
				.fetchObject(Integer.class)
				.orElseThrow();

		MetricsCollector.Snapshot snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(Integer.valueOf(1), value);
		Assertions.assertEquals(1L, snapshot.statementsExecuted());
		Assertions.assertEquals(0L, snapshot.statementsFailed());
	}

	@Test
	public void databaseTypeWarmingFailureDoesNotFailSuccessfulStream() {
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = Database.withDataSource(metadataFailingDataSource("metrics_warm_type_stream"))
				.metricsCollector(metricsCollector)
				.build();

		List<Integer> rows = database.query("SELECT id FROM (VALUES (1), (2)) AS t(id) ORDER BY id")
				.fetchStream(Integer.class, stream -> stream.collect(Collectors.toList()));

		MetricsCollector.Snapshot snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(List.of(1, 2), rows);
		Assertions.assertEquals(1L, snapshot.streamsOpened());
		Assertions.assertEquals(0L, snapshot.streamsOpenFailures());
		Assertions.assertEquals(1L, snapshot.streamsClosedNormally());
		Assertions.assertEquals(1L, snapshot.statementsExecuted());
		Assertions.assertEquals(0L, snapshot.statementsFailed());
	}

	@Test
	public void commitFailureFollowedByRollbackSuccessRecordsFailedClosureAndInDoubtHookResult() {
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		AtomicInteger rollbacks = new AtomicInteger();
		AtomicReference<TransactionResult> transactionResult = new AtomicReference<>();
		Database database = Database.withDataSource(commitFailingDataSource("metrics_commit_failure", rollbacks))
				.metricsCollector(metricsCollector)
				.build();

		database.query("CREATE TABLE t (id INT)").execute();
		metricsCollector.reset();

		Assertions.assertThrows(DatabaseException.class, () ->
				database.transaction(() -> {
					database.currentTransaction().orElseThrow().addPostTransactionOperation(transactionResult::set);
					database.query("INSERT INTO t VALUES (1)").execute();
				}));

		MetricsCollector.Snapshot snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(TransactionResult.IN_DOUBT, transactionResult.get());
		Assertions.assertEquals(1, rollbacks.get());
		Assertions.assertEquals(1L, snapshot.physicalTransactionsCommitFailed());
		Assertions.assertEquals(1L, snapshot.physicalTransactionsRolledBack());
		Assertions.assertEquals(0L, snapshot.transactionClosuresRolledBack());
		Assertions.assertEquals(1L, snapshot.transactionClosuresFailed());
	}

	@Test
	public void connectionReleaseFailuresAreCountedForStatementAndTransactionScopes() {
		MetricsCollector statementMetricsCollector = MetricsCollector.inMemoryInstance();
		Database statementDatabase = Database.withDataSource(closeFailingDataSource("metrics_statement_release_failure"))
				.metricsCollector(statementMetricsCollector)
				.build();

		Assertions.assertThrows(DatabaseException.class, () -> statementDatabase.query("CREATE TABLE t (id INT)").execute());
		Assertions.assertEquals(1L, snapshot(statementMetricsCollector).connectionReleaseFailuresStatementScope());

		MetricsCollector transactionMetricsCollector = MetricsCollector.inMemoryInstance();
		Database transactionDatabase = Database.withDataSource(closeFailingDataSource("metrics_transaction_release_failure"))
				.metricsCollector(transactionMetricsCollector)
				.build();

		Assertions.assertThrows(DatabaseException.class, () -> transactionDatabase.transaction(() ->
				transactionDatabase.query("CREATE TABLE t (id INT)").execute()));
		Assertions.assertEquals(1L, snapshot(transactionMetricsCollector).connectionReleaseFailuresTransactionScope());
	}

	@Test
	public void savepointEventsAreCounted() {
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = Database.withDataSource(createInMemoryDataSource("metrics_savepoints"))
				.metricsCollector(metricsCollector)
				.build();

		database.query("CREATE TABLE t (id INT)").execute();
		metricsCollector.reset();

		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			transaction.withSavepoint(() -> database.query("INSERT INTO t VALUES (1)").execute());
			Assertions.assertThrows(RuntimeException.class, () -> transaction.withSavepoint(() -> {
				database.query("INSERT INTO t VALUES (2)").execute();
				throw new RuntimeException("rollback to savepoint");
			}));
		});

		MetricsCollector.Snapshot snapshot = snapshot(metricsCollector);
		Assertions.assertEquals(2L, snapshot.savepointsCreated());
		Assertions.assertEquals(1L, snapshot.savepointsRolledBack());
		Assertions.assertEquals(1L, snapshot.savepointsReleased());
	}

	@Test
	public void statementResultSeparatesReturnedAndAffectedRows() {
		RecordingMetricsCollector metricsCollector = new RecordingMetricsCollector();
		Database database = Database.withDataSource(createInMemoryDataSource("metrics_statement_result"))
				.metricsCollector(metricsCollector)
				.build();

		database.query("CREATE TABLE t (id INT)").execute();
		metricsCollector.results.clear();

		database.query("INSERT INTO t VALUES (1)").execute();
		database.query("SELECT id FROM t").fetchList(Integer.class);

		Assertions.assertEquals(2, metricsCollector.results.size());
		Assertions.assertEquals(1L, metricsCollector.results.get(0).rowsAffected());
		Assertions.assertNull(metricsCollector.results.get(0).rowsReturned());
		Assertions.assertEquals(1L, metricsCollector.results.get(1).rowsReturned());
		Assertions.assertNull(metricsCollector.results.get(1).rowsAffected());
	}

	@Test
	public void transactionCallbacksReceiveDatabaseTypeSnapshotWithoutTransactionPublicAccessor() throws Exception {
		RecordingMetricsCollector metricsCollector = new RecordingMetricsCollector();
		Database database = Database.withDataSource(createInMemoryDataSource("metrics_database_type"))
				.databaseType(DatabaseType.POSTGRESQL)
				.metricsCollector(metricsCollector)
				.build();

		database.query("CREATE TABLE t (id INT)").execute();
		database.transaction(() -> database.query("INSERT INTO t VALUES (1)").execute());

		Assertions.assertEquals(DatabaseType.POSTGRESQL, metricsCollector.transactionDatabaseType.get());
		Assertions.assertThrows(NoSuchMethodException.class, () -> Transaction.class.getMethod("getDatabaseType"));
	}

	private static MetricsCollector.Snapshot snapshot(@NonNull MetricsCollector metricsCollector) {
		return metricsCollector.snapshot().orElseThrow();
	}

	@NonNull
	private static DataSource createInMemoryDataSource(@NonNull String databaseName) {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s;sql.syntax_pgs=true", databaseName));
		dataSource.setUser("SA");
		return dataSource;
	}

	@NonNull
	private static MetricsCollector throwingMetricsCollector() {
		return (MetricsCollector) Proxy.newProxyInstance(
				MetricsCollector.class.getClassLoader(),
				new Class<?>[]{MetricsCollector.class},
				(proxy, method, args) -> {
					if (method.getDeclaringClass() == Object.class)
						return switch (method.getName()) {
							case "toString" -> "throwingMetricsCollector";
							case "hashCode" -> System.identityHashCode(proxy);
							case "equals" -> proxy == args[0];
							default -> throw new UnsupportedOperationException(method.getName());
						};

					throw new AssertionError("metrics failure: " + method.getName());
				});
	}

	@NonNull
	private static DataSource closeFailingDataSource(@NonNull String databaseName) {
		return wrappingDataSource(databaseName, connection -> proxyConnection(connection, (delegate, method, args) -> {
			if ("close".equals(method.getName())) {
				try {
					invoke(delegate, method, args);
				} catch (Throwable ignored) {
					// The synthetic close failure below is the behavior under test.
				}
				throw new SQLException("close failed");
			}

			return invoke(delegate, method, args);
		}));
	}

	@NonNull
	private static DataSource commitFailingDataSource(@NonNull String databaseName,
																										@NonNull AtomicInteger rollbacks) {
		return wrappingDataSource(databaseName, connection -> proxyConnection(connection, (delegate, method, args) -> {
			if ("commit".equals(method.getName()))
				throw new SQLException("commit failed", "40001");

			if ("rollback".equals(method.getName()) && (args == null || args.length == 0))
				rollbacks.incrementAndGet();

			return invoke(delegate, method, args);
		}));
	}

	@NonNull
	private static DataSource metadataFailingDataSource(@NonNull String databaseName) {
		return wrappingDataSource(databaseName, connection -> proxyConnection(connection, (delegate, method, args) -> {
			if ("getMetaData".equals(method.getName()))
				throw new SQLException("metadata unavailable");

			return invoke(delegate, method, args);
		}));
	}

	@NonNull
	private static DataSource iterationFailingDataSource(@NonNull String databaseName,
																											 @NonNull AtomicInteger nextCalls) {
		return wrappingDataSource(databaseName, connection -> proxyConnection(connection, (delegate, method, args) -> {
			Object result = invoke(delegate, method, args);

			if ("prepareStatement".equals(method.getName()) && result instanceof PreparedStatement preparedStatement)
				return proxyPreparedStatement(preparedStatement, (preparedStatementDelegate, preparedStatementMethod, preparedStatementArgs) -> {
					Object preparedStatementResult = invoke(preparedStatementDelegate, preparedStatementMethod, preparedStatementArgs);

					if ("executeQuery".equals(preparedStatementMethod.getName()) && preparedStatementResult instanceof ResultSet resultSet)
						return proxyResultSet(resultSet, (resultSetDelegate, resultSetMethod, resultSetArgs) -> {
							if ("next".equals(resultSetMethod.getName())) {
								nextCalls.incrementAndGet();
								throw new SQLException("iteration failed");
							}

							return invoke(resultSetDelegate, resultSetMethod, resultSetArgs);
						});

					return preparedStatementResult;
				});

			return result;
		}));
	}

	@NonNull
	private static DataSource wrappingDataSource(@NonNull String databaseName,
																							 @NonNull ConnectionWrapper connectionWrapper) {
		return new WrappingDataSource(createInMemoryDataSource(databaseName), connectionWrapper);
	}

	@NonNull
	private static Connection proxyConnection(@NonNull Connection delegate,
																						@NonNull ConnectionInvocation invocation) {
		return (Connection) Proxy.newProxyInstance(
				Connection.class.getClassLoader(),
				new Class<?>[]{Connection.class},
				(proxy, method, args) -> invocation.invoke(delegate, method, args));
	}

	@NonNull
	private static PreparedStatement proxyPreparedStatement(@NonNull PreparedStatement delegate,
																												 @NonNull PreparedStatementInvocation invocation) {
		return (PreparedStatement) Proxy.newProxyInstance(
				PreparedStatement.class.getClassLoader(),
				new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> invocation.invoke(delegate, method, args));
	}

	@NonNull
	private static ResultSet proxyResultSet(@NonNull ResultSet delegate,
																					@NonNull ResultSetInvocation invocation) {
		return (ResultSet) Proxy.newProxyInstance(
				ResultSet.class.getClassLoader(),
				new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> invocation.invoke(delegate, method, args));
	}

	@Nullable
	private static Object invoke(@NonNull Object delegate,
															 @NonNull Method method,
															 Object[] args) throws Throwable {
		try {
			return method.invoke(delegate, args);
		} catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}

	@FunctionalInterface
	private interface ConnectionWrapper {
		@NonNull
		Connection wrap(@NonNull Connection connection) throws SQLException;
	}

	@FunctionalInterface
	private interface ConnectionInvocation {
		@Nullable
		Object invoke(@NonNull Connection delegate,
									@NonNull Method method,
									Object[] args) throws Throwable;
	}

	@FunctionalInterface
	private interface PreparedStatementInvocation {
		@Nullable
		Object invoke(@NonNull PreparedStatement delegate,
									@NonNull Method method,
									Object[] args) throws Throwable;
	}

	@FunctionalInterface
	private interface ResultSetInvocation {
		@Nullable
		Object invoke(@NonNull ResultSet delegate,
									@NonNull Method method,
									Object[] args) throws Throwable;
	}

	private static final class WrappingDataSource implements DataSource {
		@NonNull
		private final DataSource delegate;
		@NonNull
		private final ConnectionWrapper connectionWrapper;

		private WrappingDataSource(@NonNull DataSource delegate,
															 @NonNull ConnectionWrapper connectionWrapper) {
			this.delegate = delegate;
			this.connectionWrapper = connectionWrapper;
		}

		@Override
		public Connection getConnection() throws SQLException {
			return this.connectionWrapper.wrap(this.delegate.getConnection());
		}

		@Override
		public Connection getConnection(String username,
																		String password) throws SQLException {
			return this.connectionWrapper.wrap(this.delegate.getConnection(username, password));
		}

		@Override
		public PrintWriter getLogWriter() throws SQLException {
			return this.delegate.getLogWriter();
		}

		@Override
		public void setLogWriter(PrintWriter out) throws SQLException {
			this.delegate.setLogWriter(out);
		}

		@Override
		public void setLoginTimeout(int seconds) throws SQLException {
			this.delegate.setLoginTimeout(seconds);
		}

		@Override
		public int getLoginTimeout() throws SQLException {
			return this.delegate.getLoginTimeout();
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			return this.delegate.getParentLogger();
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			return this.delegate.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return this.delegate.isWrapperFor(iface);
		}
	}

	@ThreadSafe
	private static final class RecordingMetricsCollector implements MetricsCollector {
		private final List<StatementResult> results;
		private final AtomicReference<DatabaseType> transactionDatabaseType;

		private RecordingMetricsCollector() {
			this.results = new ArrayList<>();
			this.transactionDatabaseType = new AtomicReference<>();
		}

		@Override
		public synchronized void didExecuteStatement(@NonNull StatementContext<?> ctx,
																								 @NonNull StatementLog<?> statementLog,
																								 @NonNull StatementResult result) {
			this.results.add(result);
		}

		@Override
		public void didBeginPhysicalTransaction(@NonNull Transaction transaction,
																						@NonNull TransactionIsolation isolation,
																						@NonNull DatabaseType databaseType) {
			this.transactionDatabaseType.set(databaseType);
		}
	}
}
