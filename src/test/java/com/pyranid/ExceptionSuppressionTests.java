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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.0.0
 */
public class ExceptionSuppressionTests {
	@Test
	public void testStatementLoggerExceptionSuppressedWhenOperationFails() {
		RuntimeException loggerFailure = new RuntimeException("logger failed");
		StatementLogger statementLogger = (statementLog) -> {
			throw loggerFailure;
		};

		Database db = Database.withDataSource(createInMemoryDataSource("logger_suppressed"))
				.statementLogger(statementLogger)
				.build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class,
				() -> db.query("SELECT * FROM missing_table").fetchList(String.class));

		Assertions.assertTrue(
				Arrays.stream(ex.getSuppressed()).anyMatch(suppressed -> "logger failed".equals(suppressed.getMessage())),
				"Expected statement logger failure to be suppressed");
	}

	@Test
	public void testPostTransactionOperationExceptionSuppressedWhenOperationFails() {
		Database db = Database.withDataSource(createInMemoryDataSource("txn_suppressed")).build();
		RuntimeException boom = new RuntimeException("boom");
		RuntimeException postFailure = new RuntimeException("post");

		RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> {
			db.transaction(() -> {
				db.currentTransaction().orElseThrow()
						.addPostTransactionOperation(result -> {
							throw postFailure;
						});
				throw boom;
			});
		});

		Assertions.assertSame(boom, ex, "Expected original exception to be thrown");
		PostTransactionOperationException suppressed = suppressedPostTransactionOperationException(ex);
		Assertions.assertEquals(TransactionResult.ROLLED_BACK, suppressed.getTransactionResult());
		Assertions.assertSame(postFailure, suppressed.getCause());
	}

	@Test
	public void testPostTransactionOperationExceptionThrownWhenOperationSucceeds() {
		Database db = Database.withDataSource(createInMemoryDataSource("txn_post_failure_after_commit")).build();
		RuntimeException postFailure = new RuntimeException("post");

		db.query("CREATE TABLE t (id INT)").execute();

		PostTransactionOperationException ex = Assertions.assertThrows(PostTransactionOperationException.class, () ->
				db.transaction(() -> {
					db.currentTransaction().orElseThrow()
							.addPostTransactionOperation(result -> {
								throw postFailure;
							});
					db.query("INSERT INTO t VALUES (1)").execute();
				}));

		Assertions.assertEquals(TransactionResult.COMMITTED, ex.getTransactionResult());
		Assertions.assertSame(postFailure, ex.getCause());
		Assertions.assertEquals(1L, db.query("SELECT COUNT(*) FROM t").fetchObject(Long.class).orElseThrow());
	}

	@Test
	public void testPostTransactionOperationExceptionSuppressedWhenCommitIsInDoubt() {
		Database db = Database.withDataSource(createCommitFailingDataSource("txn_post_failure_after_commit_failure")).build();
		RuntimeException postFailure = new RuntimeException("post");

		db.query("CREATE TABLE t (id INT)").execute();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.transaction(() -> {
					db.currentTransaction().orElseThrow()
							.addPostTransactionOperation(result -> {
								throw postFailure;
							});
					db.query("INSERT INTO t VALUES (1)").execute();
				}));

		Assertions.assertTrue(ex.getMessage().contains("Unable to commit transaction"));
		PostTransactionOperationException suppressed = suppressedPostTransactionOperationException(ex);
		Assertions.assertEquals(TransactionResult.IN_DOUBT, suppressed.getTransactionResult());
		Assertions.assertSame(postFailure, suppressed.getCause());
	}

	@Test
	public void testDatabaseExceptionIncludesStatementContextWithRedactedParameterValues() {
		Database db = Database.withDataSource(createInMemoryDataSource("statement_context_diagnostics")).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT :secret, :token FROM missing_table")
						.bind("secret", "super-secret")
						.bind("token", Parameters.secure("raw-token", "<token>"))
						.fetchList(String.class));

		Assertions.assertTrue(ex.getMessage().contains("sql=SELECT ?, ? FROM missing_table"),
				"Expected SQL in exception message");
		Assertions.assertTrue(ex.getMessage().contains("statementId=1"),
				"Expected default statement ID to use the numeric counter");
		Assertions.assertTrue(ex.getMessage().contains("parameters=[super-secret, <token>]"),
				"Expected parameter display values in exception message");
		Assertions.assertFalse(ex.getMessage().contains("parameterCount="),
				"Exception message should not include redundant parameter count");
		Assertions.assertFalse(ex.getMessage().contains("raw-token"),
				"Exception message must not include raw secured parameter values");
	}

	@Test
	public void testDatabaseExceptionTruncatesLongParameterValues() {
		Database db = Database.withDataSource(createInMemoryDataSource("statement_context_parameter_truncation")).build();
		String longParameter = "x".repeat(1000);

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT :secret FROM missing_table")
						.bind("secret", longParameter)
						.fetchList(String.class));

		Assertions.assertTrue(ex.getMessage().contains("parameters=["),
				"Expected parameters in exception message");
		Assertions.assertTrue(ex.getMessage().contains("... (truncated)"),
				"Expected long parameter value to be truncated in exception message");
		Assertions.assertFalse(ex.getMessage().contains(longParameter),
				"Expected full long parameter value to be omitted");
		Assertions.assertTrue(ex.getMessage().length() < 4096,
				"Expected bounded exception message length");
	}

	@Test
	public void testDatabaseExceptionTruncatesParameterListToTotalBudget() {
		Database db = Database.withDataSource(createInMemoryDataSource("statement_context_parameter_list_truncation")).build();
		StringBuilder sql = new StringBuilder("SELECT ");

		for (int i = 0; i < 20; i++) {
			if (i > 0)
				sql.append(", ");

			sql.append(":p").append(i);
		}

		sql.append(" FROM missing_table");

		Query query = db.query(sql.toString());

		for (int i = 0; i < 20; i++)
			query.bind(format("p%s", i), format("value-%02d-%s", i, "x".repeat(300)));

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () -> query.fetchList(String.class));
		String parameterDiagnostic = parameterDiagnostic(ex.getMessage());

		Assertions.assertTrue(parameterDiagnostic.length() <= 2048,
				"Expected parameter diagnostic to respect the total budget");
		Assertions.assertTrue(parameterDiagnostic.contains("... (truncated)"),
				"Expected total parameter diagnostic to be truncated");
		Assertions.assertTrue(parameterDiagnostic.endsWith("]"),
				"Expected truncated parameter diagnostic to remain bracketed");
	}

	@Test
	public void testDatabaseExceptionMasksSecureInListValues() {
		Database db = Database.withDataSource(createInMemoryDataSource("statement_context_secure_inlist_exception")).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT * FROM missing_table WHERE token IN (:tokens)")
						.bind("tokens", Parameters.secure(Parameters.inList(List.of("raw-token-1", "raw-token-2")), "<token>"))
						.fetchList(String.class));

		Assertions.assertTrue(ex.getMessage().contains("parameters=[<token>, <token>]"),
				"Expected secure IN-list values to remain masked in exception diagnostics");
		Assertions.assertFalse(ex.getMessage().contains("raw-token-1"));
		Assertions.assertFalse(ex.getMessage().contains("raw-token-2"));
	}

	@Test
	public void testDatabaseExceptionUsesBatchSummaryForBatchFailures() {
		Database db = Database.withDataSource(createInMemoryDataSource("statement_context_batch_exception")).build();

		db.query("CREATE TABLE batch_items (id INT PRIMARY KEY, token VARCHAR(255))").execute();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO batch_items (id, token) VALUES (:id, :token)")
						.executeBatch(List.of(
								Map.of("id", 1, "token", Parameters.secure("raw-token-1")),
								Map.of("id", 1, "token", Parameters.secure("raw-token-2"))
						)));

		Assertions.assertTrue(ex.getMessage().contains("parameters=[<batch: 2 groups x 2 parameters>]"),
				"Expected batch exception diagnostics to use the bounded batch summary");
		Assertions.assertFalse(ex.getMessage().contains("raw-token-1"));
		Assertions.assertFalse(ex.getMessage().contains("raw-token-2"));
	}

	@Test
	public void testDatabaseExceptionSuppressesParameterRenderingFailure() {
		RuntimeException redactionFailure = new RuntimeException("redaction boom");
		Database db = Database.withDataSource(createInMemoryDataSource("statement_context_redaction_failure"))
				.parameterRedactor((statementContext, parameterIndex, parameter) -> {
					throw redactionFailure;
				})
				.build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT :secret FROM missing_table")
						.bind("secret", "super-secret")
						.fetchList(String.class));

		Assertions.assertTrue(ex.getMessage().contains("parameters=<unavailable>"),
				"Expected explicit unavailable parameters marker");
		Assertions.assertTrue(ex.getMessage().contains("parameterRenderingFailure=java.lang.RuntimeException: redaction boom"),
				"Expected parameter rendering failure summary");
		Assertions.assertSame(redactionFailure, Arrays.stream(ex.getSuppressed())
				.filter(suppressed -> suppressed == redactionFailure)
				.findFirst()
				.orElse(null));
	}

	@Test
	public void testDatabaseExceptionSuppressesSecureParameterMaskFailure() {
		RuntimeException maskFailure = new RuntimeException("mask boom");
		SecureParameter throwingMask = new SecureParameter() {
			@NonNull
			@Override
			public Optional<Object> getValue() {
				return Optional.of("raw-token");
			}

			@NonNull
			@Override
			public String getMask() {
				throw maskFailure;
			}
		};
		Database db = Database.withDataSource(createInMemoryDataSource("statement_context_mask_failure")).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT :token FROM missing_table")
						.bind("token", throwingMask)
						.fetchList(String.class));

		Assertions.assertTrue(ex.getMessage().contains("parameters=<unavailable>"),
				"Expected explicit unavailable parameters marker");
		Assertions.assertTrue(ex.getMessage().contains("parameterRenderingFailure=java.lang.RuntimeException: mask boom"),
				"Expected mask rendering failure summary");
		Assertions.assertFalse(ex.getMessage().contains("raw-token"));
		Assertions.assertSame(maskFailure, Arrays.stream(ex.getSuppressed())
				.filter(suppressed -> suppressed == maskFailure)
				.findFirst()
				.orElse(null));
	}

	@Test
	public void testDatabaseExceptionSuppressesSecureParameterNullMaskFailure() {
		SecureParameter nullMask = new SecureParameter() {
			@NonNull
			@Override
			public Optional<Object> getValue() {
				return Optional.of("raw-token");
			}

			@SuppressWarnings("DataFlowIssue")
			@NonNull
			@Override
			public String getMask() {
				return null;
			}
		};
		Database db = Database.withDataSource(createInMemoryDataSource("statement_context_null_mask_failure")).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT :token FROM missing_table")
						.bind("token", nullMask)
						.fetchList(String.class));

		Assertions.assertTrue(ex.getMessage().contains("parameters=<unavailable>"),
				"Expected explicit unavailable parameters marker");
		Assertions.assertTrue(ex.getMessage().contains("parameterRenderingFailure=java.lang.NullPointerException"),
				"Expected null-mask rendering failure summary");
		Assertions.assertFalse(ex.getMessage().contains("raw-token"));
		Assertions.assertTrue(Arrays.stream(ex.getSuppressed()).anyMatch(NullPointerException.class::isInstance),
				"Expected null-mask failure to be suppressed");
	}

	@Test
	public void testDatabaseExceptionParameterRenderingFailureSummaryDoesNotCallToString() {
		RuntimeException redactionFailure = new RuntimeException("redaction boom") {
			@Override
			public String toString() {
				throw new RuntimeException("toString boom");
			}
		};
		Database db = Database.withDataSource(createInMemoryDataSource("statement_context_redaction_failure_to_string"))
				.parameterRedactor((statementContext, parameterIndex, parameter) -> {
					throw redactionFailure;
				})
				.build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT :secret FROM missing_table")
						.bind("secret", "super-secret")
						.fetchList(String.class));

		Assertions.assertTrue(ex.getMessage().contains("parameters=<unavailable>"),
				"Expected explicit unavailable parameters marker");
		Assertions.assertTrue(ex.getMessage().contains("redaction boom"),
				"Expected rendering failure message");
		Assertions.assertFalse(ex.getMessage().contains("toString boom"),
				"Expected rendering failure summary not to invoke Throwable.toString()");
		Assertions.assertSame(redactionFailure, Arrays.stream(ex.getSuppressed())
				.filter(suppressed -> suppressed == redactionFailure)
				.findFirst()
				.orElse(null));
	}

	@Test
	public void testDatabaseExceptionTruncatesLongSqlInStatementContext() {
		Database db = Database.withDataSource(createInMemoryDataSource("statement_context_truncation")).build();
		StringBuilder sql = new StringBuilder("SELECT 1 FROM missing_table WHERE ");

		for (int i = 0; i < 500; i++) {
			if (i > 0)
				sql.append(" OR ");

			sql.append("x").append(i).append(" = ").append(i);
		}

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query(sql.toString()).fetchList(Integer.class));

		Assertions.assertTrue(ex.getMessage().contains("... (truncated)"),
				"Expected long SQL to be truncated in exception message");
		Assertions.assertTrue(ex.getMessage().length() < 4096,
				"Expected bounded exception message length");
	}

	@Test
	public void testDatabaseExceptionTruncatesLongCauseMessage() {
		String longMessage = "driver failure ".repeat(1000);
		Database db = Database.withDataSource(new QueryThrowingDataSource(createInMemoryDataSource("long_cause_message"),
				new SQLException(longMessage))).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT 1 FROM (VALUES (0)) AS t(x)").fetchObject(Integer.class));

		Assertions.assertTrue(ex.getMessage().contains("... (truncated)"),
				"Expected long cause message to be truncated");
		Assertions.assertTrue(ex.getMessage().contains("sql=SELECT 1 FROM (VALUES (0)) AS t(x)"),
				"Expected statement context to remain available after truncating cause message");
		Assertions.assertTrue(ex.getMessage().length() < 4096,
				"Expected bounded exception message length");
	}

	@Test
	public void testDatabaseExceptionPreservesPostgresqlMetadataWithoutServerErrorMessage() {
		PSQLException cause = new PSQLException("connection failed", PSQLState.CONNECTION_FAILURE);

		DatabaseException ex = new DatabaseException("Unable to connect", cause);

		Assertions.assertEquals(cause.getSQLState(), ex.getSqlState().orElseThrow(),
				"Expected SQLState from the PostgreSQL exception itself");
		Assertions.assertEquals(cause.getErrorCode(), ex.getErrorCode().orElseThrow(),
				"Expected vendor error code from the PostgreSQL exception itself");
		Assertions.assertTrue(ex.getConstraint().isEmpty(),
				"Expected no server-error fields when PostgreSQL did not provide ServerErrorMessage");
	}

	@Test
	public void testStatementLoggerReceivesRawExecutionException() {
		List<Exception> exceptions = new ArrayList<>();
		Database db = Database.withDataSource(createInMemoryDataSource("statement_logger_raw_exception"))
				.statementLogger(statementLog -> statementLog.getException().ifPresent(exception -> exceptions.add((Exception) exception)))
				.build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT * FROM missing_table").fetchList(String.class));

		Assertions.assertTrue(ex.getMessage().contains("sql=SELECT * FROM missing_table"),
				"Expected thrown exception to include statement context");
		Assertions.assertEquals(1, exceptions.size(), "Expected one failed statement log");
		Assertions.assertTrue(exceptions.get(0) instanceof SQLException,
				"StatementLog should expose the raw JDBC exception, not Pyranid's wrapper");
	}

	@Test
	public void testRollbackExceptionSuppressedWhenRuntimeExceptionThrown() {
		Database db = Database.withDataSource(createRollbackFailingDataSource("rollback_runtime_suppressed")).build();
		RuntimeException boom = new RuntimeException("boom");

		RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> {
			db.transaction(() -> {
				db.query("SELECT 1 FROM (VALUES (0)) AS t(x)").fetchObject(Integer.class);
				throw boom;
			});
		});

		Assertions.assertSame(boom, ex, "Expected original runtime exception to be thrown");
		assertSuppressedRollbackFailure(ex);
	}

	@Test
	public void testRollbackExceptionSuppressedWhenErrorThrown() {
		Database db = Database.withDataSource(createRollbackFailingDataSource("rollback_error_suppressed")).build();
		AssertionError boom = new AssertionError("boom");

		AssertionError ex = Assertions.assertThrows(AssertionError.class, () -> {
			db.transaction(() -> {
				db.query("SELECT 1 FROM (VALUES (0)) AS t(x)").fetchObject(Integer.class);
				throw boom;
			});
		});

		Assertions.assertSame(boom, ex, "Expected original error to be thrown");
		assertSuppressedRollbackFailure(ex);
	}

	@Test
	public void testRollbackExceptionSuppressedWhenCheckedExceptionThrown() {
		Database db = Database.withDataSource(createRollbackFailingDataSource("rollback_checked_suppressed")).build();
		Exception boom = new Exception("boom");

		RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> {
			db.transaction(() -> {
				db.query("SELECT 1 FROM (VALUES (0)) AS t(x)").fetchObject(Integer.class);
				throw boom;
			});
		});

		Assertions.assertSame(boom, ex.getCause(), "Expected checked exception to be wrapped");
		assertSuppressedRollbackFailure(ex);
	}

	private void assertSuppressedRollbackFailure(@NonNull Throwable throwable) {
		requireNonNull(throwable);

		Assertions.assertTrue(
					Arrays.stream(throwable.getSuppressed()).anyMatch(suppressed ->
							"Unable to roll back transaction".equals(suppressed.getMessage())
									&& suppressed.getCause() != null
									&& "rollback failed".equals(suppressed.getCause().getMessage())),
				"Expected suppressed rollback failure");
	}

	@NonNull
	private String parameterDiagnostic(@NonNull String message) {
		requireNonNull(message);

		int parametersStart = message.indexOf("parameters=");

		if (parametersStart < 0)
			throw new AssertionError("Expected parameters diagnostic in message: " + message);

		int valueStart = parametersStart + "parameters=".length();
		int valueEnd = message.indexOf("]", valueStart);

		if (valueEnd < 0)
			throw new AssertionError("Expected closing parameter diagnostic bracket in message: " + message);

		return message.substring(valueStart, valueEnd + 1);
	}

	@NonNull
	private PostTransactionOperationException suppressedPostTransactionOperationException(@NonNull Throwable throwable) {
		requireNonNull(throwable);

		return Arrays.stream(throwable.getSuppressed())
				.filter(PostTransactionOperationException.class::isInstance)
				.map(PostTransactionOperationException.class::cast)
				.findFirst()
				.orElseThrow(() -> new AssertionError("Expected suppressed post-transaction operation failure"));
	}

	@NonNull
	private DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}

	@NonNull
	private DataSource createRollbackFailingDataSource(@NonNull String databaseName) {
		return createConnectionWrappingDataSource(databaseName, this::connectionThatThrowsOnRollback);
	}

	@NonNull
	private DataSource createCommitFailingDataSource(@NonNull String databaseName) {
		return createConnectionWrappingDataSource(databaseName, this::connectionThatThrowsOnCommit);
	}

	@NonNull
	private DataSource createConnectionWrappingDataSource(@NonNull String databaseName,
																											 @NonNull ConnectionWrapper connectionWrapper) {
		requireNonNull(databaseName);
		requireNonNull(connectionWrapper);

		DataSource delegate = createInMemoryDataSource(databaseName);

		return new DataSource() {
			@Override
			public Connection getConnection() throws SQLException {
				return connectionWrapper.wrap(delegate.getConnection());
			}

			@Override
			public Connection getConnection(String username,
																			String password) throws SQLException {
				return connectionWrapper.wrap(delegate.getConnection(username, password));
			}

			@Override
			public PrintWriter getLogWriter() throws SQLException {
				return delegate.getLogWriter();
			}

			@Override
			public void setLogWriter(PrintWriter out) throws SQLException {
				delegate.setLogWriter(out);
			}

			@Override
			public void setLoginTimeout(int seconds) throws SQLException {
				delegate.setLoginTimeout(seconds);
			}

			@Override
			public int getLoginTimeout() throws SQLException {
				return delegate.getLoginTimeout();
			}

			@Override
			public Logger getParentLogger() {
				return Logger.getLogger(DataSource.class.getName());
			}

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				return delegate.unwrap(iface);
			}

			@Override
			public boolean isWrapperFor(Class<?> iface) throws SQLException {
				return delegate.isWrapperFor(iface);
			}
		};
	}

	@FunctionalInterface
	private interface ConnectionWrapper {
		@NonNull
		Connection wrap(@NonNull Connection connection) throws SQLException;
	}

	private static final class QueryThrowingDataSource implements DataSource {
		@NonNull
		private final DataSource delegate;
		@NonNull
		private final SQLException exception;

		private QueryThrowingDataSource(@NonNull DataSource delegate,
																		@NonNull SQLException exception) {
			this.delegate = requireNonNull(delegate);
			this.exception = requireNonNull(exception);
		}

		@Override
		public Connection getConnection() throws SQLException {
			return connectionThatThrowsOnQuery(this.delegate.getConnection(), this.exception);
		}

		@Override
		public Connection getConnection(String username,
																		String password) throws SQLException {
			return connectionThatThrowsOnQuery(this.delegate.getConnection(username, password), this.exception);
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
		public Logger getParentLogger() {
			return Logger.getLogger(DataSource.class.getName());
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

	@NonNull
	private static Connection connectionThatThrowsOnQuery(@NonNull Connection connection,
																												@NonNull SQLException exception) {
		requireNonNull(connection);
		requireNonNull(exception);

		return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
				(proxy, method, args) -> {
					if ("prepareStatement".equals(method.getName()) && method.getParameterCount() > 0)
						throw exception;

					try {
						return method.invoke(connection, args);
					} catch (InvocationTargetException e) {
						throw e.getCause();
					}
				});
	}

	@NonNull
	private Connection connectionThatThrowsOnRollback(@NonNull Connection connection) {
		requireNonNull(connection);

		return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
				(proxy, method, args) -> {
					if ("rollback".equals(method.getName()) && method.getParameterCount() == 0)
						throw new SQLException("rollback failed");

					try {
						return method.invoke(connection, args);
					} catch (InvocationTargetException e) {
						throw e.getCause();
					}
				});
	}

	@NonNull
	private Connection connectionThatThrowsOnCommit(@NonNull Connection connection) {
		requireNonNull(connection);

		return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
				(proxy, method, args) -> {
					if ("commit".equals(method.getName()) && method.getParameterCount() == 0)
						throw new SQLException("commit failed");

					try {
						return method.invoke(connection, args);
					} catch (InvocationTargetException e) {
						throw e.getCause();
					}
				});
	}
}
