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

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
		Assertions.assertTrue(
				Arrays.stream(ex.getSuppressed()).anyMatch(suppressed -> "post".equals(suppressed.getMessage())),
				"Expected post-transaction failure to be suppressed");
	}

	@Test
	public void testDatabaseExceptionIncludesStatementContextWithoutParameterValues() {
		Database db = Database.withDataSource(createInMemoryDataSource("statement_context_diagnostics")).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT :secret FROM missing_table")
						.bind("secret", "super-secret")
						.fetchList(String.class));

		Assertions.assertTrue(ex.getMessage().contains("sql=SELECT ? FROM missing_table"),
				"Expected SQL in exception message");
		Assertions.assertTrue(ex.getMessage().contains("parameterCount=1"),
				"Expected parameter count in exception message");
		Assertions.assertFalse(ex.getMessage().contains("super-secret"),
				"Exception message must not include raw parameter values by default");
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
		requireNonNull(databaseName);

		DataSource delegate = createInMemoryDataSource(databaseName);

		return new DataSource() {
			@Override
			public Connection getConnection() throws SQLException {
				return connectionThatThrowsOnRollback(delegate.getConnection());
			}

			@Override
			public Connection getConnection(String username,
																			String password) throws SQLException {
				return connectionThatThrowsOnRollback(delegate.getConnection(username, password));
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
}
