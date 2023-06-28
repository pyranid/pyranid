/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2023 Revetware LLC.
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.logging.Level.WARNING;

/**
 * Main class for performing database access operations.
 *
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public class Database {
	@Nonnull
	private static final ThreadLocal<Deque<Transaction>> TRANSACTION_STACK_HOLDER;

	static {
		TRANSACTION_STACK_HOLDER = ThreadLocal.withInitial(() -> new ArrayDeque<>());
	}

	@Nonnull
	private final DataSource dataSource;
	@Nonnull
	private final DatabaseType databaseType;
	@Nonnull
	private final ZoneId timeZone;
	@Nonnull
	private final InstanceProvider instanceProvider;
	@Nonnull
	private final PreparedStatementBinder preparedStatementBinder;
	@Nonnull
	private final ResultSetMapper resultSetMapper;
	@Nonnull
	private final StatementLogger statementLogger;
	@Nonnull
	private final AtomicInteger defaultIdGenerator;
	@Nonnull
	private final Logger logger;

	@Nonnull
	private volatile DatabaseOperationSupportStatus executeLargeBatchSupported;
	@Nonnull
	private volatile DatabaseOperationSupportStatus executeLargeUpdateSupported;

	protected Database(@Nonnull Builder builder) {
		requireNonNull(builder);

		this.dataSource = requireNonNull(builder.dataSource);
		this.databaseType = requireNonNull(builder.databaseType);
		this.timeZone = builder.timeZone == null ? ZoneId.systemDefault() : builder.timeZone;
		this.instanceProvider = builder.instanceProvider == null ? new DefaultInstanceProvider() : builder.instanceProvider;
		this.preparedStatementBinder = builder.preparedStatementBinder == null ? new DefaultPreparedStatementBinder(this.databaseType, this.timeZone) : builder.preparedStatementBinder;
		this.resultSetMapper = builder.resultSetMapper == null ? new DefaultResultSetMapper(this.databaseType, this.instanceProvider, this.timeZone) : builder.resultSetMapper;
		this.statementLogger = builder.statementLogger == null ? new DefaultStatementLogger() : builder.statementLogger;
		this.defaultIdGenerator = new AtomicInteger();
		this.logger = Logger.getLogger(getClass().getName());
		this.executeLargeBatchSupported = DatabaseOperationSupportStatus.UNKNOWN;
		this.executeLargeUpdateSupported = DatabaseOperationSupportStatus.UNKNOWN;
	}

	/**
	 * Provides a {@link Database} builder for the given {@link DataSource}.
	 *
	 * @param dataSource data source used to create the {@link Database} builder
	 * @return a {@link Database} builder
	 */
	@Nonnull
	public static Builder forDataSource(@Nonnull DataSource dataSource) {
		requireNonNull(dataSource);
		return new Builder(dataSource);
	}

	/**
	 * Gets a reference to the current transaction, if any.
	 *
	 * @return the current transaction
	 */
	@Nonnull
	public Optional<Transaction> currentTransaction() {
		Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();
		return Optional.ofNullable(transactionStack.size() == 0 ? null : transactionStack.peek());
	}

	/**
	 * Performs an operation transactionally.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 *
	 * @param transactionalOperation the operation to perform transactionally
	 */
	public void transaction(@Nonnull TransactionalOperation transactionalOperation) {
		requireNonNull(transactionalOperation);

		transaction(() -> {
			transactionalOperation.perform();
			return Optional.empty();
		});
	}

	/**
	 * Performs an operation transactionally with the given isolation level.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 *
	 * @param transactionIsolation   the desired database transaction isolation level
	 * @param transactionalOperation the operation to perform transactionally
	 */
	public void transaction(@Nonnull TransactionIsolation transactionIsolation,
													@Nonnull TransactionalOperation transactionalOperation) {
		requireNonNull(transactionIsolation);
		requireNonNull(transactionalOperation);

		transaction(transactionIsolation, () -> {
			transactionalOperation.perform();
			return Optional.empty();
		});
	}

	/**
	 * Performs an operation transactionally and optionally returns a value.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 *
	 * @param transactionalOperation the operation to perform transactionally
	 * @param <T>                    the type to be returned
	 * @return the result of the transactional operation
	 */
	@Nonnull
	public <T> Optional<T> transaction(@Nonnull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transactionalOperation);
		return transaction(TransactionIsolation.DEFAULT, transactionalOperation);
	}

	/**
	 * Performs an operation transactionally with the given isolation level, optionally returning a value.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 *
	 * @param transactionIsolation   the desired database transaction isolation level
	 * @param transactionalOperation the operation to perform transactionally
	 * @param <T>                    the type to be returned
	 * @return the result of the transactional operation
	 */
	@Nonnull
	public <T> Optional<T> transaction(@Nonnull TransactionIsolation transactionIsolation,
																		 @Nonnull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transactionIsolation);
		requireNonNull(transactionalOperation);

		Transaction transaction = new Transaction(dataSource, transactionIsolation);
		TRANSACTION_STACK_HOLDER.get().push(transaction);
		boolean committed = false;

		try {
			Optional<T> returnValue = transactionalOperation.perform();

			// Safeguard in case user code accidentally returns null instead of Optional.empty()
			if (returnValue == null)
				returnValue = Optional.empty();

			if (transaction.isRollbackOnly()) {
				transaction.rollback();
			} else {
				transaction.commit();
				committed = true;
			}

			return returnValue;
		} catch (RuntimeException e) {
			try {
				transaction.rollback();
			} catch (Exception rollbackException) {
				logger.log(WARNING, "Unable to roll back transaction", rollbackException);
			}

			throw e;
		} catch (Throwable t) {
			try {
				transaction.rollback();
			} catch (Exception rollbackException) {
				logger.log(WARNING, "Unable to roll back transaction", rollbackException);
			}

			throw new RuntimeException(t);
		} finally {
			TRANSACTION_STACK_HOLDER.get().pop();

			try {
				try {
					if (transaction.initialAutoCommit().isPresent() && transaction.initialAutoCommit().get())
						// Autocommit was true initially, so restoring to true now that transaction has completed
						transaction.setAutoCommit(true);
				} finally {
					if (transaction.hasConnection())
						closeConnection(transaction.connection());
				}
			} finally {
				// Execute any user-supplied post-execution hooks
				for (Consumer<TransactionResult> postTransactionOperation : transaction.postTransactionOperations())
					postTransactionOperation.accept(committed ? TransactionResult.COMMITTED : TransactionResult.ROLLED_BACK);
			}
		}
	}

	protected void closeConnection(@Nonnull Connection connection) {
		requireNonNull(connection);

		try {
			connection.close();
		} catch (SQLException e) {
			throw new DatabaseException("Unable to close database connection", e);
		}
	}

	/**
	 * Performs an operation in the context of a pre-existing transaction.
	 * <p>
	 * No commit or rollback on the transaction will occur when {@code transactionalOperation} completes.
	 * <p>
	 * However, if an exception bubbles out of {@code transactionalOperation}, the transaction will be marked as rollback-only.
	 *
	 * @param transaction            the transaction in which to participate
	 * @param transactionalOperation the operation that should participate in the transaction
	 */
	public void participate(@Nonnull Transaction transaction,
													@Nonnull TransactionalOperation transactionalOperation) {
		requireNonNull(transaction);
		requireNonNull(transactionalOperation);

		participate(transaction, () -> {
			transactionalOperation.perform();
			return Optional.empty();
		});
	}

	/**
	 * Performs an operation in the context of a pre-existing transaction, optionall returning a value.
	 * <p>
	 * No commit or rollback on the transaction will occur when {@code transactionalOperation} completes.
	 * <p>
	 * However, if an exception bubbles out of {@code transactionalOperation}, the transaction will be marked as rollback-only.
	 *
	 * @param transaction            the transaction in which to participate
	 * @param transactionalOperation the operation that should participate in the transaction
	 * @param <T>                    the type to be returned
	 * @return the result of the transactional operation
	 */
	@Nonnull
	public <T> Optional<T> participate(@Nonnull Transaction transaction,
																		 @Nonnull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transaction);
		requireNonNull(transactionalOperation);

		TRANSACTION_STACK_HOLDER.get().push(transaction);

		try {
			Optional<T> returnValue = transactionalOperation.perform();
			return returnValue == null ? Optional.empty() : returnValue;
		} catch (RuntimeException e) {
			transaction.setRollbackOnly(true);
			throw e;
		} catch (Throwable t) {
			transaction.setRollbackOnly(true);
			throw new RuntimeException(t);
		} finally {
			TRANSACTION_STACK_HOLDER.get().pop();
		}
	}

	/**
	 * Performs a SQL query that is expected to return 0 or 1 result rows.
	 *
	 * @param sql              the SQL query to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a single result (or no result)
	 * @throws DatabaseException if > 1 row is returned
	 */
	@Nonnull
	public <T> Optional<T> queryForObject(@Nonnull String sql,
																				@Nonnull Class<T> resultSetRowType,
																				@Nullable Object... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return queryForObject(new Statement(generateId(), sql), resultSetRowType, parameters);
	}

	/**
	 * Performs a SQL query that is expected to return 0 or 1 result rows.
	 *
	 * @param statement        the SQL statement to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a single result (or no result)
	 * @throws DatabaseException if > 1 row is returned
	 */
	public <T> Optional<T> queryForObject(@Nonnull Statement statement,
																				@Nonnull Class<T> resultSetRowType,
																				@Nullable Object... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		List<T> list = queryForList(statement, resultSetRowType, parameters);

		if (list.size() > 1)
			throw new DatabaseException(format("Expected 1 row in resultset but got %s instead", list.size()));

		return Optional.ofNullable(list.size() == 0 ? null : list.get(0));
	}

	/**
	 * Performs a SQL query that is expected to return any number of result rows.
	 *
	 * @param sql              the SQL query to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a list of results
	 */
	@Nonnull
	public <T> List<T> queryForList(@Nonnull String sql,
																	@Nonnull Class<T> resultSetRowType,
																	@Nullable Object... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return queryForList(new Statement(generateId(), sql), resultSetRowType, parameters);
	}

	/**
	 * Performs a SQL query that is expected to return any number of result rows.
	 *
	 * @param statement        the SQL statement to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a list of results
	 */
	@Nonnull
	public <T> List<T> queryForList(@Nonnull Statement statement,
																	@Nonnull Class<T> resultSetRowType,
																	@Nullable Object... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		List<T> list = new ArrayList<>();
		StatementContext<T> statementContext = new StatementContext.Builder<T>(statement)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.build();

		performDatabaseOperation(statementContext, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();

			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
				startTime = nanoTime();

				while (resultSet.next()) {
					T listElement = getResultSetMapper().map(statementContext, resultSet, statementContext.getResultSetRowType().get()).orElse(null);
					list.add(listElement);
				}

				Duration resultSetMappingDuration = Duration.ofNanos(nanoTime() - startTime);
				return new DatabaseOperationResult(executionDuration, resultSetMappingDuration);
			}
		});

		return list;
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE};
	 * or a SQL statement that returns nothing, such as a DDL statement.
	 *
	 * @param sql        the SQL to execute
	 * @param parameters {@link PreparedStatement} parameters, if any
	 * @return the number of rows affected by the SQL statement
	 */
	@Nonnull
	public Long execute(@Nonnull String sql,
											@Nullable Object... parameters) {
		requireNonNull(sql);
		return execute(new Statement(generateId(), sql), parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE};
	 * or a SQL statement that returns nothing, such as a DDL statement.
	 *
	 * @param statement  the SQL statement to execute
	 * @param parameters {@link PreparedStatement} parameters, if any
	 * @return the number of rows affected by the SQL statement
	 */
	@Nonnull
	public Long execute(@Nonnull Statement statement,
											@Nullable Object... parameters) {
		requireNonNull(statement);

		ResultHolder<Long> resultHolder = new ResultHolder<>();
		StatementContext<Void> statementContext = new StatementContext.Builder<>(statement)
				.parameters(parameters)
				.build();

		performDatabaseOperation(statementContext, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();

			DatabaseOperationSupportStatus executeLargeUpdateSupported = getExecuteLargeUpdateSupported();

			// Use the appropriate "large" value if we know it.
			// If we don't know it, detect it and store it.
			if (executeLargeUpdateSupported == DatabaseOperationSupportStatus.YES) {
				resultHolder.value = preparedStatement.executeLargeUpdate();
			} else if (executeLargeUpdateSupported == DatabaseOperationSupportStatus.NO) {
				resultHolder.value = (long) preparedStatement.executeUpdate();
			} else {
				// If the driver doesn't support executeLargeUpdate, then UnsupportedOperationException is thrown.
				try {
					resultHolder.value = preparedStatement.executeLargeUpdate();
					setExecuteLargeUpdateSupported(DatabaseOperationSupportStatus.YES);
				} catch (UnsupportedOperationException e) {
					setExecuteLargeUpdateSupported(DatabaseOperationSupportStatus.NO);
					resultHolder.value = (long) preparedStatement.executeUpdate();
				}
			}

			Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
			return new DatabaseOperationResult(executionDuration, null);
		});

		return resultHolder.value;
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns 0 or 1 rows, e.g. with Postgres/Oracle's {@code RETURNING} clause.
	 *
	 * @param sql              the SQL query to execute
	 * @param resultSetRowType the type to which the {@link ResultSet} row should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a single result (or no result)
	 * @throws DatabaseException if > 1 row is returned
	 */
	@Nonnull
	public <T> Optional<T> executeForObject(@Nonnull String sql,
																					@Nonnull Class<T> resultSetRowType,
																					@Nullable Object... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return executeForObject(new Statement(generateId(), sql), resultSetRowType, parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns 0 or 1 rows, e.g. with Postgres/Oracle's {@code RETURNING} clause.
	 *
	 * @param statement        the SQL statement to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a single result (or no result)
	 * @throws DatabaseException if > 1 row is returned
	 */
	public <T> Optional<T> executeForObject(@Nonnull Statement statement,
																					@Nonnull Class<T> resultSetRowType,
																					@Nullable Object... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		// Ultimately we just delegate to queryForObject.
		// Having `executeForList` is to allow for users to explicitly express intent
		// and make static analysis of code easier (e.g. maybe you'd like to hook all of your "execute" statements for
		// logging, or delegation to a writable master as opposed to a read replica)
		return queryForObject(statement, resultSetRowType, parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns any number of rows, e.g. with Postgres/Oracle's {@code RETURNING} clause.
	 *
	 * @param sql              the SQL to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a list of results
	 */
	@Nonnull
	public <T> List<T> executeForList(@Nonnull String sql,
																		@Nonnull Class<T> resultSetRowType,
																		@Nullable Object... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return executeForList(new Statement(generateId(), sql), resultSetRowType, parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns any number of rows, e.g. with Postgres/Oracle's {@code RETURNING} clause.
	 *
	 * @param statement        the SQL statement to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a list of results
	 */
	@Nonnull
	public <T> List<T> executeForList(@Nonnull Statement statement,
																		@Nonnull Class<T> resultSetRowType,
																		@Nullable Object... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		// Ultimately we just delegate to queryForList.
		// Having `executeForList` is to allow for users to explicitly express intent
		// and make static analysis of code easier (e.g. maybe you'd like to hook all of your "execute" statements for
		// logging, or delegation to a writable master as opposed to a read replica)
		return queryForList(statement, resultSetRowType, parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE}
	 * in "batch" over a set of parameter groups.
	 * <p>
	 * Useful for bulk-inserting or updating large amounts of data.
	 *
	 * @param sql             the SQL to execute
	 * @param parameterGroups Groups of {@link PreparedStatement} parameters
	 * @return the number of rows affected by the SQL statement per-group
	 */
	@Nonnull
	public List<Long> executeBatch(@Nonnull String sql,
																 @Nonnull List<List<Object>> parameterGroups) {
		requireNonNull(sql);
		requireNonNull(parameterGroups);

		return executeBatch(new Statement(generateId(), sql), parameterGroups);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE}
	 * in "batch" over a set of parameter groups.
	 * <p>
	 * Useful for bulk-inserting or updating large amounts of data.
	 *
	 * @param statement       the SQL statement to execute
	 * @param parameterGroups Groups of {@link PreparedStatement} parameters
	 * @return the number of rows affected by the SQL statement per-group
	 */
	@Nonnull
	public List<Long> executeBatch(@Nonnull Statement statement,
																 @Nonnull List<List<Object>> parameterGroups) {
		requireNonNull(statement);
		requireNonNull(parameterGroups);

		ResultHolder<List<Long>> resultHolder = new ResultHolder<>();
		StatementContext<List<Long>> statementContext = new StatementContext.Builder<>(statement)
				.parameters(parameterGroups)
				.resultSetRowType(List.class)
				.build();

		performDatabaseOperation(statementContext, (preparedStatement) -> {
			for (List<Object> parameterGroup : parameterGroups) {
				if (parameterGroup != null && parameterGroup.size() > 0)
					getPreparedStatementBinder().bind(statementContext, preparedStatement);

				preparedStatement.addBatch();
			}
		}, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();
			List<Long> result;

			DatabaseOperationSupportStatus executeLargeBatchSupported = getExecuteLargeBatchSupported();

			// Use the appropriate "large" value if we know it.
			// If we don't know it, detect it and store it.
			if (executeLargeBatchSupported == DatabaseOperationSupportStatus.YES) {
				long[] resultArray = preparedStatement.executeLargeBatch();
				result = Arrays.stream(resultArray).boxed().collect(Collectors.toList());
			} else if (executeLargeBatchSupported == DatabaseOperationSupportStatus.NO) {
				int[] resultArray = preparedStatement.executeBatch();
				result = Arrays.stream(resultArray).asLongStream().boxed().collect(Collectors.toList());
			} else {
				// If the driver doesn't support executeLargeBatch, then UnsupportedOperationException is thrown.
				try {
					long[] resultArray = preparedStatement.executeLargeBatch();
					result = Arrays.stream(resultArray).boxed().collect(Collectors.toList());
					setExecuteLargeBatchSupported(DatabaseOperationSupportStatus.YES);
				} catch (UnsupportedOperationException e) {
					setExecuteLargeBatchSupported(DatabaseOperationSupportStatus.NO);
					int[] resultArray = preparedStatement.executeBatch();
					result = Arrays.stream(resultArray).asLongStream().boxed().collect(Collectors.toList());
				}
			}

			resultHolder.value = result;
			Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
			return new DatabaseOperationResult(executionDuration, null);
		});

		return resultHolder.value;
	}

	protected <T> void performDatabaseOperation(@Nonnull StatementContext<T> statementContext,
																							@Nonnull DatabaseOperation databaseOperation) {
		requireNonNull(statementContext);
		requireNonNull(databaseOperation);

		performDatabaseOperation(statementContext, (preparedStatement) -> {
			if (statementContext.getParameters().size() > 0)
				getPreparedStatementBinder().bind(statementContext, preparedStatement);
		}, databaseOperation);
	}

	protected <T> void performDatabaseOperation(@Nonnull StatementContext<T> statementContext,
																							@Nonnull PreparedStatementBindingOperation preparedStatementBindingOperation,
																							@Nonnull DatabaseOperation databaseOperation) {
		requireNonNull(statementContext);
		requireNonNull(preparedStatementBindingOperation);
		requireNonNull(databaseOperation);

		long startTime = nanoTime();
		Duration connectionAcquisitionDuration = null;
		Duration preparationDuration = null;
		Duration executionDuration = null;
		Duration resultSetMappingDuration = null;
		Exception exception = null;
		Connection connection = null;

		try {
			boolean alreadyHasConnection = currentTransaction().isPresent() && currentTransaction().get().hasConnection();
			connection = acquireConnection();
			connectionAcquisitionDuration = alreadyHasConnection ? null : Duration.ofNanos(nanoTime() - startTime);
			startTime = nanoTime();

			try (PreparedStatement preparedStatement = connection.prepareStatement(statementContext.getStatement().getSql())) {
				preparedStatementBindingOperation.perform(preparedStatement);
				preparationDuration = Duration.ofNanos(nanoTime() - startTime);

				DatabaseOperationResult databaseOperationResult = databaseOperation.perform(preparedStatement);
				executionDuration = databaseOperationResult.getExecutionDuration().orElse(null);
				resultSetMappingDuration = databaseOperationResult.getResultSetMappingDuration().orElse(null);
			}
		} catch (DatabaseException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw new DatabaseException(e);
		} finally {
			try {
				// If this was a single-shot operation (not in a transaction), close the connection
				if (connection != null && !currentTransaction().isPresent())
					closeConnection(connection);
			} finally {
				StatementLog statementLog =
						StatementLog.forStatementContext(statementContext)
								.connectionAcquisitionDuration(connectionAcquisitionDuration)
								.preparationDuration(preparationDuration)
								.executionDuration(executionDuration)
								.resultSetMappingDuration(resultSetMappingDuration)
								.exception(exception)
								.build();

				getStatementLogger().log(statementLog);
			}
		}
	}

	@Nonnull
	protected Connection acquireConnection() {
		Optional<Transaction> transaction = currentTransaction();

		if (transaction.isPresent())
			return transaction.get().connection();

		try {
			return getDataSource().getConnection();
		} catch (SQLException e) {
			throw new DatabaseException("Unable to acquire database connection", e);
		}
	}

	@Nonnull
	protected DataSource getDataSource() {
		return this.dataSource;
	}

	@Nonnull
	protected InstanceProvider getInstanceProvider() {
		return this.instanceProvider;
	}

	@Nonnull
	protected PreparedStatementBinder getPreparedStatementBinder() {
		return this.preparedStatementBinder;
	}

	@Nonnull
	protected ResultSetMapper getResultSetMapper() {
		return this.resultSetMapper;
	}

	@Nonnull
	protected StatementLogger getStatementLogger() {
		return this.statementLogger;
	}

	@Nonnull
	protected DatabaseOperationSupportStatus getExecuteLargeBatchSupported() {
		return this.executeLargeBatchSupported;
	}

	protected void setExecuteLargeBatchSupported(@Nonnull DatabaseOperationSupportStatus executeLargeBatchSupported) {
		requireNonNull(executeLargeBatchSupported);
		this.executeLargeBatchSupported = executeLargeBatchSupported;
	}

	@Nonnull
	protected DatabaseOperationSupportStatus getExecuteLargeUpdateSupported() {
		return this.executeLargeUpdateSupported;
	}

	protected void setExecuteLargeUpdateSupported(@Nonnull DatabaseOperationSupportStatus executeLargeUpdateSupported) {
		requireNonNull(executeLargeUpdateSupported);
		this.executeLargeUpdateSupported = executeLargeUpdateSupported;
	}

	@Nonnull
	protected Integer generateId() {
		return this.defaultIdGenerator.incrementAndGet();
	}

	@FunctionalInterface
	protected interface DatabaseOperation {
		@Nonnull
		DatabaseOperationResult perform(@Nonnull PreparedStatement preparedStatement) throws Exception;
	}

	@FunctionalInterface
	protected interface PreparedStatementBindingOperation {
		void perform(@Nonnull PreparedStatement preparedStatement) throws Exception;
	}

	/**
	 * Builder used to construct instances of {@link Database}.
	 * <p>
	 * This class is intended for use by a single thread.
	 *
	 * @author <a href="https://www.revetware.com">Mark Allen</a>
	 * @since 1.0.0
	 */
	@NotThreadSafe
	public static class Builder {
		@Nonnull
		private final DataSource dataSource;
		@Nonnull
		private final DatabaseType databaseType;
		@Nullable
		private ZoneId timeZone;
		@Nullable
		private InstanceProvider instanceProvider;
		@Nullable
		private PreparedStatementBinder preparedStatementBinder;
		@Nullable
		private ResultSetMapper resultSetMapper;
		@Nullable
		private StatementLogger statementLogger;

		private Builder(@Nonnull DataSource dataSource) {
			this.dataSource = requireNonNull(dataSource);
			this.databaseType = DatabaseType.fromDataSource(dataSource);
		}

		@Nonnull
		public Builder timeZone(@Nullable ZoneId timeZone) {
			this.timeZone = timeZone;
			return this;
		}

		@Nonnull
		public Builder instanceProvider(@Nullable InstanceProvider instanceProvider) {
			this.instanceProvider = instanceProvider;
			return this;
		}

		@Nonnull
		public Builder preparedStatementBinder(@Nullable PreparedStatementBinder preparedStatementBinder) {
			this.preparedStatementBinder = preparedStatementBinder;
			return this;
		}

		@Nonnull
		public Builder resultSetMapper(@Nullable ResultSetMapper resultSetMapper) {
			this.resultSetMapper = resultSetMapper;
			return this;
		}

		@Nonnull
		public Builder statementLogger(@Nullable StatementLogger statementLogger) {
			this.statementLogger = statementLogger;
			return this;
		}

		@Nonnull
		public Database build() {
			return new Database(this);
		}
	}

	@ThreadSafe
	static class DatabaseOperationResult {
		@Nullable
		private final Duration executionDuration;
		@Nullable
		private final Duration resultSetMappingDuration;

		public DatabaseOperationResult(@Nullable Duration executionDuration,
																	 @Nullable Duration resultSetMappingDuration) {
			this.executionDuration = executionDuration;
			this.resultSetMappingDuration = resultSetMappingDuration;
		}

		@Nonnull
		public Optional<Duration> getExecutionDuration() {
			return Optional.ofNullable(this.executionDuration);
		}

		@Nonnull
		public Optional<Duration> getResultSetMappingDuration() {
			return Optional.ofNullable(this.resultSetMappingDuration);
		}
	}

	@NotThreadSafe
	static class ResultHolder<T> {
		T value;
	}

	enum DatabaseOperationSupportStatus {
		UNKNOWN,
		YES,
		NO
	}
}