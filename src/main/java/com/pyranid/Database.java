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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;

/**
 * Main class for performing database access operations.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public final class Database {
	@NonNull
	private static final ThreadLocal<Deque<Transaction>> TRANSACTION_STACK_HOLDER;
	private static final int DEFAULT_PARSED_SQL_CACHE_CAPACITY = 1024;
	private static final int DEFAULT_POSTGRESQL_STREAM_FETCH_SIZE = 256;
	private static final int MAX_DIAGNOSTIC_MESSAGE_LENGTH = 1024;
	private static final int MAX_DIAGNOSTIC_SQL_LENGTH = 2048;
	@NonNull
	private static final String TRUNCATED_SUFFIX = "... (truncated)";
	@NonNull
	private static final Pattern DIAGNOSTIC_WHITESPACE_PATTERN = Pattern.compile("\\s+");

	static {
		TRANSACTION_STACK_HOLDER = new ThreadLocal<>();
	}

	@NonNull
	private final DataSource dataSource;
	@NonNull
	private final AtomicReference<@Nullable DatabaseType> databaseType;
	@NonNull
	private final ThreadLocal<Connection> databaseTypeDetectionConnectionHolder;
	@NonNull
	private final ZoneId timeZone;
	@NonNull
	private final AmbiguousTimestampBindingStrategy ambiguousTimestampBindingStrategy;
	@NonNull
	private final InstanceProvider instanceProvider;
	@NonNull
	private final PreparedStatementBinder preparedStatementBinder;
	@NonNull
	private final ResultSetMapper resultSetMapper;
	@NonNull
	private final StatementLogger statementLogger;
	@NonNull
	private final MetricsCollectorDispatcher metricsCollectorDispatcher;
	@Nullable
	private final Duration queryTimeout;
	@Nullable
	private final Integer fetchSize;
	@Nullable
	private final Integer maxRows;
	@Nullable
	private final Map<String, ParsedSql> parsedSqlCache;
	@NonNull
	private final AtomicLong defaultIdGenerator;
	@NonNull
	private final Logger logger;

	@NonNull
	private volatile DatabaseOperationSupportStatus executeLargeBatchSupported;
	@NonNull
	private volatile DatabaseOperationSupportStatus executeLargeUpdateSupported;

	protected Database(@NonNull Builder builder) {
		requireNonNull(builder);

		this.dataSource = requireNonNull(builder.dataSource);
		this.databaseType = new AtomicReference<>(builder.databaseType);
		this.databaseTypeDetectionConnectionHolder = new ThreadLocal<>();
		this.timeZone = builder.timeZone == null ? ZoneId.systemDefault() : builder.timeZone;
		this.ambiguousTimestampBindingStrategy = builder.ambiguousTimestampBindingStrategy == null
				? AmbiguousTimestampBindingStrategy.TIMESTAMP_WITH_TIME_ZONE
				: builder.ambiguousTimestampBindingStrategy;
		this.instanceProvider = builder.instanceProvider == null ? new InstanceProvider() {} : builder.instanceProvider;
		this.preparedStatementBinder = builder.preparedStatementBinder == null ? PreparedStatementBinder.withDefaultConfiguration() : builder.preparedStatementBinder;
		this.resultSetMapper = builder.resultSetMapper == null ? ResultSetMapper.withDefaultConfiguration() : builder.resultSetMapper;
		this.statementLogger = builder.statementLogger == null ? (statementLog) -> {} : builder.statementLogger;
		this.metricsCollectorDispatcher = new MetricsCollectorDispatcher(builder.metricsCollector);
		this.queryTimeout = validateQueryTimeout(builder.queryTimeout);
		this.fetchSize = validateNonNegativeStatementSetting("fetchSize", builder.fetchSize);
		this.maxRows = validateNonNegativeStatementSetting("maxRows", builder.maxRows);
		if (builder.parsedSqlCacheCapacity != null && builder.parsedSqlCacheCapacity < 0)
			throw new IllegalArgumentException("parsedSqlCacheCapacity must be >= 0");

		int parsedSqlCacheCapacity = builder.parsedSqlCacheCapacity == null
				? DEFAULT_PARSED_SQL_CACHE_CAPACITY
				: builder.parsedSqlCacheCapacity;
		this.parsedSqlCache = parsedSqlCacheCapacity == 0 ? null : new ConcurrentLruMap<>(parsedSqlCacheCapacity);
		this.defaultIdGenerator = new AtomicLong();
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
	@NonNull
	public static Builder withDataSource(@NonNull DataSource dataSource) {
		requireNonNull(dataSource);
		return new Builder(dataSource);
	}

	/**
	 * Gets a reference to the current transaction, if any.
	 *
	 * @return the current transaction
	 */
	@NonNull
	public Optional<Transaction> currentTransaction() {
		@Nullable Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();
		Transaction transaction = transactionStack == null || transactionStack.isEmpty() ? null : transactionStack.peek();

		if (transaction == null || !isTransactionOwnedByThisDatabase(transaction))
			return Optional.empty();

		return Optional.of(transaction);
	}

	@NonNull
	private Optional<Transaction> currentTransactionForDatabaseOperation() {
		@Nullable Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();
		Transaction transaction = transactionStack == null || transactionStack.isEmpty() ? null : transactionStack.peek();

		if (transaction == null)
			return Optional.empty();

		if (!isTransactionOwnedByThisDatabase(transaction))
			throw wrongDatabaseTransactionException(transaction);

		return Optional.of(transaction);
	}

	@NonNull
	private Deque<Transaction> transactionStackForPush() {
		Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();

		if (transactionStack == null) {
			transactionStack = new ArrayDeque<>();
			TRANSACTION_STACK_HOLDER.set(transactionStack);
		}

		return transactionStack;
	}

	private boolean isTransactionOwnedByThisDatabase(@NonNull Transaction transaction) {
		requireNonNull(transaction);
		return transaction.isOwnedBy(getDataSource());
	}

	@NonNull
	private DatabaseException wrongDatabaseTransactionException(@NonNull Transaction transaction) {
		requireNonNull(transaction);
		return new DatabaseException(format("Transaction %s belongs to a different %s than this %s. "
						+ "Use the %s instance that created the transaction, or explicitly participate only with a transaction "
						+ "created from the same %s.",
				transaction.id(), DataSource.class.getSimpleName(), Database.class.getSimpleName(),
				Database.class.getSimpleName(), DataSource.class.getSimpleName()));
	}

	/**
	 * Performs an operation transactionally.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 * <p>
	 * Nested calls to {@code transaction(...)} are independent transactions with independent JDBC connections; they do
	 * not automatically join an outer transaction. Use {@link #participate(Transaction, TransactionalOperation)} to join an
	 * existing transaction explicitly. A transaction is scoped to the {@link DataSource} instance that created it; a
	 * {@link Database} using a different {@link DataSource} fails fast instead of joining it.
	 *
	 * @param transactionalOperation the operation to perform transactionally
	 */
	public void transaction(@NonNull TransactionalOperation transactionalOperation) {
		requireNonNull(transactionalOperation);

		transaction(() -> {
			transactionalOperation.perform();
			return Optional.empty();
		});
	}

	/**
	 * Performs an operation transactionally with the given options.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 * <p>
	 * Nested calls to {@code transaction(...)} are independent transactions with independent JDBC connections; they do
	 * not automatically join an outer transaction. Use {@link #participate(Transaction, TransactionalOperation)} to join an
	 * existing transaction explicitly. A transaction is scoped to the {@link DataSource} instance that created it; a
	 * {@link Database} using a different {@link DataSource} fails fast instead of joining it.
	 *
	 * @param transactionOptions     options to apply to the transaction
	 * @param transactionalOperation the operation to perform transactionally
	 * @since 4.2.0
	 */
	public void transaction(@NonNull TransactionOptions transactionOptions,
													@NonNull TransactionalOperation transactionalOperation) {
		requireNonNull(transactionOptions);
		requireNonNull(transactionalOperation);

		transaction(transactionOptions, () -> {
			transactionalOperation.perform();
			return Optional.empty();
		});
	}

	/**
	 * Performs an operation transactionally and optionally returns a value.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 * <p>
	 * Nested calls to {@code transaction(...)} are independent transactions with independent JDBC connections; they do
	 * not automatically join an outer transaction. Use {@link #participate(Transaction, ReturningTransactionalOperation)} to
	 * join an existing transaction explicitly. A transaction is scoped to the {@link DataSource} instance that created it; a
	 * {@link Database} using a different {@link DataSource} fails fast instead of joining it.
	 *
	 * @param transactionalOperation the operation to perform transactionally
	 * @param <T>                    the type to be returned
	 * @return the result of the transactional operation
	 */
	@NonNull
	public <T> Optional<T> transaction(@NonNull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transactionalOperation);
		return transaction(TransactionOptions.DEFAULT, transactionalOperation);
	}

	/**
	 * Performs an operation transactionally with the given options, optionally returning a value.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 * <p>
	 * Nested calls to {@code transaction(...)} are independent transactions with independent JDBC connections; they do
	 * not automatically join an outer transaction. Use {@link #participate(Transaction, ReturningTransactionalOperation)} to
	 * join an existing transaction explicitly. A transaction is scoped to the {@link DataSource} instance that created it; a
	 * {@link Database} using a different {@link DataSource} fails fast instead of joining it.
	 *
	 * @param transactionOptions     options to apply to the transaction
	 * @param transactionalOperation the operation to perform transactionally
	 * @param <T>                    the type to be returned
	 * @return the result of the transactional operation
	 * @since 4.2.0
	 */
	@NonNull
	public <T> Optional<T> transaction(@NonNull TransactionOptions transactionOptions,
																		 @NonNull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transactionOptions);
		requireNonNull(transactionalOperation);

		Transaction transaction = new Transaction(dataSource, transactionOptions, getMetricsCollectorDispatcher(), peekDatabaseType());
		Deque<Transaction> transactionStack = transactionStackForPush();
		transactionStack.push(transaction);
		boolean committed = false;
		boolean commitFailed = false;
		boolean rollbackFailed = false;
		boolean rollbackAttempted = false;
		Throwable thrown = null;
		long transactionStartTime = nanoTime();
		getMetricsCollectorDispatcher().didEnterTransactionClosure(transaction, transactionOptions.getIsolation(), transaction.getDatabaseType());

		try {
			Optional<T> returnValue = transactionalOperation.perform();

			// Safeguard in case user code accidentally returns null instead of Optional.empty()
			if (returnValue == null)
				returnValue = Optional.empty();

			transaction.getConnectionLock().lock();

			try {
				if (transaction.isRollbackOnly()) {
					rollbackAttempted = true;
					transaction.rollback();
				} else {
					try {
						transaction.commit();
					} catch (RuntimeException | Error e) {
						commitFailed = true;
						throw e;
					}
					committed = true;
				}

				transaction.markCompleted();
			} finally {
				transaction.getConnectionLock().unlock();
			}

			return returnValue;
		} catch (RuntimeException e) {
			thrown = e;
			if (rollbackAttempted) {
				rollbackFailed = true;
				markTransactionCompleted(transaction);
			} else {
				rollbackFailed = rollbackTransactionAfterFailure(transaction, e);
			}

			restoreInterruptIfNeeded(e);
			throw e;
		} catch (Error e) {
			thrown = e;
			if (rollbackAttempted) {
				rollbackFailed = true;
				markTransactionCompleted(transaction);
			} else {
				rollbackFailed = rollbackTransactionAfterFailure(transaction, e);
			}

			restoreInterruptIfNeeded(e);
			throw e;
		} catch (Throwable t) {
			RuntimeException wrapped = new RuntimeException(t);
			thrown = wrapped;
			if (rollbackAttempted) {
				rollbackFailed = true;
				markTransactionCompleted(transaction);
			} else {
				rollbackFailed = rollbackTransactionAfterFailure(transaction, wrapped);
			}

			restoreInterruptIfNeeded(t);
			throw wrapped;
		} finally {
			transactionStack.pop();

			// Ensure txn stack is fully cleaned up
			if (transactionStack.isEmpty())
				TRANSACTION_STACK_HOLDER.remove();

			Throwable cleanupFailure = null;
			boolean hadPhysicalTransaction = false;

			try {
				transaction.getConnectionLock().lock();

				try {
					hadPhysicalTransaction = transaction.hasConnection();

					if (!transaction.isCompleted())
						transaction.markCompleted();

					cleanupFailure = cleanupCompletedTransactionConnection(transaction, cleanupFailure);
				} finally {
					transaction.getConnectionLock().unlock();
				}
			} finally {
				// Execute any user-supplied post-execution hooks
				for (Consumer<TransactionResult> postTransactionOperation : transaction.getPostTransactionOperations()) {
					long postTransactionStartTime = nanoTime();
					Throwable postTransactionThrowable = null;
					TransactionResult transactionResult = transactionResult(committed, commitFailed);
					try {
						postTransactionOperation.accept(transactionResult);
					} catch (Throwable cleanupException) {
						postTransactionThrowable = cleanupException;
						PostTransactionOperationException postTransactionOperationException =
								new PostTransactionOperationException(transactionResult, cleanupException);

						if (cleanupFailure == null)
							cleanupFailure = postTransactionOperationException;
						else
							cleanupFailure.addSuppressed(postTransactionOperationException);
					} finally {
						getMetricsCollectorDispatcher().didRunPostTransactionOperation(transaction, transactionResult, transaction.getDatabaseType(),
								Duration.ofNanos(nanoTime() - postTransactionStartTime), postTransactionThrowable);
					}
				}
			}

			Throwable exitThrown = thrown == null ? cleanupFailure : thrown;
			getMetricsCollectorDispatcher().didExitTransactionClosure(transaction,
					transactionClosureOutcome(committed, commitFailed, hadPhysicalTransaction, rollbackFailed),
					transaction.getDatabaseType(), Duration.ofNanos(nanoTime() - transactionStartTime), exitThrown);

			if (cleanupFailure != null) {
				if (thrown != null) {
					thrown.addSuppressed(cleanupFailure);
				} else if (cleanupFailure instanceof RuntimeException) {
					throw (RuntimeException) cleanupFailure;
				} else if (cleanupFailure instanceof Error) {
					throw (Error) cleanupFailure;
				} else {
					throw new RuntimeException(cleanupFailure);
				}
			}
		}
	}

	private boolean rollbackTransactionAfterFailure(@NonNull Transaction transaction,
																								 @NonNull Throwable primary) {
		requireNonNull(transaction);
		requireNonNull(primary);

		boolean rollbackFailed = false;
		transaction.getConnectionLock().lock();

		try {
			try {
				transaction.rollback();
			} catch (Throwable rollbackException) {
				rollbackFailed = true;
				primary.addSuppressed(rollbackException);
			} finally {
				transaction.markCompleted();
			}
		} finally {
			transaction.getConnectionLock().unlock();
		}

		return rollbackFailed;
	}

	private void markTransactionCompleted(@NonNull Transaction transaction) {
		requireNonNull(transaction);
		transaction.getConnectionLock().lock();

		try {
			transaction.markCompleted();
		} finally {
			transaction.getConnectionLock().unlock();
		}
	}

	@Nullable
	private Throwable cleanupCompletedTransactionConnection(@NonNull Transaction transaction,
																												 @Nullable Throwable cleanupFailure) {
		requireNonNull(transaction);

		try {
			transaction.restoreTransactionIsolationIfNeeded();
		} catch (Throwable cleanupException) {
			cleanupFailure = appendSuppressed(cleanupFailure, cleanupException);
		}

		try {
			transaction.restoreReadOnlyIfNeeded();
		} catch (Throwable cleanupException) {
			cleanupFailure = appendSuppressed(cleanupFailure, cleanupException);
		}

		if (transaction.getInitialAutoCommit().isPresent() && transaction.getInitialAutoCommit().get()) {
			try {
				// Autocommit was true initially, so restoring to true now that transaction has completed
				transaction.setAutoCommit(true);
			} catch (Throwable cleanupException) {
				cleanupFailure = appendSuppressed(cleanupFailure, cleanupException);
			}
		}

		Connection connection = transaction.getExistingConnection().orElse(null);

		if (connection != null) {
			Duration heldDuration = transaction.getConnectionAcquiredAtNanos()
					.map(acquiredAtNanos -> Duration.ofNanos(nanoTime() - acquiredAtNanos))
					.orElse(Duration.ZERO);

			try {
				closeConnection(connection);
				getMetricsCollectorDispatcher().didReleaseTransactionConnection(transaction, transaction.getDatabaseType(), heldDuration);
				transaction.clearConnection();
			} catch (Throwable cleanupException) {
				getMetricsCollectorDispatcher().didFailToReleaseTransactionConnection(transaction, transaction.getDatabaseType(), heldDuration, cleanupException);
				cleanupFailure = appendSuppressed(cleanupFailure, cleanupException);
			}
		}

		return cleanupFailure;
	}

	@NonNull
	private static Throwable appendSuppressed(@Nullable Throwable existing,
																						@NonNull Throwable additional) {
		requireNonNull(additional);

		if (existing == null)
			return additional;

		existing.addSuppressed(additional);
		return existing;
	}

	protected void closeConnection(@NonNull Connection connection) {
		requireNonNull(connection);

		try {
			connection.close();
		} catch (SQLException e) {
			throw new DatabaseException("Unable to close database connection", e);
		}
	}

	@NonNull
	private static DatabaseException databaseExceptionWithStatementContext(@NonNull StatementContext<?> statementContext,
																																				@NonNull Throwable cause) {
		requireNonNull(statementContext);
		requireNonNull(cause);

		String message = cause.getMessage();

		if (message == null || message.trim().length() == 0)
			message = "Database operation failed";

		return databaseExceptionWithStatementContext(statementContext, message, cause);
	}

	@NonNull
	private static DatabaseException databaseExceptionWithStatementContext(@NonNull StatementContext<?> statementContext,
																																				@NonNull String message,
																																				@NonNull Throwable cause) {
		requireNonNull(statementContext);
		requireNonNull(message);
		requireNonNull(cause);

		return new DatabaseException(format("%s [%s]", boundedDiagnosticMessage(message), statementDiagnostic(statementContext)), cause);
	}

	@NonNull
	private static String statementDiagnostic(@NonNull StatementContext<?> statementContext) {
		requireNonNull(statementContext);

		Statement statement = statementContext.getStatement();
		return format("statementId=%s, sql=%s, parameterCount=%s",
				statement.getId(), boundedSql(statement.getSql()), statementContext.getParameters().size());
	}

	@NonNull
	private static TransactionResult transactionResult(@NonNull Boolean committed,
																										 @NonNull Boolean commitFailed) {
		requireNonNull(committed);
		requireNonNull(commitFailed);

		if (committed)
			return TransactionResult.COMMITTED;

		return commitFailed ? TransactionResult.IN_DOUBT : TransactionResult.ROLLED_BACK;
	}

	private static MetricsCollector.TransactionClosureOutcome transactionClosureOutcome(@NonNull Boolean committed,
																																										 @NonNull Boolean commitFailed,
																																										 @NonNull Boolean hadPhysicalTransaction,
																																										 @NonNull Boolean rollbackFailed) {
		requireNonNull(committed);
		requireNonNull(commitFailed);
		requireNonNull(hadPhysicalTransaction);
		requireNonNull(rollbackFailed);

		if (!hadPhysicalTransaction)
			return MetricsCollector.TransactionClosureOutcome.NO_PHYSICAL_TX;

		if (committed)
			return MetricsCollector.TransactionClosureOutcome.COMMITTED;

		if (commitFailed)
			return MetricsCollector.TransactionClosureOutcome.FAILED;

		return rollbackFailed
				? MetricsCollector.TransactionClosureOutcome.FAILED
				: MetricsCollector.TransactionClosureOutcome.ROLLED_BACK;
	}

	@Nullable
	static Long sumBatchUpdateCounts(@NonNull List<Long> updateCounts) {
		requireNonNull(updateCounts);

		long total = 0L;

		for (Long updateCount : updateCounts) {
			if (updateCount == null || updateCount < 0L)
				return null;

			try {
				total = Math.addExact(total, updateCount);
			} catch (ArithmeticException e) {
				return null;
			}
		}

		return total;
	}

	@NonNull
	private static String boundedDiagnosticMessage(@NonNull String message) {
		requireNonNull(message);

		String compactMessage = DIAGNOSTIC_WHITESPACE_PATTERN.matcher(message).replaceAll(" ").trim();

		if (compactMessage.length() <= MAX_DIAGNOSTIC_MESSAGE_LENGTH)
			return compactMessage;

		int prefixLength = Math.max(0, MAX_DIAGNOSTIC_MESSAGE_LENGTH - TRUNCATED_SUFFIX.length());
		return compactMessage.substring(0, prefixLength) + TRUNCATED_SUFFIX;
	}

	@NonNull
	private static String boundedSql(@NonNull String sql) {
		requireNonNull(sql);

		String compactSql = DIAGNOSTIC_WHITESPACE_PATTERN.matcher(sql).replaceAll(" ").trim();

		if (compactSql.length() <= MAX_DIAGNOSTIC_SQL_LENGTH)
			return compactSql;

		int prefixLength = Math.max(0, MAX_DIAGNOSTIC_SQL_LENGTH - TRUNCATED_SUFFIX.length());
		return compactSql.substring(0, prefixLength) + TRUNCATED_SUFFIX;
	}

	/**
	 * Performs an operation in the context of a pre-existing transaction.
	 * <p>
	 * No commit or rollback on the transaction will occur when {@code transactionalOperation} completes.
	 * <p>
	 * However, if an exception bubbles out of {@code transactionalOperation}, the transaction will be marked as rollback-only.
	 * <p>
	 * The transaction must have been created by this {@link Database}, or by another {@link Database} using the same
	 * {@link DataSource} instance.
	 *
	 * @param transaction            the transaction in which to participate
	 * @param transactionalOperation the operation that should participate in the transaction
	 */
	public void participate(@NonNull Transaction transaction,
													@NonNull TransactionalOperation transactionalOperation) {
		requireNonNull(transaction);
		requireNonNull(transactionalOperation);

		participate(transaction, () -> {
			transactionalOperation.perform();
			return Optional.empty();
		});
	}

	/**
	 * Performs an operation in the context of a pre-existing transaction, optionally returning a value.
	 * <p>
	 * No commit or rollback on the transaction will occur when {@code transactionalOperation} completes.
	 * <p>
	 * However, if an exception bubbles out of {@code transactionalOperation}, the transaction will be marked as rollback-only.
	 * <p>
	 * The transaction must have been created by this {@link Database}, or by another {@link Database} using the same
	 * {@link DataSource} instance.
	 *
	 * @param transaction            the transaction in which to participate
	 * @param transactionalOperation the operation that should participate in the transaction
	 * @param <T>                    the type to be returned
	 * @return the result of the transactional operation
	 */
	@NonNull
	public <T> Optional<T> participate(@NonNull Transaction transaction,
																		 @NonNull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transaction);
		requireNonNull(transactionalOperation);

		if (!isTransactionOwnedByThisDatabase(transaction))
			throw wrongDatabaseTransactionException(transaction);

		if (transaction.isCompleted())
			throw new IllegalStateException(format("Transaction %s has already completed and cannot participate", transaction.id()));

		Deque<Transaction> transactionStack = transactionStackForPush();
		transactionStack.push(transaction);

		try {
			Optional<T> returnValue = transactionalOperation.perform();
			return returnValue == null ? Optional.empty() : returnValue;
		} catch (RuntimeException e) {
			setRollbackOnlyAfterParticipationFailure(transaction, e);
			restoreInterruptIfNeeded(e);
			throw e;
		} catch (Error e) {
			setRollbackOnlyAfterParticipationFailure(transaction, e);
			restoreInterruptIfNeeded(e);
			throw e;
		} catch (Throwable t) {
			RuntimeException wrapped = new RuntimeException(t);
			setRollbackOnlyAfterParticipationFailure(transaction, wrapped);
			restoreInterruptIfNeeded(t);
			throw wrapped;
		} finally {
			try {
				transactionStack.pop();
			} finally {
				if (transactionStack.isEmpty())
					TRANSACTION_STACK_HOLDER.remove();
			}
		}
	}

	private void setRollbackOnlyAfterParticipationFailure(@NonNull Transaction transaction,
																												@NonNull Throwable primary) {
		requireNonNull(transaction);
		requireNonNull(primary);

		try {
			transaction.setRollbackOnly(true);
		} catch (IllegalStateException e) {
			primary.addSuppressed(e);
		}
	}

	/**
	 * Creates a fluent builder for executing SQL.
	 * <p>
	 * Named parameters use the {@code :paramName} syntax and are bound via {@link Query#bind(String, Object)}.
	 * Positional parameters via {@code ?} are not supported.
	 * Pyranid ignores parameter-looking text inside SQL string literals, quoted identifiers, comments, PostgreSQL
	 * dollar-quoted strings, and SQL Server-style bracket-quoted identifiers. PostgreSQL JSONB/hstore {@code ?},
	 * {@code ?|}, and {@code ?&} operators are supported; when running against {@link DatabaseType#POSTGRESQL}, Pyranid
	 * emits pgjdbc's escaped {@code ??} form automatically. Unterminated quotes and comments fail fast.
	 * <p>
	 * Example:
	 * <pre>{@code
	 * Optional<Employee> employee = database.query("SELECT * FROM employee WHERE id = :id")
	 *   .bind("id", 42)
	 *   .fetchObject(Employee.class);
	 * }</pre>
	 *
	 * @param sql SQL containing {@code :paramName} placeholders
	 * @return a fluent builder for binding parameters and executing
	 * @since 4.0.0
	 */
	@NonNull
	public Query query(@NonNull String sql) {
		requireNonNull(sql);
		return new DefaultQuery(this, sql);
	}

	private static void restoreInterruptIfNeeded(@NonNull Throwable throwable) {
		requireNonNull(throwable);

		Throwable current = throwable;

		while (current != null) {
			if (current instanceof InterruptedException) {
				Thread.currentThread().interrupt();
				return;
			}

			current = current.getCause();
		}
	}

	@Nullable
	private static Object unwrapOptionalValue(@Nullable Object value) {
		if (value == null)
			return null;

		if (value instanceof Optional<?> optional)
			return optional.orElse(null);
		if (value instanceof OptionalInt optionalInt)
			return optionalInt.isPresent() ? optionalInt.getAsInt() : null;
		if (value instanceof OptionalLong optionalLong)
			return optionalLong.isPresent() ? optionalLong.getAsLong() : null;
		if (value instanceof OptionalDouble optionalDouble)
			return optionalDouble.isPresent() ? optionalDouble.getAsDouble() : null;

		return value;
	}

	@Nullable
	private static Throwable closeStatementContextResources(@NonNull StatementContext<?> statementContext,
																													@Nullable Throwable cleanupFailure) {
		requireNonNull(statementContext);

		Queue<AutoCloseable> cleanupOperations = statementContext.getCleanupOperations();
		AutoCloseable cleanupOperation;

		while ((cleanupOperation = cleanupOperations.poll()) != null) {
			try {
				cleanupOperation.close();
			} catch (Throwable cleanupException) {
				if (cleanupFailure == null)
					cleanupFailure = cleanupException;
				else
					cleanupFailure.addSuppressed(cleanupException);
			}
		}

		return cleanupFailure;
	}

	private static boolean isUnsupportedSqlFeature(@NonNull SQLException e) {
		requireNonNull(e);

		String sqlState = e.getSQLState();
		if (sqlState != null) {
			if (sqlState.startsWith("0A") || "HYC00".equals(sqlState))
				return true;
		}

		Throwable cause = e.getCause();
		if (cause instanceof SQLFeatureNotSupportedException
				|| cause instanceof UnsupportedOperationException
				|| cause instanceof AbstractMethodError)
			return true;

		String message = e.getMessage();
		if (message == null)
			return false;

		String lower = message.toLowerCase(Locale.ROOT);
		return lower.contains("not supported")
				|| lower.contains("unsupported")
				|| lower.contains("not implemented")
				|| lower.contains("feature not supported");
	}

	@Nullable
	private static Duration validateQueryTimeout(@Nullable Duration queryTimeout) {
		if (queryTimeout != null) {
			if (queryTimeout.isNegative())
				throw new IllegalArgumentException("queryTimeout must be >= 0");

			queryTimeoutSeconds(queryTimeout);
		}

		return queryTimeout;
	}

	@Nullable
	private static Integer validateNonNegativeStatementSetting(@NonNull String name,
																														 @Nullable Integer value) {
		requireNonNull(name);

		if (value != null && value < 0)
			throw new IllegalArgumentException(format("%s must be >= 0", name));

		return value;
	}

	private static int queryTimeoutSeconds(@NonNull Duration queryTimeout) {
		requireNonNull(queryTimeout);

		long seconds = queryTimeout.getSeconds();

		if (queryTimeout.getNano() > 0) {
			if (seconds == Long.MAX_VALUE)
				throw new IllegalArgumentException(format("queryTimeout must be <= %s seconds", Integer.MAX_VALUE));

			++seconds;
		}

		if (seconds > Integer.MAX_VALUE)
			throw new IllegalArgumentException(format("queryTimeout must be <= %s seconds", Integer.MAX_VALUE));

		return (int) seconds;
	}

	@NonNull
	private ParsedSql getParsedSql(@NonNull String sql) {
		requireNonNull(sql);

		if (this.parsedSqlCache == null)
			return parseNamedParameterSql(sql);

		return this.parsedSqlCache.computeIfAbsent(sql, Database::parseNamedParameterSql);
	}

	/**
	 * Default internal implementation of {@link Query}.
	 * <p>
	 * This class is intended for use by a single thread.
	 */
	@NotThreadSafe
	private static final class DefaultQuery implements Query {
		@NonNull
		private final Database database;
		@NonNull
		private final String originalSql;
		@NonNull
		private final ParsedSql parsedSql;
		@NonNull
		private final List<String> sqlFragments;
		@NonNull
		private final List<String> parameterNames;
		@NonNull
		private final Set<String> distinctParameterNames;
		@NonNull
		private final Map<String, Object> bindings;
		@Nullable
		private PreparedStatementCustomizer preparedStatementCustomizer;
		@Nullable
		private Duration queryTimeout;
		@Nullable
		private Integer fetchSize;
		@Nullable
		private Integer maxRows;
		@Nullable
		private Object id;

		private DefaultQuery(@NonNull Database database,
												 @NonNull String sql) {
			requireNonNull(database);
			requireNonNull(sql);

			this.database = database;
			this.originalSql = sql;

			ParsedSql parsedSql = database.getParsedSql(sql);
			this.parsedSql = parsedSql;
			this.sqlFragments = parsedSql.sqlFragments;
			this.parameterNames = parsedSql.parameterNames;
			this.distinctParameterNames = parsedSql.distinctParameterNames;

			this.bindings = new LinkedHashMap<>(Math.max(8, this.distinctParameterNames.size()));
			this.preparedStatementCustomizer = null;
			this.queryTimeout = null;
			this.fetchSize = null;
			this.maxRows = null;
		}

		@NonNull
		@Override
		public Query bind(@NonNull String name,
											@Nullable Object value) {
			requireNonNull(name);

			if (!this.distinctParameterNames.contains(name))
				throw new IllegalArgumentException(format("Unknown named parameter '%s' for SQL: %s", name, this.originalSql));

			this.bindings.put(name, value);
			return this;
		}

		@NonNull
		@Override
		public Query bindAll(@NonNull Map<@NonNull String, @Nullable Object> parameters) {
			requireNonNull(parameters);

			for (Map.Entry<@NonNull String, @Nullable Object> entry : parameters.entrySet())
				bind(entry.getKey(), entry.getValue());

			return this;
		}

		@NonNull
		@Override
		public Query id(@Nullable Object id) {
			this.id = id;
			return this;
		}

		@NonNull
		@Override
		public Query queryTimeout(@Nullable Duration queryTimeout) {
			this.queryTimeout = validateQueryTimeout(queryTimeout);
			return this;
		}

		@NonNull
		@Override
		public Query fetchSize(@Nullable Integer fetchSize) {
			this.fetchSize = validateNonNegativeStatementSetting("fetchSize", fetchSize);
			return this;
		}

		@NonNull
		@Override
		public Query maxRows(@Nullable Integer maxRows) {
			this.maxRows = validateNonNegativeStatementSetting("maxRows", maxRows);
			return this;
		}

		@NonNull
		@Override
		public Query customize(@NonNull PreparedStatementCustomizer preparedStatementCustomizer) {
			requireNonNull(preparedStatementCustomizer);
			this.preparedStatementCustomizer = preparedStatementCustomizer;

			return this;
		}

		@NonNull
		@Override
		public <T> Optional<T> fetchObject(@NonNull Class<T> resultType) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.queryForObject(preparedQuery.statement, resultType, effectivePreparedStatementCustomizer(), preparedQuery.parameters);
		}

		@NonNull
		@Override
		public <T> List<@Nullable T> fetchList(@NonNull Class<T> resultType) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.queryForList(preparedQuery.statement, resultType, effectivePreparedStatementCustomizer(), preparedQuery.parameters);
		}

		@Nullable
		@Override
		public <T, R> R fetchStream(@NonNull Class<T> resultType,
																@NonNull Function<Stream<@Nullable T>, R> streamFunction) {
			requireNonNull(resultType);
			requireNonNull(streamFunction);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.queryForStream(preparedQuery.statement, resultType, effectivePreparedStatementCustomizer(), streamFunction, preparedQuery.parameters);
		}


		@NonNull
		@Override
		public Long execute() {
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.execute(preparedQuery.statement, effectivePreparedStatementCustomizer(), preparedQuery.parameters);
		}

		@NonNull
		@Override
		public <T> Optional<T> executeReturningGeneratedKey(@NonNull Class<T> resultType) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.executeReturningGeneratedKey(preparedQuery.statement, resultType,
					effectivePreparedStatementCustomizer(), new String[0], preparedQuery.parameters);
		}

		@NonNull
		@Override
		public <T> Optional<T> executeReturningGeneratedKey(@NonNull Class<T> resultType,
																												@NonNull String @NonNull ... keyColumnNames) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.executeReturningGeneratedKey(preparedQuery.statement, resultType,
					effectivePreparedStatementCustomizer(), keyColumnNames, preparedQuery.parameters);
		}

		@NonNull
		@Override
		public <T> List<@Nullable T> executeReturningGeneratedKeys(@NonNull Class<T> resultType) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.executeReturningGeneratedKeys(preparedQuery.statement, resultType,
					effectivePreparedStatementCustomizer(), new String[0], preparedQuery.parameters);
		}

		@NonNull
		@Override
		public <T> List<@Nullable T> executeReturningGeneratedKeys(@NonNull Class<T> resultType,
																													 @NonNull String @NonNull ... keyColumnNames) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.executeReturningGeneratedKeys(preparedQuery.statement, resultType,
					effectivePreparedStatementCustomizer(), keyColumnNames, preparedQuery.parameters);
		}

		@NonNull
		@Override
		public List<Long> executeBatch(@NonNull List<@NonNull Map<@NonNull String, @Nullable Object>> parameterGroups) {
			requireNonNull(parameterGroups);
			if (parameterGroups.isEmpty())
				return List.of();

			List<List<Object>> parametersAsList = new ArrayList<>(parameterGroups.size());
			Object statementId = this.id == null ? this.database.generateId() : this.id;
			Statement statement = null;
			String expandedSql = null;

			for (Map<@NonNull String, @Nullable Object> parameterGroup : parameterGroups) {
				requireNonNull(parameterGroup);

				for (String parameterName : parameterGroup.keySet())
					if (!this.distinctParameterNames.contains(parameterName))
						throw new IllegalArgumentException(format("Unknown named parameter '%s' for SQL: %s", parameterName, this.originalSql));

				Map<String, Object> mergedBindings;
				if (this.bindings.isEmpty()) {
					mergedBindings = parameterGroup;
				} else if (parameterGroup.isEmpty()) {
					mergedBindings = this.bindings;
				} else {
					Map<String, Object> combinedBindings = new LinkedHashMap<>(this.bindings);
					combinedBindings.putAll(parameterGroup);
					mergedBindings = combinedBindings;
				}

				PreparedQuery preparedQuery = prepare(mergedBindings, statementId);

				if (expandedSql == null) {
					expandedSql = preparedQuery.statement.getSql();
					statement = preparedQuery.statement;
				} else if (!expandedSql.equals(preparedQuery.statement.getSql())) {
					throw new IllegalArgumentException(format(
							"Inconsistent SQL after expanding parameters for batch execution; ensure collection sizes are consistent. SQL: %s",
							this.originalSql));
				}

				parametersAsList.add(Arrays.asList(preparedQuery.parameters));
			}

			if (statement == null)
				statement = Statement.of(statementId, buildPlaceholderSql());

			return this.database.executeBatch(statement, parametersAsList, effectivePreparedStatementCustomizer());
		}

		@NonNull
		@Override
		public <T> Optional<T> executeForObject(@NonNull Class<T> resultType) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.executeForObject(preparedQuery.statement, resultType, effectivePreparedStatementCustomizer(), preparedQuery.parameters);
		}

		@NonNull
		@Override
		public <T> List<@Nullable T> executeForList(@NonNull Class<T> resultType) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.executeForList(preparedQuery.statement, resultType, effectivePreparedStatementCustomizer(), preparedQuery.parameters);
		}

		@Nullable
		private PreparedStatementCustomizer effectivePreparedStatementCustomizer() {
			if (!this.database.hasDefaultPreparedStatementSettings()
					&& !hasQueryPreparedStatementSettings()
					&& this.preparedStatementCustomizer == null)
				return null;

			return (statementContext, preparedStatement) -> {
				this.database.applyDefaultPreparedStatementSettings(preparedStatement);
				applyPreparedStatementSettings(preparedStatement, this.queryTimeout, this.fetchSize, this.maxRows);

				if (this.preparedStatementCustomizer != null)
					this.preparedStatementCustomizer.customize(statementContext, preparedStatement);
			};
		}

		private boolean hasQueryPreparedStatementSettings() {
			return this.queryTimeout != null || this.fetchSize != null || this.maxRows != null;
		}

		@NonNull
		private PreparedQuery prepare(@NonNull Map<String, Object> bindings) {
			Object statementId = this.id == null ? this.database.generateId() : this.id;
			return prepare(bindings, statementId);
		}

		@NonNull
		private PreparedQuery prepare(@NonNull Map<String, Object> bindings,
																	@NonNull Object statementId) {
			requireNonNull(bindings);
			requireNonNull(statementId);

			List<String> sqlFragments = sqlFragmentsForDatabaseType();

			if (this.parameterNames.isEmpty())
				return new PreparedQuery(Statement.of(statementId, sqlFragments.get(0)), new Object[0]);

			StringBuilder sql = new StringBuilder(this.originalSql.length() + this.parameterNames.size() * 2);
			List<String> missingParameterNames = null;
			List<Object> parameters = new ArrayList<>(this.parameterNames.size());

			for (int i = 0; i < this.parameterNames.size(); ++i) {
				String parameterName = this.parameterNames.get(i);
				sql.append(sqlFragments.get(i));

				if (!bindings.containsKey(parameterName)) {
					if (missingParameterNames == null)
						missingParameterNames = new ArrayList<>();

					missingParameterNames.add(parameterName);
					sql.append('?');
					continue;
				}

				Object value = unwrapOptionalValue(bindings.get(parameterName));

				if (value instanceof InListParameter inListParameter) {
					Object[] elements = inListParameter.getElements();

					if (elements.length == 0)
						throw new IllegalArgumentException(format("IN-list parameter '%s' for SQL: %s is empty", parameterName, this.originalSql));

					appendPlaceholders(sql, elements.length);

					for (int j = 0; j < elements.length; ++j) {
						Object element = unwrapOptionalValue(elements[j]);

						if (element == null)
							throw new IllegalArgumentException(format(
									"IN-list parameter '%s' for SQL: %s contains null element at index %d. "
											+ "SQL IN does not match NULL values; use an explicit IS NULL predicate instead.",
									parameterName, this.originalSql, j));

						parameters.add(element);
					}
				} else if (value instanceof Collection<?>) {
					throw new IllegalArgumentException(format(
							"Collection parameter '%s' for SQL: %s must be wrapped with %s.inList(...) or %s.listOf/%s.setOf(...)",
							parameterName, this.originalSql,
							Parameters.class.getSimpleName(),
							Parameters.class.getSimpleName(), Parameters.class.getSimpleName()));
				} else if (value != null && value.getClass().isArray() && !(value instanceof byte[])) {
					throw new IllegalArgumentException(format(
							"Array parameter '%s' for SQL: %s must be wrapped with %s.inList(...), %s.sqlArrayOf(...), or %s.arrayOf(Class, ...)",
							parameterName, this.originalSql,
							Parameters.class.getSimpleName(), Parameters.class.getSimpleName(), Parameters.class.getSimpleName()));
				} else {
					sql.append('?');
					parameters.add(value);
				}
			}

			sql.append(sqlFragments.get(sqlFragments.size() - 1));

			if (missingParameterNames != null)
				throw new IllegalArgumentException(format("Missing required named parameters %s for SQL: %s", missingParameterNames, this.originalSql));

			return new PreparedQuery(Statement.of(statementId, sql.toString()), parameters.toArray());
		}

		@NonNull
		private String buildPlaceholderSql() {
			List<String> sqlFragments = sqlFragmentsForDatabaseType();

			if (this.parameterNames.isEmpty())
				return sqlFragments.get(0);

			StringBuilder sql = new StringBuilder(this.originalSql.length() + this.parameterNames.size() * 2);

			for (int i = 0; i < this.parameterNames.size(); ++i)
				sql.append(sqlFragments.get(i)).append('?');

			sql.append(sqlFragments.get(sqlFragments.size() - 1));
			return sql.toString();
		}

		@NonNull
		private List<String> sqlFragmentsForDatabaseType() {
			if (!this.parsedSql.hasQuestionMarkOperators)
				return this.sqlFragments;

			return this.database.getDatabaseType() == DatabaseType.POSTGRESQL
					? this.parsedSql.postgresqlSqlFragments
					: this.sqlFragments;
		}

		private void appendPlaceholders(@NonNull StringBuilder sql,
																		int count) {
			requireNonNull(sql);

			for (int i = 0; i < count; ++i) {
				if (i > 0)
					sql.append(", ");
				sql.append('?');
			}
		}

		private static final class PreparedQuery {
			@NonNull
			private final Statement statement;
			@NonNull
			private final Object @NonNull [] parameters;

			private PreparedQuery(@NonNull Statement statement,
														Object @NonNull [] parameters) {
				this.statement = requireNonNull(statement);
				this.parameters = requireNonNull(parameters);
			}
		}

	}

	static final class ParsedSql {
		@NonNull
		private final List<String> sqlFragments;
		@NonNull
		private final List<String> postgresqlSqlFragments;
		private final boolean hasQuestionMarkOperators;
		@NonNull
		private final List<String> parameterNames;
		@NonNull
		private final Set<String> distinctParameterNames;

		private ParsedSql(@NonNull List<String> sqlFragments,
											@NonNull List<@NonNull List<@NonNull Integer>> questionMarkOperatorFragmentIndexes,
											@NonNull List<String> parameterNames,
											@NonNull Set<String> distinctParameterNames) {
			requireNonNull(sqlFragments);
			requireNonNull(questionMarkOperatorFragmentIndexes);
			requireNonNull(parameterNames);
			requireNonNull(distinctParameterNames);

			this.sqlFragments = sqlFragments;
			this.postgresqlSqlFragments = postgresqlSqlFragments(sqlFragments, questionMarkOperatorFragmentIndexes);
			this.hasQuestionMarkOperators = questionMarkOperatorFragmentIndexes.stream().anyMatch(indexes -> !indexes.isEmpty());
			this.parameterNames = parameterNames;
			this.distinctParameterNames = distinctParameterNames;
		}

		@NonNull
		private static List<String> postgresqlSqlFragments(@NonNull List<String> sqlFragments,
																											 @NonNull List<@NonNull List<@NonNull Integer>> questionMarkOperatorFragmentIndexes) {
			requireNonNull(sqlFragments);
			requireNonNull(questionMarkOperatorFragmentIndexes);

			if (sqlFragments.size() != questionMarkOperatorFragmentIndexes.size())
				throw new IllegalArgumentException("SQL fragments and question-mark operator indexes must have the same size");

			List<String> postgresqlSqlFragments = new ArrayList<>(sqlFragments.size());

			for (int i = 0; i < sqlFragments.size(); ++i)
				postgresqlSqlFragments.add(postgresqlSqlFragment(sqlFragments.get(i), questionMarkOperatorFragmentIndexes.get(i)));

			return List.copyOf(postgresqlSqlFragments);
		}

		@NonNull
		private static String postgresqlSqlFragment(@NonNull String sqlFragment,
																							 @NonNull List<@NonNull Integer> questionMarkOperatorIndexes) {
			requireNonNull(sqlFragment);
			requireNonNull(questionMarkOperatorIndexes);

			if (questionMarkOperatorIndexes.isEmpty())
				return sqlFragment;

			StringBuilder postgresqlSqlFragment = new StringBuilder(sqlFragment.length() + questionMarkOperatorIndexes.size());
			int previousIndex = 0;

			for (Integer questionMarkOperatorIndex : questionMarkOperatorIndexes) {
				if (questionMarkOperatorIndex == null || questionMarkOperatorIndex < previousIndex || questionMarkOperatorIndex >= sqlFragment.length()
						|| sqlFragment.charAt(questionMarkOperatorIndex) != '?')
					throw new IllegalArgumentException("Invalid question-mark operator index");

				postgresqlSqlFragment.append(sqlFragment, previousIndex, questionMarkOperatorIndex);
				postgresqlSqlFragment.append("??");
				previousIndex = questionMarkOperatorIndex + 1;
			}

			postgresqlSqlFragment.append(sqlFragment, previousIndex, sqlFragment.length());
			return postgresqlSqlFragment.toString();
		}
	}

	@NonNull
	static ParsedSql parseNamedParameterSql(@NonNull String sql) {
		requireNonNull(sql);

		List<String> sqlFragments = new ArrayList<>();
		StringBuilder sqlFragment = new StringBuilder(sql.length());
		List<List<Integer>> questionMarkOperatorFragmentIndexes = new ArrayList<>();
		List<Integer> currentQuestionMarkOperatorIndexes = new ArrayList<>();
		List<String> parameterNames = new ArrayList<>();
		Set<String> distinctParameterNames = new HashSet<>();

		boolean inSingleQuote = false;
		boolean inSingleQuoteEscapesBackslash = false;
		int singleQuoteStartIndex = -1;
		boolean inDoubleQuote = false;
		int doubleQuoteStartIndex = -1;
		boolean inBacktickQuote = false;
		int backtickQuoteStartIndex = -1;
		boolean inBracketQuote = false;
		int bracketQuoteStartIndex = -1;
		boolean inLineComment = false;
		int blockCommentDepth = 0;
		int blockCommentStartIndex = -1;
		String dollarQuoteDelimiter = null;
		int dollarQuoteStartIndex = -1;
		int previousMeaningfulIndex = -1;

		for (int i = 0; i < sql.length(); ) {
			if (dollarQuoteDelimiter != null) {
				if (sql.startsWith(dollarQuoteDelimiter, i)) {
					sqlFragment.append(dollarQuoteDelimiter);
					previousMeaningfulIndex = i + dollarQuoteDelimiter.length() - 1;
					i += dollarQuoteDelimiter.length();
					dollarQuoteDelimiter = null;
					dollarQuoteStartIndex = -1;
				} else {
					sqlFragment.append(sql.charAt(i));
					++i;
				}

				continue;
			}

			char c = sql.charAt(i);

			if (inLineComment) {
				sqlFragment.append(c);
				++i;

				if (c == '\n' || c == '\r')
					inLineComment = false;

				continue;
			}

			if (blockCommentDepth > 0) {
				if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
					sqlFragment.append("/*");
					i += 2;
					++blockCommentDepth;
				} else if (c == '*' && i + 1 < sql.length() && sql.charAt(i + 1) == '/') {
					sqlFragment.append("*/");
					i += 2;
					--blockCommentDepth;
					if (blockCommentDepth == 0)
						blockCommentStartIndex = -1;
				} else {
					sqlFragment.append(c);
					++i;
				}

				continue;
			}

			if (inSingleQuote) {
				sqlFragment.append(c);

				if (inSingleQuoteEscapesBackslash && c == '\\' && i + 1 < sql.length()) {
					sqlFragment.append(sql.charAt(i + 1));
					i += 2;
					continue;
				}

				if (c == '\'') {
					// Escaped quote: ''
					if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
						sqlFragment.append('\'');
						i += 2;
						continue;
					}

					inSingleQuote = false;
					inSingleQuoteEscapesBackslash = false;
					singleQuoteStartIndex = -1;
					previousMeaningfulIndex = i;
				}

				++i;
				continue;
			}

			if (inDoubleQuote) {
				sqlFragment.append(c);

				if (c == '"') {
					// Escaped quote: ""
					if (i + 1 < sql.length() && sql.charAt(i + 1) == '"') {
						sqlFragment.append('"');
						i += 2;
						continue;
					}

					inDoubleQuote = false;
					doubleQuoteStartIndex = -1;
					previousMeaningfulIndex = i;
				}

				++i;
				continue;
			}

			if (inBacktickQuote) {
				sqlFragment.append(c);

				if (c == '`') {
					inBacktickQuote = false;
					backtickQuoteStartIndex = -1;
					previousMeaningfulIndex = i;
				}

				++i;
				continue;
			}

			if (inBracketQuote) {
				sqlFragment.append(c);

				if (c == ']' && i + 1 < sql.length() && sql.charAt(i + 1) == ']') {
					sqlFragment.append(']');
					i += 2;
					continue;
				}

				if (c == ']') {
					inBracketQuote = false;
					bracketQuoteStartIndex = -1;
					previousMeaningfulIndex = i;
				}

				++i;
				continue;
			}

			// Not inside string/comment
			if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
				sqlFragment.append("--");
				i += 2;
				inLineComment = true;
				continue;
			}

			if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
				sqlFragment.append("/*");
				i += 2;
				blockCommentDepth = 1;
				blockCommentStartIndex = i - 2;
				continue;
			}

			if ((c == 'U' || c == 'u') && !isIdentifierContinuation(sql, i)
					&& i + 2 < sql.length() && sql.charAt(i + 1) == '&' && sql.charAt(i + 2) == '\'') {
				inSingleQuote = true;
				inSingleQuoteEscapesBackslash = true;
				singleQuoteStartIndex = i;
				sqlFragment.append(c).append("&'");
				i += 3;
				continue;
			}

			if ((c == 'E' || c == 'e') && !isIdentifierContinuation(sql, i)
					&& i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
				inSingleQuote = true;
				inSingleQuoteEscapesBackslash = true;
				singleQuoteStartIndex = i;
				sqlFragment.append(c).append('\'');
				i += 2;
				continue;
			}

			if (c == '\'') {
				inSingleQuote = true;
				inSingleQuoteEscapesBackslash = false;
				singleQuoteStartIndex = i;
				sqlFragment.append(c);
				++i;
				continue;
			}

			if (c == '"') {
				inDoubleQuote = true;
				doubleQuoteStartIndex = i;
				sqlFragment.append(c);
				++i;
				continue;
			}

			if (c == '`') {
				inBacktickQuote = true;
				backtickQuoteStartIndex = i;
				sqlFragment.append(c);
				++i;
				continue;
			}

			if (c == '[' && !isBracketSubscriptStart(sql, i)) {
				inBracketQuote = true;
				bracketQuoteStartIndex = i;
				sqlFragment.append(c);
				++i;
				continue;
			}

			if (c == '$' && !isIdentifierContinuation(sql, i)) {
				String delimiter = parseDollarQuoteDelimiter(sql, i);

				if (delimiter != null) {
					sqlFragment.append(delimiter);
					i += delimiter.length();
					dollarQuoteDelimiter = delimiter;
					dollarQuoteStartIndex = i - delimiter.length();
					continue;
				}
			}

			if (c == '?') {
				if (isAllowedQuestionMarkOperator(sql, i, previousMeaningfulIndex)) {
					currentQuestionMarkOperatorIndexes.add(sqlFragment.length());
					sqlFragment.append(c);
					++i;
					continue;
				}

				throw new IllegalArgumentException(format("Positional parameters ('?') are not supported. Use named parameters (e.g. ':id') and %s#bind. SQL: %s",
						Query.class.getSimpleName(), sql));
			}

			if (c == ':' && i + 1 < sql.length() && sql.charAt(i + 1) == ':') {
				// Postgres type-cast operator (::), do not treat second ':' as a parameter prefix.
				sqlFragment.append("::");
				i += 2;
				continue;
			}

			if (c == ':' && i + 1 < sql.length() && Character.isJavaIdentifierStart(sql.charAt(i + 1))) {
				int nameStartIndex = i + 1;
				int nameEndIndex = nameStartIndex + 1;

				while (nameEndIndex < sql.length() && Character.isJavaIdentifierPart(sql.charAt(nameEndIndex)))
					++nameEndIndex;

				String parameterName = sql.substring(nameStartIndex, nameEndIndex);
				parameterNames.add(parameterName);
				distinctParameterNames.add(parameterName);
				sqlFragments.add(sqlFragment.toString());
				questionMarkOperatorFragmentIndexes.add(List.copyOf(currentQuestionMarkOperatorIndexes));
				currentQuestionMarkOperatorIndexes.clear();
				sqlFragment.setLength(0);
				i = nameEndIndex;
				previousMeaningfulIndex = nameEndIndex - 1;
				continue;
			}

			sqlFragment.append(c);
			if (!Character.isWhitespace(c))
				previousMeaningfulIndex = i;
			++i;
		}

		validateParserTerminalState(sql, inSingleQuote, singleQuoteStartIndex, inDoubleQuote, doubleQuoteStartIndex,
				inBacktickQuote, backtickQuoteStartIndex, inBracketQuote, bracketQuoteStartIndex,
				blockCommentDepth, blockCommentStartIndex, dollarQuoteDelimiter, dollarQuoteStartIndex);

		sqlFragments.add(sqlFragment.toString());
		questionMarkOperatorFragmentIndexes.add(List.copyOf(currentQuestionMarkOperatorIndexes));

		return new ParsedSql(List.copyOf(sqlFragments), List.copyOf(questionMarkOperatorFragmentIndexes),
				List.copyOf(parameterNames), Set.copyOf(distinctParameterNames));
	}

	@Nullable
	private static String parseDollarQuoteDelimiter(@NonNull String sql,
																									int startIndex) {
		requireNonNull(sql);

		if (startIndex < 0 || startIndex >= sql.length())
			return null;

		if (sql.charAt(startIndex) != '$')
			return null;

		int i = startIndex + 1;

		if (i >= sql.length())
			return null;

		char firstTagCharacter = sql.charAt(i);

		if (firstTagCharacter == '$')
			return "$$";

		if (!isDollarQuoteTagStart(firstTagCharacter))
			return null;

		++i;

		while (i < sql.length()) {
			char c = sql.charAt(i);

			if (c == '$')
				return sql.substring(startIndex, i + 1);

			if (!isDollarQuoteTagPart(c))
				return null;

			++i;
		}

		return null;
	}

	private static boolean isDollarQuoteTagStart(char character) {
		return Character.isLetter(character) || character == '_';
	}

	private static boolean isDollarQuoteTagPart(char character) {
		return Character.isLetterOrDigit(character) || character == '_';
	}

	private static boolean isIdentifierContinuation(@NonNull String sql,
																								 int startIndex) {
		requireNonNull(sql);

		if (startIndex <= 0)
			return false;

		return Character.isJavaIdentifierPart(sql.charAt(startIndex - 1));
	}

	private static boolean isBracketSubscriptStart(@NonNull String sql,
																								 int startIndex) {
		requireNonNull(sql);

		if (startIndex <= 0)
			return false;

		char previousChar = sql.charAt(startIndex - 1);
		return Character.isJavaIdentifierPart(previousChar)
				|| previousChar == ')'
				|| previousChar == ']'
				|| previousChar == '"';
	}

	private static void validateParserTerminalState(@NonNull String sql,
																									boolean inSingleQuote,
																									int singleQuoteStartIndex,
																									boolean inDoubleQuote,
																									int doubleQuoteStartIndex,
																									boolean inBacktickQuote,
																									int backtickQuoteStartIndex,
																									boolean inBracketQuote,
																									int bracketQuoteStartIndex,
																									int blockCommentDepth,
																									int blockCommentStartIndex,
																									@Nullable String dollarQuoteDelimiter,
																									int dollarQuoteStartIndex) {
		requireNonNull(sql);

		if (inSingleQuote)
			throw unterminatedSqlConstructException("single-quoted string", singleQuoteStartIndex, sql);
		if (inDoubleQuote)
			throw unterminatedSqlConstructException("double-quoted identifier", doubleQuoteStartIndex, sql);
		if (inBacktickQuote)
			throw unterminatedSqlConstructException("backtick-quoted identifier", backtickQuoteStartIndex, sql);
		if (inBracketQuote)
			throw unterminatedSqlConstructException("bracket-quoted identifier", bracketQuoteStartIndex, sql);
		if (blockCommentDepth > 0)
			throw unterminatedSqlConstructException("block comment", blockCommentStartIndex, sql);
		if (dollarQuoteDelimiter != null)
			throw unterminatedSqlConstructException(format("dollar-quoted string %s", dollarQuoteDelimiter), dollarQuoteStartIndex, sql);
	}

	@NonNull
	private static IllegalArgumentException unterminatedSqlConstructException(@NonNull String construct,
																																			 int startIndex,
																																			 @NonNull String sql) {
		requireNonNull(construct);
		requireNonNull(sql);
		return new IllegalArgumentException(format("Unterminated %s starting at index %s. SQL: %s", construct, startIndex, sql));
	}

	@NonNull
	private static final Set<@NonNull String> QUESTION_MARK_PREFIX_KEYWORDS = Set.of(
			"SELECT", "WHERE", "AND", "OR", "ON", "HAVING", "WHEN", "THEN", "ELSE", "IN",
			"VALUES", "SET", "RETURNING", "USING", "LIKE", "BETWEEN", "IS", "NOT", "NULL",
			"JOIN", "FROM"
	);

	@NonNull
	private static final Set<@NonNull String> QUESTION_MARK_SUFFIX_KEYWORDS = Set.of(
			"FROM", "WHERE", "AND", "OR", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET",
			"UNION", "EXCEPT", "INTERSECT", "RETURNING", "JOIN", "ON"
	);

	private static boolean isAllowedQuestionMarkOperator(@NonNull String sql,
																											 int questionMarkIndex,
																											 int previousMeaningfulIndex) {
		requireNonNull(sql);

		int previousIndex = previousMeaningfulIndex;
		int nextIndex = nextNonWhitespaceIndex(sql, questionMarkIndex + 1);

		if (previousIndex < 0 || nextIndex < 0)
			return false;

		char previousChar = sql.charAt(previousIndex);
		char nextChar = sql.charAt(nextIndex);

		if (isOperatorBeforeQuestionMark(previousChar))
			return false;

		if (isTerminatorAfterQuestionMark(nextChar))
			return false;

		String previousKeyword = keywordBefore(sql, previousIndex);
		if (previousKeyword != null && QUESTION_MARK_PREFIX_KEYWORDS.contains(previousKeyword))
			return false;

		String nextKeyword = keywordAfter(sql, nextIndex);
		if (nextKeyword != null && QUESTION_MARK_SUFFIX_KEYWORDS.contains(nextKeyword))
			return false;

		if (questionMarkIndex + 1 < sql.length()) {
			char immediateNextChar = sql.charAt(questionMarkIndex + 1);
			if (immediateNextChar == '|' || immediateNextChar == '&')
				return true;
		}

		return true;
	}

	private static boolean isOperatorBeforeQuestionMark(char c) {
		return switch (c) {
			case '=', '<', '>', '!', '+', '-', '*', '/', '%', ',', '(' -> true;
			default -> false;
		};
	}

	private static boolean isTerminatorAfterQuestionMark(char c) {
		return switch (c) {
			case ')', ',', ';' -> true;
			default -> false;
		};
	}

	private static int nextNonWhitespaceIndex(@NonNull String sql,
																						int startIndex) {
		for (int i = startIndex; i < sql.length(); i++)
			if (!Character.isWhitespace(sql.charAt(i)))
				return i;
		return -1;
	}

	@Nullable
	private static String keywordBefore(@NonNull String sql,
																			int index) {
		char c = sql.charAt(index);
		if (!Character.isJavaIdentifierPart(c))
			return null;

		int endIndex = index + 1;
		int startIndex = index;
		while (startIndex >= 0 && Character.isJavaIdentifierPart(sql.charAt(startIndex)))
			--startIndex;

		return sql.substring(startIndex + 1, endIndex).toUpperCase(Locale.ROOT);
	}

	@Nullable
	private static String keywordAfter(@NonNull String sql,
																		 int index) {
		char c = sql.charAt(index);
		if (!Character.isJavaIdentifierPart(c))
			return null;

		int endIndex = index + 1;
		while (endIndex < sql.length() && Character.isJavaIdentifierPart(sql.charAt(endIndex)))
			++endIndex;

		return sql.substring(index, endIndex).toUpperCase(Locale.ROOT);
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
	@NonNull
	private <T> Optional<T> queryForObject(@NonNull String sql,
																				 @NonNull Class<T> resultSetRowType,
																				 Object @Nullable ... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return queryForObject(Statement.of(generateId(), sql), resultSetRowType, parameters);
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
	private <T> Optional<T> queryForObject(@NonNull Statement statement,
																				 @NonNull Class<T> resultSetRowType,
																				 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		return queryForObject(statement, resultSetRowType, null, parameters);
	}

	private <T> Optional<T> queryForObject(@NonNull Statement statement,
																				 @NonNull Class<T> resultSetRowType,
																				 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																				 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		ResultHolder<Optional<T>> resultHolder = new ResultHolder<>();
		StatementContext<T> statementContext = StatementContext.<T>with(statement, this)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);

		performDatabaseOperation(statementContext, parametersAsList, preparedStatementCustomizer, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();

			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
				startTime = nanoTime();

				Optional<T> result = Optional.empty();
				long rowsReturned = 0L;

				if (resultSet.next()) {
					rowsReturned = 1L;
					try {
						T value = getResultSetMapper().map(statementContext, resultSet, statementContext.getResultSetRowType().get(), getInstanceProvider()).orElse(null);
						result = Optional.ofNullable(value);
					} catch (SQLException e) {
						throw databaseExceptionWithStatementContext(statementContext,
								format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), statementContext.getResultSetRowType().get()), e);
					}

					if (resultSet.next())
						throw new DatabaseException("Expected 1 row in resultset but got more than 1 instead");
				}

				resultHolder.value = result;
				Duration resultSetMappingDuration = Duration.ofNanos(nanoTime() - startTime);
				StatementResult statementResult = getMetricsCollectorDispatcher().isEnabled()
						? StatementResult.ofRowsReturned(rowsReturned)
						: StatementResult.empty();
				return new DatabaseOperationResult(executionDuration, resultSetMappingDuration, statementResult);
			}
		});

		return resultHolder.value;
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
	@NonNull
	private <T> List<@Nullable T> queryForList(@NonNull String sql,
																						 @NonNull Class<T> resultSetRowType,
																						 Object @Nullable ... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return queryForList(Statement.of(generateId(), sql), resultSetRowType, parameters);
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
	@NonNull
	private <T> List<@Nullable T> queryForList(@NonNull Statement statement,
																						 @NonNull Class<T> resultSetRowType,
																						 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		return queryForList(statement, resultSetRowType, null, parameters);
	}

	private <T> List<@Nullable T> queryForList(@NonNull Statement statement,
																						 @NonNull Class<T> resultSetRowType,
																						 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																						 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		List<T> list = new ArrayList<>();
		StatementContext<T> statementContext = StatementContext.<T>with(statement, this)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);

		performDatabaseOperation(statementContext, parametersAsList, preparedStatementCustomizer, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();

			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
				startTime = nanoTime();

				while (resultSet.next()) {
					try {
						T listElement = getResultSetMapper().map(statementContext, resultSet, statementContext.getResultSetRowType().get(), getInstanceProvider()).orElse(null);
						list.add(listElement);
					} catch (SQLException e) {
						throw databaseExceptionWithStatementContext(statementContext,
								format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), statementContext.getResultSetRowType().get()), e);
					}
				}

				Duration resultSetMappingDuration = Duration.ofNanos(nanoTime() - startTime);
				StatementResult statementResult = getMetricsCollectorDispatcher().isEnabled()
						? StatementResult.ofRowsReturned((long) list.size())
						: StatementResult.empty();
				return new DatabaseOperationResult(executionDuration, resultSetMappingDuration, statementResult);
			}
		});

		return list;
	}

	@Nullable
	private <T, R> R queryForStream(@NonNull Statement statement,
																	@NonNull Class<T> resultSetRowType,
																	@Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																	@NonNull Function<Stream<@Nullable T>, R> streamFunction,
																	Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);
		requireNonNull(streamFunction);

		StatementContext<T> statementContext = StatementContext.<T>with(statement, this)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);
		StreamingResultSet<T> iterator = new StreamingResultSet<>(this, statementContext, parametersAsList, preparedStatementCustomizer);

		try {
			try (Stream<@Nullable T> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
					.onClose(iterator::close)) {
				try {
					return streamFunction.apply(stream);
				} catch (Throwable throwable) {
					iterator.callbackFailed(throwable);
					if (throwable instanceof RuntimeException runtimeException)
						throw runtimeException;
					if (throwable instanceof Error error)
						throw error;
					throw new RuntimeException(throwable);
				}
			}
		} finally {
			iterator.emitTerminalMetrics();
		}
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE};
	 * or a SQL statement that returns nothing, such as a DDL statement.
	 *
	 * @param sql        the SQL to execute
	 * @param parameters {@link PreparedStatement} parameters, if any
	 * @return the number of rows affected by the SQL statement
	 */
	@NonNull
	private Long execute(@NonNull String sql,
											 Object @Nullable ... parameters) {
		requireNonNull(sql);
		return execute(Statement.of(generateId(), sql), parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE};
	 * or a SQL statement that returns nothing, such as a DDL statement.
	 *
	 * @param statement  the SQL statement to execute
	 * @param parameters {@link PreparedStatement} parameters, if any
	 * @return the number of rows affected by the SQL statement
	 */
	@NonNull
	private Long execute(@NonNull Statement statement,
											 Object @Nullable ... parameters) {
		requireNonNull(statement);

		return execute(statement, null, parameters);
	}

	private Long execute(@NonNull Statement statement,
											 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
											 Object @Nullable ... parameters) {
		requireNonNull(statement);

		ResultHolder<Long> resultHolder = new ResultHolder<>();
		StatementContext<Void> statementContext = StatementContext.with(statement, this)
				.parameters(parameters)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);

		performDatabaseOperation(statementContext, parametersAsList, preparedStatementCustomizer, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();
			resultHolder.value = executeUpdate(preparedStatement);

			Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
			StatementResult statementResult = getMetricsCollectorDispatcher().isEnabled()
					? StatementResult.ofRowsAffected(resultHolder.value)
					: StatementResult.empty();
			return new DatabaseOperationResult(executionDuration, null, statementResult);
		});

		return resultHolder.value;
	}

	@NonNull
	private Long executeUpdate(@NonNull PreparedStatement preparedStatement) throws SQLException {
		requireNonNull(preparedStatement);

		DatabaseOperationSupportStatus executeLargeUpdateSupported = getExecuteLargeUpdateSupported();

		// Use the appropriate "large" value if we know it.
		// If we don't know it, detect it and store it.
		if (executeLargeUpdateSupported == DatabaseOperationSupportStatus.YES)
			return preparedStatement.executeLargeUpdate();

		if (executeLargeUpdateSupported == DatabaseOperationSupportStatus.NO)
			return (long) preparedStatement.executeUpdate();

		// If the driver doesn't support executeLargeUpdate, then UnsupportedOperationException is thrown.
		try {
			Long result = preparedStatement.executeLargeUpdate();
			setExecuteLargeUpdateSupported(DatabaseOperationSupportStatus.YES);
			return result;
		} catch (SQLFeatureNotSupportedException | UnsupportedOperationException | AbstractMethodError e) {
			setExecuteLargeUpdateSupported(DatabaseOperationSupportStatus.NO);
			return (long) preparedStatement.executeUpdate();
		} catch (SQLException e) {
			if (isUnsupportedSqlFeature(e)) {
				setExecuteLargeUpdateSupported(DatabaseOperationSupportStatus.NO);
				return (long) preparedStatement.executeUpdate();
			}

			throw e;
		}
	}

	@NonNull
	private <T> Optional<T> executeReturningGeneratedKey(@NonNull Statement statement,
																											 @NonNull Class<T> resultSetRowType,
																											 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																											 @Nullable String @Nullable [] keyColumnNames,
																											 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		ResultHolder<Optional<T>> resultHolder = new ResultHolder<>();
		StatementContext<T> statementContext = StatementContext.<T>with(statement, this)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);
		String[] requestedKeyColumnNames = copyGeneratedKeyColumnNames(keyColumnNames);

		performDatabaseOperation(statementContext, parametersAsList, preparedStatementCustomizer, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();
			Long rowsAffected = executeUpdate(preparedStatement);

			try (ResultSet resultSet = preparedStatement.getGeneratedKeys()) {
				Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
				startTime = nanoTime();

				Optional<T> result = Optional.empty();
				long rowsReturned = 0L;

				if (resultSet.next()) {
					rowsReturned = 1L;
					try {
						T value = getResultSetMapper().map(statementContext, resultSet, statementContext.getResultSetRowType().get(), getInstanceProvider()).orElse(null);
						result = Optional.ofNullable(value);
					} catch (SQLException e) {
						throw databaseExceptionWithStatementContext(statementContext,
								format("Unable to map JDBC generated-key row to %s", statementContext.getResultSetRowType().get()), e);
					}

					if (resultSet.next())
						throw new DatabaseException("Expected 1 generated-key row but got more than 1 instead");
				}

				resultHolder.value = result;
				Duration resultSetMappingDuration = Duration.ofNanos(nanoTime() - startTime);
				StatementResult statementResult = getMetricsCollectorDispatcher().isEnabled()
						? new StatementResult(rowsReturned, rowsAffected)
						: StatementResult.empty();
				return new DatabaseOperationResult(executionDuration, resultSetMappingDuration, statementResult);
			}
		}, generatedKeysPreparedStatementFactory(requestedKeyColumnNames));

		return resultHolder.value;
	}

	@NonNull
	private <T> List<@Nullable T> executeReturningGeneratedKeys(@NonNull Statement statement,
																														 @NonNull Class<T> resultSetRowType,
																														 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																														 @Nullable String @Nullable [] keyColumnNames,
																														 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		List<T> list = new ArrayList<>();
		StatementContext<T> statementContext = StatementContext.<T>with(statement, this)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);
		String[] requestedKeyColumnNames = copyGeneratedKeyColumnNames(keyColumnNames);

		performDatabaseOperation(statementContext, parametersAsList, preparedStatementCustomizer, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();
			Long rowsAffected = executeUpdate(preparedStatement);

			try (ResultSet resultSet = preparedStatement.getGeneratedKeys()) {
				Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
				startTime = nanoTime();

				while (resultSet.next()) {
					try {
						T listElement = getResultSetMapper().map(statementContext, resultSet, statementContext.getResultSetRowType().get(), getInstanceProvider()).orElse(null);
						list.add(listElement);
					} catch (SQLException e) {
						throw databaseExceptionWithStatementContext(statementContext,
								format("Unable to map JDBC generated-key row to %s", statementContext.getResultSetRowType().get()), e);
					}
				}

				Duration resultSetMappingDuration = Duration.ofNanos(nanoTime() - startTime);
				StatementResult statementResult = getMetricsCollectorDispatcher().isEnabled()
						? new StatementResult((long) list.size(), rowsAffected)
						: StatementResult.empty();
				return new DatabaseOperationResult(executionDuration, resultSetMappingDuration, statementResult);
			}
		}, generatedKeysPreparedStatementFactory(requestedKeyColumnNames));

		return list;
	}

	@NonNull
	private String[] copyGeneratedKeyColumnNames(@Nullable String @Nullable [] keyColumnNames) {
		if (keyColumnNames == null || keyColumnNames.length == 0)
			return new String[0];

		String[] copy = Arrays.copyOf(keyColumnNames, keyColumnNames.length);

		for (String keyColumnName : copy)
			requireNonNull(keyColumnName);

		return copy;
	}

	@NonNull
	private PreparedStatementFactory generatedKeysPreparedStatementFactory(@NonNull String @NonNull [] keyColumnNames) {
		requireNonNull(keyColumnNames);

		return (connection, statementContext) -> keyColumnNames.length == 0
				? connection.prepareStatement(statementContext.getStatement().getSql(), java.sql.Statement.RETURN_GENERATED_KEYS)
				: connection.prepareStatement(statementContext.getStatement().getSql(), keyColumnNames);
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
	@NonNull
	private <T> Optional<T> executeForObject(@NonNull String sql,
																					 @NonNull Class<T> resultSetRowType,
																					 Object @Nullable ... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return executeForObject(Statement.of(generateId(), sql), resultSetRowType, parameters);
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
	private <T> Optional<T> executeForObject(@NonNull Statement statement,
																					 @NonNull Class<T> resultSetRowType,
																					 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		return executeForObject(statement, resultSetRowType, null, parameters);
	}

	private <T> Optional<T> executeForObject(@NonNull Statement statement,
																					 @NonNull Class<T> resultSetRowType,
																					 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																					 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		// Ultimately we just delegate to queryForObject.
		// Having `executeForList` is to allow for users to explicitly express intent
		// and make static analysis of code easier (e.g. maybe you'd like to hook all of your "execute" statements for
		// logging, or delegation to a writable master as opposed to a read replica)
		return queryForObject(statement, resultSetRowType, preparedStatementCustomizer, parameters);
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
	@NonNull
	private <T> List<@Nullable T> executeForList(@NonNull String sql,
																							 @NonNull Class<T> resultSetRowType,
																							 Object @Nullable ... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return executeForList(Statement.of(generateId(), sql), resultSetRowType, parameters);
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
	@NonNull
	private <T> List<@Nullable T> executeForList(@NonNull Statement statement,
																							 @NonNull Class<T> resultSetRowType,
																							 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		return executeForList(statement, resultSetRowType, null, parameters);
	}

	private <T> List<@Nullable T> executeForList(@NonNull Statement statement,
																							 @NonNull Class<T> resultSetRowType,
																							 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																							 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		// Ultimately we just delegate to queryForList.
		// Having `executeForList` is to allow for users to explicitly express intent
		// and make static analysis of code easier (e.g. maybe you'd like to hook all of your "execute" statements for
		// logging, or delegation to a writable master as opposed to a read replica)
		return queryForList(statement, resultSetRowType, preparedStatementCustomizer, parameters);
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
	@NonNull
	private List<Long> executeBatch(@NonNull String sql,
																	@NonNull List<List<Object>> parameterGroups) {
		requireNonNull(sql);
		requireNonNull(parameterGroups);

		return executeBatch(Statement.of(generateId(), sql), parameterGroups);
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
	@NonNull
	private List<Long> executeBatch(@NonNull Statement statement,
																	@NonNull List<List<Object>> parameterGroups) {
		requireNonNull(statement);
		requireNonNull(parameterGroups);

		return executeBatch(statement, parameterGroups, null);
	}

	private List<Long> executeBatch(@NonNull Statement statement,
																	@NonNull List<List<Object>> parameterGroups,
																	@Nullable PreparedStatementCustomizer preparedStatementCustomizer) {
		requireNonNull(statement);
		requireNonNull(parameterGroups);
		if (parameterGroups.isEmpty())
			return List.of();

		Integer expectedParameterCount = null;

		for (int i = 0; i < parameterGroups.size(); i++) {
			List<Object> parameterGroup = parameterGroups.get(i);

			if (parameterGroup == null)
				throw new IllegalArgumentException(format("Parameter group at index %s is null", i));

			int parameterCount = parameterGroup.size();
			if (expectedParameterCount == null) {
				expectedParameterCount = parameterCount;
			} else if (parameterCount != expectedParameterCount) {
				throw new IllegalArgumentException(format(
						"Inconsistent parameter group size at index %s: expected %s but found %s",
						i, expectedParameterCount, parameterCount));
			}
		}

		ResultHolder<List<Long>> resultHolder = new ResultHolder<>();
		StatementContext<List<Long>> statementContext = StatementContext.with(statement, this)
				.parameters((List) parameterGroups)
				.resultSetRowType(List.class)
				.build();

		performDatabaseOperation(statementContext, (preparedStatement) -> {
			applyPreparedStatementCustomizer(statementContext, preparedStatement, preparedStatementCustomizer);

			for (List<Object> parameterGroup : parameterGroups) {
				if (parameterGroup.size() > 0)
					performPreparedStatementBinding(statementContext, preparedStatement, parameterGroup);

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
				} catch (SQLFeatureNotSupportedException | UnsupportedOperationException | AbstractMethodError e) {
					setExecuteLargeBatchSupported(DatabaseOperationSupportStatus.NO);
					int[] resultArray = preparedStatement.executeBatch();
					result = Arrays.stream(resultArray).asLongStream().boxed().collect(Collectors.toList());
				} catch (SQLException e) {
					if (isUnsupportedSqlFeature(e)) {
						setExecuteLargeBatchSupported(DatabaseOperationSupportStatus.NO);
						int[] resultArray = preparedStatement.executeBatch();
						result = Arrays.stream(resultArray).asLongStream().boxed().collect(Collectors.toList());
					} else {
						throw e;
					}
				}
			}

			resultHolder.value = result;
			Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
			StatementResult statementResult = StatementResult.empty();
			if (getMetricsCollectorDispatcher().isEnabled()) {
				Long rowsAffected = sumBatchUpdateCounts(result);
				statementResult = rowsAffected == null ? StatementResult.empty() : StatementResult.ofRowsAffected(rowsAffected);
			}
			return new DatabaseOperationResult(executionDuration, null, statementResult);
		}, parameterGroups.size());

		return resultHolder.value;
	}

	/**
	 * Exposes a temporary handle to JDBC {@link DatabaseMetaData}, which provides comprehensive vendor-specific information about this database as a whole.
	 * <p>
	 * This method acquires {@link DatabaseMetaData} on its own newly-borrowed connection, which it manages internally.
	 * <p>
	 * It does <strong>not</strong> participate in the active transaction, if one exists.
	 * <p>
	 * The connection is closed as soon as {@link DatabaseMetaDataReader#read(DatabaseMetaData)} completes.
	 * <p>
	 * See <a href="https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/DatabaseMetaData.html">{@code DatabaseMetaData} Javadoc</a> for details.
	 */
	public void readDatabaseMetaData(@NonNull DatabaseMetaDataReader databaseMetaDataReader) {
		requireNonNull(databaseMetaDataReader);

		performRawConnectionOperation((connection -> {
			databaseMetaDataReader.read(connection.getMetaData());
			return Optional.empty();
		}), false);
	}

	/**
	 * Performs raw JDBC work with a Pyranid-managed {@link Connection}.
	 * <p>
	 * If called inside a Pyranid transaction, this operation uses the transaction's connection and participates in that
	 * transaction. Otherwise, Pyranid borrows a connection for the duration of the callback and closes it afterwards.
	 * <p>
	 * The {@link Connection} passed to {@code rawConnectionOperation} is a guarded handle. Normal JDBC operations are
	 * delegated to the underlying driver connection, but lifecycle and transaction-management methods such as
	 * {@link Connection#close()}, {@link Connection#commit()}, {@link Connection#rollback()}, and
	 * {@link Connection#setAutoCommit(boolean)} throw {@link IllegalStateException}. Use Pyranid transaction APIs instead.
	 * <p>
	 * The connection handle is valid only for the duration of the callback. Do not close it, retain it, or use it after this
	 * method returns.
	 *
	 * @param rawConnectionOperation the raw JDBC operation to perform
	 * @param <T>                    the type to be returned
	 * @return the operation result
	 * @throws DatabaseException if connection acquisition, callback execution, or cleanup fails
	 * @since 4.2.0
	 */
	@NonNull
	public <T> Optional<T> useRawConnection(@NonNull RawConnectionOperation<T> rawConnectionOperation) {
		requireNonNull(rawConnectionOperation);

		return performRawConnectionOperation(connection -> {
			PyranidRawConnection rawConnection = new PyranidRawConnection(connection);

			try {
				Optional<T> result = rawConnectionOperation.perform(rawConnection);
				return result == null ? Optional.empty() : result;
			} finally {
				rawConnection.release();
			}
		}, true);
	}

	protected <T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
																							@NonNull List<Object> parameters,
																							@NonNull DatabaseOperation databaseOperation) {
		requireNonNull(statementContext);
		requireNonNull(parameters);
		requireNonNull(databaseOperation);

		performDatabaseOperation(statementContext, parameters, null, databaseOperation);
	}

	protected <T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
																							@NonNull List<Object> parameters,
																							@Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																							@NonNull DatabaseOperation databaseOperation) {
		performDatabaseOperation(statementContext, parameters, preparedStatementCustomizer, databaseOperation,
				(connection, context) -> connection.prepareStatement(context.getStatement().getSql()));
	}

	protected <T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
																							@NonNull List<Object> parameters,
																							@Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																							@NonNull DatabaseOperation databaseOperation,
																							@NonNull PreparedStatementFactory preparedStatementFactory) {
		requireNonNull(statementContext);
		requireNonNull(parameters);
		requireNonNull(databaseOperation);
		requireNonNull(preparedStatementFactory);

		performDatabaseOperation(statementContext, (preparedStatement) -> {
			applyPreparedStatementCustomizer(statementContext, preparedStatement, preparedStatementCustomizer);
			if (parameters.size() > 0)
				performPreparedStatementBinding(statementContext, preparedStatement, parameters);
		}, databaseOperation, null, preparedStatementFactory);
	}

	protected <T> void performPreparedStatementBinding(@NonNull StatementContext<T> statementContext,
																										 @NonNull PreparedStatement preparedStatement,
																										 @NonNull List<Object> parameters) {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);
		requireNonNull(parameters);

		try {
			DefaultPreparedStatementBinder.ParameterSqlTypeResolver parameterSqlTypeResolver =
					new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(preparedStatement);
			PreparedStatementBinder preparedStatementBinder = getPreparedStatementBinder();

			for (int i = 0; i < parameters.size(); ++i) {
				Object parameter = parameters.get(i);
				Integer parameterIndex = i + 1;

				if (parameter != null) {
					if (preparedStatementBinder instanceof DefaultPreparedStatementBinder defaultPreparedStatementBinder) {
						defaultPreparedStatementBinder.bindParameter(statementContext, preparedStatement, parameterIndex, parameter,
								parameterSqlTypeResolver);
					} else {
						preparedStatementBinder.bindParameter(statementContext, preparedStatement, parameterIndex, parameter);
					}
				} else {
					Integer sqlType = parameterSqlTypeResolver.determineParameterSqlType(parameterIndex)
							.map(DefaultPreparedStatementBinder.ParameterSqlType::getSqlType)
							.orElse(Types.NULL);
					try {
						preparedStatement.setNull(parameterIndex, sqlType);
					} catch (SQLException | AbstractMethodError e) {
						if (sqlType == Types.NULL)
							throw e;

						preparedStatement.setNull(parameterIndex, Types.NULL);
					}
				}
			}
		} catch (Exception e) {
			throw databaseExceptionWithStatementContext(statementContext, e);
		}
	}

	protected void applyPreparedStatementCustomizer(@NonNull StatementContext<?> statementContext,
																									@NonNull PreparedStatement preparedStatement,
																									@Nullable PreparedStatementCustomizer preparedStatementCustomizer) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);

		if (preparedStatementCustomizer == null)
			return;

		preparedStatementCustomizer.customize(statementContext, preparedStatement);
	}

	private boolean hasDefaultPreparedStatementSettings() {
		return this.queryTimeout != null || this.fetchSize != null || this.maxRows != null;
	}

	private void applyDefaultPreparedStatementSettings(@NonNull PreparedStatement preparedStatement) throws SQLException {
		requireNonNull(preparedStatement);
		applyPreparedStatementSettings(preparedStatement, this.queryTimeout, this.fetchSize, this.maxRows);
	}

	private static void applyPreparedStatementSettings(@NonNull PreparedStatement preparedStatement,
																										 @Nullable Duration queryTimeout,
																										 @Nullable Integer fetchSize,
																										 @Nullable Integer maxRows) throws SQLException {
		requireNonNull(preparedStatement);

		if (queryTimeout != null)
			preparedStatement.setQueryTimeout(queryTimeoutSeconds(queryTimeout));

		if (fetchSize != null)
			preparedStatement.setFetchSize(fetchSize);

		if (maxRows != null)
			preparedStatement.setMaxRows(maxRows);
	}

	@FunctionalInterface
	protected interface InternalRawConnectionOperation<R> {
		@NonNull
		Optional<R> perform(@NonNull Connection connection) throws Exception;
	}

	/**
	 * Gets the database type for this database.
	 * <p>
	 * If {@link Builder#databaseType(DatabaseType)} was not configured and the database type has not already been detected,
	 * this method may acquire a connection and inspect {@link DatabaseMetaData}.  Configure an explicit database type to avoid
	 * runtime detection.
	 *
	 * @return the database type
	 * @throws DatabaseException if automatic database type detection fails
	 * @since 3.0.0
	 */
	@NonNull
	public DatabaseType getDatabaseType() {
		return getDatabaseType(this.databaseTypeDetectionConnectionHolder.get());
	}

	@NonNull
	DatabaseType peekDatabaseType() {
		DatabaseType cachedDatabaseType = this.databaseType.get();
		return cachedDatabaseType == null ? DatabaseType.GENERIC : cachedDatabaseType;
	}

	private void warmDatabaseTypeCacheForMetricsIfNeeded(@NonNull StatementContext<?> statementContext) {
		requireNonNull(statementContext);

		if (!getMetricsCollectorDispatcher().isEnabled())
			return;

		// Trigger lazy database-type detection while the statement's JDBC connection is active so later
		// transaction-scope metrics can report an accurate db.system.name without opening a second metadata connection.
		try {
			statementContext.getDatabaseType();
		} catch (Throwable t) {
			this.logger.log(Level.FINE, "Unable to warm database type cache for metrics", t);
		}
	}

	private void dispatchWithDatabaseTypeDetectionConnection(@NonNull Connection connection,
																													 @NonNull Runnable operation) {
		requireNonNull(connection);
		requireNonNull(operation);

		Connection previousDatabaseTypeDetectionConnection = this.databaseTypeDetectionConnectionHolder.get();
		this.databaseTypeDetectionConnectionHolder.set(connection);

		try {
			operation.run();
		} finally {
			if (previousDatabaseTypeDetectionConnection == null)
				this.databaseTypeDetectionConnectionHolder.remove();
			else
				this.databaseTypeDetectionConnectionHolder.set(previousDatabaseTypeDetectionConnection);
		}
	}

	@NonNull
	private DatabaseType getDatabaseType(@Nullable Connection connection) {
		DatabaseType cachedDatabaseType = this.databaseType.get();

		if (cachedDatabaseType != null)
			return cachedDatabaseType;

		DatabaseType detectedDatabaseType;

		try {
			detectedDatabaseType = connection == null
					? DatabaseType.fromDataSource(getDataSource())
					: DatabaseType.fromConnection(connection);
		} catch (DatabaseException e) {
			throw new DatabaseException(format(
					"Unable to determine database type automatically. Configure %s.%s(%s) explicitly to avoid runtime detection.",
					Builder.class.getSimpleName(), "databaseType", DatabaseType.class.getSimpleName()), e);
		}

		if (this.databaseType.compareAndSet(null, detectedDatabaseType))
			return detectedDatabaseType;

		return this.databaseType.get();
	}

	/**
	 * @since 3.0.0
	 */
	@NonNull
	public ZoneId getTimeZone() {
		return this.timeZone;
	}

	/**
	 * How should Pyranid bind {@link java.time.Instant} and {@link java.time.OffsetDateTime} parameters when JDBC
	 * parameter metadata cannot identify whether the target is {@code TIMESTAMP} or {@code TIMESTAMP WITH TIME ZONE}?
	 *
	 * @return behavior to use when timestamp target metadata is unavailable or non-identifying
	 * @since 4.2.0
	 */
	@NonNull
	public AmbiguousTimestampBindingStrategy getAmbiguousTimestampBindingStrategy() {
		return this.ambiguousTimestampBindingStrategy;
	}

	/**
	 * Useful for single-shot "utility" calls that operate outside of normal query operations, e.g. pulling DB metadata.
	 * <p>
	 * Example: {@link #readDatabaseMetaData(DatabaseMetaDataReader)}.
	 */
	@NonNull
	protected <R> Optional<R> performRawConnectionOperation(@NonNull InternalRawConnectionOperation<R> rawConnectionOperation,
																													@NonNull Boolean shouldParticipateInExistingTransactionIfPossible) {
		requireNonNull(rawConnectionOperation);
		requireNonNull(shouldParticipateInExistingTransactionIfPossible);

		if (shouldParticipateInExistingTransactionIfPossible) {
			Optional<Transaction> transaction = currentTransactionForDatabaseOperation();
			ReentrantLock connectionLock = transaction.isPresent() ? transaction.get().getConnectionLock() : null;
			// Try to participate in txn if it's available
			Connection connection = null;
			Throwable thrown = null;

			if (connectionLock != null)
				connectionLock.lock();

			try {
				connection = transaction.isPresent() ? transaction.get().getConnection() : acquireConnection();
				return rawConnectionOperation.perform(connection);
			} catch (DatabaseException e) {
				thrown = e;
				throw e;
			} catch (Exception e) {
				DatabaseException wrapped = new DatabaseException(e);
				thrown = wrapped;
				throw wrapped;
			} finally {
				Throwable cleanupFailure = null;

				try {
					// If this was a single-shot operation (not in a transaction), close the connection
					if (connection != null && !transaction.isPresent()) {
						try {
							closeConnection(connection);
						} catch (Throwable cleanupException) {
							cleanupFailure = cleanupException;
						}
					}
				} finally {
					if (connectionLock != null)
						connectionLock.unlock();

					if (cleanupFailure != null) {
						if (thrown != null) {
							thrown.addSuppressed(cleanupFailure);
						} else if (cleanupFailure instanceof RuntimeException) {
							throw (RuntimeException) cleanupFailure;
						} else if (cleanupFailure instanceof Error) {
							throw (Error) cleanupFailure;
						} else {
							throw new RuntimeException(cleanupFailure);
						}
					}
				}
			}
		} else {
			boolean acquiredConnection = false;
			Connection connection = null;
			Throwable thrown = null;

			// Always get a fresh connection no matter what and close it afterwards
			try {
				connection = getDataSource().getConnection();
				acquiredConnection = true;
				return rawConnectionOperation.perform(connection);
			} catch (DatabaseException e) {
				thrown = e;
				throw e;
			} catch (Exception e) {
				DatabaseException wrapped = acquiredConnection
						? new DatabaseException(e)
						: new DatabaseException("Unable to acquire database connection", e);
				thrown = wrapped;
				throw wrapped;
			} finally {
				if (connection != null) {
					try {
						closeConnection(connection);
					} catch (Throwable cleanupException) {
						if (thrown != null) {
							thrown.addSuppressed(cleanupException);
						} else if (cleanupException instanceof RuntimeException) {
							throw (RuntimeException) cleanupException;
						} else if (cleanupException instanceof Error) {
							throw (Error) cleanupException;
						} else {
							throw new RuntimeException(cleanupException);
						}
					}
				}
			}
		}
	}

	protected <T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
																							@NonNull PreparedStatementBindingOperation preparedStatementBindingOperation,
																							@NonNull DatabaseOperation databaseOperation) {
		performDatabaseOperation(statementContext, preparedStatementBindingOperation, databaseOperation, null);
	}

	protected <T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
																							@NonNull PreparedStatementBindingOperation preparedStatementBindingOperation,
																							@NonNull DatabaseOperation databaseOperation,
																							@Nullable Integer batchSize) {
		performDatabaseOperation(statementContext, preparedStatementBindingOperation, databaseOperation, batchSize,
				(connection, context) -> connection.prepareStatement(context.getStatement().getSql()));
	}

	protected <T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
																							@NonNull PreparedStatementBindingOperation preparedStatementBindingOperation,
																							@NonNull DatabaseOperation databaseOperation,
																							@Nullable Integer batchSize,
																							@NonNull PreparedStatementFactory preparedStatementFactory) {
		requireNonNull(statementContext);
		requireNonNull(preparedStatementBindingOperation);
		requireNonNull(databaseOperation);
		requireNonNull(preparedStatementFactory);

		long startTime = nanoTime();
		Duration connectionAcquisitionDuration = null;
		Duration preparationDuration = null;
		Duration executionDuration = null;
		Duration resultSetMappingDuration = null;
		StatementResult statementResult = StatementResult.empty();
		Exception exception = null;
		Throwable thrown = null;
		Connection connection = null;
		long connectionHeldStartTime = 0L;
		Optional<Transaction> transaction = currentTransactionForDatabaseOperation();
		ReentrantLock connectionLock = transaction.isPresent() ? transaction.get().getConnectionLock() : null;

		if (connectionLock != null)
			connectionLock.lock();

		try {
			boolean alreadyHasConnection = transaction.isPresent() && transaction.get().hasConnection();
			if (transaction.isPresent()) {
				connection = transaction.get().getConnection();
			} else {
				getMetricsCollectorDispatcher().willAcquireStatementConnection(statementContext);
				try {
					connection = getDataSource().getConnection();
				} catch (SQLException e) {
					DatabaseException wrapped = new DatabaseException("Unable to acquire database connection", e);
					connectionAcquisitionDuration = Duration.ofNanos(nanoTime() - startTime);
					getMetricsCollectorDispatcher().didFailToAcquireStatementConnection(statementContext, peekDatabaseType(),
							connectionAcquisitionDuration, wrapped);
					throw wrapped;
				} catch (RuntimeException e) {
					connectionAcquisitionDuration = Duration.ofNanos(nanoTime() - startTime);
					getMetricsCollectorDispatcher().didFailToAcquireStatementConnection(statementContext, peekDatabaseType(),
							connectionAcquisitionDuration, e);
					throw e;
				}
			}

			connectionAcquisitionDuration = alreadyHasConnection ? null : Duration.ofNanos(nanoTime() - startTime);
			if (!transaction.isPresent()) {
				connectionHeldStartTime = nanoTime();
				Duration acquiredDuration = connectionAcquisitionDuration;
				MetricsCollectorDispatcher metricsCollectorDispatcher = getMetricsCollectorDispatcher();
				if (metricsCollectorDispatcher.isEnabled())
					dispatchWithDatabaseTypeDetectionConnection(connection, () ->
							metricsCollectorDispatcher.didAcquireStatementConnection(statementContext, acquiredDuration));
			}
			startTime = nanoTime();

			try (PreparedStatement preparedStatement = preparedStatementFactory.prepare(connection, statementContext)) {
				Connection previousDatabaseTypeDetectionConnection = this.databaseTypeDetectionConnectionHolder.get();
				this.databaseTypeDetectionConnectionHolder.set(connection);

				try {
					preparedStatementBindingOperation.perform(preparedStatement);
					preparationDuration = Duration.ofNanos(nanoTime() - startTime);

					getMetricsCollectorDispatcher().willExecuteStatement(statementContext);
					DatabaseOperationResult databaseOperationResult = databaseOperation.perform(preparedStatement);
					executionDuration = databaseOperationResult.getExecutionDuration().orElse(null);
					resultSetMappingDuration = databaseOperationResult.getResultSetMappingDuration().orElse(null);
					statementResult = databaseOperationResult.getStatementResult();
					warmDatabaseTypeCacheForMetricsIfNeeded(statementContext);
				} finally {
					if (previousDatabaseTypeDetectionConnection == null)
						this.databaseTypeDetectionConnectionHolder.remove();
					else
						this.databaseTypeDetectionConnectionHolder.set(previousDatabaseTypeDetectionConnection);
				}
			}
		} catch (DatabaseException e) {
			exception = e;
			thrown = e;
			throw e;
		} catch (Error e) {
			exception = databaseExceptionWithStatementContext(statementContext, e);
			thrown = e;
			throw e;
		} catch (Exception e) {
			DatabaseException wrapped = databaseExceptionWithStatementContext(statementContext, e);
			exception = e;
			thrown = wrapped;
			throw wrapped;
		} finally {
			Throwable cleanupFailure = null;

			try {
				cleanupFailure = closeStatementContextResources(statementContext, cleanupFailure);

				// If this was a single-shot operation (not in a transaction), close the connection
				if (connection != null && !transaction.isPresent()) {
					Duration heldDuration = Duration.ofNanos(nanoTime() - connectionHeldStartTime);
					try {
						closeConnection(connection);
						getMetricsCollectorDispatcher().didReleaseStatementConnection(statementContext, heldDuration);
					} catch (Throwable cleanupException) {
						getMetricsCollectorDispatcher().didFailToReleaseStatementConnection(statementContext, heldDuration, cleanupException);
						if (cleanupFailure == null)
							cleanupFailure = cleanupException;
						else
							cleanupFailure.addSuppressed(cleanupException);
					}
				}
			} finally {
				if (connectionLock != null)
					connectionLock.unlock();

				StatementLog statementLog =
						StatementLog.withStatementContext(statementContext)
								.connectionAcquisitionDuration(connectionAcquisitionDuration)
								.preparationDuration(preparationDuration)
								.executionDuration(executionDuration)
								.resultSetMappingDuration(resultSetMappingDuration)
								.batchSize(batchSize)
								.exception(exception)
								.build();

				if (thrown == null && exception == null) {
					getMetricsCollectorDispatcher().didExecuteStatement(statementContext, statementLog, statementResult);
				} else {
					Throwable statementThrowable = thrown == null ? exception : thrown;
					getMetricsCollectorDispatcher().didFailToExecuteStatement(statementContext, statementLog, peekDatabaseType(),
							requireNonNull(statementThrowable));
				}

				try {
					getStatementLogger().log(statementLog);
				} catch (Throwable cleanupException) {
					if (cleanupFailure == null)
						cleanupFailure = cleanupException;
					else
						cleanupFailure.addSuppressed(cleanupException);
				}
			}

			if (cleanupFailure != null) {
				if (thrown != null) {
					thrown.addSuppressed(cleanupFailure);
				} else if (cleanupFailure instanceof RuntimeException) {
					throw (RuntimeException) cleanupFailure;
				} else if (cleanupFailure instanceof Error) {
					throw (Error) cleanupFailure;
				} else {
					throw new RuntimeException(cleanupFailure);
				}
			}
		}
	}

	@NonNull
	protected Connection acquireConnection() {
		Optional<Transaction> transaction = currentTransactionForDatabaseOperation();

		if (transaction.isPresent())
			return transaction.get().getConnection();

		try {
			return getDataSource().getConnection();
		} catch (SQLException e) {
			throw new DatabaseException("Unable to acquire database connection", e);
		}
	}

	@NonNull
	protected DataSource getDataSource() {
		return this.dataSource;
	}

	@NonNull
	protected InstanceProvider getInstanceProvider() {
		return this.instanceProvider;
	}

	@NonNull
	protected PreparedStatementBinder getPreparedStatementBinder() {
		return this.preparedStatementBinder;
	}

	@NonNull
	protected ResultSetMapper getResultSetMapper() {
		return this.resultSetMapper;
	}

	@NonNull
	protected StatementLogger getStatementLogger() {
		return this.statementLogger;
	}

	@NonNull
	public MetricsCollector getMetricsCollector() {
		return getMetricsCollectorDispatcher().getMetricsCollector();
	}

	@NonNull
	MetricsCollectorDispatcher getMetricsCollectorDispatcher() {
		return this.metricsCollectorDispatcher;
	}

	@NonNull
	protected DatabaseOperationSupportStatus getExecuteLargeBatchSupported() {
		return this.executeLargeBatchSupported;
	}

	protected void setExecuteLargeBatchSupported(@NonNull DatabaseOperationSupportStatus executeLargeBatchSupported) {
		requireNonNull(executeLargeBatchSupported);
		this.executeLargeBatchSupported = executeLargeBatchSupported;
	}

	@NonNull
	protected DatabaseOperationSupportStatus getExecuteLargeUpdateSupported() {
		return this.executeLargeUpdateSupported;
	}

	protected void setExecuteLargeUpdateSupported(@NonNull DatabaseOperationSupportStatus executeLargeUpdateSupported) {
		requireNonNull(executeLargeUpdateSupported);
		this.executeLargeUpdateSupported = executeLargeUpdateSupported;
	}

	@NonNull
	protected Object generateId() {
		// "Unique" keys
		return format("com.pyranid.%s", this.defaultIdGenerator.incrementAndGet());
	}

	@FunctionalInterface
	protected interface DatabaseOperation {
		@NonNull
		DatabaseOperationResult perform(@NonNull PreparedStatement preparedStatement) throws Exception;
	}

	@FunctionalInterface
	protected interface PreparedStatementFactory {
		@NonNull
		PreparedStatement prepare(@NonNull Connection connection,
															@NonNull StatementContext<?> statementContext) throws SQLException;
	}

	@FunctionalInterface
	protected interface PreparedStatementBindingOperation {
		void perform(@NonNull PreparedStatement preparedStatement) throws Exception;
	}

	@NotThreadSafe
	private static final class StreamingResultSet<T> implements java.util.Iterator<T>, AutoCloseable {
		private final Database database;
		private final StatementContext<T> statementContext;
		private final List<Object> parameters;
		@Nullable
		private final PreparedStatementCustomizer preparedStatementCustomizer;
		@NonNull
		private final Optional<Transaction> transaction;
		@Nullable
		private final ReentrantLock connectionLock;
		@Nullable
		private Connection connection;
		@Nullable
		private PreparedStatement preparedStatement;
		@Nullable
		private ResultSet resultSet;
		private boolean closed;
		private boolean hasNextEvaluated;
		private boolean hasNext;
		@Nullable
		private Duration connectionAcquisitionDuration;
		@Nullable
		private Duration preparationDuration;
		@Nullable
		private Duration executionDuration;
		private long resultSetMappingNanos;
		@Nullable
		private Exception exception;
		@Nullable
		private Throwable thrown;
		private long rowsConsumed;
		private long openStartTime;
		private boolean exhausted;
		private boolean openFailed;
		private boolean terminalMetricsEmitted;
		@Nullable
		private Throwable callbackThrowable;
		@Nullable
		private Throwable iterationThrowable;
		@Nullable
		private Throwable cleanupFailure;
		private long connectionHeldStartTime;
		@Nullable
		private Boolean initialAutoCommit;
		private boolean postgresqlStreamTransactionStarted;

		private StreamingResultSet(@NonNull Database database,
															 @NonNull StatementContext<T> statementContext,
															 @NonNull List<Object> parameters,
															 @Nullable PreparedStatementCustomizer preparedStatementCustomizer) {
			this.database = requireNonNull(database);
			this.statementContext = requireNonNull(statementContext);
			this.parameters = requireNonNull(parameters);
			this.preparedStatementCustomizer = preparedStatementCustomizer;
			this.transaction = database.currentTransactionForDatabaseOperation();
			this.connectionLock = this.transaction.isPresent() ? this.transaction.get().getConnectionLock() : null;

			open();
		}

		private void open() {
			long startTime = nanoTime();
			this.openStartTime = startTime;
			this.database.getMetricsCollectorDispatcher().willOpenStream(this.statementContext);

			if (this.connectionLock != null)
				this.connectionLock.lock();

			try {
				boolean alreadyHasConnection = this.transaction.isPresent() && this.transaction.get().hasConnection();
				if (this.transaction.isPresent()) {
					this.connection = this.transaction.get().getConnection();
				} else {
					this.database.getMetricsCollectorDispatcher().willAcquireStatementConnection(this.statementContext);
					try {
						this.connection = this.database.getDataSource().getConnection();
					} catch (SQLException e) {
						DatabaseException wrapped = new DatabaseException("Unable to acquire database connection", e);
						this.connectionAcquisitionDuration = Duration.ofNanos(nanoTime() - startTime);
						this.database.getMetricsCollectorDispatcher().didFailToAcquireStatementConnection(this.statementContext,
								this.database.peekDatabaseType(), this.connectionAcquisitionDuration, wrapped);
						throw wrapped;
					} catch (RuntimeException e) {
						this.connectionAcquisitionDuration = Duration.ofNanos(nanoTime() - startTime);
						this.database.getMetricsCollectorDispatcher().didFailToAcquireStatementConnection(this.statementContext,
								this.database.peekDatabaseType(), this.connectionAcquisitionDuration, e);
						throw e;
					}
				}
				this.connectionAcquisitionDuration = alreadyHasConnection ? null : Duration.ofNanos(nanoTime() - startTime);
				if (this.transaction.isEmpty()) {
					this.connectionHeldStartTime = nanoTime();
					MetricsCollectorDispatcher metricsCollectorDispatcher = this.database.getMetricsCollectorDispatcher();
					if (metricsCollectorDispatcher.isEnabled())
						this.database.dispatchWithDatabaseTypeDetectionConnection(requireNonNull(this.connection), () ->
								metricsCollectorDispatcher.didAcquireStatementConnection(this.statementContext, this.connectionAcquisitionDuration));
				}
				startTime = nanoTime();

				DatabaseType databaseType = databaseTypeForStreamingConnection();
				configurePostgreSqlStreamingIfNeeded(databaseType);

				this.preparedStatement = this.connection.prepareStatement(this.statementContext.getStatement().getSql());
				if (databaseType == DatabaseType.POSTGRESQL)
					this.preparedStatement.setFetchSize(DEFAULT_POSTGRESQL_STREAM_FETCH_SIZE);
				Connection previousDatabaseTypeDetectionConnection = this.database.databaseTypeDetectionConnectionHolder.get();
				this.database.databaseTypeDetectionConnectionHolder.set(this.connection);

				try {
					this.database.applyPreparedStatementCustomizer(this.statementContext, this.preparedStatement, this.preparedStatementCustomizer);
					if (this.parameters.size() > 0)
						this.database.performPreparedStatementBinding(this.statementContext, this.preparedStatement, this.parameters);
					this.preparationDuration = Duration.ofNanos(nanoTime() - startTime);

					startTime = nanoTime();
					this.resultSet = this.preparedStatement.executeQuery();
					this.executionDuration = Duration.ofNanos(nanoTime() - startTime);
					this.database.warmDatabaseTypeCacheForMetricsIfNeeded(this.statementContext);
					this.database.getMetricsCollectorDispatcher().didOpenStream(this.statementContext, Duration.ofNanos(nanoTime() - this.openStartTime));
				} finally {
					if (previousDatabaseTypeDetectionConnection == null)
						this.database.databaseTypeDetectionConnectionHolder.remove();
					else
						this.database.databaseTypeDetectionConnectionHolder.set(previousDatabaseTypeDetectionConnection);
				}
			} catch (DatabaseException e) {
				this.exception = e;
				this.thrown = e;
				this.openFailed = true;
				this.database.getMetricsCollectorDispatcher().didFailToOpenStream(this.statementContext, this.database.peekDatabaseType(),
						Duration.ofNanos(nanoTime() - this.openStartTime), e);
				close();
				throw e;
			} catch (Exception e) {
				DatabaseException wrapped = databaseExceptionWithStatementContext(this.statementContext, e);
				this.exception = e;
				this.thrown = wrapped;
				this.openFailed = true;
				this.database.getMetricsCollectorDispatcher().didFailToOpenStream(this.statementContext, this.database.peekDatabaseType(),
						Duration.ofNanos(nanoTime() - this.openStartTime), wrapped);
				close();
				throw wrapped;
			} catch (Error e) {
				this.exception = databaseExceptionWithStatementContext(this.statementContext, e);
				this.thrown = e;
				this.openFailed = true;
				this.database.getMetricsCollectorDispatcher().didFailToOpenStream(this.statementContext, this.database.peekDatabaseType(),
						Duration.ofNanos(nanoTime() - this.openStartTime), e);
				close();
				throw e;
			}
		}

		@NonNull
		private DatabaseType databaseTypeForStreamingConnection() {
			try {
				return this.database.getDatabaseType(requireNonNull(this.connection));
			} catch (DatabaseException e) {
				return this.database.peekDatabaseType();
			}
		}

		private void configurePostgreSqlStreamingIfNeeded(@NonNull DatabaseType databaseType) throws SQLException {
			requireNonNull(databaseType);

			if (databaseType != DatabaseType.POSTGRESQL || this.transaction.isPresent())
				return;

			Connection connection = requireNonNull(this.connection);
			this.initialAutoCommit = connection.getAutoCommit();

			if (Boolean.TRUE.equals(this.initialAutoCommit))
				connection.setAutoCommit(false);

			this.postgresqlStreamTransactionStarted = true;
		}

		@Override
		public boolean hasNext() {
			if (this.closed)
				return false;

			if (!this.hasNextEvaluated) {
				try {
					this.hasNext = this.resultSet != null && this.resultSet.next();
					this.hasNextEvaluated = true;
					if (!this.hasNext) {
						this.exhausted = true;
						close();
					}
				} catch (SQLException e) {
					DatabaseException wrapped = databaseExceptionWithStatementContext(this.statementContext, e);
					this.exception = e;
					this.thrown = wrapped;
					this.iterationThrowable = wrapped;
					close();
					throw wrapped;
				} catch (RuntimeException e) {
					this.exception = e;
					this.thrown = e;
					this.iterationThrowable = e;
					close();
					throw e;
				} catch (Error e) {
					this.exception = databaseExceptionWithStatementContext(this.statementContext, e);
					this.thrown = e;
					this.iterationThrowable = e;
					close();
					throw e;
				}
			}

			return this.hasNext;
		}

		@Override
		public T next() {
			if (!hasNext())
				throw new java.util.NoSuchElementException();

			this.hasNextEvaluated = false;
			long startTime = nanoTime();

			try {
				Connection previousDatabaseTypeDetectionConnection = this.database.databaseTypeDetectionConnectionHolder.get();
				this.database.databaseTypeDetectionConnectionHolder.set(requireNonNull(this.connection));
				T value;

				try {
					value = this.database.getResultSetMapper()
							.map(this.statementContext, requireNonNull(this.resultSet), this.statementContext.getResultSetRowType().get(), this.database.getInstanceProvider())
							.orElse(null);
				} finally {
					if (previousDatabaseTypeDetectionConnection == null)
						this.database.databaseTypeDetectionConnectionHolder.remove();
					else
						this.database.databaseTypeDetectionConnectionHolder.set(previousDatabaseTypeDetectionConnection);
				}

				this.resultSetMappingNanos += nanoTime() - startTime;
				this.rowsConsumed++;
				return value;
			} catch (SQLException e) {
				DatabaseException wrapped = databaseExceptionWithStatementContext(this.statementContext,
						format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), this.statementContext.getResultSetRowType().get()), e);
				this.exception = e;
				this.thrown = wrapped;
				this.iterationThrowable = wrapped;
				close();
				throw wrapped;
			} catch (DatabaseException e) {
				this.exception = e;
				this.thrown = e;
				this.iterationThrowable = e;
				close();
				throw e;
			} catch (RuntimeException e) {
				this.exception = e;
				this.thrown = e;
				this.iterationThrowable = e;
				close();
				throw e;
			} catch (Error e) {
				this.exception = databaseExceptionWithStatementContext(this.statementContext,
						format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), this.statementContext.getResultSetRowType().get()), e);
				this.thrown = e;
				this.iterationThrowable = e;
				close();
				throw e;
			}
		}

		private void callbackFailed(@NonNull Throwable throwable) {
			requireNonNull(throwable);
			this.callbackThrowable = throwable;
		}

		@Override
		public void close() {
			if (this.closed)
				return;

			this.closed = true;
			Throwable cleanupFailure = null;

			try {
				cleanupFailure = closeStatementContextResources(this.statementContext, cleanupFailure);

				if (this.resultSet != null) {
					try {
						this.resultSet.close();
					} catch (Throwable cleanupException) {
						cleanupFailure = cleanupFailure == null ? cleanupException : addSuppressed(cleanupFailure, cleanupException);
					}
				}

				if (this.preparedStatement != null) {
					try {
						this.preparedStatement.close();
					} catch (Throwable cleanupException) {
						cleanupFailure = cleanupFailure == null ? cleanupException : addSuppressed(cleanupFailure, cleanupException);
					}
				}

				cleanupFailure = completePostgreSqlStreamTransactionIfNeeded(cleanupFailure);

				if (this.connection != null && this.transaction.isEmpty()) {
					Duration heldDuration = this.connectionHeldStartTime == 0L
							? Duration.ZERO
							: Duration.ofNanos(nanoTime() - this.connectionHeldStartTime);
					try {
						this.database.closeConnection(this.connection);
						this.database.getMetricsCollectorDispatcher().didReleaseStatementConnection(this.statementContext, heldDuration);
					} catch (Throwable cleanupException) {
						this.database.getMetricsCollectorDispatcher().didFailToReleaseStatementConnection(this.statementContext, heldDuration, cleanupException);
						cleanupFailure = cleanupFailure == null ? cleanupException : addSuppressed(cleanupFailure, cleanupException);
					}
				}
			} finally {
				if (this.connectionLock != null)
					this.connectionLock.unlock();

				Duration mappingDuration = this.resultSetMappingNanos == 0L ? null : Duration.ofNanos(this.resultSetMappingNanos);

				StatementLog statementLog =
						StatementLog.withStatementContext(this.statementContext)
								.connectionAcquisitionDuration(this.connectionAcquisitionDuration)
								.preparationDuration(this.preparationDuration)
								.executionDuration(this.executionDuration)
								.resultSetMappingDuration(mappingDuration)
								.exception(this.exception)
								.build();

				if (this.thrown == null && this.exception == null) {
					this.database.getMetricsCollectorDispatcher().didExecuteStatement(this.statementContext, statementLog, StatementResult.empty());
				} else {
					Throwable statementThrowable = this.thrown == null ? this.exception : this.thrown;
					this.database.getMetricsCollectorDispatcher().didFailToExecuteStatement(this.statementContext, statementLog,
							this.database.peekDatabaseType(), requireNonNull(statementThrowable));
				}

				try {
					this.database.getStatementLogger().log(statementLog);
				} catch (Throwable cleanupException) {
					cleanupFailure = cleanupFailure == null ? cleanupException : addSuppressed(cleanupFailure, cleanupException);
				}
			}

			this.cleanupFailure = cleanupFailure;

			if (cleanupFailure != null) {
				if (this.thrown != null) {
					this.thrown.addSuppressed(cleanupFailure);
				} else if (cleanupFailure instanceof RuntimeException) {
					throw (RuntimeException) cleanupFailure;
				} else if (cleanupFailure instanceof Error) {
					throw (Error) cleanupFailure;
				} else {
					throw new RuntimeException(cleanupFailure);
				}
			}
		}

		@Nullable
		private Throwable completePostgreSqlStreamTransactionIfNeeded(@Nullable Throwable cleanupFailure) {
			if (!this.postgresqlStreamTransactionStarted || this.connection == null)
				return cleanupFailure;

			boolean shouldCommit = this.thrown == null && this.exception == null && this.callbackThrowable == null && !this.openFailed;

			try {
				if (shouldCommit)
					this.connection.commit();
				else
					this.connection.rollback();
			} catch (Throwable cleanupException) {
				cleanupFailure = cleanupFailure == null ? cleanupException : addSuppressed(cleanupFailure, cleanupException);
			}

			if (Boolean.TRUE.equals(this.initialAutoCommit)) {
				try {
					this.connection.setAutoCommit(true);
				} catch (Throwable cleanupException) {
					cleanupFailure = cleanupFailure == null ? cleanupException : addSuppressed(cleanupFailure, cleanupException);
				}
			}

			this.postgresqlStreamTransactionStarted = false;
			return cleanupFailure;
		}

		private void emitTerminalMetrics() {
			if (this.terminalMetricsEmitted)
				return;

			this.terminalMetricsEmitted = true;

			if (this.openFailed)
				return;

			MetricsCollector.StreamTerminalOutcome outcome;
			Throwable throwable;

			if (this.iterationThrowable != null) {
				outcome = MetricsCollector.StreamTerminalOutcome.ITERATION_FAILURE;
				throwable = this.iterationThrowable;
			} else if (this.callbackThrowable != null) {
				outcome = MetricsCollector.StreamTerminalOutcome.CALLBACK_FAILURE;
				throwable = this.callbackThrowable;
			} else if (this.exhausted) {
				outcome = MetricsCollector.StreamTerminalOutcome.COMPLETED_NORMALLY;
				throwable = this.cleanupFailure;
			} else {
				outcome = MetricsCollector.StreamTerminalOutcome.EARLY_CLOSE;
				throwable = this.cleanupFailure;
			}

			this.database.getMetricsCollectorDispatcher().didCloseStream(this.statementContext, outcome, this.rowsConsumed,
					Duration.ofNanos(nanoTime() - this.openStartTime), throwable);
		}

		@NonNull
		private static Throwable addSuppressed(@NonNull Throwable existing,
																					 @NonNull Throwable additional) {
			existing.addSuppressed(additional);
			return existing;
		}
	}

	/**
	 * Builder used to construct instances of {@link Database}.
	 * <p>
	 * This class is intended for use by a single thread.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 1.0.0
	 */
	@NotThreadSafe
	public static class Builder {
		@NonNull
		private final DataSource dataSource;
		@Nullable
		private DatabaseType databaseType;
		@Nullable
		private ZoneId timeZone;
		@Nullable
		private AmbiguousTimestampBindingStrategy ambiguousTimestampBindingStrategy;
		@Nullable
		private InstanceProvider instanceProvider;
		@Nullable
		private PreparedStatementBinder preparedStatementBinder;
		@Nullable
		private ResultSetMapper resultSetMapper;
		@Nullable
		private StatementLogger statementLogger;
		@Nullable
		private MetricsCollector metricsCollector;
		@Nullable
		private Duration queryTimeout;
		@Nullable
		private Integer fetchSize;
		@Nullable
		private Integer maxRows;
		@Nullable
		private Integer parsedSqlCacheCapacity;

		private Builder(@NonNull DataSource dataSource) {
			this.dataSource = requireNonNull(dataSource);
			this.databaseType = null;
			this.metricsCollector = null;
			this.queryTimeout = null;
			this.fetchSize = null;
			this.maxRows = null;
			this.parsedSqlCacheCapacity = null;
		}

		/**
		 * Overrides automatic database type detection.
		 * <p>
		 * If {@code null}, the database type is detected lazily when database-type-specific behavior is first needed.
		 * Supplying a non-null value avoids automatic detection and its metadata lookup entirely.
		 *
		 * @param databaseType the database type to use (null to enable auto-detection)
		 * @return this {@code Builder}, for chaining
		 * @since 4.0.0
		 */
		@NonNull
		public Builder databaseType(@Nullable DatabaseType databaseType) {
			this.databaseType = databaseType;
			return this;
		}

		/**
		 * Configures the database time zone Pyranid should use when converting zone-less temporal values.
		 * <p>
		 * This value is used when mapping {@code TIMESTAMP} values to instant-based Java types, and when binding
		 * {@link java.time.Instant} or {@link java.time.OffsetDateTime} parameters to known {@code TIMESTAMP}
		 * targets. It also applies to ambiguous timestamp bindings if
		 * {@link #ambiguousTimestampBindingStrategy(AmbiguousTimestampBindingStrategy)} is configured with
		 * {@link AmbiguousTimestampBindingStrategy#TIMESTAMP_WITHOUT_TIME_ZONE}.
		 * <p>
		 * If {@code null}, Pyranid uses {@link ZoneId#systemDefault()}.
		 *
		 * @param timeZone database time zone to use, or {@code null} for the JVM default zone
		 * @return this {@code Builder}, for chaining
		 * @since 3.0.0
		 */
		@NonNull
		public Builder timeZone(@Nullable ZoneId timeZone) {
			this.timeZone = timeZone;
			return this;
		}

		/**
		 * Configures how Pyranid binds {@link java.time.Instant} and {@link java.time.OffsetDateTime} parameters
		 * when JDBC parameter metadata cannot identify whether the target is {@code TIMESTAMP} or
		 * {@code TIMESTAMP WITH TIME ZONE}.
		 * <p>
		 * The default, {@link AmbiguousTimestampBindingStrategy#TIMESTAMP_WITH_TIME_ZONE}, is appropriate for
		 * timestamp-with-time-zone targets. Use
		 * {@link AmbiguousTimestampBindingStrategy#TIMESTAMP_WITHOUT_TIME_ZONE} for drivers or proxies that
		 * cannot provide identifying parameter metadata when your target columns are zone-less {@code TIMESTAMP}
		 * values that should be interpreted in {@link #timeZone(ZoneId)}.
		 *
		 * @param ambiguousTimestampBindingStrategy strategy to use, or {@code null} for the default
		 * @return this {@code Builder}, for chaining
		 * @since 4.2.0
		 */
		@NonNull
		public Builder ambiguousTimestampBindingStrategy(@Nullable AmbiguousTimestampBindingStrategy ambiguousTimestampBindingStrategy) {
			this.ambiguousTimestampBindingStrategy = ambiguousTimestampBindingStrategy;
			return this;
		}

		@NonNull
		public Builder instanceProvider(@Nullable InstanceProvider instanceProvider) {
			this.instanceProvider = instanceProvider;
			return this;
		}

		@NonNull
		public Builder preparedStatementBinder(@Nullable PreparedStatementBinder preparedStatementBinder) {
			this.preparedStatementBinder = preparedStatementBinder;
			return this;
		}

		@NonNull
		public Builder resultSetMapper(@Nullable ResultSetMapper resultSetMapper) {
			this.resultSetMapper = resultSetMapper;
			return this;
		}

		/**
		 * Configures the statement logger for the {@link Database} being built.
		 * <p>
		 * {@link StatementLogger} failures are fail-fast: a logger exception can make a successful statement operation throw,
		 * and inside a Pyranid transaction it participates in normal rollback handling. If the database statement itself failed,
		 * logger failures are suppressed onto the primary statement failure.
		 *
		 * @param statementLogger statement logger to use, or {@code null} for a no-op logger
		 * @return this {@code Builder}, for chaining
		 */
		@NonNull
		public Builder statementLogger(@Nullable StatementLogger statementLogger) {
			this.statementLogger = statementLogger;
			return this;
		}

		/**
		 * Configures the metrics collector for the {@link Database} being built.
		 * <p>
		 * Like all {@code Database} configuration, this value is fixed at {@link #build()} time. Specify {@code null}
		 * or omit this setter to disable metrics collection.
		 *
		 * @param metricsCollector metrics collector to use, or {@code null} to disable
		 * @return this {@code Builder}, for chaining
		 * @since 4.2.0
		 */
		@NonNull
		public Builder metricsCollector(@Nullable MetricsCollector metricsCollector) {
			this.metricsCollector = metricsCollector;
			return this;
		}

		/**
		 * Configures a database-wide JDBC query timeout default.
		 * <p>
		 * This maps to {@link java.sql.Statement#setQueryTimeout(int)}. {@code null} leaves the timeout unset.
		 * {@link Duration#ZERO} disables the JDBC timeout. Positive sub-second durations are rounded up to one second
		 * because JDBC accepts whole seconds. Per-query {@link Query#queryTimeout(Duration)} settings override this value,
		 * and {@link Query#customize(PreparedStatementCustomizer)} runs last.
		 *
		 * @param queryTimeout timeout to apply by default, or {@code null} to leave unset
		 * @return this {@code Builder}, for chaining
		 * @since 4.2.0
		 */
		@NonNull
		public Builder queryTimeout(@Nullable Duration queryTimeout) {
			this.queryTimeout = validateQueryTimeout(queryTimeout);
			return this;
		}

		/**
		 * Configures a database-wide JDBC fetch size default.
		 * <p>
		 * This maps to {@link java.sql.Statement#setFetchSize(int)}. {@code null} leaves the fetch size unset. A value
		 * of {@code 0} uses the driver's default fetch-size behavior. Per-query {@link Query#fetchSize(Integer)}
		 * settings override this value, and {@link Query#customize(PreparedStatementCustomizer)} runs last.
		 *
		 * @param fetchSize fetch size to apply by default, or {@code null} to leave unset
		 * @return this {@code Builder}, for chaining
		 * @since 4.2.0
		 */
		@NonNull
		public Builder fetchSize(@Nullable Integer fetchSize) {
			this.fetchSize = validateNonNegativeStatementSetting("fetchSize", fetchSize);
			return this;
		}

		/**
		 * Configures a database-wide JDBC maximum row count default.
		 * <p>
		 * This maps to {@link java.sql.Statement#setMaxRows(int)}. {@code null} leaves the maximum row count unset. A
		 * value of {@code 0} disables the JDBC row limit. Per-query {@link Query#maxRows(Integer)} settings override
		 * this value, and {@link Query#customize(PreparedStatementCustomizer)} runs last.
		 *
		 * @param maxRows maximum rows to apply by default, or {@code null} to leave unset
		 * @return this {@code Builder}, for chaining
		 * @since 4.2.0
		 */
		@NonNull
		public Builder maxRows(@Nullable Integer maxRows) {
			this.maxRows = validateNonNegativeStatementSetting("maxRows", maxRows);
			return this;
		}

		/**
		 * Configures the size of the parsed SQL cache.
		 * <p>
		 * A value of {@code 0} disables caching. A value of {@code null} uses the default size.
		 *
		 * @param parsedSqlCacheCapacity cache size (0 disables caching, null uses default)
		 * @return this {@code Builder}, for chaining
		 */
		@NonNull
		public Builder parsedSqlCacheCapacity(@Nullable Integer parsedSqlCacheCapacity) {
			if (parsedSqlCacheCapacity != null && parsedSqlCacheCapacity < 0)
				throw new IllegalArgumentException("parsedSqlCacheCapacity must be >= 0");

			this.parsedSqlCacheCapacity = parsedSqlCacheCapacity;
			return this;
		}

		@NonNull
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
		@NonNull
		private final StatementResult statementResult;

		public DatabaseOperationResult(@Nullable Duration executionDuration,
																	 @Nullable Duration resultSetMappingDuration) {
			this(executionDuration, resultSetMappingDuration, StatementResult.empty());
		}

		public DatabaseOperationResult(@Nullable Duration executionDuration,
																	 @Nullable Duration resultSetMappingDuration,
																	 @NonNull StatementResult statementResult) {
			this.executionDuration = executionDuration;
			this.resultSetMappingDuration = resultSetMappingDuration;
			this.statementResult = requireNonNull(statementResult);
		}

		@NonNull
		public Optional<Duration> getExecutionDuration() {
			return Optional.ofNullable(this.executionDuration);
		}

		@NonNull
		public Optional<Duration> getResultSetMappingDuration() {
			return Optional.ofNullable(this.resultSetMappingDuration);
		}

		@NonNull
		public StatementResult getStatementResult() {
			return this.statementResult;
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
