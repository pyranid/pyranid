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
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

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
	private static final int DEFAULT_PARSED_SQL_CACHE_CAPACITY = 1024;
	private static final int MAX_DIAGNOSTIC_MESSAGE_LENGTH = 1024;
	private static final int MAX_DIAGNOSTIC_PARAMETER_LENGTH = 256;
	private static final int MAX_DIAGNOSTIC_PARAMETERS_LENGTH = 2048;
	private static final int MAX_DIAGNOSTIC_SQL_LENGTH = 2048;
	@NonNull
	private static final String TRUNCATED_SUFFIX = "... (truncated)";
	@NonNull
	private static final Pattern DIAGNOSTIC_WHITESPACE_PATTERN = Pattern.compile("\\s+");

	@NonNull
	private final DataSource dataSource;
	@NonNull
	private final AtomicReference<@Nullable DatabaseType> databaseType;
	@NonNull
	private final AtomicReference<@Nullable DatabaseDialect> databaseDialect;
	@NonNull
	private final ThreadLocal<Connection> databaseTypeDetectionConnectionHolder;
	@NonNull
	private final ZoneId timeZone;
	@NonNull
	private final AmbiguousTimestampBindingStrategy ambiguousTimestampBindingStrategy;
	@NonNull
	private final InstanceProvider instanceProvider;
	@NonNull
	private final ResultSetMapper resultSetMapper;
	@NonNull
	private final ParameterRedactor parameterRedactor;
	@NonNull
	private final MetricsCollectorDispatcher metricsCollectorDispatcher;
	@NonNull
	private final TransactionCoordinator transactions;
	@Nullable
	private final Duration queryTimeout;
	@Nullable
	private final Integer fetchSize;
	@Nullable
	private final Integer maxRows;
	@NonNull
	private final SqlCompiler sqlCompiler;
	@NonNull
	private final AtomicLong defaultIdGenerator;

	@NonNull
	private final StatementExecutor statementExecutor;

	private Database(@NonNull Builder builder) {
		requireNonNull(builder);

		this.dataSource = requireNonNull(builder.dataSource);
		this.databaseType = new AtomicReference<>(builder.databaseType);
		this.databaseDialect = new AtomicReference<>(builder.databaseType == null ? null : builder.databaseType.dialect());
		this.databaseTypeDetectionConnectionHolder = new ThreadLocal<>();
		this.timeZone = builder.timeZone == null ? ZoneId.systemDefault() : builder.timeZone;
		this.ambiguousTimestampBindingStrategy = builder.ambiguousTimestampBindingStrategy == null
				? AmbiguousTimestampBindingStrategy.TIMESTAMP_WITH_TIME_ZONE
				: builder.ambiguousTimestampBindingStrategy;
		this.instanceProvider = builder.instanceProvider == null ? new InstanceProvider() {} : builder.instanceProvider;
		PreparedStatementBinder preparedStatementBinder = builder.preparedStatementBinder == null ? PreparedStatementBinder.withDefaultConfiguration() : builder.preparedStatementBinder;
		this.resultSetMapper = builder.resultSetMapper == null ? ResultSetMapper.withDefaultConfiguration() : builder.resultSetMapper;
		StatementLogger statementLogger = builder.statementLogger == null ? (statementLog) -> {} : builder.statementLogger;
		this.parameterRedactor = builder.parameterRedactor == null ? ParameterRedactor.none() : builder.parameterRedactor;
		this.metricsCollectorDispatcher = new MetricsCollectorDispatcher(builder.metricsCollector);
		this.transactions = new TransactionCoordinator(this.dataSource, this.metricsCollectorDispatcher,
				this::peekDatabaseType, this::getDatabaseType);
		this.queryTimeout = validateQueryTimeout(builder.queryTimeout);
		this.fetchSize = validateNonNegativeStatementSetting("fetchSize", builder.fetchSize);
		this.maxRows = validateNonNegativeStatementSetting("maxRows", builder.maxRows);
		if (builder.parsedSqlCacheCapacity != null && builder.parsedSqlCacheCapacity < 0)
			throw new IllegalArgumentException("parsedSqlCacheCapacity must be >= 0");

		int parsedSqlCacheCapacity = builder.parsedSqlCacheCapacity == null
				? DEFAULT_PARSED_SQL_CACHE_CAPACITY
				: builder.parsedSqlCacheCapacity;
		this.sqlCompiler = new SqlCompiler(parsedSqlCacheCapacity);
		this.defaultIdGenerator = new AtomicLong();
		this.statementExecutor = new StatementExecutor(this, this.instanceProvider, this.resultSetMapper, preparedStatementBinder,
				statementLogger, this.metricsCollectorDispatcher, new StatementConnections(this.dataSource,
				this::currentTransactionForDatabaseOperation, Database::closeConnection, this.databaseTypeDetectionConnectionHolder,
				this::getDatabaseDialect, this::peekDatabaseType));
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
		return this.transactions.currentTransaction();
	}

	@NonNull
	Optional<Transaction> currentTransactionForDatabaseOperation() {
		return this.transactions.currentTransactionForDatabaseOperation();
	}

	static boolean hasAmbientTransaction() {
		return TransactionCoordinator.hasAmbientTransaction();
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
		return this.transactions.run(transactionOptions, transactionalOperation);
	}

	/**
	 * Performs an operation transactionally, retrying according to the given retry policy.
	 * <p>
	 * The entire transaction closure may run more than once. Keep non-idempotent external side effects outside the closure
	 * unless they are safe to repeat.
	 * <p>
	 * Pyranid consults the retry policy only when replay is known to be safe. This includes a failure before a physical
	 * transaction becomes available to application work, even if best-effort rollback of that begin candidate fails, because
	 * the candidate is discarded. It also includes a recognized serialization failure reported by physical commit when the
	 * follow-up rollback succeeds. Other commit failures and rollback failures after application work could execute are
	 * terminal because their outcome is indeterminate.
	 * <p>
	 * Unlike {@link #transaction(TransactionalOperation)} and related transaction methods, retrying methods return
	 * {@link TransactionRetryResult} so callers can inspect failures that were recovered before success.
	 * <p>
	 * This method fails fast if called inside an active transaction for this {@code Database}. Retrying a nested unit cannot
	 * restart the outer transaction safely.
	 *
	 * @param retryPolicy           retry policy to apply
	 * @param transactionalOperation the operation to perform transactionally
	 * @return retry result containing any failures retried before success
	 * @since 4.4.0
	 */
	@NonNull
	public TransactionRetryResult<Void> transactionWithRetry(@NonNull RetryPolicy retryPolicy,
																													@NonNull TransactionalOperation transactionalOperation) {
		requireNonNull(retryPolicy);
		requireNonNull(transactionalOperation);

		return transactionWithRetry(retryPolicy, () -> {
			transactionalOperation.perform();
			return Optional.<Void>empty();
		});
	}

	/**
	 * Performs an operation transactionally with the given options, retrying according to the given retry policy.
	 * <p>
	 * The entire transaction closure may run more than once. Keep non-idempotent external side effects outside the closure
	 * unless they are safe to repeat.
	 * <p>
	 * Pyranid consults the retry policy only when replay is known to be safe. This includes a failure before a physical
	 * transaction becomes available to application work, even if best-effort rollback of that begin candidate fails, because
	 * the candidate is discarded. It also includes a recognized serialization failure reported by physical commit when the
	 * follow-up rollback succeeds. Other commit failures and rollback failures after application work could execute are
	 * terminal because their outcome is indeterminate.
	 * <p>
	 * Unlike {@link #transaction(TransactionOptions, TransactionalOperation)} and related transaction methods, retrying
	 * methods return {@link TransactionRetryResult} so callers can inspect failures that were recovered before success.
	 * <p>
	 * This method fails fast if called inside an active transaction for this {@code Database}. Retrying a nested unit cannot
	 * restart the outer transaction safely.
	 *
	 * @param retryPolicy           retry policy to apply
	 * @param transactionOptions     options to apply to each transaction attempt
	 * @param transactionalOperation the operation to perform transactionally
	 * @return retry result containing any failures retried before success
	 * @since 4.4.0
	 */
	@NonNull
	public TransactionRetryResult<Void> transactionWithRetry(@NonNull RetryPolicy retryPolicy,
																													@NonNull TransactionOptions transactionOptions,
																													@NonNull TransactionalOperation transactionalOperation) {
		requireNonNull(retryPolicy);
		requireNonNull(transactionOptions);
		requireNonNull(transactionalOperation);

		return transactionWithRetry(retryPolicy, transactionOptions, () -> {
			transactionalOperation.perform();
			return Optional.<Void>empty();
		});
	}

	/**
	 * Performs an operation transactionally and optionally returns a value, retrying according to the given retry policy.
	 * <p>
	 * The entire transaction closure may run more than once. Keep non-idempotent external side effects outside the closure
	 * unless they are safe to repeat.
	 * <p>
	 * Pyranid consults the retry policy only when replay is known to be safe. This includes a failure before a physical
	 * transaction becomes available to application work, even if best-effort rollback of that begin candidate fails, because
	 * the candidate is discarded. It also includes a recognized serialization failure reported by physical commit when the
	 * follow-up rollback succeeds. Other commit failures and rollback failures after application work could execute are
	 * terminal because their outcome is indeterminate.
	 * <p>
	 * Unlike {@link #transaction(ReturningTransactionalOperation)} and related transaction methods, retrying methods return
	 * {@link TransactionRetryResult} so callers can inspect failures that were recovered before success.
	 * <p>
	 * This method fails fast if called inside an active transaction for this {@code Database}. Retrying a nested unit cannot
	 * restart the outer transaction safely.
	 *
	 * @param retryPolicy           retry policy to apply
	 * @param transactionalOperation the operation to perform transactionally
	 * @param <T>                    the type to be returned
	 * @return retry result containing the successful transaction value and any failures retried before success
	 * @since 4.4.0
	 */
	@NonNull
	public <T> TransactionRetryResult<T> transactionWithRetry(@NonNull RetryPolicy retryPolicy,
																													 @NonNull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(retryPolicy);
		requireNonNull(transactionalOperation);

		return transactionWithRetry(retryPolicy, TransactionOptions.DEFAULT, transactionalOperation);
	}

	/**
	 * Performs an operation transactionally with the given options and optionally returns a value, retrying according to the
	 * given retry policy.
	 * <p>
	 * The entire transaction closure may run more than once. Keep non-idempotent external side effects outside the closure
	 * unless they are safe to repeat.
	 * <p>
	 * Pyranid consults the retry policy only when replay is known to be safe. This includes a failure before a physical
	 * transaction becomes available to application work, even if best-effort rollback of that begin candidate fails, because
	 * the candidate is discarded. It also includes a recognized serialization failure reported by physical commit when the
	 * follow-up rollback succeeds. Other commit failures and rollback failures after application work could execute are
	 * terminal because their outcome is indeterminate.
	 * <p>
	 * Unlike {@link #transaction(TransactionOptions, ReturningTransactionalOperation)} and related transaction methods,
	 * retrying methods return {@link TransactionRetryResult} so callers can inspect failures that were recovered before
	 * success.
	 * <p>
	 * This method fails fast if called inside an active transaction for this {@code Database}. Retrying a nested unit cannot
	 * restart the outer transaction safely.
	 *
	 * @param retryPolicy           retry policy to apply
	 * @param transactionOptions     options to apply to each transaction attempt
	 * @param transactionalOperation the operation to perform transactionally
	 * @param <T>                    the type to be returned
	 * @return retry result containing the successful transaction value and any failures retried before success
	 * @since 4.4.0
	 */
	@NonNull
	public <T> TransactionRetryResult<T> transactionWithRetry(@NonNull RetryPolicy retryPolicy,
																													 @NonNull TransactionOptions transactionOptions,
																													 @NonNull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(retryPolicy);
		requireNonNull(transactionOptions);
		requireNonNull(transactionalOperation);

		if (currentTransaction().isPresent())
			throw new IllegalStateException("transactionWithRetry must not be called within an existing transaction");

		return RetryExecutor.DEFAULT.execute(retryPolicy, () -> transaction(transactionOptions, transactionalOperation));
	}

	static void closeConnection(@NonNull Connection connection) {
		requireNonNull(connection);

		try {
			connection.close();
		} catch (SQLException e) {
			throw DatabaseException.wrapSafely("Unable to close database connection", e, DatabaseDialect.forExceptionCause(e));
		}
	}

	@NonNull
	static DatabaseException databaseExceptionWithStatementContext(@NonNull StatementContext<?> statementContext,
																															@NonNull Throwable cause) {
		requireNonNull(statementContext);
		requireNonNull(cause);

		if (cause instanceof DatabaseException databaseException && databaseException.areStatementDiagnosticsApplied())
			return databaseException;

		String message = DatabaseException.messageSafely(cause, "Database operation failed");

		if (cause instanceof DatabaseException databaseException) {
			SecureParameterSupport.DiagnosticScrub diagnosticScrub = diagnosticScrubForStatementContext(statementContext);

			if (diagnosticScrub.redactor() == null && diagnosticScrub.needleRenderingFailureDescriptions().isEmpty())
				return databaseException;

			return databaseExceptionWithStatementContext(statementContext, message, cause, diagnosticScrub);
		}

		return databaseExceptionWithStatementContext(statementContext, message, cause);
	}

	@NonNull
	static DatabaseException databaseExceptionWithStatementContext(@NonNull StatementContext<?> statementContext,
																																				@NonNull String message,
																																				@NonNull Throwable cause) {
		requireNonNull(statementContext);
		requireNonNull(message);
		requireNonNull(cause);

		SecureParameterSupport.DiagnosticScrub diagnosticScrub = diagnosticScrubForStatementContext(statementContext);
		return databaseExceptionWithStatementContext(statementContext, message, cause, diagnosticScrub);
	}

	@NonNull
	static DatabaseException databaseExceptionWithStatementContext(@NonNull StatementContext<?> statementContext,
																															@NonNull String message,
																															@NonNull Throwable cause,
																															SecureParameterSupport.@NonNull DiagnosticScrub diagnosticScrub) {
		requireNonNull(statementContext);
		requireNonNull(message);
		requireNonNull(cause);
		requireNonNull(diagnosticScrub);

		UnaryOperator<String> diagnosticRedactor = diagnosticScrub.redactor();

		// Scrub the RAW message before whitespace-collapsing/truncation: bounding first would break needles
		// containing consecutive whitespace and can cut a secret mid-way, leaving an unmatchable prefix
		String scrubbedMessage;
		try {
			scrubbedMessage = diagnosticRedactor == null ? message : diagnosticRedactor.apply(message);
		} catch (Throwable diagnosticFailure) {
			if (cause != diagnosticFailure) cause.addSuppressed(diagnosticFailure);
			// A failed redactor must not expose the unredacted driver text as the replacement message.
			scrubbedMessage = "Database operation failed";
		}

		StatementDiagnostic statementDiagnostic = statementDiagnostic(statementContext);
		String diagnosticMessage = format("%s [%s]",
				boundedDiagnosticMessage(scrubbedMessage), statementDiagnostic.diagnostic());
		DatabaseException databaseException = DatabaseException.wrapSafely(diagnosticMessage, cause,
				databaseDialectForException(statementContext, cause), diagnosticRedactor);
		databaseException.markStatementDiagnosticsApplied();

		if (statementDiagnostic.parameterRenderingFailure() != null)
			databaseException.addSuppressed(statementDiagnostic.parameterRenderingFailure());

		for (String failureDescription : diagnosticScrub.needleRenderingFailureDescriptions())
			databaseException.addSuppressed(new RuntimeException(format(
					"Unable to render a secure parameter for diagnostic scrubbing: %s", failureDescription)));

		return databaseException;
	}

	private static SecureParameterSupport.@NonNull DiagnosticScrub diagnosticScrubForStatementContext(@NonNull StatementContext<?> statementContext) {
		requireNonNull(statementContext);

		try {
			return SecureParameterSupport.diagnosticScrubForParameters(statementContext.getParameters());
		} catch (Throwable ignored) {
			// Per-needle failures are handled inside diagnosticScrubForParameters; reaching here would be
			// catastrophic (a plain getter throwing) - fall back to no scrubbing rather than losing the exception
			return SecureParameterSupport.DiagnosticScrub.NONE;
		}
	}

	@NonNull
	private static DatabaseDialect databaseDialectForException(@NonNull StatementContext<?> statementContext,
																														 @NonNull Throwable cause) {
		requireNonNull(statementContext);
		requireNonNull(cause);

		try {
			return statementContext.getDatabaseDialect();
		} catch (Throwable ignored) {
			return DatabaseDialect.forExceptionCause(cause);
		}
	}

	@NonNull
	private DatabaseException databaseExceptionWithRawConnectionContext(@Nullable Connection connection,
																																			@NonNull Exception cause) {
		requireNonNull(cause);

		return DatabaseException.wrapSafely(DatabaseException.messageSafely(cause, "Database operation failed"),
				cause, databaseDialectForRawConnectionException(connection, cause));
	}

	@NonNull
	private DatabaseDialect databaseDialectForRawConnectionException(@Nullable Connection connection,
																																	@NonNull Throwable cause) {
		requireNonNull(cause);

		if (connection != null) {
			try {
				return getDatabaseDialect(connection);
			} catch (Throwable ignored) {
				// Fall through to cause-based classification below.
			}
		}

		return DatabaseDialect.forExceptionCause(cause);
	}

	@NonNull
	private static StatementDiagnostic statementDiagnostic(@NonNull StatementContext<?> statementContext) {
		requireNonNull(statementContext);

		Statement statement = statementContext.getStatement();
		String parameters;
		Throwable parameterRenderingFailure = null;

		try {
			parameters = boundedDiagnosticParameters(statementContext.getRedactedParameters());
		} catch (Throwable t) {
			parameters = "<unavailable>";
			parameterRenderingFailure = t;
		}

		String diagnostic = format("statementId=%s, sql=%s, parameters=%s",
				statement.getId(), boundedSql(statement.getSql()), parameters);

		if (parameterRenderingFailure != null)
			diagnostic = format("%s, parameterRenderingFailure=%s", diagnostic,
					boundedDiagnosticParameter(parameterRenderingFailureDiagnostic(parameterRenderingFailure)));

		return new StatementDiagnostic(diagnostic, parameterRenderingFailure);
	}

	private record StatementDiagnostic(@NonNull String diagnostic,
																		 @Nullable Throwable parameterRenderingFailure) {}

	@NonNull
	private static String parameterRenderingFailureDiagnostic(@NonNull Throwable parameterRenderingFailure) {
		requireNonNull(parameterRenderingFailure);

		String message;

		try {
			message = parameterRenderingFailure.getMessage();
		} catch (Throwable ignored) {
			message = null;
		}

		if (message == null || message.trim().length() == 0)
			return parameterRenderingFailure.getClass().getName();

		return format("%s: %s", parameterRenderingFailure.getClass().getName(), message);
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

	@NonNull
	private static String boundedDiagnosticParameters(@NonNull List<@Nullable Object> parameters) {
		requireNonNull(parameters);

		StringBuilder parametersBuilder = new StringBuilder("[");

		for (int i = 0; i < parameters.size(); ++i) {
			String separator = i == 0 ? "" : ", ";
			String renderedParameter = boundedDiagnosticParameter(parameters.get(i));
			int requiredLength = parametersBuilder.length() + separator.length() + renderedParameter.length() + 1;

			if (requiredLength > MAX_DIAGNOSTIC_PARAMETERS_LENGTH) {
				int availableLength = MAX_DIAGNOSTIC_PARAMETERS_LENGTH
						- parametersBuilder.length()
						- separator.length()
						- TRUNCATED_SUFFIX.length()
						- 1;

				parametersBuilder.append(separator);

				if (availableLength > 0)
					parametersBuilder.append(renderedParameter, 0, Math.min(availableLength, renderedParameter.length()));

				parametersBuilder.append(TRUNCATED_SUFFIX).append(']');
				if (parametersBuilder.length() > MAX_DIAGNOSTIC_PARAMETERS_LENGTH) {
					parametersBuilder.setLength(MAX_DIAGNOSTIC_PARAMETERS_LENGTH - 1);
					parametersBuilder.append(']');
				}
				return parametersBuilder.toString();
			}

			parametersBuilder.append(separator).append(renderedParameter);
		}

		parametersBuilder.append(']');
		return parametersBuilder.toString();
	}

	@NonNull
	private static String boundedDiagnosticParameter(@Nullable Object parameter) {
		String renderedParameter = String.valueOf(parameter);
		String compactParameter = DIAGNOSTIC_WHITESPACE_PATTERN.matcher(renderedParameter).replaceAll(" ").trim();

		if (compactParameter.length() <= MAX_DIAGNOSTIC_PARAMETER_LENGTH)
			return compactParameter;

		int prefixLength = Math.max(0, MAX_DIAGNOSTIC_PARAMETER_LENGTH - TRUNCATED_SUFFIX.length());
		return compactParameter.substring(0, prefixLength) + TRUNCATED_SUFFIX;
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
	 * <p>
	 * If this thread is interrupted while waiting for another participant to release the transaction connection, Pyranid
	 * restores the interrupt flag and throws {@link DatabaseException}.
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
	 * <p>
	 * If this thread is interrupted while waiting for another participant to release the transaction connection, Pyranid
	 * restores the interrupt flag and throws {@link DatabaseException}.
	 *
	 * @param transaction            the transaction in which to participate
	 * @param transactionalOperation the operation that should participate in the transaction
	 * @param <T>                    the type to be returned
	 * @return the result of the transactional operation
	 */
	@NonNull
	public <T> Optional<T> participate(@NonNull Transaction transaction,
																		 @NonNull ReturningTransactionalOperation<T> transactionalOperation) {
		return this.transactions.participate(transaction, transactionalOperation);
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
		return new DefaultQuery(this, sql, this.sqlCompiler, this.statementExecutor);
	}

	/**
	 * Performs a portable connectivity check using JDBC {@link Connection#isValid(int)}.
	 * <p>
	 * This method borrows a fresh connection from this database's {@link DataSource}, calls
	 * {@link Connection#isValid(int)}, and closes the connection before returning. It does <strong>not</strong>
	 * participate in an active Pyranid transaction, if one exists.
	 * <p>
	 * JDBC accepts timeout values in whole seconds. Positive sub-second durations are rounded up to one second;
	 * {@link Duration#ZERO} passes a timeout of {@code 0} to the driver.
	 *
	 * @param timeout maximum time to wait for driver validation
	 * @throws IllegalArgumentException if {@code timeout} is negative or too large for JDBC's integer-second timeout
	 * @throws DatabaseException if connection acquisition fails, validation throws, or the driver reports the
	 *                           connection is not valid
	 * @since 4.2.0
	 */
	public void performHealthCheck(@NonNull Duration timeout) {
		requireNonNull(timeout);
		int timeoutSeconds = healthCheckTimeoutSeconds(timeout);

		performRawConnectionOperation(connection -> {
			boolean valid;

			try {
				valid = connection.isValid(timeoutSeconds);
			} catch (SQLException e) {
				throw new DatabaseException("Unable to perform database health check", e);
			}

			if (!valid)
				throw new DatabaseException("Database health check failed: connection is not valid");

			return Optional.empty();
		}, false);
	}

	static void restoreInterruptIfNeeded(@NonNull Throwable throwable) {
		requireNonNull(throwable);
		// Failure inspection is best effort; it must never prevent cleanup or replace the primary exception.
		if (ThrowableGraph.inspect(throwable, current -> current instanceof InterruptedException ? 1 : 0, 0) != 0)
			Thread.currentThread().interrupt();
	}

	static void lockInterruptibly(@NonNull ReentrantLock lock,
																				@NonNull String operation) {
		requireNonNull(lock);
		requireNonNull(operation);

		try {
			lock.lockInterruptibly();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new DatabaseException(format("Interrupted while waiting to %s", operation), e);
		}
	}

	@Nullable
	static Throwable closeStatementContextResources(@NonNull StatementContext<?> statementContext,
																													@Nullable Throwable cleanupFailure) {
		requireNonNull(statementContext);

		Queue<AutoCloseable> cleanupOperations = statementContext.getCleanupOperations();
		AutoCloseable cleanupOperation;

		while ((cleanupOperation = cleanupOperations.poll()) != null) {
			try {
				cleanupOperation.close();
			} catch (Throwable cleanupException) {
				cleanupFailure = appendCleanupFailure(cleanupFailure, cleanupException);
			}
		}

		return cleanupFailure;
	}

	/** Keeps the first failure while allowing cleanup to continue when a driver reuses an exception instance. */
	@NonNull
	static Throwable appendCleanupFailure(@Nullable Throwable existing, @NonNull Throwable additional) {
		requireNonNull(additional);
		if (existing == null)
			return additional;
		if (existing != additional)
			existing.addSuppressed(additional);
		return existing;
	}

	static boolean isUnsupportedSqlFeature(@NonNull SQLException e) {
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
	static Duration validateQueryTimeout(@Nullable Duration queryTimeout) {
		if (queryTimeout != null) {
			if (queryTimeout.isNegative())
				throw new IllegalArgumentException("queryTimeout must be >= 0");

			queryTimeoutSeconds(queryTimeout);
		}

		return queryTimeout;
	}

	@Nullable
	static Integer validateNonNegativeStatementSetting(@NonNull String name,
																														 @Nullable Integer value) {
		requireNonNull(name);

		if (value != null && value < 0)
			throw new IllegalArgumentException(format("%s must be >= 0", name));

		return value;
	}

	@Nullable
	static Integer validatePositiveQuerySetting(@NonNull String name,
																										 @Nullable Integer value) {
		requireNonNull(name);

		if (value != null && value <= 0)
			throw new IllegalArgumentException(format("%s must be > 0", name));

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

	private static int healthCheckTimeoutSeconds(@NonNull Duration timeout) {
		requireNonNull(timeout);

		if (timeout.isNegative())
			throw new IllegalArgumentException("timeout must be >= 0");

		long seconds = timeout.getSeconds();

		if (timeout.getNano() > 0) {
			if (seconds == Long.MAX_VALUE)
				throw new IllegalArgumentException(format("timeout must be <= %s seconds", Integer.MAX_VALUE));

			++seconds;
		}

		if (seconds > Integer.MAX_VALUE)
			throw new IllegalArgumentException(format("timeout must be <= %s seconds", Integer.MAX_VALUE));

		return (int) seconds;
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
	 * delegated to the underlying driver connection, but lifecycle, transaction-management, and connection-wide state
	 * methods such as
	 * {@link Connection#close()}, {@link Connection#commit()}, {@link Connection#rollback()},
	 * {@link Connection#setAutoCommit(boolean)}, {@link Connection#setCatalog(String)}, {@link Connection#setSchema(String)}, and
	 * {@link Connection#setNetworkTimeout(java.util.concurrent.Executor, int)} throw {@link IllegalStateException}. Use
	 * Pyranid transaction APIs instead.
	 * {@link Connection#unwrap(Class)} may return a guarded, callback-scoped proxy for a vendor interface, but never a
	 * castable physical {@link Connection}; the proxy blocks lifecycle methods and expires with the callback.
	 * JDBC objects created from this handle are also guarded: {@link java.sql.Statement#getConnection()} and
	 * {@link java.sql.DatabaseMetaData#getConnection()} return the Pyranid-managed handle, and
	 * {@link ResultSet#getStatement()} returns a guarded statement. Guarded statements, resultsets, and metadata refuse
	 * driver-specific {@code unwrap(...)} calls that could expose the driver's underlying connection.
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
	public <T> Optional<T> useRawConnection(@NonNull RawConnectionOperation<Connection, T> rawConnectionOperation) {
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

	/**
	 * Performs driver-specific raw JDBC work with a Pyranid-managed connection of the requested type.
	 * <p>
	 * If called inside a Pyranid transaction, this operation uses the transaction's connection. Standard JDBC work
	 * participates in that transaction; driver-extension APIs may impose additional transaction-start or lifecycle rules.
	 * Otherwise, Pyranid borrows a connection for the duration of the callback and closes it afterwards.
	 * <p>
	 * If the managed connection is already an instance of {@code connectionType}, it is passed directly to
	 * {@code rawConnectionOperation}. Otherwise, Pyranid obtains the requested driver connection through
	 * {@link Connection#unwrap(Class)}. Failure to obtain the requested type results in a {@link DatabaseException}.
	 * <p>
	 * <strong>Unlike {@link #useRawConnection(RawConnectionOperation)}, the connection supplied by this overload is
	 * unguarded.</strong> This is necessary for concrete driver APIs, but it also means Pyranid cannot prevent operations
	 * that interfere with its connection and transaction management. Guarding is selected by the overload, so the
	 * connection remains unguarded when {@code connectionType} is {@code Connection.class}. Do not close the connection,
	 * retain it, call
	 * {@link Connection#commit()}, {@link Connection#rollback()}, or {@link Connection#setAutoCommit(boolean)}, or mutate
	 * connection-wide state such as schema, catalog, client info, holdability, type map, or network timeout.
	 * <p>
	 * The connection and every object obtained from it are valid only for the duration of the callback. Statements, result
	 * sets, appenders, readers, and other derived resources must be closed before this method returns. Retaining or using any
	 * of these objects after the callback returns has undefined behavior.
	 *
	 * @param connectionType         driver connection class or interface to expose
	 * @param rawConnectionOperation the driver-specific raw JDBC operation to perform
	 * @param <C>                    the requested connection type
	 * @param <T>                    the type to be returned
	 * @return the operation result
	 * @throws DatabaseException if connection acquisition, unwrapping, callback execution, or cleanup fails
	 * @since 4.7.0
	 */
	@NonNull
	public <C extends Connection, T> Optional<T> useRawConnection(@NonNull Class<C> connectionType,
																											@NonNull RawConnectionOperation<? super C, T> rawConnectionOperation) {
		requireNonNull(connectionType);
		requireNonNull(rawConnectionOperation);

		return performRawConnectionOperation(connection -> {
			C rawConnection = connectionType.isInstance(connection)
					? connectionType.cast(connection)
					: connection.unwrap(connectionType);

			if (rawConnection == null)
				throw new SQLException(format("Connection.unwrap(%s) returned null", connectionType.getName()));

			Optional<T> result = rawConnectionOperation.perform(rawConnection);
			return result == null ? Optional.empty() : result;
		}, true);
	}

	/**
	 * Performs an operation with one callback-scoped database-notification listener session.
	 * <p>
	 * This method is synchronous and blocking. It acquires at most one listener connection from this
	 * {@code Database}'s configured {@link DataSource}, registers every requested channel, invokes {@code operation}
	 * at most once, expires the supplied {@link NotificationSession}, and completes cleanup before returning or
	 * throwing. It never reconnects.
	 * <p>
	 * The configured source must preserve one physical backend session for the entire checkout. For PostgreSQL,
	 * direct connections and session pooling are suitable. Using PgBouncer transaction or statement pooling as the
	 * listener source is unsupported: registration can appear to succeed before backend-session affinity is lost and
	 * notification delivery silently stops. Pyranid does not inspect or validate proxy topology. Applications whose
	 * ordinary source cannot provide the required affinity should construct a separate {@code Database} over a suitable
	 * listener source and invoke this method on that instance.
	 * <p>
	 * Notifications are lossy hints. Durable applications should normally reconcile authoritative state as the first
	 * callback action. The operation may use ordinary database methods, but those methods acquire or select their
	 * connection normally and never reuse the listener connection. Pyranid transaction entry is interrupt-sensitive;
	 * when a receive returns a batch with a racing interrupt, temporarily clear and remember the flag during bounded
	 * reconciliation and restore it afterward as described by {@link NotificationSession#awaitNotifications(Duration)}.
	 * <p>
	 * A terminal receive failure is retained by the session and rethrown after cleanup even if {@code operation} catches
	 * it and returns. A retained transport {@link Error} propagates as that exact, unwrapped instance. If the callback
	 * instead throws a distinct {@code Error}, that callback error remains primary and the retained transport failure is
	 * suppressed beneath it.
	 *
	 * @param channels  fixed, nonempty set of nonblank channels to register
	 * @param operation operation to invoke after every channel has been registered
	 * @throws NullPointerException if {@code channels}, a channel, or {@code operation} is null
	 * @throws IllegalArgumentException if the set is empty or a channel violates common or backend-specific limits
	 * @throws IllegalStateException if any Pyranid transaction is active on the calling thread
	 * @throws InterruptedException if cooperative interruption wins after any required cleanup
	 * @throws UnsupportedOperationException if notification listening is unavailable through the database dialect or
	 * runtime driver, or is unsupported or disabled for the current server version or configuration
	 * @throws DatabaseException if connection acquisition, setup, receive, callback execution, or cleanup fails
	 * @since 4.6.0
	 */
	public void withNotificationSession(@NonNull Set<@NonNull String> channels,
																			@NonNull NotificationSessionOperation operation)
			throws InterruptedException {
		NotificationSessionRunner.run(this, channels, operation);
	}

	/**
	 * Performs an operation with one callback-scoped database-notification listener session for a single channel.
	 * <p>
	 * This is the single-channel convenience form of
	 * {@link #withNotificationSession(Set, NotificationSessionOperation)}. The listener connection comes from this
	 * {@code Database}'s configured {@link DataSource}, which must preserve physical backend-session affinity for the
	 * entire checkout. For PostgreSQL, using PgBouncer transaction or statement pooling as the listener source is
	 * unsupported: registration can appear to succeed before affinity is lost and delivery silently stops. Pyranid does
	 * not inspect or validate proxy topology.
	 * <p>
	 * A terminal receive failure is retained and rethrown after cleanup even if {@code operation} catches it and returns.
	 * A retained transport {@link Error} propagates as the exact, unwrapped instance unless a distinct callback
	 * {@code Error} takes precedence as described by the set-based overload.
	 *
	 * @param channel   nonblank channel to register
	 * @param operation operation to invoke after the channel has been registered
	 * @throws NullPointerException if {@code channel} or {@code operation} is null
	 * @throws IllegalArgumentException if the channel violates common or backend-specific limits
	 * @throws IllegalStateException if any Pyranid transaction is active on the calling thread
	 * @throws InterruptedException if cooperative interruption wins after any required cleanup
	 * @throws UnsupportedOperationException if notification listening is unavailable through the database dialect or
	 * runtime driver, or is unsupported or disabled for the current server version or configuration
	 * @throws DatabaseException if connection acquisition, setup, receive, callback execution, or cleanup fails
	 * @since 4.6.0
	 */
	public void withNotificationSession(@NonNull String channel,
																			@NonNull NotificationSessionOperation operation)
			throws InterruptedException {
		requireNonNull(channel);
		requireNonNull(operation);
		withNotificationSession(Set.of(channel), operation);
	}

	/**
	 * Sends a transient database notification without specifying a payload.
	 * <p>
	 * Sending follows ordinary Pyranid statement and transaction selection. On PostgreSQL, a send inside a Pyranid
	 * transaction becomes visible only if that transaction commits. Payload representation is database-specific;
	 * PostgreSQL converts the resulting null payload to the empty string.
	 *
	 * @param channel nonblank notification channel
	 * @throws NullPointerException if {@code channel} is null
	 * @throws IllegalArgumentException if the channel violates common or backend-specific limits
	 * @throws UnsupportedOperationException if notification sending is unavailable through the database dialect, or is
	 * unsupported or disabled for the current server version or configuration
	 * @throws DatabaseException if the send fails
	 * @since 4.6.0
	 */
	public void sendNotification(@NonNull String channel) {
		sendNotification(channel, null);
	}

	/**
	 * Sends a transient database notification.
	 * <p>
	 * Sending follows ordinary Pyranid statement and transaction selection, including connection ownership,
	 * parameter binding and redaction, statement logging, timeout configuration, and metrics. Payload nullability
	 * and null/empty-string handling are database-specific; Pyranid performs no generic normalization.
	 * <p>
	 * On PostgreSQL this executes bound {@code pg_notify(?, ?)} SQL. PostgreSQL converts a null payload to the empty
	 * string. Delivery occurs only after commit and is discarded by rollback; notification delivery itself remains
	 * non-durable and may be coalesced.
	 *
	 * @param channel nonblank notification channel
	 * @param payload notification payload, possibly null or empty
	 * @throws NullPointerException if {@code channel} is null
	 * @throws IllegalArgumentException if the channel or payload violates common or backend-specific limits
	 * @throws UnsupportedOperationException if notification sending is unavailable through the database dialect, or is
	 * unsupported or disabled for the current server version or configuration
	 * @throws DatabaseException if the send fails
	 * @since 4.6.0
	 */
	public void sendNotification(@NonNull String channel,
												 @Nullable String payload) {
		Notification.validateChannel(channel);
		DatabaseNotificationSupport notificationSupport = getDatabaseDialect().notificationSupport();

		notificationSupport.validateChannel(channel);
		notificationSupport.validatePayload(payload);

		if (!notificationSupport.isSendSupported())
			throw new UnsupportedOperationException(format(
					"Database type %s does not support notification sends", getDatabaseType()));

		Statement statement = Statement.of(generateId(), notificationSupport.sendStatementSql());
		StatementContext<Void> statementContext = StatementContext.with(statement, this)
				.parameters(channel, payload)
				.build();

		PreparedStatementCustomizer defaultPreparedStatementCustomizer = hasDefaultPreparedStatementSettings()
				? (context, preparedStatement) -> applyDefaultPreparedStatementSettings(preparedStatement)
				: null;

		// Scoped deliberately to PreparedStatement.execute(). Connection acquisition, result draining and
		// JDBC cleanup can all raise these same SQLStates for unrelated reasons.
		AtomicReference<SQLException> sendStatementFailure = new AtomicReference<>();

		try {
			this.statementExecutor.performDatabaseOperation(statementContext, Arrays.asList(channel, payload),
					defaultPreparedStatementCustomizer, (preparedStatement, timing) -> {
				long startTime = nanoTime();

				boolean resultAvailable;

				try {
					resultAvailable = preparedStatement.execute();
				} catch (SQLException e) {
					if (isNotificationStatementUnsupported(e))
						sendStatementFailure.set(e);

					throw e;
				}

				drainStatementResults(preparedStatement, resultAvailable);

				return new StatementExecutor.DatabaseOperationResult(
						Duration.ofNanos(nanoTime() - startTime), null, StatementResult.empty());
			});
		} catch (DatabaseException e) {
			// The send statement is Pyranid's own, so the server rejecting it as unsupported syntax or an
			// unavailable function is a server-capability issue rather than malformed caller SQL.
			if (sendStatementFailure.get() != null)
				throw new UnsupportedOperationException(
						notificationServerCapabilityUnavailableMessage(getDatabaseType()), e);

			throw e;
		}
	}

	/**
	 * Reports whether the configured database dialect and currently loadable runtime adapter expose the APIs required
	 * to attempt a notification-listening session.
	 * <p>
	 * This method resolves the full database type. If it has not been configured or cached, resolution may acquire a
	 * metadata connection and may throw {@link DatabaseException}. It does not verify whether notifications are enabled
	 * or supported by the current server version or configuration, acquire a listener session, inspect pool or proxy mode,
	 * prove backend-session affinity, unwrap a physical listener connection, or emit notification-session lifecycle
	 * metrics.
	 *
	 * @return true if notification listening can be attempted with the current dialect and runtime
	 * @throws DatabaseException if automatic database-type detection fails
	 * @since 4.6.0
	 */
	@NonNull
	public Boolean isNotificationListeningSupported() {
		return getDatabaseDialect().notificationSupport().isReceiveRuntimeAvailable();
	}

	static void drainStatementResults(@NonNull PreparedStatement preparedStatement,
			boolean resultAvailable) throws SQLException {
		requireNonNull(preparedStatement);

		for (;;) {
			if (resultAvailable) {
				ResultSet resultSet = preparedStatement.getResultSet();
				Throwable resultSetFailure = null;
				try {
					if (resultSet != null) {
						while (resultSet.next()) {
							// Drain every row before advancing to a possible subsequent result.
						}
					}
				} catch (SQLException | RuntimeException | Error failure) {
					resultSetFailure = failure;
					throw failure;
				} finally {
					JdbcResources.close(resultSet, resultSetFailure);
				}
			} else if (preparedStatement.getUpdateCount() == -1) {
				break;
			}

			resultAvailable = preparedStatement.getMoreResults(java.sql.Statement.CLOSE_CURRENT_RESULT);
		}
	}

	static boolean isNotificationStatementUnsupported(@Nullable Throwable throwable) {
		return NotificationSessionRunner.isNotificationStatementUnsupported(throwable);
	}

	@NonNull
	private String notificationServerCapabilityUnavailableMessage(@NonNull DatabaseType databaseType) {
		return NotificationSessionRunner.notificationServerCapabilityUnavailableMessage(databaseType);
	}

	boolean hasDefaultPreparedStatementSettings() {
		return this.queryTimeout != null || this.fetchSize != null || this.maxRows != null;
	}

	void applyDefaultPreparedStatementSettings(@NonNull PreparedStatement preparedStatement) throws SQLException {
		requireNonNull(preparedStatement);
		applyPreparedStatementSettings(preparedStatement, this.queryTimeout, this.fetchSize, this.maxRows);
	}

	static void applyPreparedStatementSettings(@NonNull PreparedStatement preparedStatement,
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
	interface InternalRawConnectionOperation<R> {
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

	@NonNull
	DatabaseDialect getDatabaseDialect() {
		return getDatabaseDialect(this.databaseTypeDetectionConnectionHolder.get());
	}

	@NonNull
	DatabaseDialect getDatabaseDialect(@Nullable Connection connection) {
		DatabaseDialect cachedDatabaseDialect = this.databaseDialect.get();

		if (cachedDatabaseDialect != null)
			return cachedDatabaseDialect;

		DatabaseDialect detectedDatabaseDialect = getDatabaseType(connection).dialect();

		if (this.databaseDialect.compareAndSet(null, detectedDatabaseDialect))
			return detectedDatabaseDialect;

		return requireNonNull(this.databaseDialect.get());
	}

	@NonNull
	DatabaseType getDatabaseType(@Nullable Connection connection) {
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
	 * Gets the configured redactor used for non-secure parameters in diagnostics.
	 *
	 * @return the configured parameter redactor
	 * @since 4.4.0
	 */
	@NonNull
	public ParameterRedactor getParameterRedactor() {
		return this.parameterRedactor;
	}

	/**
	 * Useful for single-shot "utility" calls that operate outside of normal query operations, e.g. pulling DB metadata.
	 * <p>
	 * Example: {@link #readDatabaseMetaData(DatabaseMetaDataReader)}.
	 */
	@NonNull
	<R> Optional<R> performRawConnectionOperation(@NonNull InternalRawConnectionOperation<R> rawConnectionOperation,
																													@NonNull Boolean shouldParticipateInExistingTransactionIfPossible) {
		requireNonNull(rawConnectionOperation);
		requireNonNull(shouldParticipateInExistingTransactionIfPossible);

		if (shouldParticipateInExistingTransactionIfPossible) {
			Optional<Transaction> transaction = currentTransactionForDatabaseOperation();
			ReentrantLock connectionLock = transaction.isPresent() ? transaction.get().getConnectionLock() : null;
			// Try to participate in txn if it's available
			Connection connection = null;
			Throwable thrown = null;
			boolean connectionLockAcquired = false;

			try {
				if (connectionLock != null) {
					lockInterruptibly(connectionLock, "use the transaction connection for a raw connection operation");
					connectionLockAcquired = true;
				}

				if (transaction.isPresent()) {
					connection = transaction.get().getConnection();
				} else {
					try {
						connection = getDataSource().getConnection();
					} catch (SQLException e) {
						throw new DatabaseException("Unable to acquire database connection", e);
					}
				}

				return rawConnectionOperation.perform(connection);
			} catch (DatabaseException e) {
				thrown = e;
				throw e;
			} catch (Error e) {
				thrown = e;
				throw e;
			} catch (Exception e) {
				DatabaseException wrapped = databaseExceptionWithRawConnectionContext(connection, e);
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
					if (connectionLockAcquired)
						connectionLock.unlock();

					if (cleanupFailure != null) {
						if (thrown != null) {
							appendCleanupFailure(thrown, cleanupFailure);
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
			} catch (Error e) {
				thrown = e;
				throw e;
			} catch (Exception e) {
				DatabaseException wrapped = acquiredConnection
						? databaseExceptionWithRawConnectionContext(connection, e)
						: new DatabaseException("Unable to acquire database connection", e);
				thrown = wrapped;
				throw wrapped;
			} finally {
				if (connection != null) {
					try {
						closeConnection(connection);
					} catch (Throwable cleanupException) {
						if (thrown != null) {
							appendCleanupFailure(thrown, cleanupException);
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

	@NonNull
	DataSource getDataSource() {
		return this.dataSource;
	}

	@NonNull
	InstanceProvider getInstanceProvider() {
		return this.instanceProvider;
	}

	@NonNull
	ResultSetMapper getResultSetMapper() {
		return this.resultSetMapper;
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
	Object generateId() {
		// "Unique" keys
		return this.defaultIdGenerator.incrementAndGet();
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
		private ParameterRedactor parameterRedactor;
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
		 * Configures the redactor used for non-secure parameters in diagnostics.
		 * <p>
		 * {@link SecureParameter} values always render via {@link SecureParameter#getMask()} and are never passed to this
		 * redactor. Batch executions render a bounded batch summary instead of invoking the redactor for each batch value.
		 * Specify {@code null} or omit this setter to render non-secure, non-batch values verbatim.
		 *
		 * @param parameterRedactor parameter redactor to use, or {@code null} for the default
		 * @return this {@code Builder}, for chaining
		 * @since 4.4.0
		 */
		@NonNull
		public Builder parameterRedactor(@Nullable ParameterRedactor parameterRedactor) {
			this.parameterRedactor = parameterRedactor;
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
		 * and {@link Query#customize(PreparedStatementCustomizer)} can override both settings before parameter binding.
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
		 * settings override this value, and {@link Query#customize(PreparedStatementCustomizer)} can override both
		 * settings before parameter binding.
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
		 * this value, and {@link Query#customize(PreparedStatementCustomizer)} can override both settings before parameter
		 * binding.
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

}
