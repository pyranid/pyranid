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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;

import static com.pyranid.Database.*;

@NotThreadSafe
final class StreamCursor<T> implements java.util.Iterator<T>, AutoCloseable {
	private final StatementExecutor executor;
	private final StatementContext<T> statementContext;
	private final List<Object> parameters;
	@Nullable
	private final PreparedStatementCustomizer preparedStatementCustomizer;
	private final StatementExecution lease;
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
	@NonNull
	private DatabaseDialect databaseStreamDialect = GenericDialect.INSTANCE;
	@NonNull
	private DatabaseStreamState databaseStreamState = DatabaseStreamState.none();
	private final boolean queryFetchSizeConfigured;

	StreamCursor(@NonNull StatementExecutor executor,
														 @NonNull StatementContext<T> statementContext,
														 @NonNull List<Object> parameters,
														 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
														 boolean queryFetchSizeConfigured) {
		this.executor = requireNonNull(executor);
		this.statementContext = requireNonNull(statementContext);
		this.parameters = requireNonNull(parameters);
		this.preparedStatementCustomizer = preparedStatementCustomizer;
		this.lease = new StatementExecution(statementContext, executor.connections, executor.getMetricsCollectorDispatcher());
		this.queryFetchSizeConfigured = queryFetchSizeConfigured;

		open();
	}

	private void open() {
		long startTime = nanoTime();
		this.openStartTime = startTime;
		this.executor.getMetricsCollectorDispatcher().willExecuteStatement(this.statementContext);
		this.executor.getMetricsCollectorDispatcher().willOpenStream(this.statementContext);
		try (var ignored = JdbcCallbackContext.activate(this.lease)) {
			this.connection = this.lease.acquire("open a stream on the transaction connection");
			startTime = nanoTime();

			ParameterBindingContext.Scope bindingScope = null;
			try {
				try {
					this.databaseStreamDialect = this.executor.connections.dialect();
					this.databaseStreamState = this.databaseStreamDialect.configureStreamingConnection(requireNonNull(this.connection), this.lease.transactional());

					this.preparedStatement = this.databaseStreamDialect.prepareStreamingStatement(requireNonNull(this.connection), this.statementContext);
					bindingScope = ParameterBindingContext.activate(this.statementContext, this.preparedStatement,
							new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(this.statementContext, this.preparedStatement));
					this.executor.applyPreparedStatementCustomizer(this.statementContext, this.preparedStatement, this.preparedStatementCustomizer);
					this.databaseStreamDialect.configureStreamingPreparedStatement(this.preparedStatement, this.databaseStreamState,
							this.lease.transactional(), this.queryFetchSizeConfigured);
					if (this.parameters.size() > 0)
						this.executor.performPreparedStatementBinding(this.statementContext, this.preparedStatement, this.parameters);
				} finally {
					this.preparationDuration = Duration.ofNanos(nanoTime() - startTime);
				}

				startTime = nanoTime();
				try {
					this.resultSet = this.preparedStatement.executeQuery();
				} finally {
					this.executionDuration = Duration.ofNanos(nanoTime() - startTime);
				}
			} finally {
				if (bindingScope != null) bindingScope.close();
			}
			this.executor.getMetricsCollectorDispatcher().didOpenStream(this.statementContext, Duration.ofNanos(nanoTime() - this.openStartTime));
		} catch (DatabaseException e) {
			DatabaseException wrapped = databaseExceptionWithStatementContext(this.statementContext, e);
			this.exception = wrapped;
			this.thrown = wrapped;
			this.lease.recordFailure(wrapped);
			this.openFailed = true;
			this.executor.getMetricsCollectorDispatcher().didFailToOpenStream(this.statementContext, this.executor.connections.databaseType(),
					Duration.ofNanos(nanoTime() - this.openStartTime), wrapped);
			close();
			throw wrapped;
		} catch (Exception e) {
			DatabaseException wrapped = databaseExceptionWithStatementContext(this.statementContext, e);
			this.exception = wrapped;
			this.thrown = wrapped;
			this.lease.recordFailure(wrapped);
			this.openFailed = true;
			this.executor.getMetricsCollectorDispatcher().didFailToOpenStream(this.statementContext, this.executor.connections.databaseType(),
					Duration.ofNanos(nanoTime() - this.openStartTime), wrapped);
			close();
			throw wrapped;
		} catch (Error e) {
			this.exception = databaseExceptionWithStatementContext(this.statementContext, e);
			this.thrown = e;
			this.lease.recordFailure(e);
			this.openFailed = true;
			this.executor.getMetricsCollectorDispatcher().didFailToOpenStream(this.statementContext, this.executor.connections.databaseType(),
					Duration.ofNanos(nanoTime() - this.openStartTime), e);
			close();
			throw e;
		}
	}

	@Override
	public boolean hasNext() {
		if (this.closed)
			return false;

		if (!this.hasNextEvaluated) {
			try {
				long startTime = nanoTime();
				try {
					this.hasNext = this.resultSet != null && this.resultSet.next();
				} finally {
					this.resultSetMappingNanos += nanoTime() - startTime;
				}
				this.hasNextEvaluated = true;
				if (!this.hasNext) {
					this.exhausted = true;
					close();
				}
			} catch (SQLException e) {
				DatabaseException wrapped = databaseExceptionWithStatementContext(this.statementContext, e);
				this.exception = wrapped;
				this.thrown = wrapped;
				this.lease.recordFailure(wrapped);
				this.iterationThrowable = wrapped;
				close();
				throw wrapped;
			} catch (RuntimeException e) {
				this.exception = e;
				this.thrown = e;
				this.lease.recordFailure(e);
				this.iterationThrowable = e;
				close();
				throw e;
			} catch (Error e) {
				this.exception = databaseExceptionWithStatementContext(this.statementContext, e);
				this.thrown = e;
				this.lease.recordFailure(e);
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

		try (var ignored = JdbcCallbackContext.activate(this.lease)) {
			T value;
			try {
				value = this.executor.mapResultSetRow(this.statementContext, requireNonNull(this.resultSet))
						.orElse(null);
			} finally {
				this.resultSetMappingNanos += nanoTime() - startTime;
			}
			this.rowsConsumed++;
			return value;
		} catch (SQLException e) {
			DatabaseException wrapped = databaseExceptionWithStatementContext(this.statementContext,
					format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), this.statementContext.getResultSetRowType().get()), e);
			this.exception = wrapped;
			this.thrown = wrapped;
			this.lease.recordFailure(wrapped);
			this.iterationThrowable = wrapped;
			close();
			throw wrapped;
		} catch (DatabaseException e) {
			DatabaseException wrapped = databaseExceptionWithStatementContext(this.statementContext, e);
			this.exception = wrapped;
			this.thrown = wrapped;
			this.lease.recordFailure(wrapped);
			this.iterationThrowable = wrapped;
			close();
			throw wrapped;
		} catch (RuntimeException e) {
			this.exception = e;
			this.thrown = e;
			this.lease.recordFailure(e);
			this.iterationThrowable = e;
			close();
			throw e;
		} catch (Error e) {
			this.exception = databaseExceptionWithStatementContext(this.statementContext,
					format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), this.statementContext.getResultSetRowType().get()), e);
			this.thrown = e;
			this.lease.recordFailure(e);
			this.iterationThrowable = e;
			close();
			throw e;
		}
	}

	void callbackFailed(@NonNull Throwable throwable) {
		requireNonNull(throwable);
		this.callbackThrowable = throwable;
		this.lease.recordFailure(throwable);
	}

	@Override
	public void close() {
		if (this.closed)
			return;

		this.lease.assertCloseThread();

		this.closed = true;
		Throwable cleanupFailure = null;

		try {
			if (this.resultSet != null) {
				try {
					this.resultSet.close();
				} catch (Throwable cleanupException) {
					cleanupFailure = appendCleanupFailure(cleanupFailure, cleanupException);
				}
			}

			if (this.preparedStatement != null) {
				try {
					this.preparedStatement.close();
				} catch (Throwable cleanupException) {
					cleanupFailure = appendCleanupFailure(cleanupFailure, cleanupException);
				}
			}

			// After the cursor and statement, never before: a streamed execution is still consuming the values it
			// was given as parameters while it delivers rows.
			cleanupFailure = closeStatementContextResources(this.statementContext, cleanupFailure);

			cleanupFailure = completeDialectStreamingConnectionIfNeeded(cleanupFailure);

		} finally {
			Throwable primary = this.thrown != null ? this.thrown : this.callbackThrowable;
			cleanupFailure = normalizeCleanupFailure(cleanupFailure, primary);
			cleanupFailure = this.lease.close(cleanupFailure);
			cleanupFailure = normalizeCleanupFailure(cleanupFailure, primary);

			Throwable statementFailure = this.thrown != null ? this.thrown : this.exception;
			if (statementFailure == null && primary == null) statementFailure = cleanupFailure;
			if (this.exception == null && statementFailure != null)
				this.exception = statementFailure instanceof Exception failure ? failure
						: databaseExceptionWithStatementContext(this.statementContext, statementFailure);

			Duration mappingDuration = this.resultSetMappingNanos == 0L ? null : Duration.ofNanos(this.resultSetMappingNanos);

			StatementLog statementLog =
					StatementLog.withStatementContext(this.statementContext)
							.connectionAcquisitionDuration(this.lease.acquisitionDuration())
							.elapsedDuration(this.lease.elapsedDuration())
							.preparationDuration(this.preparationDuration)
							.executionDuration(this.executionDuration)
							.resultSetMappingDuration(mappingDuration)
							.exception(this.exception)
							.build();

			if (statementFailure == null) {
				this.executor.getMetricsCollectorDispatcher().didExecuteStatement(this.statementContext, statementLog, StatementResult.empty());
			} else {
				this.executor.getMetricsCollectorDispatcher().didFailToExecuteStatement(this.statementContext, statementLog,
						this.executor.connections.databaseType(), statementFailure);
			}

			try {
				this.executor.getStatementLogger().log(statementLog);
			} catch (Throwable cleanupException) {
				cleanupFailure = appendCleanupFailure(cleanupFailure, cleanupException);
			}
		}

		cleanupFailure = normalizeCleanupFailure(cleanupFailure, this.thrown != null ? this.thrown : this.callbackThrowable);
		this.cleanupFailure = cleanupFailure;

		if (cleanupFailure != null) {
			Throwable primary = this.thrown != null ? this.thrown : this.callbackThrowable;
			if (primary != null) {
				appendCleanupFailure(primary, cleanupFailure);
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
	private Throwable normalizeCleanupFailure(@Nullable Throwable failure, @Nullable Throwable primary) {
		if (failure == null || primary != null || failure instanceof Error) return failure;
		return databaseExceptionWithStatementContext(this.statementContext, failure);
	}

	@Nullable
	private Throwable completeDialectStreamingConnectionIfNeeded(@Nullable Throwable cleanupFailure) {
		if (this.connection == null)
			return cleanupFailure;

		boolean streamSucceeded = this.thrown == null && this.exception == null && this.callbackThrowable == null && !this.openFailed;
		cleanupFailure = this.databaseStreamDialect.completeStreamingConnection(requireNonNull(this.connection),
				this.databaseStreamState, streamSucceeded, cleanupFailure);

		this.databaseStreamDialect = GenericDialect.INSTANCE;
		this.databaseStreamState = DatabaseStreamState.none();
		return cleanupFailure;
	}

	void emitTerminalMetrics() {
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

		this.executor.getMetricsCollectorDispatcher().didCloseStream(this.statementContext, outcome, this.rowsConsumed,
				Duration.ofNanos(nanoTime() - this.openStartTime), throwable);
	}

}
