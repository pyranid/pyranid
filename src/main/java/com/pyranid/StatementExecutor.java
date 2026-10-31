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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;

import static com.pyranid.Database.*;

/** Executes prepared statements; immutable services are shared, operation state is per invocation. */
final class StatementExecutor {
	private final Database database;
	final StatementConnections connections;
	private final InstanceProvider instanceProvider;
	private final ResultSetMapper resultSetMapper;
	private final PreparedStatementBinder preparedStatementBinder;
	private final StatementLogger statementLogger;
	private final MetricsCollectorDispatcher metricsCollectorDispatcher;
	private volatile DatabaseOperationSupportStatus executeLargeBatchSupported = DatabaseOperationSupportStatus.UNKNOWN;
	private volatile DatabaseOperationSupportStatus executeLargeUpdateSupported = DatabaseOperationSupportStatus.UNKNOWN;

	StatementExecutor(Database database, InstanceProvider instanceProvider, ResultSetMapper resultSetMapper,
			PreparedStatementBinder preparedStatementBinder, StatementLogger statementLogger, MetricsCollectorDispatcher metricsCollectorDispatcher, StatementConnections connections) {
		this.database = database;
		this.connections = connections;
		this.instanceProvider = instanceProvider;
		this.resultSetMapper = resultSetMapper;
		this.preparedStatementBinder = preparedStatementBinder;
		this.statementLogger = statementLogger;
		this.metricsCollectorDispatcher = metricsCollectorDispatcher;
	}
	InstanceProvider getInstanceProvider() { return this.instanceProvider; }
	ResultSetMapper getResultSetMapper() { return this.resultSetMapper; }
	PreparedStatementBinder getPreparedStatementBinder() { return this.preparedStatementBinder; }
	StatementLogger getStatementLogger() { return this.statementLogger; }
	MetricsCollectorDispatcher getMetricsCollectorDispatcher() { return this.metricsCollectorDispatcher; }

	void warmDatabaseTypeCacheForMetricsIfNeeded(StatementContext<?> context) {
		if (!this.metricsCollectorDispatcher.isEnabled()) return;
		try {
			context.getDatabaseType();
		} catch (Throwable failure) {
			if (JdbcFallbacks.isTerminal(failure)) {
				if (failure instanceof Error error) throw error;
				if (failure instanceof RuntimeException runtime) throw runtime;
				throw DatabaseException.wrapSafely("Unable to determine database type for metrics", failure,
						DatabaseDialect.forExceptionCause(failure));
			}
			Logger.getLogger(Database.class.getName()).log(Level.FINE, "Unable to warm database type cache for metrics", failure);
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
	@NonNull
	<T> Optional<T> queryForObject(@NonNull String sql,
																				 @NonNull Class<T> resultSetRowType,
																				 Object @Nullable ... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return queryForObject(Statement.of(this.database.generateId(), sql), resultSetRowType, parameters);
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
	<T> Optional<T> queryForObject(@NonNull Statement statement,
																				 @NonNull Class<T> resultSetRowType,
																				 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		return queryForObject(statement, resultSetRowType, null, null, parameters);
	}

	<T> Optional<T> queryForObject(@NonNull Statement statement,
																				 @NonNull Class<T> resultSetRowType,
																				 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																				 StatementContext.@Nullable SpiOverrides spiOverrides,
																				 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		ResultHolder<Optional<T>> resultHolder = new ResultHolder<>();
		StatementContext<T> statementContext = StatementContext.<T>with(statement, this.database)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.spiOverrides(spiOverrides)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);

		performDatabaseOperation(statementContext, parametersAsList, preparedStatementCustomizer, (PreparedStatement preparedStatement, DatabaseOperationTiming timing) -> {
			long startTime = nanoTime();

			ResultSet resultSet = preparedStatement.executeQuery();
			Throwable resultSetFailure = null;
			try {
				timing.beginMapping();
				Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
				startTime = nanoTime();

				Optional<T> result = Optional.empty();
				long rowsReturned = 0L;

				if (resultSet.next()) {
					rowsReturned = 1L;
					try {
						T value = resultSetMapperFor(statementContext).map(statementContext, resultSet, statementContext.getResultSetRowType().get(), getInstanceProvider()).orElse(null);
						result = Optional.ofNullable(value);
					} catch (SQLException e) {
						throw databaseExceptionWithStatementContext(statementContext,
								format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), statementContext.getResultSetRowType().get()), e);
					}

					if (resultSet.next())
						throw databaseExceptionWithStatementContext(statementContext,
								"Expected 1 row in resultset but got more than 1 instead",
								new IllegalStateException("Expected 1 row in resultset but got more than 1 instead"));
				}

				resultHolder.value = result;
				Duration resultSetMappingDuration = Duration.ofNanos(nanoTime() - startTime);
				StatementResult statementResult = getMetricsCollectorDispatcher().isEnabled()
						? StatementResult.ofRowsReturned(rowsReturned)
						: StatementResult.empty();
				return new DatabaseOperationResult(executionDuration, resultSetMappingDuration, statementResult);
			} catch (Exception | Error failure) {
				resultSetFailure = failure;
				throw failure;
			} finally {
				JdbcResources.close(resultSet, resultSetFailure);
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
	<T> List<@Nullable T> queryForList(@NonNull String sql,
																						 @NonNull Class<T> resultSetRowType,
																						 Object @Nullable ... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return queryForList(Statement.of(this.database.generateId(), sql), resultSetRowType, parameters);
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
	<T> List<@Nullable T> queryForList(@NonNull Statement statement,
																						 @NonNull Class<T> resultSetRowType,
																						 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		return queryForList(statement, resultSetRowType, null, null, parameters);
	}

	<T> List<@Nullable T> queryForList(@NonNull Statement statement,
																						 @NonNull Class<T> resultSetRowType,
																						 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																						 StatementContext.@Nullable SpiOverrides spiOverrides,
																						 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		List<T> list = new ArrayList<>();
		StatementContext<T> statementContext = StatementContext.<T>with(statement, this.database)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.spiOverrides(spiOverrides)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);

		performDatabaseOperation(statementContext, parametersAsList, preparedStatementCustomizer, (PreparedStatement preparedStatement, DatabaseOperationTiming timing) -> {
			long startTime = nanoTime();

			ResultSet resultSet = preparedStatement.executeQuery();
			Throwable resultSetFailure = null;
			try {
				timing.beginMapping();
				Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
				startTime = nanoTime();

				while (resultSet.next()) {
					try {
						T listElement = resultSetMapperFor(statementContext).map(statementContext, resultSet, statementContext.getResultSetRowType().get(), getInstanceProvider()).orElse(null);
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
			} catch (Exception | Error failure) {
				resultSetFailure = failure;
				throw failure;
			} finally {
				JdbcResources.close(resultSet, resultSetFailure);
			}
		});

		return list;
	}

	@Nullable
	<T, R> R queryForStream(@NonNull Statement statement,
																	@NonNull Class<T> resultSetRowType,
																	@Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																	boolean queryFetchSizeConfigured,
																	@NonNull Function<Stream<@Nullable T>, R> streamFunction,
																	StatementContext.@Nullable SpiOverrides spiOverrides,
																	Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);
		requireNonNull(streamFunction);

		StatementContext<T> statementContext = StatementContext.<T>with(statement, this.database)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.spiOverrides(spiOverrides)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);
		StreamCursor<T> iterator = new StreamCursor<>(this, statementContext, parametersAsList,
				preparedStatementCustomizer, queryFetchSizeConfigured);

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
	Long execute(@NonNull String sql,
											 Object @Nullable ... parameters) {
		requireNonNull(sql);
		return execute(Statement.of(this.database.generateId(), sql), parameters);
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
	Long execute(@NonNull Statement statement,
											 Object @Nullable ... parameters) {
		requireNonNull(statement);

		return execute(statement, null, null, parameters);
	}

	Long execute(@NonNull Statement statement,
											 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
											 StatementContext.@Nullable SpiOverrides spiOverrides,
											 Object @Nullable ... parameters) {
		requireNonNull(statement);

		ResultHolder<Long> resultHolder = new ResultHolder<>();
		StatementContext<Void> statementContext = StatementContext.with(statement, this.database)
				.parameters(parameters)
				.spiOverrides(spiOverrides)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);

		performDatabaseOperation(statementContext, parametersAsList, preparedStatementCustomizer, (PreparedStatement preparedStatement, DatabaseOperationTiming timing) -> {
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
			if (JdbcFallbacks.isTerminal(e))
				throw e;
			setExecuteLargeUpdateSupported(DatabaseOperationSupportStatus.NO);
			return (long) preparedStatement.executeUpdate();
		} catch (SQLException e) {
			if (JdbcFallbacks.isTerminal(e))
				throw e;
			if (isUnsupportedSqlFeature(e)) {
				setExecuteLargeUpdateSupported(DatabaseOperationSupportStatus.NO);
				return (long) preparedStatement.executeUpdate();
			}

			throw e;
		}
	}

	@NonNull
	<T> Optional<T> executeReturningGeneratedKey(@NonNull Statement statement,
																											 @NonNull Class<T> resultSetRowType,
																											 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																											 @Nullable String @Nullable [] keyColumnNames,
																											 StatementContext.@Nullable SpiOverrides spiOverrides,
																											 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		ResultHolder<Optional<T>> resultHolder = new ResultHolder<>();
		StatementContext<T> statementContext = StatementContext.<T>with(statement, this.database)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.spiOverrides(spiOverrides)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);
		String[] requestedKeyColumnNames = copyGeneratedKeyColumnNames(keyColumnNames);

		performDatabaseOperation(statementContext, parametersAsList, preparedStatementCustomizer, (PreparedStatement preparedStatement, DatabaseOperationTiming timing) -> {
			long startTime = nanoTime();
			Long rowsAffected = executeUpdate(preparedStatement);

			ResultSet resultSet = preparedStatement.getGeneratedKeys();
			Throwable resultSetFailure = null;
			try {
				timing.beginMapping();
				Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
				startTime = nanoTime();

				Optional<T> result = Optional.empty();
				long rowsReturned = 0L;

				if (resultSet.next()) {
					rowsReturned = 1L;
					try {
						T value = resultSetMapperFor(statementContext).map(statementContext, resultSet, statementContext.getResultSetRowType().get(), getInstanceProvider()).orElse(null);
						result = Optional.ofNullable(value);
					} catch (SQLException e) {
						throw databaseExceptionWithStatementContext(statementContext,
								format("Unable to map JDBC generated-key row to %s", statementContext.getResultSetRowType().get()), e);
					}

					if (resultSet.next())
						throw databaseExceptionWithStatementContext(statementContext,
								"Expected 1 generated-key row but got more than 1 instead",
								new IllegalStateException("Expected 1 generated-key row but got more than 1 instead"));
				}

				resultHolder.value = result;
				Duration resultSetMappingDuration = Duration.ofNanos(nanoTime() - startTime);
				StatementResult statementResult = getMetricsCollectorDispatcher().isEnabled()
						? new StatementResult(rowsReturned, rowsAffected)
						: StatementResult.empty();
				return new DatabaseOperationResult(executionDuration, resultSetMappingDuration, statementResult);
			} catch (Exception | Error failure) {
				resultSetFailure = failure;
				throw failure;
			} finally {
				JdbcResources.close(resultSet, resultSetFailure);
			}
		}, generatedKeysPreparedStatementFactory(requestedKeyColumnNames));

		return resultHolder.value;
	}

	@NonNull
	<T> List<@Nullable T> executeReturningGeneratedKeys(@NonNull Statement statement,
																														 @NonNull Class<T> resultSetRowType,
																														 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																														 @Nullable String @Nullable [] keyColumnNames,
																														 StatementContext.@Nullable SpiOverrides spiOverrides,
																														 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		List<T> list = new ArrayList<>();
		StatementContext<T> statementContext = StatementContext.<T>with(statement, this.database)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.spiOverrides(spiOverrides)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);
		String[] requestedKeyColumnNames = copyGeneratedKeyColumnNames(keyColumnNames);

		performDatabaseOperation(statementContext, parametersAsList, preparedStatementCustomizer, (PreparedStatement preparedStatement, DatabaseOperationTiming timing) -> {
			long startTime = nanoTime();
			Long rowsAffected = executeUpdate(preparedStatement);

			ResultSet resultSet = preparedStatement.getGeneratedKeys();
			Throwable resultSetFailure = null;
			try {
				timing.beginMapping();
				Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
				startTime = nanoTime();

				while (resultSet.next()) {
					try {
						T listElement = resultSetMapperFor(statementContext).map(statementContext, resultSet, statementContext.getResultSetRowType().get(), getInstanceProvider()).orElse(null);
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
			} catch (Exception | Error failure) {
				resultSetFailure = failure;
				throw failure;
			} finally {
				JdbcResources.close(resultSet, resultSetFailure);
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

		return (connection, statementContext) -> this.connections.dialect(connection)
				.prepareGeneratedKeysStatement(connection, statementContext, keyColumnNames);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns 0 or 1 rows with database-native syntax such as PostgreSQL/SQLite {@code RETURNING},
	 * MariaDB {@code INSERT ... RETURNING},
	 * or SQL Server {@code OUTPUT}.
	 *
	 * @param sql              the SQL query to execute
	 * @param resultSetRowType the type to which the {@link ResultSet} row should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a single result (or no result)
	 * @throws DatabaseException if > 1 row is returned
	 */
	@NonNull
	<T> Optional<T> executeForObject(@NonNull String sql,
																					 @NonNull Class<T> resultSetRowType,
																					 Object @Nullable ... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return executeForObject(Statement.of(this.database.generateId(), sql), resultSetRowType, parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns 0 or 1 rows with database-native syntax such as PostgreSQL/SQLite {@code RETURNING},
	 * MariaDB {@code INSERT ... RETURNING},
	 * or SQL Server {@code OUTPUT}.
	 *
	 * @param statement        the SQL statement to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a single result (or no result)
	 * @throws DatabaseException if > 1 row is returned
	 */
	<T> Optional<T> executeForObject(@NonNull Statement statement,
																					 @NonNull Class<T> resultSetRowType,
																					 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		return executeForObject(statement, resultSetRowType, null, null, parameters);
	}

	<T> Optional<T> executeForObject(@NonNull Statement statement,
																					 @NonNull Class<T> resultSetRowType,
																					 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																					 StatementContext.@Nullable SpiOverrides spiOverrides,
																					 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		// Ultimately we just delegate to queryForObject.
		// Having `executeForList` is to allow for users to explicitly express intent
		// and make static analysis of code easier (e.g. maybe you'd like to hook all of your "execute" statements for
		// logging, or delegation to a writable master as opposed to a read replica)
		return queryForObject(statement, resultSetRowType, preparedStatementCustomizer, spiOverrides, parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns any number of rows with database-native syntax such as PostgreSQL/SQLite {@code RETURNING},
	 * MariaDB {@code INSERT ... RETURNING},
	 * or SQL Server {@code OUTPUT}.
	 *
	 * @param sql              the SQL to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a list of results
	 */
	@NonNull
	<T> List<@Nullable T> executeForList(@NonNull String sql,
																							 @NonNull Class<T> resultSetRowType,
																							 Object @Nullable ... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return executeForList(Statement.of(this.database.generateId(), sql), resultSetRowType, parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns any number of rows with database-native syntax such as PostgreSQL/SQLite {@code RETURNING},
	 * MariaDB {@code INSERT ... RETURNING},
	 * or SQL Server {@code OUTPUT}.
	 *
	 * @param statement        the SQL statement to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a list of results
	 */
	@NonNull
	<T> List<@Nullable T> executeForList(@NonNull Statement statement,
																							 @NonNull Class<T> resultSetRowType,
																							 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		return executeForList(statement, resultSetRowType, null, null, parameters);
	}

	<T> List<@Nullable T> executeForList(@NonNull Statement statement,
																							 @NonNull Class<T> resultSetRowType,
																							 @Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																							 StatementContext.@Nullable SpiOverrides spiOverrides,
																							 Object @Nullable ... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		// Ultimately we just delegate to queryForList.
		// Having `executeForList` is to allow for users to explicitly express intent
		// and make static analysis of code easier (e.g. maybe you'd like to hook all of your "execute" statements for
		// logging, or delegation to a writable master as opposed to a read replica)
		return queryForList(statement, resultSetRowType, preparedStatementCustomizer, spiOverrides, parameters);
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
	List<Long> executeBatch(@NonNull String sql,
																	@NonNull List<List<Object>> parameterGroups) {
		requireNonNull(sql);
		requireNonNull(parameterGroups);

		return executeBatch(Statement.of(this.database.generateId(), sql), parameterGroups);
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
	List<Long> executeBatch(@NonNull Statement statement,
																	@NonNull List<List<Object>> parameterGroups) {
		requireNonNull(statement);
		requireNonNull(parameterGroups);

		return executeBatch(statement, parameterGroups, null, null, null);
	}

	List<Long> executeBatch(@NonNull Statement statement,
																	@NonNull List<List<Object>> parameterGroups,
																	@Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																	@Nullable Integer batchChunkSize,
																	StatementContext.@Nullable SpiOverrides spiOverrides) {
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
		StatementContext<List<Long>> statementContext = StatementContext.with(statement, this.database)
				.parameters((List) parameterGroups)
				.resultSetRowType(List.class)
				.batchParameterGroups(true)
				.spiOverrides(spiOverrides)
				.build();

		if (batchChunkSize == null || batchChunkSize >= parameterGroups.size()) {
			performDatabaseOperation(statementContext, (preparedStatement) -> {
				applyPreparedStatementCustomizer(statementContext, preparedStatement, preparedStatementCustomizer);

				for (List<Object> parameterGroup : parameterGroups) {
					if (parameterGroup.size() > 0)
						performPreparedStatementBinding(statementContext, preparedStatement, parameterGroup);

					preparedStatement.addBatch();
				}
			}, (PreparedStatement preparedStatement, DatabaseOperationTiming timing) -> {
				long startTime = nanoTime();
				List<Long> result = executePreparedStatementBatch(preparedStatement);

				resultHolder.value = result;
				return batchDatabaseOperationResult(startTime, result);
			}, parameterGroups.size());
		} else {
			int effectiveBatchChunkSize = batchChunkSize;

			performDatabaseOperation(statementContext, (preparedStatement) -> {
				applyPreparedStatementCustomizer(statementContext, preparedStatement, preparedStatementCustomizer);
			}, (PreparedStatement preparedStatement, DatabaseOperationTiming timing) -> {
				long startTime = nanoTime();
				int currentBatchSize = 0;
				List<Long> result = new ArrayList<>(parameterGroups.size());
				Throwable chunkCleanupFailure = null;

				try {
					for (List<Object> parameterGroup : parameterGroups) {
						if (parameterGroup.size() > 0)
							performPreparedStatementBinding(statementContext, preparedStatement, parameterGroup);

						preparedStatement.addBatch();
						++currentBatchSize;

						if (currentBatchSize == effectiveBatchChunkSize) {
							result.addAll(executePreparedStatementBatch(preparedStatement));
							chunkCleanupFailure = releaseChunkParameterResources(statementContext, preparedStatement, chunkCleanupFailure);
							currentBatchSize = 0;
						}
					}

					// The trailing partial chunk releases on the same terms as a full one; leaving it to statement exit
					// would make per-chunk release depend on whether batchChunkSize happens to divide the batch.
					if (currentBatchSize > 0) {
						result.addAll(executePreparedStatementBatch(preparedStatement));
						chunkCleanupFailure = releaseChunkParameterResources(statementContext, preparedStatement, chunkCleanupFailure);
					}
				} finally {
					if (chunkCleanupFailure != null) {
						// A failing chunk must not discard a release failure from a chunk that already succeeded, and the
						// statement's own drain keeps the first failure it sees. Draining what a failing chunk left queued
						// here, with the earlier failure as the seed, keeps that failure primary; re-queueing it behind
						// those entries instead would let a later release displace it.
						chunkCleanupFailure = closeStatementContextResources(statementContext, chunkCleanupFailure);
						statementContext.addCleanupOperation(rethrowingCleanupOperation(chunkCleanupFailure));
					}
				}

				resultHolder.value = result;
				return batchDatabaseOperationResult(startTime, result);
			}, parameterGroups.size());
		}

		return resultHolder.value;
	}

	@NonNull
	private DatabaseOperationResult batchDatabaseOperationResult(long startTime,
																															@NonNull List<Long> result) {
		requireNonNull(result);

		Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
		StatementResult statementResult = StatementResult.empty();
		if (getMetricsCollectorDispatcher().isEnabled()) {
			Long rowsAffected = sumBatchUpdateCounts(result);
			statementResult = rowsAffected == null ? StatementResult.empty() : StatementResult.ofRowsAffected(rowsAffected);
		}

		return new DatabaseOperationResult(executionDuration, null, statementResult);
	}

	/**
	 * Releases what a chunk's parameter groups bound, now that the execution which consumed their values has
	 * completed: the statement's batch and then its parameter slots, so the statement holds none of the chunk's
	 * values, and only afterwards the deferred cleanup registered for those groups. Deferred cleanup is registered
	 * per parameter group, so draining only at statement exit would hold every chunk's freeable resources and leave
	 * {@code batchChunkSize} bounding the JDBC batch alone.
	 * <p>
	 * A release failure accumulates into the returned value rather than propagating, so the chunks after this one
	 * still execute. If a driver explicitly does not support clearing parameter slots, this method leaves the
	 * deferred cleanup queued. Other reset failures propagate so transaction rollback, cancellation, and connection
	 * failures cannot disappear while later chunks continue executing.
	 */
	@Nullable
	private static Throwable releaseChunkParameterResources(@NonNull StatementContext<?> statementContext,
																													@NonNull PreparedStatement preparedStatement,
																													@Nullable Throwable chunkCleanupFailure) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);

		preparedStatement.clearBatch();

		if (statementContext.getCleanupOperations().isEmpty())
			return chunkCleanupFailure;

		// clearBatch discards the driver's copies of this chunk's commands, but the values the chunk's last group
		// bound stay in the statement's parameter slots until clearParameters - a driver free to read a slot again
		// must not find a released resource there.
		try {
			preparedStatement.clearParameters();
		} catch (SQLFeatureNotSupportedException | UnsupportedOperationException | AbstractMethodError e) {
			if (JdbcFallbacks.isTerminal(e))
				throw e;
			// An explicitly unsupported reset falls back to deferred release. Do not extend this catch to all
			// SQLExceptions: a reset can report cancellation or a rolled-back transaction, and continuing would
			// hide the failure and could commit only the chunks that follow it.
			return chunkCleanupFailure;
		}

		return closeStatementContextResources(statementContext, chunkCleanupFailure);
	}

	/**
	 * Re-queues a cleanup failure the caller already accumulated, so the statement's own cleanup drain
	 * accounts for it exactly as it accounts for a release that fails there: suppressed onto the statement's
	 * failure when there is one, and the exit failure only when there is none.
	 */
	@NonNull
	private static AutoCloseable rethrowingCleanupOperation(@NonNull Throwable cleanupFailure) {
		requireNonNull(cleanupFailure);

		return () -> {
			if (cleanupFailure instanceof Exception exception)
				throw exception;

			if (cleanupFailure instanceof Error error)
				throw error;

			throw new RuntimeException(cleanupFailure);
		};
	}

	@NonNull
	private List<Long> executePreparedStatementBatch(@NonNull PreparedStatement preparedStatement) throws SQLException {
		requireNonNull(preparedStatement);

		DatabaseOperationSupportStatus executeLargeBatchSupported = getExecuteLargeBatchSupported();

		// Use the appropriate "large" value if we know it.
		// If we don't know it, detect it and store it.
		if (executeLargeBatchSupported == DatabaseOperationSupportStatus.YES) {
			long[] resultArray = preparedStatement.executeLargeBatch();
			return Arrays.stream(resultArray).boxed().collect(Collectors.toList());
		}
		if (executeLargeBatchSupported == DatabaseOperationSupportStatus.NO) {
			int[] resultArray = preparedStatement.executeBatch();
			return Arrays.stream(resultArray).asLongStream().boxed().collect(Collectors.toList());
		}

		// If the driver doesn't support executeLargeBatch, then UnsupportedOperationException is thrown.
		try {
			long[] resultArray = preparedStatement.executeLargeBatch();
			setExecuteLargeBatchSupported(DatabaseOperationSupportStatus.YES);
			return Arrays.stream(resultArray).boxed().collect(Collectors.toList());
		} catch (SQLFeatureNotSupportedException | UnsupportedOperationException | AbstractMethodError e) {
			if (JdbcFallbacks.isTerminal(e))
				throw e;
			setExecuteLargeBatchSupported(DatabaseOperationSupportStatus.NO);
			int[] resultArray = preparedStatement.executeBatch();
			return Arrays.stream(resultArray).asLongStream().boxed().collect(Collectors.toList());
		} catch (SQLException e) {
			if (JdbcFallbacks.isTerminal(e))
				throw e;
			if (!isUnsupportedSqlFeature(e))
				throw e;

			setExecuteLargeBatchSupported(DatabaseOperationSupportStatus.NO);
			int[] resultArray = preparedStatement.executeBatch();
			return Arrays.stream(resultArray).asLongStream().boxed().collect(Collectors.toList());
		}
	}

	<T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
																							@NonNull List<Object> parameters,
																							@NonNull DatabaseOperation databaseOperation) {
		requireNonNull(statementContext);
		requireNonNull(parameters);
		requireNonNull(databaseOperation);

		performDatabaseOperation(statementContext, parameters, null, databaseOperation);
	}

	<T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
																							@NonNull List<Object> parameters,
																							@Nullable PreparedStatementCustomizer preparedStatementCustomizer,
																							@NonNull DatabaseOperation databaseOperation) {
		performDatabaseOperation(statementContext, parameters, preparedStatementCustomizer, databaseOperation,
				(connection, context) -> connection.prepareStatement(context.getStatement().getSql()));
	}

	<T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
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

	<T> void performPreparedStatementBinding(@NonNull StatementContext<T> statementContext,
																										 @NonNull PreparedStatement preparedStatement,
																										 @NonNull List<Object> parameters) {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);
		requireNonNull(parameters);

		try {
			DefaultPreparedStatementBinder.ParameterSqlTypeResolver parameterSqlTypeResolver =
					new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(preparedStatement);
			PreparedStatementBinder preparedStatementBinder = preparedStatementBinderFor(statementContext);

			for (int i = 0; i < parameters.size(); ++i) {
				Object parameter = SecureParameterSupport.unwrapSecureAndOptionalParameter(parameters.get(i));
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
						if (JdbcFallbacks.isTerminal(e))
							throw e;
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

	void applyPreparedStatementCustomizer(@NonNull StatementContext<?> statementContext,
																									@NonNull PreparedStatement preparedStatement,
																									@Nullable PreparedStatementCustomizer preparedStatementCustomizer) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);

		if (preparedStatementCustomizer == null)
			return;

		preparedStatementCustomizer.customize(statementContext, preparedStatement);
	}

	<T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
																							@NonNull PreparedStatementBindingOperation preparedStatementBindingOperation,
																							@NonNull DatabaseOperation databaseOperation) {
		performDatabaseOperation(statementContext, preparedStatementBindingOperation, databaseOperation, null);
	}

	<T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
																							@NonNull PreparedStatementBindingOperation preparedStatementBindingOperation,
																							@NonNull DatabaseOperation databaseOperation,
																							@Nullable Integer batchSize) {
		performDatabaseOperation(statementContext, preparedStatementBindingOperation, databaseOperation, batchSize,
				(connection, context) -> connection.prepareStatement(context.getStatement().getSql()));
	}

	<T> void performDatabaseOperation(@NonNull StatementContext<T> statementContext,
																							@NonNull PreparedStatementBindingOperation preparedStatementBindingOperation,
																							@NonNull DatabaseOperation databaseOperation,
																							@Nullable Integer batchSize,
																							@NonNull PreparedStatementFactory preparedStatementFactory) {
		requireNonNull(statementContext);
		requireNonNull(preparedStatementBindingOperation);
		requireNonNull(databaseOperation);
		requireNonNull(preparedStatementFactory);

		StatementExecution lease = new StatementExecution(statementContext, this.connections, this.metricsCollectorDispatcher);
		long startTime;
		Duration preparationDuration = null;
		Duration executionDuration = null;
		Duration resultSetMappingDuration = null;
		StatementResult statementResult = StatementResult.empty();
		Exception exception = null;
		Throwable thrown = null;
		StatementConnections.MetadataScope metadataScope = null;

		try {
			Connection connection = lease.acquire("execute a statement on the transaction connection");
			metadataScope = lease.metadataScope();
			startTime = nanoTime();

			try {
				PreparedStatement preparedStatement = preparedStatementFactory.prepare(connection, statementContext);
				Throwable statementFailure = null;
				try {
					try {
						preparedStatementBindingOperation.perform(preparedStatement);
					} finally {
						preparationDuration = Duration.ofNanos(nanoTime() - startTime);
					}

					getMetricsCollectorDispatcher().willExecuteStatement(statementContext);
					DatabaseOperationTiming timing = new DatabaseOperationTiming();
					try {
						DatabaseOperationResult databaseOperationResult = databaseOperation.perform(preparedStatement, timing);
						executionDuration = databaseOperationResult.getExecutionDuration().orElse(null);
						resultSetMappingDuration = databaseOperationResult.getResultSetMappingDuration().orElse(null);
						statementResult = databaseOperationResult.getStatementResult();
					} finally {
						timing.finish();
						// Failed operations do not return a DatabaseOperationResult; retain the elapsed active phase.
						if (executionDuration == null)
							executionDuration = timing.executionDuration;
						if (resultSetMappingDuration == null)
							resultSetMappingDuration = timing.resultSetMappingDuration;
					}
					warmDatabaseTypeCacheForMetricsIfNeeded(statementContext);
				} catch (Exception | Error failure) {
					statementFailure = failure;
					throw failure;
				} finally {
					JdbcResources.close(preparedStatement, statementFailure);
				}
			} finally {
				// prepareStatement itself can fail before the binding block is entered.
				if (preparationDuration == null)
					preparationDuration = Duration.ofNanos(nanoTime() - startTime);
			}
		} catch (DatabaseException e) {
			DatabaseException wrapped = databaseExceptionWithStatementContext(statementContext, e);
			exception = wrapped;
			thrown = wrapped;
			throw wrapped;
		} catch (Error e) {
			exception = databaseExceptionWithStatementContext(statementContext, e);
			thrown = e;
			throw e;
		} catch (Exception e) {
			DatabaseException wrapped = databaseExceptionWithStatementContext(statementContext, e);
			// Store the wrapped (scrubbed) form in the StatementLog, consistent with the DatabaseException
			// and Error paths - the raw driver exception remains available via the wrapped cause
			exception = wrapped;
			thrown = wrapped;
			throw wrapped;
		} finally {
			Throwable cleanupFailure = null;

			if (metadataScope != null) metadataScope.close();
			try {
				cleanupFailure = closeStatementContextResources(statementContext, cleanupFailure);
			} finally {
				cleanupFailure = lease.close(cleanupFailure);

				StatementLog statementLog =
						StatementLog.withStatementContext(statementContext)
								.connectionAcquisitionDuration(lease.acquisitionDuration())
								.elapsedDuration(lease.elapsedDuration())
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
					getMetricsCollectorDispatcher().didFailToExecuteStatement(statementContext, statementLog, this.connections.cachedType(),
							requireNonNull(statementThrowable));
				}

				try {
					getStatementLogger().log(statementLog);
				} catch (Throwable cleanupException) {
					cleanupFailure = appendCleanupFailure(cleanupFailure, cleanupException);
				}
			}

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

	/**
	 * Resolves the effective {@link ResultSetMapper} for a statement: the per-query override carried on
	 * the {@link StatementContext} when present, otherwise the database-wide instance.
	 */
	@NonNull
	ResultSetMapper resultSetMapperFor(@NonNull StatementContext<?> statementContext) {
		requireNonNull(statementContext);
		ResultSetMapper resultSetMapperOverride = statementContext.getResultSetMapperOverride();
		return resultSetMapperOverride != null ? resultSetMapperOverride : getResultSetMapper();
	}

	/**
	 * Resolves the effective {@link PreparedStatementBinder} for a statement: the per-query override
	 * carried on the {@link StatementContext} when present, otherwise the database-wide instance.
	 */
	@NonNull
	private PreparedStatementBinder preparedStatementBinderFor(@NonNull StatementContext<?> statementContext) {
		requireNonNull(statementContext);
		PreparedStatementBinder preparedStatementBinderOverride = statementContext.getPreparedStatementBinderOverride();
		return preparedStatementBinderOverride != null ? preparedStatementBinderOverride : getPreparedStatementBinder();
	}

	@NonNull
	private DatabaseOperationSupportStatus getExecuteLargeBatchSupported() {
		return this.executeLargeBatchSupported;
	}

	private void setExecuteLargeBatchSupported(@NonNull DatabaseOperationSupportStatus executeLargeBatchSupported) {
		requireNonNull(executeLargeBatchSupported);
		this.executeLargeBatchSupported = executeLargeBatchSupported;
	}

	@NonNull
	private DatabaseOperationSupportStatus getExecuteLargeUpdateSupported() {
		return this.executeLargeUpdateSupported;
	}

	private void setExecuteLargeUpdateSupported(@NonNull DatabaseOperationSupportStatus executeLargeUpdateSupported) {
		requireNonNull(executeLargeUpdateSupported);
		this.executeLargeUpdateSupported = executeLargeUpdateSupported;
	}

	@FunctionalInterface
	interface DatabaseOperation {
		@NonNull
		DatabaseOperationResult perform(@NonNull PreparedStatement preparedStatement,
																		@NonNull DatabaseOperationTiming timing) throws Exception;
	}

	@NotThreadSafe
	static final class DatabaseOperationTiming {
		private final long executionStartTime = nanoTime();
		private long mappingStartTime;
		private boolean mappingStarted;
		@Nullable
		private Duration executionDuration;
		@Nullable
		private Duration resultSetMappingDuration;

		void beginMapping() {
			long now = nanoTime();
			this.executionDuration = Duration.ofNanos(now - this.executionStartTime);
			this.mappingStartTime = now;
			this.mappingStarted = true;
		}

		private void finish() {
			long now = nanoTime();
			if (this.mappingStarted)
				this.resultSetMappingDuration = Duration.ofNanos(now - this.mappingStartTime);
			else
				this.executionDuration = Duration.ofNanos(now - this.executionStartTime);
		}
	}

	@FunctionalInterface
	private interface PreparedStatementFactory {
		@NonNull
		PreparedStatement prepare(@NonNull Connection connection,
															@NonNull StatementContext<?> statementContext) throws SQLException;
	}

	@FunctionalInterface
	private interface PreparedStatementBindingOperation {
		void perform(@NonNull PreparedStatement preparedStatement) throws Exception;
	}

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
