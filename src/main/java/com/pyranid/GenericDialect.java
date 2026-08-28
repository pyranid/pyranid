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

import com.pyranid.JsonParameter.BindingPreference;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLRecoverableException;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.sql.Types;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Portable fallback behavior for databases which do not need special handling.
 */
class GenericDialect implements DatabaseDialect {
	@NonNull
	static final GenericDialect INSTANCE = new GenericDialect();

	protected GenericDialect() {}

	@NonNull
	@Override
	public DatabaseNotificationSupport notificationSupport() {
		return UnsupportedDatabaseNotificationSupport.INSTANCE;
	}

	@NonNull
	@Override
	public List<String> sqlFragmentsForOperators(@NonNull Boolean hasQuestionMarkOperators,
																							 @NonNull List<String> sqlFragments,
																							 @NonNull List<@NonNull List<@NonNull Integer>> questionMarkOperatorFragmentIndexes) {
		requireNonNull(hasQuestionMarkOperators);
		requireNonNull(sqlFragments);
		requireNonNull(questionMarkOperatorFragmentIndexes);

		return sqlFragments;
	}

	@NonNull
	@Override
	public Boolean supportsSqlArray() {
		return true;
	}

	@NonNull
	@Override
	public Boolean supportsVector() {
		return false;
	}

	@NonNull
	@Override
	public Boolean supportsSetObjectWithTargetSqlType() {
		return true;
	}

	@NonNull
	@Override
	public Boolean bindsOffsetTimeAsString() {
		return false;
	}

	@NonNull
	@Override
	public Object normalizeSqlArrayElement(@NonNull String baseTypeName,
																				 @NonNull Object element,
																				 @NonNull ZoneId timeZone) {
		requireNonNull(baseTypeName);
		requireNonNull(element);
		requireNonNull(timeZone);
		return element;
	}

	@NonNull
	@Override
	public Object normalizeUuid(@NonNull UUID uuid) {
		return requireNonNull(uuid);
	}

	@Override
	public void bindJson(@NonNull PreparedStatement preparedStatement,
											 @NonNull Integer parameterIndex,
											 @NonNull String json,
											 @NonNull BindingPreference bindingPreference) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(json);
		requireNonNull(bindingPreference);

		preparedStatement.setString(parameterIndex, json);
	}

	@Override
	public void bindNullJson(@NonNull PreparedStatement preparedStatement,
													 @NonNull Integer parameterIndex,
													 @NonNull BindingPreference bindingPreference,
													 @Nullable Integer fallbackSqlType) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(bindingPreference);

		DefaultPreparedStatementBinder.setNullWithFallback(preparedStatement, parameterIndex, Types.LONGVARCHAR, null, fallbackSqlType);
	}

	@Override
	public void bindVector(@NonNull PreparedStatement preparedStatement,
												 @NonNull Integer parameterIndex,
												 double @Nullable [] elements,
												 @Nullable Integer fallbackSqlType) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);

		throw new IllegalArgumentException(format("%s supported only on %s.%s and %s.%s",
				VectorParameter.class.getSimpleName(), DatabaseType.class.getSimpleName(), DatabaseType.POSTGRESQL.name(),
				DatabaseType.class.getSimpleName(), DatabaseType.DUCK_DB.name()));
	}

	@NonNull
	@Override
	public DatabaseStreamState configureStreamingConnection(@NonNull Connection connection,
																												 @NonNull Boolean transactionPresent) throws SQLException {
		requireNonNull(connection);
		requireNonNull(transactionPresent);

		return DatabaseStreamState.none();
	}

	@NonNull
	@Override
	public PreparedStatement prepareStreamingStatement(@NonNull Connection connection,
																										 @NonNull StatementContext<?> statementContext) throws SQLException {
		requireNonNull(connection);
		requireNonNull(statementContext);

		return connection.prepareStatement(statementContext.getStatement().getSql());
	}

	@NonNull
	@Override
	public PreparedStatement prepareGeneratedKeysStatement(@NonNull Connection connection,
																												@NonNull StatementContext<?> statementContext,
																												@NonNull String @NonNull [] keyColumnNames) throws SQLException {
		requireNonNull(connection);
		requireNonNull(statementContext);
		requireNonNull(keyColumnNames);

		return keyColumnNames.length == 0
				? connection.prepareStatement(statementContext.getStatement().getSql(), Statement.RETURN_GENERATED_KEYS)
				: connection.prepareStatement(statementContext.getStatement().getSql(), keyColumnNames);
	}

	@Override
	public void configureStreamingPreparedStatement(@NonNull PreparedStatement preparedStatement,
																									@NonNull DatabaseStreamState databaseStreamState,
																									@NonNull Boolean transactionPresent,
																									@NonNull Boolean queryFetchSizeConfigured) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(databaseStreamState);
		requireNonNull(transactionPresent);
		requireNonNull(queryFetchSizeConfigured);
	}

	@Nullable
	@Override
	public Throwable completeStreamingConnection(@NonNull Connection connection,
																							 @NonNull DatabaseStreamState databaseStreamState,
																							 @NonNull Boolean streamSucceeded,
																							 @Nullable Throwable cleanupFailure) {
		requireNonNull(connection);
		requireNonNull(databaseStreamState);
		requireNonNull(streamSucceeded);

		return cleanupFailure;
	}

	@NonNull
	@Override
	public DatabaseExceptionMetadata databaseExceptionMetadata(@Nullable Throwable cause) {
		return DatabaseExceptionMetadata.fromCause(cause);
	}

	@NonNull
	@Override
	public Boolean isUniqueConstraintViolation(@NonNull DatabaseExceptionMetadata metadata,
																						 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasSqlState(metadata, cause, "23505");
	}

	@NonNull
	@Override
	public Boolean isForeignKeyViolation(@NonNull DatabaseExceptionMetadata metadata,
																			 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasSqlState(metadata, cause, "23503");
	}

	@NonNull
	@Override
	public Boolean isDeadlock(@NonNull DatabaseExceptionMetadata metadata,
														@Nullable Throwable cause) {
		requireNonNull(metadata);

		return false;
	}

	@NonNull
	@Override
	public Boolean isTransient(@NonNull DatabaseExceptionMetadata metadata,
														 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasSqlException(cause, sqlException -> sqlException instanceof SQLTransientException
				|| sqlException instanceof SQLRecoverableException)
				|| hasSqlStateClass(metadata, cause, "08", "40");
	}

	@NonNull
	@Override
	public Boolean isSerializationFailure(@NonNull DatabaseExceptionMetadata metadata,
																				@Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasSqlState(metadata, cause, "40001");
	}

	@NonNull
	@Override
	public Boolean isTimeout(@NonNull DatabaseExceptionMetadata metadata,
													 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasSqlException(cause, sqlException -> sqlException instanceof SQLTimeoutException)
				|| hasSqlState(metadata, cause, "57014");
	}

	@Nullable
	@Override
	public Object unwrapResultSetValue(@NonNull Object resultSetValue) {
		return requireNonNull(resultSetValue);
	}

	@NonNull
	@Override
	public Boolean isTimestampWithTimeZone(@NonNull ResultSetMetaData resultSetMetaData,
																				 @NonNull Integer columnIndex) throws SQLException {
		requireNonNull(resultSetMetaData);
		requireNonNull(columnIndex);

		if (resultSetMetaData.getColumnType(columnIndex) == Types.TIMESTAMP_WITH_TIMEZONE)
			return true;

		String typeName = resultSetMetaData.getColumnTypeName(columnIndex);
		if (typeName == null)
			return false;

		String normalizedTypeName = typeName.toUpperCase(Locale.ROOT);
		return normalizedTypeName.contains("TIMESTAMP WITH TIME ZONE")
				|| normalizedTypeName.contains("TIMESTAMP WITH LOCAL TIME ZONE")
				|| normalizedTypeName.contains("TIMESTAMPTZ");
	}

	protected boolean hasErrorCode(@NonNull DatabaseExceptionMetadata metadata,
																 @Nullable Throwable cause,
																 int @NonNull ... errorCodes) {
		requireNonNull(metadata);
		requireNonNull(errorCodes);

		for (int errorCode : errorCodes)
			if (metadata.errorCode != null && metadata.errorCode == errorCode)
				return true;

		return hasSqlException(cause, sqlException -> {
			for (int errorCode : errorCodes)
				if (sqlException.getErrorCode() == errorCode)
					return true;

			return false;
		});
	}

	protected boolean hasSqlState(@NonNull DatabaseExceptionMetadata metadata,
																@Nullable Throwable cause,
																@NonNull String @NonNull ... sqlStates) {
		requireNonNull(metadata);
		requireNonNull(sqlStates);

		Set<String> sqlStatesSet = Set.copyOf(Arrays.asList(sqlStates));

		if (metadata.sqlState != null && sqlStatesSet.contains(metadata.sqlState))
			return true;

		return hasSqlException(cause, sqlException -> sqlException.getSQLState() != null
				&& sqlStatesSet.contains(sqlException.getSQLState()));
	}

	protected boolean hasSqlStateClass(@NonNull DatabaseExceptionMetadata metadata,
																		 @Nullable Throwable cause,
																		 @NonNull String @NonNull ... sqlStateClasses) {
		requireNonNull(metadata);
		requireNonNull(sqlStateClasses);

		Set<String> sqlStateClassesSet = Set.copyOf(Arrays.asList(sqlStateClasses));

		if (metadata.sqlState != null && metadata.sqlState.length() >= 2
				&& sqlStateClassesSet.contains(metadata.sqlState.substring(0, 2)))
			return true;

		return hasSqlException(cause, sqlException -> {
			String sqlState = sqlException.getSQLState();
			return sqlState != null && sqlState.length() >= 2 && sqlStateClassesSet.contains(sqlState.substring(0, 2));
		});
	}

	protected boolean hasSqlException(@Nullable Throwable cause,
																		@NonNull Predicate<@NonNull SQLException> predicate) {
		requireNonNull(predicate);

		if (cause == null)
			return false;

		Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		Deque<Throwable> stack = new ArrayDeque<>();
		stack.add(cause);

		while (!stack.isEmpty()) {
			Throwable throwable = stack.removeFirst();

			if (!visited.add(throwable))
				continue;

			if (throwable instanceof SQLException sqlException) {
				if (predicate.test(sqlException))
					return true;

				enqueueCause(sqlException, stack, visited);

				SQLException nextException = sqlException.getNextException();
				while (nextException != null && visited.add(nextException)) {
					if (predicate.test(nextException))
						return true;

					enqueueCause(nextException, stack, visited);
					nextException = nextException.getNextException();
				}
			} else {
				enqueueCause(throwable, stack, visited);
			}
		}

		return false;
	}

	private void enqueueCause(@NonNull Throwable throwable,
														@NonNull Deque<Throwable> stack,
														@NonNull Set<Throwable> visited) {
		requireNonNull(throwable);
		requireNonNull(stack);
		requireNonNull(visited);

		Throwable cause = throwable.getCause();
		if (cause != null && cause != throwable && !visited.contains(cause))
			stack.add(cause);
	}
}
