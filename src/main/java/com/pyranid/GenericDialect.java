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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

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
	public List<String> sqlFragmentsForOperators(boolean hasQuestionMarkOperators,
																							 @NonNull List<String> sqlFragments,
																							 @NonNull List<@NonNull List<@NonNull Integer>> questionMarkOperatorFragmentIndexes) {
		requireNonNull(sqlFragments);
		requireNonNull(questionMarkOperatorFragmentIndexes);

		return sqlFragments;
	}

	@Override
	public boolean supportsSqlArray() {
		return true;
	}

	@Override
	public boolean supportsVector() {
		return false;
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

		throw new IllegalArgumentException(format("%s supported only on %s.%s",
				VectorParameter.class.getSimpleName(), DatabaseType.class.getSimpleName(), DatabaseType.POSTGRESQL.name()));
	}

	@NonNull
	@Override
	public DatabaseStreamState configureStreamingConnection(@NonNull Connection connection,
																												 boolean transactionPresent) throws SQLException {
		requireNonNull(connection);

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
																									boolean transactionPresent,
																									boolean queryFetchSizeConfigured) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(databaseStreamState);
	}

	@Nullable
	@Override
	public Throwable completeStreamingConnection(@NonNull Connection connection,
																							 @NonNull DatabaseStreamState databaseStreamState,
																							 boolean streamSucceeded,
																							 @Nullable Throwable cleanupFailure) {
		requireNonNull(connection);
		requireNonNull(databaseStreamState);

		return cleanupFailure;
	}

	@NonNull
	@Override
	public DatabaseExceptionMetadata databaseExceptionMetadata(@Nullable Throwable cause) {
		return DatabaseExceptionMetadata.fromCause(cause);
	}

	@Nullable
	@Override
	public Object unwrapResultSetValue(@NonNull Object resultSetValue) {
		return requireNonNull(resultSetValue);
	}

	@Override
	public boolean isTimestampWithTimeZone(@NonNull ResultSetMetaData resultSetMetaData,
																				 @NonNull Integer columnIndex) throws SQLException {
		requireNonNull(resultSetMetaData);
		requireNonNull(columnIndex);

		if (resultSetMetaData.getColumnType(columnIndex) == Types.TIMESTAMP_WITH_TIMEZONE)
			return true;

		String typeName = resultSetMetaData.getColumnTypeName(columnIndex);
		if (typeName == null)
			return false;

		String normalizedTypeName = typeName.toUpperCase(Locale.ROOT);
		return normalizedTypeName.contains("WITH TIME ZONE") || normalizedTypeName.contains("TIMESTAMPTZ");
	}
}
