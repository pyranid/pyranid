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
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * PostgreSQL-specific behavior.
 */
final class PostgresDialect extends GenericDialect {
	@NonNull
	static final PostgresDialect INSTANCE = new PostgresDialect();
	private static final int DEFAULT_STREAM_FETCH_SIZE = 256;

	private PostgresDialect() {}

	@NonNull
	@Override
	public List<String> sqlFragmentsForOperators(boolean hasQuestionMarkOperators,
																							 @NonNull List<String> sqlFragments,
																							 @NonNull List<@NonNull List<@NonNull Integer>> questionMarkOperatorFragmentIndexes) {
		requireNonNull(sqlFragments);
		requireNonNull(questionMarkOperatorFragmentIndexes);

		if (!hasQuestionMarkOperators)
			return sqlFragments;

		if (sqlFragments.size() != questionMarkOperatorFragmentIndexes.size())
			throw new IllegalArgumentException("SQL fragments and question-mark operator indexes must have the same size");

		List<String> postgresqlSqlFragments = new ArrayList<>(sqlFragments.size());

		for (int i = 0; i < sqlFragments.size(); ++i)
			postgresqlSqlFragments.add(postgresqlSqlFragment(sqlFragments.get(i), questionMarkOperatorFragmentIndexes.get(i)));

		return List.copyOf(postgresqlSqlFragments);
	}

	@Override
	public boolean supportsVector() {
		return true;
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

		org.postgresql.util.PGobject pg = new org.postgresql.util.PGobject();
		pg.setType(bindingPreference == BindingPreference.TEXT ? "json" : "jsonb");
		pg.setValue(json);
		preparedStatement.setObject(parameterIndex, pg);
	}

	@Override
	public void bindNullJson(@NonNull PreparedStatement preparedStatement,
													 @NonNull Integer parameterIndex,
													 @NonNull BindingPreference bindingPreference,
													 @Nullable Integer fallbackSqlType) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(bindingPreference);

		String typeName = bindingPreference == BindingPreference.TEXT ? "json" : "jsonb";
		DefaultPreparedStatementBinder.setNullWithFallback(preparedStatement, parameterIndex, Types.OTHER, typeName, fallbackSqlType);
	}

	@Override
	public void bindVector(@NonNull PreparedStatement preparedStatement,
												 @NonNull Integer parameterIndex,
												 double @Nullable [] elements,
												 @Nullable Integer fallbackSqlType) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);

		if (elements == null) {
			DefaultPreparedStatementBinder.setNullWithFallback(preparedStatement, parameterIndex, Types.OTHER, "vector", fallbackSqlType);
			return;
		}

		org.postgresql.util.PGobject pg = new org.postgresql.util.PGobject();
		pg.setType("vector");
		pg.setValue(postgresVectorLiteralValue(elements));
		preparedStatement.setObject(parameterIndex, pg);
	}

	@NonNull
	@Override
	public DatabaseStreamState configureStreamingConnection(@NonNull Connection connection,
																												 boolean transactionPresent) throws SQLException {
		requireNonNull(connection);

		if (transactionPresent)
			return DatabaseStreamState.none();

		Boolean initialAutoCommit = connection.getAutoCommit();

		if (Boolean.TRUE.equals(initialAutoCommit))
			connection.setAutoCommit(false);

		return DatabaseStreamState.managedTransaction(initialAutoCommit);
	}

	@Override
	public void configureStreamingPreparedStatement(@NonNull PreparedStatement preparedStatement,
																									@NonNull DatabaseStreamState databaseStreamState,
																									boolean transactionPresent,
																									boolean queryFetchSizeConfigured) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(databaseStreamState);

		if (!databaseStreamState.isManagedTransactionStarted() || transactionPresent || queryFetchSizeConfigured)
			return;

		if (preparedStatement.getFetchSize() <= 0)
			preparedStatement.setFetchSize(DEFAULT_STREAM_FETCH_SIZE);
	}

	@Nullable
	@Override
	public Throwable completeStreamingConnection(@NonNull Connection connection,
																							 @NonNull DatabaseStreamState databaseStreamState,
																							 boolean streamSucceeded,
																							 @Nullable Throwable cleanupFailure) {
		requireNonNull(connection);
		requireNonNull(databaseStreamState);

		if (!databaseStreamState.isManagedTransactionStarted())
			return cleanupFailure;

		boolean streamTransactionCleanupFailed = false;

		try {
			if (streamSucceeded)
				connection.commit();
			else
				connection.rollback();
		} catch (Throwable cleanupException) {
			streamTransactionCleanupFailed = true;
			cleanupFailure = addCleanupFailure(cleanupFailure, cleanupException);
		}

		if (Boolean.TRUE.equals(databaseStreamState.getInitialAutoCommit())) {
			try {
				connection.setAutoCommit(true);
			} catch (Throwable cleanupException) {
				streamTransactionCleanupFailed = true;
				cleanupFailure = addCleanupFailure(cleanupFailure, cleanupException);
			}
		}

		if (streamTransactionCleanupFailed)
			cleanupFailure = abortStreamingConnection(connection, cleanupFailure);

		return cleanupFailure;
	}

	@NonNull
	@Override
	public DatabaseExceptionMetadata databaseExceptionMetadata(@Nullable Throwable cause) {
		DatabaseExceptionMetadata.Builder builder = DatabaseExceptionMetadata.builder(cause);

		if (cause != null && "org.postgresql.util.PSQLException".equals(cause.getClass().getName())) {
			org.postgresql.util.PSQLException psqlException = (org.postgresql.util.PSQLException) cause;
			org.postgresql.util.ServerErrorMessage serverErrorMessage = psqlException.getServerErrorMessage();

			if (serverErrorMessage != null) {
				builder.column(serverErrorMessage.getColumn())
						.constraint(serverErrorMessage.getConstraint())
						.datatype(serverErrorMessage.getDatatype())
						.detail(serverErrorMessage.getDetail())
						.file(serverErrorMessage.getFile())
						.hint(serverErrorMessage.getHint())
						.internalQuery(serverErrorMessage.getInternalQuery())
						.dbmsMessage(serverErrorMessage.getMessage())
						.routine(serverErrorMessage.getRoutine())
						.schema(serverErrorMessage.getSchema())
						.severity(serverErrorMessage.getSeverity())
						.table(serverErrorMessage.getTable())
						.where(serverErrorMessage.getWhere())
						.internalPosition(serverErrorMessage.getInternalPosition())
						.line(serverErrorMessage.getLine())
						.position(serverErrorMessage.getPosition());

				if (serverErrorMessage.getSQLState() != null)
					builder.sqlState(serverErrorMessage.getSQLState());
			}
		}

		return builder.build();
	}

	@Override
	public boolean isDeadlock(@NonNull DatabaseExceptionMetadata metadata,
														@Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasSqlState(metadata, cause, "40P01");
	}

	@Nullable
	@Override
	public Object unwrapResultSetValue(@NonNull Object resultSetValue) {
		requireNonNull(resultSetValue);

		if ("org.postgresql.util.PGobject".equals(resultSetValue.getClass().getName())) {
			org.postgresql.util.PGobject pgObject = (org.postgresql.util.PGobject) resultSetValue;
			return pgObject.getValue();
		}

		return resultSetValue;
	}

	@Nullable
	private Throwable abortStreamingConnection(@NonNull Connection connection,
																						 @Nullable Throwable cleanupFailure) {
		requireNonNull(connection);

		try {
			connection.abort(Runnable::run);
		} catch (Throwable cleanupException) {
			cleanupFailure = addCleanupFailure(cleanupFailure, cleanupException);
		}

		return cleanupFailure;
	}

	@NonNull
	private Throwable addCleanupFailure(@Nullable Throwable cleanupFailure,
																			@NonNull Throwable cleanupException) {
		requireNonNull(cleanupException);

		if (cleanupFailure == null)
			return cleanupException;

		cleanupFailure.addSuppressed(cleanupException);
		return cleanupFailure;
	}

	@NonNull
	private String postgresqlSqlFragment(@NonNull String sqlFragment,
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

	@NonNull
	private String postgresVectorLiteralValue(double @NonNull [] elements) {
		requireNonNull(elements);

		StringBuilder sb = new StringBuilder(2 + elements.length * 8);

		sb.append('[');

		for (int i = 0; i < elements.length; i++) {
			if (i > 0) sb.append(", ");
			// Use Java default formatting (locale-independent) which is fine for pgvector
			sb.append(Double.toString(elements[i]));
		}

		sb.append(']');

		return sb.toString();
	}
}
