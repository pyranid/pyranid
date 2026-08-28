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
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

/**
 * Internal strategy for database-specific behavior.
 */
interface DatabaseDialect {
	@NonNull
	DatabaseNotificationSupport notificationSupport();

	@NonNull
	List<String> sqlFragmentsForOperators(@NonNull Boolean hasQuestionMarkOperators,
																				@NonNull List<String> sqlFragments,
																				@NonNull List<@NonNull List<@NonNull Integer>> questionMarkOperatorFragmentIndexes);

	@NonNull
	Boolean supportsSqlArray();

	@NonNull
	Boolean supportsVector();

	/**
	 * Whether the driver supports {@link PreparedStatement#setObject(int, Object, int)} with an explicit
	 * target SQL type when binding {@code java.time} temporal values. Drivers that reject target-typed
	 * binds outright (e.g. DuckDB's "Unknown target type") but accept untyped
	 * {@link PreparedStatement#setObject(int, Object)} should return {@code false}.
	 */
	@NonNull
	Boolean supportsSetObjectWithTargetSqlType();

	/**
	 * Whether {@link java.time.OffsetTime} parameters must bind as ISO-8601 strings. Some drivers
	 * (e.g. DuckDB) accept an {@code OffsetTime} at {@code setObject(...)} time but reject it at execute
	 * time, so try-bind detection cannot fall back; the dialect declares the string form up front.
	 */
	@NonNull
	Boolean bindsOffsetTimeAsString();

	/**
	 * Massages a normalized, non-null SQL ARRAY element into a form accepted by this dialect's JDBC
	 * driver. The default dialect leaves elements unchanged.
	 */
	@NonNull
	Object normalizeSqlArrayElement(@NonNull String baseTypeName,
																		@NonNull Object element,
																		@NonNull ZoneId timeZone);

	@NonNull
	Object normalizeUuid(@NonNull UUID uuid);

	void bindJson(@NonNull PreparedStatement preparedStatement,
								@NonNull Integer parameterIndex,
								@NonNull String json,
								@NonNull BindingPreference bindingPreference) throws SQLException;

	void bindNullJson(@NonNull PreparedStatement preparedStatement,
										@NonNull Integer parameterIndex,
										@NonNull BindingPreference bindingPreference,
										@Nullable Integer fallbackSqlType) throws SQLException;

	void bindVector(@NonNull PreparedStatement preparedStatement,
									@NonNull Integer parameterIndex,
									double @Nullable [] elements,
									@Nullable Integer fallbackSqlType) throws SQLException;

	@NonNull
	DatabaseStreamState configureStreamingConnection(@NonNull Connection connection,
																									 @NonNull Boolean transactionPresent) throws SQLException;

	@NonNull
	PreparedStatement prepareStreamingStatement(@NonNull Connection connection,
																							@NonNull StatementContext<?> statementContext) throws SQLException;

	@NonNull
	PreparedStatement prepareGeneratedKeysStatement(@NonNull Connection connection,
																									@NonNull StatementContext<?> statementContext,
																									@NonNull String @NonNull [] keyColumnNames) throws SQLException;

	void configureStreamingPreparedStatement(@NonNull PreparedStatement preparedStatement,
																					 @NonNull DatabaseStreamState databaseStreamState,
																					 @NonNull Boolean transactionPresent,
																					 @NonNull Boolean queryFetchSizeConfigured) throws SQLException;

	@Nullable
	Throwable completeStreamingConnection(@NonNull Connection connection,
																				@NonNull DatabaseStreamState databaseStreamState,
																				@NonNull Boolean streamSucceeded,
																				@Nullable Throwable cleanupFailure);

	@NonNull
	DatabaseExceptionMetadata databaseExceptionMetadata(@Nullable Throwable cause);

	@NonNull
	Boolean isUniqueConstraintViolation(@NonNull DatabaseExceptionMetadata metadata,
																			@Nullable Throwable cause);

	@NonNull
	Boolean isForeignKeyViolation(@NonNull DatabaseExceptionMetadata metadata,
																@Nullable Throwable cause);

	@NonNull
	Boolean isDeadlock(@NonNull DatabaseExceptionMetadata metadata,
										 @Nullable Throwable cause);

	@NonNull
	Boolean isTransient(@NonNull DatabaseExceptionMetadata metadata,
											@Nullable Throwable cause);

	@NonNull
	Boolean isSerializationFailure(@NonNull DatabaseExceptionMetadata metadata,
																 @Nullable Throwable cause);

	@NonNull
	Boolean isTimeout(@NonNull DatabaseExceptionMetadata metadata,
										@Nullable Throwable cause);

	@Nullable
	Object unwrapResultSetValue(@NonNull Object resultSetValue);

	@NonNull
	Boolean isTimestampWithTimeZone(@NonNull ResultSetMetaData resultSetMetaData,
																	@NonNull Integer columnIndex) throws SQLException;

	@NonNull
	static DatabaseDialect forExceptionCause(@Nullable Throwable cause) {
		if (cause != null && "org.postgresql.util.PSQLException".equals(cause.getClass().getName()))
			return PostgresDialect.INSTANCE;

		return GenericDialect.INSTANCE;
	}
}
