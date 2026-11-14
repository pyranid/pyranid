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

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;
import java.util.Locale;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Oracle-specific behavior.
 */
final class OracleDialect extends GenericDialect {
	@NonNull
	static final OracleDialect INSTANCE = new OracleDialect();

	private OracleDialect() {}

	@NonNull
	@Override
	public Boolean bindTemporalParameter(@NonNull PreparedStatement preparedStatement,
																				 @NonNull Integer parameterIndex,
																				 @NonNull Object parameter,
																				 @NonNull Integer sqlType) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(parameter);
		requireNonNull(sqlType);
		if (!(parameter instanceof LocalTime localTime)) return false;
		// Oracle has no SQL TIME. An explicit TIME target passes through java.sql.Time,
		// losing fractions and applying the JVM's epoch-date offset; native LocalTime does neither.
		preparedStatement.setObject(parameterIndex, localTime);
		return true;
	}

	@NonNull
	@Override
	public Boolean supportsSqlArray() {
		return false;
	}

	@NonNull
	@Override
	public Object normalizeUuid(@NonNull UUID uuid) {
		requireNonNull(uuid);

		ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
		byteBuffer.putLong(uuid.getMostSignificantBits());
		byteBuffer.putLong(uuid.getLeastSignificantBits());
		return byteBuffer.array();
	}

	@NonNull
	@Override
	public Boolean requiresUuidParameterSqlType() {
		return true;
	}

	@NonNull
	@Override
	public Object normalizeUuid(@NonNull UUID uuid, @Nullable Integer sqlType) {
		requireNonNull(uuid);
		if (sqlType != null) {
			switch (sqlType) {
				case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
						 Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR, Types.CLOB, Types.NCLOB:
					return uuid.toString();
			}
		}
		// Keep RAW(16) compatibility when a driver cannot infer the parameter's target type.
		// Callers targeting text without metadata can pass uuid.toString() or use a custom binder.
		return normalizeUuid(uuid);
	}

	@NonNull
	@Override
	public PreparedStatement prepareGeneratedKeysStatement(@NonNull Connection connection,
																													@NonNull StatementContext<?> statementContext,
																													@NonNull String @NonNull [] keyColumnNames) throws SQLException {
		requireNonNull(connection);
		requireNonNull(statementContext);
		requireNonNull(keyColumnNames);

		if (keyColumnNames.length == 0)
			throw new IllegalArgumentException("Oracle generated-key retrieval requires explicit key column names; "
					+ "call executeReturningGeneratedKey(..., \"ID\") or executeReturningGeneratedKeys(..., \"ID\")");

		return super.prepareGeneratedKeysStatement(connection, statementContext, keyColumnNames);
	}

	@NonNull
	@Override
	public Boolean isUniqueConstraintViolation(@NonNull DatabaseExceptionMetadata metadata,
																						 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 1);
	}

	@NonNull
	@Override
	public Boolean isForeignKeyViolation(@NonNull DatabaseExceptionMetadata metadata,
																			 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 2291, 2292);
	}

	@NonNull
	@Override
	public Boolean isDeadlock(@NonNull DatabaseExceptionMetadata metadata,
														@Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 60);
	}

	@NonNull
	@Override
	public Boolean isTransient(@NonNull DatabaseExceptionMetadata metadata,
														 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isTransient(metadata, cause) || hasErrorCode(metadata, cause, 54, 60, 8177, 30006);
	}

	@NonNull
	@Override
	public Boolean isSerializationFailure(@NonNull DatabaseExceptionMetadata metadata,
																				@Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 8177);
	}

	@NonNull
	@Override
	public Boolean isTimeout(@NonNull DatabaseExceptionMetadata metadata,
													 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isTimeout(metadata, cause) || hasErrorCode(metadata, cause, 1013, 51, 54, 30006);
	}

	@NonNull
	@Override
	public Boolean isTimestampWithTimeZone(@NonNull ResultSetMetaData resultSetMetaData,
																				 @NonNull Integer columnIndex) throws SQLException {
		requireNonNull(resultSetMetaData);
		requireNonNull(columnIndex);

		if (super.isTimestampWithTimeZone(resultSetMetaData, columnIndex))
			return true;

		String typeName = resultSetMetaData.getColumnTypeName(columnIndex);
		return typeName != null && typeName.toUpperCase(Locale.ROOT).contains("WITH LOCAL TIME ZONE");
	}
}
