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

	@Override
	public boolean supportsSqlArray() {
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

	@Override
	public boolean isUniqueConstraintViolation(@NonNull DatabaseExceptionMetadata metadata,
																						 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 1);
	}

	@Override
	public boolean isForeignKeyViolation(@NonNull DatabaseExceptionMetadata metadata,
																			 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 2291, 2292);
	}

	@Override
	public boolean isDeadlock(@NonNull DatabaseExceptionMetadata metadata,
														@Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 60);
	}

	@Override
	public boolean isTransient(@NonNull DatabaseExceptionMetadata metadata,
														 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isTransient(metadata, cause) || hasErrorCode(metadata, cause, 60, 8177);
	}

	@Override
	public boolean isSerializationFailure(@NonNull DatabaseExceptionMetadata metadata,
																				@Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 8177);
	}

	@Override
	public boolean isTimeout(@NonNull DatabaseExceptionMetadata metadata,
													 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isTimeout(metadata, cause) || hasErrorCode(metadata, cause, 1013, 51);
	}

	@Override
	public boolean isTimestampWithTimeZone(@NonNull ResultSetMetaData resultSetMetaData,
																				 @NonNull Integer columnIndex) throws SQLException {
		requireNonNull(resultSetMetaData);
		requireNonNull(columnIndex);

		if (super.isTimestampWithTimeZone(resultSetMetaData, columnIndex))
			return true;

		String typeName = resultSetMetaData.getColumnTypeName(columnIndex);
		return typeName != null && typeName.toUpperCase(Locale.ROOT).contains("WITH LOCAL TIME ZONE");
	}
}
