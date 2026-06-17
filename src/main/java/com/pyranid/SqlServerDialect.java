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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

/**
 * SQL Server-specific behavior.
 */
final class SqlServerDialect extends UuidStringDialect {
	@NonNull
	static final SqlServerDialect INSTANCE = new SqlServerDialect();

	private SqlServerDialect() {}

	@Override
	public boolean isUniqueConstraintViolation(@NonNull DatabaseExceptionMetadata metadata,
																						 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 2627, 2601);
	}

	@Override
	public boolean isDeadlock(@NonNull DatabaseExceptionMetadata metadata,
														@Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 1205);
	}

	@Override
	public boolean isTransient(@NonNull DatabaseExceptionMetadata metadata,
														 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isTransient(metadata, cause) || hasErrorCode(metadata, cause, 1205, 1222);
	}

	@Override
	public boolean isTimestampWithTimeZone(@NonNull ResultSetMetaData resultSetMetaData,
																				 @NonNull Integer columnIndex) throws SQLException {
		requireNonNull(resultSetMetaData);
		requireNonNull(columnIndex);

		if (super.isTimestampWithTimeZone(resultSetMetaData, columnIndex))
			return true;

		String typeName = resultSetMetaData.getColumnTypeName(columnIndex);
		return typeName != null && typeName.toUpperCase(Locale.ROOT).contains("DATETIMEOFFSET");
	}
}
