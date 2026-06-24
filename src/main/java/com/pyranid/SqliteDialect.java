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

import java.sql.SQLException;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * SQLite-specific behavior.
 */
final class SqliteDialect extends UuidStringDialect {
	@NonNull
	static final SqliteDialect INSTANCE = new SqliteDialect();
	@NonNull
	private static final Set<String> UNIQUE_CONSTRAINT_RESULT_CODES = Set.of(
			"SQLITE_CONSTRAINT_PRIMARYKEY",
			"SQLITE_CONSTRAINT_UNIQUE");
	@NonNull
	private static final Set<String> FOREIGN_KEY_RESULT_CODES = Set.of("SQLITE_CONSTRAINT_FOREIGNKEY");
	@NonNull
	private static final Set<String> TRANSIENT_RESULT_CODES = Set.of(
			"SQLITE_BUSY",
			"SQLITE_BUSY_RECOVERY",
			"SQLITE_BUSY_SNAPSHOT",
			"SQLITE_BUSY_TIMEOUT",
			"SQLITE_LOCKED",
			"SQLITE_LOCKED_SHAREDCACHE",
			"SQLITE_LOCKED_VTAB");
	@NonNull
	private static final Set<String> TIMEOUT_RESULT_CODES = Set.of(
			"SQLITE_BUSY",
			"SQLITE_BUSY_RECOVERY",
			"SQLITE_BUSY_SNAPSHOT",
			"SQLITE_BUSY_TIMEOUT",
			"SQLITE_LOCKED",
			"SQLITE_LOCKED_SHAREDCACHE",
			"SQLITE_LOCKED_VTAB");

	private SqliteDialect() {}

	@Override
	public boolean isUniqueConstraintViolation(@NonNull DatabaseExceptionMetadata metadata,
																						 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isUniqueConstraintViolation(metadata, cause)
				|| hasSqliteResultCode(cause, UNIQUE_CONSTRAINT_RESULT_CODES);
	}

	@Override
	public boolean isForeignKeyViolation(@NonNull DatabaseExceptionMetadata metadata,
																			 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isForeignKeyViolation(metadata, cause)
				|| hasSqliteResultCode(cause, FOREIGN_KEY_RESULT_CODES);
	}

	@Override
	public boolean isTransient(@NonNull DatabaseExceptionMetadata metadata,
														 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isTransient(metadata, cause)
				|| hasErrorCode(metadata, cause, 5, 6)
				|| hasSqliteResultCode(cause, TRANSIENT_RESULT_CODES);
	}

	@Override
	public boolean isSerializationFailure(@NonNull DatabaseExceptionMetadata metadata,
																				@Nullable Throwable cause) {
		requireNonNull(metadata);

		return false;
	}

	@Override
	public boolean isTimeout(@NonNull DatabaseExceptionMetadata metadata,
													 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isTimeout(metadata, cause)
				|| hasErrorCode(metadata, cause, 5, 6)
				|| hasSqliteResultCode(cause, TIMEOUT_RESULT_CODES);
	}

	private boolean hasSqliteResultCode(@Nullable Throwable cause,
																			@NonNull Set<String> resultCodeNames) {
		requireNonNull(resultCodeNames);

		return hasSqlException(cause, sqlException -> {
			String resultCodeName = sqliteResultCodeName(sqlException);
			return resultCodeName != null && resultCodeNames.contains(resultCodeName);
		});
	}

	@Nullable
	private String sqliteResultCodeName(@NonNull SQLException sqlException) {
		requireNonNull(sqlException);

		if (!"org.sqlite.SQLiteException".equals(sqlException.getClass().getName()))
			return null;

		try {
			Object resultCode = sqlException.getClass().getMethod("getResultCode").invoke(sqlException);

			if (resultCode instanceof Enum<?> resultCodeEnum)
				return resultCodeEnum.name();

			return resultCode == null ? null : resultCode.toString();
		} catch (ReflectiveOperationException | RuntimeException e) {
			return null;
		}
	}
}
