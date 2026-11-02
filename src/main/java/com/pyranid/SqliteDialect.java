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

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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

	/** Reads the existing SQLite connection's timestamp format without acquiring or closing a JDBC resource. */
	@Nullable
	static String configuredDateStringFormat(@NonNull ResultSet resultSet) throws SQLException {
		requireNonNull(resultSet);
		try {
			Statement statement = resultSet.getStatement();
			if (statement == null) return null;
			Connection connection = statement.getConnection();
			if (connection == null) return null;

			Class<?> nativeType = null;
			for (Class<?> type = connection.getClass(); type != null; type = type.getSuperclass()) {
				if (type.getName().equals("org.sqlite.SQLiteConnection")) {
					nativeType = type;
					break;
				}
			}
			Object nativeConnection = nativeType == null ? null : connection;
			if (nativeType == null) {
				for (ClassLoader loader : sqliteClassLoaders(connection)) {
					Class<?> candidate;
					try {
						candidate = Class.forName("org.sqlite.SQLiteConnection", false, loader);
					} catch (ClassNotFoundException unavailable) {
						if (JdbcFallbacks.isTerminal(unavailable)) throw sqliteFormatFailure(unavailable);
						continue;
					}
					try {
						if (!connection.isWrapperFor(candidate)) continue;
						Object unwrapped = connection.unwrap(candidate);
						if (!candidate.isInstance(unwrapped)) continue;
						nativeType = candidate;
						nativeConnection = unwrapped;
						break;
					} catch (SQLException | UnsupportedOperationException | AbstractMethodError unavailable) {
						if (JdbcFallbacks.isTerminal(unavailable)) throw sqliteFormatFailure(unavailable);
					}
				}
			}
			if (nativeType == null) return null;
			java.lang.reflect.Method configReader = nativeType.getMethod("getConnectionConfig");
			Object config = configReader.invoke(nativeConnection);
			if (config == null) return null;
			java.lang.reflect.Method formatReader = configReader.getReturnType().getMethod("getDateStringFormat");
			if (formatReader.getReturnType() != String.class) return null;
			return (String) formatReader.invoke(config);
		} catch (InvocationTargetException reflected) {
			Throwable target = reflected.getTargetException();
			if ((target instanceof UnsupportedOperationException || target instanceof AbstractMethodError)
					&& !JdbcFallbacks.isTerminal(target)) return null;
			throw sqliteFormatFailure(target);
		} catch (NoSuchMethodException | IllegalAccessException | SecurityException
				| SQLFeatureNotSupportedException | UnsupportedOperationException | AbstractMethodError unavailable) {
			if (JdbcFallbacks.isTerminal(unavailable)) throw sqliteFormatFailure(unavailable);
			return null;
		}
	}

	private static List<ClassLoader> sqliteClassLoaders(Connection connection) throws SQLException {
		List<ClassLoader> loaders = new ArrayList<>(3);
		for (ClassLoader loader : new ClassLoader[]{connection.getClass().getClassLoader(), SqliteDialect.class.getClassLoader()})
			if (loader != null && !loaders.contains(loader)) loaders.add(loader);
		try {
			ClassLoader context = Thread.currentThread().getContextClassLoader();
			if (context != null && !loaders.contains(context)) loaders.add(context);
		} catch (SecurityException unavailable) {
			if (JdbcFallbacks.isTerminal(unavailable)) throw sqliteFormatFailure(unavailable);
		}
		return loaders;
	}

	private static SQLException sqliteFormatFailure(Throwable failure) throws SQLException {
		if (failure instanceof SQLException sqlFailure) throw sqlFailure;
		if (failure instanceof RuntimeException runtimeFailure) throw runtimeFailure;
		if (failure instanceof Error error) throw error;
		return new SQLException("Unable to inspect SQLite timestamp format", failure);
	}

	@NonNull
	@Override
	public Boolean isUniqueConstraintViolation(@NonNull DatabaseExceptionMetadata metadata,
																						 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isUniqueConstraintViolation(metadata, cause)
				|| hasSqliteResultCode(cause, UNIQUE_CONSTRAINT_RESULT_CODES);
	}

	@NonNull
	@Override
	public Boolean isForeignKeyViolation(@NonNull DatabaseExceptionMetadata metadata,
																			 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isForeignKeyViolation(metadata, cause)
				|| hasSqliteResultCode(cause, FOREIGN_KEY_RESULT_CODES);
	}

	@NonNull
	@Override
	public Boolean isTransient(@NonNull DatabaseExceptionMetadata metadata,
														 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isTransient(metadata, cause)
				|| hasErrorCode(metadata, cause, 5, 6)
				|| hasSqliteResultCode(cause, TRANSIENT_RESULT_CODES);
	}

	@NonNull
	@Override
	public Boolean isSerializationFailure(@NonNull DatabaseExceptionMetadata metadata,
																				@Nullable Throwable cause) {
		requireNonNull(metadata);

		return false;
	}

	@NonNull
	@Override
	public Boolean isTimeout(@NonNull DatabaseExceptionMetadata metadata,
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
