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

import java.sql.Array;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * DuckDB-specific behavior.
 * <p>
 * DuckDB's JDBC driver reports neither SQLStates nor vendor error codes ({@code getSQLState()} is {@code null} and
 * {@code getErrorCode()} is {@code 0}), so exception classification matches the stable, documented message prefixes
 * DuckDB emits (for example {@code Constraint Error:} and {@code TransactionContext Error:}). Classification remains
 * best-effort: a value embedded in a message could in principle contain a matched phrase, which is acceptable for
 * these convenience predicates.
 */
final class DuckDbDialect extends GenericDialect {
	@NonNull
	static final DuckDbDialect INSTANCE = new DuckDbDialect();

	@NonNull
	private static final String DUCKDB_STRUCT_CLASS_NAME = "org.duckdb.DuckDBStruct";
	@NonNull
	private static final String DUCKDB_JSON_NODE_CLASS_NAME = "org.duckdb.JsonNode";
	@NonNull
	private static final String DUCKDB_PACKAGE_PREFIX = "org.duckdb.";

	// Lowercased fragments of DuckDB's diagnostic messages. The driver sometimes nests the originating
	// error inside a wrapper message (e.g. "Invalid Input Error: ... Error: Constraint Error: ..."), so
	// prefixes are matched with contains() rather than startsWith().
	@NonNull
	private static final String CONSTRAINT_ERROR_FRAGMENT = "constraint error:";
	@NonNull
	private static final String TRANSACTION_CONTEXT_ERROR_FRAGMENT = "transactioncontext error:";
	@NonNull
	private static final String PRIMARY_KEY_VIOLATION_FRAGMENT = "violates primary key constraint";
	@NonNull
	private static final String UNIQUE_VIOLATION_FRAGMENT = "violates unique constraint";
	@NonNull
	private static final String FOREIGN_KEY_VIOLATION_FRAGMENT = "violates foreign key constraint";
	@NonNull
	private static final String CONFLICT_FRAGMENT = "conflict";

	private DuckDbDialect() {}

	@Override
	public boolean supportsVector() {
		return true;
	}

	@Override
	public boolean supportsSetObjectWithTargetSqlType() {
		// The driver rejects setObject(int, Object, int targetSqlType) for temporal types with
		// "Unknown target type"; it binds java.time values natively via untyped setObject
		return false;
	}

	@Override
	public boolean bindsOffsetTimeAsString() {
		// The driver accepts setObject(OffsetTime) but fails at execute time with
		// "Invalid Input Error: Unsupported parameter type"; the ISO-8601 string form casts cleanly to TIMETZ
		return true;
	}

	@Override
	public void bindVector(@NonNull PreparedStatement preparedStatement,
												 @NonNull Integer parameterIndex,
												 double @Nullable [] elements,
												 @Nullable Integer fallbackSqlType) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);

		if (elements == null) {
			DefaultPreparedStatementBinder.setNullWithFallback(preparedStatement, parameterIndex, Types.ARRAY, null, fallbackSqlType);
			return;
		}

		Double[] boxedElements = new Double[elements.length];

		for (int i = 0; i < elements.length; i++)
			boxedElements[i] = elements[i];

		// Binds as a DOUBLE list; DuckDB implicitly casts lists to fixed-size ARRAY columns (e.g. FLOAT[3])
		// on INSERT. Comparison contexts such as array_cosine_distance(...) require an explicit cast of the
		// parameter, e.g. CAST(? AS FLOAT[3]).
		Array sqlArray = preparedStatement.getConnection().createArrayOf("DOUBLE", boxedElements);
		preparedStatement.setObject(parameterIndex, sqlArray);
	}

	@NonNull
	@Override
	public PreparedStatement prepareGeneratedKeysStatement(@NonNull Connection connection,
																												@NonNull StatementContext<?> statementContext,
																												@NonNull String @NonNull [] keyColumnNames) throws SQLException {
		requireNonNull(connection);
		requireNonNull(statementContext);
		requireNonNull(keyColumnNames);

		// DuckDB's JDBC driver rejects both RETURN_GENERATED_KEYS and key-column-name prepares with an
		// unhelpful SQLFeatureNotSupportedException; fail with actionable guidance instead.
		throw new DatabaseException(format("JDBC generated-key retrieval is not supported for %s.%s. "
						+ "Use DuckDB's INSERT ... RETURNING syntax with executeForObject(...) or executeForList(...) instead.",
				DatabaseType.class.getSimpleName(), DatabaseType.DUCK_DB.name()));
	}

	@Override
	public boolean isUniqueConstraintViolation(@NonNull DatabaseExceptionMetadata metadata,
																						 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isUniqueConstraintViolation(metadata, cause)
				|| hasDuckDbMessage(cause, CONSTRAINT_ERROR_FRAGMENT, PRIMARY_KEY_VIOLATION_FRAGMENT, UNIQUE_VIOLATION_FRAGMENT);
	}

	@Override
	public boolean isForeignKeyViolation(@NonNull DatabaseExceptionMetadata metadata,
																			 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isForeignKeyViolation(metadata, cause)
				|| hasDuckDbMessage(cause, CONSTRAINT_ERROR_FRAGMENT, FOREIGN_KEY_VIOLATION_FRAGMENT);
	}

	@Override
	public boolean isTransient(@NonNull DatabaseExceptionMetadata metadata,
														 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isTransient(metadata, cause) || isWriteWriteConflict(cause);
	}

	@Override
	public boolean isSerializationFailure(@NonNull DatabaseExceptionMetadata metadata,
																				@Nullable Throwable cause) {
		requireNonNull(metadata);

		// DuckDB uses optimistic concurrency control; a write-write conflict is its snapshot-isolation
		// equivalent of a serialization failure and is safely retryable.
		return super.isSerializationFailure(metadata, cause) || isWriteWriteConflict(cause);
	}

	@Nullable
	@Override
	public Object unwrapResultSetValue(@NonNull Object resultSetValue) {
		requireNonNull(resultSetValue);

		String className = resultSetValue.getClass().getName();

		// STRUCT values become insertion-ordered attribute-name -> value maps so downstream mapping (records,
		// Map rows) never sees driver-specific types. Attribute values are unwrapped recursively.
		if (DUCKDB_STRUCT_CLASS_NAME.equals(className) && resultSetValue instanceof Struct)
			return structAttributesByName(resultSetValue);

		if (DUCKDB_JSON_NODE_CLASS_NAME.equals(className))
			return resultSetValue.toString();

		if (resultSetValue instanceof Blob blob && className.startsWith(DUCKDB_PACKAGE_PREFIX))
			return extractBlobValue(blob);

		// Elements of an already-extracted LIST/ARRAY column (e.g. STRUCT[], JSON[]) may still be driver
		// objects; unwrap them so Map rows never carry driver-specific types. The original array is
		// returned untouched when no element needs unwrapping.
		if (resultSetValue instanceof Object[] elements)
			return unwrapArrayElements(elements);

		return resultSetValue;
	}

	private boolean isWriteWriteConflict(@Nullable Throwable cause) {
		return hasDuckDbMessage(cause, TRANSACTION_CONTEXT_ERROR_FRAGMENT, CONFLICT_FRAGMENT);
	}

	private boolean hasDuckDbMessage(@Nullable Throwable cause,
																	 @NonNull String prefixFragment,
																	 @NonNull String @NonNull ... phraseFragments) {
		requireNonNull(prefixFragment);
		requireNonNull(phraseFragments);

		return hasSqlException(cause, sqlException -> {
			String message = sqlException.getMessage();

			if (message == null)
				return false;

			String messageLowercase = message.toLowerCase(Locale.ROOT);

			if (!messageLowercase.contains(prefixFragment))
				return false;

			for (String phraseFragment : phraseFragments)
				if (messageLowercase.contains(phraseFragment))
					return true;

			return false;
		});
	}

	@NonNull
	private Map<String, Object> structAttributesByName(@NonNull Object duckDbStruct) {
		requireNonNull(duckDbStruct);

		Map<String, Object> attributesByName;

		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> extracted =
					(Map<String, Object>) duckDbStruct.getClass().getMethod("getMap").invoke(duckDbStruct);
			attributesByName = extracted;
		} catch (ReflectiveOperationException | RuntimeException e) {
			Throwable cause = e instanceof java.lang.reflect.InvocationTargetException ite ? ite.getTargetException() : e;
			throw new DatabaseException("Unable to extract DuckDB STRUCT attributes", cause);
		}

		Map<String, Object> unwrappedAttributesByName = new LinkedHashMap<>(attributesByName.size());

		for (Map.Entry<String, Object> entry : attributesByName.entrySet())
			unwrappedAttributesByName.put(entry.getKey(), unwrapStructAttributeValue(entry.getValue()));

		return unwrappedAttributesByName;
	}

	@NonNull
	private Object[] unwrapArrayElements(@NonNull Object @NonNull [] elements) {
		requireNonNull(elements);

		Object[] unwrappedElements = null;

		for (int i = 0; i < elements.length; i++) {
			Object unwrappedElement = unwrapStructAttributeValue(elements[i]);

			if (unwrappedElements == null && unwrappedElement != elements[i]) {
				unwrappedElements = new Object[elements.length];
				System.arraycopy(elements, 0, unwrappedElements, 0, i);
			}

			if (unwrappedElements != null)
				unwrappedElements[i] = unwrappedElement;
		}

		return unwrappedElements == null ? elements : unwrappedElements;
	}

	@Nullable
	private Object unwrapStructAttributeValue(@Nullable Object attributeValue) {
		if (attributeValue == null)
			return null;

		// Nested lists inside STRUCT attributes are extracted here because no downstream code path would
		// otherwise call getArray() on them.
		if (attributeValue instanceof Array sqlArray && attributeValue.getClass().getName().startsWith(DUCKDB_PACKAGE_PREFIX))
			return extractArrayValue(sqlArray);

		return unwrapResultSetValue(attributeValue);
	}

	@Nullable
	private Object extractArrayValue(@NonNull Array sqlArray) {
		requireNonNull(sqlArray);

		Object arrayValue;
		Throwable thrown = null;

		try {
			arrayValue = sqlArray.getArray();
		} catch (SQLException e) {
			thrown = e;
			throw new DatabaseException("Unable to extract DuckDB STRUCT list attribute", e);
		} finally {
			try {
				sqlArray.free();
			} catch (SQLException e) {
				if (thrown != null)
					thrown.addSuppressed(e);
				else
					throw new DatabaseException("Unable to release DuckDB STRUCT list attribute", e);
			}
		}

		if (arrayValue instanceof Object[] elements) {
			Object[] unwrappedElements = new Object[elements.length];

			for (int i = 0; i < elements.length; i++)
				unwrappedElements[i] = unwrapStructAttributeValue(elements[i]);

			return unwrappedElements;
		}

		return arrayValue;
	}

	private byte @NonNull [] extractBlobValue(@NonNull Blob blob) {
		requireNonNull(blob);

		Throwable thrown = null;

		try {
			long length = blob.length();

			if (length > Integer.MAX_VALUE)
				throw new DatabaseException(format("DuckDB BLOB value of length %d exceeds the maximum supported size", length));

			return blob.getBytes(1, (int) length);
		} catch (SQLException e) {
			thrown = e;
			throw new DatabaseException("Unable to extract DuckDB BLOB value", e);
		} catch (DatabaseException e) {
			thrown = e;
			throw e;
		} finally {
			try {
				blob.free();
			} catch (SQLException e) {
				if (thrown != null)
					thrown.addSuppressed(e);
				else
					throw new DatabaseException("Unable to release DuckDB BLOB value", e);
			}
		}
	}
}
