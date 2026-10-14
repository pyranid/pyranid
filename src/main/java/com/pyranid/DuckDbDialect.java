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

import java.math.BigInteger;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
	private static final String UNIQUE_INDEX_DUPLICATES_FRAGMENT = "data contains duplicates on indexed column";
	@NonNull
	private static final String FOREIGN_KEY_VIOLATION_FRAGMENT = "violates foreign key constraint";
	@NonNull
	private static final String CONFLICT_FRAGMENT = "conflict";
	@NonNull
	private static final Set<String> NON_TEMPORAL_CONTAINER_TYPE_NAMES = Set.of(
			"ANY", "BIGINT", "BIGNUM", "BINARY", "BIT", "BITSTRING", "BLOB", "BOOL", "BOOLEAN",
			"BPCHAR", "BYTEA", "CHAR", "DATE", "DEC", "DECIMAL", "DOUBLE", "ENUM", "FLOAT", "FLOAT4",
			"FLOAT8", "HUGEINT", "INT", "INT1", "INT2", "INT4", "INT8", "INTEGER", "INTERVAL", "JSON",
			"LIST", "LOGICAL", "LONG", "MAP", "NUMERIC", "REAL", "SHORT", "SIGNED", "SMALLINT", "STRING",
			"STRUCT", "TEXT", "TIME", "TIMETZ", "TINYINT", "UBIGINT", "UHUGEINT", "UINT16", "UINT32",
			"UINT64", "UINT8", "UINTEGER", "UNION", "USMALLINT", "UTINYINT", "UUID", "VARBINARY", "VARCHAR");

	private DuckDbDialect() {}

	@NonNull
	@Override
	public Boolean supportsVector() {
		return true;
	}

	@NonNull
	@Override
	public Boolean supportsSqlStruct() {
		return true;
	}

	@NonNull
	@Override
	public Boolean supportsSetObjectWithTargetSqlType() {
		// The driver rejects setObject(int, Object, int targetSqlType) for temporal types with
		// "Unknown target type"; it binds java.time values natively via untyped setObject
		return false;
	}

	@NonNull
	@Override
	public Boolean bindsOffsetTimeAsString() {
		// The driver accepts setObject(OffsetTime) but fails at execute time with
		// "Invalid Input Error: Unsupported parameter type"; the ISO-8601 string form casts cleanly to TIMETZ
		return true;
	}

	@NonNull
	@Override
	public Object normalizeSqlArrayElement(@NonNull String baseTypeName,
																				 @NonNull Object element,
																				 @NonNull ZoneId timeZone) {
		requireNonNull(baseTypeName);
		requireNonNull(element);
		requireNonNull(timeZone);

		return normalizeContainerElement(baseTypeName, element, timeZone);
	}

	@NonNull
	@Override
	public Object normalizeSqlStructAttribute(@NonNull String typeName,
																														@NonNull Integer attributeIndex,
																														@NonNull Object attribute,
																														@NonNull ZoneId timeZone) {
		requireNonNull(typeName);
		requireNonNull(attributeIndex);
		requireNonNull(attribute);
		requireNonNull(timeZone);

		String attributeTypeName = structAttributeTypeName(typeName, attributeIndex);
		String targetDescription = attributeTypeName == null
				? format("attribute %s of STRUCT type '%s'", attributeIndex + 1, typeName)
				: format("STRUCT attribute type '%s'", attributeTypeName);
		return normalizeContainerElement(attributeTypeName, targetDescription, attribute, timeZone);
	}

	@NonNull
	private Object normalizeContainerElement(@NonNull String targetTypeName,
																 @NonNull Object element,
																 @NonNull ZoneId timeZone) {
		requireNonNull(targetTypeName);
		return normalizeContainerElement(targetTypeName, format("ARRAY element type '%s'", targetTypeName),
				element, timeZone);
	}

	@NonNull
	private Object normalizeContainerElement(@Nullable String targetTypeName,
																 @NonNull String targetDescription,
																 @NonNull Object element,
																 @NonNull ZoneId timeZone) {
		requireNonNull(targetDescription);
		requireNonNull(element);
		requireNonNull(timeZone);

		// DuckDB's driver accepts these values as scalar parameters but rejects them inside
		// createArrayOf(...)/createStruct(...) with "Unsupported parameter type". Supply an equivalent
		// form while preserving Pyranid's scalar timestamp-zone semantics.
		if (element instanceof BigInteger || element instanceof OffsetTime)
			return element.toString();

		Instant instant;
		if (element instanceof Instant instantValue) {
			instant = instantValue;
		} else if (element instanceof OffsetDateTime offsetDateTime) {
			instant = offsetDateTime.toInstant();
		} else {
			return element;
		}

		return switch (classifyContainerTargetType(targetTypeName)) {
			case TIMESTAMP_WITHOUT_TIME_ZONE -> LocalDateTime.ofInstant(instant, timeZone);
			case TIMESTAMP_WITH_TIME_ZONE -> OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
			case NON_TEMPORAL -> element.toString();
			case UNKNOWN -> throw new IllegalArgumentException(format(
					"Cannot safely bind %s to DuckDB %s because its timestamp semantics are unknown; "
							+ "use an explicit TIMESTAMP or TIMESTAMPTZ type declaration",
					element.getClass().getSimpleName(), targetDescription));
		};
	}

	@NonNull
	private ContainerTargetType classifyContainerTargetType(@Nullable String targetTypeName) {
		if (targetTypeName == null || targetTypeName.isBlank())
			return ContainerTargetType.UNKNOWN;

		String normalizedTypeName = targetTypeName.trim().toUpperCase(Locale.ROOT)
				.replaceAll("\\s+", " ");
		if (normalizedTypeName.equals("TIMESTAMPTZ")
				|| normalizedTypeName.matches("TIMESTAMP(?:\\(\\d+\\))? WITH (?:LOCAL )?TIME ZONE"))
			return ContainerTargetType.TIMESTAMP_WITH_TIME_ZONE;
		if (normalizedTypeName.equals("DATETIME")
				|| normalizedTypeName.matches("TIMESTAMP(?:_S|_MS|_NS|\\(\\d+\\))?(?: WITHOUT TIME ZONE)?"))
			return ContainerTargetType.TIMESTAMP_WITHOUT_TIME_ZONE;

		int typeNameEnd = normalizedTypeName.length();
		for (int i = 0; i < normalizedTypeName.length(); i++) {
			char c = normalizedTypeName.charAt(i);
			if (c == '(' || c == '[' || Character.isWhitespace(c)) {
				typeNameEnd = i;
				break;
			}
		}
		String rootTypeName = normalizedTypeName.substring(0, typeNameEnd);
		return NON_TEMPORAL_CONTAINER_TYPE_NAMES.contains(rootTypeName)
				? ContainerTargetType.NON_TEMPORAL
				: ContainerTargetType.UNKNOWN;
	}

	private enum ContainerTargetType {
		TIMESTAMP_WITH_TIME_ZONE,
		TIMESTAMP_WITHOUT_TIME_ZONE,
		NON_TEMPORAL,
		UNKNOWN
	}

	@Nullable
	private String structAttributeTypeName(@NonNull String structTypeName,
																													 @NonNull Integer attributeIndex) {
		requireNonNull(structTypeName);
		requireNonNull(attributeIndex);

		String trimmedTypeName = structTypeName.trim();
		if (!trimmedTypeName.regionMatches(true, 0, "STRUCT", 0, "STRUCT".length()))
			return null;

		int openParenthesisIndex = trimmedTypeName.indexOf('(');
		int closeParenthesisIndex = trimmedTypeName.lastIndexOf(')');
		if (openParenthesisIndex < 0 || closeParenthesisIndex <= openParenthesisIndex)
			return null;

		List<String> attributeDeclarations = splitTopLevelTypeList(
				trimmedTypeName.substring(openParenthesisIndex + 1, closeParenthesisIndex));
		if (attributeIndex < 0 || attributeIndex >= attributeDeclarations.size())
			return null;

		String declaration = attributeDeclarations.get(attributeIndex).trim();
		boolean quoted = false;
		for (int i = 0; i < declaration.length(); i++) {
			char c = declaration.charAt(i);
			if (c == '"') {
				if (quoted && i + 1 < declaration.length() && declaration.charAt(i + 1) == '"') {
					i++;
					continue;
				}
				quoted = !quoted;
				continue;
			}
			if (!quoted && Character.isWhitespace(c)) {
				String attributeTypeName = declaration.substring(i).trim();
				return attributeTypeName.isEmpty() ? null : attributeTypeName;
			}
		}

		return null;
	}

	@NonNull
	private List<String> splitTopLevelTypeList(@NonNull String typeList) {
		requireNonNull(typeList);

		List<String> elements = new ArrayList<>();
		int elementStart = 0;
		int parenthesisDepth = 0;
		boolean quoted = false;

		for (int i = 0; i < typeList.length(); i++) {
			char c = typeList.charAt(i);
			if (c == '"') {
				if (quoted && i + 1 < typeList.length() && typeList.charAt(i + 1) == '"') {
					i++;
					continue;
				}
				quoted = !quoted;
				continue;
			}
			if (quoted)
				continue;
			if (c == '(') {
				parenthesisDepth++;
			} else if (c == ')') {
				parenthesisDepth--;
			} else if (c == ',' && parenthesisDepth == 0) {
				elements.add(typeList.substring(elementStart, i));
				elementStart = i + 1;
			}
		}

		elements.add(typeList.substring(elementStart));
		return elements;
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

	@NonNull
	@Override
	public Boolean isUniqueConstraintViolation(@NonNull DatabaseExceptionMetadata metadata,
																						 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isUniqueConstraintViolation(metadata, cause)
				|| hasDuckDbMessage(cause, CONSTRAINT_ERROR_FRAGMENT, PRIMARY_KEY_VIOLATION_FRAGMENT,
				UNIQUE_VIOLATION_FRAGMENT, UNIQUE_INDEX_DUPLICATES_FRAGMENT);
	}

	@NonNull
	@Override
	public Boolean isForeignKeyViolation(@NonNull DatabaseExceptionMetadata metadata,
																			 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isForeignKeyViolation(metadata, cause)
				|| hasDuckDbMessage(cause, CONSTRAINT_ERROR_FRAGMENT, FOREIGN_KEY_VIOLATION_FRAGMENT);
	}

	@NonNull
	@Override
	public Boolean isTransient(@NonNull DatabaseExceptionMetadata metadata,
														 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isTransient(metadata, cause) || isWriteWriteConflict(cause);
	}

	@NonNull
	@Override
	public Boolean isSerializationFailure(@NonNull DatabaseExceptionMetadata metadata,
																				@Nullable Throwable cause) {
		requireNonNull(metadata);

		// DuckDB uses optimistic concurrency control; a write-write conflict is its snapshot-isolation
		// equivalent of a serialization failure and is safely retryable.
		return super.isSerializationFailure(metadata, cause) || isWriteWriteConflict(cause);
	}

	@Nullable
	@Override
	public Object unwrapResultSetValue(@NonNull Object resultSetValue) {
		return unwrapResultSetValue(resultSetValue, true);
	}

	/** Typed mapping owns bounded LOB materialization; native Map/Object unwrapping retains its contract. */
	@Nullable
	Object unwrapForTypedMapping(@NonNull Object resultSetValue) {
		return unwrapResultSetValue(resultSetValue, false);
	}

	@Nullable
	private Object unwrapResultSetValue(@NonNull Object resultSetValue, boolean materializeLobs) {
		requireNonNull(resultSetValue);

		String className = resultSetValue.getClass().getName();

		// STRUCT values become insertion-ordered attribute-name -> value maps so downstream mapping (records,
		// Map rows) never sees driver-specific types. Attribute values are unwrapped recursively.
		if (DUCKDB_STRUCT_CLASS_NAME.equals(className) && resultSetValue instanceof Struct)
			return structAttributesByName(resultSetValue, materializeLobs);

		if (DUCKDB_JSON_NODE_CLASS_NAME.equals(className))
			return resultSetValue.toString();

		if (materializeLobs && resultSetValue instanceof Blob blob && className.startsWith(DUCKDB_PACKAGE_PREFIX))
			return extractBlobValue(blob);

		// Elements of an already-extracted LIST/ARRAY column (e.g. STRUCT[], JSON[]) may still be driver
		// objects; unwrap them so Map rows never carry driver-specific types. The original array is
		// returned untouched when no element needs unwrapping.
		if (resultSetValue instanceof Object[] elements)
			return unwrapArrayElements(elements, materializeLobs);

		if (resultSetValue instanceof List<?> elements)
			return unwrapListElements(elements, materializeLobs);

		if (resultSetValue instanceof Map<?, ?> entries)
			return unwrapMapEntries(entries, materializeLobs);

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
	private Map<String, Object> structAttributesByName(@NonNull Object duckDbStruct, boolean materializeLobs) {
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
			unwrappedAttributesByName.put(entry.getKey(), unwrapStructAttributeValue(entry.getValue(), materializeLobs));

		return unwrappedAttributesByName;
	}

	@NonNull
	private Object[] unwrapArrayElements(@NonNull Object @NonNull [] elements, boolean materializeLobs) {
		requireNonNull(elements);

		Object[] unwrappedElements = null;

		for (int i = 0; i < elements.length; i++) {
			Object unwrappedElement = unwrapStructAttributeValue(elements[i], materializeLobs);

			if (unwrappedElements == null && unwrappedElement != elements[i]) {
				unwrappedElements = new Object[elements.length];
				System.arraycopy(elements, 0, unwrappedElements, 0, i);
			}

			if (unwrappedElements != null)
				unwrappedElements[i] = unwrappedElement;
		}

		return unwrappedElements == null ? elements : unwrappedElements;
	}

	@NonNull
	private List<?> unwrapListElements(@NonNull List<?> elements, boolean materializeLobs) {
		requireNonNull(elements);

		List<Object> unwrappedElements = new ArrayList<>(elements.size());
		boolean changed = false;

		for (Object element : elements) {
			Object unwrappedElement = unwrapStructAttributeValue(element, materializeLobs);
			unwrappedElements.add(unwrappedElement);
			changed |= unwrappedElement != element;
		}

		return changed ? unwrappedElements : elements;
	}

	@NonNull
	private Map<?, ?> unwrapMapEntries(@NonNull Map<?, ?> entries, boolean materializeLobs) {
		requireNonNull(entries);

		Map<Object, Object> unwrappedEntries = new LinkedHashMap<>(entries.size());
		boolean changed = false;

		for (Map.Entry<?, ?> entry : entries.entrySet()) {
			Object key = entry.getKey();
			Object value = entry.getValue();
			Object unwrappedKey = unwrapStructAttributeValue(key, materializeLobs);
			Object unwrappedValue = unwrapStructAttributeValue(value, materializeLobs);
			unwrappedEntries.put(unwrappedKey, unwrappedValue);
			changed |= unwrappedKey != key || unwrappedValue != value;
		}

		return changed ? unwrappedEntries : entries;
	}

	@Nullable
	private Object unwrapStructAttributeValue(@Nullable Object attributeValue, boolean materializeLobs) {
		if (attributeValue == null)
			return null;

		// Nested lists inside STRUCT attributes are extracted here because no downstream code path would
		// otherwise call getArray() on them.
		if (attributeValue instanceof Array sqlArray && attributeValue.getClass().getName().startsWith(DUCKDB_PACKAGE_PREFIX))
			return extractArrayValue(sqlArray, materializeLobs);

		return unwrapResultSetValue(attributeValue, materializeLobs);
	}

	@Nullable
	private Object extractArrayValue(@NonNull Array sqlArray, boolean materializeLobs) {
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
				unwrappedElements[i] = unwrapStructAttributeValue(elements[i], materializeLobs);

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
