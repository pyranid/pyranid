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

import com.pyranid.CustomParameterBinder.BindingResult;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.ThreadSafe;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Basic implementation of {@link PreparedStatementBinder}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
class DefaultPreparedStatementBinder implements PreparedStatementBinder {
	private static final int MAX_SQL_CONTAINER_NESTING_DEPTH = 64;
	private static final int[] NO_CUSTOM_PARAMETER_BINDERS = new int[0];
	@NonNull
	private final List<CustomParameterBinder> customParameterBinders;

	// Cache indices rather than callbacks, which can refer back to this binder or its Database.
	@NonNull
	private final ClassValue<int[]> bindersByRawClassCache =
			new ClassValue<>() {
				@Override
				protected int[] computeValue(Class<?> type) {
					return computeCustomParameterBinderIndicesFor(TargetType.of(type));
				}
			};

	DefaultPreparedStatementBinder() {
		this(List.of());
	}

	DefaultPreparedStatementBinder(@NonNull List<CustomParameterBinder> customParameterBinders) {
		requireNonNull(customParameterBinders);
		this.customParameterBinders = List.copyOf(customParameterBinders);
	}

	@Override
	public <T> void bindParameter(@NonNull StatementContext<T> statementContext,
																@NonNull PreparedStatement preparedStatement,
																@NonNull Integer parameterIndex,
																@NonNull Object parameter) throws SQLException {
		ParameterSqlTypeResolver resolver = ParameterBindingContext.current(statementContext, preparedStatement);
		bindParameter(statementContext, preparedStatement, parameterIndex, parameter,
				resolver == null ? new ParameterSqlTypeResolver(statementContext, preparedStatement) : resolver);
	}

	<T> void bindParameter(@NonNull StatementContext<T> statementContext,
												 @NonNull PreparedStatement preparedStatement,
												 @NonNull Integer parameterIndex,
												 @NonNull Object parameter,
												 @NonNull ParameterSqlTypeResolver parameterSqlTypeResolver) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(parameter);
		requireNonNull(parameterSqlTypeResolver);

		@Nullable Object effectiveParameter = SecureParameterSupport.unwrapSecureAndOptionalParameter(parameter);

		TargetType explicitTargetType = (effectiveParameter instanceof TypedParameter typedParameter)
				? TargetType.of(typedParameter.getExplicitType())
				: null;

		// Unwrap typed parameters if needed
		Object rawParameter = (effectiveParameter instanceof TypedParameter typedParameter) ? typedParameter.getValue().orElse(null) : effectiveParameter;
		Object unwrappedParameter = unwrapOptionalValue(rawParameter);

		if (unwrappedParameter == null) {
			Optional<ParameterSqlType> sqlTypeOptional = parameterSqlTypeResolver.determineNullParameterSqlType(parameterIndex);
			Integer sqlType = sqlTypeOptional.map(ParameterSqlType::getSqlType).orElse(null);
			if (sqlType == null || sqlType == Types.NULL) {
				sqlType = explicitTargetType == null
						? Types.NULL
						: defaultNullSqlTypeForTargetType(explicitTargetType);
			}

			if (explicitTargetType != null
					&& !getCustomParameterBinders().isEmpty()
					&& tryCustomNullBinders(statementContext, preparedStatement, parameterIndex, sqlType, explicitTargetType))
				return;

			if (effectiveParameter instanceof TypedParameter typedParameter)
				throw new DatabaseException(format("This parameter requires a CustomParameterBinder (even when null): %s. "
								+ "Parameters.listOf/setOf/mapOf/arrayOf(Class, ...) are typed wrappers intended for use with custom binders. "
								+ "Register a CustomParameterBinder that appliesTo(%s), "
								+ "or use a concrete parameter type supported by the binder/driver (e.g. Parameters.sqlArrayOf(...) or Parameters.json(...)).",
						typedParameter.getExplicitType(), typedParameter.getExplicitType()));

			preparedStatement.setNull(parameterIndex, sqlType);

			return;
		}

		if (explicitTargetType == null) {
			Class<?> parameterClass = unwrappedParameter instanceof Enum<?> enumValue
					? enumValue.getDeclaringClass() : unwrappedParameter.getClass();
			explicitTargetType = TargetType.of(parameterClass);
		}

		// Try custom binders first (if they exist) on the unwrapped value
		if (!getCustomParameterBinders().isEmpty() && !customBindersFor(explicitTargetType).isEmpty()) {
			if (tryCustomBinders(statementContext, preparedStatement, parameterIndex, unwrappedParameter, explicitTargetType))
				return;
		}

		// If this parameter was explicitly wrapped as a TypedParameter (e.g., Parameters.listOf/setOf/mapOf/arrayOf(Class, ...))
		// and no CustomParameterBinder claimed it, fail fast with a clear message.
		if (effectiveParameter instanceof TypedParameter typedParameter)
			throw new DatabaseException(format("This parameter requires a CustomParameterBinder: %s. "
							+ "Parameters.listOf/setOf/mapOf/arrayOf(Class, ...) are typed wrappers intended for use with custom binders. "
							+ "Register a CustomParameterBinder that appliesTo(%s), "
							+ "or use a concrete parameter type supported by the binder/driver (e.g. Parameters.sqlArrayOf(...) or Parameters.json(...)).",
					typedParameter.getExplicitType(), typedParameter.getExplicitType()));

		Object normalizedParameter;
		if (unwrappedParameter instanceof UUID uuid && statementContext.getDatabaseDialect().requiresUuidParameterSqlType()) {
			Integer sqlType = determineParameterSqlType(parameterSqlTypeResolver, parameterIndex)
					.map(ParameterSqlType::getSqlType).orElse(null);
			normalizedParameter = statementContext.getDatabaseDialect().normalizeUuid(uuid, sqlType);
		} else {
			normalizedParameter = normalizeParameter(statementContext, unwrappedParameter);
		}

		if (normalizedParameter instanceof LocalDate localDate) {
			if (requiresLocalTemporalSqlType(statementContext) && determineTemporalParameterSqlType(parameterSqlTypeResolver, parameterIndex, true)
					.map(ParameterSqlType::isTimestampWithTimeZone).orElse(false)) {
				OffsetDateTime zoned = localDate.atStartOfDay(statementContext.getTimeZone()).toOffsetDateTime();
				if (!trySetTemporalObject(statementContext, preparedStatement, parameterIndex, zoned, Types.TIMESTAMP_WITH_TIMEZONE))
					preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(zoned.toInstant()));
				return;
			}
			if (!trySetTemporalObject(statementContext, preparedStatement, parameterIndex, localDate, Types.DATE))
				preparedStatement.setDate(parameterIndex, java.sql.Date.valueOf(localDate)); // fallback

			return;
		}

		if (normalizedParameter instanceof LocalTime localTime) {
			if (!trySetTemporalObject(statementContext, preparedStatement, parameterIndex, localTime, Types.TIME)) {
				if (localTime.getNano() == 0)
					preparedStatement.setTime(parameterIndex, java.sql.Time.valueOf(localTime));
				else
					preparedStatement.setString(parameterIndex, localTime.toString());
			}

			return;
		}

		if (normalizedParameter instanceof LocalDateTime localDateTime) {
			if (requiresLocalTemporalSqlType(statementContext) && determineTemporalParameterSqlType(parameterSqlTypeResolver, parameterIndex, true)
					.map(ParameterSqlType::isTimestampWithTimeZone).orElse(false)) {
				OffsetDateTime zoned = localDateTime.atZone(statementContext.getTimeZone()).toOffsetDateTime();
				if (!trySetTemporalObject(statementContext, preparedStatement, parameterIndex, zoned, Types.TIMESTAMP_WITH_TIMEZONE))
					preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(zoned.toInstant()));
				return;
			}
			if (!trySetTemporalObject(statementContext, preparedStatement, parameterIndex, localDateTime, Types.TIMESTAMP))
				preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(localDateTime)); // fallback

			return;
		}

		if (normalizedParameter instanceof OffsetDateTime offsetDateTime) {
			Optional<ParameterSqlType> sqlTypeOptional = determineTemporalParameterSqlType(parameterSqlTypeResolver, parameterIndex, false);
			ParameterSqlType sqlType = sqlTypeOptional.orElse(ParameterSqlType.UNKNOWN);

			if (sqlType.isTimestampWithoutTimeZone() || shouldBindAsTimestampWithoutTimeZone(statementContext, sqlTypeOptional)) {
				// Coerce to DB zone and drop the offset.
				LocalDateTime localDateTime = offsetDateTime.atZoneSameInstant(statementContext.getTimeZone()).toLocalDateTime();
				if (!trySetTemporalObject(statementContext, preparedStatement, parameterIndex, localDateTime, Types.TIMESTAMP))
					preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(localDateTime));

				return;
			}

			if (sqlType.isTimestampWithTimeZone()) {
				if (!trySetTemporalObject(statementContext, preparedStatement, parameterIndex, offsetDateTime, Types.TIMESTAMP_WITH_TIMEZONE))
					preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(offsetDateTime.toInstant()));

				return;
			}

			// Ambiguous target: default to TIMESTAMP WITH TIME ZONE.
			if (!trySetTemporalObject(statementContext, preparedStatement, parameterIndex, offsetDateTime, Types.TIMESTAMP_WITH_TIMEZONE))
				preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(offsetDateTime.toInstant()));

			return;
		}

		if (normalizedParameter instanceof Instant instant) {
			Optional<ParameterSqlType> sqlTypeOptional = determineTemporalParameterSqlType(parameterSqlTypeResolver, parameterIndex, false);
			ParameterSqlType sqlType = sqlTypeOptional.orElse(ParameterSqlType.UNKNOWN);

			if (sqlType.isTimestampWithoutTimeZone() || shouldBindAsTimestampWithoutTimeZone(statementContext, sqlTypeOptional)) {
				LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, statementContext.getTimeZone());

				if (!trySetTemporalObject(statementContext, preparedStatement, parameterIndex, localDateTime, Types.TIMESTAMP))
					preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(localDateTime));

				return;
			}

			// Ambiguous target: default to TIMESTAMP WITH TIME ZONE.
			if (!trySetTemporalObject(statementContext, preparedStatement, parameterIndex, OffsetDateTime.ofInstant(instant, ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE))
				preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(instant));

			return;
		}

		if (normalizedParameter instanceof java.time.OffsetTime offsetTime) {
			// Some drivers (e.g. DuckDB) accept setObject(OffsetTime) but reject it at execute time, so
			// try-bind detection cannot fall back; the dialect declares the string form up front.
			if (statementContext.getDatabaseDialect().bindsOffsetTimeAsString()) {
				preparedStatement.setString(parameterIndex, offsetTime.toString());
				return;
			}

			// If driver supports TIME WITH TIME ZONE, use it; else fall back to string.
			if (!trySetTemporalObject(statementContext, preparedStatement, parameterIndex, offsetTime, Types.TIME_WITH_TIMEZONE) && !trySetObject(preparedStatement, parameterIndex, offsetTime))
				preparedStatement.setString(parameterIndex, offsetTime.toString());

			return;
		}

		if (normalizedParameter instanceof SqlArrayParameter<?> sqlArrayParameter) {
			DatabaseDialect databaseDialect = statementContext.getDatabaseDialect();

			if (!databaseDialect.supportsSqlArray())
				throw unsupportedSqlArrayParameter(statementContext);

			Object[] elements = sqlArrayParameter.getElements().orElse(null);

			if (elements == null && statementContext.getDatabaseType() != DatabaseType.POSTGRESQL) {
				Optional<ParameterSqlType> sqlTypeOptional = determineParameterSqlType(parameterSqlTypeResolver, parameterIndex);
				setNullWithFallback(preparedStatement, parameterIndex, Types.ARRAY, sqlArrayParameter.getBaseTypeName(),
						sqlTypeOptional.map(ParameterSqlType::getSqlType).orElse(null));
			} else {
				List<Array> createdArrays = new ArrayList<>();
				try {
					Array array;
					if (elements == null) {
						// PostgreSQL setNull(typeName) expects an array type name, not its element name.
						// Let the driver resolve the element's real array type, retaining its OID even without SQL context.
						array = preparedStatement.getConnection().createArrayOf(sqlArrayParameter.getBaseTypeName(), (Object[]) null);
						if (array == null)
							throw new SQLException("PostgreSQL driver did not supply a typed null SQL ARRAY");
						createdArrays.add(array);
					} else {
						array = createSqlArray(statementContext, preparedStatement.getConnection(),
								sqlArrayParameter, createdArrays, 0);
					}
					preparedStatement.setArray(parameterIndex, array);
					registerSqlArrayCleanupOperations(statementContext, createdArrays);
				} catch (SQLException | RuntimeException | Error e) {
					freeSqlArraysAfterFailure(createdArrays, e);
					throw e;
				}
			}

			return;
		}

		if (normalizedParameter instanceof SqlStructParameter sqlStructParameter) {
			DatabaseDialect databaseDialect = statementContext.getDatabaseDialect();

			if (!databaseDialect.supportsSqlStruct())
				throw unsupportedSqlStructParameter(statementContext);

			Object[] attributes = sqlStructParameter.getAttributes().orElse(null);

			if (attributes == null) {
				Optional<ParameterSqlType> sqlTypeOptional = determineParameterSqlType(parameterSqlTypeResolver, parameterIndex);
				setNullWithFallback(preparedStatement, parameterIndex, Types.STRUCT, sqlStructParameter.getTypeName(),
						sqlTypeOptional.map(ParameterSqlType::getSqlType).orElse(null));
			} else {
				List<Array> createdArrays = new ArrayList<>();
				try {
					Struct struct = createSqlStruct(statementContext, preparedStatement.getConnection(),
							sqlStructParameter, createdArrays, 0);
					preparedStatement.setObject(parameterIndex, struct);
					registerSqlArrayCleanupOperations(statementContext, createdArrays);
				} catch (SQLException | RuntimeException | Error e) {
					freeSqlArraysAfterFailure(createdArrays, e);
					throw e;
				}
			}

			return;
		}

		if (normalizedParameter instanceof VectorParameter vectorParameter) {
			DatabaseDialect databaseDialect = statementContext.getDatabaseDialect();
			double[] elements = vectorParameter.getElements().orElse(null);
			Optional<ParameterSqlType> sqlTypeOptional = elements == null && databaseDialect.supportsVector()
					? determineParameterSqlType(parameterSqlTypeResolver, parameterIndex)
					: Optional.empty();
			databaseDialect.bindVector(preparedStatement, statementContext, parameterIndex, elements,
					sqlTypeOptional.map(ParameterSqlType::getSqlType).orElse(null));

			return;
		}

		if (normalizedParameter instanceof JsonParameter jsonParameter) {
			String json = jsonParameter.getJson().orElse(null);

			if (json == null) {
				Optional<ParameterSqlType> sqlTypeOptional = determineParameterSqlType(parameterSqlTypeResolver, parameterIndex);
				statementContext.getDatabaseDialect().bindNullJson(preparedStatement, parameterIndex,
						jsonParameter.getBindingPreference(), sqlTypeOptional.map(ParameterSqlType::getSqlType).orElse(null));
			} else {
				statementContext.getDatabaseDialect().bindJson(preparedStatement, parameterIndex, json,
						jsonParameter.getBindingPreference());
			}

			return;
		}

		if (normalizedParameter instanceof SqlInterval interval) {
			Optional<ParameterSqlType> target = SqlIntervalSupport.needsZeroFamilyMetadata(statementContext.getDatabaseType(), interval)
					? determineParameterSqlType(parameterSqlTypeResolver, parameterIndex)
					: parameterSqlTypeResolver.peekParameterSqlType(parameterIndex);
			SqlIntervalSupport.bind(preparedStatement, parameterIndex, statementContext, interval,
					target.map(ParameterSqlType::getSqlType).orElse(null),
					target.map(value -> value.typeName).orElse(null));
			return;
		}

		// Everything else
		preparedStatement.setObject(parameterIndex, normalizedParameter);
	}

	private boolean shouldBindAsTimestampWithoutTimeZone(@NonNull StatementContext<?> statementContext,
																											 @NonNull Optional<ParameterSqlType> sqlTypeOptional) {
		requireNonNull(statementContext);
		requireNonNull(sqlTypeOptional);
		return !sqlTypeOptional.map(ParameterSqlType::isTimestampWithTimeZone).orElse(false)
				&& (statementContext.getDatabaseDialect().prefersTimestampWithoutTimeZone()
				|| statementContext.getAmbiguousTimestampBindingStrategy() == AmbiguousTimestampBindingStrategy.TIMESTAMP_WITHOUT_TIME_ZONE);
	}

	@NonNull
	protected <T> Object[] normalizedArrayElements(@NonNull StatementContext<T> statementContext,
																								 Object @NonNull [] elements) {
		requireNonNull(statementContext);
		requireNonNull(elements);

		Object[] normalizedElements = new Object[elements.length];

		for (int j = 0; j < elements.length; j++) {
			Object element = elements[j];
			Object unwrappedElement = unwrapOptionalValue(element);
			normalizedElements[j] = unwrappedElement == null ? null : normalizeParameter(statementContext, unwrappedElement);
		}

		return normalizedElements;
	}

	@Nullable
	private <T> Array createSqlArray(@NonNull StatementContext<T> statementContext,
																					@NonNull Connection connection,
																					@NonNull SqlArrayParameter<?> sqlArrayParameter,
																					@NonNull List<Array> createdArrays,
																					int nestingDepth) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(connection);
		requireNonNull(sqlArrayParameter);
		requireNonNull(createdArrays);
		ensureSqlContainerNestingDepth(nestingDepth);

		if (!statementContext.getDatabaseDialect().supportsSqlArray())
			throw unsupportedSqlArrayParameter(statementContext);

		Object[] elements = sqlArrayParameter.getElements().orElse(null);
		if (elements == null)
			return null;

		Object[] normalizedElements = new Object[elements.length];
		for (int i = 0; i < elements.length; i++) {
			Object element = SecureParameterSupport.unwrapSecureAndOptionalParameter(elements[i]);
			if (element instanceof SqlStructParameter nestedSqlStructParameter) {
				normalizedElements[i] = createSqlStruct(statementContext, connection,
						nestedSqlStructParameter, createdArrays, nestingDepth + 1);
			} else if (element instanceof SqlArrayParameter<?> nestedSqlArrayParameter) {
				normalizedElements[i] = createSqlArray(statementContext, connection,
						nestedSqlArrayParameter, createdArrays, nestingDepth + 1);
			} else if (element != null) {
				if (element instanceof SqlInterval)
					throw new DatabaseException("SqlInterval SQL ARRAY elements require a whole-container CustomParameterBinder.");
				Object normalizedElement = normalizeParameter(statementContext, element);
				normalizedElements[i] = statementContext.getDatabaseDialect().normalizeSqlArrayElement(
						sqlArrayParameter.getBaseTypeName(), normalizedElement, statementContext.getTimeZone());
			}
		}

		Array array = connection.createArrayOf(sqlArrayParameter.getBaseTypeName(), normalizedElements);
		createdArrays.add(array);
		return array;
	}

	@Nullable
	private <T> Struct createSqlStruct(@NonNull StatementContext<T> statementContext,
																					 @NonNull Connection connection,
																					 @NonNull SqlStructParameter sqlStructParameter,
																					 @NonNull List<Array> createdArrays,
																					 int nestingDepth) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(connection);
		requireNonNull(sqlStructParameter);
		requireNonNull(createdArrays);
		ensureSqlContainerNestingDepth(nestingDepth);

		if (!statementContext.getDatabaseDialect().supportsSqlStruct())
			throw unsupportedSqlStructParameter(statementContext);

		Object[] attributes = sqlStructParameter.getAttributes().orElse(null);
		if (attributes == null)
			return null;

		Object[] normalizedAttributes = new Object[attributes.length];
		for (int i = 0; i < attributes.length; i++) {
			Object attribute = SecureParameterSupport.unwrapSecureAndOptionalParameter(attributes[i]);
			if (attribute instanceof SqlStructParameter nestedSqlStructParameter) {
				normalizedAttributes[i] = createSqlStruct(statementContext, connection,
						nestedSqlStructParameter, createdArrays, nestingDepth + 1);
			} else if (attribute instanceof SqlArrayParameter<?> nestedSqlArrayParameter) {
				normalizedAttributes[i] = createSqlArray(statementContext, connection,
						nestedSqlArrayParameter, createdArrays, nestingDepth + 1);
			} else if (attribute != null) {
				if (attribute instanceof SqlInterval)
					throw new DatabaseException("SqlInterval SQL STRUCT attributes require a whole-container CustomParameterBinder.");
				Object normalizedAttribute = normalizeParameter(statementContext, attribute);
				normalizedAttributes[i] = statementContext.getDatabaseDialect().normalizeSqlStructAttribute(
						sqlStructParameter.getTypeName(), i, normalizedAttribute, statementContext.getTimeZone());
			}
		}

		return connection.createStruct(sqlStructParameter.getTypeName(), normalizedAttributes);
	}

	private void ensureSqlContainerNestingDepth(int nestingDepth) {
		if (nestingDepth >= MAX_SQL_CONTAINER_NESTING_DEPTH)
			throw new IllegalArgumentException(format(
					"SQL STRUCT/ARRAY parameter nesting exceeds the maximum depth of %s",
					MAX_SQL_CONTAINER_NESTING_DEPTH));
	}

	@NonNull
	private IllegalArgumentException unsupportedSqlArrayParameter(@NonNull StatementContext<?> statementContext) {
		requireNonNull(statementContext);
		return new IllegalArgumentException(format(
				"%s is not supported for %s.%s; use %s.inList(...) for expanded IN-list parameters",
				SqlArrayParameter.class.getSimpleName(), DatabaseType.class.getSimpleName(),
				statementContext.getDatabaseType().name(), Parameters.class.getSimpleName()));
	}

	@NonNull
	private IllegalArgumentException unsupportedSqlStructParameter(@NonNull StatementContext<?> statementContext) {
		requireNonNull(statementContext);
		return new IllegalArgumentException(format(
				"%s is not supported for %s.%s; construct the value in SQL or use a CustomParameterBinder",
				SqlStructParameter.class.getSimpleName(), DatabaseType.class.getSimpleName(),
				statementContext.getDatabaseType().name()));
	}

	private void registerSqlArrayCleanupOperations(@NonNull StatementContext<?> statementContext,
																	 @NonNull List<Array> arrays) {
		requireNonNull(statementContext);
		requireNonNull(arrays);
		for (int i = arrays.size() - 1; i >= 0; i--)
			statementContext.addCleanupOperation(arrays.get(i)::free);
	}

	private void freeSqlArraysAfterFailure(@NonNull List<Array> arrays,
																					 @NonNull Throwable failure) {
		requireNonNull(arrays);
		requireNonNull(failure);
		for (int i = arrays.size() - 1; i >= 0; i--) {
			try {
				arrays.get(i).free();
			} catch (Throwable cleanupFailure) {
				if (cleanupFailure != failure)
					failure.addSuppressed(cleanupFailure);
			}
		}
	}

	@NonNull
	protected Boolean trySetObject(@NonNull PreparedStatement preparedStatement,
															 @NonNull Integer parameterIndex,
															 @Nullable Object parameter) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);

		try {
			preparedStatement.setObject(parameterIndex, parameter);
			return true;
		} catch (AbstractMethodError e) {
			if (JdbcFallbacks.isTerminal(e))
				throw e;
			return false;
		} catch (SQLException e) {
			rethrowTerminalBindingFailure(e);
			if (e instanceof SQLFeatureNotSupportedException || isUnsupportedSqlFeature(e))
				return false;
			throw e;
		}
	}

	/**
	 * Whether a failed bind looks like an unsupported driver feature rather than a real statement error:
	 * a {@code 0A}-class or {@code HYC00} SQLState, or an unsupported-sounding message, on any
	 * {@link SQLException} reachable through next-exception and {@link SQLException} cause edges.
	 */
	private static boolean isUnsupportedSqlFeature(@NonNull SQLException e) throws SQLException {
		requireNonNull(e);

		// Drivers hand back self-referential chains - SQLException#setNextException(this) CASes next = this -
		// so walk against an identity visited set instead of recursing: a StackOverflowError raised here
		// escapes as an Error, hiding the driver failure from retry classification and terminal-failure scans.
		Set<SQLException> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		Deque<SQLException> pending = new ArrayDeque<>();
		pending.add(e);

		while (!pending.isEmpty()) {
			SQLException current = pending.removeFirst();

			if (!visited.add(current))
				continue;

			if (hasUnsupportedSqlFeatureEvidence(current, e))
				return true;

			SQLException next = unsupportedSqlFeatureNextException(current, e);
			if (next != null)
				pending.add(next);

			// Deliberately only next-exception and SQLException-cause edges, the graph this classification has
			// always inspected: a suppressed cleanup failure ("array free is not supported") must not reclassify
			// the bind, and an SQLException behind a non-SQLException cause must not start a fallback bind.
			if (unsupportedSqlFeatureCause(current, e) instanceof SQLException causeSqlException)
				pending.add(causeSqlException);
		}

		return false;
	}

	/** Preserve the original bind failure if a driver cannot expose its classification. */
	private static boolean hasUnsupportedSqlFeatureEvidence(@NonNull SQLException e,
			@NonNull SQLException operationFailure) throws SQLException {
		requireNonNull(e);

		String sqlState;

		try {
			sqlState = e.getSQLState();
		} catch (Throwable unreadableSqlState) {
			ThrowableGraph.retainInspectionFailure(operationFailure, unreadableSqlState);
			throw operationFailure;
		}

		if (sqlState != null && (sqlState.startsWith("0A") || "HYC00".equals(sqlState)))
			return true;

		String message;

		try {
			message = e.getMessage();
		} catch (Throwable unreadableMessage) {
			ThrowableGraph.retainInspectionFailure(operationFailure, unreadableMessage);
			throw operationFailure;
		}

		if (message == null)
			return false;

		String lower = message.toLowerCase(Locale.ROOT);
		return lower.contains("not supported") || lower.contains("not implemented") || lower.contains("unsupported");
	}

	@Nullable
	private static SQLException unsupportedSqlFeatureNextException(@NonNull SQLException e,
			@NonNull SQLException operationFailure) throws SQLException {
		requireNonNull(e);

		try {
			return e.getNextException();
		} catch (Throwable unreadableChain) {
			ThrowableGraph.retainInspectionFailure(operationFailure, unreadableChain);
			throw operationFailure;
		}
	}

	@Nullable
	private static Throwable unsupportedSqlFeatureCause(@NonNull SQLException e,
			@NonNull SQLException operationFailure) throws SQLException {
		requireNonNull(e);

		try {
			return e.getCause();
		} catch (Throwable unreadableChain) {
			ThrowableGraph.retainInspectionFailure(operationFailure, unreadableChain);
			throw operationFailure;
		}
	}

	/**
	 * Binds a temporal value with an explicit target SQL type when the dialect supports it, or with an
	 * untyped {@link PreparedStatement#setObject(int, Object)} for drivers (e.g. DuckDB) that reject
	 * target-typed binds but accept {@code java.time} values natively.
	 */
	@NonNull
	private <T> Boolean trySetTemporalObject(@NonNull StatementContext<T> statementContext,
																					 @NonNull PreparedStatement preparedStatement,
																					 @NonNull Integer parameterIndex,
																					 @Nullable Object parameter,
																					 @NonNull Integer sqlType) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(sqlType);

		if (parameter != null && statementContext.getDatabaseDialect()
				.bindTemporalParameter(preparedStatement, parameterIndex, parameter, sqlType))
			return true;

		if (!statementContext.getDatabaseDialect().supportsSetObjectWithTargetSqlType())
			return trySetObject(preparedStatement, parameterIndex, parameter);

		return trySetObject(preparedStatement, parameterIndex, parameter, sqlType);
	}

	@NonNull
	protected Boolean trySetObject(@NonNull PreparedStatement preparedStatement,
															 @NonNull Integer parameterIndex,
															 @Nullable Object parameter,
																 @NonNull Integer sqlType) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(sqlType);

		try {
			preparedStatement.setObject(parameterIndex, parameter, sqlType);
			return true;
		} catch (AbstractMethodError e) {
			if (JdbcFallbacks.isTerminal(e))
				throw e;
			return false;
		} catch (SQLException e) {
			rethrowTerminalBindingFailure(e);
			if (e instanceof SQLFeatureNotSupportedException || isUnsupportedSqlFeature(e))
				return false;
			throw e;
		}
	}

	@NonNull
	protected Optional<ParameterSqlType> determineParameterSqlType(@NonNull PreparedStatement preparedStatement,
																																 @NonNull Integer parameterIndex) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		return determineParameterSqlType(new ParameterSqlTypeResolver(preparedStatement), parameterIndex);
	}

	@NonNull
	protected Optional<ParameterSqlType> determineParameterSqlType(@NonNull ParameterSqlTypeResolver parameterSqlTypeResolver,
																																 @NonNull Integer parameterIndex) throws SQLException {
		requireNonNull(parameterSqlTypeResolver);
		requireNonNull(parameterIndex);
		return parameterSqlTypeResolver.determineParameterSqlType(parameterIndex);
	}

	private Optional<ParameterSqlType> determineTemporalParameterSqlType(ParameterSqlTypeResolver resolver,
			int parameterIndex, boolean localValue) throws SQLException {
		Optional<ParameterSqlType> type = determineParameterSqlType(resolver, parameterIndex);
		resolver.requireSafeOracleTemporalTarget(parameterIndex, type, localValue);
		return type;
	}

	private static boolean requiresLocalTemporalSqlType(StatementContext<?> context) {
		// These dialects have no zoned timestamp target to distinguish from DATE/local TIMESTAMP.
		return switch (context.getDatabaseType()) {
			case MYSQL, MARIA_DB, SQLITE -> false;
			default -> true;
		};
	}

	/** Declare only known managed demands; the executor decides when to resolve this callback-free plan. */
	void registerParameterSqlTypeDemands(StatementContext<?> context, ParameterSqlTypeResolver resolver,
			List<? extends List<Object>> parameterGroups) {
		if (context.getDatabaseType() != DatabaseType.POSTGRESQL
				|| getClass() != DefaultPreparedStatementBinder.class || !this.customParameterBinders.isEmpty()) return;
		Set<Integer> indices = null;
		for (List<Object> parameters : parameterGroups) {
			for (int index = 0; index < parameters.size(); ++index) {
				Object value = parameters.get(index);
				// Only these concrete library/JDK wrappers are callback-free. Arbitrary implementations,
				// typed values and subclasses can change the describe context and remain ordered barriers.
				for (int depth = 0; depth < SecureParameterSupport.MAX_NESTING_DEPTH; ++depth) {
					if (value instanceof Optional<?> optional) value = optional.orElse(null);
					else if (value instanceof DefaultSecureParameter secure) value = secure.getValue().orElse(null);
					else break;
				}
				if (value instanceof OptionalInt optional) value = optional.isPresent() ? optional.getAsInt() : null;
				else if (value instanceof OptionalLong optional) value = optional.isPresent() ? optional.getAsLong() : null;
				else if (value instanceof OptionalDouble optional) value = optional.isPresent() ? optional.getAsDouble() : null;
				if (value != null && !isPlainParameter(value)) {
					if (indices != null) resolver.registerParameterIndices(indices);
					return;
				}
				if (value == null || value instanceof Instant || value instanceof OffsetDateTime
						|| value instanceof ZonedDateTime || value instanceof java.sql.Timestamp
						|| (value instanceof Date && !(value instanceof java.sql.Date || value instanceof java.sql.Time))
						|| ((value instanceof LocalDate || value instanceof LocalDateTime || value instanceof java.sql.Date)
								&& requiresLocalTemporalSqlType(context))) {
					if (indices == null) indices = new LinkedHashSet<>();
					indices.add(index + 1);
				}
			}
		}
		if (indices != null) resolver.registerParameterIndices(indices);
	}

	private static boolean isPlainParameter(Object value) {
		Class<?> type = value.getClass();
		return type == Parameters.DefaultJsonParameter.class
				|| value instanceof String || value instanceof UUID || value instanceof Boolean || value instanceof Character
				|| value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
				|| value instanceof Float || value instanceof Double || value instanceof Enum<?> || value instanceof byte[]
				|| value instanceof Instant || value instanceof OffsetDateTime || value instanceof ZonedDateTime
				|| value instanceof LocalDate || value instanceof LocalDateTime || value instanceof LocalTime
				|| value instanceof java.time.OffsetTime || value instanceof Year || value instanceof YearMonth
				|| value instanceof Locale || value instanceof Currency || value instanceof SqlInterval
				// These JDK classes are extensible; a subclass can execute arbitrary code during normalization.
				|| type == BigDecimal.class || type == java.math.BigInteger.class || type == Date.class
				|| type == java.sql.Date.class || type == java.sql.Time.class || type == java.sql.Timestamp.class;
	}

	static final class ParameterSqlTypeResolver {
		@NonNull
		private final PreparedStatement preparedStatement;
		private final boolean protectPostgresMetadata;
		private final boolean oracleMetadata;
		private final Map<Integer, Optional<ParameterSqlType>> resolvedTypes = new HashMap<>();
		@Nullable
		private Set<Integer> parameterIndices;
		private boolean initialized;
		private int parameterGroup;
		private boolean immutablePostgresMetadata;
		private final Set<Integer> boundParameterIndices = new LinkedHashSet<>();
		private final Set<Integer> untrustedPostgresIndices = new LinkedHashSet<>();
		@Nullable
		private Throwable resolutionFailure;
		@Nullable
		private ParameterMetaData parameterMetaData;
		@Nullable
		private NullPointerException oracleParserFailure;
		@Nullable
		private Set<Integer> unavailableOracleIndices;

		ParameterSqlTypeResolver(@NonNull PreparedStatement preparedStatement) {
			this(preparedStatement, false, false);
		}

		ParameterSqlTypeResolver(@NonNull StatementContext<?> context, @NonNull PreparedStatement preparedStatement) {
			this(preparedStatement, requireNonNull(context).getDatabaseType() == DatabaseType.POSTGRESQL,
					context.getDatabaseType() == DatabaseType.ORACLE);
		}

		private ParameterSqlTypeResolver(PreparedStatement preparedStatement, boolean protectPostgresMetadata, boolean oracleMetadata) {
			this.preparedStatement = requireNonNull(preparedStatement);
			this.protectPostgresMetadata = protectPostgresMetadata;
			this.oracleMetadata = oracleMetadata;
		}

		void registerParameterIndices(Collection<Integer> parameterIndices) {
			for (Integer index : requireNonNull(parameterIndices)) {
				if (this.parameterIndices == null) this.parameterIndices = new LinkedHashSet<>();
				this.parameterIndices.add(requireNonNull(index));
			}
		}

		void beginParameterGroup() {
			++this.parameterGroup;
		}

		void noteParameterBound(int parameterIndex) {
			if (this.protectPostgresMetadata && !this.initialized) this.boundParameterIndices.add(parameterIndex);
		}

		void prefetchRegisteredParameterSqlTypes() throws SQLException {
			if (this.resolutionFailure != null) rethrowMetadataFailure(this.resolutionFailure);
			if (this.parameterIndices != null) {
				for (Integer index : this.parameterIndices) {
					if (!this.resolvedTypes.containsKey(index)) {
						determineParameterSqlType(index);
						return;
					}
				}
			}
		}

		/** Existing evidence only: nonzero intervals must not introduce a new describe. */
		Optional<ParameterSqlType> peekParameterSqlType(int parameterIndex) throws SQLException {
			if (this.resolutionFailure != null) rethrowMetadataFailure(this.resolutionFailure);
			return this.resolvedTypes.getOrDefault(parameterIndex, Optional.empty());
		}

		Optional<ParameterSqlType> determineNullParameterSqlType(int parameterIndex) throws SQLException {
			if (this.resolutionFailure != null) rethrowMetadataFailure(this.resolutionFailure);
			Optional<ParameterSqlType> cached = this.resolvedTypes.get(parameterIndex);
			if (cached != null) return cached;
			// SQL NULL needs no temporal conversion. Let the server infer its type rather than reading
			// an earlier group's input OID; do not cache absence as evidence for a later nonnull value.
			if (this.protectPostgresMetadata && (this.untrustedPostgresIndices.contains(parameterIndex)
					|| (this.parameterGroup > 1 && !this.immutablePostgresMetadata))) return Optional.empty();
			return determineParameterSqlType(parameterIndex);
		}

		private void requireSafeOracleTemporalTarget(int index, Optional<ParameterSqlType> type, boolean localValue) {
			if (this.oracleParserFailure == null
					|| ((this.unavailableOracleIndices == null || !this.unavailableOracleIndices.contains(index))
							&& this.resolvedTypes.containsKey(index))) return;
			// A successful earlier index retains its ordinary ambiguity policy. A parser fallback on this
			// index (or an uncached index after the descriptor failed) must not invent a temporal target.
			if (type.map(value -> value.isTimestampWithTimeZone() || value.isTimestampWithoutTimeZone()
					|| (localValue && value.getSqlType() == Types.DATE)).orElse(false)) return;
			throw new DatabaseException("Cannot determine the Oracle temporal target after parameter metadata parser failure. "
					+ "Use an explicit SQL CAST that exposes TIMESTAMP or TIMESTAMP WITH TIME ZONE metadata, "
					+ "or a CustomParameterBinder that selects the target type.", this.oracleParserFailure);
		}

		private void markUnavailableOracleIndex(int index) {
			if (this.unavailableOracleIndices == null) this.unavailableOracleIndices = new LinkedHashSet<>();
			this.unavailableOracleIndices.add(index);
		}

		@NonNull
		Optional<ParameterSqlType> determineParameterSqlType(@NonNull Integer parameterIndex) throws SQLException {
			requireNonNull(parameterIndex);
			if (this.resolutionFailure != null) rethrowMetadataFailure(this.resolutionFailure);
			Optional<ParameterSqlType> cached = this.resolvedTypes.get(parameterIndex);
			if (cached != null) return cached;
			if (this.initialized && this.parameterMetaData == null) return Optional.empty();
			if (this.protectPostgresMetadata && (this.untrustedPostgresIndices.contains(parameterIndex)
					|| (this.parameterGroup > 1 && !this.immutablePostgresMetadata))) {
				// Foreign metadata may alias the driver's mutable input types. After a preceding group
				// has bound values, a new getter can describe those inputs rather than the SQL target.
				// Respect wrapper/custom-callback semantics and refuse an unprovable temporal coercion.
				SQLException failure = unsafeTargetMetadata(parameterIndex);
				this.resolutionFailure = failure;
				throw failure;
			}

			List<ParameterTypeLookup> lookups = new ArrayList<>();
			lookups.add(new ParameterTypeLookup(parameterIndex));
			if (this.parameterIndices != null) {
				for (Integer index : this.parameterIndices)
					if (!index.equals(parameterIndex) && !this.resolvedTypes.containsKey(index))
						lookups.add(new ParameterTypeLookup(index));
			}
			MetadataSnapshot snapshot = new MetadataSnapshot(lookups);
			while (snapshot.hasNext()) {
				try {
					lookupProtected(snapshot);
				} catch (MetadataProtectionFailure failure) {
					// Establishment/recovery failures are never optional-metadata compatibility signals.
					this.resolutionFailure = failure.original;
					rethrowMetadataFailure(failure.original);
					throw new AssertionError("Unreachable");
				} catch (AbstractMethodError failure) {
					if (JdbcFallbacks.isTerminal(failure)) {
						this.resolutionFailure = failure;
						throw failure;
					}
					snapshot.fallback();
				} catch (SQLException failure) {
					if (failure instanceof UnstableTargetMetadata || JdbcFallbacks.isTerminal(failure)) {
						this.resolutionFailure = failure;
						throw failure;
					}
					snapshot.fallback();
				} catch (RuntimeException | Error failure) {
					this.resolutionFailure = failure;
					throw failure;
				}
				// A failed describe caches its absence for this prepared statement, as before. Type/name
				// failures are per-index; recover before reading remaining indices under a fresh savepoint.
				if (this.initialized && this.parameterMetaData == null) break;
			}
			return this.resolvedTypes.getOrDefault(parameterIndex, Optional.empty());
		}

		private static SQLException unsafeTargetMetadata(int parameterIndex) {
			return new UnstableTargetMetadata(parameterIndex);
		}

		private void lookupProtected(MetadataSnapshot snapshot) throws SQLException {
			if (!this.protectPostgresMetadata) {
				snapshot.perform();
				return;
			}
			Connection connection = null;
			boolean autoCommit;
			try {
				connection = this.preparedStatement.getConnection();
				if (connection instanceof PyranidRawConnection rawConnection)
					connection = rawConnection.metadataConnection();
				autoCommit = connection.getAutoCommit();
			} catch (Throwable failure) {
				if (connection != null) Transaction.recordMetadataProtectionFailure(connection, failure);
				throw new MetadataProtectionFailure(failure);
			}
			if (autoCommit) {
				snapshot.perform();
				return;
			}
			Savepoint savepoint;
			try {
				// pgjdbc describes unknown parameters on the server. Even an optional describe can abort
				// a transaction before the real, correctly typed execution. Keep that probe recoverable.
				// Named savepoints do not consume pgjdbc's signed per-connection savepoint counter.
				// A unique name also preserves an application's existing named savepoints.
				savepoint = requireNonNull(connection.setSavepoint("pyranid_meta_" + UUID.randomUUID().toString().replace("-", "")),
						"Driver returned no parameter metadata savepoint");
			} catch (Throwable failure) {
				if (isRejectedAbortedTransaction(connection, failure))
					Transaction.recordMetadataProtectionRejection(connection, failure);
				else Transaction.recordMetadataProtectionFailure(connection, failure);
				throw new MetadataProtectionFailure(failure);
			}

			try {
				snapshot.perform();
			} catch (Throwable primary) {
				boolean recovered = true;
				try { connection.rollback(savepoint); }
				catch (Throwable cleanup) {
					Database.appendCleanupFailure(primary, cleanup);
					recovered = false;
				}
				try { connection.releaseSavepoint(savepoint); }
				catch (Throwable cleanup) {
					Database.appendCleanupFailure(primary, cleanup);
					recovered = false;
				}
				if (!recovered) {
					Transaction.recordMetadataProtectionFailure(connection, primary);
					throw new MetadataProtectionFailure(primary);
				}
				rethrowMetadataFailure(primary);
				throw new AssertionError("Unreachable");
			}
			try { connection.releaseSavepoint(savepoint); }
			catch (Throwable failure) {
				Transaction.recordMetadataProtectionFailure(connection, failure);
				throw new MetadataProtectionFailure(failure);
			}
		}

		/** A rejected command in an already-aborted transaction can still be recovered by its caller's savepoint. */
		private static boolean isRejectedAbortedTransaction(Connection connection, Throwable failure) {
			if (!(failure instanceof SQLException sqlFailure) || Thread.currentThread().isInterrupted()) return false;
			try {
				if (!"25P02".equals(sqlFailure.getSQLState())) return false;
			} catch (Throwable inspectionFailure) {
				ThrowableGraph.retainInspectionFailure(failure, inspectionFailure);
				return false;
			}
			// The cause is pgjdbc's historical statement failure, whose SQLState can be any server
			// code (including user-defined codes). Prove rejection from the physical transaction state
			// instead of guessing from that history. Unknown state retains the visible completion fence.
			PostgresDriverSupport.TransactionStateRead state = PostgresDriverSupport.transactionState(connection);
			if (state.failure() != null) ThrowableGraph.retainInspectionFailure(failure, state.failure());
			return state.state() == PostgresDriverSupport.TransactionState.FAILED && !Thread.currentThread().isInterrupted();
		}

		private final class MetadataSnapshot {
			private final List<ParameterTypeLookup> lookups;
			private int next;

			private MetadataSnapshot(List<ParameterTypeLookup> lookups) { this.lookups = lookups; }
			private boolean hasNext() { return this.next < this.lookups.size(); }

			private void perform() throws SQLException {
				if (!initialized) {
					initialized = true;
					parameterMetaData = preparedStatement.getParameterMetaData();
					if (protectPostgresMetadata && parameterMetaData != null) {
						PostgresParameterMetadataSnapshot.Snapshot frozen = PostgresParameterMetadataSnapshot.freeze(parameterMetaData);
						if (frozen != null) {
							parameterMetaData = frozen.metadata();
							immutablePostgresMetadata = true;
							// A first describe can already include earlier setters' input OIDs. Copying
							// makes those immutable, not trustworthy target evidence for a later demand.
							untrustedPostgresIndices.addAll(boundParameterIndices);
							Set<Integer> scheduled = new LinkedHashSet<>();
							for (ParameterTypeLookup lookup : this.lookups) scheduled.add(lookup.index);
							for (int index : frozen.builtinIndices()) {
								if (untrustedPostgresIndices.contains(index) || resolvedTypes.containsKey(index)) continue;
								if (scheduled.add(index)) this.lookups.add(new ParameterTypeLookup(index));
							}
						}
					}
					boundParameterIndices.clear();
				}
				while (hasNext()) {
					ParameterTypeLookup lookup = this.lookups.get(this.next);
					resolvedTypes.put(lookup.index, lookup.perform());
					++this.next;
				}
			}

			private void fallback() {
				ParameterTypeLookup lookup = this.lookups.get(this.next++);
				resolvedTypes.put(lookup.index, lookup.fallback());
			}
		}

		private final class ParameterTypeLookup {
			private final int index;
			@Nullable private Integer sqlType;

			private ParameterTypeLookup(int index) { this.index = index; }

			private Optional<ParameterSqlType> perform() throws SQLException {
				if (ParameterSqlTypeResolver.this.untrustedPostgresIndices.contains(this.index))
					throw unsafeTargetMetadata(this.index);
				ParameterMetaData metadata = ParameterSqlTypeResolver.this.parameterMetaData;
				if (metadata == null) {
					if (oracleParserFailure != null) markUnavailableOracleIndex(this.index);
					return Optional.empty();
				}
				try {
					this.sqlType = metadata.getParameterType(this.index);
					return Optional.of(new ParameterSqlType(this.sqlType, metadata.getParameterTypeName(this.index), oracleMetadata));
				} catch (NullPointerException failure) {
					if (!isUnavailableOracleMetadata(failure)) throw failure;
					ParameterSqlTypeResolver.this.oracleParserFailure = failure;
					markUnavailableOracleIndex(this.index);
					// ojdbc's optional parser cannot describe some INSERT ... SELECT binds. Its
					// descriptor remains unusable for every index, so cache that absence once. A
					// type already obtained before a name failure retains the usual code-only fallback.
					ParameterSqlTypeResolver.this.parameterMetaData = null;
					return fallback();
				}
			}

			private boolean isUnavailableOracleMetadata(NullPointerException failure) {
				if (!ParameterSqlTypeResolver.this.oracleMetadata || failure.getClass() != NullPointerException.class
						|| Thread.currentThread().isInterrupted() || failure.getCause() != null || failure.getSuppressed().length != 0)
					return false;
				StackTraceElement[] frames = failure.getStackTrace();
				if (frames.length == 0) {
					// HotSpot omits the stack/message of a hot implicit NPE. With no frames, only the
					// actual native metadata class establishes provenance; never unwrap a foreign proxy.
					ParameterMetaData metadata = ParameterSqlTypeResolver.this.parameterMetaData;
					return failure.getMessage() == null && metadata != null
							&& metadata.getClass().getName().equals("oracle.jdbc.driver.OracleParameterMetaData");
				}
				if (frames.length < 5) return false;
				String[] methods = {"storeParametersMetadata", "query", "parseAndQuery", "checkValidIndex"};
				for (int index = 0; index < methods.length; index++)
					if (!frames[index].getClassName().equals("oracle.jdbc.driver.OracleParameterMetaData")
							|| !frames[index].getMethodName().equals(methods[index])) return false;
				return frames[4].getClassName().equals("oracle.jdbc.driver.OracleParameterMetaData")
						&& frames[4].getMethodName().equals(this.sqlType == null ? "getParameterType" : "getParameterTypeName");
			}

			private Optional<ParameterSqlType> fallback() {
				return this.sqlType == null ? Optional.empty() : Optional.of(new ParameterSqlType(this.sqlType, null, oracleMetadata));
			}
		}

		private static void rethrowMetadataFailure(Throwable failure) throws SQLException {
			if (failure instanceof SQLException sqlFailure) throw sqlFailure;
			if (failure instanceof RuntimeException runtimeFailure) throw runtimeFailure;
			if (failure instanceof Error error) throw error;
			throw new SQLException("Parameter metadata lookup failed", failure);
		}

		/** Private control flow; never render or replace the original driver's failure. */
		private static final class UnstableTargetMetadata extends SQLException {
			private UnstableTargetMetadata(int parameterIndex) {
				super("Cannot safely resolve previously uncached PostgreSQL batch parameter "
						+ parameterIndex + " after earlier values were bound; establish target metadata before binding "
						+ "or bind this parameter explicitly with a custom binder", "0A000");
			}
		}

		/** Private control flow; never render or replace the original driver's failure. */
		private static final class MetadataProtectionFailure extends RuntimeException {
			private final Throwable original;
			private MetadataProtectionFailure(Throwable original) {
				super(null, null, false, false);
				this.original = original;
			}
		}
	}

	protected static final class ParameterSqlType {
		@NonNull
		private static final ParameterSqlType UNKNOWN = new ParameterSqlType(Types.OTHER, null, false);
		@NonNull
		private final Integer sqlType;
		@Nullable
		private final String typeName;
		private final boolean oracleType;

		private ParameterSqlType(@NonNull Integer sqlType,
													 @Nullable String typeName, boolean oracleType) {
			this.sqlType = requireNonNull(sqlType);
			this.typeName = typeName;
			this.oracleType = oracleType;
		}

		@NonNull
		Integer getSqlType() {
			return this.sqlType;
		}

		private boolean isTimestampWithTimeZone() {
			if (getSqlType() == Types.TIMESTAMP_WITH_TIMEZONE
					|| (this.oracleType && (getSqlType() == -101 || getSqlType() == -102)))
				return true;

			String typeName = this.typeName;

			if (typeName == null)
				return false;

			String normalizedTypeName = typeName.toUpperCase(Locale.ROOT);
			return normalizedTypeName.contains("WITH TIME ZONE")
					|| normalizedTypeName.contains("WITH LOCAL TIME ZONE")
					|| normalizedTypeName.contains("TIMESTAMPTZ")
					|| normalizedTypeName.contains("DATETIMEOFFSET");
		}

		private boolean isTimestampWithoutTimeZone() {
			if (getSqlType() == Types.TIMESTAMP && !isTimestampWithTimeZone())
				return true;

			String typeName = this.typeName;

			if (typeName == null)
				return false;

			return typeName.toUpperCase(Locale.ROOT).contains("TIMESTAMP") && !isTimestampWithTimeZone();
		}

	}

	/** Compatibility fallbacks must not hide a driver rollback, lost connection, or cancellation. */
	static void rethrowTerminalBindingFailure(@NonNull SQLException failure) throws SQLException {
		if (JdbcFallbacks.isTerminal(failure))
			throw failure;
	}

	static void setNullWithFallback(@NonNull PreparedStatement preparedStatement,
																	@NonNull Integer parameterIndex,
																	@NonNull Integer primarySqlType,
																	@Nullable String typeName,
																	@Nullable Integer fallbackSqlType) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(primarySqlType);

		SQLException lastException = null;

		if (typeName != null) {
			try {
				preparedStatement.setNull(parameterIndex, primarySqlType, typeName);
				return;
			} catch (SQLException e) {
				rethrowTerminalBindingFailure(e);
				lastException = e;
			}
		}

		try {
			preparedStatement.setNull(parameterIndex, primarySqlType);
			return;
		} catch (SQLException e) {
			rethrowTerminalBindingFailure(e);
			lastException = e;
		}

		if (fallbackSqlType != null && fallbackSqlType != Types.NULL && !fallbackSqlType.equals(primarySqlType)) {
			try {
				preparedStatement.setNull(parameterIndex, fallbackSqlType);
				return;
			} catch (SQLException e) {
				rethrowTerminalBindingFailure(e);
				lastException = e;
			}
		}

		if (primarySqlType != Types.NULL) {
			try {
				preparedStatement.setNull(parameterIndex, Types.NULL);
				return;
			} catch (SQLException e) {
				rethrowTerminalBindingFailure(e);
				lastException = e;
			}
		}

		if (lastException != null)
			throw lastException;
	}

	/**
	 * Try custom binders for the given value. Returns true if a binder handled it.
	 */
	@NonNull
	protected <T> Boolean tryCustomBinders(@NonNull StatementContext<T> statementContext,
																				 @NonNull PreparedStatement preparedStatement,
																				 @NonNull Integer parameterIndex,
																				 @NonNull Object parameter, /* must be the UNWRAPPED value */
																				 @NonNull TargetType targetType /* explicit target (from TypedParameter if present) */) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(parameter);
		requireNonNull(targetType);

		// use the provided target instead of TargetType.of(parameter.getClass())
		List<CustomParameterBinder> candidates = customBindersFor(targetType);

		if (candidates.isEmpty())
			return false;

		for (CustomParameterBinder customParameterBinder : candidates) {
			JdbcCallbackContext.requireValidation();
			BindingResult bindingResult = requireNonNull(customParameterBinder.bind(statementContext, preparedStatement, parameterIndex, parameter));

			if (bindingResult instanceof BindingResult.Handled)
				return true;
		}

		return false;
	}

	/**
	 * Try custom binders for a null typed parameter. Returns true if a binder handled it.
	 */
	@NonNull
	protected <T> Boolean tryCustomNullBinders(@NonNull StatementContext<T> statementContext,
																						 @NonNull PreparedStatement preparedStatement,
																						 @NonNull Integer parameterIndex,
																						 @NonNull Integer sqlType,
																						 @NonNull TargetType targetType) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(sqlType);
		requireNonNull(targetType);

		List<CustomParameterBinder> candidates = customBindersFor(targetType);
		if (candidates.isEmpty())
			return false;

		for (CustomParameterBinder customParameterBinder : candidates) {
			JdbcCallbackContext.requireValidation();
			BindingResult bindingResult = requireNonNull(
					customParameterBinder.bindNull(statementContext, preparedStatement, parameterIndex, targetType, sqlType));

			if (bindingResult instanceof BindingResult.Handled)
				return true;
		}

		return false;
	}

	/**
	 * Filter and cache the binders whose {@code appliesTo(targetType)} returned true.
	 */
	@NonNull
	protected List<@NonNull CustomParameterBinder> customBindersFor(@NonNull TargetType targetType) {
		requireNonNull(targetType);

		if (getCustomParameterBinders().isEmpty())
			return List.of();

		// Non-Class declarations can carry generic components, bounds or owner arguments even
		// when getTypeArguments() is empty; they must not share erased applicability entries.
		int[] matches = targetType.getType() instanceof Class<?>
				? this.bindersByRawClassCache.get(targetType.getRawClass())
				: computeCustomParameterBinderIndicesFor(targetType);
		if (matches.length == 0) return List.of();
		List<CustomParameterBinder> candidates = new ArrayList<>(matches.length);
		for (int index : matches) candidates.add(getCustomParameterBinders().get(index));
		return List.copyOf(candidates);
	}

	private int[] computeCustomParameterBinderIndicesFor(TargetType targetType) {
		List<CustomParameterBinder> binders = getCustomParameterBinders();
		int[] matches = new int[binders.size()];
		int count = 0;
		for (int index = 0; index < binders.size(); index++)
			if (binders.get(index).appliesTo(targetType)) matches[count++] = index;
		return count == 0 ? NO_CUSTOM_PARAMETER_BINDERS : Arrays.copyOf(matches, count);
	}

	/**
	 * Massages a parameter into a JDBC-friendly format if needed.
	 * <p>
	 * For example, some databases need {@link UUID} values converted before binding.
	 *
	 * @param statementContext current SQL context
	 * @param parameter        the parameter to (possibly) massage
	 * @return the result of the massaging process
	 */
	@NonNull
	protected <T> Object normalizeParameter(@NonNull StatementContext<T> statementContext,
																					@NonNull Object parameter) {
		requireNonNull(statementContext);
		requireNonNull(parameter);

		if (parameter instanceof Character character)
			return character.toString();
		if (parameter instanceof BigDecimal decimal)
			return statementContext.getDatabaseDialect().normalizeBigDecimal(decimal);

		// Coerce to java.time whenever possible
		if (parameter instanceof java.sql.Timestamp timestamp)
			return timestamp.toInstant();
		if (parameter instanceof java.sql.Date date)
			return date.toLocalDate();
		if (parameter instanceof java.sql.Time time)
			return time.toLocalTime().withNano((int) Math.floorMod(time.getTime(), 1000L) * 1_000_000);
		if (parameter instanceof Date date)
			return Instant.ofEpochMilli(date.getTime());
		if (parameter instanceof ZonedDateTime zonedDateTime)
			return zonedDateTime.toOffsetDateTime();
		if (parameter instanceof Locale)
			return ((Locale) parameter).toLanguageTag();
		if (parameter instanceof Currency)
			return ((Currency) parameter).getCurrencyCode();
		if (parameter instanceof Enum)
			return ((Enum<?>) parameter).name();
		if (parameter instanceof ZoneId)
			return ((ZoneId) parameter).getId();
		if (parameter instanceof TimeZone)
			return ((TimeZone) parameter).getID();
		if (parameter instanceof Year year)
			return year.getValue();
		if (parameter instanceof YearMonth yearMonth)
			return yearMonth.toString();

		if (parameter instanceof UUID uuid)
			return statementContext.getDatabaseDialect().normalizeUuid(uuid);

		return parameter;
	}

	@Nullable
	private static Object unwrapOptionalValue(@Nullable Object value) {
		if (value == null)
			return null;

		if (value instanceof Optional<?> optional)
			return optional.orElse(null);
		if (value instanceof OptionalInt optionalInt)
			return optionalInt.isPresent() ? optionalInt.getAsInt() : null;
		if (value instanceof OptionalLong optionalLong)
			return optionalLong.isPresent() ? optionalLong.getAsLong() : null;
		if (value instanceof OptionalDouble optionalDouble)
			return optionalDouble.isPresent() ? optionalDouble.getAsDouble() : null;

		return value;
	}

	private static int defaultNullSqlTypeForTargetType(@NonNull TargetType targetType) {
		requireNonNull(targetType);

		if (targetType.isArray() || targetType.isList() || targetType.isSet())
			return Types.ARRAY;
		if (targetType.isMap())
			return Types.OTHER;

		return Types.NULL;
	}

	@NonNull
	protected List<@NonNull CustomParameterBinder> getCustomParameterBinders() {
		return this.customParameterBinders;
	}
}
