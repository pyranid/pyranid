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
import java.sql.Array;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
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
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
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
	@NonNull
	private final List<CustomParameterBinder> customParameterBinders;

	// Cache: which binders apply for a given target type?
	@NonNull
	private final ClassValue<List<CustomParameterBinder>> bindersByRawClassCache =
			new ClassValue<>() {
				@Override
				protected List<CustomParameterBinder> computeValue(Class<?> type) {
					return computeCustomBindersFor(TargetType.of(type));
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
		bindParameter(statementContext, preparedStatement, parameterIndex, parameter, new ParameterSqlTypeResolver(preparedStatement));
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
			Optional<ParameterSqlType> sqlTypeOptional = determineParameterSqlType(parameterSqlTypeResolver, parameterIndex);
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

		if (explicitTargetType == null)
			explicitTargetType = TargetType.of(unwrappedParameter.getClass());

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

		Object normalizedParameter = normalizeParameter(statementContext, unwrappedParameter);

		if (normalizedParameter instanceof LocalDate localDate) {
			if (!trySetObject(preparedStatement, parameterIndex, localDate, Types.DATE))
				preparedStatement.setDate(parameterIndex, java.sql.Date.valueOf(localDate)); // fallback

			return;
		}

		if (normalizedParameter instanceof LocalTime localTime) {
			if (!trySetObject(preparedStatement, parameterIndex, localTime, Types.TIME)) {
				if (localTime.getNano() == 0)
					preparedStatement.setTime(parameterIndex, java.sql.Time.valueOf(localTime));
				else
					preparedStatement.setString(parameterIndex, localTime.toString());
			}

			return;
		}

		if (normalizedParameter instanceof LocalDateTime localDateTime) {
			if (!trySetObject(preparedStatement, parameterIndex, localDateTime, Types.TIMESTAMP))
				preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(localDateTime)); // fallback

			return;
		}

		if (normalizedParameter instanceof OffsetDateTime offsetDateTime) {
			Optional<ParameterSqlType> sqlTypeOptional = determineParameterSqlType(parameterSqlTypeResolver, parameterIndex);
			ParameterSqlType sqlType = sqlTypeOptional.orElse(ParameterSqlType.UNKNOWN);

			if (sqlType.isTimestampWithoutTimeZone() || shouldBindAsTimestampWithoutTimeZone(statementContext, sqlTypeOptional)) {
				// Coerce to DB zone and drop the offset.
				LocalDateTime localDateTime = offsetDateTime.atZoneSameInstant(statementContext.getTimeZone()).toLocalDateTime();
				if (!trySetObject(preparedStatement, parameterIndex, localDateTime, Types.TIMESTAMP))
					preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(localDateTime));

				return;
			}

			if (sqlType.isTimestampWithTimeZone()) {
				if (!trySetObject(preparedStatement, parameterIndex, offsetDateTime, Types.TIMESTAMP_WITH_TIMEZONE))
					preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(offsetDateTime.toInstant()));

				return;
			}

			// Ambiguous target: default to TIMESTAMP WITH TIME ZONE.
			if (!trySetObject(preparedStatement, parameterIndex, offsetDateTime, Types.TIMESTAMP_WITH_TIMEZONE))
				preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(offsetDateTime.toInstant()));

			return;
		}

		if (normalizedParameter instanceof Instant instant) {
			Optional<ParameterSqlType> sqlTypeOptional = determineParameterSqlType(parameterSqlTypeResolver, parameterIndex);
			ParameterSqlType sqlType = sqlTypeOptional.orElse(ParameterSqlType.UNKNOWN);

			if (sqlType.isTimestampWithoutTimeZone() || shouldBindAsTimestampWithoutTimeZone(statementContext, sqlTypeOptional)) {
				LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, statementContext.getTimeZone());

				if (!trySetObject(preparedStatement, parameterIndex, localDateTime, Types.TIMESTAMP))
					preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(localDateTime));

				return;
			}

			// Ambiguous target: default to TIMESTAMP WITH TIME ZONE.
			if (!trySetObject(preparedStatement, parameterIndex, OffsetDateTime.ofInstant(instant, ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE))
				preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(instant));

			return;
		}

		if (normalizedParameter instanceof java.time.OffsetTime offsetTime) {
			// If driver supports TIME WITH TIME ZONE, use it; else fall back to string.
			if (!trySetObject(preparedStatement, parameterIndex, offsetTime, Types.TIME_WITH_TIMEZONE) && !trySetObject(preparedStatement, parameterIndex, offsetTime))
				preparedStatement.setString(parameterIndex, offsetTime.toString());

			return;
		}

		if (normalizedParameter instanceof SqlArrayParameter<?> sqlArrayParameter) {
			DatabaseDialect databaseDialect = statementContext.getDatabaseDialect();

			if (!databaseDialect.supportsSqlArray())
				throw new IllegalArgumentException(format("%s is not supported for %s.%s; use %s.inList(...) for expanded IN-list parameters",
						SqlArrayParameter.class.getSimpleName(), DatabaseType.class.getSimpleName(),
						statementContext.getDatabaseType().name(), Parameters.class.getSimpleName()));

			Object[] elements = sqlArrayParameter.getElements().orElse(null);

			if (elements == null) {
				Optional<ParameterSqlType> sqlTypeOptional = determineParameterSqlType(parameterSqlTypeResolver, parameterIndex);
				setNullWithFallback(preparedStatement, parameterIndex, Types.ARRAY, sqlArrayParameter.getBaseTypeName(),
						sqlTypeOptional.map(ParameterSqlType::getSqlType).orElse(null));
			} else {
				Object[] normalizedElements = normalizedArrayElements(statementContext, elements);
				Array array = preparedStatement.getConnection().createArrayOf(sqlArrayParameter.getBaseTypeName(), normalizedElements);
				try {
					preparedStatement.setArray(parameterIndex, array);
					statementContext.addCleanupOperation(array::free);
				} catch (SQLException e) {
					try {
						array.free();
					} catch (SQLException freeException) {
						e.addSuppressed(freeException);
					}
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
			databaseDialect.bindVector(preparedStatement, parameterIndex, elements,
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

		// Everything else
		preparedStatement.setObject(parameterIndex, normalizedParameter);
	}

	private boolean shouldBindAsTimestampWithoutTimeZone(@NonNull StatementContext<?> statementContext,
																											 @NonNull Optional<ParameterSqlType> sqlTypeOptional) {
		requireNonNull(statementContext);
		requireNonNull(sqlTypeOptional);
		return statementContext.getAmbiguousTimestampBindingStrategy() == AmbiguousTimestampBindingStrategy.TIMESTAMP_WITHOUT_TIME_ZONE
				&& (sqlTypeOptional.isEmpty() || sqlTypeOptional.get().isUnknown());
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

	@NonNull
	protected boolean trySetObject(@NonNull PreparedStatement preparedStatement,
																 @NonNull Integer parameterIndex,
																 @Nullable Object parameter) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);

		try {
			preparedStatement.setObject(parameterIndex, parameter);
			return true;
		} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
			return false;
		} catch (SQLException e) {
			if (isUnsupportedSqlFeature(e))
				return false;
			throw e;
		}
	}

	private static boolean isUnsupportedSqlFeature(@NonNull SQLException e) {
		requireNonNull(e);

		String sqlState = e.getSQLState();
		if (sqlState != null && (sqlState.startsWith("0A") || "HYC00".equals(sqlState)))
			return true;

		SQLException next = e.getNextException();
		if (next != null && isUnsupportedSqlFeature(next))
			return true;

		Throwable cause = e.getCause();
		if (cause instanceof SQLException se && isUnsupportedSqlFeature(se))
			return true;

		String message = e.getMessage();
		if (message == null)
			return false;

		String lower = message.toLowerCase(Locale.ROOT);
		return lower.contains("not supported") || lower.contains("not implemented") || lower.contains("unsupported");
	}

	@NonNull
	protected boolean trySetObject(@NonNull PreparedStatement preparedStatement,
																 @NonNull Integer parameterIndex,
																 @Nullable Object parameter,
																 @NonNull Integer sqlType) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(sqlType);

		try {
			preparedStatement.setObject(parameterIndex, parameter, sqlType);
			return true;
		} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
			return false;
		} catch (SQLException e) {
			if (isUnsupportedSqlFeature(e))
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

	static final class ParameterSqlTypeResolver {
		@NonNull
		private final PreparedStatement preparedStatement;
		private boolean initialized;
		@Nullable
		private ParameterMetaData parameterMetaData;

		ParameterSqlTypeResolver(@NonNull PreparedStatement preparedStatement) {
			this.preparedStatement = requireNonNull(preparedStatement);
		}

		@NonNull
		Optional<ParameterSqlType> determineParameterSqlType(@NonNull Integer parameterIndex) throws SQLException {
			requireNonNull(parameterIndex);

			ParameterMetaData parameterMetaData = parameterMetaData();

			if (parameterMetaData == null)
				return Optional.empty();

			try {
				Integer sqlType = parameterMetaData.getParameterType(parameterIndex);
				String typeName = null;

				try {
					typeName = parameterMetaData.getParameterTypeName(parameterIndex);
				} catch (SQLFeatureNotSupportedException | AbstractMethodError ignored) {
				} catch (SQLException ignored) {
				}

				return Optional.of(new ParameterSqlType(sqlType, typeName));
			} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
				return Optional.empty();
			} catch (SQLException e) {
				return Optional.empty();
			}
		}

		@Nullable
		private ParameterMetaData parameterMetaData() throws SQLException {
			if (!this.initialized) {
				this.initialized = true;

				try {
					this.parameterMetaData = this.preparedStatement.getParameterMetaData();
				} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
					this.parameterMetaData = null;
				} catch (SQLException e) {
					this.parameterMetaData = null;
				}
			}

			return this.parameterMetaData;
		}
	}

	protected static final class ParameterSqlType {
		@NonNull
		private static final ParameterSqlType UNKNOWN = new ParameterSqlType(Types.OTHER, null);
		@NonNull
		private final Integer sqlType;
		@Nullable
		private final String typeName;

		private ParameterSqlType(@NonNull Integer sqlType,
														 @Nullable String typeName) {
			this.sqlType = requireNonNull(sqlType);
			this.typeName = typeName;
		}

		@NonNull
		Integer getSqlType() {
			return this.sqlType;
		}

		private boolean isTimestampWithTimeZone() {
			if (getSqlType() == Types.TIMESTAMP_WITH_TIMEZONE)
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

		private boolean isUnknown() {
			return getSqlType() == Types.OTHER && this.typeName == null;
		}
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
				lastException = e;
			}
		}

		try {
			preparedStatement.setNull(parameterIndex, primarySqlType);
			return;
		} catch (SQLException e) {
			lastException = e;
		}

		if (fallbackSqlType != null && fallbackSqlType != Types.NULL && !fallbackSqlType.equals(primarySqlType)) {
			try {
				preparedStatement.setNull(parameterIndex, fallbackSqlType);
				return;
			} catch (SQLException e) {
				lastException = e;
			}
		}

		if (primarySqlType != Types.NULL) {
			try {
				preparedStatement.setNull(parameterIndex, Types.NULL);
				return;
			} catch (SQLException e) {
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

		if (targetType.getTypeArguments().isEmpty())
			return this.bindersByRawClassCache.get(targetType.getRawClass());

		// Avoid caching parameterized types to prevent holding user classes under system raw types.
		return computeCustomBindersFor(targetType);
	}

	@NonNull
	private List<CustomParameterBinder> computeCustomBindersFor(@NonNull TargetType targetType) {
		requireNonNull(targetType);

		if (getCustomParameterBinders().isEmpty())
			return List.of();

		return getCustomParameterBinders().stream()
				.filter(b -> b.appliesTo(targetType))
				.toList();
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

		// Coerce to java.time whenever possible
		if (parameter instanceof java.sql.Timestamp timestamp)
			return timestamp.toInstant();
		if (parameter instanceof java.sql.Date date)
			return date.toLocalDate();
		if (parameter instanceof java.sql.Time time)
			return time.toLocalTime();
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
