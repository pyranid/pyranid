/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2025 Revetware LLC.
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
import com.pyranid.JsonParameter.BindingPreference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
	@Nonnull
	private final List<CustomParameterBinder> customParameterBinders;

	// Cache: which binders apply for a given target type?
	@Nonnull
	private final ConcurrentMap<TargetType, List<CustomParameterBinder>> bindersByTargetTypeCache;

	// Cache: which binder last won for (valueClass, sqlType)?
	@Nonnull
	private final ConcurrentMap<InboundKey, CustomParameterBinder> preferredBinderByInboundKey;


	DefaultPreparedStatementBinder() {
		this(List.of());
	}

	DefaultPreparedStatementBinder(@Nonnull List<CustomParameterBinder> customParameterBinders) {
		requireNonNull(customParameterBinders);
		this.customParameterBinders = Collections.unmodifiableList(customParameterBinders);
		this.bindersByTargetTypeCache = new ConcurrentHashMap<>();
		this.preferredBinderByInboundKey = new ConcurrentHashMap<>();
	}

	@Override
	public <T> void bindParameter(@Nonnull StatementContext<T> statementContext,
																@Nonnull PreparedStatement preparedStatement,
																@Nonnull Integer parameterIndex,
																@Nonnull Object parameter) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(parameter);

		// Unwrap typed parameters if needed
		Object rawParameter = (parameter instanceof TypedParameter typedParameter) ? typedParameter.getValue().orElse(null) : parameter;
		TargetType explicitTargetType =
				(parameter instanceof TypedParameter typedParameter)
						? TargetType.of(typedParameter.getExplicitType())
						: TargetType.of(rawParameter.getClass());

		Optional<Integer> sqlTypeOptional = determineParameterSqlType(preparedStatement, parameterIndex);

			if (rawParameter == null) {
				if (sqlTypeOptional.isPresent())
					preparedStatement.setNull(parameterIndex, sqlTypeOptional.get());
				else
					preparedStatement.setNull(parameterIndex, Types.NULL);

				return;
			}

		Integer sqlType = sqlTypeOptional.orElse(Types.OTHER);

		// Try custom binders first (if they exist) on the raw value
		if (!getCustomParameterBinders().isEmpty()
				&& tryCustomBinders(statementContext, preparedStatement, parameterIndex, rawParameter, sqlType, explicitTargetType))
			return;

		// If this parameter was explicitly wrapped as a TypedParameter (e.g., Parameters.listOf/setOf/mapOf/typedArrayOf)
		// and no CustomParameterBinder claimed it, fail fast with a clear message.
		if (parameter instanceof TypedParameter typedParameter)
			throw new DatabaseException(format("This parameter requires a CustomParameterBinder: %s. "
							+ "Parameters.listOf/setOf/mapOf/typedArrayOf are typed wrappers intended for use with custom binders. "
							+ "Register a CustomParameterBinder that appliesTo(%s), "
							+ "or use a concrete parameter type supported by the binder/driver (e.g. Parameters.arrayOf(...) or Parameters.json(...)).",
					typedParameter.getExplicitType(), typedParameter.getExplicitType()));

		Object normalizedParameter = normalizeParameter(statementContext, rawParameter);

		if (normalizedParameter instanceof LocalDate localDate) {
			if (!trySetObject(preparedStatement, parameterIndex, localDate, Types.DATE))
				preparedStatement.setDate(parameterIndex, java.sql.Date.valueOf(localDate)); // fallback

			return;
		}

		if (normalizedParameter instanceof LocalTime localTime) {
			// Some drivers used to offset LocalTime; safest is a tz-free string.
			preparedStatement.setString(parameterIndex, localTime.toString());
			return;
		}

		if (normalizedParameter instanceof LocalDateTime localDateTime) {
			if (!trySetObject(preparedStatement, parameterIndex, localDateTime, Types.TIMESTAMP))
				preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(localDateTime)); // fallback

			return;
		}

		if (normalizedParameter instanceof OffsetDateTime offsetDateTime) {
			if (sqlType == Types.TIMESTAMP) {
				// Coerce to DB zone and drop the offset.
				LocalDateTime localDateTime = offsetDateTime.atZoneSameInstant(statementContext.getTimeZone()).toLocalDateTime();
				if (!trySetObject(preparedStatement, parameterIndex, localDateTime, Types.TIMESTAMP))
					preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(localDateTime));

				return;
			}

			if (sqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
				if (!trySetObject(preparedStatement, parameterIndex, offsetDateTime, Types.TIMESTAMP_WITH_TIMEZONE))
					preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(offsetDateTime.toInstant()));

				return;
			}

			// Unknown target: prefer preserving the offset/instant.
			if (!trySetObject(preparedStatement, parameterIndex, offsetDateTime))
				preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(offsetDateTime.toInstant()));

			return;
		}

		if (normalizedParameter instanceof Instant instant) {
			if (sqlType == Types.TIMESTAMP) {
				LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, statementContext.getTimeZone());

				if (!trySetObject(preparedStatement, parameterIndex, localDateTime, Types.TIMESTAMP))
					preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(localDateTime));

				return;
			}

			// Default (and for TIMESTAMP WITH TIME ZONE): keep the instant.
			if (!trySetObject(preparedStatement, parameterIndex, instant, Types.TIMESTAMP_WITH_TIMEZONE))
				preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(instant));

			return;
		}

		if (normalizedParameter instanceof java.time.OffsetTime offsetTime) {
			// If driver supports TIME WITH TIME ZONE, use it; else fall back to string.
			if (!trySetObject(preparedStatement, parameterIndex, offsetTime, Types.TIME_WITH_TIMEZONE) && !trySetObject(preparedStatement, parameterIndex, offsetTime))
				preparedStatement.setString(parameterIndex, offsetTime.toString());

			return;
		}

		if (normalizedParameter instanceof ArrayParameter<?> arrayParameter) {
			Object[] elements = arrayParameter.getElements().orElse(null);

			if (elements == null) {
				preparedStatement.setNull(parameterIndex, Types.NULL);
			} else {
				Object[] normalizedElements = normalizedArrayElements(statementContext, elements);
				Array array = preparedStatement.getConnection().createArrayOf(arrayParameter.getBaseTypeName(), normalizedElements);
				preparedStatement.setArray(parameterIndex, array);
			}

			return;
		}

		if (normalizedParameter instanceof VectorParameter vectorParameter) {
			if (statementContext.getDatabaseType() != DatabaseType.POSTGRESQL)
				throw new IllegalArgumentException(format("%s supported only on %s.%s",
						VectorParameter.class.getSimpleName(), DatabaseType.class.getSimpleName(), DatabaseType.POSTGRESQL.name()));

			double[] elements = vectorParameter.getElements().orElse(null);

			if (elements == null) {
				preparedStatement.setNull(parameterIndex, Types.NULL);
			} else {
				org.postgresql.util.PGobject pg = new org.postgresql.util.PGobject();
				pg.setType("vector");
				pg.setValue(toPostgresVectorLiteralValue(elements));
				preparedStatement.setObject(parameterIndex, pg);
			}

			return;
		}

		if (normalizedParameter instanceof JsonParameter jsonParameter) {
			String json = jsonParameter.getJson().orElse(null);

			if (json == null) {
				preparedStatement.setNull(parameterIndex, Types.NULL);
			} else {
				// For now, only special handling for PostgreSQL.
				// Later, we can add more handling for other DB types.
				if (statementContext.getDatabaseType() == DatabaseType.POSTGRESQL) {
					org.postgresql.util.PGobject pg = new org.postgresql.util.PGobject();
					pg.setType(jsonParameter.getBindingPreference() == BindingPreference.TEXT ? "json" : "jsonb");
					pg.setValue(json);
					preparedStatement.setObject(parameterIndex, pg);
				} else {
					preparedStatement.setString(parameterIndex, json);
				}
			}

			return;
		}

		// Everything else
		preparedStatement.setObject(parameterIndex, normalizedParameter);
	}

	@Nonnull
	protected <T> Object[] normalizedArrayElements(@Nonnull StatementContext<T> statementContext,
																								 @Nonnull Object[] elements) {
		requireNonNull(statementContext);
		requireNonNull(elements);

		Object[] normalizedElements = new Object[elements.length];

		for (int j = 0; j < elements.length; j++) {
			Object element = elements[j];
			normalizedElements[j] = element == null ? null : normalizeParameter(statementContext, element);
		}

		return normalizedElements;
	}

	@Nonnull
	protected boolean trySetObject(@Nonnull PreparedStatement preparedStatement,
																 @Nonnull Integer parameterIndex,
																 @Nullable Object parameter) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);

		try {
			preparedStatement.setObject(parameterIndex, parameter);
			return true;
		} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
			return false;
		}
	}

	@Nonnull
	protected boolean trySetObject(@Nonnull PreparedStatement preparedStatement,
																 @Nonnull Integer parameterIndex,
																 @Nullable Object parameter,
																 @Nonnull Integer sqlType) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(sqlType);

		try {
			preparedStatement.setObject(parameterIndex, parameter, sqlType);
			return true;
		} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
			return false;
		}
	}

	@Nonnull
	protected Optional<Integer> determineParameterSqlType(@Nonnull PreparedStatement preparedStatement,
																												@Nonnull Integer parameterIndex) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);

		try {
			ParameterMetaData parameterMetaData = preparedStatement.getParameterMetaData();

			if (parameterMetaData == null)
				return Optional.empty();

			return Optional.of(parameterMetaData.getParameterType(parameterIndex));
		} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
			return Optional.empty();
		}
	}

	/**
	 * Try custom binders for the given value. Returns true if a binder handled it.
	 */
	@Nonnull
	protected <T> Boolean tryCustomBinders(@Nonnull StatementContext<T> statementContext,
																				 @Nonnull PreparedStatement preparedStatement,
																				 @Nonnull Integer parameterIndex,
																				 @Nonnull Object parameter, /* must be the UNWRAPPED value */
																				 @Nonnull Integer sqlType,
																				 @Nonnull TargetType targetType /* explicit target (from TypedParameter if present) */) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);
		requireNonNull(parameter);
		requireNonNull(sqlType);
		requireNonNull(targetType);

		// use the provided target instead of TargetType.of(parameter.getClass())
		List<CustomParameterBinder> candidates = customBindersFor(targetType);

		if (candidates.isEmpty())
			return false;

		InboundKey key = new InboundKey(parameter.getClass(), sqlType, targetType);
		CustomParameterBinder cached = getPreferredBinderByInboundKey().get(key);

		// Fast path: try cached binder first
		if (cached != null) {
			BindingResult bindingResult = requireNonNull(cached.bind(statementContext, preparedStatement, parameterIndex, parameter));

			if (bindingResult instanceof BindingResult.Handled)
				return true;

			// If it no longer applies, fall through to the rest (keep the hint; may win next time)
		}

		// Slow path: scan applicable binders
		for (CustomParameterBinder customParameterBinder : candidates) {
			BindingResult bindingResult = requireNonNull(customParameterBinder.bind(statementContext, preparedStatement, parameterIndex, parameter));

			if (bindingResult instanceof BindingResult.Handled) {
				getPreferredBinderByInboundKey().putIfAbsent(key, customParameterBinder);
				return true;
			}
		}

		return false;
	}

	/**
	 * Filter and cache the binders whose {@code appliesTo(targetType)} returned true.
	 */
	@Nonnull
	protected List<CustomParameterBinder> customBindersFor(@Nonnull TargetType targetType) {
		requireNonNull(targetType);

		if (getCustomParameterBinders().isEmpty())
			return List.of();

		return getBindersByTargetTypeCache().computeIfAbsent(targetType, tt -> {
			// Evaluate once per unique TargetType instance (equals/hash provided by DefaultTargetType)
			new Object(); // noop anchor for clarity
			return getCustomParameterBinders().stream()
					.filter(b -> b.appliesTo(tt))
					.toList();
		});
	}

	@ThreadSafe
	protected static final class InboundKey {
		@Nonnull
		private final Class<?> valueClass;
		@Nonnull
		private final Integer sqlType;
		@Nonnull
		private final TargetType targetType;

		InboundKey(@Nonnull Class<?> valueClass,
							 @Nonnull Integer sqlType,
							 @Nonnull TargetType targetType) {
			requireNonNull(valueClass);
			requireNonNull(sqlType);
			requireNonNull(targetType);

			this.valueClass = valueClass;
			this.sqlType = sqlType;
			this.targetType = targetType;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			InboundKey that = (InboundKey) o;
			return valueClass.equals(that.valueClass) &&
					(sqlType == null ? that.sqlType == null : sqlType.equals(that.sqlType)) &&
					targetType.equals(that.targetType);
		}

		@Override
		public int hashCode() {
			int result = valueClass.hashCode();
			result = 31 * result + (sqlType == null ? 0 : sqlType.hashCode());
			result = 31 * result + targetType.hashCode();
			return result;
		}
	}

	@Nonnull
	protected String toPostgresVectorLiteralValue(@Nonnull double[] elements) {
		requireNonNull(elements);

		StringBuilder sb = new StringBuilder(2 + elements.length * 8);

		sb.append('[');

		for (int i = 0; i < elements.length; i++) {
			if (i > 0) sb.append(", ");
			// Use Java default formatting (locale-independent) which is fine for pgvector
			sb.append(Double.toString(elements[i]));
		}

		sb.append(']');

		return sb.toString();
	}

	/**
	 * Massages a parameter into a JDBC-friendly format if needed.
	 * <p>
	 * For example, we need to do special work to prepare a {@link UUID} for Oracle.
	 *
	 * @param statementContext current SQL context
	 * @param parameter        the parameter to (possibly) massage
	 * @return the result of the massaging process
	 */
	@Nonnull
	protected <T> Object normalizeParameter(@Nonnull StatementContext<T> statementContext,
																					@Nonnull Object parameter) {
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

		// Special handling for Oracle
		if (statementContext.getDatabaseType() == DatabaseType.ORACLE) {
			if (parameter instanceof java.util.UUID) {
				ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
				byteBuffer.putLong(((UUID) parameter).getMostSignificantBits());
				byteBuffer.putLong(((UUID) parameter).getLeastSignificantBits());
				return byteBuffer.array();
			}
		}

		return parameter;
	}

	@Nonnull
	protected List<CustomParameterBinder> getCustomParameterBinders() {
		return this.customParameterBinders;
	}

	@Nonnull
	protected ConcurrentMap<TargetType, List<CustomParameterBinder>> getBindersByTargetTypeCache() {
		return this.bindersByTargetTypeCache;
	}

	@Nonnull
	protected ConcurrentMap<InboundKey, CustomParameterBinder> getPreferredBinderByInboundKey() {
		return this.preferredBinderByInboundKey;
	}
}
