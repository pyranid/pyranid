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
import java.util.Currency;
import java.util.Date;
import java.util.Locale;
import java.util.Optional;
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
public class DefaultPreparedStatementBinder implements PreparedStatementBinder {
	@Nonnull
	private final DatabaseType databaseType;
	@Nonnull
	private final ZoneId timeZone;

	/**
	 * Creates a {@code DefaultPreparedStatementBinder} for the given {@code databaseType} and DBMS {@code timeZone}.
	 *
	 * @param databaseType the type of database we're working with
	 * @param timeZone     the timezone to use when working with {@link java.sql.Timestamp} and similar values
	 * @since 2.1.0
	 */
	public DefaultPreparedStatementBinder(@Nonnull DatabaseType databaseType,
																				@Nonnull ZoneId timeZone) {
		requireNonNull(databaseType);
		requireNonNull(timeZone);

		this.databaseType = databaseType;
		this.timeZone = timeZone;
	}

	@Override
	public <T> void bindParameter(@Nonnull StatementContext<T> statementContext,
																@Nonnull PreparedStatement preparedStatement,
																@Nonnull Object parameter,
																@Nonnull Integer parameterIndex) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);
		requireNonNull(parameter);
		requireNonNull(parameterIndex);

		Object normalizedParameter = normalizeParameter(parameter).orElse(null);

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
			int sqlType = determineParameterSqlType(preparedStatement, parameterIndex);

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
			int sqlType = determineParameterSqlType(preparedStatement, parameterIndex);

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

		if (normalizedParameter instanceof ArrayParameter arrayParameter) {
			Object[] normalizedElements = normalizedArrayElements(arrayParameter.getElements());
			Array array = preparedStatement.getConnection().createArrayOf(arrayParameter.getBaseTypeName(), normalizedElements);
			preparedStatement.setArray(parameterIndex, array);
			return;
		}

		if (normalizedParameter instanceof VectorParameter vectorParameter) {
			if (getDatabaseType() != DatabaseType.POSTGRESQL)
				throw new IllegalArgumentException(format("%s supported only on %s.%s",
						VectorParameter.class.getSimpleName(), DatabaseType.class.getSimpleName(), DatabaseType.POSTGRESQL.name()));

			org.postgresql.util.PGobject pg = new org.postgresql.util.PGobject();
			pg.setType("vector");
			pg.setValue(toPostgresLiteralValue(vectorParameter));
			preparedStatement.setObject(parameterIndex, pg);
			return;
		}

		// Everything else
		preparedStatement.setObject(parameterIndex, normalizedParameter);
	}

	@Nonnull
	protected Object[] normalizedArrayElements(@Nonnull Object[] elements) {
		Object[] normalizedElements = new Object[elements.length];

		for (int j = 0; j < elements.length; j++)
			normalizedElements[j] = normalizeParameter(elements[j]).orElse(null);

		return normalizedElements;
	}

	@Nonnull
	protected boolean trySetObject(@Nonnull PreparedStatement preparedStatement,
																 @Nonnull Integer parameterIndex,
																 @Nullable Object object) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);

		try {
			preparedStatement.setObject(parameterIndex, object);
			return true;
		} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
			return false;
		}
	}

	@Nonnull
	protected boolean trySetObject(@Nonnull PreparedStatement preparedStatement,
																 @Nonnull Integer parameterIndex,
																 @Nullable Object object,
																 int sqlType) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);

		try {
			preparedStatement.setObject(parameterIndex, object, sqlType);
			return true;
		} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
			return false;
		}
	}

	protected int determineParameterSqlType(@Nonnull PreparedStatement preparedStatement,
																					@Nonnull Integer parameterIndex) {
		requireNonNull(preparedStatement);
		requireNonNull(parameterIndex);

		try {
			ParameterMetaData parameterMetaData = preparedStatement.getParameterMetaData();
			return parameterMetaData != null ? parameterMetaData.getParameterType(parameterIndex) : Integer.MIN_VALUE;
		} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
			return Integer.MIN_VALUE;
		} catch (SQLException e) {
			return Integer.MIN_VALUE;
		}
	}

	@Nonnull
	protected String toPostgresLiteralValue(@Nonnull VectorParameter vectorParameter) {
		requireNonNull(vectorParameter);

		double[] elements = vectorParameter.getElements();
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
	 * @param parameter the parameter to (possibly) massage
	 * @return the result of the massaging process
	 */
	@Nonnull
	protected Optional<Object> normalizeParameter(@Nullable Object parameter) {
		if (parameter == null)
			return Optional.empty();

		// Coerce to java.time whenever possible
		if (parameter instanceof java.sql.Timestamp timestamp)
			return Optional.of(timestamp.toInstant());
		if (parameter instanceof java.sql.Date date)
			return Optional.of(date.toLocalDate());
		if (parameter instanceof java.sql.Time time)
			return Optional.of(time.toLocalTime());
		if (parameter instanceof Date date)
			return Optional.of(Instant.ofEpochMilli(date.getTime()));
		if (parameter instanceof ZonedDateTime zonedDateTime)
			return Optional.of(zonedDateTime.toOffsetDateTime());
		if (parameter instanceof Locale)
			return Optional.of(((Locale) parameter).toLanguageTag());
		if (parameter instanceof Currency)
			return Optional.of(((Currency) parameter).getCurrencyCode());
		if (parameter instanceof Enum)
			return Optional.of(((Enum<?>) parameter).name());
		if (parameter instanceof ZoneId)
			return Optional.of(((ZoneId) parameter).getId());

		// Special handling for Oracle
		if (getDatabaseType() == DatabaseType.ORACLE) {
			if (parameter instanceof java.util.UUID) {
				ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
				byteBuffer.putLong(((UUID) parameter).getMostSignificantBits());
				byteBuffer.putLong(((UUID) parameter).getLeastSignificantBits());
				return Optional.of(byteBuffer.array());
			}
		}

		return Optional.ofNullable(parameter);
	}

	@Nonnull
	protected DatabaseType getDatabaseType() {
		return this.databaseType;
	}

	@Nonnull
	protected ZoneId getTimeZone() {
		return this.timeZone;
	}
}