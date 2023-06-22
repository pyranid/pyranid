/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2023 Revetware LLC.
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
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Basic implementation of {@link PreparedStatementBinder}.
 *
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public class DefaultPreparedStatementBinder implements PreparedStatementBinder {
	@Nonnull
	private final DatabaseType databaseType;
	@Nonnull
	private final ZoneId timeZone;
	@Nonnull
	private final Calendar timeZoneCalendar;

	/**
	 * Creates a {@code PreparedStatementBinder}.
	 */
	public DefaultPreparedStatementBinder() {
		this(null, null);
	}

	/**
	 * Creates a {@code PreparedStatementBinder} for the given {@code databaseType}.
	 *
	 * @param databaseType the type of database we're working with
	 */
	public DefaultPreparedStatementBinder(@Nullable DatabaseType databaseType) {
		this(null, null);
	}

	/**
	 * Creates a {@code PreparedStatementBinder} for the given {@code databaseType}.
	 *
	 * @param databaseType the type of database we're working with
	 * @param timeZone     the timezone to use when working with {@link java.sql.Timestamp} and similar values
	 * @since 1.0.15
	 */
	public DefaultPreparedStatementBinder(@Nullable DatabaseType databaseType,
																				@Nullable ZoneId timeZone) {
		this.databaseType = databaseType == null ? DatabaseType.GENERIC : databaseType;
		this.timeZone = timeZone == null ? ZoneId.systemDefault() : timeZone;
		this.timeZoneCalendar = Calendar.getInstance(TimeZone.getTimeZone(this.timeZone));
	}

	@Override
	public <T> void bind(@Nonnull PreparedStatement preparedStatement,
											 @Nonnull StatementContext<T> statementContext) {
		requireNonNull(preparedStatement);
		requireNonNull(statementContext);

		List<Object> parameters = statementContext.getParameters();

		try {
			for (int i = 0; i < parameters.size(); ++i) {
				Object parameter = parameters.get(i);

				if (parameter != null) {
					Object normalizedParameter = normalizeParameter(parameter).orElse(null);

					if (normalizedParameter instanceof java.sql.Timestamp) {
						java.sql.Timestamp timestamp = (java.sql.Timestamp) normalizedParameter;
						preparedStatement.setTimestamp(i + 1, timestamp, getTimeZoneCalendar());
					} else if (normalizedParameter instanceof java.sql.Date) {
						java.sql.Date date = (java.sql.Date) normalizedParameter;
						preparedStatement.setDate(i + 1, date, getTimeZoneCalendar());
					} else if (normalizedParameter instanceof java.sql.Time) {
						java.sql.Time time = (java.sql.Time) normalizedParameter;
						preparedStatement.setTime(i + 1, time, getTimeZoneCalendar());
					} else {
						preparedStatement.setObject(i + 1, normalizedParameter);
					}
				} else {
					preparedStatement.setObject(i + 1, parameter);
				}
			}
		} catch (Exception e) {
			throw new DatabaseException(e);
		}
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

		if (parameter instanceof Date)
			return Optional.of(new Timestamp(((Date) parameter).getTime()));
		if (parameter instanceof Instant)
			return Optional.of(new Timestamp(((Instant) parameter).toEpochMilli()));
		if (parameter instanceof Locale)
			return Optional.of(((Locale) parameter).toLanguageTag());
		if (parameter instanceof Enum)
			return Optional.of(((Enum<?>) parameter).name());
		// Java 11 uses internal implementation java.time.ZoneRegion, which Postgres JDBC driver does not support.
		// Force ZoneId to use its ID here
		if (parameter instanceof ZoneId)
			return Optional.of(((ZoneId) parameter).getId());

		// Special handling for Oracle
		if (databaseType() == DatabaseType.ORACLE) {
			if (parameter instanceof java.util.UUID) {
				ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
				byteBuffer.putLong(((UUID) parameter).getMostSignificantBits());
				byteBuffer.putLong(((UUID) parameter).getLeastSignificantBits());
				return Optional.of(byteBuffer.array());
			}

			// Other massaging here if needed...
		}

		return Optional.ofNullable(parameter);
	}

	@Nonnull
	protected DatabaseType databaseType() {
		return this.databaseType;
	}

	@Nonnull
	protected ZoneId getTimeZone() {
		return timeZone;
	}

	@Nonnull
	protected Calendar getTimeZoneCalendar() {
		return timeZoneCalendar;
	}
}