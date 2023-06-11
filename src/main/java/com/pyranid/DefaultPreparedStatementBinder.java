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

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Basic implementation of {@link PreparedStatementBinder}.
 *
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
public class DefaultPreparedStatementBinder implements PreparedStatementBinder {
	private final DatabaseType databaseType;
	private final ZoneId timeZone;
	private final Calendar timeZoneCalendar;

	/**
	 * Creates a {@code PreparedStatementBinder} for the given {@code databaseType}.
	 *
	 * @param databaseType the type of database we're working with
	 */
	public DefaultPreparedStatementBinder(DatabaseType databaseType) {
		this(databaseType, ZoneId.systemDefault());
	}

	/**
	 * Creates a {@code PreparedStatementBinder} for the given {@code databaseType}.
	 *
	 * @param databaseType the type of database we're working with
	 * @param timeZone     the timezone to use when working with {@link java.sql.Timestamp} and similar values
	 * @since 1.0.15
	 */
	public DefaultPreparedStatementBinder(DatabaseType databaseType, ZoneId timeZone) {
		this.databaseType = requireNonNull(databaseType);
		this.timeZone = timeZone == null ? ZoneId.systemDefault() : timeZone;
		this.timeZoneCalendar = Calendar.getInstance(TimeZone.getTimeZone(this.timeZone));
	}

	@Override
	public void bind(PreparedStatement preparedStatement, List<Object> parameters) {
		requireNonNull(preparedStatement);
		requireNonNull(parameters);

		try {
			for (int i = 0; i < parameters.size(); ++i) {
				Object parameter = parameters.get(i);

				if (parameter != null) {
					Object normalizedParameter = normalizeParameter(parameter);

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
	protected Object normalizeParameter(Object parameter) {
		if (parameter == null)
			return null;

		if (parameter instanceof Date)
			return new Timestamp(((Date) parameter).getTime());
		if (parameter instanceof Instant)
			return new Timestamp(((Instant) parameter).toEpochMilli());
		if (parameter instanceof Locale)
			return ((Locale) parameter).toLanguageTag();
		if (parameter instanceof Enum)
			return ((Enum<?>) parameter).name();
		// Java 11 uses internal implementation java.time.ZoneRegion, which Postgres JDBC driver does not support.
		// Force ZoneId to use its ID here
		if (parameter instanceof ZoneId)
			return ((ZoneId) parameter).getId();

		// Special handling for Oracle
		if (databaseType() == DatabaseType.ORACLE) {
			if (parameter instanceof java.util.UUID) {
				ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
				byteBuffer.putLong(((UUID) parameter).getMostSignificantBits());
				byteBuffer.putLong(((UUID) parameter).getLeastSignificantBits());
				return byteBuffer.array();
			}

			// Other massaging here if needed...
		}

		return parameter;
	}

	/**
	 * What kind of database are we working with?
	 *
	 * @return the kind of database we're working with
	 */
	protected DatabaseType databaseType() {
		return this.databaseType;
	}

	protected ZoneId getTimeZone() {
		return timeZone;
	}

	protected Calendar getTimeZoneCalendar() {
		return timeZoneCalendar;
	}
}