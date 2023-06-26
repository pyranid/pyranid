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
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Basic implementation of {@link ResultSetMapper}.
 *
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public class DefaultResultSetMapper implements ResultSetMapper {
	@Nonnull
	private final DatabaseType databaseType;
	@Nonnull
	private final InstanceProvider instanceProvider;
	@Nonnull
	private final ZoneId timeZone;
	@Nonnull
	private final Calendar timeZoneCalendar;
	@Nonnull
	private final Map<Class<?>, Map<String, Set<String>>> columnLabelAliasesByPropertyNameCache =
			new ConcurrentHashMap<>();

	/**
	 * Creates a {@code ResultSetMapper} for the given {@code instanceProvider}.
	 *
	 * @param instanceProvider instance-creation factory, used to instantiate resultset row objects as needed
	 */
	public DefaultResultSetMapper(@Nonnull InstanceProvider instanceProvider) {
		this(null, requireNonNull(instanceProvider), null);
	}

	/**
	 * Creates a {@code ResultSetMapper} for the given {@code databaseType} and {@code instanceProvider}.
	 *
	 * @param databaseType     the type of database we're working with
	 * @param instanceProvider instance-creation factory, used to instantiate resultset row objects as needed
	 */
	public DefaultResultSetMapper(@Nullable DatabaseType databaseType,
																@Nonnull InstanceProvider instanceProvider) {
		this(databaseType, requireNonNull(instanceProvider), null);
	}

	/**
	 * Creates a {@code ResultSetMapper} for the given {@code databaseType} and {@code instanceProvider}.
	 *
	 * @param databaseType     the type of database we're working with
	 * @param instanceProvider instance-creation factory, used to instantiate resultset row objects as needed
	 * @param timeZone         the timezone to use when working with {@link java.sql.Timestamp} and similar values
	 * @since 1.0.15
	 */
	public DefaultResultSetMapper(@Nullable DatabaseType databaseType,
																@Nonnull InstanceProvider instanceProvider,
																@Nullable ZoneId timeZone) {
		requireNonNull(instanceProvider);

		this.databaseType = databaseType == null ? DatabaseType.GENERIC : databaseType;
		this.instanceProvider = instanceProvider;
		this.timeZone = timeZone == null ? ZoneId.systemDefault() : timeZone;
		this.timeZoneCalendar = Calendar.getInstance(TimeZone.getTimeZone(this.timeZone));
	}

	@Override
	@Nonnull
	public <T> T map(@Nonnull StatementContext<T> statementContext,
									 @Nonnull ResultSet resultSet) {
		requireNonNull(statementContext);
		requireNonNull(resultSet);

		Class<T> resultSetRowType = statementContext.getResultSetRowType().get();

		try {
			StandardTypeResult<T> standardTypeResult = mapResultSetToStandardType(resultSet, resultSetRowType);

			if (standardTypeResult.isStandardType())
				return standardTypeResult.value().orElse(null);

			if (resultSetRowType.isRecord())
				return (T) mapResultSetToRecord((StatementContext<? extends Record>) statementContext, resultSet);

			return mapResultSetToBean(statementContext, resultSet);
		} catch (DatabaseException e) {
			throw e;
		} catch (Exception e) {
			throw new DatabaseException(format("Unable to map JDBC %s to %s", ResultSet.class.getSimpleName(), resultSetRowType),
					e);
		}
	}

	/**
	 * Attempts to map the current {@code resultSet} row to an instance of {@code resultClass} using one of the
	 * "out-of-the-box" types (primitives, common types like {@link UUID}, etc.
	 * <p>
	 * This does not attempt to map to a user-defined JavaBean - see {@link #mapResultSetToBean(ResultSet, Class)} for
	 * that functionality.
	 *
	 * @param <T>         result instance type token
	 * @param resultSet   provides raw row data to pull from
	 * @param resultClass the type of instance to map to
	 * @return the result of the mapping
	 * @throws Exception if an error occurs during mapping
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Nonnull
	protected <T> StandardTypeResult<T> mapResultSetToStandardType(@Nonnull ResultSet resultSet,
																																 @Nonnull Class<T> resultClass) throws Exception {
		requireNonNull(resultSet);
		requireNonNull(resultClass);

		Object value = null;
		boolean standardType = true;

		if (resultClass.isAssignableFrom(Byte.class) || resultClass.isAssignableFrom(byte.class)) {
			value = resultSet.getByte(1);
		} else if (resultClass.isAssignableFrom(Short.class) || resultClass.isAssignableFrom(short.class)) {
			value = resultSet.getShort(1);
		} else if (resultClass.isAssignableFrom(Integer.class) || resultClass.isAssignableFrom(int.class)) {
			value = resultSet.getInt(1);
		} else if (resultClass.isAssignableFrom(Long.class) || resultClass.isAssignableFrom(long.class)) {
			value = resultSet.getLong(1);
		} else if (resultClass.isAssignableFrom(Float.class) || resultClass.isAssignableFrom(float.class)) {
			value = resultSet.getFloat(1);
		} else if (resultClass.isAssignableFrom(Double.class) || resultClass.isAssignableFrom(double.class)) {
			value = resultSet.getDouble(1);
		} else if (resultClass.isAssignableFrom(Boolean.class) || resultClass.isAssignableFrom(boolean.class)) {
			value = resultSet.getBoolean(1);
		} else if (resultClass.isAssignableFrom(Character.class) || resultClass.isAssignableFrom(char.class)) {
			String string = resultSet.getString(1);

			if (string != null)
				if (string.length() == 1)
					value = string.charAt(0);
				else
					throw new DatabaseException(format("Cannot map String value '%s' to %s", resultClass.getSimpleName()));
		} else if (resultClass.isAssignableFrom(String.class)) {
			value = resultSet.getString(1);
		} else if (resultClass.isAssignableFrom(byte[].class)) {
			value = resultSet.getBytes(1);
		} else if (resultClass.isAssignableFrom(Enum.class)) {
			value = Enum.valueOf((Class) resultClass, resultSet.getString(1));
		} else if (resultClass.isAssignableFrom(UUID.class)) {
			String string = resultSet.getString(1);

			if (string != null)
				value = UUID.fromString(string);
		} else if (resultClass.isAssignableFrom(BigDecimal.class)) {
			value = resultSet.getBigDecimal(1);
		} else if (resultClass.isAssignableFrom(BigInteger.class)) {
			BigDecimal bigDecimal = resultSet.getBigDecimal(1);

			if (bigDecimal != null)
				value = bigDecimal.toBigInteger();
		} else if (resultClass.isAssignableFrom(Date.class)) {
			value = resultSet.getTimestamp(1, getTimeZoneCalendar());
		} else if (resultClass.isAssignableFrom(Instant.class)) {
			Timestamp timestamp = resultSet.getTimestamp(1, getTimeZoneCalendar());

			if (timestamp != null)
				value = timestamp.toInstant();
		} else if (resultClass.isAssignableFrom(LocalDate.class)) {
			value = resultSet.getObject(1); // DATE
		} else if (resultClass.isAssignableFrom(LocalTime.class)) {
			value = resultSet.getObject(1); // TIME
		} else if (resultClass.isAssignableFrom(LocalDateTime.class)) {
			value = resultSet.getObject(1); // TIMESTAMP
		} else if (resultClass.isAssignableFrom(OffsetTime.class)) {
			value = resultSet.getObject(1); // TIME WITH TIMEZONE
		} else if (resultClass.isAssignableFrom(OffsetDateTime.class)) {
			value = resultSet.getObject(1); // TIMESTAMP WITH TIMEZONE
		} else if (resultClass.isAssignableFrom(java.sql.Date.class)) {
			value = resultSet.getDate(1, getTimeZoneCalendar());
		} else if (resultClass.isAssignableFrom(ZoneId.class)) {
			String zoneId = resultSet.getString(1);

			if (zoneId != null)
				value = ZoneId.of(zoneId);
		} else if (resultClass.isAssignableFrom(TimeZone.class)) {
			String timeZone = resultSet.getString(1);

			if (timeZone != null)
				value = TimeZone.getTimeZone(timeZone);
		} else if (resultClass.isAssignableFrom(Locale.class)) {
			String locale = resultSet.getString(1);

			if (locale != null)
				value = Locale.forLanguageTag(locale);
		} else if (resultClass.isEnum()) {
			value = extractEnumValue(resultClass, resultSet.getObject(1));

			// TODO: revisit java.sql.* handling

			// } else if (resultClass.isAssignableFrom(java.sql.Blob.class)) {
			// value = resultSet.getBlob(1);
			// } else if (resultClass.isAssignableFrom(java.sql.Clob.class)) {
			// value = resultSet.getClob(1);
			// } else if (resultClass.isAssignableFrom(java.sql.Clob.class)) {
			// value = resultSet.getClob(1);

		} else {
			standardType = false;
		}

		if (standardType) {
			int columnCount = resultSet.getMetaData().getColumnCount();

			if (columnCount != 1) {
				List<String> columnLabels = new ArrayList<>(columnCount);

				for (int i = 1; i <= columnCount; ++i)
					columnLabels.add(resultSet.getMetaData().getColumnLabel(i));

				throw new DatabaseException(format("Expected 1 column to map to %s but encountered %s instead (%s)",
						resultClass, columnCount, columnLabels.stream().collect(joining(", "))));
			}
		}

		return new StandardTypeResult(value, standardType);
	}

	/**
	 * Attempts to map the current {@code resultSet} row to an instance of {@code resultClass}, which must be a
	 * Record.
	 * <p>
	 * The {@code resultClass} instance will be created via {@link #instanceProvider()}.
	 *
	 * @param <T>              result instance type token
	 * @param statementContext current SQL context
	 * @param resultSet        provides raw row data to pull from
	 * @return the result of the mapping
	 * @throws Exception if an error occurs during mapping
	 */
	@Nonnull
	protected <T extends Record> T mapResultSetToRecord(@Nonnull StatementContext<T> statementContext,
																											@Nonnull ResultSet resultSet) throws Exception {
		requireNonNull(statementContext);
		requireNonNull(resultSet);

		Class<T> resultSetRowType = statementContext.getResultSetRowType().get();

		RecordComponent[] recordComponents = resultSetRowType.getRecordComponents();
		Map<String, Set<String>> columnLabelAliasesByPropertyName = determineColumnLabelAliasesByPropertyName(resultSetRowType);
		Map<String, Object> columnLabelsToValues = extractColumnLabelsToValues(resultSet);
		Object[] args = new Object[recordComponents.length];

		for (int i = 0; i < recordComponents.length; ++i) {
			RecordComponent recordComponent = recordComponents[i];

			String propertyName = recordComponent.getName();

			// If there are any @DatabaseColumn annotations on this field, respect them
			Set<String> potentialPropertyNames = columnLabelAliasesByPropertyName.get(propertyName);

			// There were no @DatabaseColumn annotations, use the default naming strategy
			if (potentialPropertyNames == null || potentialPropertyNames.size() == 0)
				potentialPropertyNames = databaseColumnNamesForPropertyName(propertyName);

			// Set the value for the Record ctor
			for (String potentialPropertyName : potentialPropertyNames)
				if (columnLabelsToValues.containsKey(potentialPropertyName))
					args[i] = columnLabelsToValues.get(potentialPropertyName);
		}

		T record = instanceProvider().provideRecord(statementContext, resultSetRowType, args);

		return record;
	}

	/**
	 * Attempts to map the current {@code resultSet} row to an instance of {@code resultClass}, which should be a
	 * JavaBean.
	 * <p>
	 * The {@code resultClass} instance will be created via {@link #instanceProvider()}.
	 *
	 * @param <T>              result instance type token
	 * @param statementContext current SQL context
	 * @param resultSet        provides raw row data to pull from
	 * @return the result of the mapping
	 * @throws Exception if an error occurs during mapping
	 */
	@Nonnull
	protected <T> T mapResultSetToBean(@Nonnull StatementContext<T> statementContext,
																		 @Nonnull ResultSet resultSet) throws Exception {
		requireNonNull(statementContext);
		requireNonNull(resultSet);

		Class<T> resultSetRowType = statementContext.getResultSetRowType().get();

		T object = instanceProvider().provide(statementContext, resultSetRowType);
		BeanInfo beanInfo = Introspector.getBeanInfo(resultSetRowType);
		Map<String, Object> columnLabelsToValues = extractColumnLabelsToValues(resultSet);
		Map<String, Set<String>> columnLabelAliasesByPropertyName = determineColumnLabelAliasesByPropertyName(resultSetRowType);

		for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
			Method writeMethod = propertyDescriptor.getWriteMethod();

			if (writeMethod == null)
				continue;

			Parameter parameter = writeMethod.getParameters()[0];

			// Pull in property names, taking into account any aliases defined by @DatabaseColumn
			Set<String> propertyNames = columnLabelAliasesByPropertyName.get(propertyDescriptor.getName());

			if (propertyNames == null)
				propertyNames = new HashSet<>();
			else
				propertyNames = new HashSet<>(propertyNames);

			// If no @DatabaseColumn annotation, then use the field name itself
			if (propertyNames.size() == 0)
				propertyNames.add(propertyDescriptor.getName());

			// Normalize property names to database column names.
			// For example, a property name of "address1" would get normalized to the set of "address1" and "address_1" by
			// default
			propertyNames =
					propertyNames.stream().map(propertyName -> databaseColumnNamesForPropertyName(propertyName))
							.flatMap(columnNames -> columnNames.stream()).collect(toSet());

			for (String propertyName : propertyNames) {
				if (columnLabelsToValues.containsKey(propertyName)) {
					Object value = convertResultSetValueToPropertyType(columnLabelsToValues.get(propertyName), parameter.getType()).orElse(null);
					Class<?> writeMethodParameterType = writeMethod.getParameterTypes()[0];

					if (value != null && !writeMethodParameterType.isAssignableFrom(value.getClass())) {
						String resultSetTypeDescription = value.getClass().toString();

						throw new DatabaseException(
								format(
										"Property '%s' of %s has a write method of type %s, but the ResultSet type %s does not match. "
												+ "Consider creating your own %s and overriding convertResultSetValueToPropertyType() to detect instances of %s and convert them to %s",
										propertyDescriptor.getName(), resultSetRowType, writeMethodParameterType, resultSetTypeDescription,
										DefaultResultSetMapper.class.getSimpleName(), resultSetTypeDescription, writeMethodParameterType));
					}

					writeMethod.invoke(object, value);
				}
			}
		}

		return object;
	}

	@Nonnull
	protected Map<String, Set<String>> determineColumnLabelAliasesByPropertyName(@Nonnull Class<?> resultClass) {
		requireNonNull(resultClass);

		return columnLabelAliasesByPropertyNameCache.computeIfAbsent(
				resultClass,
				(key) -> {
					Map<String, Set<String>> cachedColumnLabelAliasesByPropertyName = new HashMap<>();

					for (Field field : resultClass.getDeclaredFields()) {
						DatabaseColumn databaseColumn = field.getAnnotation(DatabaseColumn.class);

						if (databaseColumn != null)
							cachedColumnLabelAliasesByPropertyName.put(
									field.getName(),
									unmodifiableSet(asList(databaseColumn.value()).stream()
											.map(columnLabel -> normalizeColumnLabel(columnLabel)).collect(toSet())));
					}

					return unmodifiableMap(cachedColumnLabelAliasesByPropertyName);
				});
	}

	@Nonnull
	protected Map<String, Object> extractColumnLabelsToValues(@Nonnull ResultSet resultSet) throws SQLException {
		requireNonNull(resultSet);

		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
		int columnCount = resultSetMetaData.getColumnCount();
		Set<String> columnLabels = new HashSet<>(columnCount);

		for (int i = 0; i < columnCount; i++)
			columnLabels.add(resultSetMetaData.getColumnLabel(i + 1));

		Map<String, Object> columnLabelsToValues = new HashMap<>(columnLabels.size());

		for (String columnLabel : columnLabels) {
			Object resultSetValue = resultSet.getObject(columnLabel);

			// If DB gives us time-related values, re-pull using specific RS methods so we can apply a timezone
			if (resultSetValue != null) {
				if (resultSetValue instanceof java.sql.Timestamp) {
					resultSetValue = resultSet.getTimestamp(columnLabel, getTimeZoneCalendar());
				} else if (resultSetValue instanceof java.sql.Date) {
					resultSetValue = resultSet.getDate(columnLabel, getTimeZoneCalendar());
				} else if (resultSetValue instanceof java.sql.Time) {
					resultSetValue = resultSet.getTime(columnLabel, getTimeZoneCalendar());
				}
			}

			columnLabelsToValues.put(normalizeColumnLabel(columnLabel), resultSetValue);
		}

		return columnLabelsToValues;
	}

	/**
	 * Massages a {@link ResultSet#getObject(String)} value to match the given {@code propertyType}.
	 * <p>
	 * For example, the JDBC driver might give us {@link java.sql.Timestamp} but our corresponding JavaBean field is of
	 * type {@link java.util.Date}, so we need to manually convert that ourselves.
	 *
	 * @param resultSetValue the value returned by {@link ResultSet#getObject(String)}
	 * @param propertyType   the JavaBean property type we'd like to map {@code resultSetValue} to
	 * @return a representation of {@code resultSetValue} that is of type {@code propertyType}
	 */
	@Nonnull
	protected Optional<Object> convertResultSetValueToPropertyType(Object resultSetValue, Class<?> propertyType) {
		requireNonNull(propertyType);

		if (resultSetValue == null)
			return Optional.empty();

		if (resultSetValue instanceof BigDecimal) {
			BigDecimal bigDecimal = (BigDecimal) resultSetValue;

			if (BigDecimal.class.isAssignableFrom(propertyType))
				return Optional.ofNullable(bigDecimal);
			if (BigInteger.class.isAssignableFrom(propertyType))
				return Optional.of(bigDecimal.toBigInteger());
		}

		if (resultSetValue instanceof BigInteger) {
			BigInteger bigInteger = (BigInteger) resultSetValue;

			if (BigDecimal.class.isAssignableFrom(propertyType))
				return Optional.of(new BigDecimal(bigInteger));
			if (BigInteger.class.isAssignableFrom(propertyType))
				return Optional.ofNullable(bigInteger);
		}

		if (resultSetValue instanceof Number) {
			Number number = (Number) resultSetValue;

			if (Byte.class.isAssignableFrom(propertyType))
				return Optional.of(number.byteValue());
			if (Short.class.isAssignableFrom(propertyType))
				return Optional.of(number.shortValue());
			if (Integer.class.isAssignableFrom(propertyType))
				return Optional.of(number.intValue());
			if (Long.class.isAssignableFrom(propertyType))
				return Optional.of(number.longValue());
			if (Float.class.isAssignableFrom(propertyType))
				return Optional.of(number.floatValue());
			if (Double.class.isAssignableFrom(propertyType))
				return Optional.of(number.doubleValue());
			if (BigDecimal.class.isAssignableFrom(propertyType))
				return Optional.of(new BigDecimal(number.doubleValue()));
			if (BigInteger.class.isAssignableFrom(propertyType))
				return Optional.of(new BigDecimal(number.doubleValue()).toBigInteger());
		} else if (resultSetValue instanceof java.sql.Timestamp) {
			java.sql.Timestamp date = (java.sql.Timestamp) resultSetValue;

			if (Date.class.isAssignableFrom(propertyType))
				return Optional.ofNullable(date);
			if (Instant.class.isAssignableFrom(propertyType))
				return Optional.of(date.toInstant());
			if (LocalDate.class.isAssignableFrom(propertyType))
				return Optional.of(date.toInstant().atZone(getTimeZone()).toLocalDate());
			if (LocalDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(date.toLocalDateTime());
		} else if (resultSetValue instanceof java.sql.Date) {
			java.sql.Date date = (java.sql.Date) resultSetValue;

			if (Date.class.isAssignableFrom(propertyType))
				return Optional.ofNullable(date);
			if (Instant.class.isAssignableFrom(propertyType))
				return Optional.of(date.toInstant());
			if (LocalDate.class.isAssignableFrom(propertyType))
				return Optional.of(date.toLocalDate());
			if (LocalDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(LocalDateTime.ofInstant(date.toInstant(), getTimeZone()));
		} else if (resultSetValue instanceof java.sql.Time) {
			java.sql.Time time = (java.sql.Time) resultSetValue;

			if (LocalTime.class.isAssignableFrom(propertyType))
				return Optional.ofNullable(time.toLocalTime());
		} else if (propertyType.isAssignableFrom(ZoneId.class)) {
			return Optional.ofNullable(ZoneId.of(resultSetValue.toString()));
		} else if (propertyType.isAssignableFrom(TimeZone.class)) {
			return Optional.ofNullable(TimeZone.getTimeZone(resultSetValue.toString()));
		} else if (propertyType.isAssignableFrom(Locale.class)) {
			return Optional.ofNullable(Locale.forLanguageTag(resultSetValue.toString()));
		} else if (propertyType.isEnum()) {
			return Optional.ofNullable(extractEnumValue(propertyType, resultSetValue));
		} else if ("org.postgresql.util.PGobject".equals(resultSetValue.getClass().getName())) {
			org.postgresql.util.PGobject pgObject = (org.postgresql.util.PGobject) resultSetValue;
			return Optional.ofNullable(pgObject.getValue());
		}

		return Optional.ofNullable(resultSetValue);
	}

	/**
	 * Attempts to convert {@code object} to a corresponding value for enum type {@code enumClass}.
	 * <p>
	 * Normally {@code object} is a {@code String}, but other types may be used - the {@code toString()} method of
	 * {@code object} will be invoked to determine the final value for conversion.
	 *
	 * @param enumClass the enum to which we'd like to convert {@code object}
	 * @param object    the object to convert to an enum value
	 * @return the enum value of {@code object} for {@code enumClass}
	 * @throws DatabaseException if {@code object} does not correspond to a valid enum value
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Nonnull
	protected Enum<?> extractEnumValue(@Nonnull Class<?> enumClass,
																		 @Nonnull Object object) {
		requireNonNull(enumClass);
		requireNonNull(object);

		if (!enumClass.isEnum())
			throw new IllegalArgumentException(format("%s is not an enum type", enumClass));

		String objectAsString = object.toString();

		try {
			return Enum.valueOf((Class<? extends Enum>) enumClass, objectAsString);
		} catch (IllegalArgumentException | NullPointerException e) {
			throw new DatabaseException(format("The value '%s' is not present in enum %s", objectAsString, enumClass), e);
		}
	}

	/**
	 * Massages a {@link ResultSet} column label so it's easier to match against a JavaBean property name.
	 * <p>
	 * This implementation lowercases the label using the locale provided by {@link #normalizationLocale()}.
	 *
	 * @param columnLabel the {@link ResultSet} column label to massage
	 * @return the massaged label
	 */
	@Nonnull
	protected String normalizeColumnLabel(@Nonnull String columnLabel) {
		requireNonNull(columnLabel);
		return columnLabel.toLowerCase(normalizationLocale());
	}

	/**
	 * Massages a JavaBean property name to match standard database column name (camelCase -> camel_case).
	 * <p>
	 * Uses {@link #normalizationLocale()} to perform case-changing.
	 * <p>
	 * There may be multiple database column name mappings, for example property {@code address1} might map to both
	 * {@code address1} and {@code address_1} column names.
	 *
	 * @param propertyName the JavaBean property name to massage
	 * @return the column names that match the JavaBean property name
	 */
	@Nonnull
	protected Set<String> databaseColumnNamesForPropertyName(@Nonnull String propertyName) {
		requireNonNull(propertyName);

		Set<String> normalizedPropertyNames = new HashSet<>(2);

		// Converts camelCase to camel_case
		String camelCaseRegex = "([a-z])([A-Z]+)";
		String replacement = "$1_$2";

		String normalizedPropertyName =
				propertyName.replaceAll(camelCaseRegex, replacement).toLowerCase(normalizationLocale());
		normalizedPropertyNames.add(normalizedPropertyName);

		// Converts address1 to address_1
		String letterFollowedByNumberRegex = "(\\D)(\\d)";
		String normalizedNumberPropertyName = normalizedPropertyName.replaceAll(letterFollowedByNumberRegex, replacement);
		normalizedPropertyNames.add(normalizedNumberPropertyName);

		return normalizedPropertyNames;
	}

	/**
	 * The locale to use when massaging JDBC column names for matching against JavaBean property names.
	 * <p>
	 * Used by {@link #normalizeColumnLabel(String)}.
	 *
	 * @return the locale to use for massaging, hardcoded to {@link Locale#ENGLISH} by default
	 */
	@Nonnull
	protected Locale normalizationLocale() {
		return ENGLISH;
	}

	/**
	 * What kind of database are we working with?
	 *
	 * @return the kind of database we're working with
	 */
	@Nonnull
	protected DatabaseType databaseType() {
		return this.databaseType;
	}

	/**
	 * Returns an instance-creation factory, used to instantiate resultset row objects as needed.
	 *
	 * @return the instance-creation factory
	 */
	@Nonnull
	protected InstanceProvider instanceProvider() {
		return this.instanceProvider;
	}

	@Nonnull
	protected ZoneId getTimeZone() {
		return timeZone;
	}

	@Nonnull
	protected Calendar getTimeZoneCalendar() {
		return timeZoneCalendar;
	}

	/**
	 * The result of attempting to map a {@link ResultSet} to a "standard" type like primitive or {@link UUID}.
	 *
	 * @author <a href="https://www.revetware.com">Mark Allen</a>
	 * @since 1.0.0
	 */
	@ThreadSafe
	protected static class StandardTypeResult<T> {
		@Nullable
		private final T value;
		@Nonnull
		private final Boolean standardType;

		/**
		 * Creates a {@code StandardTypeResult} with the given {@code value} and {@code standardType} flag.
		 *
		 * @param value        the mapping result, may be {@code null}
		 * @param standardType {@code true} if the mapped type was a standard type, {@code false} otherwise
		 */
		public StandardTypeResult(@Nullable T value,
															@Nonnull Boolean standardType) {
			requireNonNull(standardType);

			this.value = value;
			this.standardType = standardType;
		}

		/**
		 * Gets the result of the mapping.
		 *
		 * @return the mapping result value, may be {@code null}
		 */
		@Nonnull
		public Optional<T> value() {
			return Optional.ofNullable(this.value);
		}

		/**
		 * Was the mapped type a standard type?
		 *
		 * @return {@code true} if this was a standard type, {@code false} otherwise
		 */
		@Nonnull
		public Boolean isStandardType() {
			return this.standardType;
		}
	}
}