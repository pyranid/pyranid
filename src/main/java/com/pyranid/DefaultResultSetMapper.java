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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Currency;
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
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Basic implementation of {@link ResultSetMapper}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public class DefaultResultSetMapper implements ResultSetMapper {
	@Nonnull
	private final Locale normalizationLocale;
	@Nonnull
	private final Map<Class<?>, Map<String, Set<String>>> columnLabelAliasesByPropertyNameCache =
			new ConcurrentHashMap<>();

	/**
	 * Enables per-column {@link ResultSet} mapping customization.
	 */
	@FunctionalInterface
	public interface ColumnMappingCustomizer {
		/**
		 * Return Optional.of(mapped) to short-circuit default mapping,
		 * or Optional.empty() to let DefaultResultSetMapper proceed.
		 *
		 * @param statementContext    current SQL context
		 * @param resultSet           provides raw row data to pull from*
		 * @param resultSetColumnType the type to which the {@link ResultSet} column should be marshaled
		 * @param originalValue       the already-read value
		 * @param columnIndex         1-based column index, if available
		 * @param columnLabel         normalized column label, if available
		 * @param instanceProvider    instance-creation factory, may be used to instantiate values
		 * @return an {@link Optional} which holds the preferred value for this {@link ResultSet} column, or {@link Optional#empty()} to fall back to default mapping
		 */
		@Nonnull
		Optional<?> map(@Nonnull StatementContext<?> statementContext,
										@Nonnull ResultSet resultSet,
										@Nonnull Class<?> resultSetColumnType,
										@Nonnull Object originalValue,
										@Nullable Integer columnIndex,
										@Nullable String columnLabel,
										@Nonnull InstanceProvider instanceProvider) throws SQLException;
	}

	/**
	 * Creates a {@code ResultSetMapper} with an {@link Locale#ENGLISH} {@code normalizationLocale}.
	 * <p>
	 * The {@code normalizationLocale} is used when massaging JDBC column names for matching against JavaBean property names.
	 */
	public DefaultResultSetMapper() {
		this(Locale.ENGLISH);
	}

	/**
	 * Creates a {@code ResultSetMapper} for the given {@code normalizationLocale}.
	 *
	 * @param normalizationLocale The locale to use when massaging JDBC column names for matching against JavaBean property names.
	 */
	public DefaultResultSetMapper(@Nonnull Locale normalizationLocale) {
		requireNonNull(normalizationLocale);
		this.normalizationLocale = normalizationLocale;
	}

	@Override
	@Nonnull
	public <T> Optional<T> map(@Nonnull StatementContext<T> statementContext,
														 @Nonnull ResultSet resultSet,
														 @Nonnull Class<T> resultSetRowType,
														 @Nonnull InstanceProvider instanceProvider) {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetRowType);
		requireNonNull(instanceProvider);

		try {
			StandardTypeResult<T> standardTypeResult = mapResultSetToStandardType(statementContext, resultSet, resultSetRowType);

			if (standardTypeResult.isStandardType())
				return standardTypeResult.getValue();

			if (resultSetRowType.isRecord())
				return Optional.ofNullable((T) mapResultSetToRecord((StatementContext<? extends Record>) statementContext, resultSet, instanceProvider));

			return Optional.ofNullable(mapResultSetToBean(statementContext, resultSet, instanceProvider));
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
	 * This does not attempt to map to a user-defined JavaBean - see {@link #mapResultSetToBean(StatementContext, ResultSet, InstanceProvider)} for
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
	protected <T> StandardTypeResult<T> mapResultSetToStandardType(@Nonnull StatementContext<T> statementContext,
																																 @Nonnull ResultSet resultSet,
																																 @Nonnull Class<T> resultClass) throws Exception {
		requireNonNull(statementContext);
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
				if (string.length() == 1) value = string.charAt(0);
				else throw new DatabaseException(format("Cannot map String value '%s' to %s", resultClass.getSimpleName()));
		} else if (resultClass.isAssignableFrom(String.class)) {
			value = resultSet.getString(1);
		} else if (resultClass.isAssignableFrom(byte[].class)) {
			value = resultSet.getBytes(1);
		} else if (resultClass.isAssignableFrom(Enum.class)) {
			value = Enum.valueOf((Class) resultClass, resultSet.getString(1));
		} else if (resultClass.isAssignableFrom(UUID.class)) {
			String string = resultSet.getString(1);
			if (string != null) value = UUID.fromString(string);
		} else if (resultClass.isAssignableFrom(BigDecimal.class)) {
			value = resultSet.getBigDecimal(1);
		} else if (resultClass.isAssignableFrom(BigInteger.class)) {
			BigDecimal bd = resultSet.getBigDecimal(1);
			if (bd != null) value = bd.toBigInteger();
		} else if (resultClass.isAssignableFrom(Date.class)) {
			Instant inst = TemporalReaders.asInstant(resultSet, 1, statementContext);
			value = (inst == null) ? null : Date.from(inst);
		} else if (resultClass.isAssignableFrom(Instant.class)) {
			value = TemporalReaders.asInstant(resultSet, 1, statementContext);
		} else if (resultClass.isAssignableFrom(LocalDate.class)) {
			value = TemporalReaders.asLocalDate(resultSet, 1);
		} else if (resultClass.isAssignableFrom(LocalTime.class)) {
			value = TemporalReaders.asLocalTime(resultSet, 1);
		} else if (resultClass.isAssignableFrom(LocalDateTime.class)) {
			value = TemporalReaders.asLocalDateTime(resultSet, 1, statementContext);
		} else if (resultClass.isAssignableFrom(OffsetTime.class)) {
			value = TemporalReaders.asOffsetTime(resultSet, 1, statementContext);
		} else if (resultClass.isAssignableFrom(OffsetDateTime.class)) {
			value = TemporalReaders.asOffsetDateTime(resultSet, 1, statementContext);
		} else if (resultClass.isAssignableFrom(java.sql.Date.class)) {
			LocalDate ld = TemporalReaders.asLocalDate(resultSet, 1);
			value = (ld == null) ? null : java.sql.Date.valueOf(ld);
		} else if (resultClass.isAssignableFrom(ZoneId.class)) {
			String zoneId = resultSet.getString(1);
			if (zoneId != null) value = ZoneId.of(zoneId);
		} else if (resultClass.isAssignableFrom(TimeZone.class)) {
			String tz = resultSet.getString(1);
			if (tz != null) value = TimeZone.getTimeZone(tz);
		} else if (resultClass.isAssignableFrom(Locale.class)) {
			String locale = resultSet.getString(1);
			if (locale != null) value = Locale.forLanguageTag(locale);
		} else if (resultClass.isAssignableFrom(Currency.class)) {
			String currency = resultSet.getString(1);
			if (currency != null) value = Currency.getInstance(currency);
		} else if (resultClass.isEnum()) {
			value = extractEnumValue(resultClass, resultSet.getObject(1));
		} else {
			standardType = false;
		}

		if (standardType) {
			int columnCount = resultSet.getMetaData().getColumnCount();
			if (columnCount != 1) {
				List<String> labels = new ArrayList<>(columnCount);
				for (int i = 1; i <= columnCount; ++i) labels.add(resultSet.getMetaData().getColumnLabel(i));
				throw new DatabaseException(format("Expected 1 column to map to %s but encountered %s instead (%s)",
						resultClass, columnCount, labels.stream().collect(joining(", "))));
			}
		}

		return new StandardTypeResult(value, standardType);
	}

	/**
	 * Attempts to map the current {@code resultSet} row to an instance of {@code resultClass}, which must be a
	 * Record.
	 *
	 * @param <T>              result instance type token
	 * @param statementContext current SQL context
	 * @param resultSet        provides raw row data to pull from
	 * @param instanceProvider an instance-creation factory, used to instantiate resultset row objects as needed.
	 * @return the result of the mapping
	 * @throws Exception if an error occurs during mapping
	 */
	@Nonnull
	protected <T extends Record> T mapResultSetToRecord(@Nonnull StatementContext<T> statementContext,
																											@Nonnull ResultSet resultSet,
																											@Nonnull InstanceProvider instanceProvider) throws Exception {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(instanceProvider);

		Class<T> resultSetRowType = statementContext.getResultSetRowType().get();

		RecordComponent[] recordComponents = resultSetRowType.getRecordComponents();
		Map<String, Set<String>> columnLabelAliasesByPropertyName = determineColumnLabelAliasesByPropertyName(resultSetRowType);
		Map<String, Object> columnLabelsToValues = extractColumnLabelsToValues(statementContext, resultSet);
		Object[] args = new Object[recordComponents.length];

		for (int i = 0; i < recordComponents.length; ++i) {
			RecordComponent recordComponent = recordComponents[i];

			String propertyName = recordComponent.getName();

			// If there are any @DatabaseColumn annotations on this field, respect them
			Set<String> potentialPropertyNames = columnLabelAliasesByPropertyName.get(propertyName);

			// There were no @DatabaseColumn annotations, use the default naming strategy
			if (potentialPropertyNames == null || potentialPropertyNames.size() == 0)
				potentialPropertyNames = databaseColumnNamesForPropertyName(propertyName);

			Class<?> recordComponentType = recordComponent.getType();

			// Set the value for the Record ctor
			for (String potentialPropertyName : potentialPropertyNames) {
				if (columnLabelsToValues.containsKey(potentialPropertyName)) {
					Object value = convertResultSetValueToPropertyType(statementContext, columnLabelsToValues.get(potentialPropertyName), recordComponentType).orElse(null);

					if (value != null && !recordComponentType.isAssignableFrom(value.getClass())) {
						String resultSetTypeDescription = value.getClass().toString();

						throw new DatabaseException(
								format(
										"Property '%s' of %s has a write method of type %s, but the ResultSet type %s does not match. "
												+ "Consider creating your own %s and overriding convertResultSetValueToPropertyType() to detect instances of %s and convert them to %s",
										recordComponent.getName(), resultSetRowType, recordComponentType, resultSetTypeDescription,
										DefaultResultSetMapper.class.getSimpleName(), resultSetTypeDescription, recordComponentType));
					}

					args[i] = value;
				}
			}
		}

		return instanceProvider.provideRecord(statementContext, resultSetRowType, args);
	}

	/**
	 * Attempts to map the current {@code resultSet} row to an instance of {@code resultClass}, which should be a
	 * JavaBean.
	 *
	 * @param <T>              result instance type token
	 * @param statementContext current SQL context
	 * @param resultSet        provides raw row data to pull from
	 * @param instanceProvider an instance-creation factory, used to instantiate resultset row objects as needed.
	 * @return the result of the mapping
	 * @throws Exception if an error occurs during mapping
	 */
	@Nonnull
	protected <T> T mapResultSetToBean(@Nonnull StatementContext<T> statementContext,
																		 @Nonnull ResultSet resultSet,
																		 @Nonnull InstanceProvider instanceProvider) throws Exception {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(instanceProvider);

		Class<T> resultSetRowType = statementContext.getResultSetRowType().get();

		T object = instanceProvider.provide(statementContext, resultSetRowType);
		BeanInfo beanInfo = Introspector.getBeanInfo(resultSetRowType);
		Map<String, Object> columnLabelsToValues = extractColumnLabelsToValues(statementContext, resultSet);
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
					Object value = convertResultSetValueToPropertyType(statementContext, columnLabelsToValues.get(propertyName), parameter.getType()).orElse(null);
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
	protected <T> Map<String, Object> extractColumnLabelsToValues(@Nonnull StatementContext<T> statementContext,
																																@Nonnull ResultSet resultSet) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);

		requireNonNull(statementContext);
		requireNonNull(resultSet);

		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
		int columnCount = resultSetMetaData.getColumnCount();
		Map<String, Object> columnLabelsToValues = new HashMap<>(columnCount);

		for (int i = 1; i <= columnCount; i++) {
			String label = normalizeColumnLabel(resultSetMetaData.getColumnLabel(i));
			int jdbcType = resultSetMetaData.getColumnType(i);

			Object resultSetValue;
			boolean tsTz = TemporalReaders.isTimestampWithTimeZone(resultSetMetaData, i, statementContext.getDatabaseType());
			boolean timeTz = TemporalReaders.isTimeWithTimeZone(resultSetMetaData, i);

			if (tsTz) {
				resultSetValue = TemporalReaders.asOffsetDateTime(resultSet, i, statementContext);
			} else if (jdbcType == Types.TIMESTAMP) {
				resultSetValue = TemporalReaders.asLocalDateTime(resultSet, i, statementContext);
			} else if (jdbcType == Types.DATE) {
				resultSetValue = TemporalReaders.asLocalDate(resultSet, i);
			} else if (timeTz) {
				resultSetValue = TemporalReaders.asOffsetTime(resultSet, i, statementContext);
			} else if (jdbcType == Types.TIME) {
				resultSetValue = TemporalReaders.asLocalTime(resultSet, i);
			} else {
				// Non-temporal or unknown: take the driver’s native object (PGobject, BigDecimal, etc.)
				resultSetValue = resultSet.getObject(i);
			}

			columnLabelsToValues.put(label, resultSetValue);
		}

		return columnLabelsToValues;
	}

	/**
	 * Massages a {@link ResultSet#getObject(String)} value to match the given {@code propertyType}.
	 * <p>
	 * For example, the JDBC driver might give us {@link java.sql.Timestamp} but our corresponding JavaBean field is of
	 * type {@link java.util.Date}, so we need to manually convert that ourselves.
	 *
	 * @param statementContext current SQL context
	 * @param resultSetValue   the value returned by {@link ResultSet#getObject(String)}
	 * @param propertyType     the JavaBean property type we'd like to map {@code resultSetValue} to
	 * @return a representation of {@code resultSetValue} that is of type {@code propertyType}
	 */
	@Nonnull
	protected <T> Optional<Object> convertResultSetValueToPropertyType(@Nonnull StatementContext<T> statementContext,
																																		 @Nullable Object resultSetValue,
																																		 @Nonnull Class<?> propertyType) {
		requireNonNull(statementContext);
		requireNonNull(propertyType);

		if (resultSetValue == null)
			return Optional.empty();

		// ---------- Numbers ----------
		if (resultSetValue instanceof BigDecimal bigDecimal) {
			if (BigDecimal.class.isAssignableFrom(propertyType))
				return Optional.of(bigDecimal);
			if (BigInteger.class.isAssignableFrom(propertyType))
				return Optional.of(bigDecimal.toBigInteger());
		}

		if (resultSetValue instanceof BigInteger bigInteger) {
			if (BigDecimal.class.isAssignableFrom(propertyType))
				return Optional.of(new BigDecimal(bigInteger));
			if (BigInteger.class.isAssignableFrom(propertyType))
				return Optional.of(bigInteger);
		}

		if (resultSetValue instanceof Number number) {
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
				return Optional.of(BigDecimal.valueOf(number.doubleValue()));
			if (BigInteger.class.isAssignableFrom(propertyType))
				return Optional.of(BigDecimal.valueOf(number.doubleValue()).toBigInteger());
		}

		// Legacy java.sql.* coming from drivers
		if (resultSetValue instanceof java.sql.Timestamp timestamp) {
			if (Date.class.isAssignableFrom(propertyType))
				return Optional.of(timestamp);
			if (Instant.class.isAssignableFrom(propertyType))
				return Optional.of(timestamp.toInstant());
			if (LocalDate.class.isAssignableFrom(propertyType))
				return Optional.of(timestamp.toLocalDateTime().toLocalDate());
			if (LocalDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(timestamp.toLocalDateTime());
			if (OffsetDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(timestamp.toInstant().atZone(statementContext.getTimeZone()).toOffsetDateTime());
		}

		if (resultSetValue instanceof java.sql.Date date) {
			if (Date.class.isAssignableFrom(propertyType))
				return Optional.of(date);
			if (Instant.class.isAssignableFrom(propertyType))
				return Optional.of(date.toInstant());
			if (LocalDate.class.isAssignableFrom(propertyType))
				return Optional.of(date.toLocalDate());
			if (LocalDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(LocalDateTime.ofInstant(date.toInstant(), statementContext.getTimeZone()));
		}

		if (resultSetValue instanceof java.sql.Time time) {
			if (LocalTime.class.isAssignableFrom(propertyType))
				return Optional.of(time.toLocalTime());
		}

		// New java.time values (preferred)
		if (resultSetValue instanceof Instant instant) {
			if (Instant.class.isAssignableFrom(propertyType))
				return Optional.of(instant);
			if (Date.class.isAssignableFrom(propertyType))
				return Optional.of(Date.from(instant));
			if (LocalDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(instant.atZone(statementContext.getTimeZone()).toLocalDateTime());
			if (LocalDate.class.isAssignableFrom(propertyType))
				return Optional.of(instant.atZone(statementContext.getTimeZone()).toLocalDate());
			if (OffsetDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(instant.atZone(statementContext.getTimeZone()).toOffsetDateTime());
			if (java.sql.Timestamp.class.isAssignableFrom(propertyType))
				return Optional.of(Timestamp.from(instant));
			if (java.sql.Date.class.isAssignableFrom(propertyType))
				return Optional.of(java.sql.Date.valueOf(instant.atZone(statementContext.getTimeZone()).toLocalDate()));
		}

		if (resultSetValue instanceof LocalDateTime localDateTime) {
			if (LocalDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(localDateTime);
			if (Instant.class.isAssignableFrom(propertyType))
				return Optional.of(localDateTime.atZone(statementContext.getTimeZone()).toInstant());
			if (Date.class.isAssignableFrom(propertyType))
				return Optional.of(Date.from(localDateTime.atZone(statementContext.getTimeZone()).toInstant()));
			if (LocalDate.class.isAssignableFrom(propertyType))
				return Optional.of(localDateTime.toLocalDate());
			if (OffsetDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(localDateTime.atZone(statementContext.getTimeZone()).toOffsetDateTime());
			if (java.sql.Timestamp.class.isAssignableFrom(propertyType))
				return Optional.of(Timestamp.valueOf(localDateTime));
			if (java.sql.Date.class.isAssignableFrom(propertyType))
				return Optional.of(java.sql.Date.valueOf(localDateTime.toLocalDate()));
		}

		if (resultSetValue instanceof OffsetDateTime offsetDateTime) {
			if (OffsetDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(offsetDateTime);
			if (Instant.class.isAssignableFrom(propertyType))
				return Optional.of(offsetDateTime.toInstant());
			if (LocalDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(offsetDateTime.atZoneSameInstant(statementContext.getTimeZone()).toLocalDateTime());
			if (Date.class.isAssignableFrom(propertyType))
				return Optional.of(Date.from(offsetDateTime.toInstant()));
			if (java.sql.Timestamp.class.isAssignableFrom(propertyType))
				return Optional.of(Timestamp.from(offsetDateTime.toInstant()));
		}

		if (resultSetValue instanceof LocalDate localDate) {
			if (LocalDate.class.isAssignableFrom(propertyType))
				return Optional.of(localDate);
			if (LocalDateTime.class.isAssignableFrom(propertyType))
				return Optional.of(localDate.atStartOfDay());
			if (Instant.class.isAssignableFrom(propertyType))
				return Optional.of(localDate.atStartOfDay(statementContext.getTimeZone()).toInstant());
			if (java.sql.Date.class.isAssignableFrom(propertyType))
				return Optional.of(java.sql.Date.valueOf(localDate));
			if (Date.class.isAssignableFrom(propertyType))
				return Optional.of(Date.from(localDate.atStartOfDay(statementContext.getTimeZone()).toInstant()));
		}

		if (resultSetValue instanceof LocalTime localTime) {
			if (LocalTime.class.isAssignableFrom(propertyType))
				return Optional.of(localTime);
			if (java.sql.Time.class.isAssignableFrom(propertyType))
				return Optional.of(java.sql.Time.valueOf(localTime));
		}

		if (resultSetValue instanceof OffsetTime offsetTime) {
			if (OffsetTime.class.isAssignableFrom(propertyType))
				return Optional.of(offsetTime);
			if (LocalTime.class.isAssignableFrom(propertyType))
				return Optional.of(offsetTime.toLocalTime());
			if (java.sql.Time.class.isAssignableFrom(propertyType))
				return Optional.of(java.sql.Time.valueOf(offsetTime.toLocalTime()));
		}

		if (propertyType.isAssignableFrom(ZoneId.class)) {
			return Optional.ofNullable(ZoneId.of(resultSetValue.toString()));
		} else if (propertyType.isAssignableFrom(TimeZone.class)) {
			return Optional.ofNullable(TimeZone.getTimeZone(resultSetValue.toString()));
		} else if (propertyType.isAssignableFrom(Locale.class)) {
			return Optional.ofNullable(Locale.forLanguageTag(resultSetValue.toString()));
		} else if (propertyType.isAssignableFrom(Currency.class)) {
			return Optional.ofNullable(Currency.getInstance(resultSetValue.toString()));
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
	 * This implementation lowercases the label using the locale provided by {@link #getNormalizationLocale()}.
	 *
	 * @param columnLabel the {@link ResultSet} column label to massage
	 * @return the massaged label
	 */
	@Nonnull
	protected String normalizeColumnLabel(@Nonnull String columnLabel) {
		requireNonNull(columnLabel);
		return columnLabel.toLowerCase(getNormalizationLocale());
	}

	/**
	 * Massages a JavaBean property name to match standard database column name (camelCase -> camel_case).
	 * <p>
	 * Uses {@link #getNormalizationLocale()} to perform case-changing.
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
				propertyName.replaceAll(camelCaseRegex, replacement).toLowerCase(getNormalizationLocale());
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
	 * @return the locale to use for massaging
	 */
	@Nonnull
	protected Locale getNormalizationLocale() {
		return this.normalizationLocale;
	}

	/**
	 * The result of attempting to map a {@link ResultSet} to a "standard" type like primitive or {@link UUID}.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
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
		public Optional<T> getValue() {
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

	@ThreadSafe
	protected static final class TemporalReaders {
		private TemporalReaders() {}

		/**
		 * Detect if the current column is TIMESTAMP WITH TIME ZONE (or equivalent)
		 */
		public static boolean isTimestampWithTimeZone(ResultSetMetaData md, int col, DatabaseType dbType) throws SQLException {
			int jdbcType = md.getColumnType(col);
			if (jdbcType == Types.TIMESTAMP_WITH_TIMEZONE) return true;

			@Nullable String typeName = md.getColumnTypeName(col);
			if (typeName == null) return false;
			String u = typeName.toUpperCase(Locale.ROOT);

			// Heuristics for drivers that still report plain TIMESTAMP:
			// PostgreSQL: TIMESTAMPTZ, "timestamp with time zone"
			// Oracle: "TIMESTAMP WITH TIME ZONE"
			// HSQLDB: "TIMESTAMP WITH TIME ZONE"
			return u.contains("WITH TIME ZONE") || u.contains("TIMESTAMPTZ");
		}

		public static boolean isTimeWithTimeZone(ResultSetMetaData md, int col) throws SQLException {
			int jdbcType = md.getColumnType(col);
			if (jdbcType == Types.TIME_WITH_TIMEZONE) return true;
			@Nullable String name = md.getColumnTypeName(col);
			return name != null && name.toUpperCase(Locale.ROOT).contains("TIME WITH TIME ZONE");
		}

		/**
		 * Try JDBC 4.2 getObject; return null if unsupported.
		 */
		@Nullable
		public static <T> T tryGet(ResultSet rs, int col, Class<T> cls) throws SQLException {
			try {
				return rs.getObject(col, cls);
			} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
				return null;
			}
		}

		/**
		 * Normalize to millisecond precision to avoid test flakiness across drivers.
		 */
		public static Instant toMillis(Instant inst) {
			return inst == null ? null : inst.truncatedTo(ChronoUnit.MILLIS);
		}

		// ==== Targeted readers =====================================================

		public static Instant asInstant(ResultSet rs, int col, StatementContext<?> ctx) throws SQLException {
			ResultSetMetaData md = rs.getMetaData();
			boolean withTz = isTimestampWithTimeZone(md, col, ctx.getDatabaseType());

			// Modern fast-path
			if (withTz) {
				OffsetDateTime odt = tryGet(rs, col, OffsetDateTime.class);
				if (odt != null) return toMillis(odt.toInstant());
			} else {
				LocalDateTime ldt = tryGet(rs, col, LocalDateTime.class);
				if (ldt != null) return toMillis(ldt.atZone(ctx.getTimeZone()).toInstant());
			}

			// Tolerant fallbacks
			Object raw = rs.getObject(col);
			if (raw == null) return null;

			if (raw instanceof Timestamp ts) {
				// DO NOT use ts.toInstant() for WITHOUT TZ; interpret using DB zone
				LocalDateTime ldt = ts.toLocalDateTime();
				return toMillis(ldt.atZone(ctx.getTimeZone()).toInstant());
			}
			if (raw instanceof OffsetDateTime odt) return toMillis(odt.toInstant());
			if (raw instanceof LocalDateTime ldt) return toMillis(ldt.atZone(ctx.getTimeZone()).toInstant());
			if (raw instanceof String s) {
				// ISO-8601 string literal? Try parse to OffsetDateTime / LocalDateTime.
				try {
					return toMillis(OffsetDateTime.parse(s).toInstant());
				} catch (DateTimeParseException ignore) {
				}
				try {
					return toMillis(LocalDateTime.parse(s).atZone(ctx.getTimeZone()).toInstant());
				} catch (DateTimeParseException ignore) {
				}
			}

			// Last resort: try classic getter
			Timestamp ts = rs.getTimestamp(col);
			if (ts != null) {
				LocalDateTime ldt = ts.toLocalDateTime();
				return toMillis(ldt.atZone(ctx.getTimeZone()).toInstant());
			}
			return null;
		}

		public static OffsetDateTime asOffsetDateTime(ResultSet rs, int col, StatementContext<?> ctx) throws SQLException {
			ResultSetMetaData md = rs.getMetaData();
			boolean withTz = isTimestampWithTimeZone(md, col, ctx.getDatabaseType());

			OffsetDateTime got = tryGet(rs, col, OffsetDateTime.class);
			if (got != null) return got;

			if (withTz) {
				// Try via Instant if driver only gives us Timestamp
				Instant inst = asInstant(rs, col, ctx);
				return inst == null ? null : inst.atOffset(ctx.getTimeZone().getRules().getOffset(inst));
			} else {
				LocalDateTime ldt = tryGet(rs, col, LocalDateTime.class);
				if (ldt == null) {
					Timestamp ts = rs.getTimestamp(col);
					ldt = ts == null ? null : ts.toLocalDateTime();
				}
				if (ldt == null) return null;
				ZoneOffset off = ctx.getTimeZone().getRules().getOffset(ldt.atZone(ctx.getTimeZone()).toInstant());
				return ldt.atOffset(off);
			}
		}

		public static ZonedDateTime asZonedDateTime(ResultSet rs, int col, StatementContext<?> ctx) throws SQLException {
			Instant inst = asInstant(rs, col, ctx);
			return inst == null ? null : inst.atZone(ctx.getTimeZone());
		}

		public static LocalDateTime asLocalDateTime(ResultSet rs, int col, StatementContext<?> ctx) throws SQLException {
			ResultSetMetaData md = rs.getMetaData();
			boolean withTz = isTimestampWithTimeZone(md, col, ctx.getDatabaseType());

			LocalDateTime ldt = tryGet(rs, col, LocalDateTime.class);
			if (ldt != null) return ldt;

			Object raw = rs.getObject(col);
			if (raw == null) return null;

			if (raw instanceof Timestamp ts) return ts.toLocalDateTime();
			if (raw instanceof OffsetDateTime odt) {
				// normalize into DB zone then drop zone for stable wall time
				return odt.atZoneSameInstant(ctx.getTimeZone()).toLocalDateTime();
			}
			if (raw instanceof Instant inst) return inst.atZone(ctx.getTimeZone()).toLocalDateTime();
			if (raw instanceof String s) {
				try {
					return LocalDateTime.parse(s);
				} catch (DateTimeParseException ignore) {
				}
				try {
					return OffsetDateTime.parse(s).atZoneSameInstant(ctx.getTimeZone()).toLocalDateTime();
				} catch (DateTimeParseException ignore) {
				}
			}

			if (withTz) {
				OffsetDateTime odt2 = tryGet(rs, col, OffsetDateTime.class);
				if (odt2 != null) return odt2.atZoneSameInstant(ctx.getTimeZone()).toLocalDateTime();
			}

			Timestamp ts = rs.getTimestamp(col);
			return ts == null ? null : ts.toLocalDateTime();
		}

		public static LocalDate asLocalDate(ResultSet rs, int col) throws SQLException {
			LocalDate d = tryGet(rs, col, LocalDate.class);
			if (d != null) return d;
			Object raw = rs.getObject(col);
			if (raw == null) return null;
			if (raw instanceof java.sql.Date sqlDate) return sqlDate.toLocalDate();
			if (raw instanceof String s) {
				try {
					return LocalDate.parse(s);
				} catch (DateTimeParseException ignore) {
				}
			}
			java.sql.Date d2 = rs.getDate(col);
			return d2 == null ? null : d2.toLocalDate();
		}

		public static LocalTime asLocalTime(ResultSet rs, int col) throws SQLException {
			LocalTime t = tryGet(rs, col, LocalTime.class);
			if (t != null) return t;
			Object raw = rs.getObject(col);
			if (raw == null) return null;
			if (raw instanceof java.sql.Time sqlTime) return sqlTime.toLocalTime();
			if (raw instanceof String s) {
				try {
					return LocalTime.parse(s);
				} catch (DateTimeParseException ignore) {
				}
			}
			java.sql.Time t2 = rs.getTime(col);
			return t2 == null ? null : t2.toLocalTime();
		}

		public static OffsetTime asOffsetTime(ResultSet rs, int col, StatementContext<?> ctx) throws SQLException {
			if (isTimeWithTimeZone(rs.getMetaData(), col)) {
				OffsetTime ot = tryGet(rs, col, OffsetTime.class);
				if (ot != null) return ot;
			}
			// If DB has plain TIME, attach DB zone offset at an arbitrary date (offset depends on date!)
			LocalTime lt = asLocalTime(rs, col);
			if (lt == null) return null;
			// Choose an arbitrary date with consistent offset, e.g., epoch day 0 in DB zone
			Instant now = Instant.now();
			ZoneOffset off = ctx.getTimeZone().getRules().getOffset(now);
			return lt.atOffset(off);
		}
	}
}