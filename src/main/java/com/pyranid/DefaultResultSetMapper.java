/*
 * Copyright 2015-2018 Transmogrify LLC.
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

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Basic implementation of {@link ResultSetMapper}.
 * 
 * @author <a href="http://revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
public class DefaultResultSetMapper implements ResultSetMapper {
  private final DatabaseType databaseType;
  private final InstanceProvider instanceProvider;
  private final ZoneId timeZone;
  private final Calendar timeZoneCalendar;
  private final Map<Class<?>, Map<String, Set<String>>> columnLabelAliasesByPropertyNameCache =
      new ConcurrentHashMap<>();

  /**
   * Creates a {@code ResultSetMapper} for the given {@code databaseType} and {@code instanceProvider}.
   *
   * @param databaseType
   *          the type of database we're working with
   * @param instanceProvider
   *          instance-creation factory, used to instantiate resultset row objects as needed
   */
  public DefaultResultSetMapper(DatabaseType databaseType, InstanceProvider instanceProvider) {
    this(databaseType, instanceProvider, ZoneId.systemDefault());
  }

  /**
   * Creates a {@code ResultSetMapper} for the given {@code databaseType} and {@code instanceProvider}.
   * 
   * @param databaseType
   *          the type of database we're working with
   * @param instanceProvider
   *          instance-creation factory, used to instantiate resultset row objects as needed
   * @param timeZone
   *          the timezone to use when working with {@link java.sql.Timestamp} and similar values
   * @since 1.0.15
   */
  public DefaultResultSetMapper(DatabaseType databaseType, InstanceProvider instanceProvider, ZoneId timeZone) {
    this.databaseType = requireNonNull(databaseType);
    this.instanceProvider = requireNonNull(instanceProvider);
    this.timeZone = timeZone == null ? ZoneId.systemDefault() : timeZone;
    this.timeZoneCalendar = Calendar.getInstance(TimeZone.getTimeZone(this.timeZone));
  }

  @Override
  public <T> T map(ResultSet resultSet, Class<T> resultClass) {
    requireNonNull(resultSet);
    requireNonNull(resultClass);

    try {
      StandardTypeResult<T> standardTypeResult = mapResultSetToStandardType(resultSet, resultClass);

      if (standardTypeResult.isStandardType())
        return standardTypeResult.value();

      return mapResultSetToBean(resultSet, resultClass);
    } catch (DatabaseException e) {
      throw e;
    } catch (Exception e) {
      throw new DatabaseException(format("Unable to map JDBC %s to %s", ResultSet.class.getSimpleName(), resultClass),
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
   * @param <T>
   *          result instance type token
   * @param resultSet
   *          provides raw row data to pull from
   * @param resultClass
   *          the type of instance to map to
   * @return the result of the mapping
   * @throws Exception
   *           if an error occurs during mapping
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected <T> StandardTypeResult<T> mapResultSetToStandardType(ResultSet resultSet, Class<T> resultClass)
      throws Exception {
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
      value = (Object) Enum.valueOf((Class) resultClass, resultSet.getString(1));
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

    return new StandardTypeResult((T) value, standardType);
  }

  /**
   * Attempts to map the current {@code resultSet} row to an instance of {@code resultClass}, which should be a
   * JavaBean.
   * <p>
   * The {@code resultClass} instance will be created via {@link #instanceProvider()}.
   *
   * @param <T>
   *          result instance type token
   * @param resultSet
   *          provides raw row data to pull from
   * @param resultClass
   *          the type of instance to map to
   * @return the result of the mapping
   * @throws Exception
   *           if an error occurs during mapping
   */
  protected <T> T mapResultSetToBean(ResultSet resultSet, Class<T> resultClass) throws Exception {
    T object = instanceProvider().provide(resultClass);
    BeanInfo beanInfo = Introspector.getBeanInfo(resultClass);
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    int columnCount = resultSetMetaData.getColumnCount();
    Set<String> columnLabels = new HashSet<>(columnCount);

    for (int i = 0; i < columnCount; i++)
      columnLabels.add(resultSetMetaData.getColumnLabel(i + 1));

    Map<String, Object> columnLabelsToValues = new HashMap<>(columnLabels.size());

    for (String columnLabel : columnLabels) {
      Object resultSetValue = resultSet.getObject(columnLabel);

      // If DB gives us time-related values, re-pull using specific RS methods so we can apply a timezone
      if(resultSetValue != null) {
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

    Map<String, Set<String>> columnLabelAliasesByPropertyName =
        columnLabelAliasesByPropertyNameCache.computeIfAbsent(
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
          Object value =
              convertResultSetValueToPropertyType(columnLabelsToValues.get(propertyName), parameter.getType());

          Class<?> writeMethodParameterType = writeMethod.getParameterTypes()[0];

          if (value != null && !writeMethodParameterType.isAssignableFrom(value.getClass())) {
            String resultSetTypeDescription = value.getClass().toString();

            throw new DatabaseException(
              format(
                "Property '%s' of %s has a write method of type %s, but the ResultSet type %s does not match. "
                    + "Consider creating your own %s and overriding convertResultSetValueToPropertyType() to detect instances of %s and convert them to %s",
                propertyDescriptor.getName(), resultClass, writeMethodParameterType, resultSetTypeDescription,
                DefaultResultSetMapper.class.getSimpleName(), resultSetTypeDescription, writeMethodParameterType));
          }

          writeMethod.invoke(object, value);
        }
      }
    }

    return object;
  }

  /**
   * Massages a {@link ResultSet#getObject(String)} value to match the given {@code propertyType}.
   * <p>
   * For example, the JDBC driver might give us {@link java.sql.Timestamp} but our corresponding JavaBean field is of
   * type {@link java.util.Date}, so we need to manually convert that ourselves.
   * 
   * @param resultSetValue
   *          the value returned by {@link ResultSet#getObject(String)}
   * @param propertyType
   *          the JavaBean property type we'd like to map {@code resultSetValue} to
   * @return a representation of {@code resultSetValue} that is of type {@code propertyType}
   */
  protected Object convertResultSetValueToPropertyType(Object resultSetValue, Class<?> propertyType) {
    requireNonNull(propertyType);

    if (resultSetValue == null)
      return null;

    if (resultSetValue instanceof BigDecimal) {
      BigDecimal bigDecimal = (BigDecimal) resultSetValue;

      if (BigDecimal.class.isAssignableFrom(propertyType))
        return bigDecimal;
      if (BigInteger.class.isAssignableFrom(propertyType))
        return bigDecimal.toBigInteger();
    }

    if (resultSetValue instanceof BigInteger) {
      BigInteger bigInteger = (BigInteger) resultSetValue;

      if (BigDecimal.class.isAssignableFrom(propertyType))
        return new BigDecimal(bigInteger);
      if (BigInteger.class.isAssignableFrom(propertyType))
        return bigInteger;
    }

    if (resultSetValue instanceof Number) {
      Number number = (Number) resultSetValue;

      if (Byte.class.isAssignableFrom(propertyType))
        return number.byteValue();
      if (Short.class.isAssignableFrom(propertyType))
        return number.shortValue();
      if (Integer.class.isAssignableFrom(propertyType))
        return number.intValue();
      if (Long.class.isAssignableFrom(propertyType))
        return number.longValue();
      if (Float.class.isAssignableFrom(propertyType))
        return number.floatValue();
      if (Double.class.isAssignableFrom(propertyType))
        return number.doubleValue();
      if (BigDecimal.class.isAssignableFrom(propertyType))
        return new BigDecimal(number.doubleValue());
      if (BigInteger.class.isAssignableFrom(propertyType))
        return new BigDecimal(number.doubleValue()).toBigInteger();
    } else if (resultSetValue instanceof java.sql.Timestamp) {
      java.sql.Timestamp date = (java.sql.Timestamp) resultSetValue;

      if (Date.class.isAssignableFrom(propertyType))
        return date;
      if (Instant.class.isAssignableFrom(propertyType))
        return date.toInstant();
      if (LocalDate.class.isAssignableFrom(propertyType))
        return date.toInstant().atZone(getTimeZone()).toLocalDate();
      if (LocalDateTime.class.isAssignableFrom(propertyType))
        return date.toLocalDateTime();
    } else if (resultSetValue instanceof java.sql.Date) {
      java.sql.Date date = (java.sql.Date) resultSetValue;
      
      if (Date.class.isAssignableFrom(propertyType))
        return date;
      if (Instant.class.isAssignableFrom(propertyType))
        return date.toInstant();
      if (LocalDate.class.isAssignableFrom(propertyType))
        return date.toLocalDate();      
      if (LocalDateTime.class.isAssignableFrom(propertyType))
        return LocalDateTime.ofInstant(date.toInstant(), getTimeZone());
    } else if (resultSetValue instanceof java.sql.Time) {
      java.sql.Time time = (java.sql.Time) resultSetValue;

      if (LocalTime.class.isAssignableFrom(propertyType))
        return time.toLocalTime();
    } else if (propertyType.isAssignableFrom(ZoneId.class)) {
      return ZoneId.of(resultSetValue.toString());
    } else if (propertyType.isAssignableFrom(TimeZone.class)) {
      return TimeZone.getTimeZone(resultSetValue.toString());
    } else if (propertyType.isAssignableFrom(Locale.class)) {
      return Locale.forLanguageTag(resultSetValue.toString());
    } else if (propertyType.isEnum()) {
      return extractEnumValue(propertyType, resultSetValue);
    } else if ("org.postgresql.util.PGobject".equals(resultSetValue.getClass().getName())) {
      org.postgresql.util.PGobject pgObject = (org.postgresql.util.PGobject) resultSetValue;
      return pgObject.getValue();
    }

    return resultSetValue;
  }

  /**
   * Attempts to convert {@code object} to a corresponding value for enum type {@code enumClass}.
   * <p>
   * Normally {@code object} is a {@code String}, but other types may be used - the {@code toString()} method of
   * {@code object} will be invoked to determine the final value for conversion.
   * 
   * @param enumClass
   *          the enum to which we'd like to convert {@code object}
   * @param object
   *          the object to convert to an enum value
   * @return the enum value of {@code object} for {@code enumClass}
   * @throws IllegalArgumentException
   *           if {@code enumClass} is not an enum
   * @throws DatabaseException
   *           if {@code object} does not correspond to a valid enum value
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected Enum<?> extractEnumValue(Class<?> enumClass, Object object) {
    requireNonNull(enumClass);
    requireNonNull(object);

    if (!enumClass.isEnum())
      throw new IllegalArgumentException(format("%s is not an enum type", enumClass));

    String objectAsString = object.toString();

    try {
      return Enum.valueOf((Class<? extends Enum>) enumClass, objectAsString);
    } catch (IllegalArgumentException e) {
      throw new DatabaseException(format("The value '%s' is not present in enum %s", objectAsString, enumClass), e);
    }
  }

  /**
   * Massages a {@link ResultSet} column label so it's easier to match against a JavaBean property name.
   * <p>
   * This implementation lowercases the label using the locale provided by {@link #normalizationLocale()}.
   * 
   * @param columnLabel
   *          the {@link ResultSet} column label to massage
   * @return the massaged label
   */
  protected String normalizeColumnLabel(String columnLabel) {
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
   * @param propertyName
   *          the JavaBean property name to massage
   * @return the column names that match the JavaBean property name
   */
  protected Set<String> databaseColumnNamesForPropertyName(String propertyName) {
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
  protected Locale normalizationLocale() {
    return ENGLISH;
  }

  /**
   * What kind of database are we working with?
   * 
   * @return the kind of database we're working with
   */
  protected DatabaseType databaseType() {
    return this.databaseType;
  }

  /**
   * Returns an instance-creation factory, used to instantiate resultset row objects as needed.
   * 
   * @return the instance-creation factory
   */
  protected InstanceProvider instanceProvider() {
    return this.instanceProvider;
  }

  /**
   * The result of attempting to map a {@link ResultSet} to a "standard" type like primitive or {@link UUID}.
   * 
   * @author <a href="http://revetkn.com">Mark Allen</a>
   * @since 1.0.0
   */
  protected static class StandardTypeResult<T> {
    private final T value;
    private final boolean standardType;

    /**
     * Creates a {@code StandardTypeResult} with the given {@code value} and {@code standardType} flag.
     * 
     * @param value
     *          the mapping result, may be {@code null}
     * @param standardType
     *          {@code true} if the mapped type was a standard type, {@code false} otherwise
     */
    public StandardTypeResult(T value, boolean standardType) {
      this.value = value;
      this.standardType = standardType;
    }

    /**
     * Gets the result of the mapping.
     * 
     * @return the mapping result value, may be {@code null}
     */
    public T value() {
      return this.value;
    }

    /**
     * Was the mapped type a standard type?
     * 
     * @return {@code true} if this was a standard type, {@code false} otherwise
     */
    public boolean isStandardType() {
      return this.standardType;
    }
  }

  protected ZoneId getTimeZone() {
    return timeZone;
  }

  protected Calendar getTimeZoneCalendar() {
    return timeZoneCalendar;
  }
}