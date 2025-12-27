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

import com.pyranid.CustomColumnMapper.MappingResult;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.DateTimeException;
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
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.IllformedLocaleException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Package-private standard implementation of {@link ResultSetMapper}.
 * <p>
 * "Surgical" per-column mapping customization is supported by providing {@link CustomColumnMapper} instances.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
class DefaultResultSetMapper implements ResultSetMapper {
	@NonNull
	private static final Map<Class<?>, Class<?>> WRAPPER_CLASSES_BY_PRIMITIVE_CLASS;

	@NonNull
	private final Locale normalizationLocale;
	@NonNull
	private final List<CustomColumnMapper> customColumnMappers;
	@NonNull
	private final Boolean planCachingEnabled;
	private final int planCacheCapacity;
	private final int preferredColumnMapperCacheCapacity;
	// Enables faster lookup of CustomColumnMapper instances by remembering which TargetType they've been used with before.
	@NonNull
	private final ConcurrentMap<TargetType, List<CustomColumnMapper>> customColumnMappersByTargetTypeCache;
	@NonNull
	private final Map<SourceTargetKey, CustomColumnMapper> preferredColumnMapperBySourceTargetKey;
	@NonNull
	private final ConcurrentMap<Class<?>, Map<String, TargetType>> propertyTargetTypeCache = new ConcurrentHashMap<>();
	@NonNull
	private final ConcurrentMap<Class<?>, Map<String, Set<String>>> columnLabelAliasesByPropertyNameCache;
	@NonNull
	private final ConcurrentMap<Class<?>, PropertyDescriptor[]> propertyDescriptorsCache = new ConcurrentHashMap<>();
	@NonNull
	private final ConcurrentMap<Class<?>, Map<String, Set<String>>> normalizedColumnLabelsByPropertyNameCache = new ConcurrentHashMap<>();
	@NonNull
	private final ConcurrentMap<Class<?>, Map<String, Set<String>>> columnLabelsByRecordComponentNameCache = new ConcurrentHashMap<>();
	@NonNull
	private final ConcurrentMap<Class<?>, RecordComponent[]> recordComponentsCache = new ConcurrentHashMap<>();
	// Only used if row-planning is enabled
	@NonNull
	private final Map<PlanKey, RowPlan<?>> rowPlanningCache;
	@NonNull
	private final Map<ResultSetMetaData, String> schemaSignatureByResultSetMetaData;

	static {
		WRAPPER_CLASSES_BY_PRIMITIVE_CLASS = Map.of(
				boolean.class, Boolean.class,
				byte.class, Byte.class,
				short.class, Short.class,
				int.class, Integer.class,
				long.class, Long.class,
				float.class, Float.class,
				double.class, Double.class,
				char.class, Character.class
		);
	}

	// Internal cache key for column mapper applicability cache
	@ThreadSafe
	protected static final class SourceTargetKey {
		private final Class<?> sourceClass;
		private final TargetType targetType;

		public SourceTargetKey(@NonNull Class<?> sourceClass,
													 @NonNull TargetType targetType) {
			requireNonNull(sourceClass);
			requireNonNull(targetType);

			this.sourceClass = sourceClass;
			this.targetType = targetType;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			SourceTargetKey that = (SourceTargetKey) o;
			return sourceClass.equals(that.sourceClass) && targetType.equals(that.targetType);
		}

		@Override
		public int hashCode() {
			return 31 * sourceClass.hashCode() + targetType.hashCode();
		}
	}

	@NonNull
	protected List<CustomColumnMapper> customColumnMappersFor(@NonNull TargetType targetType) {
		requireNonNull(targetType);

		return getCustomColumnMappersByTargetTypeCache().computeIfAbsent(targetType, applicableTargetType -> {
			requireNonNull(applicableTargetType);

			if (getCustomColumnMappers().isEmpty())
				return List.of();

			List<CustomColumnMapper> filtered = new ArrayList<>(getCustomColumnMappers().size());

			for (CustomColumnMapper customColumnMapper : getCustomColumnMappers())
				if (customColumnMapper.appliesTo(applicableTargetType))
					filtered.add(customColumnMapper);

			return Collections.unmodifiableList(filtered);
		});
	}

	@NonNull
	protected Boolean mappingApplied(@NonNull MappingResult mappingResult) {
		requireNonNull(mappingResult);

		// Cache preference if a mapper *applied*, regardless of whether it produced null.
		return !(mappingResult instanceof MappingResult.Fallback);
	}

	@NonNull
	protected Optional<Object> mappedValue(@NonNull MappingResult mappingResult) {
		requireNonNull(mappingResult);

		if (mappingResult instanceof MappingResult.CustomMapping u)
			return u.getValue(); // Optional<Object> already

		return Optional.empty();
	}

	@NonNull
	protected Class<?> boxedClass(@NonNull Class<?> type) {
		requireNonNull(type);
		return type.isPrimitive() ? WRAPPER_CLASSES_BY_PRIMITIVE_CLASS.get(type) : type;
	}

	@NonNull
	protected <T> Optional<Object> tryCustomColumnMappers(@NonNull StatementContext<T> statementContext,
																												@NonNull ResultSet resultSet,
																												@NonNull Object resultSetValue,
																												@NonNull TargetType targetType,
																												@NonNull Integer columnIndex,
																												@Nullable String columnLabel,
																												@NonNull InstanceProvider instanceProvider) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetValue);
		requireNonNull(targetType);
		requireNonNull(instanceProvider);

		List<CustomColumnMapper> mappers = customColumnMappersFor(targetType);
		return tryCustomColumnMappers(statementContext, resultSet, resultSetValue, targetType, mappers, columnIndex, columnLabel, instanceProvider);
	}

	@NonNull
	protected <T> Optional<Object> tryCustomColumnMappers(@NonNull StatementContext<T> statementContext,
																												@NonNull ResultSet resultSet,
																												@NonNull Object resultSetValue,
																												@NonNull TargetType targetType,
																												@NonNull List<CustomColumnMapper> mappers,
																												@NonNull Integer columnIndex,
																												@Nullable String columnLabel,
																												@NonNull InstanceProvider instanceProvider) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetValue);
		requireNonNull(targetType);
		requireNonNull(mappers);
		requireNonNull(instanceProvider);

		if (mappers.isEmpty()) return Optional.empty();

		Class<?> sourceClass = resultSetValue.getClass();
		SourceTargetKey key = new SourceTargetKey(sourceClass, targetType);

		CustomColumnMapper preferred = getPreferredColumnMapperBySourceTargetKey().get(key);

		// 1) Try the preferred mapper first, if any
		if (preferred != null) {
			CustomColumnMapper.MappingResult mappingResult =
					preferred.map(statementContext, resultSet, resultSetValue, targetType, columnIndex, columnLabel, instanceProvider);

			if (mappingApplied(mappingResult))
				// Keep the preference (it applied); return its (possibly null) value
				return mappedValue(mappingResult);
			// If it didn’t apply this time, fall through to try others; retain hint for future rows.
		}

		// 2) Try the remaining applicable mappers
		for (CustomColumnMapper mapper : mappers) {
			if (mapper == preferred) continue;

			CustomColumnMapper.MappingResult mappingResult =
					mapper.map(statementContext, resultSet, resultSetValue, targetType, columnIndex, columnLabel, instanceProvider);

			if (mappingApplied(mappingResult)) {
				// Learn the winner for (sourceClass, targetType) even if it produced null
				getPreferredColumnMapperBySourceTargetKey().put(key, mapper);
				return mappedValue(mappingResult);
			}
		}

		// 3) No custom mapper applied
		return Optional.empty();
	}

	/**
	 * Determines (and caches) the TargetType of each writable JavaBean property and each Record component
	 * for the given result class. Keys are property/record-component names.
	 */
	@NonNull
	protected Map<String, TargetType> determinePropertyTargetTypes(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return getPropertyTargetTypeCache().computeIfAbsent(resultClass, rc -> {
			Map<String, TargetType> types = new HashMap<>();

			// JavaBean setters (preserve generics like List<UUID>)
			try {
				BeanInfo beanInfo = Introspector.getBeanInfo(rc);
				for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
					Method writeMethod = propertyDescriptor.getWriteMethod();

					if (writeMethod == null)
						continue;

					Type genericParameterType = writeMethod.getGenericParameterTypes()[0];
					types.put(propertyDescriptor.getName(), TargetType.of(genericParameterType));
				}
			} catch (IntrospectionException e) {
				throw new RuntimeException(format("Unable to introspect properties for %s", rc.getName()), e);
			}

			// Java records (preserve generics)
			if (rc.isRecord())
				for (RecordComponent recordComponent : rc.getRecordComponents())
					types.put(recordComponent.getName(), TargetType.of(recordComponent.getGenericType()));

			return Collections.unmodifiableMap(types);
		});
	}

	@NonNull
	protected PropertyDescriptor[] determinePropertyDescriptors(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return getPropertyDescriptorsCache().computeIfAbsent(resultClass, rc -> {
			try {
				BeanInfo beanInfo = Introspector.getBeanInfo(rc);
				PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
				return descriptors == null ? new PropertyDescriptor[0] : descriptors;
			} catch (IntrospectionException e) {
				throw new RuntimeException(format("Unable to introspect properties for %s", rc.getName()), e);
			}
		});
	}

	@NonNull
	protected RecordComponent[] determineRecordComponents(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return getRecordComponentsCache().computeIfAbsent(resultClass, rc -> {
			RecordComponent[] components = rc.getRecordComponents();
			return components == null ? new RecordComponent[0] : components;
		});
	}

	@NonNull
	protected Map<String, Set<String>> determineNormalizedColumnLabelsByPropertyName(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return getNormalizedColumnLabelsByPropertyNameCache().computeIfAbsent(resultClass, rc -> {
			Map<String, Set<String>> columnLabelAliasesByPropertyName = determineColumnLabelAliasesByPropertyName(rc);
			Map<String, Set<String>> normalizedLabelsByPropertyName = new HashMap<>();

			for (PropertyDescriptor propertyDescriptor : determinePropertyDescriptors(rc)) {
				if (propertyDescriptor.getWriteMethod() == null)
					continue;

				String propertyName = propertyDescriptor.getName();
				Set<String> baseNames = columnLabelAliasesByPropertyName.get(propertyName);
				if (baseNames == null || baseNames.isEmpty())
					baseNames = Set.of(propertyName);

				Set<String> normalized = new HashSet<>();
				for (String baseName : baseNames)
					normalized.addAll(databaseColumnNamesForPropertyName(baseName));

				normalizedLabelsByPropertyName.put(propertyName, Collections.unmodifiableSet(normalized));
			}

			return Collections.unmodifiableMap(normalizedLabelsByPropertyName);
		});
	}

	@NonNull
	protected Map<String, Set<String>> determineColumnLabelsByRecordComponentName(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return getColumnLabelsByRecordComponentNameCache().computeIfAbsent(resultClass, rc -> {
			Map<String, Set<String>> columnLabelAliasesByPropertyName = determineColumnLabelAliasesByPropertyName(rc);
			Map<String, Set<String>> labelsByComponentName = new HashMap<>();

			for (RecordComponent recordComponent : determineRecordComponents(rc)) {
				String propertyName = recordComponent.getName();
				Set<String> labels = columnLabelAliasesByPropertyName.get(propertyName);
				if (labels == null || labels.isEmpty())
					labels = databaseColumnNamesForPropertyName(propertyName);

				labelsByComponentName.put(propertyName, Collections.unmodifiableSet(labels));
			}

			return Collections.unmodifiableMap(labelsByComponentName);
		});
	}

	DefaultResultSetMapper(@NonNull Builder builder) {
		requireNonNull(builder);

		this.normalizationLocale = requireNonNull(builder.normalizationLocale);
		this.customColumnMappers = Collections.unmodifiableList(requireNonNull(builder.customColumnMappers));
		this.planCachingEnabled = requireNonNull(builder.planCachingEnabled);

		this.customColumnMappersByTargetTypeCache = new ConcurrentHashMap<>();
		this.preferredColumnMapperCacheCapacity = builder.preferredColumnMapperCacheCapacity;
		this.preferredColumnMapperBySourceTargetKey = createCache(this.preferredColumnMapperCacheCapacity);
		this.columnLabelAliasesByPropertyNameCache = new ConcurrentHashMap<>();
		this.planCacheCapacity = builder.planCacheCapacity;
		this.rowPlanningCache = createCache(this.planCacheCapacity);
		this.schemaSignatureByResultSetMetaData = Collections.synchronizedMap(new WeakHashMap<>());
	}

	@NonNull
	private static <K, V> Map<K, V> createCache(@NonNull Integer capacity) {
		requireNonNull(capacity);
		if (capacity <= 0)
			return new ConcurrentHashMap<>();
		return new ConcurrentLruMap<>(capacity);
	}

	@Override
	@NonNull
	public <T> Optional<T> map(@NonNull StatementContext<T> statementContext,
														 @NonNull ResultSet resultSet,
														 @NonNull Class<T> resultSetRowType,
														 @NonNull InstanceProvider instanceProvider) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetRowType);
		requireNonNull(instanceProvider);

		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

		if (getPlanCachingEnabled())
			return mapWithRowPlanning(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData);

		return mapWithoutRowPlanning(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData);
	}

	@NonNull
	protected <T> Optional<T> mapWithoutRowPlanning(@NonNull StatementContext<T> statementContext,
																									@NonNull ResultSet resultSet,
																									@NonNull Class<T> resultSetRowType,
																									@NonNull InstanceProvider instanceProvider,
																									@NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetRowType);
		requireNonNull(instanceProvider);
		requireNonNull(resultSetMetaData);

		try {
			StandardTypeResult<T> standardTypeResult = mapResultSetToStandardType(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData);

			if (standardTypeResult.isStandardType())
				return standardTypeResult.getValue();

			if (resultSetRowType.isRecord())
				return Optional.ofNullable((T) mapResultSetToRecord((StatementContext<? extends Record>) statementContext, resultSet, instanceProvider, resultSetMetaData));

			return Optional.ofNullable(mapResultSetToBean(statementContext, resultSet, instanceProvider, resultSetMetaData));
		} catch (DatabaseException e) {
			throw e;
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new DatabaseException(format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), resultSetRowType),
					e);
		}
	}

	@NonNull
	protected <T> Optional<T> mapWithRowPlanning(@NonNull StatementContext<T> statementContext,
																							 @NonNull ResultSet resultSet,
																							 @NonNull Class<T> resultSetRowType,
																							 @NonNull InstanceProvider instanceProvider,
																							 @NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetRowType);
		requireNonNull(instanceProvider);
		requireNonNull(resultSetMetaData);

		try {
			// Use "standard type" fast path
			StandardTypeResult<T> standardTypeResult = mapResultSetToStandardType(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData);
			if (standardTypeResult.isStandardType()) return standardTypeResult.getValue();

			// Otherwise, use a per-schema row plan
			String schemaSignature = schemaSignatureFor(statementContext, resultSetMetaData);
			RowPlan<T> plan = buildPlan(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData, schemaSignature);
			final boolean hasFanOut = plan.hasFanOut;
			// Per-row raw cache: 1-based, size = columnCount + 1 (only when we have fan-out)
			final Object[] rawByCol = hasFanOut ? new Object[plan.columnCount + 1] : null;
			final boolean[] rawLoaded = hasFanOut ? new boolean[plan.columnCount + 1] : null;

			if (plan.isRecord) {
				RecordComponent[] rc = determineRecordComponents(resultSetRowType);
				Object[] args = new Object[rc.length];

				for (ColumnBinding b : plan.bindings) {
					if (b.recordArgIndexOrMinusOne < 0) continue; // bean binding or SKIP
					Object raw;
					if (hasFanOut) {
						if (!rawLoaded[b.columnIndex]) {
							raw = b.reader.read(resultSet, statementContext, b.columnIndex);
							rawByCol[b.columnIndex] = raw;
							rawLoaded[b.columnIndex] = true;
						} else {
							raw = rawByCol[b.columnIndex];
						}
					} else {
						raw = b.reader.read(resultSet, statementContext, b.columnIndex);
					}
					Object val = b.converter.convert(raw, resultSet, statementContext, instanceProvider);

					if (val == null) {
						if (b.targetIsPrimitive) {
							throw new DatabaseException(format(
									"Column for record component '%s' of %s is NULL but the component is primitive (%s). "
											+ "Use a non-primitive type or COALESCE/CAST in SQL.",
									b.propertyName, resultSetRowType.getSimpleName(), b.targetRawClass.getSimpleName()));
						}
						args[b.recordArgIndexOrMinusOne] = null;
						continue;
					}

					if (!b.targetRawClass.isInstance(val)) {
						String resultSetTypeDescription = val.getClass().toString();
						throw new DatabaseException(format(
								"Property '%s' of %s expects type %s, but the ResultSet produced %s. "
										+ "Consider providing a %s to %s to detect instances of %s and convert them to %s",
								b.propertyName, resultSetRowType, b.targetRawClass, resultSetTypeDescription,
								CustomColumnMapper.class.getSimpleName(), DefaultResultSetMapper.class.getSimpleName(),
								resultSetTypeDescription, b.targetRawClass));
					}

					args[b.recordArgIndexOrMinusOne] = val;
				}

				// Construct via InstanceProvider to keep semantics consistent with non-planned path
				@SuppressWarnings("unchecked")
				T rec = (T) provideRecordVia(instanceProvider, statementContext, resultSetRowType, args);
				return Optional.of(rec);
			} else {
				// Bean
				T object = instanceProvider.provide(statementContext, resultSetRowType);

				for (ColumnBinding b : plan.bindings) {
					if (b.setterOrNull == null) continue; // record-only or SKIP
					Object raw;
					if (hasFanOut) {
						if (!rawLoaded[b.columnIndex]) {
							raw = b.reader.read(resultSet, statementContext, b.columnIndex);
							rawByCol[b.columnIndex] = raw;
							rawLoaded[b.columnIndex] = true;
						} else {
							raw = rawByCol[b.columnIndex];
						}
					} else {
						raw = b.reader.read(resultSet, statementContext, b.columnIndex);
					}
					Object val = b.converter.convert(raw, resultSet, statementContext, instanceProvider);

					if (val == null) {
						if (b.targetIsPrimitive) {
							throw new DatabaseException(format(
									"Column for bean property '%s' of %s is NULL but the property is primitive (%s). "
											+ "Use a non-primitive type or COALESCE/CAST in SQL.",
									b.propertyName, resultSetRowType.getSimpleName(), b.targetRawClass.getSimpleName()));
						}
						// Mirror non-planned path: call setter with null
						b.setterOrNull.invoke(object, (Object) null);
						continue;
					}

					if (!b.targetRawClass.isInstance(val)) {
						String resultSetTypeDescription = val.getClass().toString();
						throw new DatabaseException(format(
								"Property '%s' of %s has a write method of type %s, but the ResultSet type %s does not match. "
										+ "Consider providing a %s to %s to detect instances of %s and convert them to %s",
								b.propertyName, resultSetRowType, b.targetRawClass, resultSetTypeDescription,
								CustomColumnMapper.class.getSimpleName(), DefaultResultSetMapper.class.getSimpleName(),
								resultSetTypeDescription, b.targetRawClass));
					}

					b.setterOrNull.invoke(object, val);
				}

				return Optional.of(object);
			}
		} catch (DatabaseException e) {
			throw e;
		} catch (SQLException e) {
			throw e;
		} catch (Throwable t) {
			throw new DatabaseException(format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), resultSetRowType), t);
		}
	}

	@NonNull
	protected String schemaSignatureFor(@NonNull StatementContext<?> statementContext,
																			@NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSetMetaData);

		String signature = getSchemaSignatureByResultSetMetaData().get(resultSetMetaData);
		if (signature != null)
			return signature;

		signature = buildSchemaSignature(statementContext, resultSetMetaData);
		getSchemaSignatureByResultSetMetaData().put(resultSetMetaData, signature);
		return signature;
	}

	@NonNull
	private String buildSchemaSignature(@NonNull StatementContext<?> statementContext,
																			@NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSetMetaData);

		int cols = resultSetMetaData.getColumnCount();
		StringBuilder sig = new StringBuilder(64 + cols * 48).append(cols);

		for (int i = 1; i <= cols; i++) {
			String labelNorm = normalizeColumnLabel(resultSetMetaData.getColumnLabel(i));
			int jdbcType = resultSetMetaData.getColumnType(i);
			boolean tsTz = TemporalReaders.isTimestampWithTimeZone(resultSetMetaData, i, statementContext.getDatabaseType());
			boolean timeTz = TemporalReaders.isTimeWithTimeZone(resultSetMetaData, i);
			String typeName = Objects.toString(resultSetMetaData.getColumnTypeName(i), "").toUpperCase(Locale.ROOT);
			sig.append('|').append(labelNorm)
					.append(':').append(jdbcType)
					.append(':').append(tsTz ? 'T' : 't')
					.append(':').append(timeTz ? 'Z' : 'z')
					.append(':').append(typeName);
		}

		return sig.toString();
	}

	@SuppressWarnings("unchecked")
	private static <R extends Record> R provideRecordVia(
			InstanceProvider ip,
			StatementContext<?> ctx,
			Class<?> recordClass,
			Object[] args
	) {
		// Safe by construction: caller only uses this when resultSetRowType.isRecord() is true
		return ip.provideRecord((StatementContext<R>) ctx, (Class<R>) recordClass, args);
	}

	/**
	 * Attempts to map the current {@code resultSet} row to an instance of {@code resultClass} using one of the
	 * "out-of-the-box" types (primitives, common types like {@link UUID}, etc.)
	 * <p>
	 * This does not attempt to map to a user-defined JavaBean - see {@link #mapResultSetToBean(StatementContext, ResultSet, InstanceProvider, ResultSetMetaData)} for
	 * that functionality.
	 *
	 * @param <T>              result instance type token
	 * @param resultSet        provides raw row data to pull from
	 * @param resultClass      the type of instance to map to
	 * @param instanceProvider creates instances for a given type
	 * @return the result of the mapping
	 * @throws Exception if an error occurs during mapping
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	@NonNull
	protected <T> StandardTypeResult<T> mapResultSetToStandardType(@NonNull StatementContext<T> statementContext,
																																 @NonNull ResultSet resultSet,
																																 @NonNull Class<T> resultClass,
																																 @NonNull InstanceProvider instanceProvider,
																																 @NonNull ResultSetMetaData resultSetMetaData) throws Exception {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultClass);
		requireNonNull(instanceProvider);
		requireNonNull(resultSetMetaData);

		Object value = null;
		boolean standardType = true;

		// See if custom mappers apply, e.g. user mapping a SQL ARRAY to a List<UUID>
		TargetType targetType = TargetType.of(resultClass);
		List<CustomColumnMapper> mappers = customColumnMappersFor(targetType);

		if (!mappers.isEmpty()) {
			Object resultSetValue = extractColumnValue(resultSetMetaData, statementContext, resultSet, 1).orElse(null);

			// If single-column AND value is NULL AND target has applicable custom mappers,
			// treat this as a "standard" mapping that yields null => Optional.empty()
			int columnCount = resultSetMetaData.getColumnCount();

			if (resultSetValue == null) {
				if (columnCount != 1) {
					List<String> labels = new ArrayList<>(columnCount);
					for (int i = 1; i <= columnCount; ++i)
						labels.add(resultSetMetaData.getColumnLabel(i));
					throw new DatabaseException(format(
							"Expected 1 column to map to %s but encountered %s instead (%s)",
							resultClass, columnCount, labels.stream().collect(joining(", "))));
				}

				return new StandardTypeResult<>(null, true); // Ensure Optional.empty() if null standard type
			}

			if (resultSetValue != null) {
				Optional<Object> maybe =
						tryCustomColumnMappers(statementContext,
								resultSet,
								resultSetValue,
								targetType,
								1,
								resultSetMetaData.getColumnLabel(1),
								instanceProvider);

				if (maybe.isPresent()) {
					// Enforce the single-column invariant, just like the normal fast path does
					if (columnCount != 1) {
						List<String> labels = new ArrayList<>(columnCount);

						for (int i = 1; i <= columnCount; ++i)
							labels.add(resultSetMetaData.getColumnLabel(i));

						throw new DatabaseException(format("Expected 1 column to map to %s but encountered %s instead (%s)",
								resultClass, columnCount, labels.stream().collect(joining(", "))));
					}

					// Return exactly like the fast path: Optional.ofNullable(value), standardType=true
					@SuppressWarnings("unchecked")
					T cast = (T) maybe.orElse(null);
					return new StandardTypeResult<>(cast, true);
				}

				// If no mapper applied (Fallback), we proceed to built-ins below.
			}
		}

		if (resultClass.isAssignableFrom(Byte.class) || resultClass.isAssignableFrom(byte.class)) {
			value = getNullableByte(resultSet, 1);
		} else if (resultClass.isAssignableFrom(Short.class) || resultClass.isAssignableFrom(short.class)) {
			value = getNullableShort(resultSet, 1);
		} else if (resultClass.isAssignableFrom(Integer.class) || resultClass.isAssignableFrom(int.class)) {
			value = getNullableInt(resultSet, 1);
		} else if (resultClass.isAssignableFrom(Long.class) || resultClass.isAssignableFrom(long.class)) {
			value = getNullableLong(resultSet, 1);
		} else if (resultClass.isAssignableFrom(Float.class) || resultClass.isAssignableFrom(float.class)) {
			value = getNullableFloat(resultSet, 1);
		} else if (resultClass.isAssignableFrom(Double.class) || resultClass.isAssignableFrom(double.class)) {
			value = getNullableDouble(resultSet, 1);
		} else if (resultClass.isAssignableFrom(Boolean.class) || resultClass.isAssignableFrom(boolean.class)) {
			value = getNullableBoolean(resultSet, 1);
		} else if (resultClass.isAssignableFrom(Character.class) || resultClass.isAssignableFrom(char.class)) {
			String string = resultSet.getString(1);

			if (string != null) {
				if (string.length() == 1)
					value = string.charAt(0);
				else
					throw new DatabaseException(format("Cannot map String value '%s' to %s", string, resultClass.getSimpleName()));
			}
		} else if (resultClass.isAssignableFrom(String.class)) {
			value = resultSet.getString(1);
		} else if (resultClass.isAssignableFrom(byte[].class)) {
			value = resultSet.getBytes(1);
		} else if (resultClass.isEnum()) {
			Object raw = resultSet.getObject(1);
			if (raw == null)
				value = null; // -> Optional.empty()
			else
				value = extractEnumValue(resultClass, raw);
		} else if (resultClass.isAssignableFrom(UUID.class)) {
			String string = resultSet.getString(1);
			if (string != null) {
				try {
					value = UUID.fromString(string);
				} catch (IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to UUID", string), e);
				}
			}
		} else if (resultClass.isAssignableFrom(BigDecimal.class)) {
			value = resultSet.getBigDecimal(1);
		} else if (resultClass.isAssignableFrom(BigInteger.class)) {
			BigDecimal bd = resultSet.getBigDecimal(1);
			if (bd != null) value = bd.toBigInteger();
		} else if (resultClass.isAssignableFrom(Date.class)) {
			Instant inst = TemporalReaders.asInstant(resultSet, 1, statementContext, resultSetMetaData);
			value = (inst == null) ? null : Date.from(inst);
		} else if (resultClass.isAssignableFrom(Instant.class)) {
			value = TemporalReaders.asInstant(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass.isAssignableFrom(LocalDate.class)) {
			value = TemporalReaders.asLocalDate(resultSet, 1);
		} else if (resultClass.isAssignableFrom(LocalTime.class)) {
			value = TemporalReaders.asLocalTime(resultSet, 1);
		} else if (resultClass.isAssignableFrom(LocalDateTime.class)) {
			value = TemporalReaders.asLocalDateTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass.isAssignableFrom(OffsetTime.class)) {
			value = TemporalReaders.asOffsetTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass.isAssignableFrom(OffsetDateTime.class)) {
			value = TemporalReaders.asOffsetDateTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass.isAssignableFrom(java.sql.Timestamp.class)) {
			boolean withTz = TemporalReaders.isTimestampWithTimeZone(resultSetMetaData, 1, statementContext.getDatabaseType());

			if (withTz) {
				Instant inst = TemporalReaders.asInstant(resultSet, 1, statementContext, resultSetMetaData);
				value = (inst == null) ? null : Timestamp.from(inst);
			} else {
				LocalDateTime ldt = TemporalReaders.asLocalDateTime(resultSet, 1, statementContext, resultSetMetaData);
				value = (ldt == null) ? null : Timestamp.valueOf(ldt);
			}
		} else if (resultClass.isAssignableFrom(java.sql.Date.class)) {
			LocalDate ld = TemporalReaders.asLocalDate(resultSet, 1);
			value = (ld == null) ? null : java.sql.Date.valueOf(ld);
		} else if (resultClass.isAssignableFrom(ZoneId.class)) {
			String zoneId = resultSet.getString(1);
			if (zoneId != null) {
				try {
					value = ZoneId.of(zoneId);
				} catch (DateTimeException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to ZoneId", zoneId), e);
				}
			}
		} else if (resultClass.isAssignableFrom(TimeZone.class)) {
			String tz = resultSet.getString(1);
			if (tz != null) value = timeZoneFromId(tz);
		} else if (resultClass.isAssignableFrom(Locale.class)) {
			String locale = resultSet.getString(1);
			if (locale != null) value = localeFromLanguageTag(locale);
		} else if (resultClass.isAssignableFrom(Currency.class)) {
			String currency = resultSet.getString(1);
			if (currency != null) {
				try {
					value = Currency.getInstance(currency);
				} catch (IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to Currency", currency), e);
				}
			}
		} else {
			standardType = false;
		}

		if (standardType) {
			int columnCount = resultSetMetaData.getColumnCount();
			if (columnCount != 1) {
				List<String> labels = new ArrayList<>(columnCount);
				for (int i = 1; i <= columnCount; ++i) labels.add(resultSetMetaData.getColumnLabel(i));
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
	@NonNull
	protected <T extends Record> T mapResultSetToRecord(@NonNull StatementContext<T> statementContext,
																											@NonNull ResultSet resultSet,
																											@NonNull InstanceProvider instanceProvider,
																											@NonNull ResultSetMetaData resultSetMetaData) throws Exception {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(instanceProvider);
		requireNonNull(resultSetMetaData);

		Class<T> resultSetRowType = statementContext.getResultSetRowType().get();

		RecordComponent[] recordComponents = determineRecordComponents(resultSetRowType);
		Map<String, Set<String>> columnLabelsByRecordComponentName = determineColumnLabelsByRecordComponentName(resultSetRowType);
		Map<String, Object> columnLabelsToValues = extractColumnLabelsToValues(statementContext, resultSet, resultSetMetaData);

		Map<String, TargetType> propertyTargetTypes = determinePropertyTargetTypes(resultSetRowType);

		Object[] args = new Object[recordComponents.length];

		for (int i = 0; i < recordComponents.length; ++i) {
			RecordComponent recordComponent = recordComponents[i];
			String propertyName = recordComponent.getName();

			// If there are any @DatabaseColumn annotations on this field, respect them
			Set<String> potentialPropertyNames = columnLabelsByRecordComponentName.get(propertyName);

			// There were no @DatabaseColumn annotations, use the default naming strategy
			if (potentialPropertyNames == null || potentialPropertyNames.isEmpty())
				potentialPropertyNames = databaseColumnNamesForPropertyName(propertyName);

			Class<?> recordComponentType = recordComponent.getType();

			// Get the precise TargetType (generic-aware) for this component
			TargetType targetType = propertyTargetTypes.get(propertyName);
			if (targetType == null) targetType = TargetType.of(recordComponent.getGenericType());

			// Set the value for the Record ctor
			for (String potentialPropertyName : potentialPropertyNames) {
				if (!columnLabelsToValues.containsKey(potentialPropertyName))
					continue;

				Object rawValue = columnLabelsToValues.get(potentialPropertyName);

				Object value = rawValue == null
						? null
						: // Try custom mappers first; if none apply, fall back to built-ins
						tryCustomColumnMappers(statementContext, resultSet, rawValue, targetType, null, potentialPropertyName, instanceProvider)
								.or(() -> convertResultSetValueToPropertyType(statementContext, rawValue, recordComponentType))
								.orElse(null);

				// It's considered programmer error to have a NULL value in the ResultSet and map it to a primitive (which does not support null)
				if (value == null && recordComponentType.isPrimitive())
					throw new DatabaseException(format("Column '%s' is NULL but record component '%s' of %s is primitive (%s). Use a non-primitive type or COALESCE/CAST in SQL.",
							potentialPropertyName, recordComponent.getName(), resultSetRowType.getSimpleName(), recordComponentType.getSimpleName()));

				Class<?> checkedType = boxedClass(recordComponentType);

				if (value != null && !checkedType.isInstance(value)) {
					String resultSetTypeDescription = value.getClass().toString();

					throw new DatabaseException(
							format(
									"Property '%s' of %s has a write method of type %s, but the ResultSet type %s does not match. "
											+ "Consider providing a %s to %s to detect instances of %s and convert them to %s",
									recordComponent.getName(), resultSetRowType, recordComponentType, resultSetTypeDescription,
									CustomColumnMapper.class.getSimpleName(), DefaultResultSetMapper.class.getSimpleName(), resultSetTypeDescription, recordComponentType));
				}

				args[i] = value;
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
	@NonNull
	protected <T> T mapResultSetToBean(@NonNull StatementContext<T> statementContext,
																		 @NonNull ResultSet resultSet,
																		 @NonNull InstanceProvider instanceProvider,
																		 @NonNull ResultSetMetaData resultSetMetaData) throws Exception {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(instanceProvider);
		requireNonNull(resultSetMetaData);

		Class<T> resultSetRowType = statementContext.getResultSetRowType().get();

		T object = instanceProvider.provide(statementContext, resultSetRowType);
		PropertyDescriptor[] propertyDescriptors = determinePropertyDescriptors(resultSetRowType);
		Map<String, Object> columnLabelsToValues = extractColumnLabelsToValues(statementContext, resultSet, resultSetMetaData);
		Map<String, Set<String>> normalizedColumnLabelsByPropertyName = determineNormalizedColumnLabelsByPropertyName(resultSetRowType);

		// Compute once per class (generic-aware)
		Map<String, TargetType> propertyTargetTypes = determinePropertyTargetTypes(resultSetRowType);

		for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
			Method writeMethod = propertyDescriptor.getWriteMethod();
			if (writeMethod == null) continue;

			Parameter parameter = writeMethod.getParameters()[0];
			Class<?> writeMethodParameterType = writeMethod.getParameterTypes()[0];

			// Pull in property names, taking into account any aliases defined by @DatabaseColumn
			Set<String> propertyNames = normalizedColumnLabelsByPropertyName.get(propertyDescriptor.getName());
			if (propertyNames == null || propertyNames.isEmpty())
				propertyNames = databaseColumnNamesForPropertyName(propertyDescriptor.getName());

			// Precise TargetType for this property (generic-aware)
			TargetType targetType = propertyTargetTypes.get(propertyDescriptor.getName());
			if (targetType == null) targetType = TargetType.of(parameter.getParameterizedType());

			for (String propertyName : propertyNames) {
				if (!columnLabelsToValues.containsKey(propertyName))
					continue;

				Object rawValue = columnLabelsToValues.get(propertyName);

				Object value = (rawValue == null)
						? null
						: // Try custom mappers first; if none apply, fall back to built-ins
						tryCustomColumnMappers(statementContext, resultSet, rawValue, targetType, null, propertyName, instanceProvider)
								.or(() -> convertResultSetValueToPropertyType(statementContext, rawValue, writeMethodParameterType))
								.orElse(null);

				// It's considered programmer error to have a NULL value in the ResultSet and map it to a primitive (which does not support null)
				if (value == null && writeMethodParameterType.isPrimitive())
					throw new DatabaseException(format("Column '%s' is NULL but bean property '%s' of %s is primitive (%s). Use a non-primitive type or COALESCE/CAST in SQL.",
							propertyName, propertyDescriptor.getName(), resultSetRowType.getSimpleName(), writeMethodParameterType.getSimpleName()));

				Class<?> checkedType = boxedClass(writeMethodParameterType);

				if (value != null && !checkedType.isInstance(value)) {
					String resultSetTypeDescription = value.getClass().toString();
					throw new DatabaseException(
							format(
									"Property '%s' of %s has a write method of type %s, but the ResultSet type %s does not match. "
											+ "Consider providing a %s to %s to detect instances of %s and convert them to %s",
									propertyDescriptor.getName(), resultSetRowType, writeMethodParameterType, resultSetTypeDescription,
									CustomColumnMapper.class.getSimpleName(), DefaultResultSetMapper.class.getSimpleName(), resultSetTypeDescription, writeMethodParameterType));
				}

				writeMethod.invoke(object, value);
			}
		}

		return object;
	}

	@NonNull
	protected Map<String, Set<String>> determineColumnLabelAliasesByPropertyName(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return getColumnLabelAliasesByPropertyNameCache().computeIfAbsent(
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

	@NonNull
	protected <T> Map<String, Object> extractColumnLabelsToValues(@NonNull StatementContext<T> statementContext,
																																@NonNull ResultSet resultSet,
																																@NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetMetaData);

		int columnCount = resultSetMetaData.getColumnCount();
		Map<String, Object> columnLabelsToValues = new HashMap<>(columnCount);
		Map<String, String> normalizedLabelsToRawLabels = new HashMap<>(columnCount);

		for (int i = 1; i <= columnCount; i++) {
			String rawLabel = resultSetMetaData.getColumnLabel(i);
			String label = normalizeColumnLabel(rawLabel);
			String previousRawLabel = normalizedLabelsToRawLabels.putIfAbsent(label, rawLabel);

			if (previousRawLabel != null) {
				throw new DatabaseException(format(
						"Duplicate column label '%s' (normalized from '%s' and '%s'); use column aliases to disambiguate.",
						label, previousRawLabel, rawLabel));
			}

			Object resultSetValue = extractColumnValue(resultSetMetaData, statementContext, resultSet, i).orElse(null);

			columnLabelsToValues.put(label, resultSetValue);
		}

		return columnLabelsToValues;
	}

	@NonNull
	protected <T> Optional<Object> extractColumnValue(@NonNull ResultSetMetaData resultSetMetaData,
																										@NonNull StatementContext<T> statementContext,
																										@NonNull ResultSet resultSet,
																										int columnIndex) throws SQLException {
		Object resultSetValue;

		int jdbcType = resultSetMetaData.getColumnType(columnIndex);
		boolean tsTz = TemporalReaders.isTimestampWithTimeZone(resultSetMetaData, columnIndex, statementContext.getDatabaseType());
		boolean timeTz = TemporalReaders.isTimeWithTimeZone(resultSetMetaData, columnIndex);

		if (tsTz) {
			resultSetValue = TemporalReaders.asOffsetDateTime(resultSet, columnIndex, statementContext, resultSetMetaData);
		} else if (jdbcType == Types.TIMESTAMP) {
			resultSetValue = TemporalReaders.asLocalDateTime(resultSet, columnIndex, statementContext, resultSetMetaData);
		} else if (jdbcType == Types.DATE) {
			resultSetValue = TemporalReaders.asLocalDate(resultSet, columnIndex);
		} else if (timeTz) {
			resultSetValue = TemporalReaders.asOffsetTime(resultSet, columnIndex, statementContext, resultSetMetaData);
		} else if (jdbcType == Types.TIME) {
			resultSetValue = TemporalReaders.asLocalTime(resultSet, columnIndex);
		} else {
			// Non-temporal or unknown: take the driver’s native object (PGobject, BigDecimal, etc.)
			resultSetValue = resultSet.getObject(columnIndex);
		}

		return Optional.ofNullable(resultSetValue);
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
	@NonNull
	protected <T> Optional<Object> convertResultSetValueToPropertyType(@NonNull StatementContext<T> statementContext,
																																		 @Nullable Object resultSetValue,
																																		 @NonNull Class<?> propertyType) {
		requireNonNull(statementContext);
		requireNonNull(propertyType);

		if (resultSetValue == null)
			return Optional.empty();

		// Normalize primitives to wrappers
		Class<?> targetType = boxedClass(propertyType);

		// Numbers
		if (resultSetValue instanceof BigDecimal bigDecimal) {
			if (BigDecimal.class.isAssignableFrom(targetType))
				return Optional.of(bigDecimal);
			if (BigInteger.class.isAssignableFrom(targetType))
				return Optional.of(bigDecimal.toBigInteger());
		}

		if (resultSetValue instanceof BigInteger bigInteger) {
			if (BigDecimal.class.isAssignableFrom(targetType))
				return Optional.of(new BigDecimal(bigInteger));
			if (BigInteger.class.isAssignableFrom(targetType))
				return Optional.of(bigInteger);
		}

			if (resultSetValue instanceof Number number) {
				if (Byte.class.isAssignableFrom(targetType))
					return Optional.of(number.byteValue());
				if (Short.class.isAssignableFrom(targetType))
					return Optional.of(number.shortValue());
			if (Integer.class.isAssignableFrom(targetType))
				return Optional.of(number.intValue());
			if (Long.class.isAssignableFrom(targetType))
				return Optional.of(number.longValue());
			if (Float.class.isAssignableFrom(targetType))
				return Optional.of(number.floatValue());
				if (Double.class.isAssignableFrom(targetType))
					return Optional.of(number.doubleValue());
				if (BigDecimal.class.isAssignableFrom(targetType)) {
					if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long)
						return Optional.of(BigDecimal.valueOf(number.longValue()));
					return Optional.of(new BigDecimal(number.toString()));
				}
				if (BigInteger.class.isAssignableFrom(targetType)) {
					if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long)
						return Optional.of(BigInteger.valueOf(number.longValue()));
					return Optional.of(new BigDecimal(number.toString()).toBigInteger());
				}
			}

		// Legacy java.sql.* coming from drivers
		if (resultSetValue instanceof java.sql.Timestamp timestamp) {
			if (Date.class.isAssignableFrom(targetType))
				return Optional.of(timestamp);
			if (Instant.class.isAssignableFrom(targetType))
				return Optional.of(timestamp.toInstant());
			if (LocalDate.class.isAssignableFrom(targetType))
				return Optional.of(timestamp.toLocalDateTime().toLocalDate());
			if (LocalDateTime.class.isAssignableFrom(targetType))
				return Optional.of(timestamp.toLocalDateTime());
			if (OffsetDateTime.class.isAssignableFrom(targetType))
				return Optional.of(timestamp.toInstant().atZone(statementContext.getTimeZone()).toOffsetDateTime());
		}

		if (resultSetValue instanceof java.sql.Date date) {
			if (Date.class.isAssignableFrom(targetType))
				return Optional.of(date);
			if (Instant.class.isAssignableFrom(targetType))
				return Optional.of(date.toInstant());
			if (LocalDate.class.isAssignableFrom(targetType))
				return Optional.of(date.toLocalDate());
			if (LocalDateTime.class.isAssignableFrom(targetType))
				return Optional.of(LocalDateTime.ofInstant(date.toInstant(), statementContext.getTimeZone()));
		}

		if (resultSetValue instanceof java.sql.Time time) {
			if (LocalTime.class.isAssignableFrom(targetType))
				return Optional.of(time.toLocalTime());
		}

		// New java.time values (preferred)
		if (resultSetValue instanceof Instant instant) {
			if (Instant.class.isAssignableFrom(targetType))
				return Optional.of(instant);
			if (Date.class.isAssignableFrom(targetType))
				return Optional.of(Date.from(instant));
			if (LocalDateTime.class.isAssignableFrom(targetType))
				return Optional.of(instant.atZone(statementContext.getTimeZone()).toLocalDateTime());
			if (LocalDate.class.isAssignableFrom(targetType))
				return Optional.of(instant.atZone(statementContext.getTimeZone()).toLocalDate());
			if (OffsetDateTime.class.isAssignableFrom(targetType))
				return Optional.of(instant.atZone(statementContext.getTimeZone()).toOffsetDateTime());
			if (java.sql.Timestamp.class.isAssignableFrom(targetType))
				return Optional.of(Timestamp.from(instant));
			if (java.sql.Date.class.isAssignableFrom(targetType))
				return Optional.of(java.sql.Date.valueOf(instant.atZone(statementContext.getTimeZone()).toLocalDate()));
		}

		if (resultSetValue instanceof LocalDateTime localDateTime) {
			if (LocalDateTime.class.isAssignableFrom(targetType))
				return Optional.of(localDateTime);
			if (Instant.class.isAssignableFrom(targetType))
				return Optional.of(localDateTime.atZone(statementContext.getTimeZone()).toInstant());
			if (Date.class.isAssignableFrom(targetType))
				return Optional.of(Date.from(localDateTime.atZone(statementContext.getTimeZone()).toInstant()));
			if (LocalDate.class.isAssignableFrom(targetType))
				return Optional.of(localDateTime.toLocalDate());
			if (OffsetDateTime.class.isAssignableFrom(targetType))
				return Optional.of(localDateTime.atZone(statementContext.getTimeZone()).toOffsetDateTime());
			if (java.sql.Timestamp.class.isAssignableFrom(targetType))
				return Optional.of(Timestamp.valueOf(localDateTime));
			if (java.sql.Date.class.isAssignableFrom(targetType))
				return Optional.of(java.sql.Date.valueOf(localDateTime.toLocalDate()));
		}

		if (resultSetValue instanceof OffsetDateTime offsetDateTime) {
			if (OffsetDateTime.class.isAssignableFrom(targetType))
				return Optional.of(offsetDateTime);
			if (Instant.class.isAssignableFrom(targetType))
				return Optional.of(offsetDateTime.toInstant());
			if (LocalDateTime.class.isAssignableFrom(targetType))
				return Optional.of(offsetDateTime.atZoneSameInstant(statementContext.getTimeZone()).toLocalDateTime());
			if (Date.class.isAssignableFrom(targetType))
				return Optional.of(Date.from(offsetDateTime.toInstant()));
			if (java.sql.Timestamp.class.isAssignableFrom(targetType))
				return Optional.of(Timestamp.from(offsetDateTime.toInstant()));
		}

		if (resultSetValue instanceof LocalDate localDate) {
			if (LocalDate.class.isAssignableFrom(targetType))
				return Optional.of(localDate);
			if (LocalDateTime.class.isAssignableFrom(targetType))
				return Optional.of(localDate.atStartOfDay());
			if (Instant.class.isAssignableFrom(targetType))
				return Optional.of(localDate.atStartOfDay(statementContext.getTimeZone()).toInstant());
			if (java.sql.Date.class.isAssignableFrom(targetType))
				return Optional.of(java.sql.Date.valueOf(localDate));
			if (Date.class.isAssignableFrom(targetType))
				return Optional.of(Date.from(localDate.atStartOfDay(statementContext.getTimeZone()).toInstant()));
		}

		if (resultSetValue instanceof LocalTime localTime) {
			if (LocalTime.class.isAssignableFrom(targetType))
				return Optional.of(localTime);

			if (OffsetTime.class.isAssignableFrom(targetType)) {
				ZoneOffset off = statementContext.getTimeZone().getRules().getOffset(Instant.EPOCH);
				return Optional.of(localTime.atOffset(off));
			}

			if (java.sql.Time.class.isAssignableFrom(targetType))
				return Optional.of(java.sql.Time.valueOf(localTime));
		}

		if (resultSetValue instanceof OffsetTime offsetTime) {
			if (OffsetTime.class.isAssignableFrom(targetType))
				return Optional.of(offsetTime);
			if (LocalTime.class.isAssignableFrom(targetType))
				return Optional.of(offsetTime.toLocalTime());
			if (java.sql.Time.class.isAssignableFrom(targetType))
				return Optional.of(java.sql.Time.valueOf(offsetTime.toLocalTime()));
		}

		if (UUID.class.isAssignableFrom(targetType)) {
			try {
				return Optional.ofNullable(UUID.fromString(resultSetValue.toString()));
			} catch (IllegalArgumentException e) {
				throw new DatabaseException(format("Unable to convert value '%s' to UUID", resultSetValue), e);
			}
		} else if (ZoneId.class.isAssignableFrom(targetType)) {
			try {
				return Optional.ofNullable(ZoneId.of(resultSetValue.toString()));
			} catch (DateTimeException e) {
				throw new DatabaseException(format("Unable to convert value '%s' to ZoneId", resultSetValue), e);
			}
		} else if (TimeZone.class.isAssignableFrom(targetType)) {
			return Optional.of(timeZoneFromId(resultSetValue.toString()));
		} else if (Locale.class.isAssignableFrom(targetType)) {
			return Optional.of(localeFromLanguageTag(resultSetValue.toString()));
		} else if (Currency.class.isAssignableFrom(targetType)) {
			try {
				return Optional.ofNullable(Currency.getInstance(resultSetValue.toString()));
			} catch (IllegalArgumentException e) {
				throw new DatabaseException(format("Unable to convert value '%s' to Currency", resultSetValue), e);
			}
		} else if (targetType.isEnum()) {
			return Optional.ofNullable(extractEnumValue(targetType, resultSetValue));
		} else if ("org.postgresql.util.PGobject".equals(resultSetValue.getClass().getName())) {
			org.postgresql.util.PGobject pgObject = (org.postgresql.util.PGobject) resultSetValue;
			return Optional.ofNullable(pgObject.getValue());
		}

		if (Boolean.class.isAssignableFrom(targetType)) {
			// Native boolean
			if (resultSetValue instanceof Boolean b)
				return Optional.of(b);

			// Numeric truthiness (0=false, nonzero=true)
			if (resultSetValue instanceof Number number)
				return Optional.of(number.intValue() != 0);

			throw new DatabaseException(format("Cannot map value '%s' (%s) to boolean", resultSetValue, resultSetValue.getClass().getName()));
		}

		if (Character.class.isAssignableFrom(targetType)) {
			// Native char
			if (resultSetValue instanceof Character c) return Optional.of(c);

			// Single-character string
			if (resultSetValue instanceof String string) {
				if (string.length() == 1)
					return Optional.of(string.charAt(0));

				throw new DatabaseException(format("Cannot map String value '%s' to %s; expected length 1", string, propertyType.getSimpleName()));
			}

			// Numeric -> Unicode code point (useful for some JDBC drivers / legacy schemas)
			if (resultSetValue instanceof Number number) {
				int code = number.intValue();
				if (code >= Character.MIN_VALUE && code <= Character.MAX_VALUE)
					return Optional.of((char) code);

				throw new DatabaseException(format("Numeric value %d is outside valid char range", code));
			}

			throw new DatabaseException(format("Cannot map %s to %s",
					resultSetValue.getClass().getName(), propertyType.getSimpleName()));
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
	@NonNull
	protected Enum<?> extractEnumValue(@NonNull Class<?> enumClass,
																		 @NonNull Object object) {
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

	@NonNull
	private static TimeZone timeZoneFromId(@NonNull String zoneId) {
		requireNonNull(zoneId);

		String trimmed = zoneId.trim();
		TimeZone timeZone = TimeZone.getTimeZone(trimmed);

		if ("GMT".equals(timeZone.getID())) {
			String upper = trimmed.toUpperCase(Locale.ROOT);

			if (!upper.equals("GMT") && !upper.equals("UTC") && !upper.equals("UT")) {
				if (upper.startsWith("GMT") || upper.startsWith("UTC") || upper.startsWith("UT")) {
					try {
						ZoneId.of(upper);
					} catch (DateTimeException e) {
						throw new DatabaseException(format("Unable to convert value '%s' to TimeZone", zoneId), e);
					}
				} else {
					throw new DatabaseException(format("Unable to convert value '%s' to TimeZone", zoneId));
				}
			}
		}

		return timeZone;
	}

	@NonNull
	private static Locale localeFromLanguageTag(@NonNull String languageTag) {
		requireNonNull(languageTag);

		String trimmed = languageTag.trim();
		if (trimmed.isEmpty())
			return Locale.ROOT;

		try {
			return new Locale.Builder().setLanguageTag(trimmed).build();
		} catch (IllformedLocaleException e) {
			throw new DatabaseException(format("Unable to convert value '%s' to Locale", languageTag), e);
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
	@NonNull
	protected String normalizeColumnLabel(@NonNull String columnLabel) {
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
	@NonNull
	protected Set<String> databaseColumnNamesForPropertyName(@NonNull String propertyName) {
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
	@NonNull
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
		@NonNull
		private final Boolean standardType;

		/**
		 * Creates a {@code StandardTypeResult} with the given {@code value} and {@code standardType} flag.
		 *
		 * @param value        the mapping result, may be {@code null}
		 * @param standardType {@code true} if the mapped type was a standard type, {@code false} otherwise
		 */
		public StandardTypeResult(@Nullable T value,
															@NonNull Boolean standardType) {
			requireNonNull(standardType);

			this.value = value;
			this.standardType = standardType;
		}

		/**
		 * Gets the result of the mapping.
		 *
		 * @return the mapping result value, may be {@code null}
		 */
		@NonNull
		public Optional<T> getValue() {
			return Optional.ofNullable(this.value);
		}

		/**
		 * Was the mapped type a standard type?
		 *
		 * @return {@code true} if this was a standard type, {@code false} otherwise
		 */
		@NonNull
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

		public static Instant asInstant(ResultSet rs, int col, StatementContext<?> ctx, ResultSetMetaData resultSetMetaData) throws SQLException {
			boolean withTz = isTimestampWithTimeZone(resultSetMetaData, col, ctx.getDatabaseType());
			return asInstant(rs, col, ctx, withTz);
		}

		public static Instant asInstant(ResultSet rs, int col, StatementContext<?> ctx, boolean withTz) throws SQLException {
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

		public static OffsetDateTime asOffsetDateTime(ResultSet rs, int col, StatementContext<?> ctx, ResultSetMetaData resultSetMetaData) throws SQLException {
			boolean withTz = isTimestampWithTimeZone(resultSetMetaData, col, ctx.getDatabaseType());
			return asOffsetDateTime(rs, col, ctx, withTz);
		}

		public static OffsetDateTime asOffsetDateTime(ResultSet rs, int col, StatementContext<?> ctx, boolean withTz) throws SQLException {
			OffsetDateTime got = tryGet(rs, col, OffsetDateTime.class);
			if (got != null) return got;

			if (withTz) {
				// Try via Instant if driver only gives us Timestamp
				Instant inst = asInstant(rs, col, ctx, true);
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

		public static ZonedDateTime asZonedDateTime(ResultSet rs, int col, StatementContext<?> ctx, ResultSetMetaData resultSetMetaData) throws SQLException {
			Instant inst = asInstant(rs, col, ctx, resultSetMetaData);
			return inst == null ? null : inst.atZone(ctx.getTimeZone());
		}

		public static LocalDateTime asLocalDateTime(ResultSet rs, int col, StatementContext<?> ctx, ResultSetMetaData resultSetMetaData) throws SQLException {
			boolean withTz = isTimestampWithTimeZone(resultSetMetaData, col, ctx.getDatabaseType());
			return asLocalDateTime(rs, col, ctx, withTz);
		}

		public static LocalDateTime asLocalDateTime(ResultSet rs, int col, StatementContext<?> ctx, boolean withTz) throws SQLException {
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

		public static OffsetTime asOffsetTime(ResultSet rs, int col, StatementContext<?> ctx, ResultSetMetaData resultSetMetaData) throws SQLException {
			boolean withTz = isTimeWithTimeZone(resultSetMetaData, col);
			return asOffsetTime(rs, col, ctx, withTz);
		}

		public static OffsetTime asOffsetTime(ResultSet rs, int col, StatementContext<?> ctx, boolean withTz) throws SQLException {
			if (withTz) {
				OffsetTime ot = tryGet(rs, col, OffsetTime.class);
				if (ot != null) return ot;
			}
			// If DB has plain TIME, attach DB zone offset at an arbitrary date (offset depends on date!)
			LocalTime lt = asLocalTime(rs, col);
			if (lt == null) return null;
			// Choose a stable anchor instant to avoid DST-dependent offsets (use epoch)
			Instant anchor = Instant.EPOCH;
			ZoneOffset off = ctx.getTimeZone().getRules().getOffset(anchor);
			return lt.atOffset(off);
		}
	}

	@Nullable
	private Integer getNullableInt(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		int v = resultSet.getInt(columnIndex);
		return resultSet.wasNull() ? null : v;
	}

	@Nullable
	private Long getNullableLong(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		long v = resultSet.getLong(columnIndex);
		return resultSet.wasNull() ? null : v;
	}

	@Nullable
	private Short getNullableShort(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		short v = resultSet.getShort(columnIndex);
		return resultSet.wasNull() ? null : v;
	}

	@Nullable
	private Byte getNullableByte(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		byte v = resultSet.getByte(columnIndex);
		return resultSet.wasNull() ? null : v;
	}

	@Nullable
	private Float getNullableFloat(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		float v = resultSet.getFloat(columnIndex);
		return resultSet.wasNull() ? null : v;
	}

	@Nullable
	private Double getNullableDouble(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		double v = resultSet.getDouble(columnIndex);
		return resultSet.wasNull() ? null : v;
	}

	@Nullable
	private Boolean getNullableBoolean(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		boolean v = resultSet.getBoolean(columnIndex);
		return resultSet.wasNull() ? null : v;
	}

	@ThreadSafe
	protected static final class PlanKey {
		final Class<?> resultClass;
		final String schemaSignature;

		PlanKey(@NonNull Class<?> resultClass, @NonNull String schemaSignature) {
			this.resultClass = requireNonNull(resultClass);
			this.schemaSignature = requireNonNull(schemaSignature);
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			PlanKey that = (PlanKey) o;
			return resultClass.equals(that.resultClass) && schemaSignature.equals(that.schemaSignature);
		}

		@Override
		public int hashCode() {
			return 31 * resultClass.hashCode() + schemaSignature.hashCode();
		}
	}

	/**
	 * Reads a single column value, using the right temporal/native getter decided at plan time.
	 */
	@FunctionalInterface
	protected interface ColumnReader {
		@Nullable
		Object read(@NonNull ResultSet rs,
								@NonNull StatementContext<?> ctx,
								int col) throws SQLException;
	}

	/**
	 * Converts a raw value to the property/record target type, including custom mappers.
	 */
	@FunctionalInterface
	protected interface Converter {
		@Nullable
		Object convert(@Nullable Object raw,
									 @NonNull ResultSet rs,
									 @NonNull StatementContext<?> ctx,
									 @NonNull InstanceProvider ip);
	}

	@ThreadSafe
	protected static final class ColumnBinding {
		final int columnIndex; // 1-based
		final int recordArgIndexOrMinusOne; // >=0 for record components, -1 for bean or SKIP
		final @Nullable MethodHandle setterOrNull; // for bean properties; type: (Declaring, Param)void or (Declaring, Param)Object
		final @NonNull ColumnReader reader; // chosen at plan time
		final @NonNull Converter converter; // chosen at plan time
		final boolean targetIsPrimitive;        // original target declared as primitive?
		final @NonNull Class<?> targetRawClass; // BOXED class for type checks
		final @NonNull String propertyName;     // for helpful errors; empty if SKIP

		ColumnBinding(int columnIndex,
									int recordArgIndexOrMinusOne,
									@Nullable MethodHandle setterOrNull,
									@NonNull ColumnReader reader,
									@NonNull Converter converter,
									boolean targetIsPrimitive,
									@NonNull Class<?> targetRawClass,
									@NonNull String propertyName) {
			this.columnIndex = columnIndex;
			this.recordArgIndexOrMinusOne = recordArgIndexOrMinusOne;
			this.setterOrNull = setterOrNull;
			this.reader = requireNonNull(reader);
			this.converter = requireNonNull(converter);
			this.targetIsPrimitive = targetIsPrimitive;
			this.targetRawClass = requireNonNull(targetRawClass);
			this.propertyName = requireNonNull(propertyName);
		}
	}

	@ThreadSafe
	protected static final class RowPlan<T> {
		final boolean isRecord;
		final int columnCount;                   // for per-row raw caching
		final boolean hasFanOut;                 // any column maps to multiple properties?
		final @NonNull List<ColumnBinding> bindings; // may contain multiple entries per column (fan-out)

		RowPlan(boolean isRecord,
						int columnCount,
						boolean hasFanOut,
						@NonNull List<ColumnBinding> bindings) {
			this.isRecord = isRecord;
			this.columnCount = columnCount;
			this.hasFanOut = hasFanOut;
			this.bindings = requireNonNull(bindings);
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@NonNull
	protected <T> RowPlan<T> buildPlan(@NonNull StatementContext<T> ctx,
																		 @NonNull ResultSet rs,
																		 @NonNull Class<T> resultClass,
																		 @NonNull InstanceProvider instanceProvider,
																		 @NonNull ResultSetMetaData resultSetMetaData,
																		 @NonNull String schemaSignature) throws Exception {
		requireNonNull(ctx);
		requireNonNull(rs);
		requireNonNull(resultClass);
		requireNonNull(instanceProvider);
		requireNonNull(resultSetMetaData);
		requireNonNull(schemaSignature);

		int cols = resultSetMetaData.getColumnCount();

		PlanKey key = new PlanKey(resultClass, schemaSignature);
		RowPlan<?> cached = getRowPlanningCache().get(key);
		if (cached != null) return (RowPlan<T>) cached;

		// Precompute property metadata for this class
		Map<String, TargetType> propertyTargetTypes = determinePropertyTargetTypes(resultClass);
		Map<String, Set<String>> aliasByProp = determineColumnLabelAliasesByPropertyName(resultClass);

		final boolean isRecord = resultClass.isRecord();
		final MethodHandles.Lookup lookup = MethodHandles.lookup();

		// record: name->index map
		final Map<String, Integer> recordIndexByProp = new HashMap<>();
		final RecordComponent[] recordComponents = isRecord ? determineRecordComponents(resultClass) : null;
		if (isRecord) {
			for (int i = 0; i < recordComponents.length; i++) {
				recordIndexByProp.put(recordComponents[i].getName(), i);
			}
		}

		// bean: property -> setter handle
		final Map<String, MethodHandle> setterByProp = new HashMap<>();
		if (!isRecord) {
			BeanInfo beanInfo = Introspector.getBeanInfo(resultClass);
			for (PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
				Method w = pd.getWriteMethod();
				if (w != null) setterByProp.put(pd.getName(), lookup.unreflect(w));
			}
		}

		// Union of all writable property names
		Set<String> allProps = new HashSet<>();
		allProps.addAll(setterByProp.keySet());
		allProps.addAll(recordIndexByProp.keySet());

		// Precompute label -> properties to avoid scanning all properties per column
		Map<String, List<String>> propertiesByLabel = new HashMap<>(allProps.size() * 2);
		for (String prop : allProps) {
			Set<String> labels = new HashSet<>();
			Set<String> anno = aliasByProp.get(prop);
			if (anno != null) labels.addAll(anno);
			labels.addAll(databaseColumnNamesForPropertyName(prop));

			for (String label : labels)
				propertiesByLabel.computeIfAbsent(label, labelKey -> new ArrayList<>(1)).add(prop);
		}

		// Build per-column readers (decided once)
		final ColumnReader[] readers = new ColumnReader[cols + 1]; // 1-based indexing
		for (int i = 1; i <= cols; i++) {
			int jdbcType = resultSetMetaData.getColumnType(i);
			boolean tsTz = TemporalReaders.isTimestampWithTimeZone(resultSetMetaData, i, ctx.getDatabaseType());
			boolean timeTz = TemporalReaders.isTimeWithTimeZone(resultSetMetaData, i);
			if (tsTz) {
				readers[i] = (r, c, col) -> TemporalReaders.asOffsetDateTime(r, col, c, true);
			} else if (jdbcType == Types.TIMESTAMP) {
				readers[i] = (r, c, col) -> TemporalReaders.asLocalDateTime(r, col, c, false);
			} else if (jdbcType == Types.DATE) {
				readers[i] = (r, c, col) -> TemporalReaders.asLocalDate(r, col);
			} else if (timeTz) {
				readers[i] = (r, c, col) -> TemporalReaders.asOffsetTime(r, col, c, true);
			} else if (jdbcType == Types.TIME) {
				readers[i] = (r, c, col) -> TemporalReaders.asLocalTime(r, col);
			} else {
				readers[i] = (r, c, col) -> r.getObject(col);
			}
		}

		// For each column, bind ALL matching properties (fan-out)
		List<ColumnBinding> bindings = new ArrayList<>();
		boolean hasFanOut = false;
		Map<String, String> normalizedLabelsToRawLabels = new HashMap<>(cols);
		for (int i = 1; i <= cols; i++) {
			String rawLabel = resultSetMetaData.getColumnLabel(i);
			String labelNorm = normalizeColumnLabel(rawLabel);
			String previousRawLabel = normalizedLabelsToRawLabels.putIfAbsent(labelNorm, rawLabel);

			if (previousRawLabel != null) {
				throw new DatabaseException(format(
						"Duplicate column label '%s' (normalized from '%s' and '%s'); use column aliases to disambiguate.",
						labelNorm, previousRawLabel, rawLabel));
			}
			ColumnReader reader = readers[i];

			// Find all properties that claim this label
			List<String> matchedProps = propertiesByLabel.get(labelNorm);

			if (matchedProps == null || matchedProps.isEmpty()) {
				// No property claims this column — add a SKIP binding to keep behavior explicit
				bindings.add(
						new ColumnBinding(
								i,                            // columnIndex
								-1,                           // recordArgIndexOrMinusOne (SKIP)
								null,                         // setterOrNull
								reader,                       // reader
								(raw, rrs, cctx, ip) -> raw,  // converter: 4-arg form now
								false,                        // targetIsPrimitive (SKIP => false)
								Object.class,                 // targetRawClass (boxed)
								""                            // propertyName (empty for SKIP)
						)
				);
				continue;
			}

			if (matchedProps.size() > 1)
				hasFanOut = true;

			// Create one binding per matched property
			for (String prop : matchedProps) {
				// Determine precise TargetType (generic aware)
				TargetType ttype = propertyTargetTypes.get(prop);
				if (ttype == null) {
					if (isRecord) {
						int argIndex = requireNonNull(recordIndexByProp.get(prop));
						ttype = TargetType.of(recordComponents[argIndex].getGenericType());
					} else {
						MethodHandle setter = setterByProp.get(prop);
						Class<?> param = setter.type().parameterType(1); // (Declaring, Param)
						ttype = TargetType.of(param);
					}
				}

				final int recordArgIndex = isRecord ? requireNonNull(recordIndexByProp.get(prop)) : -1;
				final MethodHandle setterMH = isRecord ? null : setterByProp.get(prop);

				// Unboxed + boxed target, and primitive flag
				final Class<?> rawTargetClassUnboxed = isRecord
						? recordComponents[recordArgIndex].getType()
						: setterMH.type().parameterType(1);
				final Class<?> rawTargetClassBoxed = boxedClass(rawTargetClassUnboxed);
				final boolean targetIsPrimitive = rawTargetClassUnboxed.isPrimitive();

				final TargetType targetTypeFinal = ttype;
				final String propertyNameFinal = prop;
				final int colIndexFinal = i;
				final String labelFinal = labelNorm;

				List<CustomColumnMapper> customMappers = customColumnMappersFor(targetTypeFinal);
				Converter converter = (raw, rrs, cctx, ip) -> {
					if (raw == null) return null;
					try {
						if (!customMappers.isEmpty()) {
							Optional<Object> mapped = tryCustomColumnMappers(cctx, rrs, raw, targetTypeFinal, customMappers, colIndexFinal, labelFinal, ip);
							if (mapped.isPresent())
								return mapped.get();
						}

						if (rawTargetClassBoxed.isInstance(raw))
							return raw;

						return convertResultSetValueToPropertyType(cctx, raw, rawTargetClassBoxed)
								.orElse(raw);
					} catch (SQLException e) {
						throw new DatabaseException(e);
					}
				};

				bindings.add(new ColumnBinding(i, recordArgIndex, setterMH, reader, converter,
						targetIsPrimitive, rawTargetClassBoxed, propertyNameFinal));
			}
		}

		RowPlan<T> plan = new RowPlan<>(isRecord, cols, hasFanOut, bindings);
		getRowPlanningCache().putIfAbsent(key, plan);
		return plan;
	}

	@NonNull
	protected List<CustomColumnMapper> getCustomColumnMappers() {
		return this.customColumnMappers;
	}

	@NonNull
	protected Boolean getPlanCachingEnabled() {
		return this.planCachingEnabled;
	}

	@NonNull
	protected ConcurrentMap<TargetType, List<CustomColumnMapper>> getCustomColumnMappersByTargetTypeCache() {
		return this.customColumnMappersByTargetTypeCache;
	}

	@NonNull
	protected Map<SourceTargetKey, CustomColumnMapper> getPreferredColumnMapperBySourceTargetKey() {
		return this.preferredColumnMapperBySourceTargetKey;
	}

	@NonNull
	protected ConcurrentMap<Class<?>, Map<String, TargetType>> getPropertyTargetTypeCache() {
		return this.propertyTargetTypeCache;
	}

	@NonNull
	protected ConcurrentMap<Class<?>, Map<String, Set<String>>> getColumnLabelAliasesByPropertyNameCache() {
		return this.columnLabelAliasesByPropertyNameCache;
	}

	@NonNull
	protected ConcurrentMap<Class<?>, PropertyDescriptor[]> getPropertyDescriptorsCache() {
		return this.propertyDescriptorsCache;
	}

	@NonNull
	protected ConcurrentMap<Class<?>, Map<String, Set<String>>> getNormalizedColumnLabelsByPropertyNameCache() {
		return this.normalizedColumnLabelsByPropertyNameCache;
	}

	@NonNull
	protected ConcurrentMap<Class<?>, Map<String, Set<String>>> getColumnLabelsByRecordComponentNameCache() {
		return this.columnLabelsByRecordComponentNameCache;
	}

	@NonNull
	protected ConcurrentMap<Class<?>, RecordComponent[]> getRecordComponentsCache() {
		return this.recordComponentsCache;
	}

	@NonNull
	protected Map<PlanKey, RowPlan<?>> getRowPlanningCache() {
		return this.rowPlanningCache;
	}

	@NonNull
	protected Map<ResultSetMetaData, String> getSchemaSignatureByResultSetMetaData() {
		return this.schemaSignatureByResultSetMetaData;
	}
}
