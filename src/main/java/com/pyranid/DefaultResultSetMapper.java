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

import com.pyranid.CustomColumnMapper.MappingResult;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Array;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.IllformedLocaleException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

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
	private static final Pattern CAMEL_CASE_PATTERN = Pattern.compile("([a-z])([A-Z]+)");
	@NonNull
	private static final Pattern LETTER_FOLLOWED_BY_NUMBER_PATTERN = Pattern.compile("(\\D)(\\d)");

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
	private final ClassValue<List<CustomColumnMapper>> customColumnMappersByRawClassCache =
			new ClassValue<>() {
				@Override
				protected List<CustomColumnMapper> computeValue(Class<?> type) {
					return computeCustomColumnMappersFor(TargetType.of(type));
				}
			};
	@NonNull
	private final ClassValue<Map<PreferredColumnMapperKey, CustomColumnMapper>> preferredColumnMapperBySourceClass =
			new ClassValue<>() {
				@Override
				protected Map<PreferredColumnMapperKey, CustomColumnMapper> computeValue(Class<?> type) {
					return createCache(DefaultResultSetMapper.this.preferredColumnMapperCacheCapacity);
				}
			};
	@NonNull
	private final ClassValue<Map<String, TargetType>> propertyTargetTypeCache =
			new ClassValue<>() {
				@Override
				protected Map<String, TargetType> computeValue(Class<?> type) {
					return computePropertyTargetTypes(type);
				}
			};
	@NonNull
	private final ClassValue<Map<String, Set<String>>> columnLabelAliasesByPropertyNameCache =
			new ClassValue<>() {
				@Override
				protected Map<String, Set<String>> computeValue(Class<?> type) {
					return computeColumnLabelAliasesByPropertyName(type);
				}
			};
	@NonNull
	private final ClassValue<WritableProperty[]> writablePropertiesCache =
			new ClassValue<>() {
				@Override
				protected WritableProperty[] computeValue(Class<?> type) {
					return computeWritableProperties(type);
				}
			};
	@NonNull
	private final ClassValue<Map<String, Set<String>>> normalizedColumnLabelsByPropertyNameCache =
			new ClassValue<>() {
				@Override
				protected Map<String, Set<String>> computeValue(Class<?> type) {
					return computeNormalizedColumnLabelsByPropertyName(type);
				}
			};
	@NonNull
	private final ClassValue<Map<String, Set<String>>> columnLabelsByRecordComponentNameCache =
			new ClassValue<>() {
				@Override
				protected Map<String, Set<String>> computeValue(Class<?> type) {
					return computeColumnLabelsByRecordComponentName(type);
				}
			};
	@NonNull
	private final ClassValue<RecordComponent[]> recordComponentsCache =
			new ClassValue<>() {
				@Override
				protected RecordComponent[] computeValue(Class<?> type) {
					RecordComponent[] components = type.getRecordComponents();
					return components == null ? new RecordComponent[0] : components;
				}
			};
	// Only used if row-planning is enabled
	@NonNull
	private final ClassValue<Map<String, RowPlan<?>>> rowPlanningCacheByResultClass =
			new ClassValue<>() {
				@Override
				protected Map<String, RowPlan<?>> computeValue(Class<?> type) {
					return createCache(DefaultResultSetMapper.this.planCacheCapacity);
				}
			};
	@NonNull
	private final Map<ResultSetMetaData, String> schemaSignatureByResultSetMetaData;
	@NonNull
	private final ReentrantLock schemaSignatureByResultSetMetaDataLock;
	@NonNull
	private final ThreadLocal<SchemaSignatureMemo> schemaSignatureMemo;

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

	@NonNull
	protected List<@NonNull CustomColumnMapper> customColumnMappersFor(@NonNull TargetType targetType) {
		requireNonNull(targetType);

		if (getCustomColumnMappers().isEmpty())
			return List.of();

		if (targetType.getTypeArguments().isEmpty())
			return this.customColumnMappersByRawClassCache.get(targetType.getRawClass());

		// Avoid caching parameterized types to prevent holding user classes under system raw types.
		return computeCustomColumnMappersFor(targetType);
	}

	@NonNull
	private List<CustomColumnMapper> computeCustomColumnMappersFor(@NonNull TargetType targetType) {
		requireNonNull(targetType);

		if (getCustomColumnMappers().isEmpty())
			return List.of();

		List<CustomColumnMapper> filtered = new ArrayList<>(getCustomColumnMappers().size());

		for (CustomColumnMapper customColumnMapper : getCustomColumnMappers())
			if (customColumnMapper.appliesTo(targetType))
				filtered.add(customColumnMapper);

		return Collections.unmodifiableList(filtered);
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

	@ThreadSafe
	private static final class CustomMappingOutcome {
		@NonNull
		private static final CustomMappingOutcome NOT_APPLIED = new CustomMappingOutcome(false, null);
		private final boolean applied;
		@Nullable
		private final Object value;

		private CustomMappingOutcome(boolean applied,
																 @Nullable Object value) {
			this.applied = applied;
			this.value = value;
		}

		@NonNull
		private static CustomMappingOutcome notApplied() {
			return NOT_APPLIED;
		}

		@NonNull
		private static CustomMappingOutcome applied(@Nullable Object value) {
			return new CustomMappingOutcome(true, value);
		}

		private boolean isApplied() {
			return this.applied;
		}

		@Nullable
		private Object getValue() {
			return this.value;
		}
	}

	@NonNull
	protected Class<?> boxedClass(@NonNull Class<?> type) {
		requireNonNull(type);
		return type.isPrimitive() ? WRAPPER_CLASSES_BY_PRIMITIVE_CLASS.get(type) : type;
	}

	@NonNull
	protected <T> CustomMappingOutcome tryCustomColumnMappers(@NonNull StatementContext<T> statementContext,
																												@NonNull ResultSet resultSet,
																												@NonNull Object resultSetValue,
																												@NonNull TargetType targetType,
																												@NonNull Integer columnIndex,
																												@Nullable String columnLabel,
																												@NonNull InstanceProvider instanceProvider) {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetValue);
		requireNonNull(targetType);
		requireNonNull(instanceProvider);

		List<CustomColumnMapper> mappers = customColumnMappersFor(targetType);
		return tryCustomColumnMappers(statementContext, resultSet, resultSetValue, targetType, mappers, columnIndex, columnLabel, instanceProvider);
	}

	@NonNull
	protected <T> CustomMappingOutcome tryCustomColumnMappers(@NonNull StatementContext<T> statementContext,
																												@NonNull ResultSet resultSet,
																												@NonNull Object resultSetValue,
																												@NonNull TargetType targetType,
																												@NonNull List<CustomColumnMapper> mappers,
																												@NonNull Integer columnIndex,
																												@Nullable String columnLabel,
																												@NonNull InstanceProvider instanceProvider) {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetValue);
		requireNonNull(targetType);
		requireNonNull(mappers);
		requireNonNull(instanceProvider);

		if (mappers.isEmpty())
			return CustomMappingOutcome.notApplied();

		Class<?> sourceClass = resultSetValue.getClass();
		PreferredColumnMapperKey preferredKey = preferredColumnMapperKeyFor(targetType);
		Map<PreferredColumnMapperKey, CustomColumnMapper> preferredByTargetType =
				preferredKey == null ? null : getPreferredColumnMapperCacheForSourceClass(sourceClass);

		for (CustomColumnMapper mapper : mappers) {
			CustomColumnMapper.MappingResult mappingResult;

			try {
				mappingResult = mapper.map(statementContext, resultSet, resultSetValue, targetType, columnIndex, columnLabel, instanceProvider);
			} catch (SQLException e) {
				throw new DatabaseException(e);
			}

			if (mappingApplied(mappingResult)) {
				if (preferredByTargetType != null && preferredKey != null)
					preferredByTargetType.put(preferredKey, mapper);
				return CustomMappingOutcome.applied(mappedValue(mappingResult).orElse(null));
			}
		}

		return CustomMappingOutcome.notApplied();
	}

	/**
	 * Determines (and caches) the TargetType of each writable JavaBean property and each Record component
	 * for the given result class. Keys are property/record-component names.
	 */
	@NonNull
	protected Map<@NonNull String, @NonNull TargetType> determinePropertyTargetTypes(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return this.propertyTargetTypeCache.get(resultClass);
	}

	@NonNull
	private Map<String, TargetType> computePropertyTargetTypes(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		Map<String, TargetType> types = new HashMap<>();

		// JavaBean setters (preserve generics like List<UUID>)
		for (WritableProperty writableProperty : determineWritableProperties(resultClass)) {
			Type genericParameterType = writableProperty.getWriteMethod().getGenericParameterTypes()[0];
			types.put(writableProperty.getName(), TargetType.of(genericParameterType));
		}

		// Java records (preserve generics)
		if (resultClass.isRecord())
			for (RecordComponent recordComponent : resultClass.getRecordComponents())
				types.put(recordComponent.getName(), TargetType.of(recordComponent.getGenericType()));

		return Collections.unmodifiableMap(types);
	}

	@NonNull
	protected WritableProperty[] determineWritableProperties(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return this.writablePropertiesCache.get(resultClass);
	}

	@NonNull
	private WritableProperty[] computeWritableProperties(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		List<WritableProperty> discoveredProperties = new ArrayList<>();

		for (Method method : resultClass.getMethods()) {
			String propertyName = propertyNameForSetter(method).orElse(null);

			if (propertyName != null)
				discoveredProperties.add(new WritableProperty(propertyName, method));
		}

		discoveredProperties.sort((left, right) -> {
			int propertyNameComparison = left.getName().compareTo(right.getName());
			if (propertyNameComparison != 0)
				return propertyNameComparison;

			int declaringClassComparison = Integer.compare(
					declaringClassDistance(resultClass, left.getWriteMethod().getDeclaringClass()),
					declaringClassDistance(resultClass, right.getWriteMethod().getDeclaringClass()));
			if (declaringClassComparison != 0)
				return declaringClassComparison;

			return methodSignature(left.getWriteMethod()).compareTo(methodSignature(right.getWriteMethod()));
		});

		Map<String, WritableProperty> propertiesByName = new HashMap<>();
		for (WritableProperty discoveredProperty : discoveredProperties)
			propertiesByName.putIfAbsent(discoveredProperty.getName(), discoveredProperty);

		List<WritableProperty> writableProperties = new ArrayList<>(propertiesByName.values());
		writableProperties.sort((left, right) -> left.getName().compareTo(right.getName()));

		return writableProperties.toArray(WritableProperty[]::new);
	}

	@NonNull
	private Optional<String> propertyNameForSetter(@NonNull Method method) {
		requireNonNull(method);

		if (method.isBridge() || method.isSynthetic())
			return Optional.empty();
		if (Modifier.isStatic(method.getModifiers()))
			return Optional.empty();
		if (!method.getName().startsWith("set") || method.getName().length() <= 3)
			return Optional.empty();
		if (method.getParameterCount() != 1)
			return Optional.empty();
		if (method.getReturnType() != Void.TYPE)
			return Optional.empty();

		return Optional.of(decapitalizeJavaBeansPropertyName(method.getName().substring(3)));
	}

	@NonNull
	private String decapitalizeJavaBeansPropertyName(@NonNull String propertyName) {
		requireNonNull(propertyName);

		if (propertyName.length() > 1 && Character.isUpperCase(propertyName.charAt(0)) && Character.isUpperCase(propertyName.charAt(1)))
			return propertyName;

		char[] chars = propertyName.toCharArray();
		chars[0] = Character.toLowerCase(chars[0]);
		return new String(chars);
	}

	private int declaringClassDistance(@NonNull Class<?> resultClass,
																		 @NonNull Class<?> declaringClass) {
		requireNonNull(resultClass);
		requireNonNull(declaringClass);

		int distance = 0;
		for (Class<?> current = resultClass; current != null; current = current.getSuperclass()) {
			if (current == declaringClass)
				return distance;

			++distance;
		}

		return Integer.MAX_VALUE;
	}

	@NonNull
	private String methodSignature(@NonNull Method method) {
		requireNonNull(method);

		return format("%s(%s)", method.getName(), asList(method.getParameterTypes()).stream()
				.map(Class::getName)
				.collect(joining(",")));
	}

	@NonNull
	protected RecordComponent[] determineRecordComponents(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return this.recordComponentsCache.get(resultClass);
	}

	@NonNull
	protected Map<@NonNull String, @NonNull Set<@NonNull String>> determineNormalizedColumnLabelsByPropertyName(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return this.normalizedColumnLabelsByPropertyNameCache.get(resultClass);
	}

	@NonNull
	private Map<String, Set<String>> computeNormalizedColumnLabelsByPropertyName(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		Map<String, Set<String>> columnLabelAliasesByPropertyName = determineColumnLabelAliasesByPropertyName(resultClass);
		Map<String, Set<String>> normalizedLabelsByPropertyName = new HashMap<>();

		for (WritableProperty writableProperty : determineWritableProperties(resultClass)) {
			String propertyName = writableProperty.getName();
			Set<String> baseNames = columnLabelAliasesByPropertyName.get(propertyName);
			if (baseNames == null || baseNames.isEmpty())
				baseNames = Set.of(propertyName);

			Set<String> normalized = new HashSet<>();
			for (String baseName : baseNames)
				normalized.addAll(databaseColumnNamesForPropertyName(baseName));

			normalizedLabelsByPropertyName.put(propertyName, Collections.unmodifiableSet(normalized));
		}

		return Collections.unmodifiableMap(normalizedLabelsByPropertyName);
	}

	@NonNull
	protected Map<@NonNull String, @NonNull Set<@NonNull String>> determineColumnLabelsByRecordComponentName(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return this.columnLabelsByRecordComponentNameCache.get(resultClass);
	}

	@NonNull
	private Map<String, Set<String>> computeColumnLabelsByRecordComponentName(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		Map<String, Set<String>> columnLabelAliasesByPropertyName = determineColumnLabelAliasesByPropertyName(resultClass);
		Map<String, Set<String>> labelsByComponentName = new HashMap<>();

		for (RecordComponent recordComponent : determineRecordComponents(resultClass)) {
			String propertyName = recordComponent.getName();
			Set<String> labels = columnLabelAliasesByPropertyName.get(propertyName);
			if (labels == null || labels.isEmpty())
				labels = databaseColumnNamesForPropertyName(propertyName);

			labelsByComponentName.put(propertyName, Collections.unmodifiableSet(labels));
		}

		return Collections.unmodifiableMap(labelsByComponentName);
	}

	DefaultResultSetMapper(@NonNull Builder builder) {
		requireNonNull(builder);

		this.normalizationLocale = requireNonNull(builder.normalizationLocale);
		this.customColumnMappers = Collections.unmodifiableList(requireNonNull(builder.customColumnMappers));
		this.planCachingEnabled = requireNonNull(builder.planCachingEnabled);

		this.preferredColumnMapperCacheCapacity = builder.preferredColumnMapperCacheCapacity;
		this.planCacheCapacity = builder.planCacheCapacity;
		this.schemaSignatureByResultSetMetaData = new WeakHashMap<>();
		this.schemaSignatureByResultSetMetaDataLock = new ReentrantLock();
		this.schemaSignatureMemo = new ThreadLocal<>();
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

		// Map row target: return the row as an insertion-ordered (column-ordered) map of
		// normalized (lowercase) column labels to dialect-aware extracted values. Checked before the
		// row-planning fork — no row plan applies to a Map. Note: subclasses invoking the protected
		// mapWithRowPlanning/mapWithoutRowPlanning methods directly bypass this special case.
		if (resultSetRowType == Map.class || resultSetRowType == LinkedHashMap.class) {
			@SuppressWarnings("unchecked")
			T row = (T) mapResultSetToMap(statementContext, resultSet, resultSetMetaData);
			return Optional.of(row);
		}

		if (UNSUPPORTED_MAP_ROW_TYPES.contains(resultSetRowType))
			throw new DatabaseException(format(
					"%s is not a supported Map row type. Use Map.class or LinkedHashMap.class to fetch rows as maps.",
					resultSetRowType.getName()));

		if (getPlanCachingEnabled())
			return mapWithRowPlanning(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData);

		return mapWithoutRowPlanning(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData);
	}

	/**
	 * Well-known JDK map types that are rejected with a clear error instead of falling confusingly into
	 * the JavaBean mapping path. User classes that happen to implement {@link Map} are deliberately NOT
	 * rejected — they retain their existing bean-path behavior.
	 */
	@NonNull
	private static final Set<Class<?>> UNSUPPORTED_MAP_ROW_TYPES = Set.of(
			HashMap.class, TreeMap.class, Hashtable.class, ConcurrentHashMap.class,
			SortedMap.class, NavigableMap.class, ConcurrentMap.class);

	/**
	 * Maps the current row of {@code resultSet} to an insertion-ordered map of normalized (lowercase)
	 * column labels to values.
	 * <p>
	 * Keys are normalized via {@link #normalizeColumnLabel(String)} so lookups are portable across
	 * databases that report unquoted labels in different cases (e.g. Oracle and HSQLDB uppercase,
	 * PostgreSQL lowercases). Column order is preserved. {@code NULL} columns are present with a
	 * {@code null} value. Values are extracted with the same dialect-aware logic used for record and
	 * JavaBean mapping. Duplicate normalized labels (e.g. an unaliased multi-table join) fail fast.
	 *
	 * @param statementContext  current SQL context
	 * @param resultSet         provides raw row data to pull from
	 * @param resultSetMetaData metadata for {@code resultSet}
	 * @return the row as an insertion-ordered label-to-value map
	 * @throws SQLException if an error occurs during mapping
	 * @since 4.4.1
	 */
	@NonNull
	protected <T> Map<String, Object> mapResultSetToMap(@NonNull StatementContext<T> statementContext,
																											@NonNull ResultSet resultSet,
																											@NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetMetaData);

		int columnCount = resultSetMetaData.getColumnCount();
		Map<String, Object> row = new LinkedHashMap<>(columnCount);
		Map<String, String> normalizedLabelsToRawLabels = new HashMap<>(columnCount);

		for (int i = 1; i <= columnCount; i++) {
			String rawLabel = resultSetMetaData.getColumnLabel(i);
			String label = normalizeColumnLabel(rawLabel);
			String previousRawLabel = normalizedLabelsToRawLabels.putIfAbsent(label, rawLabel);

			if (previousRawLabel != null)
				throw new DatabaseException(format(
						"Duplicate column label '%s' (normalized from '%s' and '%s'); use column aliases to disambiguate.",
						label, previousRawLabel, rawLabel));

			row.put(label, extractColumnValue(resultSetMetaData, statementContext, resultSet, i).orElse(null));
		}

		return row;
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
			String schemaSignature = schemaSignatureFor(statementContext, resultSet, resultSetMetaData);
			RowPlan<T> plan = buildPlan(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData, schemaSignature);
			final boolean hasFanOut = plan.hasFanOut;
			// Per-row raw cache: 1-based, size = columnCount + 1 (only when we have fan-out)
			final Object[] rawByCol = hasFanOut ? new Object[plan.columnCount + 1] : null;
			final boolean[] rawLoaded = hasFanOut ? new boolean[plan.columnCount + 1] : null;

			if (plan.isRecord) {
				if (plan.mappedColumnCount == 0)
					throw noRecordColumnMappingsException(resultSetRowType, resultSetMetaData);

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

				for (int i = 0; i < rc.length; i++) {
					if (rc[i].getType().isPrimitive() && args[i] == null) {
						throw new DatabaseException(format(
								"No column matches record component '%s' of %s, but the component is primitive (%s). "
										+ "Ensure the column is selected/aliased or use a non-primitive type.",
								rc[i].getName(), resultSetRowType.getSimpleName(), rc[i].getType().getSimpleName()));
					}
				}

				// Construct via InstanceProvider to keep semantics consistent with non-planned path
				@SuppressWarnings("unchecked")
				T rec = (T) provideRecordVia(instanceProvider, statementContext, resultSetRowType, args);
				return Optional.of(rec);
			} else {
				// Bean
				if (plan.mappedColumnCount == 0)
					throw noWritableBeanColumnMappingsException(resultSetRowType, resultSetMetaData);

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
																			@NonNull ResultSet resultSet,
																			@NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetMetaData);

		SchemaSignatureMemo memo = getSchemaSignatureMemo().get();

		if (memo != null && memo.matches(resultSet))
			return memo.getSchemaSignature();

		String schemaSignature = schemaSignatureFor(statementContext, resultSetMetaData);
		getSchemaSignatureMemo().set(new SchemaSignatureMemo(resultSet, schemaSignature));
		return schemaSignature;
	}

	@NonNull
	protected String schemaSignatureFor(@NonNull StatementContext<?> statementContext,
																			@NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSetMetaData);

		ReentrantLock lock = getSchemaSignatureByResultSetMetaDataLock();
		lock.lock();

		try {
			String signature = getSchemaSignatureByResultSetMetaData().get(resultSetMetaData);
			if (signature != null)
				return signature;
		} finally {
			lock.unlock();
		}

		String signature = buildSchemaSignature(statementContext, resultSetMetaData);

		lock.lock();

		try {
			String existingSignature = getSchemaSignatureByResultSetMetaData().get(resultSetMetaData);
			if (existingSignature != null)
				return existingSignature;

			getSchemaSignatureByResultSetMetaData().put(resultSetMetaData, signature);
		} finally {
			lock.unlock();
		}

		return signature;
	}

	@NonNull
	private String buildSchemaSignature(@NonNull StatementContext<?> statementContext,
																			@NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSetMetaData);

		int cols = resultSetMetaData.getColumnCount();
		StringBuilder sig = new StringBuilder(64 + cols * 48).append(cols);
		DatabaseDialect databaseDialect = statementContext.getDatabaseDialect();

		for (int i = 1; i <= cols; i++) {
			String labelNorm = normalizeColumnLabel(resultSetMetaData.getColumnLabel(i));
			int jdbcType = resultSetMetaData.getColumnType(i);
			boolean tsTz = databaseDialect.isTimestampWithTimeZone(resultSetMetaData, i);
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

	@NonNull
	private ThreadLocal<SchemaSignatureMemo> getSchemaSignatureMemo() {
		return this.schemaSignatureMemo;
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
			int columnCount = resultSetMetaData.getColumnCount();
			Boolean rowColumnsClaimed = null;

			if (columnCount != 1) {
				rowColumnsClaimed = resultSetHasMappedRowColumns(resultClass, resultSetMetaData);
				if (rowColumnsClaimed)
					return new StandardTypeResult<>(null, false);
			}

			Object resultSetValue = extractColumnValue(resultSetMetaData, statementContext, resultSet, 1).orElse(null);

			if (resultSetValue == null) {
				if (rowColumnsClaimed == null)
					rowColumnsClaimed = resultSetHasMappedRowColumns(resultClass, resultSetMetaData);
				if (rowColumnsClaimed)
					return new StandardTypeResult<>(null, false);

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
				String columnLabel = normalizeColumnLabel(resultSetMetaData.getColumnLabel(1));
				CustomMappingOutcome outcome =
						tryCustomColumnMappers(statementContext,
								resultSet,
								resultSetValue,
								targetType,
								1,
								columnLabel,
								instanceProvider);

				if (outcome.isApplied()) {
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
					T cast = (T) outcome.getValue();
					return new StandardTypeResult<>(cast, true);
				}

				// If no mapper applied (Fallback), we proceed to built-ins below.
			}
		}

		if (resultClass == Byte.class || resultClass == byte.class) {
			value = getNullableByte(resultSet, 1);
		} else if (resultClass == Short.class || resultClass == short.class) {
			value = getNullableShort(resultSet, 1);
		} else if (resultClass == Integer.class || resultClass == int.class) {
			value = getNullableInt(resultSet, 1);
		} else if (resultClass == Long.class || resultClass == long.class) {
			value = getNullableLong(resultSet, 1);
		} else if (resultClass == Float.class || resultClass == float.class) {
			value = getNullableFloat(resultSet, 1);
		} else if (resultClass == Double.class || resultClass == double.class) {
			value = getNullableDouble(resultSet, 1);
		} else if (resultClass == Boolean.class || resultClass == boolean.class) {
			value = getNullableBoolean(resultSet, 1);
		} else if (resultClass == Character.class || resultClass == char.class) {
			String string = resultSet.getString(1);

			if (string != null) {
				if (string.length() == 1)
					value = string.charAt(0);
				else
					throw new DatabaseException(format("Cannot map String value '%s' to %s", string, resultClass.getSimpleName()));
			}
		} else if (resultClass == String.class) {
			value = resultSet.getString(1);
		} else if (resultClass == byte[].class) {
			value = resultSet.getBytes(1);
		} else if (resultClass.isArray() || resultClass == List.class || resultClass == Set.class) {
			Object raw = resultSet.getObject(1);
			value = raw == null ? null : convertResultSetValueToTargetType(statementContext, raw, TargetType.of(resultClass))
					.orElse(null);

			if (value != null && !resultClass.isInstance(value)) {
				throw new DatabaseException(format("Cannot map value '%s' (%s) to %s",
						raw, raw.getClass().getName(), resultClass.getSimpleName()));
			}
		} else if (resultClass.isEnum()) {
			Object raw = resultSet.getObject(1);
			if (raw == null)
				value = null; // -> Optional.empty()
			else
				value = extractEnumValue(resultClass, raw);
		} else if (resultClass == UUID.class) {
			value = uuidFromValue(resultSet.getObject(1));
		} else if (resultClass == BigDecimal.class) {
			value = resultSet.getBigDecimal(1);
		} else if (resultClass == BigInteger.class) {
			BigDecimal bd = resultSet.getBigDecimal(1);
			if (bd != null) value = exactBigInteger(bd);
		} else if (resultClass == Number.class) {
			Object raw = resultSet.getObject(1);
			if (raw == null) {
				value = null;
			} else if (raw instanceof Number) {
				value = raw;
			} else {
				throw new DatabaseException(format("Cannot map value '%s' (%s) to %s",
						raw, raw.getClass().getName(), resultClass.getSimpleName()));
			}
		} else if (resultClass == Date.class) {
			Instant inst = TemporalReaders.asInstant(resultSet, 1, statementContext, resultSetMetaData);
			value = (inst == null) ? null : Date.from(inst);
		} else if (resultClass == Instant.class) {
			value = TemporalReaders.asInstant(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == LocalDate.class) {
			value = TemporalReaders.asLocalDate(resultSet, 1);
		} else if (resultClass == LocalTime.class) {
			value = TemporalReaders.asLocalTime(resultSet, 1);
		} else if (resultClass == LocalDateTime.class) {
			value = TemporalReaders.asLocalDateTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == OffsetTime.class) {
			value = TemporalReaders.asOffsetTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == OffsetDateTime.class) {
			value = TemporalReaders.asOffsetDateTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == ZonedDateTime.class) {
			value = TemporalReaders.asZonedDateTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == java.sql.Timestamp.class) {
			boolean withTz = statementContext.getDatabaseDialect().isTimestampWithTimeZone(resultSetMetaData, 1);

			if (withTz) {
				Instant inst = TemporalReaders.asInstant(resultSet, 1, statementContext, resultSetMetaData);
				value = (inst == null) ? null : Timestamp.from(inst);
			} else {
				LocalDateTime ldt = TemporalReaders.asLocalDateTime(resultSet, 1, statementContext, resultSetMetaData);
				value = (ldt == null) ? null : Timestamp.valueOf(ldt);
			}
		} else if (resultClass == java.sql.Date.class) {
			LocalDate ld = TemporalReaders.asLocalDate(resultSet, 1);
			value = (ld == null) ? null : java.sql.Date.valueOf(ld);
		} else if (resultClass == ZoneId.class) {
			String zoneId = resultSet.getString(1);
			if (zoneId != null) {
				try {
					value = ZoneId.of(zoneId);
				} catch (DateTimeException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to ZoneId", zoneId), e);
				}
			}
		} else if (resultClass == TimeZone.class) {
			String tz = resultSet.getString(1);
			if (tz != null) value = timeZoneFromId(tz);
		} else if (resultClass == Locale.class) {
			String locale = resultSet.getString(1);
			if (locale != null) value = localeFromLanguageTag(locale);
		} else if (resultClass == Currency.class) {
			String currency = resultSet.getString(1);
			if (currency != null) {
				try {
					value = Currency.getInstance(currency);
				} catch (IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to Currency", currency), e);
				}
			}
		} else if (resultClass == Object.class) {
			value = resultSet.getObject(1);
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

	private boolean resultSetHasMappedRowColumns(@NonNull Class<?> resultClass,
																							 @NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(resultClass);
		requireNonNull(resultSetMetaData);

		if (resultClass.isRecord())
			return resultSetHasMappedRowColumns(determineColumnLabelsByRecordComponentName(resultClass), resultSetMetaData);

		return resultSetHasMappedRowColumns(determineNormalizedColumnLabelsByPropertyName(resultClass), resultSetMetaData);
	}

	private boolean resultSetHasMappedRowColumns(@NonNull Map<@NonNull String, @NonNull Set<@NonNull String>> labelsByPropertyName,
																							 @NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(labelsByPropertyName);
		requireNonNull(resultSetMetaData);

		if (labelsByPropertyName.isEmpty())
			return false;

		int columnCount = resultSetMetaData.getColumnCount();

		for (int i = 1; i <= columnCount; ++i) {
			String label = normalizeColumnLabel(resultSetMetaData.getColumnLabel(i));

			for (Set<String> propertyLabels : labelsByPropertyName.values())
				if (propertyLabels.contains(label))
					return true;
		}

		return false;
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
		ColumnLabelMaps columnLabelMaps = extractColumnLabelsToValues(statementContext, resultSet, resultSetMetaData);
		Map<String, Object> columnLabelsToValues = columnLabelMaps.getValuesByLabel();
		Map<String, Integer> columnLabelsToIndexes = columnLabelMaps.getIndexesByLabel();

		Map<String, TargetType> propertyTargetTypes = determinePropertyTargetTypes(resultSetRowType);

		Object[] args = new Object[recordComponents.length];
		int mappedColumnCount = 0;
		List<RecordComponent> missingPrimitiveRecordComponents = new ArrayList<>();

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

			// Set the value for the Record ctor (apply in column order for deterministic results)
			List<String> orderedPropertyNames = new ArrayList<>(potentialPropertyNames.size());
			for (String potentialPropertyName : potentialPropertyNames)
				if (columnLabelsToValues.containsKey(potentialPropertyName))
					orderedPropertyNames.add(potentialPropertyName);

			if (orderedPropertyNames.isEmpty()) {
				if (recordComponentType.isPrimitive())
					missingPrimitiveRecordComponents.add(recordComponent);
				continue;
			}

			orderedPropertyNames.sort((left, right) ->
					Integer.compare(columnLabelsToIndexes.get(left), columnLabelsToIndexes.get(right)));

			for (String potentialPropertyName : orderedPropertyNames) {
				++mappedColumnCount;
				Object rawValue = columnLabelsToValues.get(potentialPropertyName);

				Integer columnIndex = requireNonNull(columnLabelsToIndexes.get(potentialPropertyName));
				Object value;
				if (rawValue == null) {
					value = null;
				} else {
					CustomMappingOutcome outcome = tryCustomColumnMappers(statementContext, resultSet, rawValue, targetType, columnIndex, potentialPropertyName, instanceProvider);
					value = outcome.isApplied()
							? outcome.getValue()
							: convertResultSetValueToTargetType(statementContext, rawValue, targetType).orElse(null);
				}

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

		if (mappedColumnCount == 0)
			throw noRecordColumnMappingsException(resultSetRowType, resultSetMetaData);

		if (!missingPrimitiveRecordComponents.isEmpty()) {
			RecordComponent missingPrimitiveRecordComponent = missingPrimitiveRecordComponents.get(0);
			throw new DatabaseException(format(
					"No column matches record component '%s' of %s, but the component is primitive (%s). "
							+ "Ensure the column is selected/aliased or use a non-primitive type.",
					missingPrimitiveRecordComponent.getName(), resultSetRowType.getSimpleName(), missingPrimitiveRecordComponent.getType().getSimpleName()));
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

		T object = null;
		WritableProperty[] writableProperties = determineWritableProperties(resultSetRowType);
		ColumnLabelMaps columnLabelMaps = extractColumnLabelsToValues(statementContext, resultSet, resultSetMetaData);
		Map<String, Object> columnLabelsToValues = columnLabelMaps.getValuesByLabel();
		Map<String, Integer> columnLabelsToIndexes = columnLabelMaps.getIndexesByLabel();
		Map<String, Set<String>> normalizedColumnLabelsByPropertyName = determineNormalizedColumnLabelsByPropertyName(resultSetRowType);

		// Compute once per class (generic-aware)
		Map<String, TargetType> propertyTargetTypes = determinePropertyTargetTypes(resultSetRowType);
		int mappedColumnCount = 0;

		for (WritableProperty writableProperty : writableProperties) {
			Method writeMethod = writableProperty.getWriteMethod();

			Parameter parameter = writeMethod.getParameters()[0];
			Class<?> writeMethodParameterType = writeMethod.getParameterTypes()[0];

			// Pull in property names, taking into account any aliases defined by @DatabaseColumn
			Set<String> propertyNames = normalizedColumnLabelsByPropertyName.get(writableProperty.getName());
			if (propertyNames == null || propertyNames.isEmpty())
				propertyNames = databaseColumnNamesForPropertyName(writableProperty.getName());

			// Precise TargetType for this property (generic-aware)
			TargetType targetType = propertyTargetTypes.get(writableProperty.getName());
			if (targetType == null) targetType = TargetType.of(parameter.getParameterizedType());

			List<String> orderedPropertyNames = new ArrayList<>(propertyNames.size());
			for (String propertyName : propertyNames)
				if (columnLabelsToValues.containsKey(propertyName))
					orderedPropertyNames.add(propertyName);

			orderedPropertyNames.sort((left, right) ->
					Integer.compare(columnLabelsToIndexes.get(left), columnLabelsToIndexes.get(right)));

			for (String propertyName : orderedPropertyNames) {
				++mappedColumnCount;
				Object rawValue = columnLabelsToValues.get(propertyName);

				Integer columnIndex = requireNonNull(columnLabelsToIndexes.get(propertyName));
				Object value;
				if (rawValue == null) {
					value = null;
				} else {
					CustomMappingOutcome outcome = tryCustomColumnMappers(statementContext, resultSet, rawValue, targetType, columnIndex, propertyName, instanceProvider);
					value = outcome.isApplied()
							? outcome.getValue()
							: convertResultSetValueToTargetType(statementContext, rawValue, targetType).orElse(null);
				}

				// It's considered programmer error to have a NULL value in the ResultSet and map it to a primitive (which does not support null)
				if (value == null && writeMethodParameterType.isPrimitive())
					throw new DatabaseException(format("Column '%s' is NULL but bean property '%s' of %s is primitive (%s). Use a non-primitive type or COALESCE/CAST in SQL.",
							propertyName, writableProperty.getName(), resultSetRowType.getSimpleName(), writeMethodParameterType.getSimpleName()));

				Class<?> checkedType = boxedClass(writeMethodParameterType);

				if (value != null && !checkedType.isInstance(value)) {
					String resultSetTypeDescription = value.getClass().toString();
					throw new DatabaseException(
							format(
									"Property '%s' of %s has a write method of type %s, but the ResultSet type %s does not match. "
											+ "Consider providing a %s to %s to detect instances of %s and convert them to %s",
									writableProperty.getName(), resultSetRowType, writeMethodParameterType, resultSetTypeDescription,
									CustomColumnMapper.class.getSimpleName(), DefaultResultSetMapper.class.getSimpleName(), resultSetTypeDescription, writeMethodParameterType));
				}

				if (object == null)
					object = instanceProvider.provide(statementContext, resultSetRowType);

				writeMethod.invoke(object, value);
			}
		}

		if (mappedColumnCount == 0)
			throw noWritableBeanColumnMappingsException(resultSetRowType, resultSetMetaData);

		return requireNonNull(object);
	}

	@NonNull
	private DatabaseException noWritableBeanColumnMappingsException(@NonNull Class<?> resultSetRowType,
																																 @NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(resultSetRowType);
		requireNonNull(resultSetMetaData);

		String columnLabels = selectedColumnLabels(resultSetMetaData).stream().collect(joining(", "));
		return new DatabaseException(format(
				"No result columns map to writable bean properties of %s. Selected columns: %s. "
						+ "Ensure the SQL selects/aliases columns that match bean setters, use a record target, or provide a custom ResultSetMapper.",
				resultSetRowType.getName(), columnLabels));
	}

	@NonNull
	private DatabaseException noRecordColumnMappingsException(@NonNull Class<?> resultSetRowType,
																													 @NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(resultSetRowType);
		requireNonNull(resultSetMetaData);

		String columnLabels = selectedColumnLabels(resultSetMetaData).stream().collect(joining(", "));
		return new DatabaseException(format(
				"No result columns map to record components of %s. Selected columns: %s. "
						+ "Ensure the SQL selects/aliases columns that match record components or provide a custom ResultSetMapper.",
				resultSetRowType.getName(), columnLabels));
	}

	@NonNull
	private List<String> selectedColumnLabels(@NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(resultSetMetaData);

		int columnCount = resultSetMetaData.getColumnCount();
		List<String> columnLabels = new ArrayList<>(columnCount);

		for (int i = 1; i <= columnCount; ++i)
			columnLabels.add(resultSetMetaData.getColumnLabel(i));

		return columnLabels;
	}

	@NonNull
	protected Map<@NonNull String, @NonNull Set<@NonNull String>> determineColumnLabelAliasesByPropertyName(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		return this.columnLabelAliasesByPropertyNameCache.get(resultClass);
	}

	@NonNull
	private Map<String, Set<String>> computeColumnLabelAliasesByPropertyName(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);

		Map<String, Set<String>> cachedColumnLabelAliasesByPropertyName = new HashMap<>();

		for (Class<?> current = resultClass; current != null && current != Object.class; current = current.getSuperclass()) {
			for (Field field : current.getDeclaredFields()) {
				DatabaseColumn databaseColumn = field.getAnnotation(DatabaseColumn.class);

				if (databaseColumn != null)
					cachedColumnLabelAliasesByPropertyName.putIfAbsent(
							field.getName(),
							unmodifiableSet(asList(databaseColumn.value()).stream()
									.map(columnLabel -> normalizeColumnLabel(columnLabel)).collect(toSet())));
			}
		}

		return unmodifiableMap(cachedColumnLabelAliasesByPropertyName);
	}

	@NonNull
	private static final class ColumnLabelMaps {
		@NonNull
		private final Map<@NonNull String, @Nullable Object> valuesByLabel;
		@NonNull
		private final Map<@NonNull String, @NonNull Integer> indexesByLabel;

		private ColumnLabelMaps(@NonNull Map<@NonNull String, @Nullable Object> valuesByLabel,
														@NonNull Map<@NonNull String, @NonNull Integer> indexesByLabel) {
			this.valuesByLabel = requireNonNull(valuesByLabel);
			this.indexesByLabel = requireNonNull(indexesByLabel);
		}

		@NonNull
		private Map<@NonNull String, @Nullable Object> getValuesByLabel() {
			return this.valuesByLabel;
		}

		@NonNull
		private Map<@NonNull String, @NonNull Integer> getIndexesByLabel() {
			return this.indexesByLabel;
		}
	}

	protected <T> ColumnLabelMaps extractColumnLabelsToValues(@NonNull StatementContext<T> statementContext,
																													 @NonNull ResultSet resultSet,
																													 @NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetMetaData);

		int columnCount = resultSetMetaData.getColumnCount();
		Map<String, Object> columnLabelsToValues = new HashMap<>(columnCount);
		Map<String, Integer> columnLabelsToIndexes = new HashMap<>(columnCount);
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
			columnLabelsToIndexes.put(label, i);
		}

		return new ColumnLabelMaps(columnLabelsToValues, columnLabelsToIndexes);
	}

	@NonNull
	protected <T> Optional<Object> extractColumnValue(@NonNull ResultSetMetaData resultSetMetaData,
																										@NonNull StatementContext<T> statementContext,
																										@NonNull ResultSet resultSet,
																										int columnIndex) throws SQLException {
		Object resultSetValue;

		int jdbcType = resultSetMetaData.getColumnType(columnIndex);
		boolean tsTz = statementContext.getDatabaseDialect().isTimestampWithTimeZone(resultSetMetaData, columnIndex);
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
			// Non-temporal or unknown: take the driver's native object (BigDecimal, SQLXML, etc.)
			resultSetValue = resultSet.getObject(columnIndex);

			if (resultSetValue instanceof Array sqlArray)
				resultSetValue = extractSqlArrayResultSetValue(sqlArray);
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
		return convertResultSetValueToTargetType(statementContext, resultSetValue, TargetType.of(propertyType));
	}

	@NonNull
	protected <T> Optional<Object> convertResultSetValueToTargetType(@NonNull StatementContext<T> statementContext,
																																 @Nullable Object resultSetValue,
																																 @NonNull TargetType targetType) {
		requireNonNull(statementContext);
		requireNonNull(targetType);

		if (resultSetValue == null)
			return Optional.empty();

		resultSetValue = statementContext.getDatabaseDialect().unwrapResultSetValue(resultSetValue);

		if (resultSetValue == null)
			return Optional.empty();

		Class<?> propertyType = targetType.getRawClass();

		if (resultSetValue instanceof Array sqlArray && (targetType.isArray() || targetType.isList() || targetType.isSet()))
			return Optional.of(convertSqlArrayResultSetValue(statementContext, sqlArray, targetType));

		if (resultSetValue.getClass().isArray() && (targetType.isArray() || targetType.isList() || targetType.isSet()))
			return Optional.of(convertArrayResultSetValue(statementContext, resultSetValue, targetType));

		// Normalize primitives to wrappers
		Class<?> targetClass = boxedClass(propertyType);

		// Numbers
		if (resultSetValue instanceof BigDecimal bigDecimal) {
			if (BigDecimal.class.isAssignableFrom(targetClass))
				return Optional.of(bigDecimal);
			if (BigInteger.class.isAssignableFrom(targetClass))
				return Optional.of(exactBigInteger(bigDecimal));
		}

		if (resultSetValue instanceof BigInteger bigInteger) {
			if (BigDecimal.class.isAssignableFrom(targetClass))
				return Optional.of(new BigDecimal(bigInteger));
			if (BigInteger.class.isAssignableFrom(targetClass))
				return Optional.of(bigInteger);
		}

		if (resultSetValue instanceof Number number) {
			if (Byte.class.isAssignableFrom(targetClass))
				return Optional.of(exactByte(number));
			if (Short.class.isAssignableFrom(targetClass))
				return Optional.of(exactShort(number));
			if (Integer.class.isAssignableFrom(targetClass))
				return Optional.of(exactInt(number));
			if (Long.class.isAssignableFrom(targetClass))
				return Optional.of(exactLong(number));
			if (Float.class.isAssignableFrom(targetClass))
				return Optional.of(finiteFloat(number));
			if (Double.class.isAssignableFrom(targetClass))
				return Optional.of(finiteDouble(number));
			if (BigDecimal.class.isAssignableFrom(targetClass)) {
				if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long)
					return Optional.of(BigDecimal.valueOf(number.longValue()));
				return Optional.of(new BigDecimal(number.toString()));
			}
			if (BigInteger.class.isAssignableFrom(targetClass)) {
				if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long)
					return Optional.of(BigInteger.valueOf(number.longValue()));
				return Optional.of(exactBigInteger(decimalForNumericConversion(number)));
			}
		}

		// Legacy java.sql.* coming from drivers
		if (resultSetValue instanceof java.sql.Timestamp timestamp) {
			if (Date.class.isAssignableFrom(targetClass))
				return Optional.of(timestamp);
			if (Instant.class.isAssignableFrom(targetClass))
				return Optional.of(timestamp.toInstant());
			if (LocalDate.class.isAssignableFrom(targetClass))
				return Optional.of(timestamp.toLocalDateTime().toLocalDate());
			if (LocalDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(timestamp.toLocalDateTime());
			if (OffsetDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(timestamp.toInstant().atZone(statementContext.getTimeZone()).toOffsetDateTime());
			if (ZonedDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(timestamp.toInstant().atZone(statementContext.getTimeZone()));
		}

		if (resultSetValue instanceof java.sql.Date date) {
			if (Date.class.isAssignableFrom(targetClass))
				return Optional.of(date);
			if (Instant.class.isAssignableFrom(targetClass))
				return Optional.of(date.toInstant());
			if (LocalDate.class.isAssignableFrom(targetClass))
				return Optional.of(date.toLocalDate());
			if (LocalDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(LocalDateTime.ofInstant(date.toInstant(), statementContext.getTimeZone()));
		}

		if (resultSetValue instanceof java.sql.Time time) {
			if (LocalTime.class.isAssignableFrom(targetClass))
				return Optional.of(time.toLocalTime());
		}

		// New java.time values (preferred)
		if (resultSetValue instanceof Instant instant) {
			if (Instant.class.isAssignableFrom(targetClass))
				return Optional.of(instant);
			if (Date.class.isAssignableFrom(targetClass))
				return Optional.of(Date.from(instant));
			if (LocalDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(instant.atZone(statementContext.getTimeZone()).toLocalDateTime());
			if (LocalDate.class.isAssignableFrom(targetClass))
				return Optional.of(instant.atZone(statementContext.getTimeZone()).toLocalDate());
			if (OffsetDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(instant.atZone(statementContext.getTimeZone()).toOffsetDateTime());
			if (ZonedDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(instant.atZone(statementContext.getTimeZone()));
			if (java.sql.Timestamp.class.isAssignableFrom(targetClass))
				return Optional.of(Timestamp.from(instant));
			if (java.sql.Date.class.isAssignableFrom(targetClass))
				return Optional.of(java.sql.Date.valueOf(instant.atZone(statementContext.getTimeZone()).toLocalDate()));
		}

		if (resultSetValue instanceof LocalDateTime localDateTime) {
			if (LocalDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(localDateTime);
			if (Instant.class.isAssignableFrom(targetClass))
				return Optional.of(localDateTime.atZone(statementContext.getTimeZone()).toInstant());
			if (Date.class.isAssignableFrom(targetClass))
				return Optional.of(Date.from(localDateTime.atZone(statementContext.getTimeZone()).toInstant()));
			if (LocalDate.class.isAssignableFrom(targetClass))
				return Optional.of(localDateTime.toLocalDate());
			if (OffsetDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(localDateTime.atZone(statementContext.getTimeZone()).toOffsetDateTime());
			if (ZonedDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(localDateTime.atZone(statementContext.getTimeZone()));
			if (java.sql.Timestamp.class.isAssignableFrom(targetClass))
				return Optional.of(Timestamp.valueOf(localDateTime));
			if (java.sql.Date.class.isAssignableFrom(targetClass))
				return Optional.of(java.sql.Date.valueOf(localDateTime.toLocalDate()));
		}

		if (resultSetValue instanceof OffsetDateTime offsetDateTime) {
			if (OffsetDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(offsetDateTime);
			if (Instant.class.isAssignableFrom(targetClass))
				return Optional.of(offsetDateTime.toInstant());
			if (LocalDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(offsetDateTime.atZoneSameInstant(statementContext.getTimeZone()).toLocalDateTime());
			if (Date.class.isAssignableFrom(targetClass))
				return Optional.of(Date.from(offsetDateTime.toInstant()));
			if (ZonedDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(offsetDateTime.atZoneSameInstant(statementContext.getTimeZone()));
			if (java.sql.Timestamp.class.isAssignableFrom(targetClass))
				return Optional.of(Timestamp.from(offsetDateTime.toInstant()));
		}

		if (resultSetValue instanceof LocalDate localDate) {
			if (LocalDate.class.isAssignableFrom(targetClass))
				return Optional.of(localDate);
			if (LocalDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(localDate.atStartOfDay());
			if (ZonedDateTime.class.isAssignableFrom(targetClass))
				return Optional.of(localDate.atStartOfDay(statementContext.getTimeZone()));
			if (Instant.class.isAssignableFrom(targetClass))
				return Optional.of(localDate.atStartOfDay(statementContext.getTimeZone()).toInstant());
			if (java.sql.Date.class.isAssignableFrom(targetClass))
				return Optional.of(java.sql.Date.valueOf(localDate));
			if (Date.class.isAssignableFrom(targetClass))
				return Optional.of(Date.from(localDate.atStartOfDay(statementContext.getTimeZone()).toInstant()));
		}

		if (resultSetValue instanceof LocalTime localTime) {
			if (LocalTime.class.isAssignableFrom(targetClass))
				return Optional.of(localTime);

			if (OffsetTime.class.isAssignableFrom(targetClass)) {
				ZoneOffset off = statementContext.getTimeZone().getRules().getOffset(Instant.EPOCH);
				return Optional.of(localTime.atOffset(off));
			}

			if (java.sql.Time.class.isAssignableFrom(targetClass))
				return Optional.of(java.sql.Time.valueOf(localTime));
		}

		if (resultSetValue instanceof OffsetTime offsetTime) {
			if (OffsetTime.class.isAssignableFrom(targetClass))
				return Optional.of(offsetTime);
			if (LocalTime.class.isAssignableFrom(targetClass))
				return Optional.of(offsetTime.toLocalTime());
			if (java.sql.Time.class.isAssignableFrom(targetClass))
				return Optional.of(java.sql.Time.valueOf(offsetTime.toLocalTime()));
		}

		if (resultSetValue instanceof String string) {
			if (BigDecimal.class.isAssignableFrom(targetClass)) {
				try {
					return Optional.of(new BigDecimal(string));
				} catch (NumberFormatException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to BigDecimal", resultSetValue), e);
				}
			}

			if (BigInteger.class.isAssignableFrom(targetClass)) {
				try {
					return Optional.of(exactBigInteger(new BigDecimal(string)));
				} catch (NumberFormatException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to BigInteger", resultSetValue), e);
				}
			}

			if (LocalDate.class.isAssignableFrom(targetClass)) {
				try {
					return Optional.of(localDateFromString(string));
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to LocalDate", resultSetValue), e);
				}
			}

			if (LocalTime.class.isAssignableFrom(targetClass)) {
				try {
					return Optional.of(localTimeFromString(string));
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to LocalTime", resultSetValue), e);
				}
			}

			if (LocalDateTime.class.isAssignableFrom(targetClass)) {
				try {
					return Optional.of(localDateTimeFromString(string));
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to LocalDateTime", resultSetValue), e);
				}
			}

			if (Instant.class.isAssignableFrom(targetClass)) {
				try {
					return Optional.of(instantFromString(string, statementContext));
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to Instant", resultSetValue), e);
				}
			}

			if (OffsetDateTime.class.isAssignableFrom(targetClass)) {
				try {
					return Optional.of(offsetDateTimeFromString(string, statementContext));
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to OffsetDateTime", resultSetValue), e);
				}
			}

			if (ZonedDateTime.class.isAssignableFrom(targetClass)) {
				try {
					return Optional.of(zonedDateTimeFromString(string, statementContext));
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to ZonedDateTime", resultSetValue), e);
				}
			}
		}

		if (UUID.class.isAssignableFrom(targetClass)) {
			return Optional.ofNullable(uuidFromValue(resultSetValue));
		} else if (ZoneId.class.isAssignableFrom(targetClass)) {
			try {
				return Optional.ofNullable(ZoneId.of(resultSetValue.toString()));
			} catch (DateTimeException e) {
				throw new DatabaseException(format("Unable to convert value '%s' to ZoneId", resultSetValue), e);
			}
		} else if (TimeZone.class.isAssignableFrom(targetClass)) {
			return Optional.of(timeZoneFromId(resultSetValue.toString()));
		} else if (Locale.class.isAssignableFrom(targetClass)) {
			return Optional.of(localeFromLanguageTag(resultSetValue.toString()));
		} else if (Currency.class.isAssignableFrom(targetClass)) {
			try {
				return Optional.ofNullable(Currency.getInstance(resultSetValue.toString()));
			} catch (IllegalArgumentException e) {
				throw new DatabaseException(format("Unable to convert value '%s' to Currency", resultSetValue), e);
			}
		} else if (targetClass.isEnum()) {
			return Optional.ofNullable(extractEnumValue(targetClass, resultSetValue));
		}

		if (Boolean.class.isAssignableFrom(targetClass)) {
			// Native boolean
			if (resultSetValue instanceof Boolean b)
				return Optional.of(b);

			// Numeric truthiness (0=false, nonzero=true)
			if (resultSetValue instanceof Number number)
				return Optional.of(decimalForNumericConversion(number).signum() != 0);

			throw new DatabaseException(format("Cannot map value '%s' (%s) to boolean", resultSetValue, resultSetValue.getClass().getName()));
		}

		if (Character.class.isAssignableFrom(targetClass)) {
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
				int code = exactInt(number);
				if (code >= Character.MIN_VALUE && code <= Character.MAX_VALUE)
					return Optional.of((char) code);

				throw new DatabaseException(format("Numeric value %d is outside valid char range", code));
			}

			throw new DatabaseException(format("Cannot map %s to %s",
					resultSetValue.getClass().getName(), propertyType.getSimpleName()));
		}

		return Optional.ofNullable(resultSetValue);
	}

	@NonNull
	private <T> Object convertSqlArrayResultSetValue(@NonNull StatementContext<T> statementContext,
																									 @NonNull Array sqlArray,
																									 @NonNull TargetType targetType) {
		requireNonNull(statementContext);
		requireNonNull(sqlArray);
		requireNonNull(targetType);

		try {
			return convertArrayResultSetValue(statementContext, extractSqlArrayResultSetValue(sqlArray), targetType);
		} catch (SQLException e) {
			DatabaseException wrapped = new DatabaseException("Unable to extract JDBC Array value", e);
			throw wrapped;
		}
	}

	@Nullable
	private static Object extractSqlArrayResultSetValue(@NonNull Array sqlArray) throws SQLException {
		requireNonNull(sqlArray);

		Throwable thrown = null;

		try {
			return sqlArray.getArray();
		} catch (SQLException e) {
			thrown = e;
			throw e;
		} finally {
			try {
				sqlArray.free();
			} catch (SQLException e) {
				if (thrown != null)
					thrown.addSuppressed(e);
				else
					throw e;
			}
		}
	}

	@NonNull
	private <T> Object convertArrayResultSetValue(@NonNull StatementContext<T> statementContext,
																								@NonNull Object arrayValue,
																								@NonNull TargetType targetType) {
		requireNonNull(statementContext);
		requireNonNull(arrayValue);
		requireNonNull(targetType);

		Class<?> arrayClass = arrayValue.getClass();
		if (!arrayClass.isArray())
			throw new DatabaseException(format("Expected JDBC Array value to expose a Java array but encountered %s",
					arrayClass.getName()));

		int length = java.lang.reflect.Array.getLength(arrayValue);

		if (targetType.isList()) {
			TargetType elementType = targetType.getListElementType().orElse(TargetType.of(Object.class));
			List<Object> values = new ArrayList<>(length);

			for (int i = 0; i < length; ++i)
				values.add(convertResultSetArrayElement(statementContext, java.lang.reflect.Array.get(arrayValue, i), elementType, i));

			return values;
		}

		if (targetType.isSet()) {
			TargetType elementType = targetType.getSetElementType().orElse(TargetType.of(Object.class));
			Set<Object> values = new LinkedHashSet<>(length);

			for (int i = 0; i < length; ++i)
				values.add(convertResultSetArrayElement(statementContext, java.lang.reflect.Array.get(arrayValue, i), elementType, i));

			return values;
		}

		TargetType componentType = targetType.getArrayComponentType().orElse(TargetType.of(Object.class));
		Class<?> componentClass = componentType.getRawClass();
		Object convertedArray = java.lang.reflect.Array.newInstance(componentClass, length);

		for (int i = 0; i < length; ++i) {
			Object convertedElement = convertResultSetArrayElement(statementContext, java.lang.reflect.Array.get(arrayValue, i), componentType, i);

			if (convertedElement == null && componentClass.isPrimitive())
				throw new DatabaseException(format("SQL ARRAY element %s is NULL but target array component type is primitive (%s)",
						i, componentClass.getSimpleName()));

			java.lang.reflect.Array.set(convertedArray, i, convertedElement);
		}

		return convertedArray;
	}

	@Nullable
	private <T> Object convertResultSetArrayElement(@NonNull StatementContext<T> statementContext,
																									@Nullable Object value,
																									@NonNull TargetType targetType,
																									int elementIndex) {
		requireNonNull(statementContext);
		requireNonNull(targetType);

		if (value == null)
			return null;

		Class<?> targetClass = boxedClass(targetType.getRawClass());

		if (targetClass == Object.class || targetClass.isInstance(value))
			return value;

		Object converted = convertResultSetValueToTargetType(statementContext, value, targetType).orElse(null);

		if (converted != null && !targetClass.isInstance(converted)) {
			throw new DatabaseException(format("SQL ARRAY element %s of type %s cannot be mapped to %s",
					elementIndex, value.getClass().getName(), targetClass.getName()));
		}

		return converted;
	}

	private static byte exactByte(@NonNull Number number) {
		requireNonNull(number);

		if (number instanceof Byte value)
			return value;

		try {
			return decimalForNumericConversion(number).byteValueExact();
		} catch (ArithmeticException e) {
			throw new DatabaseException(format("Numeric value '%s' cannot be represented exactly as byte", number), e);
		}
	}

	private static short exactShort(@NonNull Number number) {
		requireNonNull(number);

		if (number instanceof Byte || number instanceof Short)
			return number.shortValue();

		try {
			return decimalForNumericConversion(number).shortValueExact();
		} catch (ArithmeticException e) {
			throw new DatabaseException(format("Numeric value '%s' cannot be represented exactly as short", number), e);
		}
	}

	private static int exactInt(@NonNull Number number) {
		requireNonNull(number);

		if (number instanceof Byte || number instanceof Short || number instanceof Integer)
			return number.intValue();

		try {
			return decimalForNumericConversion(number).intValueExact();
		} catch (ArithmeticException e) {
			throw new DatabaseException(format("Numeric value '%s' cannot be represented exactly as int", number), e);
		}
	}

	private static long exactLong(@NonNull Number number) {
		requireNonNull(number);

		if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long)
			return number.longValue();

		try {
			return decimalForNumericConversion(number).longValueExact();
		} catch (ArithmeticException e) {
			throw new DatabaseException(format("Numeric value '%s' cannot be represented exactly as long", number), e);
		}
	}

	@NonNull
	private static BigInteger exactBigInteger(@NonNull BigDecimal bigDecimal) {
		requireNonNull(bigDecimal);

		try {
			return bigDecimal.toBigIntegerExact();
		} catch (ArithmeticException e) {
			throw new DatabaseException(format("Numeric value '%s' cannot be represented exactly as BigInteger", bigDecimal), e);
		}
	}

	private static float finiteFloat(@NonNull Number number) {
		requireNonNull(number);

		float value = number.floatValue();

		if (!Float.isFinite(value))
			throw new DatabaseException(format("Numeric value '%s' cannot be represented as finite float", number));

		return value;
	}

	private static double finiteDouble(@NonNull Number number) {
		requireNonNull(number);

		double value = number.doubleValue();

		if (!Double.isFinite(value))
			throw new DatabaseException(format("Numeric value '%s' cannot be represented as finite double", number));

		return value;
	}

	@NonNull
	private static BigDecimal decimalForNumericConversion(@NonNull Number number) {
		requireNonNull(number);

		try {
			if (number instanceof BigDecimal bigDecimal)
				return bigDecimal;
			if (number instanceof BigInteger bigInteger)
				return new BigDecimal(bigInteger);
			if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long)
				return BigDecimal.valueOf(number.longValue());
			if (number instanceof Float || number instanceof Double) {
				double value = number.doubleValue();

				if (!Double.isFinite(value))
					throw new DatabaseException(format("Numeric value '%s' is not finite", number));

				return BigDecimal.valueOf(value);
			}

			return new BigDecimal(number.toString());
		} catch (NumberFormatException e) {
			throw new DatabaseException(format("Unable to interpret numeric value '%s'", number), e);
		}
	}

	@NonNull
	private static LocalDate localDateFromString(@NonNull String value) {
		requireNonNull(value);
		String trimmed = value.trim();

		try {
			return LocalDate.parse(trimmed);
		} catch (DateTimeParseException ignored) {
			return java.sql.Date.valueOf(trimmed).toLocalDate();
		}
	}

	@NonNull
	private static LocalTime localTimeFromString(@NonNull String value) {
		requireNonNull(value);
		String trimmed = value.trim();

		try {
			return LocalTime.parse(trimmed);
		} catch (DateTimeParseException ignored) {
			return java.sql.Time.valueOf(trimmed).toLocalTime();
		}
	}

	@NonNull
	private static LocalDateTime localDateTimeFromString(@NonNull String value) {
		requireNonNull(value);
		String trimmed = value.trim();

		try {
			return LocalDateTime.parse(trimmed);
		} catch (DateTimeParseException ignored) {
		}

		if (trimmed.indexOf(' ') >= 0) {
			try {
				return LocalDateTime.parse(trimmed.replace(' ', 'T'));
			} catch (DateTimeParseException ignored) {
			}
		}

		return Timestamp.valueOf(trimmed).toLocalDateTime();
	}

	@NonNull
	private static Instant instantFromString(@NonNull String value,
																					 @NonNull StatementContext<?> statementContext) {
		requireNonNull(value);
		requireNonNull(statementContext);
		String trimmed = value.trim();

		try {
			return Instant.parse(trimmed);
		} catch (DateTimeParseException ignored) {
		}

		try {
			return OffsetDateTime.parse(trimmed).toInstant();
		} catch (DateTimeParseException ignored) {
		}

		return localDateTimeFromString(trimmed).atZone(statementContext.getTimeZone()).toInstant();
	}

	@NonNull
	private static OffsetDateTime offsetDateTimeFromString(@NonNull String value,
																												 @NonNull StatementContext<?> statementContext) {
		requireNonNull(value);
		requireNonNull(statementContext);
		String trimmed = value.trim();

		try {
			return OffsetDateTime.parse(trimmed);
		} catch (DateTimeParseException ignored) {
		}

		try {
			return Instant.parse(trimmed).atZone(statementContext.getTimeZone()).toOffsetDateTime();
		} catch (DateTimeParseException ignored) {
		}

		LocalDateTime localDateTime = localDateTimeFromString(trimmed);
		ZoneOffset offset = statementContext.getTimeZone().getRules().getOffset(localDateTime.atZone(statementContext.getTimeZone()).toInstant());
		return localDateTime.atOffset(offset);
	}

	@NonNull
	private static ZonedDateTime zonedDateTimeFromString(@NonNull String value,
																										 @NonNull StatementContext<?> statementContext) {
		requireNonNull(value);
		requireNonNull(statementContext);
		String trimmed = value.trim();

		try {
			return ZonedDateTime.parse(trimmed);
		} catch (DateTimeParseException ignored) {
		}

		try {
			return OffsetDateTime.parse(trimmed).atZoneSameInstant(statementContext.getTimeZone());
		} catch (DateTimeParseException ignored) {
		}

		try {
			return Instant.parse(trimmed).atZone(statementContext.getTimeZone());
		} catch (DateTimeParseException ignored) {
		}

		return localDateTimeFromString(trimmed).atZone(statementContext.getTimeZone());
	}

	@Nullable
	private static UUID uuidFromValue(@Nullable Object value) {
		if (value == null)
			return null;
		if (value instanceof UUID uuid)
			return uuid;
		if (value instanceof byte[] bytes)
			return uuidFromBytes(bytes);

		try {
			return UUID.fromString(value.toString());
		} catch (IllegalArgumentException e) {
			throw new DatabaseException(format("Unable to convert value '%s' to UUID", value), e);
		}
	}

	@NonNull
	private static UUID uuidFromBytes(byte @NonNull [] bytes) {
		requireNonNull(bytes);

		if (bytes.length != 16)
			throw new DatabaseException(format("Unable to convert byte[] of length %s to UUID", bytes.length));

		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		return new UUID(byteBuffer.getLong(), byteBuffer.getLong());
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
	protected Set<@NonNull String> databaseColumnNamesForPropertyName(@NonNull String propertyName) {
		requireNonNull(propertyName);

		Set<String> normalizedPropertyNames = new HashSet<>(2);

		// Converts camelCase to camel_case
		String replacement = "$1_$2";

		String normalizedPropertyName =
				CAMEL_CASE_PATTERN.matcher(propertyName).replaceAll(replacement).toLowerCase(getNormalizationLocale());
		normalizedPropertyNames.add(normalizedPropertyName);

		// Converts address1 to address_1
		String normalizedNumberPropertyName = LETTER_FOLLOWED_BY_NUMBER_PATTERN.matcher(normalizedPropertyName).replaceAll(replacement);
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

	@ThreadSafe
	protected static final class WritableProperty {
		@NonNull
		private final String name;
		@NonNull
		private final Method writeMethod;

		private WritableProperty(@NonNull String name,
														 @NonNull Method writeMethod) {
			this.name = requireNonNull(name);
			this.writeMethod = requireNonNull(writeMethod);
		}

		@NonNull
		protected String getName() {
			return this.name;
		}

		@NonNull
		protected Method getWriteMethod() {
			return this.writeMethod;
		}
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

		// ==== Targeted readers =====================================================

		public static Instant asInstant(ResultSet rs, int col, StatementContext<?> ctx, ResultSetMetaData resultSetMetaData) throws SQLException {
			boolean withTz = ctx.getDatabaseDialect().isTimestampWithTimeZone(resultSetMetaData, col);
			return asInstant(rs, col, ctx, withTz);
		}

		public static Instant asInstant(ResultSet rs, int col, StatementContext<?> ctx, boolean withTz) throws SQLException {
			// Modern fast-path
			if (withTz) {
				OffsetDateTime odt = tryGet(rs, col, OffsetDateTime.class);
				if (odt != null) return odt.toInstant();
			} else {
				LocalDateTime ldt = tryGet(rs, col, LocalDateTime.class);
				if (ldt != null) return ldt.atZone(ctx.getTimeZone()).toInstant();
			}

			// Tolerant fallbacks
			Object raw = rs.getObject(col);
			if (raw == null) return null;

			if (raw instanceof Timestamp ts) {
				if (withTz)
					return ts.toInstant();
				// DO NOT use ts.toInstant() for WITHOUT TZ; interpret using DB zone
				LocalDateTime ldt = ts.toLocalDateTime();
				return ldt.atZone(ctx.getTimeZone()).toInstant();
			}
			if (raw instanceof OffsetDateTime odt) return odt.toInstant();
			if (raw instanceof LocalDateTime ldt) return ldt.atZone(ctx.getTimeZone()).toInstant();
			if (raw instanceof String s) {
				// ISO-8601 string literal? Try parse to OffsetDateTime / LocalDateTime.
				try {
					return OffsetDateTime.parse(s).toInstant();
				} catch (DateTimeParseException ignore) {
				}
				try {
					return LocalDateTime.parse(s).atZone(ctx.getTimeZone()).toInstant();
				} catch (DateTimeParseException ignore) {
				}
			}

			// Last resort: try classic getter
			Timestamp ts = rs.getTimestamp(col);
			if (ts != null) {
				if (withTz)
					return ts.toInstant();
				LocalDateTime ldt = ts.toLocalDateTime();
				return ldt.atZone(ctx.getTimeZone()).toInstant();
			}
			return null;
		}

		public static OffsetDateTime asOffsetDateTime(ResultSet rs, int col, StatementContext<?> ctx, ResultSetMetaData resultSetMetaData) throws SQLException {
			boolean withTz = ctx.getDatabaseDialect().isTimestampWithTimeZone(resultSetMetaData, col);
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
			boolean withTz = ctx.getDatabaseDialect().isTimestampWithTimeZone(resultSetMetaData, col);
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
		Number value = getNullableNumber(resultSet, columnIndex, Integer.class);
		return value == null ? null : exactInt(value);
	}

	@Nullable
	private Long getNullableLong(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		Number value = getNullableNumber(resultSet, columnIndex, Long.class);
		return value == null ? null : exactLong(value);
	}

	@Nullable
	private Short getNullableShort(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		Number value = getNullableNumber(resultSet, columnIndex, Short.class);
		return value == null ? null : exactShort(value);
	}

	@Nullable
	private Byte getNullableByte(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		Number value = getNullableNumber(resultSet, columnIndex, Byte.class);
		return value == null ? null : exactByte(value);
	}

	@Nullable
	private Float getNullableFloat(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		Number value = getNullableNumber(resultSet, columnIndex, Float.class);
		return value == null ? null : finiteFloat(value);
	}

	@Nullable
	private Double getNullableDouble(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		Number value = getNullableNumber(resultSet, columnIndex, Double.class);
		return value == null ? null : finiteDouble(value);
	}

	@Nullable
	private Boolean getNullableBoolean(@NonNull ResultSet resultSet, int columnIndex) throws SQLException {
		Object value = resultSet.getObject(columnIndex);

		if (value == null)
			return null;
		if (value instanceof Boolean b)
			return b;
		if (value instanceof Number number)
			return decimalForNumericConversion(number).signum() != 0;

		boolean v = resultSet.getBoolean(columnIndex);
		return resultSet.wasNull() ? null : v;
	}

	@Nullable
	private Number getNullableNumber(@NonNull ResultSet resultSet,
																	 int columnIndex,
																	 @NonNull Class<?> targetType) throws SQLException {
		requireNonNull(resultSet);
		requireNonNull(targetType);

		Object value = resultSet.getObject(columnIndex);

		if (value == null)
			return null;
		if (value instanceof Number number)
			return number;

		throw new DatabaseException(format("Cannot map value '%s' (%s) to %s",
				value, value.getClass().getName(), targetType.getSimpleName()));
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
		final int mappedColumnCount;             // selected columns that map to at least one property/component
		final @NonNull List<ColumnBinding> bindings; // may contain multiple entries per column (fan-out)

		RowPlan(boolean isRecord,
						int columnCount,
						boolean hasFanOut,
						int mappedColumnCount,
						@NonNull List<ColumnBinding> bindings) {
			this.isRecord = isRecord;
			this.columnCount = columnCount;
			this.hasFanOut = hasFanOut;
			this.mappedColumnCount = mappedColumnCount;
			this.bindings = requireNonNull(bindings);
		}
	}

	protected static final class SchemaSignatureMemo {
		@NonNull
		private final WeakReference<ResultSet> resultSetReference;
		@NonNull
		private final String schemaSignature;

		SchemaSignatureMemo(@NonNull ResultSet resultSet,
												@NonNull String schemaSignature) {
			this.resultSetReference = new WeakReference<>(requireNonNull(resultSet));
			this.schemaSignature = requireNonNull(schemaSignature);
		}

		boolean matches(@NonNull ResultSet resultSet) {
			requireNonNull(resultSet);
			return this.resultSetReference.get() == resultSet;
		}

		@NonNull
		String getSchemaSignature() {
			return this.schemaSignature;
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

		Map<String, RowPlan<?>> planCache = getRowPlanningCacheForResultClass(resultClass);
		RowPlan<?> cached = planCache.get(schemaSignature);
		if (cached != null) return (RowPlan<T>) cached;

		int cols = resultSetMetaData.getColumnCount();

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
			for (WritableProperty writableProperty : determineWritableProperties(resultClass))
				setterByProp.put(writableProperty.getName(), lookup.unreflect(writableProperty.getWriteMethod()));
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
		DatabaseDialect databaseDialect = ctx.getDatabaseDialect();
		for (int i = 1; i <= cols; i++) {
			int jdbcType = resultSetMetaData.getColumnType(i);
			boolean tsTz = databaseDialect.isTimestampWithTimeZone(resultSetMetaData, i);
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
				readers[i] = (r, c, col) -> {
					Object value = r.getObject(col);
					return value instanceof Array sqlArray ? extractSqlArrayResultSetValue(sqlArray) : value;
				};
			}
		}

		// For each column, bind ALL matching properties (fan-out)
		List<ColumnBinding> bindings = new ArrayList<>();
		boolean hasFanOut = false;
		int mappedColumnCount = 0;
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

			++mappedColumnCount;

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

					if (!customMappers.isEmpty()) {
						CustomMappingOutcome outcome = tryCustomColumnMappers(cctx, rrs, raw, targetTypeFinal, customMappers, colIndexFinal, labelFinal, ip);
						if (outcome.isApplied())
							return outcome.getValue();
					}

					if (rawTargetClassBoxed.isInstance(raw))
						return raw;

					return convertResultSetValueToTargetType(cctx, raw, targetTypeFinal)
							.orElse(raw);
				};

				bindings.add(new ColumnBinding(i, recordArgIndex, setterMH, reader, converter,
						targetIsPrimitive, rawTargetClassBoxed, propertyNameFinal));
			}
		}

		RowPlan<T> plan = new RowPlan<>(isRecord, cols, hasFanOut, mappedColumnCount, bindings);
		planCache.putIfAbsent(schemaSignature, plan);
		return plan;
	}

	@NonNull
	protected List<@NonNull CustomColumnMapper> getCustomColumnMappers() {
		return this.customColumnMappers;
	}

	@NonNull
	protected Boolean getPlanCachingEnabled() {
		return this.planCachingEnabled;
	}

	@NonNull
	protected Map<@NonNull PreferredColumnMapperKey, @NonNull CustomColumnMapper> getPreferredColumnMapperCacheForSourceClass(@NonNull Class<?> sourceClass) {
		requireNonNull(sourceClass);
		return this.preferredColumnMapperBySourceClass.get(sourceClass);
	}

	@Nullable
	private PreferredColumnMapperKey preferredColumnMapperKeyFor(@NonNull TargetType targetType) {
		requireNonNull(targetType);

		if (!targetType.getTypeArguments().isEmpty())
			return null;

		return PreferredColumnMapperKey.of(targetType);
	}

	@ThreadSafe
	protected static final class PreferredColumnMapperKey {
		@NonNull
		private final WeakReference<Class<?>> rawClassRef;
		private final int rawClassHash;

		private PreferredColumnMapperKey(@NonNull Class<?> rawClass) {
			requireNonNull(rawClass);
			this.rawClassRef = new WeakReference<>(rawClass);
			this.rawClassHash = System.identityHashCode(rawClass);
		}

		@NonNull
		static PreferredColumnMapperKey of(@NonNull TargetType targetType) {
			requireNonNull(targetType);
			return new PreferredColumnMapperKey(targetType.getRawClass());
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			PreferredColumnMapperKey that = (PreferredColumnMapperKey) o;
			Class<?> lhs = rawClassRef.get();
			Class<?> rhs = that.rawClassRef.get();
			return lhs != null && lhs == rhs;
		}

		@Override
		public int hashCode() {
			return rawClassHash;
		}
	}

	@NonNull
	protected Map<@NonNull String, @NonNull RowPlan<?>> getRowPlanningCacheForResultClass(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);
		return this.rowPlanningCacheByResultClass.get(resultClass);
	}

	@NonNull
	protected Map<@NonNull ResultSetMetaData, @NonNull String> getSchemaSignatureByResultSetMetaData() {
		return this.schemaSignatureByResultSetMetaData;
	}

	@NonNull
	protected ReentrantLock getSchemaSignatureByResultSetMetaDataLock() {
		return this.schemaSignatureByResultSetMetaDataLock;
	}
}
