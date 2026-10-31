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
import java.time.Year;
import java.time.YearMonth;
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
	private static final ClassValue<TargetType> RAW_TARGET_TYPES = new ClassValue<>() {
		@Override protected TargetType computeValue(Class<?> type) { return TargetType.of(type); }
	};
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
	private final @Nullable Long maxLobBytes;
	private final @Nullable Long maxLobCharacters;
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
	// Class-only reflection stays reusable even when schema-plan retention is disabled.
	private final ClassValue<RowTypeMetadata> rowTypeMetadata = new ClassValue<>() {
		@Override protected RowTypeMetadata computeValue(Class<?> type) {
			return computeRowTypeMetadata(type);
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
	private final Map<ResultSetMetaData, Map<DatabaseDialect, String>> schemaSignatureByResultSetMetaData;
	@NonNull
	private final ReentrantLock schemaSignatureByResultSetMetaDataLock;
	@NonNull
	private final ThreadLocal<SchemaSignatureMemo> schemaSignatureMemo;
	// The retained value contains only empty resource bookkeeping and weak cursor metadata.
	private final ThreadLocal<MappingResources> mappingResources = new ThreadLocal<>();

	private MappingResources acquireMappingResources() {
		MappingResources resources = this.mappingResources.get();
		if (resources == null) {
			resources = new MappingResources(this.maxLobBytes, this.maxLobCharacters);
			this.mappingResources.set(resources);
		}
		return resources.acquire();
	}

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

		for (CustomColumnMapper mapper : mappers) {
			CustomColumnMapper.MappingResult mappingResult;

			try {
				mappingResult = mapper.map(statementContext, resultSet, resultSetValue, targetType, columnIndex, columnLabel, instanceProvider);
			} catch (SQLException e) {
				throw DatabaseException.wrapSafely(DatabaseException.messageSafely(e, "Unable to map custom JDBC column"),
						e, DatabaseDialect.forExceptionCause(e));
			}

			if (mappingApplied(mappingResult)) {
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
			Set<String> baseNames = new HashSet<>();
			baseNames.add(propertyName);
			baseNames.addAll(columnLabelAliasesByPropertyName.getOrDefault(propertyName, Set.of()));

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
			Set<String> labels = new HashSet<>(databaseColumnNamesForPropertyName(propertyName));

			for (String alias : columnLabelAliasesByPropertyName.getOrDefault(propertyName, Set.of()))
				labels.addAll(databaseColumnNamesForPropertyName(alias));

			labelsByComponentName.put(propertyName, Collections.unmodifiableSet(labels));
		}

		return Collections.unmodifiableMap(labelsByComponentName);
	}

	DefaultResultSetMapper(@NonNull Builder builder) {
		requireNonNull(builder);

		this.normalizationLocale = requireNonNull(builder.normalizationLocale);
		this.customColumnMappers = List.copyOf(requireNonNull(builder.customColumnMappers));
		this.planCachingEnabled = requireNonNull(builder.planCachingEnabled);

		this.planCacheCapacity = builder.planCacheCapacity;
		this.maxLobBytes = builder.maxLobBytes;
		this.maxLobCharacters = builder.maxLobCharacters;
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
		// row assembly - Map targets retain native/dialect-normalized values.
		if (resultSetRowType == Map.class || resultSetRowType == LinkedHashMap.class) {
			@SuppressWarnings("unchecked")
			T row = (T) mapResultSetToMap(statementContext, resultSet, resultSetMetaData);
			return Optional.of(row);
		}

		if (UNSUPPORTED_MAP_ROW_TYPES.contains(resultSetRowType))
			throw new DatabaseException(format(
					"%s is not a supported Map row type. Use Map.class or LinkedHashMap.class to fetch rows as maps.",
					resultSetRowType.getName()));

		return mapWithRowPlanning(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData);
	}

	/**
	 * Well-known JDK map types that are rejected with a clear error instead of falling confusingly into
	 * the JavaBean mapping path. User classes that happen to implement {@link Map} are deliberately NOT
	 * rejected - they retain their existing bean-path behavior.
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
	 * @since 4.5.0
	 */
	@NonNull
	protected <T> Map<String, Object> mapResultSetToMap(@NonNull StatementContext<T> statementContext,
																											@NonNull ResultSet resultSet,
																											@NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetMetaData);

		MappingResources resources = acquireMappingResources();
		Throwable mappingFailure = null;
		try {
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

				Object columnValue = extractColumnValue(resultSetMetaData, statementContext, resultSet, i, resources).orElse(null);

				// Match record/bean mapping: unwrap driver-specific wrappers (e.g. PostgreSQL PGobject for
				// JSONB/vector columns becomes its String form) so map rows never carry raw driver objects
				if (columnValue != null)
					columnValue = resources.nativeValue(columnValue, statementContext.getDatabaseDialect());

				row.put(label, columnValue);
			}

			return row;
		} catch (Throwable failure) {
			mappingFailure = failure;
			throw failure;
		} finally {
			resources.close(mappingFailure);
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

		MappingResources resources = acquireMappingResources();
		Throwable mappingFailure = null;
		try {
			// Use "standard type" fast path
			StandardTypeResult<T> standardTypeResult = mapResultSetToStandardType(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData, resources);
			if (standardTypeResult.isStandardType()) return standardTypeResult.getValue();

			// Otherwise, use a per-schema row plan
			String schemaSignature = getPlanCachingEnabled() ? schemaSignatureFor(statementContext, resultSet, resultSetMetaData) : "";
			RowPlan<T> plan = buildPlan(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData, schemaSignature);
			RowReadScope reads = plan.hasFanOut ? new RowReadScope(resultSet, statementContext, plan, resources) : null;

			if (plan.isRecord) {
				if (plan.mappedColumnCount == 0) {
					if (isSingleStructColumn(resultSetMetaData))
						return Optional.ofNullable(mapStructColumnToRecord(statementContext, resultSet, resultSetRowType, instanceProvider, resultSetMetaData, resources));

					throw noRecordColumnMappingsException(resultSetRowType, resultSetMetaData);
				}

				RecordComponent[] rc = determineRecordComponents(resultSetRowType);
				Object[] args = new Object[rc.length];

				for (ColumnBinding b : plan.bindings) {
					if (b.recordArgIndexOrMinusOne < 0) continue; // bean binding or SKIP
					Object raw = reads == null ? readColumn(b.reader, resultSet, statementContext, b.columnIndex, resources) : reads.read(b);
					Object val = b.converter.convert(raw, resultSet, statementContext, instanceProvider, resources);

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

				// Both cache modes construct through the configured InstanceProvider.
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
					Object raw = reads == null ? readColumn(b.reader, resultSet, statementContext, b.columnIndex, resources) : reads.read(b);
					Object val = b.converter.convert(raw, resultSet, statementContext, instanceProvider, resources);

					if (val == null) {
						if (b.targetIsPrimitive) {
							throw new DatabaseException(format(
									"Column for bean property '%s' of %s is NULL but the property is primitive (%s). "
											+ "Use a non-primitive type or COALESCE/CAST in SQL.",
									b.propertyName, resultSetRowType.getSimpleName(), b.targetRawClass.getSimpleName()));
						}
						// A selected SQL NULL invokes the setter with null.
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
			mappingFailure = e;
			throw e;
		} catch (SQLException e) {
			mappingFailure = e;
			throw e;
		} catch (Error error) {
			mappingFailure = error;
			throw error;
		} catch (Throwable t) {
			mappingFailure = t;
			throw DatabaseException.wrapSafely(format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), resultSetRowType),
					t, DatabaseDialect.forExceptionCause(t));
		} finally {
			try {
				resources.close(mappingFailure);
			} catch (DatabaseException failure) {
				throw failure;
			} catch (RuntimeException failure) {
				throw DatabaseException.wrapSafely(format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), resultSetRowType),
						failure, DatabaseDialect.forExceptionCause(failure));
			}
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

		if (memo != null && memo.matches(resultSet, statementContext.getDatabaseDialect()))
			return memo.getSchemaSignature();

		String schemaSignature = schemaSignatureFor(statementContext, resultSetMetaData);
		getSchemaSignatureMemo().set(new SchemaSignatureMemo(resultSet, statementContext.getDatabaseDialect(), schemaSignature));
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
			Map<DatabaseDialect, String> signatures = getSchemaSignatureByResultSetMetaData().get(resultSetMetaData);
			String signature = signatures == null ? null : signatures.get(statementContext.getDatabaseDialect());
			if (signature != null)
				return signature;
		} finally {
			lock.unlock();
		}

		String signature = buildSchemaSignature(statementContext, resultSetMetaData);

		lock.lock();

		try {
			Map<DatabaseDialect, String> signatures = getSchemaSignatureByResultSetMetaData().computeIfAbsent(resultSetMetaData, ignored -> new WeakHashMap<>());
			String existingSignature = signatures.get(statementContext.getDatabaseDialect());
			if (existingSignature != null)
				return existingSignature;

			signatures.put(statementContext.getDatabaseDialect(), signature);
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
			sig.append('|').append(labelNorm.length()).append(':').append(labelNorm)
					.append(':').append(jdbcType)
					.append(':').append(tsTz ? 'T' : 't')
					.append(':').append(timeTz ? 'Z' : 'z')
					.append(':').append(typeName.length()).append(':').append(typeName);
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
	 * This does not assemble a user-defined JavaBean; the row plan handles
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
																																 @NonNull ResultSetMetaData resultSetMetaData, @NonNull MappingResources resources) throws Exception {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultClass);
		requireNonNull(instanceProvider);
		requireNonNull(resultSetMetaData);

		Object value = null;
		boolean standardType = true;
		Object customRaw = null;
		boolean customRawRead = false;

		// See if custom mappers apply, e.g. user mapping a SQL ARRAY to a List<UUID>
		TargetType targetType = RAW_TARGET_TYPES.get(resultClass);
		List<CustomColumnMapper> mappers = customColumnMappersFor(targetType);

		if (!mappers.isEmpty()) {
			int columnCount = resultSetMetaData.getColumnCount();
			Boolean rowColumnsClaimed = null;

			if (columnCount != 1) {
				rowColumnsClaimed = resultSetHasMappedRowColumns(resultClass, resultSetMetaData);
				if (rowColumnsClaimed)
					return new StandardTypeResult<>(null, false);
			}

			Object resultSetValue = extractColumnValue(resultSetMetaData, statementContext, resultSet, 1, resources).orElse(null);
			customRaw = resultSetValue;
			customRawRead = true;
			resources.borrow(resultSetValue);

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
					resources.borrow(cast);
					return new StandardTypeResult<>(cast, true);
				}

				// If no mapper applied (Fallback), we proceed to built-ins below.
			}
		}

		// Target-aware JDBC readers preserve driver getter behavior. All detached values then
		// pass through the same built-in conversion engine used by bean/record bindings.
		Class<?> boxed = boxedClass(resultClass);
		if (boxed == Byte.class || boxed == Short.class || boxed == Integer.class || boxed == Long.class
				|| boxed == Float.class || boxed == Double.class || boxed == Number.class) {
			value = getNullableNumber(resultSet, 1, resultClass);
		} else if (boxed == Boolean.class) {
			value = getNullableBoolean(resultSet, 1);
		} else if (boxed == Character.class) {
			value = resultSet.getString(1);
		} else if (resultClass == String.class || resultClass == byte[].class) {
			value = readDetachedColumn(resultSet, resultSetMetaData, 1, resultClass, customRawRead, customRaw, resources);
		} else if (resultClass.isArray() || resultClass == List.class || resultClass == Set.class
				|| resultClass.isEnum() || resultClass == UUID.class || resultClass == Year.class || resultClass == Object.class
				|| resultClass == java.sql.Blob.class || resultClass == java.sql.Clob.class || resultClass == java.sql.NClob.class) {
			value = customRawRead && (resultClass.isArray() || resultClass == List.class || resultClass == Set.class)
					? customRaw : resultSet.getObject(1);
		} else if (resultClass == BigDecimal.class || resultClass == BigInteger.class) {
			value = resultSet.getBigDecimal(1);
		} else if (resultClass == Date.class || resultClass == Instant.class || resultClass == ZonedDateTime.class) {
			value = TemporalReaders.asInstant(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == LocalDate.class) {
			value = TemporalReaders.asLocalDate(resultSet, 1);
		} else if (resultClass == LocalTime.class) {
			value = TemporalReaders.asLocalTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == LocalDateTime.class) {
			value = TemporalReaders.asLocalDateTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == OffsetTime.class) {
			value = TemporalReaders.asOffsetTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == OffsetDateTime.class) {
			value = TemporalReaders.asOffsetDateTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == java.sql.Timestamp.class) {
			value = statementContext.getDatabaseDialect().isTimestampWithTimeZone(resultSetMetaData, 1)
					? TemporalReaders.asInstant(resultSet, 1, statementContext, true)
					: TemporalReaders.asLocalDateTime(resultSet, 1, statementContext, false);
		} else if (resultClass == java.sql.Date.class) {
			value = statementContext.getDatabaseDialect().isTimestampWithTimeZone(resultSetMetaData, 1)
					? TemporalReaders.asInstant(resultSet, 1, statementContext, true)
					: TemporalReaders.asLocalDate(resultSet, 1);
		} else if (resultClass == ZoneId.class || resultClass == TimeZone.class || resultClass == Locale.class
				|| resultClass == Currency.class || resultClass == YearMonth.class) {
			value = resultSet.getString(1);
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

		if (standardType && resultClass != Object.class) {
			value = (resultClass.isArray() && resultClass != byte[].class) || resultClass == List.class || resultClass == Set.class
					? convertResultSetValueToTargetType(statementContext, value, targetType, resources)
					: convertDetachedValue(statementContext, value, targetType, resources);
			if (value != null && !boxed.isInstance(value))
				throw new DatabaseException("Cannot map " + value.getClass().getName() + " to " + resultClass.getName());
		}

		if (standardType && (resultClass == Object.class || resultClass == java.sql.Blob.class
				|| resultClass == java.sql.Clob.class || resultClass == java.sql.NClob.class)) resources.borrow(value);
		return new StandardTypeResult(value, standardType);
	}

	@Nullable
	private Object readDetachedColumn(ResultSet resultSet, ResultSetMetaData metadata, int column,
			Class<?> target, boolean rawRead, @Nullable Object raw, MappingResources resources) throws SQLException {
		int jdbcType = resources.scalarJdbcType(resultSet, metadata);
		if (MappingResources.requiresMaterialization(raw, target, jdbcType)) {
			if (!rawRead) raw = resultSet.getObject(column);
			return resources.materialize(raw, target, jdbcType, "column '" + metadata.getColumnLabel(column) + "'");
		}
		// Cross-type LOB conversions belong to the driver (e.g. DuckDB BLOB -> String).
		// Preserve JDBC getter semantics outside the matching typed-LOB detachment policy.
		return target == String.class ? resultSet.getString(column) : resultSet.getBytes(column);
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
	protected <T> Optional<Object> extractColumnValue(@NonNull ResultSetMetaData resultSetMetaData,
																										@NonNull StatementContext<T> statementContext,
																										@NonNull ResultSet resultSet,
																										int columnIndex) throws SQLException {
		MappingResources resources = acquireMappingResources();
		Throwable mappingFailure = null;
		try {
			return extractColumnValue(resultSetMetaData, statementContext, resultSet, columnIndex, resources);
		} catch (Throwable failure) {
			mappingFailure = failure;
			throw failure;
		} finally {
			resources.close(mappingFailure);
		}
	}

	private Optional<Object> extractColumnValue(ResultSetMetaData metadata, StatementContext<?> context,
			ResultSet resultSet, int column, MappingResources resources) throws SQLException {
		return Optional.ofNullable(readColumn(columnReaderFor(metadata, context, column), resultSet, context, column, resources));
	}

	private static Object readColumn(ColumnReader reader, ResultSet resultSet, StatementContext<?> context,
			int column, MappingResources resources) throws SQLException {
		Object raw = reader.read(resultSet, context, column);
		return raw instanceof Array array ? extractSqlArrayResultSetValue(array, resources) : raw;
	}

	private ColumnReader columnReaderFor(ResultSetMetaData metadata, StatementContext<?> context, int column) throws SQLException {
		int jdbcType = metadata.getColumnType(column);
		// An array type name can itself contain "TIMESTAMP WITH TIME ZONE".
		if (jdbcType == Types.ARRAY || jdbcType == Types.STRUCT)
			return (r, c, i) -> r.getObject(i);
		if (context.getDatabaseDialect().isTimestampWithTimeZone(metadata, column))
			return (r, c, i) -> TemporalReaders.asOffsetDateTime(r, i, c, true);
		if (jdbcType == Types.TIMESTAMP)
			return (r, c, i) -> TemporalReaders.asLocalDateTime(r, i, c, false);
		if (jdbcType == Types.DATE)
			return (r, c, i) -> TemporalReaders.asLocalDate(r, i);
		if (TemporalReaders.isTimeWithTimeZone(metadata, column))
			return (r, c, i) -> TemporalReaders.asOffsetTime(r, i, c, true);
		if (jdbcType == Types.TIME)
			return (r, c, i) -> TemporalReaders.asLocalTime(r, i);
		return (r, c, i) -> r.getObject(i);
	}

	@Nullable
	private Object convertColumn(@Nullable Object raw, TargetType target, List<CustomColumnMapper> mappers,
			ResultSet resultSet, StatementContext<?> context, InstanceProvider instances,
			MappingResources resources, int column, String label, int jdbcType) {
		if (raw == null) return null;
		if (!mappers.isEmpty()) {
			// Even a mapper returning fallback can retain the borrowed driver value.
			resources.borrow(raw);
			CustomMappingOutcome custom = tryCustomColumnMappers(context, resultSet, raw, target,
					mappers, column, label, instances);
			if (custom.isApplied()) {
				resources.borrow(custom.getValue());
				return custom.getValue();
			}
		}
		Object value = MappingResources.requiresMaterialization(raw, target.getRawClass(), jdbcType)
				? resources.materialize(raw, target.getRawClass(), jdbcType, "column '" + label + "'") : raw;
		return convertResultSetValueToTargetType(context, value, target, resources);
	}

	/** Convenience entry point for a detached value; row execution shares one resource scope. */
	@NonNull
	protected <T> Optional<Object> convertResultSetValueToTargetType(StatementContext<T> context,
			@Nullable Object value, TargetType target) {
		MappingResources resources = acquireMappingResources();
		Throwable mappingFailure = null;
		try {
			return Optional.ofNullable(convertResultSetValueToTargetType(context, value, target, resources));
		} catch (Throwable failure) {
			mappingFailure = failure;
			throw failure;
		} finally {
			try {
				resources.close(mappingFailure);
			} catch (SQLException failure) {
				throw DatabaseException.wrapSafely("Unable to release mapping resources", failure, DatabaseDialect.forExceptionCause(failure));
			}
		}
	}

	@Nullable
	private <T> Object convertResultSetValueToTargetType(@NonNull StatementContext<T> statementContext,
																																 @Nullable Object resultSetValue,
																																 @NonNull TargetType targetType, @NonNull MappingResources resources) {
		requireNonNull(statementContext);
		requireNonNull(targetType);

		if (resultSetValue == null)
			return null;

		// All built-in dialects preserve these exact base scalar values. Column execution has
		// already run custom hooks and validated JDBC-identified detached LOBs before this call.
		if (isIdentityScalar(resultSetValue, boxedClass(targetType.getRawClass())))
			return resultSetValue;

		if (MappingResources.requiresMaterialization(resultSetValue, targetType.getRawClass(), Types.OTHER))
			resultSetValue = resources.materialize(resultSetValue, targetType.getRawClass(), Types.OTHER, "target " + targetType.getRawClass().getTypeName());
		if (targetType.getRawClass() == Object.class || targetType.getRawClass().isInstance(resultSetValue))
			resources.borrow(resultSetValue);
		DatabaseDialect dialect = statementContext.getDatabaseDialect();
		boolean nativeTarget = targetType.getRawClass() == Object.class || Map.class.isAssignableFrom(targetType.getRawClass());
		Object beforeUnwrap = resultSetValue;
		resultSetValue = nativeTarget ? resources.nativeValue(resultSetValue, dialect)
				: dialect instanceof DuckDbDialect duck ? duck.unwrapForTypedMapping(resultSetValue, resources)
				: dialect.unwrapResultSetValue(resultSetValue);

		resources.copyArrayType(beforeUnwrap, resultSetValue);
		return convertDetachedValue(statementContext, resultSetValue, targetType, resources);
	}

	/** Pure built-in conversion also serves scalar JDBC readers without forcing dialect detection. */
	@Nullable
	private <T> Object convertDetachedValue(StatementContext<T> statementContext, @Nullable Object resultSetValue,
			TargetType targetType, MappingResources resources) {
		if (resultSetValue == null) return null;
		Class<?> targetClass = boxedClass(targetType.getRawClass());
		// Keep this shared identity path small enough to inline. Floating-point values still
		// undergo finite checks; containers still undergo recursive element conversion.
		if (isIdentityScalar(resultSetValue, targetClass)) return resultSetValue;
		return convertDetachedValueSlow(statementContext, resultSetValue, targetType, resources);
	}

	private static boolean isIdentityScalar(Object value, Class<?> targetClass) {
		return value.getClass() == targetClass && (targetClass == String.class
				|| targetClass == Integer.class || targetClass == Long.class || targetClass == Short.class
				|| targetClass == Byte.class || targetClass == Boolean.class || targetClass == Character.class
				|| targetClass == BigInteger.class || targetClass == BigDecimal.class || targetClass == byte[].class);
	}

	@Nullable
	private <T> Object convertDetachedValueSlow(StatementContext<T> statementContext, Object resultSetValue,
			TargetType targetType, MappingResources resources) {
		Class<?> propertyType = targetType.getRawClass();
		if (propertyType == byte[].class && resultSetValue instanceof byte[]) return resultSetValue;

		if (resultSetValue instanceof Array sqlArray && (targetType.isArray() || targetType.isList() || targetType.isSet()))
			return convertSqlArrayResultSetValue(statementContext, sqlArray, targetType, resources);

		if (resultSetValue.getClass().isArray() && (targetType.isArray() || targetType.isList() || targetType.isSet()))
			return convertArrayResultSetValue(statementContext, resultSetValue, targetType, resources);

		// STRUCT-shaped values: dialects that surface SQL STRUCT columns as insertion-ordered
		// attribute-name -> value maps (for example DuckDB - see DuckDbDialect#unwrapResultSetValue)
		// map directly to record targets, recursively converting each attribute to its component type.
		if (resultSetValue instanceof Map<?, ?> mapValue && propertyType.isRecord())
			return convertMapResultSetValueToRecord(statementContext, mapValue, propertyType, STRUCT_RECORD_INSTANCE_PROVIDER, resources);

		// Vector read-back: drivers surface vector columns (e.g. pgvector) as text like "[1.5,2.5]" -
		// unwrapResultSetValue above already converted PGobject to its String form. Parse into
		// float[]/double[] targets; non-vector-shaped strings fall through to normal handling.
		if (resultSetValue instanceof String string && (propertyType == float[].class || propertyType == double[].class)) {
			Object vector = vectorFromLiteral(string, propertyType == float[].class);
			if (vector != null)
				return vector;
		}

		// Normalize primitives to wrappers
		Class<?> targetClass = boxedClass(propertyType);

		// Already converted scalar values and native property values retain their representation.
		if (targetClass.isInstance(resultSetValue) && !(resultSetValue instanceof Number))
			return resultSetValue;

		// Numbers
		if (resultSetValue instanceof BigDecimal bigDecimal) {
			if (BigDecimal.class.isAssignableFrom(targetClass))
				return bigDecimal;
			if (BigInteger.class.isAssignableFrom(targetClass))
				return exactBigInteger(bigDecimal);
		}

		if (resultSetValue instanceof BigInteger bigInteger) {
			if (BigDecimal.class.isAssignableFrom(targetClass))
				return new BigDecimal(bigInteger);
			if (BigInteger.class.isAssignableFrom(targetClass))
				return bigInteger;
		}

		if (resultSetValue instanceof Number number) {
			if (Byte.class.isAssignableFrom(targetClass))
				return exactByte(number);
			if (Short.class.isAssignableFrom(targetClass))
				return exactShort(number);
			if (Integer.class.isAssignableFrom(targetClass))
				return exactInt(number);
			if (Long.class.isAssignableFrom(targetClass))
				return exactLong(number);
			if (Float.class.isAssignableFrom(targetClass))
				return finiteFloat(number);
			if (Double.class.isAssignableFrom(targetClass))
				return finiteDouble(number);
			if (BigDecimal.class.isAssignableFrom(targetClass)) {
				if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long)
					return BigDecimal.valueOf(number.longValue());
				return new BigDecimal(number.toString());
			}
			if (BigInteger.class.isAssignableFrom(targetClass)) {
				if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long)
					return BigInteger.valueOf(number.longValue());
				return exactBigInteger(decimalForNumericConversion(number));
			}
		}

		// Legacy java.sql.* coming from drivers
		if (resultSetValue instanceof java.sql.Timestamp timestamp) {
			if (java.sql.Date.class.isAssignableFrom(targetClass))
				return java.sql.Date.valueOf(timestamp.toLocalDateTime().toLocalDate());
			if (Date.class.isAssignableFrom(targetClass))
				return timestamp;
			if (Instant.class.isAssignableFrom(targetClass))
				return timestamp.toInstant();
			if (LocalDate.class.isAssignableFrom(targetClass))
				return timestamp.toLocalDateTime().toLocalDate();
			if (LocalDateTime.class.isAssignableFrom(targetClass))
				return timestamp.toLocalDateTime();
			if (OffsetDateTime.class.isAssignableFrom(targetClass))
				return timestamp.toInstant().atZone(statementContext.getTimeZone()).toOffsetDateTime();
			if (ZonedDateTime.class.isAssignableFrom(targetClass))
				return timestamp.toInstant().atZone(statementContext.getTimeZone());
		}

		if (resultSetValue instanceof java.sql.Date date)
			return convertDetachedValue(statementContext, date.toLocalDate(), targetType, resources);

		if (resultSetValue instanceof java.sql.Time time) {
			if (LocalTime.class.isAssignableFrom(targetClass))
				return time.toLocalTime();
		}

		// New java.time values (preferred)
		if (resultSetValue instanceof Instant instant) {
			if (Instant.class.isAssignableFrom(targetClass))
				return instant;
			if (java.sql.Timestamp.class.isAssignableFrom(targetClass))
				return Timestamp.from(instant);
			if (java.sql.Date.class.isAssignableFrom(targetClass))
				return java.sql.Date.valueOf(instant.atZone(statementContext.getTimeZone()).toLocalDate());
			if (Date.class.isAssignableFrom(targetClass))
				return Date.from(instant);
			if (LocalDateTime.class.isAssignableFrom(targetClass))
				return instant.atZone(statementContext.getTimeZone()).toLocalDateTime();
			if (LocalDate.class.isAssignableFrom(targetClass))
				return instant.atZone(statementContext.getTimeZone()).toLocalDate();
			if (OffsetDateTime.class.isAssignableFrom(targetClass))
				return instant.atZone(statementContext.getTimeZone()).toOffsetDateTime();
			if (ZonedDateTime.class.isAssignableFrom(targetClass))
				return instant.atZone(statementContext.getTimeZone());
		}

		if (resultSetValue instanceof LocalDateTime localDateTime) {
			if (LocalDateTime.class.isAssignableFrom(targetClass))
				return localDateTime;
			if (Instant.class.isAssignableFrom(targetClass))
				return localDateTime.atZone(statementContext.getTimeZone()).toInstant();
			if (java.sql.Timestamp.class.isAssignableFrom(targetClass))
				return Timestamp.valueOf(localDateTime);
			if (java.sql.Date.class.isAssignableFrom(targetClass))
				return java.sql.Date.valueOf(localDateTime.toLocalDate());
			if (Date.class.isAssignableFrom(targetClass))
				return Date.from(localDateTime.atZone(statementContext.getTimeZone()).toInstant());
			if (LocalDate.class.isAssignableFrom(targetClass))
				return localDateTime.toLocalDate();
			if (OffsetDateTime.class.isAssignableFrom(targetClass))
				return localDateTime.atZone(statementContext.getTimeZone()).toOffsetDateTime();
			if (ZonedDateTime.class.isAssignableFrom(targetClass))
				return localDateTime.atZone(statementContext.getTimeZone());
		}

		if (resultSetValue instanceof OffsetDateTime offsetDateTime) {
			if (OffsetDateTime.class.isAssignableFrom(targetClass))
				return offsetDateTime;
			if (Instant.class.isAssignableFrom(targetClass))
				return offsetDateTime.toInstant();
			if (LocalDateTime.class.isAssignableFrom(targetClass))
				return offsetDateTime.atZoneSameInstant(statementContext.getTimeZone()).toLocalDateTime();
			if (LocalDate.class.isAssignableFrom(targetClass))
				return offsetDateTime.atZoneSameInstant(statementContext.getTimeZone()).toLocalDate();
			if (java.sql.Timestamp.class.isAssignableFrom(targetClass))
				return Timestamp.from(offsetDateTime.toInstant());
			if (java.sql.Date.class.isAssignableFrom(targetClass))
				return java.sql.Date.valueOf(offsetDateTime.atZoneSameInstant(statementContext.getTimeZone()).toLocalDate());
			if (Date.class.isAssignableFrom(targetClass))
				return Date.from(offsetDateTime.toInstant());
			if (ZonedDateTime.class.isAssignableFrom(targetClass))
				return offsetDateTime.atZoneSameInstant(statementContext.getTimeZone());
		}

		if (resultSetValue instanceof LocalDate localDate) {
			if (LocalDate.class.isAssignableFrom(targetClass))
				return localDate;
			if (LocalDateTime.class.isAssignableFrom(targetClass))
				return localDate.atStartOfDay();
			if (ZonedDateTime.class.isAssignableFrom(targetClass))
				return localDate.atStartOfDay(statementContext.getTimeZone());
			if (Instant.class.isAssignableFrom(targetClass))
				return localDate.atStartOfDay(statementContext.getTimeZone()).toInstant();
			if (java.sql.Date.class.isAssignableFrom(targetClass))
				return java.sql.Date.valueOf(localDate);
			if (java.sql.Timestamp.class.isAssignableFrom(targetClass))
				return Timestamp.valueOf(localDate.atStartOfDay());
			if (OffsetDateTime.class.isAssignableFrom(targetClass))
				return localDate.atStartOfDay(statementContext.getTimeZone()).toOffsetDateTime();
			if (Date.class.isAssignableFrom(targetClass))
				return Date.from(localDate.atStartOfDay(statementContext.getTimeZone()).toInstant());
		}

		if (resultSetValue instanceof LocalTime localTime) {
			if (LocalTime.class.isAssignableFrom(targetClass))
				return localTime;

			if (OffsetTime.class.isAssignableFrom(targetClass)) {
				ZoneOffset off = statementContext.getTimeZone().getRules().getOffset(Instant.EPOCH);
				return localTime.atOffset(off);
			}

			if (java.sql.Time.class.isAssignableFrom(targetClass))
				return java.sql.Time.valueOf(localTime);
		}

		if (resultSetValue instanceof OffsetTime offsetTime) {
			if (OffsetTime.class.isAssignableFrom(targetClass))
				return offsetTime;
			if (LocalTime.class.isAssignableFrom(targetClass))
				return offsetTime.toLocalTime();
			if (java.sql.Time.class.isAssignableFrom(targetClass))
				return java.sql.Time.valueOf(offsetTime.toLocalTime());
		}

		if (resultSetValue instanceof String string) {
			if (BigDecimal.class.isAssignableFrom(targetClass)) {
				try {
					return new BigDecimal(string);
				} catch (NumberFormatException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to BigDecimal", resultSetValue), e);
				}
			}

			if (BigInteger.class.isAssignableFrom(targetClass)) {
				try {
					return exactBigInteger(new BigDecimal(string));
				} catch (NumberFormatException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to BigInteger", resultSetValue), e);
				}
			}

			if (LocalDate.class.isAssignableFrom(targetClass)) {
				try {
					return localDateFromString(string);
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to LocalDate", resultSetValue), e);
				}
			}

			if (LocalTime.class.isAssignableFrom(targetClass)) {
				try {
					return localTimeFromString(string);
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to LocalTime", resultSetValue), e);
				}
			}

			if (LocalDateTime.class.isAssignableFrom(targetClass)) {
				try {
					return localDateTimeFromString(string);
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to LocalDateTime", resultSetValue), e);
				}
			}

			if (Instant.class.isAssignableFrom(targetClass)) {
				try {
					return instantFromString(string, statementContext);
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to Instant", resultSetValue), e);
				}
			}

			if (OffsetDateTime.class.isAssignableFrom(targetClass)) {
				try {
					return offsetDateTimeFromString(string, statementContext);
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to OffsetDateTime", resultSetValue), e);
				}
			}

			if (ZonedDateTime.class.isAssignableFrom(targetClass)) {
				try {
					return zonedDateTimeFromString(string, statementContext);
				} catch (DateTimeException | IllegalArgumentException e) {
					throw new DatabaseException(format("Unable to convert value '%s' to ZonedDateTime", resultSetValue), e);
				}
			}
		}

		if (UUID.class.isAssignableFrom(targetClass)) {
			return uuidFromValue(resultSetValue);
		} else if (ZoneId.class.isAssignableFrom(targetClass)) {
			try {
				return ZoneId.of(resultSetValue.toString());
			} catch (DateTimeException e) {
				throw new DatabaseException(format("Unable to convert value '%s' to ZoneId", resultSetValue), e);
			}
		} else if (TimeZone.class.isAssignableFrom(targetClass)) {
			return timeZoneFromId(resultSetValue.toString());
		} else if (Locale.class.isAssignableFrom(targetClass)) {
			return localeFromLanguageTag(resultSetValue.toString());
		} else if (Currency.class.isAssignableFrom(targetClass)) {
			try {
				return Currency.getInstance(resultSetValue.toString());
			} catch (IllegalArgumentException e) {
				throw new DatabaseException(format("Unable to convert value '%s' to Currency", resultSetValue), e);
			}
		} else if (Year.class.isAssignableFrom(targetClass)) {
			return yearFromValue(resultSetValue);
		} else if (YearMonth.class.isAssignableFrom(targetClass)) {
			return yearMonthFromString(resultSetValue.toString());
		} else if (targetClass.isEnum()) {
			return extractEnumValue(targetClass, resultSetValue);
		}

		if (Boolean.class.isAssignableFrom(targetClass)) {
			// Native boolean
			if (resultSetValue instanceof Boolean b)
				return b;

			// Numeric truthiness (0=false, nonzero=true)
			if (resultSetValue instanceof Number number)
				return decimalForNumericConversion(number).signum() != 0;

			throw new DatabaseException(format("Cannot map value '%s' (%s) to boolean", resultSetValue, resultSetValue.getClass().getName()));
		}

		if (Character.class.isAssignableFrom(targetClass)) {
			// Native char
			if (resultSetValue instanceof Character c) return c;

			// Single-character string
			if (resultSetValue instanceof String string) {
				if (string.length() == 1)
					return string.charAt(0);

				throw new DatabaseException(format("Cannot map String value '%s' to %s; expected length 1", string, propertyType.getSimpleName()));
			}

			// Numeric -> Unicode code point (useful for some JDBC drivers / legacy schemas)
			if (resultSetValue instanceof Number number) {
				int code = exactInt(number);
				if (code >= Character.MIN_VALUE && code <= Character.MAX_VALUE)
					return (char) code;

				throw new DatabaseException(format("Numeric value %d is outside valid char range", code));
			}

			throw new DatabaseException(format("Cannot map %s to %s",
					resultSetValue.getClass().getName(), propertyType.getSimpleName()));
		}

		return resultSetValue;
	}

	@NonNull
	private <T> Object convertSqlArrayResultSetValue(@NonNull StatementContext<T> statementContext,
																									 @NonNull Array sqlArray,
																									 @NonNull TargetType targetType, @NonNull MappingResources resources) {
		requireNonNull(statementContext);
		requireNonNull(sqlArray);
		requireNonNull(targetType);

		try {
			return convertArrayResultSetValue(statementContext, extractSqlArrayResultSetValue(sqlArray, resources), targetType, resources);
		} catch (SQLException e) {
			throw DatabaseException.wrapSafely("Unable to extract JDBC Array value", e, DatabaseDialect.forExceptionCause(e));
		}
	}

	@Nullable
	static Object extractSqlArrayResultSetValue(@NonNull Array sqlArray, MappingResources resources) throws SQLException {
		requireNonNull(sqlArray);

		Throwable thrown = null;

		try {
			Object values = sqlArray.getArray();
			int baseType = Types.OTHER;
			try {
				baseType = sqlArray.getBaseType();
				if (baseType == Types.TIMESTAMP || baseType == Types.TIME) {
					// Some drivers report both zoned and zone-less types with the legacy JDBC code.
					try {
						String typeName = sqlArray.getBaseTypeName();
						if (typeName != null) {
							String normalized = typeName.toUpperCase(Locale.ROOT);
							if (baseType == Types.TIMESTAMP && (normalized.equals("TIMESTAMPTZ")
									|| normalized.contains("WITH TIME ZONE") || normalized.contains("WITH LOCAL TIME ZONE")))
								baseType = Types.TIMESTAMP_WITH_TIMEZONE;
							else if (baseType == Types.TIME && TemporalReaders.isTimeWithTimeZoneName(normalized))
								baseType = Types.TIME_WITH_TIMEZONE;
						}
					} catch (SQLException | UnsupportedOperationException | AbstractMethodError unsupportedTypeName) {
						if (JdbcFallbacks.isTerminal(unsupportedTypeName))
							throw unsupportedTypeName;
						// A supported JDBC type remains useful when the optional SQL name is unavailable.
					}
				}
			} catch (SQLException | UnsupportedOperationException | AbstractMethodError unsupportedMetadata) {
				if (JdbcFallbacks.isTerminal(unsupportedMetadata))
					throw unsupportedMetadata;
				// Older drivers can expose array contents without optional base-type metadata.
			}
			resources.registerArrayType(values, baseType);
			if ((baseType == Types.TIME || baseType == Types.TIME_WITH_TIMEZONE) && containsLegacyTime(values)) {
				// java.sql.Time has already lost fractional seconds and, for zoned values, the offset.
				// Keep raw inputs for custom/native targets and detach a lossless alternate while alive.
				TimeArrayRead read = readTimeArray(sqlArray, values, false, baseType);
				if (read.failure != null) {
					if (read.cleanupFailed || JdbcFallbacks.isTerminal(read.failure) || !isUnsupportedArrayReader(read.failure))
						rethrowArrayFailure(read.failure);
					resources.registerArrayTimeValues(values, read.failure);
				} else resources.registerArrayTimeValues(values, read.values);
			}
			return values;
		} catch (SQLException | RuntimeException | Error e) {
			thrown = e;
			throw e;
		} finally {
			try {
				sqlArray.free();
			} catch (SQLException | RuntimeException | Error e) {
				if (thrown != null && thrown != e)
					thrown.addSuppressed(e);
				else
					throw e;
			}
		}
	}

	private static boolean isUnsupportedArrayReader(Throwable failure) {
		if (failure instanceof SQLFeatureNotSupportedException || failure instanceof UnsupportedOperationException
				|| failure instanceof AbstractMethodError) return true;
		if (failure instanceof SQLException sql) {
			try {
				String state = sql.getSQLState();
				return state != null && (state.startsWith("0A") || state.equals("HYC00"));
			} catch (Throwable unreadableState) {
				// Preserve the original read failure if a driver cannot expose its classification.
			}
		}
		return false;
	}

	private static boolean containsLegacyTime(@Nullable Object value) {
		if (value instanceof java.sql.Time) return true;
		if (value instanceof Object[] elements)
			for (Object element : elements) if (containsLegacyTime(element)) return true;
		return false;
	}

	private record TimeArrayRead(@Nullable Object values, @Nullable Throwable failure, boolean cleanupFailed) {}

	private static TimeArrayRead readTimeArray(Array array, Object raw, boolean free, int jdbcType) {
		ResultSet rows = null;
		Object[] values = null;
		Throwable failure = null;
		boolean cleanupFailed = false;
		try {
			int length = java.lang.reflect.Array.getLength(raw);
			values = new Object[length];
			rows = array.getResultSet();
			if (rows == null) throw new SQLFeatureNotSupportedException("Driver did not provide SQL ARRAY element rows");
			for (int index = 0; index < length; index++) {
				if (!rows.next()) throw new SQLException("SQL ARRAY element rows ended before the array contents");
				Object element = java.lang.reflect.Array.get(raw, index);
				if (element != null && element.getClass().isArray()) {
					Array nested = rows.getArray(2);
					if (nested == null) throw new SQLException("Driver returned NULL for a non-null nested SQL ARRAY");
					TimeArrayRead read = readTimeArray(nested, element, true, jdbcType);
					if (read.failure != null) {
						failure = read.failure;
						cleanupFailed = read.cleanupFailed;
						break;
					}
					values[index] = read.values;
				} else {
					values[index] = jdbcType == Types.TIME ? rows.getObject(2, LocalTime.class)
							: rows.getObject(2, OffsetTime.class);
					if (element != null && values[index] == null)
						throw new SQLException("Driver returned NULL for a non-null SQL TIME element");
				}
			}
			if (failure == null && rows.next()) throw new SQLException("SQL ARRAY element rows exceeded the array contents");
		} catch (SQLException | RuntimeException | Error readFailure) {
			failure = readFailure;
		} finally {
			if (rows != null) {
				try { rows.close(); }
				catch (SQLException | RuntimeException | Error closeFailure) {
					cleanupFailed = true;
					if (failure == null) failure = closeFailure;
					else if (failure != closeFailure) failure.addSuppressed(closeFailure);
				}
			}
			if (free) {
				try { array.free(); }
				catch (SQLException | RuntimeException | Error freeFailure) {
					cleanupFailed = true;
					if (failure == null) failure = freeFailure;
					else if (failure != freeFailure) failure.addSuppressed(freeFailure);
				}
			}
		}
		return new TimeArrayRead(values, failure, cleanupFailed);
	}

	private static void rethrowArrayFailure(Throwable failure) throws SQLException {
		if (failure instanceof SQLException sql) throw sql;
		if (failure instanceof RuntimeException runtime) throw runtime;
		throw (Error) failure;
	}

	private static boolean isTimeContainerTarget(TargetType target) {
		while (target.isArray() || target.isList() || target.isSet()) {
			target = (target.isArray() ? target.getArrayComponentType()
					: target.isList() ? target.getListElementType() : target.getSetElementType()).orElse(TargetType.of(Object.class));
		}
		return target.getRawClass() == OffsetTime.class || target.getRawClass() == LocalTime.class
				|| target.getRawClass() == java.sql.Time.class;
	}

	@NonNull
	private <T> Object convertArrayResultSetValue(@NonNull StatementContext<T> statementContext,
																								@NonNull Object arrayValue,
																								@NonNull TargetType targetType, @NonNull MappingResources resources) {
		requireNonNull(statementContext);
		requireNonNull(arrayValue);
		requireNonNull(targetType);

		Class<?> arrayClass = arrayValue.getClass();
		if (!arrayClass.isArray())
			throw new DatabaseException(format("Expected JDBC Array value to expose a Java array but encountered %s",
					arrayClass.getName()));

		int elementJdbcType = resources.arrayType(arrayValue);
		if ((elementJdbcType == Types.TIME || elementJdbcType == Types.TIME_WITH_TIMEZONE) && isTimeContainerTarget(targetType))
			arrayValue = resources.arrayTimeValues(arrayValue);
		int length = java.lang.reflect.Array.getLength(arrayValue);

		if (targetType.isList()) {
			TargetType elementType = targetType.getListElementType().orElse(TargetType.of(Object.class));
			List<Object> values = new ArrayList<>(length);

			for (int i = 0; i < length; ++i)
				values.add(convertResultSetArrayElement(statementContext, java.lang.reflect.Array.get(arrayValue, i), elementType, i, resources, elementJdbcType));

			return values;
		}

		if (targetType.isSet()) {
			TargetType elementType = targetType.getSetElementType().orElse(TargetType.of(Object.class));
			Set<Object> values = new LinkedHashSet<>(length);

			for (int i = 0; i < length; ++i)
				values.add(convertResultSetArrayElement(statementContext, java.lang.reflect.Array.get(arrayValue, i), elementType, i, resources, elementJdbcType));

			return values;
		}

		TargetType componentType = targetType.getArrayComponentType().orElse(TargetType.of(Object.class));
		Class<?> componentClass = componentType.getRawClass();
		Object convertedArray = java.lang.reflect.Array.newInstance(componentClass, length);

		for (int i = 0; i < length; ++i) {
			Object convertedElement = convertResultSetArrayElement(statementContext, java.lang.reflect.Array.get(arrayValue, i), componentType, i, resources, elementJdbcType);

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
																									int elementIndex, @NonNull MappingResources resources, int jdbcType) {
		requireNonNull(statementContext);
		requireNonNull(targetType);

		if (value == null)
			return null;

		Class<?> targetClass = boxedClass(targetType.getRawClass());

		if (targetClass == Object.class)
			return resources.nativeValue(value, statementContext.getDatabaseDialect());
		value = normalizeContainerTemporalValue(value, jdbcType);
		if (value.getClass().isArray()) resources.registerArrayType(value, jdbcType);
		if (targetClass.isInstance(value) && !(value.getClass().isArray()
				&& (targetType.isArray() || targetType.isList() || targetType.isSet()))) {
			resources.borrow(value);
			return value;
		}

		Object converted = convertResultSetValueToTargetType(statementContext, value, targetType, resources);

		if (converted != null && !targetClass.isInstance(converted)) {
			throw new DatabaseException(format("SQL ARRAY element %s of type %s cannot be mapped to %s",
					elementIndex, value.getClass().getName(), targetClass.getName()));
		}

		return converted;
	}

	private static Object normalizeContainerTemporalValue(Object value, int jdbcType) {
		// A Timestamp stores a JVM-zone-based epoch even for SQL TIMESTAMP WITHOUT TIME ZONE.
		// Preserve the SQL container's type so only zone-less values become wall-clock values.
		if (value instanceof Timestamp timestamp) {
			if (jdbcType == Types.TIMESTAMP) value = timestamp.toLocalDateTime();
			else if (jdbcType == Types.TIMESTAMP_WITH_TIMEZONE) value = timestamp.toInstant();
		} else if (value instanceof java.sql.Date date) {
			value = date.toLocalDate();
		} else if (value instanceof java.sql.Time time && jdbcType == Types.TIME) {
			value = time.toLocalTime();
		}
		return value;
	}

	private static boolean isTimestampConversionTarget(Class<?> target) {
		return target == Instant.class || target == LocalDate.class || target == LocalDateTime.class
				|| target == OffsetDateTime.class || target == ZonedDateTime.class || Date.class.isAssignableFrom(target);
	}

	/**
	 * Used to construct nested record instances during SQL STRUCT mapping via the canonical record constructor
	 * (the {@link InstanceProvider} default behavior).
	 */
	@NonNull
	private static final InstanceProvider STRUCT_RECORD_INSTANCE_PROVIDER = new InstanceProvider() {};

	private static boolean isSingleStructColumn(@NonNull ResultSetMetaData resultSetMetaData) throws SQLException {
		requireNonNull(resultSetMetaData);

		return resultSetMetaData.getColumnCount() == 1 && resultSetMetaData.getColumnType(1) == Types.STRUCT;
	}

	/**
	 * Maps a single-column SQL STRUCT row directly to a record: {@code SELECT person FROM ...} with a record
	 * target whose components match the STRUCT's attributes. Only attempted when no result columns map to
	 * record components by name, so ordinary row mapping always takes precedence.
	 *
	 * @return the mapped record, or {@code null} when the STRUCT value is {@code NULL}
	 * @throws DatabaseException if the dialect does not expose the STRUCT's attributes as a name-to-value map
	 */
	@Nullable
	private <T> T mapStructColumnToRecord(@NonNull StatementContext<T> statementContext,
																				@NonNull ResultSet resultSet,
																				@NonNull Class<T> resultSetRowType,
																				@NonNull InstanceProvider instanceProvider,
																				@NonNull ResultSetMetaData resultSetMetaData, @NonNull MappingResources resources) throws SQLException {
		requireNonNull(statementContext);
		requireNonNull(resultSet);
		requireNonNull(resultSetRowType);
		requireNonNull(instanceProvider);
		requireNonNull(resultSetMetaData);

		Object resultSetValue = extractColumnValue(resultSetMetaData, statementContext, resultSet, 1, resources).orElse(null);

		if (resultSetValue != null)
			resultSetValue = statementContext.getDatabaseDialect() instanceof DuckDbDialect duck
				? duck.unwrapForTypedMapping(resultSetValue, resources)
				: statementContext.getDatabaseDialect().unwrapResultSetValue(resultSetValue);

		if (resultSetValue == null)
			return null;

		if (!(resultSetValue instanceof Map<?, ?> mapValue))
			throw noRecordColumnMappingsException(resultSetRowType, resultSetMetaData);

		@SuppressWarnings("unchecked")
		T record = (T) convertMapResultSetValueToRecord(statementContext, mapValue, resultSetRowType, instanceProvider, resources);
		return record;
	}

	/**
	 * Converts a SQL STRUCT value - surfaced by the dialect as an attribute-name to value map - to an instance
	 * of {@code recordClass}.
	 * <p>
	 * Attribute names are normalized with {@link #normalizeColumnLabel(String)} and matched against record
	 * components with the same conventions used for result columns ({@code camelCase} components match
	 * {@code snake_case} attributes, and {@link DatabaseColumn} aliases apply). Attribute values are converted
	 * recursively, so nested STRUCTs map to nested records and STRUCT lists map to record collections.
	 * {@link CustomColumnMapper}s are not consulted for individual STRUCT attributes. The record is
	 * constructed via {@code instanceProvider}: single-column STRUCT fallbacks pass the configured
	 * {@link InstanceProvider} (so top-level row records honor it, like any other row mapping), while
	 * nested STRUCT attributes pass {@link #STRUCT_RECORD_INSTANCE_PROVIDER} (canonical constructors).
	 */
	@NonNull
	private <T> Object convertMapResultSetValueToRecord(@NonNull StatementContext<T> statementContext,
																											@NonNull Map<?, ?> mapValue,
																											@NonNull Class<?> recordClass,
																											@NonNull InstanceProvider instanceProvider, @NonNull MappingResources resources) {
		requireNonNull(statementContext);
		requireNonNull(mapValue);
		requireNonNull(recordClass);
		requireNonNull(instanceProvider);

		Map<String, Object> valuesByNormalizedName = new HashMap<>(mapValue.size());
		Map<String, Integer> attributeIndexesByNormalizedName = new HashMap<>(mapValue.size());
		Map<String, String> normalizedNamesToRawNames = new HashMap<>(mapValue.size());
		int attributeIndex = 0;

		for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
			String rawName = String.valueOf(entry.getKey());
			String normalizedName = normalizeColumnLabel(rawName);
			String previousRawName = normalizedNamesToRawNames.putIfAbsent(normalizedName, rawName);

			if (previousRawName != null)
				throw new DatabaseException(format(
						"Duplicate STRUCT attribute name '%s' (normalized from '%s' and '%s'); use distinct attribute names.",
						normalizedName, previousRawName, rawName));

			valuesByNormalizedName.put(normalizedName, entry.getValue());
			attributeIndexesByNormalizedName.put(normalizedName, attributeIndex++);
		}

		RecordComponent[] recordComponents = determineRecordComponents(recordClass);
		Map<String, Set<String>> columnLabelsByRecordComponentName = determineColumnLabelsByRecordComponentName(recordClass);
		Map<String, TargetType> propertyTargetTypes = determinePropertyTargetTypes(recordClass);

		Object[] args = new Object[recordComponents.length];
		int mappedAttributeCount = 0;
		List<RecordComponent> missingPrimitiveRecordComponents = new ArrayList<>();

		for (int i = 0; i < recordComponents.length; ++i) {
			RecordComponent recordComponent = recordComponents[i];
			String propertyName = recordComponent.getName();

			// If there are any @DatabaseColumn annotations on this component, respect them
			Set<String> potentialAttributeNames = columnLabelsByRecordComponentName.get(propertyName);

			// There were no @DatabaseColumn annotations, use the default naming strategy
			if (potentialAttributeNames == null || potentialAttributeNames.isEmpty())
				potentialAttributeNames = databaseColumnNamesForPropertyName(propertyName);

			Class<?> recordComponentType = recordComponent.getType();

			// Get the precise TargetType (generic-aware) for this component
			TargetType targetType = propertyTargetTypes.get(propertyName);
			if (targetType == null) targetType = TargetType.of(recordComponent.getGenericType());

			// Apply in attribute order for deterministic results
			List<String> orderedAttributeNames = new ArrayList<>(potentialAttributeNames.size());
			for (String potentialAttributeName : potentialAttributeNames)
				if (valuesByNormalizedName.containsKey(potentialAttributeName))
					orderedAttributeNames.add(potentialAttributeName);

			if (orderedAttributeNames.isEmpty()) {
				if (recordComponentType.isPrimitive())
					missingPrimitiveRecordComponents.add(recordComponent);
				continue;
			}

			orderedAttributeNames.sort((left, right) ->
					Integer.compare(attributeIndexesByNormalizedName.get(left), attributeIndexesByNormalizedName.get(right)));

			for (String attributeName : orderedAttributeNames) {
				++mappedAttributeCount;
				Object rawValue = valuesByNormalizedName.get(attributeName);
				if (rawValue instanceof Timestamp && isTimestampConversionTarget(recordComponentType)
						&& statementContext.getDatabaseDialect() instanceof DuckDbDialect duck)
					rawValue = normalizeContainerTemporalValue(rawValue, duck.structAttributeJdbcType(resources.structType(mapValue),
							attributeIndexesByNormalizedName.get(attributeName)));
				Object value = rawValue == null ? null
						: convertResultSetValueToTargetType(statementContext, rawValue, targetType, resources);

				// It's considered programmer error to have a NULL attribute value and map it to a primitive (which does not support null)
				if (value == null && recordComponentType.isPrimitive())
					throw new DatabaseException(format("STRUCT attribute '%s' is NULL but record component '%s' of %s is primitive (%s). Use a non-primitive type or COALESCE/CAST in SQL.",
							attributeName, recordComponent.getName(), recordClass.getSimpleName(), recordComponentType.getSimpleName()));

				Class<?> checkedType = boxedClass(recordComponentType);

				if (value != null && !checkedType.isInstance(value)) {
					throw new DatabaseException(format(
							"STRUCT attribute '%s' for record component '%s' of %s expects type %s, but the STRUCT produced %s",
							attributeName, recordComponent.getName(), recordClass, recordComponentType, value.getClass()));
				}

				args[i] = value;
			}
		}

		if (mappedAttributeCount == 0)
			throw new DatabaseException(format(
					"No STRUCT attributes map to record components of %s. STRUCT attributes: %s. "
							+ "Ensure the STRUCT's attribute names match record components or provide a custom ResultSetMapper.",
					recordClass.getName(), String.join(", ", valuesByNormalizedName.keySet())));

		if (!missingPrimitiveRecordComponents.isEmpty()) {
			RecordComponent missingPrimitiveRecordComponent = missingPrimitiveRecordComponents.get(0);
			throw new DatabaseException(format(
					"No STRUCT attribute matches record component '%s' of %s, but the component is primitive (%s). "
							+ "Ensure the attribute is present or use a non-primitive type.",
					missingPrimitiveRecordComponent.getName(), recordClass.getSimpleName(), missingPrimitiveRecordComponent.getType().getSimpleName()));
		}

		return provideRecordVia(instanceProvider, statementContext, recordClass, args);
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

	/**
	 * Parses a vector literal such as {@code [1.5,2.5,3.5]} into a {@code float[]} or {@code double[]}.
	 * Returns {@code null} when the value is not bracket-delimited (so callers can fall through to normal
	 * conversion); throws when the value looks like a vector but an element fails to parse.
	 */
	@Nullable
	private static Object vectorFromLiteral(@NonNull String value,
																					boolean asFloats) {
		requireNonNull(value);

		String trimmed = value.trim();

		if (trimmed.length() < 2 || trimmed.charAt(0) != '[' || trimmed.charAt(trimmed.length() - 1) != ']')
			return null;

		String body = trimmed.substring(1, trimmed.length() - 1).trim();

		if (body.isEmpty())
			return asFloats ? new float[0] : new double[0];

		String[] elements = body.split(",", -1);

		try {
			if (asFloats) {
				float[] floats = new float[elements.length];
				for (int i = 0; i < elements.length; ++i) {
					floats[i] = Float.parseFloat(elements[i].trim());
					if (!Float.isFinite(floats[i]))
						throw new NumberFormatException(format("Non-finite vector element '%s'", elements[i].trim()));
				}
				return floats;
			}

			double[] doubles = new double[elements.length];
			for (int i = 0; i < elements.length; ++i) {
				doubles[i] = Double.parseDouble(elements[i].trim());
				if (!Double.isFinite(doubles[i]))
					throw new NumberFormatException(format("Non-finite vector element '%s'", elements[i].trim()));
			}
			return doubles;
		} catch (NumberFormatException e) {
			String bounded = trimmed.length() > 128 ? trimmed.substring(0, 128) + "..." : trimmed;
			throw new DatabaseException(format("Unable to parse vector literal '%s'", bounded), e);
		}
	}

	@Nullable
	private static Year yearFromValue(@Nullable Object value) {
		if (value == null)
			return null;
		if (value instanceof Year year)
			return year;

		// Some drivers surface year columns as dates - e.g. MySQL Connector/J maps YEAR to DATE by default
		if (value instanceof LocalDate localDate)
			return Year.of(localDate.getYear());
		if (value instanceof java.sql.Date date)
			return Year.of(date.toLocalDate().getYear());

		if (value instanceof Number number) {
			try {
				return Year.of(decimalForNumericConversion(number).intValueExact());
			} catch (ArithmeticException | DateTimeException e) {
				throw new DatabaseException(format("Unable to convert value '%s' to Year", value), e);
			}
		}

		try {
			return Year.of(Integer.parseInt(value.toString().trim()));
		} catch (NumberFormatException | DateTimeException e) {
			throw new DatabaseException(format("Unable to convert value '%s' to Year", value), e);
		}
	}

	@Nullable
	private static YearMonth yearMonthFromString(@Nullable String value) {
		if (value == null)
			return null;

		try {
			return YearMonth.parse(value.trim());
		} catch (DateTimeParseException e) {
			throw new DatabaseException(format("Unable to convert value '%s' to YearMonth", value), e);
		}
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
						return TimeZone.getTimeZone(ZoneId.of(upper));
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
			return name != null && isTimeWithTimeZoneName(name.toUpperCase(Locale.ROOT));
		}

		private static boolean isTimeWithTimeZoneName(String normalized) {
			return normalized.equals("TIMETZ") || normalized.startsWith("TIME") && !normalized.startsWith("TIMESTAMP")
					&& normalized.contains("WITH TIME ZONE");
		}

		/**
		 * Try JDBC 4.2 getObject; return null if unsupported.
		 */
		@Nullable
		public static <T> T tryGet(ResultSet rs, int col, Class<T> cls) throws SQLException {
			try {
				return rs.getObject(col, cls);
			} catch (SQLException | AbstractMethodError e) {
				if (JdbcFallbacks.isTerminal(e))
					throw e;
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
			if (withTz) {
				OffsetDateTime got = tryGet(rs, col, OffsetDateTime.class);
				if (got != null) return got;

				// Try via Instant if driver only gives us Timestamp
				Instant inst = asInstant(rs, col, ctx, true);
				return inst == null ? null : inst.atOffset(ctx.getTimeZone().getRules().getOffset(inst));
			} else {
				// A driver's OffsetDateTime conversion would attach its session/JVM zone, not the configured zone.
				LocalDateTime ldt = asLocalDateTime(rs, col, ctx, false);
				return ldt == null ? null : ldt.atZone(ctx.getTimeZone()).toOffsetDateTime();
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
			if (withTz) {
				OffsetDateTime odt = tryGet(rs, col, OffsetDateTime.class);
				if (odt != null) return odt.atZoneSameInstant(ctx.getTimeZone()).toLocalDateTime();
			} else {
				LocalDateTime ldt = tryGet(rs, col, LocalDateTime.class);
				if (ldt != null) return ldt;
			}

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

		public static LocalTime asLocalTime(ResultSet rs, int col, StatementContext<?> ctx, ResultSetMetaData metadata) throws SQLException {
			if (isTimeWithTimeZone(metadata, col)) {
				OffsetTime time = asOffsetTime(rs, col, ctx, true);
				return time == null ? null : time.toLocalTime();
			}
			return asLocalTime(rs, col);
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
				// Some drivers (e.g. DuckDB for TIMETZ) return OffsetTime from plain getObject while their
				// typed accessors are unsupported or broken - prefer the raw value before the LocalTime path
				Object raw = rs.getObject(col);
				if (raw == null) return null;
				if (raw instanceof OffsetTime offsetTime) return offsetTime;
				if (raw instanceof String s) {
					try {
						return OffsetTime.parse(s);
					} catch (DateTimeParseException ignore) {
					}
				}
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
									 @NonNull InstanceProvider ip, @NonNull MappingResources resources);
	}

	/** Execution-only state; source values are shared by alias bindings without caching conversions. */
	private static final class RowReadScope {
		private final ResultSet resultSet;
		private final StatementContext<?> context;
		private final MappingResources resources;
		private final @Nullable Object[] raw;
		private final @Nullable boolean[] loaded;

		RowReadScope(ResultSet resultSet, StatementContext<?> context, RowPlan<?> plan, MappingResources resources) {
			this.resultSet = resultSet;
			this.context = context;
			this.resources = resources;
			this.raw = plan.hasFanOut ? new Object[plan.columnCount + 1] : null;
			this.loaded = plan.hasFanOut ? new boolean[plan.columnCount + 1] : null;
		}

		@Nullable Object read(ColumnBinding binding) throws SQLException {
			if (this.raw == null) return readColumn(binding.reader, this.resultSet, this.context, binding.columnIndex, this.resources);
			int column = binding.columnIndex;
			if (!this.loaded[column]) {
				this.raw[column] = readColumn(binding.reader, this.resultSet, this.context, column, this.resources);
				this.loaded[column] = true;
			}
			return this.raw[column];
		}
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
			this.bindings = List.copyOf(bindings);
		}
	}

	protected static final class SchemaSignatureMemo {
		@NonNull
		private final WeakReference<ResultSet> resultSetReference;
		private final WeakReference<DatabaseDialect> dialectReference;
		@NonNull
		private final String schemaSignature;

		SchemaSignatureMemo(@NonNull ResultSet resultSet, @NonNull DatabaseDialect dialect,
												@NonNull String schemaSignature) {
			this.resultSetReference = new WeakReference<>(requireNonNull(resultSet));
			this.dialectReference = new WeakReference<>(requireNonNull(dialect));
			this.schemaSignature = requireNonNull(schemaSignature);
		}

		boolean matches(@NonNull ResultSet resultSet, @NonNull DatabaseDialect dialect) {
			requireNonNull(resultSet);
			return this.resultSetReference.get() == resultSet && this.dialectReference.get() == dialect;
		}

		@NonNull
		String getSchemaSignature() {
			return this.schemaSignature;
		}
	}

	private record RowTypeMetadata(boolean isRecord, Map<String, TargetType> propertyTargetTypes,
			Map<String, Integer> recordIndexByProp, RecordComponent[] recordComponents,
			Map<String, MethodHandle> setterByProp, Map<String, List<String>> propertiesByLabel) {}

	private RowTypeMetadata computeRowTypeMetadata(Class<?> resultClass) {
		// Precompute property metadata for this class
		Map<String, TargetType> propertyTargetTypes = determinePropertyTargetTypes(resultClass);
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
				try {
					setterByProp.put(writableProperty.getName(), lookup.unreflect(writableProperty.getWriteMethod()));
				} catch (IllegalAccessException failure) {
					throw new DatabaseException("Unable to access property setter for " + resultClass.getName(), failure);
				}
		}

		// Union of all writable property names
		Set<String> allProps = new HashSet<>();
		allProps.addAll(setterByProp.keySet());
		allProps.addAll(recordIndexByProp.keySet());

		// Precompute label -> properties to avoid scanning all properties per column
		Map<String, List<String>> propertiesByLabel = new HashMap<>(allProps.size() * 2);
		Map<String, Set<String>> labelsByProperty = isRecord
				? determineColumnLabelsByRecordComponentName(resultClass)
				: determineNormalizedColumnLabelsByPropertyName(resultClass);
		for (String prop : allProps) {
			Set<String> labels = labelsByProperty.get(prop);
			if (labels == null) labels = databaseColumnNamesForPropertyName(prop);

			for (String label : labels)
				propertiesByLabel.computeIfAbsent(label, labelKey -> new ArrayList<>(1)).add(prop);
		}

		propertiesByLabel.replaceAll((label, properties) -> List.copyOf(properties));
		return new RowTypeMetadata(isRecord, propertyTargetTypes, Map.copyOf(recordIndexByProp),
				recordComponents, Map.copyOf(setterByProp), Map.copyOf(propertiesByLabel));
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

		Map<String, RowPlan<?>> planCache = getPlanCachingEnabled() ? getRowPlanningCacheForResultClass(resultClass) : null;
		RowPlan<?> cached = planCache == null ? null : planCache.get(schemaSignature);
		if (cached != null) return (RowPlan<T>) cached;

		int cols = resultSetMetaData.getColumnCount();

		RowTypeMetadata metadata = this.rowTypeMetadata.get(resultClass);
		Map<String, TargetType> propertyTargetTypes = metadata.propertyTargetTypes;
		boolean isRecord = metadata.isRecord;
		Map<String, Integer> recordIndexByProp = metadata.recordIndexByProp;
		RecordComponent[] recordComponents = metadata.recordComponents;
		Map<String, MethodHandle> setterByProp = metadata.setterByProp;
		Map<String, List<String>> propertiesByLabel = metadata.propertiesByLabel;

		// For each column, bind ALL matching properties (fan-out)
		List<ColumnBinding> bindings = new ArrayList<>(cols);
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
			ColumnReader reader = columnReaderFor(resultSetMetaData, ctx, i);
			final int jdbcType = resultSetMetaData.getColumnType(i);

			// Find all properties that claim this label
			List<String> matchedProps = propertiesByLabel.get(labelNorm);

			if (matchedProps == null || matchedProps.isEmpty()) {
				// No property claims this column - add a SKIP binding to keep behavior explicit
				bindings.add(
						new ColumnBinding(
								i,                            // columnIndex
								-1,                           // recordArgIndexOrMinusOne (SKIP)
								null,                         // setterOrNull
								reader,                       // reader
								(raw, rrs, cctx, ip, resources) -> raw,  // converter
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
				Converter converter = (raw, rrs, cctx, ip, resources) -> convertColumn(
						raw, targetTypeFinal, customMappers, rrs, cctx, ip, resources,
						colIndexFinal, labelFinal, jdbcType);


				bindings.add(new ColumnBinding(i, recordArgIndex, setterMH, reader, converter,
						targetIsPrimitive, rawTargetClassBoxed, propertyNameFinal));
			}
		}

		RowPlan<T> plan = new RowPlan<>(isRecord, cols, hasFanOut, mappedColumnCount, bindings);
		if (planCache != null) planCache.putIfAbsent(schemaSignature, plan);
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
	protected Map<@NonNull String, @NonNull RowPlan<?>> getRowPlanningCacheForResultClass(@NonNull Class<?> resultClass) {
		requireNonNull(resultClass);
		return this.rowPlanningCacheByResultClass.get(resultClass);
	}

	@NonNull
	protected Map<ResultSetMetaData, Map<DatabaseDialect, String>> getSchemaSignatureByResultSetMetaData() {
		return this.schemaSignatureByResultSetMetaData;
	}

	@NonNull
	protected ReentrantLock getSchemaSignatureByResultSetMetaDataLock() {
		return this.schemaSignatureByResultSetMetaDataLock;
	}
}
