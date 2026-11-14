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
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
	private static final int[] NO_CUSTOM_COLUMN_MAPPERS = new int[0];
	@NonNull
	private static final Pattern CAMEL_CASE_PATTERN = Pattern.compile("([a-z])([A-Z]+)");
	@NonNull
	private static final Pattern LETTER_FOLLOWED_BY_NUMBER_PATTERN = Pattern.compile("(\\D)(\\d)");
	@NonNull
	private static final Pattern NUMBER_FOLLOWED_BY_CAPITAL_PATTERN = Pattern.compile("(\\d)([A-Z])");
	@NonNull
	private static final Pattern ACRONYM_WORD_BOUNDARY_PATTERN = Pattern.compile("([A-Z]+)([A-Z][a-z])");

	@NonNull
	private final Locale normalizationLocale;
	@NonNull
	private final List<CustomColumnMapper> customColumnMappers;
	@NonNull
	private final Boolean planCachingEnabled;
	private final int planCacheCapacity;
	private final @Nullable Long maxLobBytes;
	private final @Nullable Long maxLobCharacters;
	private final boolean failOnUnmappedColumns;
	private final boolean detachLobValues;
	// Store indices rather than callbacks: a user callback can refer back to this mapper or its Database.
	@NonNull
	private final ClassValue<int[]> customColumnMappersByRawClassCache =
			new ClassValue<>() {
				@Override
				protected int[] computeValue(Class<?> type) {
					return computeCustomColumnMapperIndicesFor(TargetType.of(type));
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
	// Cursor identity remains reusable even when a driver creates fresh metadata objects for each row.
	private final Map<WeakIdentityKey<ResultSet>, Map<WeakIdentityKey<DatabaseDialect>, String>> schemaSignaturesByResultSet = new HashMap<>();
	private final ReferenceQueue<ResultSet> schemaSignatureResultSets = new ReferenceQueue<>();
	// Nested mapping shares ownership bookkeeping, but no mapper-owned value remains on a worker afterward.
	private final ThreadLocal<MappingResources> mappingResources = new ThreadLocal<>();

	private MappingResources acquireMappingResources() {
		MappingResources resources = this.mappingResources.get();
		if (resources == null) {
			resources = new MappingResources(this.maxLobBytes, this.maxLobCharacters, this.detachLobValues);
			this.mappingResources.set(resources);
		}
		return resources.acquire();
	}

	private void releaseMappingResources(MappingResources resources, @Nullable Throwable failure) throws SQLException {
		try {
			resources.close(failure);
		} finally {
			// Recursive calls acquire a child scope. Only the root's close ends this mapper's thread association.
			if (this.mappingResources.get() == resources) {
				this.mappingResources.remove();
			}
		}
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
		return customColumnMappersAtIndices(customColumnMapperIndicesFor(targetType));
	}

	private int[] customColumnMapperIndicesFor(TargetType targetType) {
		if (getCustomColumnMappers().isEmpty())
			return NO_CUSTOM_COLUMN_MAPPERS;

		if (targetType.getType() instanceof Class<?>)
			return this.customColumnMappersByRawClassCache.get(targetType.getRawClass());

		// Preserve generic arrays, variables and wildcard bounds as well as parameterized types.
		// Caching them under an erased Class loses applicability metadata and can retain user classes.
		return computeCustomColumnMapperIndicesFor(targetType);
	}

	private int[] computeCustomColumnMapperIndicesFor(TargetType targetType) {
		List<CustomColumnMapper> mappers = getCustomColumnMappers();
		int[] matches = new int[mappers.size()];
		int count = 0;
		for (int index = 0; index < mappers.size(); index++)
			if (mappers.get(index).appliesTo(targetType)) matches[count++] = index;
		return count == 0 ? NO_CUSTOM_COLUMN_MAPPERS : Arrays.copyOf(matches, count);
	}

	private List<CustomColumnMapper> customColumnMappersAtIndices(int[] matches) {
		if (matches.length == 0) return List.of();
		List<CustomColumnMapper> filtered = new ArrayList<>(matches.length);
		for (int index : matches) filtered.add(getCustomColumnMappers().get(index));
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
				JdbcCallbackContext.requireValidation();
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
			Method metadataMethod = setterMetadataMethod(writableProperty.getWriteMethod());
			Type genericParameterType = GenericPropertyTypes.resolve(resultClass, metadataMethod.getDeclaringClass(),
					metadataMethod.getGenericParameterTypes()[0]);
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
					declaringClassDistance(resultClass, setterMetadataMethod(left.getWriteMethod()).getDeclaringClass()),
					declaringClassDistance(resultClass, setterMetadataMethod(right.getWriteMethod()).getDeclaringClass()));
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

		if (Modifier.isStatic(method.getModifiers()))
			return Optional.empty();
		if (!method.getName().startsWith("set") || method.getName().length() <= 3)
			return Optional.empty();
		if (method.getParameterCount() != 1)
			return Optional.empty();
		if (method.getReturnType() != Void.TYPE)
			return Optional.empty();
		if ((method.isBridge() || method.isSynthetic()) && visibilityBridgeTarget(method).isEmpty())
			return Optional.empty();

		return Optional.of(decapitalizeJavaBeansPropertyName(method.getName().substring(3)));
	}

	@NonNull
	private Method setterMetadataMethod(@NonNull Method method) {
		// javac's public visibility bridge is invocable, but erases parameterized setter metadata.
		return visibilityBridgeTarget(method).orElse(method);
	}

	@NonNull
	private Optional<Method> visibilityBridgeTarget(@NonNull Method method) {
		requireNonNull(method);
		if (!method.isBridge() || !method.isSynthetic() || !Modifier.isPublic(method.getModifiers())
				|| !Modifier.isPublic(method.getDeclaringClass().getModifiers()))
			return Optional.empty();

		for (Class<?> ancestor = method.getDeclaringClass().getSuperclass(); ancestor != null; ancestor = ancestor.getSuperclass()) {
			Method inherited;
			try {
				inherited = ancestor.getDeclaredMethod(method.getName(), method.getParameterTypes());
			} catch (NoSuchMethodException ignored) {
				continue;
			}
			if (inherited.isBridge() || inherited.isSynthetic())
				continue;
			if (Modifier.isPublic(ancestor.getModifiers()) || !Modifier.isPublic(inherited.getModifiers())
					|| Modifier.isStatic(inherited.getModifiers()) || Modifier.isAbstract(inherited.getModifiers())
					|| inherited.getReturnType() != method.getReturnType())
				return Optional.empty();
			// A real typed override has its own invocable setter; its narrowing erasure bridge is not another
			// property. A visibility-only bridge still uses the inherited generic metadata, resolved below
			// against the concrete result class rather than erased to Object.
			for (Method candidate : method.getDeclaringClass().getDeclaredMethods())
				if (!candidate.isBridge() && !candidate.isSynthetic() && candidate.getName().equals(method.getName())
						&& Modifier.isPublic(candidate.getModifiers()) && !Modifier.isStatic(candidate.getModifiers())
						&& candidate.getParameterCount() == 1 && candidate.getReturnType() == method.getReturnType()
						&& candidate.getParameterTypes()[0] != method.getParameterTypes()[0]
						&& method.getParameterTypes()[0].isAssignableFrom(candidate.getParameterTypes()[0]))
					return Optional.empty();
			return Optional.of(inherited);
		}
		return Optional.empty();
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
		this.failOnUnmappedColumns = builder.failOnUnmappedColumns;
		this.detachLobValues = builder.detachLobValues;
		this.schemaSignatureByResultSetMetaData = new WeakHashMap<>();
		this.schemaSignatureByResultSetMetaDataLock = new ReentrantLock();
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
					columnValue = resources.nativeValue(columnValue, statementContext.getDatabaseDialect(),
							resultSetMetaData.getColumnType(i), "column '" + rawLabel + "'");

				row.put(label, columnValue);
			}

			return row;
		} catch (Throwable failure) {
			mappingFailure = failure;
			throw failure;
		} finally {
			releaseMappingResources(resources, mappingFailure);
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
					Object val = b.converter.convert(this, raw, resultSet, statementContext, instanceProvider, resources);

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
					Object val = b.converter.convert(this, raw, resultSet, statementContext, instanceProvider, resources);

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
				releaseMappingResources(resources, mappingFailure);
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

		ReentrantLock lock = getSchemaSignatureByResultSetMetaDataLock();
		DatabaseDialect dialect = statementContext.getDatabaseDialect();
		WeakIdentityKey<ResultSet> lookup = new WeakIdentityKey<>(resultSet, null);
		WeakIdentityKey<DatabaseDialect> dialectKey = new WeakIdentityKey<>(dialect, null);
		lock.lock();
		try {
			clearExpiredResultSetSignatures();
			Map<WeakIdentityKey<DatabaseDialect>, String> signatures = this.schemaSignaturesByResultSet.get(lookup);
			if (signatures != null) signatures.keySet().removeIf(key -> key.refersTo(null));
			String signature = signatures == null ? null : signatures.get(dialectKey);
			if (signature != null) return signature;
		} finally {
			lock.unlock();
		}

		String signature = schemaSignatureFor(statementContext, resultSetMetaData);
		lock.lock();
		try {
			clearExpiredResultSetSignatures();
			Map<WeakIdentityKey<DatabaseDialect>, String> signatures = this.schemaSignaturesByResultSet.computeIfAbsent(
					new WeakIdentityKey<>(resultSet, this.schemaSignatureResultSets), ignored -> new HashMap<>());
			String existing = signatures.putIfAbsent(dialectKey, signature);
			return existing == null ? signature : existing;
		} finally {
			lock.unlock();
		}
	}

	private void clearExpiredResultSetSignatures() {
		Reference<? extends ResultSet> expired;
		while ((expired = this.schemaSignatureResultSets.poll()) != null)
			this.schemaSignaturesByResultSet.remove(expired);
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

		// A whole-row custom mapper may manually supply these shapes, including an empty outer
		// fetch for SQL NULL. Automatic Optional construction remains unsupported.
		if (resultClass == Optional.class || resultClass.isArray())
			rejectOptionalMappingTargets(targetType);

		// Target-aware JDBC readers preserve driver getter behavior. All detached values then
		// pass through the same built-in conversion engine used by bean/record bindings.
		Class<?> boxed = boxedClass(resultClass);
		if (boxed == Byte.class || boxed == Short.class || boxed == Integer.class || boxed == Long.class
				|| boxed == Float.class || boxed == Double.class || boxed == Number.class) {
			value = getNullableNumber(resultSet, 1, resultClass);
		} else if (boxed == Boolean.class) {
			// Read the original value so scalar text follows the same strict rule as row/container targets.
			value = resultSet.getObject(1);
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
		} else if (resultClass == Date.class || resultClass == Timestamp.class || resultClass == java.sql.Date.class) {
			// Preserve the driver's java.time representation until the legacy target's range is checked.
			int jdbcType = resultSetMetaData.getColumnType(1);
			boolean withZone = statementContext.getDatabaseDialect().isTimestampWithTimeZone(resultSetMetaData, 1);
			if (jdbcType == Types.DATE || jdbcType == Types.TIMESTAMP || withZone)
				value = readColumn(columnReaderFor(resultSetMetaData, statementContext, 1), resultSet, statementContext, 1, resources);
			else if (resultClass == java.sql.Date.class)
				value = TemporalReaders.asLocalDate(resultSet, 1, statementContext, resultSetMetaData);
			else if (resultClass == Date.class)
				value = TemporalReaders.asInstant(resultSet, 1, statementContext, resultSetMetaData);
			else
				value = statementContext.getDatabaseDialect() instanceof SqliteDialect
						? TemporalReaders.readSqliteTimestamp(resultSet, statementContext, 1)
						: TemporalReaders.asLocalDateTime(resultSet, 1, statementContext, false);
		} else if (resultClass == Instant.class || resultClass == ZonedDateTime.class) {
			value = TemporalReaders.asInstant(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == LocalDate.class) {
			value = TemporalReaders.asLocalDate(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == LocalTime.class) {
			value = TemporalReaders.asLocalTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == LocalDateTime.class) {
			value = TemporalReaders.asLocalDateTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == OffsetTime.class) {
			value = TemporalReaders.asOffsetTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == OffsetDateTime.class) {
			value = TemporalReaders.asOffsetDateTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == java.sql.Time.class) {
			value = TemporalReaders.asLocalTime(resultSet, 1, statementContext, resultSetMetaData);
		} else if (resultClass == ZoneId.class || resultClass == ZoneOffset.class || resultClass == TimeZone.class || resultClass == Locale.class
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

		if (standardType && resultClass == Object.class && this.detachLobValues)
			value = resources.detachLobs(value, resources.scalarJdbcType(resultSet, resultSetMetaData), "scalar Object value");
		if (standardType && ((resultClass == Object.class && !this.detachLobValues) || resultClass == java.sql.Blob.class
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
		if (resultSetMetaData.getColumnCount() == 1 && (resultSetRowType == Duration.class
				|| resultSetRowType == Period.class || resultSetRowType == MonthDay.class))
			return new DatabaseException(format(
					"No built-in scalar mapping supports %s. Selected column: %s. "
							+ "Register a CustomColumnMapper through ResultSetMapper.withCustomColumnMappers(...), or provide a custom ResultSetMapper.",
					resultSetRowType.getName(), columnLabels));
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

		Map<String, Set<String>> aliasesByPropertyName = new HashMap<>();
		Map<String, Field> aliasFieldsByPropertyName = new HashMap<>();
		Set<String> annotatedFieldNames = new HashSet<>();
		Set<String> writableNames = new HashSet<>();
		if (!resultClass.isRecord())
			for (WritableProperty property : determineWritableProperties(resultClass))
				writableNames.add(property.getName());

		for (Class<?> current = resultClass; current != null && current != Object.class; current = current.getSuperclass()) {
			Field[] fields = current.getDeclaredFields();
			Map<String, Field> fieldsInClass = new HashMap<>();
			Arrays.sort(fields, (left, right) -> left.getName().compareTo(right.getName()));
			for (Field field : fields) {
				DatabaseColumn databaseColumn = field.getAnnotation(DatabaseColumn.class);
				if (databaseColumn == null) continue;
				// An annotated subclass field with the same spelling already supplied its metadata.
				// Skip the hidden ancestor before resolving it against newly introduced setters.
				if (!annotatedFieldNames.add(field.getName())) continue;

				// Record-component annotations also appear on their backing fields. Preserve those names;
				// bean fields instead name a writable property, which need not have the field's spelling.
				String propertyName = resultClass.isRecord() ? field.getName()
						: writablePropertyNameForAnnotatedField(resultClass, field, writableNames);
				Field previousInClass = fieldsInClass.putIfAbsent(propertyName, field);
				if (previousInClass != null)
					throw new DatabaseException(format("@DatabaseColumn fields '%s.%s' and '%s.%s' both identify writable property '%s' on %s",
							current.getName(), previousInClass.getName(), current.getName(), field.getName(), propertyName, resultClass.getName()));
				// The nearest annotated declaration overrides an ancestor's aliases, as before.
				if (aliasFieldsByPropertyName.putIfAbsent(propertyName, field) != null) continue;
				aliasesByPropertyName.put(propertyName, unmodifiableSet(asList(databaseColumn.value()).stream()
						.map(this::normalizeColumnLabel).collect(toSet())));
			}
		}

		return unmodifiableMap(aliasesByPropertyName);
	}

	@NonNull
	private String writablePropertyNameForAnnotatedField(Class<?> resultClass, Field field, Set<String> writableNames) {
		if (Modifier.isStatic(field.getModifiers()))
			throw new DatabaseException(format("@DatabaseColumn field '%s.%s' is static; annotations must identify writable instance properties",
					field.getDeclaringClass().getName(), field.getName()));

		String fieldName = field.getName();
		Set<String> candidates = new java.util.TreeSet<>();
		if (writableNames.contains(fieldName)) candidates.add(fieldName);
		// A field xCoordinate conventionally uses setXCoordinate, whose JavaBeans name is
		// XCoordinate because the first two letters are uppercase. Avoid java.desktop introspection.
		String generatedName = decapitalizeJavaBeansPropertyName(Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1));
		if (writableNames.contains(generatedName)) candidates.add(generatedName);
		// Lombok's primitive-boolean isActive field uses setActive. Boxed Boolean fields keep isActive.
		if (field.getType() == boolean.class && fieldName.length() > 2 && fieldName.startsWith("is")
				&& Character.isUpperCase(fieldName.charAt(2))) {
			String booleanName = decapitalizeJavaBeansPropertyName(fieldName.substring(2));
			if (writableNames.contains(booleanName)) candidates.add(booleanName);
		}
		if (candidates.isEmpty())
			throw new DatabaseException(format("@DatabaseColumn field '%s.%s' has no writable JavaBean property on %s; provide a public instance setter",
					field.getDeclaringClass().getName(), fieldName, resultClass.getName()));
		if (candidates.size() > 1)
			throw new DatabaseException(format("@DatabaseColumn field '%s.%s' ambiguously identifies writable properties %s on %s",
					field.getDeclaringClass().getName(), fieldName, candidates, resultClass.getName()));
		return candidates.iterator().next();
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
			releaseMappingResources(resources, mappingFailure);
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
			return (r, c, i) -> c.getDatabaseDialect() instanceof SqliteDialect
					? TemporalReaders.readSqliteTimestamp(r, c, i) : TemporalReaders.asLocalDateTime(r, i, c, false);
		if (jdbcType == Types.DATE)
			return (r, c, i) -> TemporalReaders.asLocalDate(r, i, c);
		if (TemporalReaders.isTimeWithTimeZone(metadata, column))
			return (r, c, i) -> TemporalReaders.asOffsetTime(r, i, c, true);
		if (jdbcType == Types.TIME)
			return (r, c, i) -> TemporalReaders.asLocalTime(r, i);
		return (r, c, i) -> r.getObject(i);
	}

	@Nullable
	private Object convertColumn(@Nullable Object raw, TargetType target, List<CustomColumnMapper> mappers,
			ResultSet resultSet, StatementContext<?> context, InstanceProvider instances,
			MappingResources resources, int column, String label, int jdbcType, @Nullable String containerTypeName,
			boolean deferredContainerType) {
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
			// An applicable whole-shape hook may construct inner Optionals itself. Declining it
			// must not re-enable unsupported automatic mapping, including for empty containers.
			rejectOptionalMappingTargets(target);
		}
		Object value = MappingResources.requiresMaterialization(raw, target.getRawClass(), jdbcType)
				? resources.materialize(raw, target.getRawClass(), jdbcType, "column '" + label + "'") : raw;
		if (this.detachLobValues && target.getRawClass() == Object.class)
			value = resources.detachLobs(value, jdbcType, "column '" + label + "'");
		if (context.getDatabaseDialect() instanceof DuckDbDialect duck
				&& (requiresRecursiveContainerConversion(target) || target.isArray())) {
			if (deferredContainerType) {
				try {
					containerTypeName = optionalContainerTypeName(resultSet.getMetaData(), column);
				} catch (SQLException failure) {
					throw DatabaseException.wrapSafely("Unable to determine typed SQL container declaration", failure,
							DatabaseDialect.forExceptionCause(failure));
				}
			}
			duck.registerContainerTypes(value, containerTypeName, resources);
		}
		return convertResultSetValueToTargetType(context, value, target, resources);
	}

	private static @Nullable String optionalContainerTypeName(ResultSetMetaData metadata, int column) throws SQLException {
		try {
			return metadata.getColumnTypeName(column);
		} catch (SQLException | UnsupportedOperationException | AbstractMethodError unsupportedMetadata) {
			if (JdbcFallbacks.isTerminal(unsupportedMetadata)) throw unsupportedMetadata;
			// Most conversions need no SQL declaration. Ambiguous legacy timestamps fail later.
			return null;
		}
	}

	/** Convenience entry point for a detached value; row execution shares one resource scope. */
	@NonNull
	protected <T> Optional<Object> convertResultSetValueToTargetType(StatementContext<T> context,
			@Nullable Object value, TargetType target) {
		rejectOptionalMappingTargets(target);
		MappingResources resources = acquireMappingResources();
		Throwable mappingFailure = null;
		try {
			return Optional.ofNullable(convertResultSetValueToTargetType(context, value, target, resources));
		} catch (Throwable failure) {
			mappingFailure = failure;
			throw failure;
		} finally {
			try {
				releaseMappingResources(resources, mappingFailure);
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
		targetType = builtInTargetType(targetType);

		if (resultSetValue == null)
			return null;

		// All built-in dialects preserve these exact base scalar values. Column execution has
		// already run custom hooks and validated JDBC-identified detached LOBs before this call.
		if (isIdentityScalar(resultSetValue, boxedClass(targetType.getRawClass())))
			return resultSetValue;

		if (MappingResources.requiresMaterialization(resultSetValue, targetType.getRawClass(), Types.OTHER))
			resultSetValue = resources.materialize(resultSetValue, targetType.getRawClass(), Types.OTHER, "target " + targetType.getRawClass().getTypeName());
		boolean recursiveContainer = requiresRecursiveContainerConversion(targetType);
		if (recursiveContainer || targetType.isArray())
			validateRecursiveMapTargets(targetType, false, new HashSet<>());
		DatabaseDialect dialect = statementContext.getDatabaseDialect();
		boolean nativeTarget = targetType.getRawClass() == Object.class
				|| (Map.class.isAssignableFrom(targetType.getRawClass()) && !recursiveContainer)
				|| (this.detachLobValues && (targetType.isList() || targetType.isSet()) && !recursiveContainer);
		if ((targetType.getRawClass() == Object.class || (targetType.getRawClass().isInstance(resultSetValue) && !recursiveContainer))
				&& !(this.detachLobValues && (nativeTarget || targetType.isArray())))
			resources.borrow(resultSetValue);
		if (recursiveContainer) borrowNativeContainerParts(resultSetValue, targetType, resources);
		Object beforeUnwrap = resultSetValue;
		resultSetValue = nativeTarget ? resources.nativeValue(resultSetValue, dialect)
				: dialect instanceof DuckDbDialect duck ? duck.unwrapForTypedMapping(resultSetValue, resources)
				: dialect.unwrapResultSetValue(resultSetValue);

		resources.copyArrayType(beforeUnwrap, resultSetValue);
		return convertDetachedValue(statementContext, resultSetValue, targetType, resources);
	}

	/** Pure built-in conversion also serves scalar JDBC readers without a statement context. */
	@Nullable
	private <T> Object convertDetachedValue(StatementContext<T> statementContext, @Nullable Object resultSetValue,
			TargetType targetType, MappingResources resources) {
		if (resultSetValue == null) return null;
		Class<?> targetClass = boxedClass(targetType.getRawClass());
		// Keep this shared identity path small enough to inline. Floating-point conversions still
		// undergo overflow checks; containers still undergo recursive element conversion.
		if (isIdentityScalar(resultSetValue, targetClass)) return resultSetValue;
		return convertDetachedValueSlow(statementContext, resultSetValue, targetType, resources);
	}

	private static boolean isIdentityScalar(Object value, Class<?> targetClass) {
		return value.getClass() == targetClass && (targetClass == String.class
				|| targetClass == Integer.class || targetClass == Long.class || targetClass == Short.class
				|| targetClass == Byte.class || targetClass == Boolean.class || targetClass == Character.class
				|| targetClass == BigInteger.class || targetClass == BigDecimal.class || targetClass == byte[].class);
	}

	/** Object-valued containers retain the native mapping contract; declared types require recursion. */
	private static boolean requiresRecursiveContainerConversion(TargetType targetType) {
		targetType = builtInTargetType(targetType);
		if (Map.class.isAssignableFrom(targetType.getRawClass())) {
			MapTargetTypes types = mapTargetTypes(targetType);
			return types.key.getRawClass() != Object.class || types.value.getRawClass() != Object.class;
		}
		if (targetType.isList())
			return targetType.getListElementType().map(element -> element.getRawClass() != Object.class).orElse(false);
		if (targetType.isSet())
			return targetType.getSetElementType().map(element -> element.getRawClass() != Object.class).orElse(false);
		return false;
	}

	private record MapTargetTypes(TargetType key, TargetType value) {}

	/** Use parameterized upper bounds for built-ins while custom hooks retain the declared type. */
	private static TargetType builtInTargetType(TargetType target) {
		Type type = target.getType();
		if (!(type instanceof TypeVariable<?> || type instanceof WildcardType)) return target;
		Set<Type> resolving = new HashSet<>();
		while (resolving.add(type)) {
			Type[] bounds = type instanceof TypeVariable<?> variable ? variable.getBounds()
					: type instanceof WildcardType wildcard ? wildcard.getUpperBounds() : null;
			if (bounds == null) break;
			type = bounds.length == 0 ? Object.class : bounds[0];
		}
		return TargetType.of(type);
	}

	/** Validate selected declared shapes without retaining reflective paths in a row plan. */
	private static void rejectOptionalMappingTargets(TargetType target) {
		Class<?> raw = builtInTargetType(target).getRawClass();
		if (raw.isArray() && raw.getComponentType().isPrimitive()) return;
		if (raw != Optional.class && !raw.isArray() && !Map.class.isAssignableFrom(raw)
				&& !Collection.class.isAssignableFrom(raw) && !raw.isRecord()) return;
		rejectOptionalMappingTargets(target, new HashSet<>(), new HashSet<>());
	}

	private static void rejectOptionalMappingTargets(TargetType target, Set<Type> path, Set<Class<?>> recordPath) {
		target = builtInTargetType(target);
		Class<?> raw = target.getRawClass();
		if (raw == Optional.class)
			throw new DatabaseException(format("Default result mapping does not support Optional-valued targets (%s). "
					+ "Declare the nullable underlying type or use a custom ResultSetMapper; a CustomColumnMapper cannot supply an Optional for a NULL Optional-valued property.",
					target.getType().getTypeName()));
		if (!path.add(target.getType())) return;
		try {
			if (target.isArray()) {
				target.getArrayComponentType().ifPresent(component -> rejectOptionalMappingTargets(component, path, recordPath));
			} else if (Map.class.isAssignableFrom(raw)) {
				MapTargetTypes types = mapTargetTypes(target);
				rejectOptionalMappingTargets(types.key, path, recordPath);
				rejectOptionalMappingTargets(types.value, path, recordPath);
			} else if (Collection.class.isAssignableFrom(raw)) {
				Type element = GenericPropertyTypes.resolve(target.getType(), Collection.class, Collection.class.getTypeParameters()[0]);
				rejectOptionalMappingTargets(TargetType.of(element), path, recordPath);
			} else if (raw.isRecord()) {
				for (TargetType argument : target.getTypeArguments())
					rejectOptionalMappingTargets(argument, path, recordPath);
				if (recordPath.add(raw)) {
					try {
						for (RecordComponent component : raw.getRecordComponents())
							rejectOptionalMappingTargets(TargetType.of(GenericPropertyTypes.resolve(target.getType(), raw,
									component.getGenericType())), path, recordPath);
					} finally {
						recordPath.remove(raw);
					}
				}
			}
		} finally {
			path.remove(target.getType());
		}
	}

	/** Validate the declared Map tree even when an outer container is empty. */
	private static void validateRecursiveMapTargets(TargetType target, boolean withinMap, Set<Type> path) {
		target = builtInTargetType(target);
		if (!path.add(target.getType())) return;
		try {
			if (Map.class.isAssignableFrom(target.getRawClass()) && requiresRecursiveContainerConversion(target)) {
				if (!target.getRawClass().isAssignableFrom(LinkedHashMap.class))
					throw new DatabaseException(format("Typed SQL MAP target %s cannot receive an insertion-ordered LinkedHashMap; "
							+ "declare Map<K, V>, HashMap<K, V>, LinkedHashMap<K, V>, or use a custom column mapper.",
							target.getType().getTypeName()));
				MapTargetTypes types = mapTargetTypes(target);
				validateRecursiveMapTargets(types.key, true, path);
				validateRecursiveMapTargets(types.value, true, path);
			} else if (target.isList() || target.isSet() || target.isArray()) {
				TargetType element = (target.isList() ? target.getListElementType() : target.isSet()
						? target.getSetElementType() : target.getArrayComponentType()).orElse(TargetType.of(Object.class));
				validateRecursiveMapTargets(element, withinMap, path);
			} else if (withinMap && Collection.class.isAssignableFrom(target.getRawClass())) {
				TargetType element = TargetType.of(GenericPropertyTypes.resolve(target.getType(), Collection.class,
						Collection.class.getTypeParameters()[0]));
				if (element.getRawClass() != Object.class)
					throw new DatabaseException(format("Typed collection target %s inside SQL MAP is unsupported; "
							+ "declare List<E>, Set<E>, an array, or use a custom column mapper.", target.getType().getTypeName()));
			}
		} finally {
			path.remove(target.getType());
		}
	}

	/** Borrow native branches before typed siblings can materialize or release shared JDBC handles. */
	private static void borrowNativeContainerParts(@Nullable Object value, TargetType target, MappingResources resources) {
		if (value == null) return;
		target = builtInTargetType(target);
		if (target.getRawClass() == Object.class
				|| (Map.class.isAssignableFrom(target.getRawClass()) && !requiresRecursiveContainerConversion(target))) {
			if (!resources.detachesLobValues()) resources.borrow(value);
			return;
		}
		if (value instanceof Map<?, ?> entries && Map.class.isAssignableFrom(target.getRawClass())) {
			MapTargetTypes types = mapTargetTypes(target);
			for (Map.Entry<?, ?> entry : entries.entrySet()) {
				borrowNativeContainerParts(entry.getKey(), types.key, resources);
				borrowNativeContainerParts(entry.getValue(), types.value, resources);
			}
		} else if (target.isList() || target.isSet() || target.isArray()) {
			TargetType element = (target.isList() ? target.getListElementType() : target.isSet()
					? target.getSetElementType() : target.getArrayComponentType()).orElse(TargetType.of(Object.class));
			if (value instanceof Object[] elements) {
				for (Object item : elements) borrowNativeContainerParts(item, element, resources);
			} else if (value instanceof Collection<?> elements) {
				for (Object item : elements) borrowNativeContainerParts(item, element, resources);
			}
		}
	}

	private static MapTargetTypes mapTargetTypes(TargetType targetType) {
		targetType = builtInTargetType(targetType);
		Type[] variables = Map.class.getTypeParameters();
		return new MapTargetTypes(
				TargetType.of(GenericPropertyTypes.resolve(targetType.getType(), Map.class, variables[0])),
				TargetType.of(GenericPropertyTypes.resolve(targetType.getType(), Map.class, variables[1])));
	}

	private <T> Object convertCollectionResultSetValue(StatementContext<T> context, Collection<?> source,
			TargetType targetType, MappingResources resources) {
		TargetType elementType = (targetType.isList() ? targetType.getListElementType()
				: targetType.getSetElementType()).orElse(TargetType.of(Object.class));
		Collection<Object> result = targetType.isList() ? new ArrayList<>(source.size()) : new LinkedHashSet<>(source.size());
		int elementJdbcType = resources.arrayType(source);
		int index = 0;
		for (Object value : source)
			result.add(convertResultSetArrayElement(context, value, elementType, index++, resources, elementJdbcType));
		return result;
	}

	private <T> Object convertMapResultSetValue(StatementContext<T> context, Map<?, ?> source,
			TargetType targetType, MappingResources resources) {
		MapTargetTypes types = mapTargetTypes(targetType);
		Map<Object, Object> result = new LinkedHashMap<>(source.size());
		int index = 0;
		for (Map.Entry<?, ?> entry : source.entrySet()) {
			Object key = convertMapEntryPart(context, entry.getKey(), types.key, source, index, true, resources);
			if (result.containsKey(key))
				throw new DatabaseException(format("SQL MAP entry %s produces a duplicate key after conversion to %s",
						index, types.key.getType().getTypeName()));
			Object value = convertMapEntryPart(context, entry.getValue(), types.value, source, index, false, resources);
			result.put(key, value);
			index++;
		}
		return result;
	}

	private <T> @Nullable Object convertMapEntryPart(StatementContext<T> context, @Nullable Object value,
			TargetType target, Map<?, ?> source, int index, boolean key, MappingResources resources) {
		if (value == null) return null;
		if (value instanceof Timestamp && isTimestampConversionTarget(target.getRawClass())
				&& context.getDatabaseDialect() instanceof DuckDbDialect duck) {
			String declaration = resources.containerType(source);
			int jdbcType = duck.mapEntryJdbcType(declaration, key ? 0 : 1);
			if (!key && jdbcType == Types.OTHER) jdbcType = duck.structAttributeJdbcType(declaration, index);
			if (jdbcType == Types.OTHER)
				throw new DatabaseException(format("DuckDB SQL MAP entry %s %s requires declared TIMESTAMP or TIMESTAMPTZ "
						+ "metadata to map legacy java.sql.Timestamp to %s", index, key ? "key" : "value", target.getType().getTypeName()));
			value = normalizeContainerTemporalValue(context, value, jdbcType);
		}
		Object converted = convertResultSetValueToTargetType(context, value, target, resources);
		if (converted != null && !boxedClass(target.getRawClass()).isInstance(converted))
			throw new DatabaseException(format("SQL MAP entry %s %s of type %s cannot be mapped to %s",
					index, key ? "key" : "value", value.getClass().getName(), target.getType().getTypeName()));
		return converted;
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

		if (resultSetValue instanceof Collection<?> values && (targetType.isList() || targetType.isSet())
				&& requiresRecursiveContainerConversion(targetType))
			return convertCollectionResultSetValue(statementContext, values, targetType, resources);

		if (resultSetValue instanceof Map<?, ?> values && Map.class.isAssignableFrom(propertyType)
				&& requiresRecursiveContainerConversion(targetType))
			return convertMapResultSetValue(statementContext, values, targetType, resources);

		// STRUCT-shaped values: dialects that surface SQL STRUCT columns as insertion-ordered
		// attribute-name -> value maps (for example DuckDB - see DuckDbDialect#unwrapResultSetValue)
		// map directly to record targets, recursively converting each attribute to its component type.
		if (resultSetValue instanceof Map<?, ?> mapValue && propertyType.isRecord())
			return convertMapResultSetValueToRecord(statementContext, mapValue, targetType, STRUCT_RECORD_INSTANCE_PROVIDER, resources);

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
		if (targetClass != Object.class) {
			int infinity = postgresInfinityDirection(statementContext, resultSetValue);
			if (infinity != 0) {
				if (Date.class.isAssignableFrom(targetClass)) throw legacyTemporalRangeFailure(targetClass);
				resultSetValue = resultSetValue instanceof Timestamp
						? (infinity > 0 ? LocalDateTime.MAX : LocalDateTime.MIN)
						: (infinity > 0 ? LocalDate.MAX : LocalDate.MIN);
			}
			if (Date.class.isAssignableFrom(targetClass) && isUnboundedTemporal(resultSetValue))
				throw legacyTemporalRangeFailure(targetClass);
			if (Date.class.isAssignableFrom(targetClass)) {
				Instant instant = resultSetValue instanceof Instant value ? value
						: resultSetValue instanceof OffsetDateTime value ? value.toInstant()
						: resultSetValue instanceof ZonedDateTime value ? value.toInstant() : null;
				if (instant != null) {
					try { instant.toEpochMilli(); }
					catch (ArithmeticException exception) { throw legacyTemporalRangeFailure(targetClass); }
				}
			}
		}

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
				return checkedFloat(number);
			if (Double.class.isAssignableFrom(targetClass))
				return checkedDouble(number);
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
				return jdbcDateFromLocalDate(timestamp.toLocalDateTime().toLocalDate());
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
				return localTimeWithMilliseconds(time);
		}

		// New java.time values (preferred)
		if (resultSetValue instanceof Instant instant) {
			if (Instant.class.isAssignableFrom(targetClass))
				return instant;
			if (java.sql.Timestamp.class.isAssignableFrom(targetClass))
				return jdbcTimestampFromInstant(instant);
			if (java.sql.Date.class.isAssignableFrom(targetClass))
				return jdbcDateFromLocalDate(instant.atZone(statementContext.getTimeZone()).toLocalDate());
			if (Date.class.isAssignableFrom(targetClass))
				return legacyDateFromInstant(instant);
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
				return jdbcTimestampFromInstant(localDateTime.atZone(statementContext.getTimeZone()).toInstant());
			if (java.sql.Date.class.isAssignableFrom(targetClass))
				return jdbcDateFromLocalDate(localDateTime.toLocalDate());
			if (Date.class.isAssignableFrom(targetClass))
				return legacyDateFromInstant(localDateTime.atZone(statementContext.getTimeZone()).toInstant());
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
				return jdbcTimestampFromInstant(offsetDateTime.toInstant());
			if (java.sql.Date.class.isAssignableFrom(targetClass))
				return jdbcDateFromLocalDate(offsetDateTime.atZoneSameInstant(statementContext.getTimeZone()).toLocalDate());
			if (Date.class.isAssignableFrom(targetClass))
				return legacyDateFromInstant(offsetDateTime.toInstant());
			if (ZonedDateTime.class.isAssignableFrom(targetClass))
				return offsetDateTime.atZoneSameInstant(statementContext.getTimeZone());
		}

		if (resultSetValue instanceof ZonedDateTime zonedDateTime)
			return convertDetachedValue(statementContext, zonedDateTime.toOffsetDateTime(), targetType, resources);

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
				return jdbcDateFromLocalDate(localDate);
			if (java.sql.Timestamp.class.isAssignableFrom(targetClass))
				return jdbcTimestampFromInstant(localDate.atStartOfDay(statementContext.getTimeZone()).toInstant());
			if (OffsetDateTime.class.isAssignableFrom(targetClass))
				return localDate.atStartOfDay(statementContext.getTimeZone()).toOffsetDateTime();
			if (Date.class.isAssignableFrom(targetClass))
				return legacyDateFromInstant(localDate.atStartOfDay(statementContext.getTimeZone()).toInstant());
		}

		if (resultSetValue instanceof LocalTime localTime) {
			if (LocalTime.class.isAssignableFrom(targetClass))
				return localTime;

			if (OffsetTime.class.isAssignableFrom(targetClass))
				return offsetTimeFromLocalTime(localTime, statementContext.getTimeZone());

			if (java.sql.Time.class.isAssignableFrom(targetClass))
				return jdbcTimeWithMilliseconds(localTime);
		}

		if (resultSetValue instanceof OffsetTime offsetTime) {
			if (OffsetTime.class.isAssignableFrom(targetClass))
				return offsetTime;
			if (LocalTime.class.isAssignableFrom(targetClass))
				return offsetTime.toLocalTime();
			if (java.sql.Time.class.isAssignableFrom(targetClass))
				return jdbcTimeWithMilliseconds(offsetTime.toLocalTime());
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

			if (OffsetTime.class.isAssignableFrom(targetClass)) {
				try {
					return offsetTimeFromString(string, statementContext.getTimeZone());
				} catch (DateTimeException | IllegalArgumentException exception) {
					throw new DatabaseException("Cannot map text to OffsetTime; expected a valid local or explicit-offset time", exception);
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
		} else if (ZoneOffset.class.isAssignableFrom(targetClass)) {
			return zoneOffsetFromValue(resultSetValue);
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

		if (Boolean.class.isAssignableFrom(targetClass))
			return booleanFromValue(resultSetValue);

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
			Object values = extractSqlArrayResultSetValue(sqlArray, resources,
					statementContext.getDatabaseDialect() instanceof DuckDbDialect || !resources.isBorrowed(sqlArray));
			if (statementContext.getDatabaseDialect() instanceof DuckDbDialect duck)
				duck.registerContainerTypes(values, resources.containerType(sqlArray), resources);
			return convertArrayResultSetValue(statementContext, values, targetType, resources);
		} catch (SQLException e) {
			throw DatabaseException.wrapSafely("Unable to extract JDBC Array value", e, DatabaseDialect.forExceptionCause(e));
		}
	}

	@Nullable
	static Object extractSqlArrayResultSetValue(@NonNull Array sqlArray, MappingResources resources) throws SQLException {
		return extractSqlArrayResultSetValue(sqlArray, resources, true);
	}

	@Nullable
	private static Object extractSqlArrayResultSetValue(@NonNull Array sqlArray, MappingResources resources,
			boolean free) throws SQLException {
		requireNonNull(sqlArray);
		MappingResources.SqlArrayExtraction previous = resources.sqlArrayExtraction(sqlArray);
		if (previous != null) {
			if (previous.failure() != null) rethrowArrayFailure(previous.failure());
			return previous.values();
		}
		Object values = null;
		Throwable thrown = null;

		try {
			values = sqlArray.getArray();
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
		} catch (SQLException | RuntimeException | Error e) {
			thrown = e;
		} finally {
			if (free) {
				try {
					sqlArray.free();
				} catch (SQLException | RuntimeException | Error e) {
					if (thrown == null) thrown = e;
					else if (thrown != e) thrown.addSuppressed(e);
				}
			}
		}
		resources.rememberSqlArrayExtraction(sqlArray, values, thrown);
		if (thrown != null) rethrowArrayFailure(thrown);
		return values;
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
		targetType = builtInTargetType(targetType);

		if (value == null)
			return null;

		Class<?> targetClass = boxedClass(targetType.getRawClass());

		if (targetClass == Object.class)
			return resources.nativeValue(value, statementContext.getDatabaseDialect(), jdbcType, "SQL ARRAY element " + elementIndex);
		// A driver may already expose a CLOB/BLOB element as String/byte[]. Preserve the
		// configured typed-LOB bound before the matching-value identity fast path returns it.
		if (MappingResources.requiresMaterialization(value, targetClass, jdbcType))
			value = resources.materialize(value, targetClass, jdbcType, "SQL ARRAY element " + elementIndex);
		value = normalizeContainerTemporalValue(statementContext, value, jdbcType);
		if (value.getClass().isArray()) resources.registerArrayType(value, jdbcType);
		if (targetClass.isInstance(value) && !requiresRecursiveContainerConversion(targetType)
				&& !(value.getClass().isArray() && (targetType.isArray() || targetType.isList() || targetType.isSet()))) {
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

	private static Object normalizeContainerTemporalValue(StatementContext<?> context, Object value, int jdbcType) {
		int infinity = postgresInfinityDirection(context, value);
		if (infinity != 0) {
			if (value instanceof Timestamp) {
				if (jdbcType == Types.TIMESTAMP_WITH_TIMEZONE)
					return infinity > 0 ? OffsetDateTime.MAX : OffsetDateTime.MIN;
				return infinity > 0 ? LocalDateTime.MAX : LocalDateTime.MIN;
			}
			return infinity > 0 ? LocalDate.MAX : LocalDate.MIN;
		}
		// A Timestamp stores a JVM-zone-based epoch even for SQL TIMESTAMP WITHOUT TIME ZONE.
		// Preserve the SQL container's type so only zone-less values become wall-clock values.
		if (value instanceof Timestamp timestamp) {
			if (jdbcType == Types.TIMESTAMP) value = timestamp.toLocalDateTime();
			else if (jdbcType == Types.TIMESTAMP_WITH_TIMEZONE) value = timestamp.toInstant();
		} else if (value instanceof java.sql.Date date) {
			value = date.toLocalDate();
		} else if (value instanceof java.sql.Time time && jdbcType == Types.TIME) {
			value = localTimeWithMilliseconds(time);
		}
		return value;
	}

	private static int postgresInfinityDirection(StatementContext<?> context, Object value) {
		if (!(context.getDatabaseDialect() instanceof PostgresDialect) || !(value instanceof Date date)) return 0;
		// Constants are inlined; loading this mapper does not require the optional PostgreSQL driver.
		if (date.getTime() == org.postgresql.PGStatement.DATE_POSITIVE_INFINITY) return 1;
		if (date.getTime() == org.postgresql.PGStatement.DATE_NEGATIVE_INFINITY) return -1;
		return 0;
	}

	private static boolean isUnboundedTemporal(Object value) {
		return value.equals(LocalDate.MIN) || value.equals(LocalDate.MAX)
				|| value.equals(LocalDateTime.MIN) || value.equals(LocalDateTime.MAX)
				|| value.equals(OffsetDateTime.MIN) || value.equals(OffsetDateTime.MAX)
				|| value.equals(Instant.MIN) || value.equals(Instant.MAX);
	}

	private static DatabaseException legacyTemporalRangeFailure(Class<?> target) {
		return new DatabaseException("Temporal value is outside the representable range of " + target.getTypeName());
	}

	private static Timestamp jdbcTimestampFromInstant(Instant instant) {
		try {
			long millis = instant.toEpochMilli();
			Timestamp timestamp = Timestamp.from(instant);
			// Timestamp.from can overflow internally instead of reporting its documented range error.
			if (timestamp.getTime() != millis || !timestamp.toInstant().equals(instant))
				throw legacyTemporalRangeFailure(Timestamp.class);
			return timestamp;
		} catch (ArithmeticException | IllegalArgumentException exception) {
			throw legacyTemporalRangeFailure(Timestamp.class);
		}
	}

	private static java.sql.Date jdbcDateFromLocalDate(LocalDate date) {
		// SQL Date's JDBC escape text has exactly four year digits and no era. Its legacy
		// constructor also normalizes some dates (including the Gregorian cutover gap).
		if (date.getYear() < 1 || date.getYear() > 9999) throw legacyTemporalRangeFailure(java.sql.Date.class);
		java.sql.Date converted = java.sql.Date.valueOf(date);
		if (!converted.toLocalDate().equals(date)) throw legacyTemporalRangeFailure(java.sql.Date.class);
		return converted;
	}

	private static Date legacyDateFromInstant(Instant instant) {
		try {
			return Date.from(instant);
		} catch (IllegalArgumentException exception) {
			throw legacyTemporalRangeFailure(Date.class);
		}
	}

	private static LocalTime localTimeWithMilliseconds(java.sql.Time time) {
		return time.toLocalTime().withNano((int) Math.floorMod(time.getTime(), 1000L) * 1_000_000);
	}

	private static java.sql.Time jdbcTimeWithMilliseconds(LocalTime localTime) {
		java.sql.Time time = java.sql.Time.valueOf(localTime);
		time.setTime(time.getTime() + localTime.getNano() / 1_000_000);
		return time;
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

		rejectOptionalMappingTargets(TargetType.of(resultSetRowType));
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
		T record = (T) convertMapResultSetValueToRecord(statementContext, mapValue, TargetType.of(resultSetRowType), instanceProvider, resources);
		return record;
	}

	/**
	 * Converts a SQL STRUCT value - surfaced by the dialect as an attribute-name to value map - to an instance
	 * of the record described by {@code recordTargetType}.
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
																											@NonNull TargetType recordTargetType,
																											@NonNull InstanceProvider instanceProvider, @NonNull MappingResources resources) {
		requireNonNull(statementContext);
		requireNonNull(mapValue);
		requireNonNull(recordTargetType);
		requireNonNull(instanceProvider);
		Class<?> recordClass = recordTargetType.getRawClass();

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

			// A nested record can bind its own variables (e.g. Box<UUID>). Resolving against
			// only its raw Class would erase those arguments before recursive conversion.
			TargetType targetType = TargetType.of(GenericPropertyTypes.resolve(recordTargetType.getType(),
					recordClass, recordComponent.getGenericType()));

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
				if (rawValue instanceof Timestamp && isTimestampConversionTarget(targetType.getRawClass())
						&& statementContext.getDatabaseDialect() instanceof DuckDbDialect duck)
					rawValue = normalizeContainerTemporalValue(statementContext, rawValue, duck.structAttributeJdbcType(resources.structType(mapValue),
							attributeIndexesByNormalizedName.get(attributeName)));
				Object value = rawValue == null ? null
						: convertResultSetValueToTargetType(statementContext, rawValue, targetType, resources);

				// It's considered programmer error to have a NULL attribute value and map it to a primitive (which does not support null)
				if (value == null && recordComponentType.isPrimitive())
					throw new DatabaseException(format("STRUCT attribute '%s' is NULL but record component '%s' of %s is primitive (%s). Use a non-primitive type or COALESCE/CAST in SQL.",
							attributeName, recordComponent.getName(), recordClass.getSimpleName(), recordComponentType.getSimpleName()));

				Class<?> checkedType = boxedClass(targetType.getRawClass());

				if (value != null && !checkedType.isInstance(value)) {
					throw new DatabaseException(format(
							"STRUCT attribute '%s' for record component '%s' of %s expects type %s, but the STRUCT produced %s",
							attributeName, recordComponent.getName(), recordClass, targetType.getType(), value.getClass()));
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

	private static float checkedFloat(@NonNull Number number) {
		requireNonNull(number);

		float value = number.floatValue();

		if (!Float.isFinite(value) && !isNonFiniteFloatingPoint(number))
			throw new DatabaseException(format("Numeric value '%s' cannot be represented as finite float", number));

		return value;
	}

	private static double checkedDouble(@NonNull Number number) {
		requireNonNull(number);

		double value = number.doubleValue();

		if (!Double.isFinite(value) && !isNonFiniteFloatingPoint(number))
			throw new DatabaseException(format("Numeric value '%s' cannot be represented as finite double", number));

		return value;
	}

	/** Native IEEE special values are representable; finite numbers must not overflow to infinity. */
	private static boolean isNonFiniteFloatingPoint(@NonNull Number number) {
		return number instanceof Float floatValue && !Float.isFinite(floatValue)
				|| number instanceof Double doubleValue && !Double.isFinite(doubleValue);
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

	private static final Pattern ISO_TIME_OFFSET_PREFIX = Pattern.compile("\\d{2}:\\d{2}(?::\\d{2}(?:[.,]\\d+)?)?\\s*(?:[+-]|[Zz])");
	private static final Pattern ISO_TIMESTAMP_TEXT_PREFIX = Pattern.compile("[+-]?\\d{4,}-\\d{2}-\\d{2}(?:[Tt]|\\s+)");

	/** A date-less time cannot select an offset from a zone whose rules vary by date. */
	private static OffsetTime offsetTimeFromLocalTime(LocalTime value, ZoneId timeZone) {
		if (!(timeZone.normalized() instanceof ZoneOffset offset))
			throw new DatabaseException("Cannot map a date-less local time to OffsetTime using a variable-offset database time zone; "
					+ "use LocalTime, an explicit-offset value, a fixed-offset database time zone, or a custom mapper");
		return value.atOffset(offset);
	}

	private static OffsetTime offsetTimeFromString(String value, ZoneId timeZone) {
		String trimmed = value.trim();
		try {
			return OffsetTime.parse(trimmed);
		} catch (DateTimeParseException ignored) {
			if (hasIsoTimeOffset(trimmed))
				throw new DatabaseException("Cannot map malformed explicit-offset time text to OffsetTime");
			return offsetTimeFromLocalTime(localTimeFromString(trimmed), timeZone);
		}
	}

	private static boolean hasIsoTimeOffset(String value) {
		return ISO_TIME_OFFSET_PREFIX.matcher(value.trim()).lookingAt();
	}

	private static boolean hasIsoTimestampOffset(String value) {
		String trimmed = value.trim();
		java.util.regex.Matcher separator = ISO_TIMESTAMP_TEXT_PREFIX.matcher(trimmed);
		return separator.lookingAt() && hasIsoTimeOffset(trimmed.substring(separator.end()));
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
		String normalized = trimmed.replace(' ', 'T');

		try {
			return Instant.parse(normalized);
		} catch (DateTimeParseException ignored) {
		}

		try {
			return OffsetDateTime.parse(normalized).toInstant();
		} catch (DateTimeParseException ignored) {
		}
		if (hasIsoTimestampOffset(trimmed))
			throw new DatabaseException("Cannot map malformed explicit-offset timestamp text to Instant");

		return localDateTimeFromString(trimmed).atZone(statementContext.getTimeZone()).toInstant();
	}

	@NonNull
	private static OffsetDateTime offsetDateTimeFromString(@NonNull String value,
																												 @NonNull StatementContext<?> statementContext) {
		requireNonNull(value);
		requireNonNull(statementContext);
		String trimmed = value.trim();
		String normalized = trimmed.replace(' ', 'T');

		try {
			return OffsetDateTime.parse(normalized);
		} catch (DateTimeParseException ignored) {
		}

		try {
			return Instant.parse(normalized).atZone(statementContext.getTimeZone()).toOffsetDateTime();
		} catch (DateTimeParseException ignored) {
		}
		if (hasIsoTimestampOffset(trimmed))
			throw new DatabaseException("Cannot map malformed explicit-offset timestamp text to OffsetDateTime");

		return localDateTimeFromString(trimmed).atZone(statementContext.getTimeZone()).toOffsetDateTime();
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

		String text = value.trim();
		// YearMonth.toString() omits '+' on expanded positive years, unlike its ISO parser.
		if (text.matches("[1-9][0-9]{4,8}-[0-9]{2}")) text = "+" + text;
		try {
			return YearMonth.parse(text);
		} catch (DateTimeParseException e) {
			throw new DatabaseException(format("Unable to convert value '%s' to YearMonth", value), e);
		}
	}

	private static ZoneOffset zoneOffsetFromValue(Object value) {
		try {
			ZoneId normalized = ZoneId.of(value.toString()).normalized();
			if (normalized instanceof ZoneOffset offset) return offset;
		} catch (DateTimeException failure) {
			throw new DatabaseException("Unable to convert value to ZoneOffset; a fixed zone offset is required", failure);
		}
		throw new DatabaseException("Unable to convert value to ZoneOffset; a fixed zone offset is required");
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
	 * ASCII uppercase letters are folded independently of locale before the label is lowercased using
	 * {@link #getNormalizationLocale()}. This keeps ordinary SQL/Java identifiers stable under locales such
	 * as Turkish and Azerbaijani while retaining locale-sensitive handling of non-ASCII letters.
	 *
	 * @param columnLabel the {@link ResultSet} column label to massage
	 * @return the massaged label
	 */
	@NonNull
	protected String normalizeColumnLabel(@NonNull String columnLabel) {
		requireNonNull(columnLabel);
		return lowercaseAsciiLetters(columnLabel).toLowerCase(getNormalizationLocale());
	}

	@NonNull
	private static String lowercaseAsciiLetters(@NonNull String value) {
		char[] folded = null;
		for (int index = 0; index < value.length(); index++) {
			char character = value.charAt(index);
			if (character >= 'A' && character <= 'Z') {
				if (folded == null) folded = value.toCharArray();
				folded[index] = (char) (character + ('a' - 'A'));
			}
		}
		return folded == null ? value : new String(folded);
	}

	/**
	 * Massages a JavaBean property name to match standard database column name (camelCase -> camel_case).
	 * <p>
	 * Uses {@link #normalizeColumnLabel(String)} to perform case-changing.
	 * <p>
	 * There may be multiple database column name mappings, for example property {@code address1} might map to both
	 * {@code address1} and {@code address_1} column names. The literal property spelling is also a candidate,
	 * so a quoted SQL alias such as {@code employeeId} matches the corresponding property. Digit-to-word
	 * boundaries and acronym-to-word boundaries support names such as {@code sha256_hash} and {@code url_value}.
	 *
	 * @param propertyName the JavaBean property name to massage
	 * @return the column names that match the JavaBean property name
	 */
	@NonNull
	protected Set<@NonNull String> databaseColumnNamesForPropertyName(@NonNull String propertyName) {
		requireNonNull(propertyName);

		Set<String> names = new HashSet<>(7);
		names.add(normalizeColumnLabel(propertyName));

		// Keep historical spellings, including address1/address_1 and older acronym spellings.
		String legacyCamel = CAMEL_CASE_PATTERN.matcher(propertyName).replaceAll("$1_$2");
		addColumnNameCandidates(names, legacyCamel);

		// A digit can end the previous word just as a lowercase letter can: sha256Hash -> sha256_Hash.
		String digitCamel = NUMBER_FOLLOWED_BY_CAPITAL_PATTERN.matcher(legacyCamel).replaceAll("$1_$2");
		addColumnNameCandidates(names, digitCamel);

		// Preserve the literal/legacy candidates while also supporting conventional acronym boundaries.
		String acronymCamel = ACRONYM_WORD_BOUNDARY_PATTERN.matcher(propertyName).replaceAll("$1_$2");
		acronymCamel = CAMEL_CASE_PATTERN.matcher(acronymCamel).replaceAll("$1_$2");
		acronymCamel = NUMBER_FOLLOWED_BY_CAPITAL_PATTERN.matcher(acronymCamel).replaceAll("$1_$2");
		addColumnNameCandidates(names, acronymCamel);
		return names;
	}

	private void addColumnNameCandidates(Set<String> candidates, String name) {
		candidates.add(normalizeColumnLabel(name));
		candidates.add(normalizeColumnLabel(LETTER_FOLLOWED_BY_NUMBER_PATTERN.matcher(name).replaceAll("$1_$2")));
	}

	/**
	 * The locale to use after folding ASCII letters when matching JDBC column names to JavaBean property names.
	 * <p>
	 * Used by {@link #normalizeColumnLabel(String)}.
	 *
	 * @return the locale to use for non-ASCII case normalization
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
		private static final Pattern ISO_TIMESTAMP_SEPARATOR = Pattern.compile("[+-]?\\d{4,}-\\d{2}-\\d{2}[Tt]");
		private static final String DEFAULT_SQLITE_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

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
			if (ctx.getDatabaseDialect() instanceof SqliteDialect) {
				Object value = readSqliteTimestamp(rs, ctx, col);
				if (value == null) return null;
				return value instanceof OffsetDateTime odt ? odt.toInstant()
						: ((LocalDateTime) value).atZone(ctx.getTimeZone()).toInstant();
			}
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
				int infinity = postgresInfinityDirection(ctx, ts);
				if (infinity != 0) return withTz
						? (infinity > 0 ? OffsetDateTime.MAX : OffsetDateTime.MIN).toInstant()
						: (infinity > 0 ? LocalDateTime.MAX : LocalDateTime.MIN).atZone(ctx.getTimeZone()).toInstant();
				if (withTz)
					return ts.toInstant();
				// DO NOT use ts.toInstant() for WITHOUT TZ; interpret using DB zone
				LocalDateTime ldt = ts.toLocalDateTime();
				return ldt.atZone(ctx.getTimeZone()).toInstant();
			}
			if (raw instanceof OffsetDateTime odt) return odt.toInstant();
			if (raw instanceof LocalDateTime ldt) return ldt.atZone(ctx.getTimeZone()).toInstant();
			if (raw instanceof String s) {
				try {
					return instantFromString(s, ctx);
				} catch (DateTimeException | IllegalArgumentException ignore) {
				}
			}

			// Last resort: try classic getter
			Timestamp ts = rs.getTimestamp(col);
			if (ts != null) {
				int infinity = postgresInfinityDirection(ctx, ts);
				if (infinity != 0) return withTz
						? (infinity > 0 ? OffsetDateTime.MAX : OffsetDateTime.MIN).toInstant()
						: (infinity > 0 ? LocalDateTime.MAX : LocalDateTime.MIN).atZone(ctx.getTimeZone()).toInstant();
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
			if (ctx.getDatabaseDialect() instanceof SqliteDialect) {
				Object value = readSqliteTimestamp(rs, ctx, col);
				if (value == null) return null;
				return value instanceof OffsetDateTime odt ? odt
						: ((LocalDateTime) value).atZone(ctx.getTimeZone()).toOffsetDateTime();
			}
			if (withTz) {
				OffsetDateTime got = tryGet(rs, col, OffsetDateTime.class);
				if (got != null) return got;
				int infinity = postgresInfinityDirection(ctx, rs.getObject(col));
				if (infinity != 0) return infinity > 0 ? OffsetDateTime.MAX : OffsetDateTime.MIN;

				// Try via Instant if driver only gives us Timestamp
				Instant inst = asInstant(rs, col, ctx, true);
				return inst == null ? null : inst.atOffset(ctx.getTimeZone().getRules().getOffset(inst));
			} else {
				Object raw = rs.getObject(col);
				if (raw == null) return null;
				if (raw instanceof String text) {
					try {
						return offsetDateTimeFromString(text, ctx);
					} catch (DateTimeException | IllegalArgumentException ignore) {
						// Non-ISO text may still use an explicitly configured classic JDBC format.
					}
				}
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
			if (ctx.getDatabaseDialect() instanceof SqliteDialect) {
				Object value = readSqliteTimestamp(rs, ctx, col);
				return value instanceof OffsetDateTime odt
						? odt.atZoneSameInstant(ctx.getTimeZone()).toLocalDateTime() : (LocalDateTime) value;
			}
			return asJdbcLocalDateTime(rs, col, ctx, withTz);
		}

		private static LocalDateTime asJdbcLocalDateTime(ResultSet rs, int col, StatementContext<?> ctx, boolean withTz) throws SQLException {
			if (withTz) {
				OffsetDateTime odt = tryGet(rs, col, OffsetDateTime.class);
				if (odt != null) return odt.atZoneSameInstant(ctx.getTimeZone()).toLocalDateTime();
			} else {
				LocalDateTime ldt = tryGet(rs, col, LocalDateTime.class);
				if (ldt != null) return ldt;
			}

			Object raw = rs.getObject(col);
			if (raw == null) return null;

			if (raw instanceof Timestamp ts) {
				int infinity = postgresInfinityDirection(ctx, ts);
				return infinity == 0 ? ts.toLocalDateTime() : infinity > 0 ? LocalDateTime.MAX : LocalDateTime.MIN;
			}
			if (raw instanceof OffsetDateTime odt) {
				// normalize into DB zone then drop zone for stable wall time
				return odt.atZoneSameInstant(ctx.getTimeZone()).toLocalDateTime();
			}
			if (raw instanceof Instant inst) return inst.atZone(ctx.getTimeZone()).toLocalDateTime();
			if (raw instanceof String s) {
				try {
					return OffsetDateTime.parse(s.trim().replace(' ', 'T')).atZoneSameInstant(ctx.getTimeZone()).toLocalDateTime();
				} catch (DateTimeParseException ignore) {
				}
				if (hasIsoTimestampOffset(s))
					throw new DatabaseException("Cannot map malformed explicit-offset timestamp text to LocalDateTime");
				try {
					return localDateTimeFromString(s);
				} catch (DateTimeException | IllegalArgumentException ignore) {
				}
			}

			if (withTz) {
				OffsetDateTime odt2 = tryGet(rs, col, OffsetDateTime.class);
				if (odt2 != null) return odt2.atZoneSameInstant(ctx.getTimeZone()).toLocalDateTime();
			}

			Timestamp ts = rs.getTimestamp(col);
			if (ts == null) return null;
			int infinity = postgresInfinityDirection(ctx, ts);
			return infinity == 0 ? ts.toLocalDateTime() : infinity > 0 ? LocalDateTime.MAX : LocalDateTime.MIN;
		}

		/**
		 * SQLite stores java.time timestamps as text. Its typed/classic getters can normalize invalid
		 * dates or interpret a nanosecond fraction as milliseconds, so parse standard text strictly before
		 * driver conversion. Explicit custom JDBC date formats take precedence and retain the driver's interpretation.
		 * Keep an explicit offset until the target type is known: converting through
		 * wall time would lose which occurrence of a daylight-saving overlap the value represents.
		 */
		@Nullable
		private static Object readSqliteTimestamp(ResultSet rs, StatementContext<?> ctx, int col) throws SQLException {
			Object raw = rs.getObject(col);
			if (raw == null) return null;
			if (raw instanceof String text) {
				String normalized = text.trim();
				String format = SqliteDialect.configuredDateStringFormat(rs);
				if (format != null && !DEFAULT_SQLITE_TIMESTAMP_FORMAT.equals(format))
					return asJdbcLocalDateTime(rs, col, ctx, false);
				normalized = normalized.replace(' ', 'T');
				if (!ISO_TIMESTAMP_SEPARATOR.matcher(normalized).lookingAt())
					return LocalDate.parse(normalized).atStartOfDay();
				TemporalAccessor parsed = DateTimeFormatter.ISO_DATE_TIME.parse(normalized);
				LocalDateTime local = LocalDateTime.from(parsed);
				ZoneOffset offset = parsed.query(TemporalQueries.offset());
				return offset == null ? local : local.atOffset(offset);
			}
			// Numeric timestamp storage retains JDBC semantics and the driver's date configuration.
			return asJdbcLocalDateTime(rs, col, ctx, false);
		}

		public static LocalDate asLocalDate(ResultSet rs, int col, StatementContext<?> ctx, ResultSetMetaData metadata) throws SQLException {
			if (ctx.getDatabaseDialect().isTimestampWithTimeZone(metadata, col)) {
				Instant value = asInstant(rs, col, ctx, true);
				return value == null ? null : value.atZone(ctx.getTimeZone()).toLocalDate();
			}
			if (ctx.getDatabaseDialect() instanceof SqliteDialect && metadata.getColumnType(col) == Types.TIMESTAMP) {
				LocalDateTime value = asLocalDateTime(rs, col, ctx, false);
				return value == null ? null : value.toLocalDate();
			}
			return asLocalDate(rs, col, ctx);
		}

		public static LocalDate asLocalDate(ResultSet rs, int col) throws SQLException {
			return asLocalDate(rs, col, null);
		}

		public static LocalDate asLocalDate(ResultSet rs, int col, @Nullable StatementContext<?> ctx) throws SQLException {
			LocalDate d = tryGet(rs, col, LocalDate.class);
			if (d != null) return d;
			Object raw = rs.getObject(col);
			if (raw == null) return null;
			if (raw instanceof java.sql.Date sqlDate) {
				int infinity = ctx == null ? 0 : postgresInfinityDirection(ctx, sqlDate);
				return infinity == 0 ? sqlDate.toLocalDate() : infinity > 0 ? LocalDate.MAX : LocalDate.MIN;
			}
			if (raw instanceof String s) {
				try {
					return LocalDate.parse(s);
				} catch (DateTimeParseException ignore) {
				}
			}
			java.sql.Date d2 = rs.getDate(col);
			if (d2 == null) return null;
			int infinity = ctx == null ? 0 : postgresInfinityDirection(ctx, d2);
			return infinity == 0 ? d2.toLocalDate() : infinity > 0 ? LocalDate.MAX : LocalDate.MIN;
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
			if (raw instanceof java.sql.Time sqlTime) return localTimeWithMilliseconds(sqlTime);
			if (raw instanceof String s) {
				try {
					return LocalTime.parse(s);
				} catch (DateTimeParseException ignore) {
				}
			}
			java.sql.Time t2 = rs.getTime(col);
			return t2 == null ? null : localTimeWithMilliseconds(t2);
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
				throw new DatabaseException("Driver cannot read SQL TIME WITH TIME ZONE without losing its offset");
			}
			Object raw = rs.getObject(col);
			if (raw == null) return null;
			if (raw instanceof OffsetTime offsetTime) return offsetTime;
			if (raw instanceof String string) {
				try {
					return offsetTimeFromString(string, ctx.getTimeZone());
				} catch (DateTimeException | IllegalArgumentException ignore) {
					// Retain the driver's classic getter for explicitly configured, non-ISO time text.
				}
			}
			// A plain TIME has no date with which to resolve regional zone rules.
			LocalTime lt = asLocalTime(rs, col);
			if (lt == null) return null;
			return offsetTimeFromLocalTime(lt, ctx.getTimeZone());
		}
	}

	/** Shared strict conversion for scalar, row and typed-container Boolean targets. */
	private static Boolean booleanFromValue(Object value) {
		if (value instanceof Boolean booleanValue) return booleanValue;
		// Keep the established numeric rule: zero is false and any finite nonzero number is true.
		if (value instanceof Number number) return decimalForNumericConversion(number).signum() != 0;
		if (value instanceof String string) {
			// PostgreSQL's Boolean input words, including unique prefixes. "o" is ambiguous between on/off.
			return switch (string.strip().toLowerCase(Locale.ROOT)) {
				case "true", "tru", "tr", "t", "yes", "ye", "y", "on", "1" -> Boolean.TRUE;
				case "false", "fals", "fal", "fa", "f", "no", "n", "off", "of", "0" -> Boolean.FALSE;
				default -> throw new DatabaseException("Cannot map text to boolean; expected true/false, yes/no, on/off, 1/0 or an unambiguous word prefix");
			};
		}
		throw new DatabaseException("Cannot map a value of type " + value.getClass().getName() + " to boolean");
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
		Object convert(@NonNull DefaultResultSetMapper mapper,
									 @Nullable Object raw,
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

	/** Cache lookups never invoke driver/dialect equals or hashCode, nor retain them through cache values. */
	private static final class WeakIdentityKey<T> extends WeakReference<T> {
		private final int hash;

		WeakIdentityKey(T referent, @Nullable ReferenceQueue<? super T> queue) {
			super(referent, queue);
			this.hash = System.identityHashCode(referent);
		}

		@Override public int hashCode() { return this.hash; }

		@Override public boolean equals(@Nullable Object other) {
			if (this == other) return true;
			T referent = get();
			return referent != null && other instanceof WeakIdentityKey<?> key && referent == key.get();
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
		List<String> unmappedColumns = this.failOnUnmappedColumns ? new ArrayList<>() : null;
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
				if (unmappedColumns != null) unmappedColumns.add(rawLabel);
				// No property claims this column - add a SKIP binding to keep behavior explicit
				bindings.add(
						new ColumnBinding(
								i,                            // columnIndex
								-1,                           // recordArgIndexOrMinusOne (SKIP)
								null,                         // setterOrNull
								reader,                       // reader
								(mapper, raw, rrs, cctx, ip, resources) -> raw,  // converter
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
				if (!isRecord && GenericPropertyTypes.hasUnresolvedVariable(ttype.getType()))
					throw new DatabaseException(format("Cannot resolve generic type %s for mapped property '%s' of %s. "
							+ "Use a concrete subclass that binds its type parameters, an Object property, or a custom ResultSetMapper.",
							ttype.getType().getTypeName(), prop, resultClass.getName()));
				int[] mapperIndices = customColumnMapperIndicesFor(ttype);
				if (builtInTargetType(ttype).getRawClass() == Optional.class || mapperIndices.length == 0)
					rejectOptionalMappingTargets(ttype);

				// Unboxed + boxed target, and primitive flag
				final Class<?> rawTargetClassUnboxed = isRecord
						? recordComponents[recordArgIndex].getType()
						: ttype.getRawClass();
				final Class<?> rawTargetClassBoxed = boxedClass(rawTargetClassUnboxed);
				final boolean targetIsPrimitive = rawTargetClassUnboxed.isPrimitive();

				final TargetType targetTypeFinal = ttype;
				final String propertyNameFinal = prop;
				final int colIndexFinal = i;
				final String labelFinal = labelNorm;

				boolean typedDuckContainer = ctx.getDatabaseDialect() instanceof DuckDbDialect
						&& (requiresRecursiveContainerConversion(targetTypeFinal) || targetTypeFinal.isArray());
				final boolean deferredContainerType = typedDuckContainer && mapperIndices.length != 0;
				final String containerTypeName = typedDuckContainer && !deferredContainerType
						? optionalContainerTypeName(resultSetMetaData, i) : null;
				// Callbacks may refer back to the mapper, so neither they nor the mapper belong in a cached plan.
				Converter converter = (mapper, raw, rrs, cctx, ip, resources) -> mapper.convertColumn(
						raw, targetTypeFinal, mapper.customColumnMappersAtIndices(mapperIndices), rrs, cctx, ip, resources,
						colIndexFinal, labelFinal, jdbcType, containerTypeName, deferredContainerType);


				bindings.add(new ColumnBinding(i, recordArgIndex, setterMH, reader, converter,
						targetIsPrimitive, rawTargetClassBoxed, propertyNameFinal));
			}
		}

		if (unmappedColumns != null && !unmappedColumns.isEmpty()
				&& !(isRecord && mappedColumnCount == 0 && cols == 1 && isSingleStructColumn(resultSetMetaData)))
			throw new DatabaseException(format("Unmapped source columns %s for %s; alias the columns, use a matching projection, "
					+ "or disable failOnUnmappedColumns", unmappedColumns, resultClass.getName()));
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
