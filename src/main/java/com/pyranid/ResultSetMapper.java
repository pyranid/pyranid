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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Contract for mapping a {@link ResultSet} row to the specified type.
 * <p>
 * A production-ready concrete implementation is available via the following static methods:
 * <ul>
 *   <li>{@link #withDefaultConfiguration()}</li>
 *   <li>{@link #withPlanCachingEnabled(Boolean)} (builder)</li>
 *   <li>{@link #withCustomColumnMappers(List)} (builder)</li>
 *   <li>{@link #withNormalizationLocale(Locale)} (builder)</li>
 * </ul>
 * <p>
 * How to acquire an instance:
 * <pre>{@code  // With out-of-the-box defaults
 * ResultSetMapper default = ResultSetMapper.withDefaultConfiguration();
 *
 * // Customized
 * ResultSetMapper custom = ResultSetMapper.withPlanCachingEnabled(false)
 *  .customColumnMappers(List.of(...))
 *  .normalizationLocale(Locale.forLanguageTag("pt-BR"))
 *  .build();}</pre> Or, implement your own: <pre>{@code  ResultSetMapper myImpl = new ResultSetMapper() {
 *   @NonNull
 *   @Override
 *   public <T> Optional<T> map(
 *     @NonNull StatementContext<T> statementContext,
 *     @NonNull ResultSet resultSet,
 *     @NonNull Class<T> resultSetRowType,
 *     @NonNull InstanceProvider instanceProvider
 *   ) throws SQLException {
 *     // Pull data from resultSet and apply to a new instance of T
 *     return Optional.empty();
 *   }
 * };}</pre>
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@FunctionalInterface
public interface ResultSetMapper {
	/**
	 * Maps the current row of {@code resultSet} to the result class indicated by {@code statementContext}.
	 *
	 * @param <T>              result instance type token
	 * @param statementContext current SQL context
	 * @param resultSet        provides raw row data to pull from
	 * @param resultSetRowType the type to which the {@link ResultSet} row should be marshaled
	 * @param instanceProvider instance-creation factory, used to instantiate {@code resultSetRowType} row objects
	 * @return an {@link Optional} containing an instance of the given {@code resultClass}, or {@link Optional#empty()} to indicate a {@code null} value
	 * @throws SQLException if an error occurs during mapping
	 */
	@NonNull
	<T> Optional<T> map(@NonNull StatementContext<T> statementContext,
											@NonNull ResultSet resultSet,
											@NonNull Class<T> resultSetRowType,
											@NonNull InstanceProvider instanceProvider) throws SQLException;

	/**
	 * Default maximum number of cached row-mapping plans per result class.
	 */
	@NonNull
	Integer DEFAULT_PLAN_CACHE_CAPACITY = 1024;

	/**
	 * Retained for source and binary compatibility. Preferred custom column mappers are not cached.
	 *
	 * @deprecated this value has no effect and will be removed in 5.0.0
	 */
	@Deprecated(since = "4.5.0", forRemoval = true)
	int DEFAULT_PREFERRED_COLUMN_MAPPER_CACHE_CAPACITY = 256;

	/**
	 * Acquires a builder with the locale used for non-ASCII case normalization of JDBC column names.
	 * ASCII letters are folded independently of locale before locale-sensitive lowercasing.
	 *
	 * @param normalizationLocale the locale used after locale-independent ASCII folding
	 * @return a {@code Builder} for a concrete implementation
	 */
	@NonNull
	static Builder withNormalizationLocale(@NonNull Locale normalizationLocale) {
		requireNonNull(normalizationLocale);
		return new Builder().normalizationLocale(normalizationLocale);
	}

	/**
	 * Acquires a builder for a concrete implementation of this interface, specifying a {@link List} of custom column-specific mapping logic to apply, in priority order.
	 *
	 * @param customColumnMappers a {@link List} of custom column-specific mapping logic to apply, in priority order
	 * @return a {@code Builder} for a concrete implementation
	 */
	@NonNull
	static Builder withCustomColumnMappers(@NonNull List<@NonNull CustomColumnMapper> customColumnMappers) {
		requireNonNull(customColumnMappers);
		return new Builder().customColumnMappers(customColumnMappers);
	}

	/**
	 * Acquires a builder for a concrete implementation of this interface, specifying whether an internal "mapping plan" cache should be used to speed up {@link ResultSet} mapping.
	 * <p>
	 * Disabling plan caching is primarily useful for highly dynamic schemas; plans use the same mapping rules but are compiled without retention.
	 *
	 * @param planCachingEnabled whether an internal "mapping plan" cache should be used to speed up {@link ResultSet} mapping
	 * @return a {@code Builder} for a concrete implementation
	 */
	@NonNull
	static Builder withPlanCachingEnabled(@NonNull Boolean planCachingEnabled) {
		requireNonNull(planCachingEnabled);
		return new Builder().planCachingEnabled(planCachingEnabled);
	}

	/**
	 * Acquires a concrete implementation of this interface with out-of-the-box defaults.
	 * <p>
	 * The returned instance is thread-safe.
	 *
	 * @return a concrete implementation of this interface with out-of-the-box defaults
	 */
	@NonNull
	static ResultSetMapper withDefaultConfiguration() {
		return new Builder().build();
	}

	/**
	 * Builder used to construct a standard implementation of {@link ResultSetMapper}.
	 * <p>
	 * This class is intended for use by a single thread.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 3.0.0
	 */
	@NotThreadSafe
	class Builder {
		@NonNull
		Locale normalizationLocale;
		@NonNull
		List<@NonNull CustomColumnMapper> customColumnMappers;
		@NonNull
		Boolean planCachingEnabled;
		@NonNull
		Integer planCacheCapacity;
		boolean failOnUnmappedColumns;
		boolean detachLobValues;
		@Nullable
		Long maxLobBytes = 16L * 1024 * 1024;
		@Nullable
		Long maxLobCharacters = 8L * 1024 * 1024;

		private Builder() {
			this.normalizationLocale = Locale.ROOT;
			this.customColumnMappers = List.of();
			this.planCachingEnabled = true;
			this.planCacheCapacity = DEFAULT_PLAN_CACHE_CAPACITY;
		}

		/**
		 * Specifies the locale used for non-ASCII case normalization when matching JDBC column names to Java properties.
		 * ASCII uppercase letters always fold to their ASCII lowercase equivalents before locale-sensitive lowercasing,
		 * keeping identifiers such as {@code ID} stable under Turkish and Azerbaijani locales. This locale still affects
		 * non-ASCII letters; the default is {@link Locale#ROOT}.
		 *
		 * @param normalizationLocale the locale used after locale-independent ASCII folding
		 * @return this {@code Builder}, for chaining
		 */
		@NonNull
		public Builder normalizationLocale(@NonNull Locale normalizationLocale) {
			requireNonNull(normalizationLocale);
			this.normalizationLocale = normalizationLocale;
			return this;
		}

		/**
		 * Specifies a {@link List} of custom column-specific mapping logic to apply, in priority order.
		 *
		 * @param customColumnMappers a {@link List} of custom column-specific mapping logic to apply, in priority order
		 * @return this {@code Builder}, for chaining
		 */
		@NonNull
		public Builder customColumnMappers(@NonNull List<@NonNull CustomColumnMapper> customColumnMappers) {
			requireNonNull(customColumnMappers);
			this.customColumnMappers = customColumnMappers;
			return this;
		}

		/**
		 * Specifies whether an internal "mapping plan" cache should be used to speed up {@link ResultSet} mapping.
		 * <p>
		 * Disabling plan caching is primarily useful for highly dynamic schemas; plans use the same mapping rules but are compiled without retention.
		 *
		 * @param planCachingEnabled whether an internal "mapping plan" cache should be used to speed up {@link ResultSet} mapping
		 * @return this {@code Builder}, for chaining
		 */
		@NonNull
		public Builder planCachingEnabled(@NonNull Boolean planCachingEnabled) {
			requireNonNull(planCachingEnabled);
			this.planCachingEnabled = planCachingEnabled;
			return this;
		}

		/**
		 * Specifies the maximum number of row-mapping plans to cache per result class when plan caching is enabled.
		 * <p>
		 * Use {@code 0} for an unbounded cache.
		 *
		 * @param planCacheCapacity maximum number of cached plans per result class, or {@code 0} for unbounded.
		 *                          Defaults to {@link #DEFAULT_PLAN_CACHE_CAPACITY}.
		 * @return this {@code Builder}, for chaining
		 */
		@NonNull
		public Builder planCacheCapacity(@NonNull Integer planCacheCapacity) {
			requireNonNull(planCacheCapacity);
			if (planCacheCapacity < 0)
				throw new IllegalArgumentException("Plan cache capacity must be >= 0");
			this.planCacheCapacity = planCacheCapacity;
			return this;
		}

		/**
		 * Rejects source columns that do not match a writable bean property or record component by throwing
		 * {@link DatabaseException} when the row is mapped.
		 * Disabled by default. This checks by-name row mappings before built-in column bindings read values; it does not
		 * require every target member to be selected. Map rows, scalar values, custom whole-row mappings,
		 * and the direct single-STRUCT record fallback keep their existing mapping rules.
		 * Validation runs when a row is mapped; empty result sets do not invoke the row mapper.
		 *
		 * @param failOnUnmappedColumns whether to reject unmatched source columns
		 * @return this builder
		 * @since 4.9.0
		 */
		@NonNull
		public Builder failOnUnmappedColumns(@NonNull Boolean failOnUnmappedColumns) {
			this.failOnUnmappedColumns = requireNonNull(failOnUnmappedColumns);
			return this;
		}

		/**
		 * Detaches LOBs exposed by native Map/Object mapping, including {@code Object}-typed bean properties
		 * and record components, as byte arrays or Strings.
		 * Disabled by default. BLOBs become {@code byte[]}; CLOBs, NCLOBs and SQLXML become
		 * {@link String}. Existing byte/character materialization limits apply, including nested
		 * Java arrays, collections and maps. Containers are copied without mutating driver values.
		 * Explicit JDBC-handle targets and handled custom mapper results retain their ownership contract.
		 *
		 * @param detachLobValues whether native result values should detach LOBs
		 * @return this builder
		 * @since 4.9.0
		 */
		@NonNull
		public Builder detachLobValues(@NonNull Boolean detachLobValues) {
			this.detachLobValues = requireNonNull(detachLobValues);
			return this;
		}

		/**
		 * Limits each BLOB value materialized by the default mapper as {@code byte[]}.
		 * Defaults to 16 MiB. The limit does not cover driver buffering, custom mappers,
		 * ordinary binary columns, or the aggregate size of a result list.
		 * This setter enables the binary limit even after {@link #unboundedLobMaterialization()}.
		 *
		 * @param maxLobBytes strictly positive maximum byte count
		 * @return this builder
		 * @since 4.8.0
		 */
		@NonNull
		public Builder maxLobBytes(@NonNull Long maxLobBytes) {
			requireNonNull(maxLobBytes);
			if (maxLobBytes <= 0)
				throw new IllegalArgumentException("Maximum LOB bytes must be > 0");
			this.maxLobBytes = maxLobBytes;
			return this;
		}

		/**
		 * Limits each CLOB/NCLOB value materialized by the default mapper as {@link String}, and SQLXML values
		 * detached through {@link #detachLobValues(Boolean)}.
		 * Defaults to 8,388,608 UTF-16 code units. The limit does not cover driver buffering,
		 * custom mappers, ordinary character columns, or the aggregate size of a result list.
		 * This setter enables the character limit even after {@link #unboundedLobMaterialization()}.
		 *
		 * @param maxLobCharacters strictly positive maximum UTF-16 code-unit count
		 * @return this builder
		 * @since 4.8.0
		 */
		@NonNull
		public Builder maxLobCharacters(@NonNull Long maxLobCharacters) {
			requireNonNull(maxLobCharacters);
			if (maxLobCharacters <= 0)
				throw new IllegalArgumentException("Maximum LOB characters must be > 0");
			this.maxLobCharacters = maxLobCharacters;
			return this;
		}

		/**
		 * Disables both configurable LOB limits. Java array and string size limits still apply.
		 * Later calls to either finite-limit setter re-enable that limit independently.
		 *
		 * @return this builder
		 * @since 4.8.0
		 */
		@NonNull
		public Builder unboundedLobMaterialization() {
			this.maxLobBytes = null;
			this.maxLobCharacters = null;
			return this;
		}

		/**
		 * Retained for source and binary compatibility. This setting has no effect.
		 * <p>
		 * Pyranid always evaluates applicable custom column mappers in their configured priority order. This method is
		 * deprecated and will be removed in 5.0.0.
		 *
		 * @param preferredColumnMapperCacheCapacity ignored compatibility value; must be greater than or equal to {@code 0}
		 * @return this {@code Builder}, for chaining
		 */
		@Deprecated(since = "4.5.0", forRemoval = true)
		@NonNull
		public Builder preferredColumnMapperCacheCapacity(@NonNull Integer preferredColumnMapperCacheCapacity) {
			requireNonNull(preferredColumnMapperCacheCapacity);
			if (preferredColumnMapperCacheCapacity < 0)
				throw new IllegalArgumentException("Preferred column mapper cache capacity must be >= 0");
			return this;
		}

		/**
		 * Constructs a default {@code ResultSetMapper} instance.
		 * <p>
		 * The constructed instance is thread-safe.
		 *
		 * @return a {@code ResultSetMapper} instance
		 */
		@NonNull
		public ResultSetMapper build() {
			return new DefaultResultSetMapper(this);
		}
	}
}
