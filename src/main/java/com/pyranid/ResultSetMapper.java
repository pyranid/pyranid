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
 *   @Nonnull
 *   @Override
 *   public <T> Optional<T> map(
 *     @Nonnull StatementContext<T> statementContext,
 *     @Nonnull ResultSet resultSet,
 *     @Nonnull Class<T> resultSetRowType,
 *     @Nonnull InstanceProvider instanceProvider
 *   ) throws SQLException {
 *     // TODO: pull data from resultSet and apply to a new instance of T
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
	@Nonnull
	<T> Optional<T> map(@Nonnull StatementContext<T> statementContext,
											@Nonnull ResultSet resultSet,
											@Nonnull Class<T> resultSetRowType,
											@Nonnull InstanceProvider instanceProvider) throws SQLException;

	/**
	 * Default maximum number of cached row-mapping plans.
	 */
	int DEFAULT_PLAN_CACHE_CAPACITY = 1024;

	/**
	 * Default maximum number of cached preferred custom column mappers.
	 */
	int DEFAULT_PREFERRED_COLUMN_MAPPER_CACHE_CAPACITY = 256;

	/**
	 * Acquires a builder for a concrete implementation of this interface, specifying the locale to use when massaging JDBC column names for matching against Java property names.
	 *
	 * @param normalizationLocale the locale to use when massaging JDBC column names for matching against Java property names
	 * @return a {@code Builder} for a concrete implementation
	 */
	@Nonnull
	static Builder withNormalizationLocale(@Nonnull Locale normalizationLocale) {
		requireNonNull(normalizationLocale);

		new ResultSetMapper() {
			@Nonnull
			@Override
			public <T> Optional<T> map(
					@Nonnull StatementContext<T> statementContext,
					@Nonnull ResultSet resultSet,
					@Nonnull Class<T> resultSetRowType,
					@Nonnull InstanceProvider instanceProvider) {
				return Optional.empty();
			}
		};

		return new Builder().normalizationLocale(normalizationLocale);
	}

	/**
	 * Acquires a builder for a concrete implementation of this interface, specifying a {@link List} of custom column-specific mapping logic to apply, in priority order.
	 *
	 * @param customColumnMappers a {@link List} of custom column-specific mapping logic to apply, in priority order
	 * @return a {@code Builder} for a concrete implementation
	 */
	@Nonnull
	static Builder withCustomColumnMappers(@Nonnull List<CustomColumnMapper> customColumnMappers) {
		requireNonNull(customColumnMappers);
		return new Builder().customColumnMappers(customColumnMappers);
	}

	/**
	 * Acquires a builder for a concrete implementation of this interface, specifying whether an internal "mapping plan" cache should be used to speed up {@link ResultSet} mapping.
	 *
	 * @param planCachingEnabled whether an internal "mapping plan" cache should be used to speed up {@link ResultSet} mapping
	 * @return a {@code Builder} for a concrete implementation
	 */
	@Nonnull
	static Builder withPlanCachingEnabled(@Nonnull Boolean planCachingEnabled) {
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
	@Nonnull
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
		@Nonnull
		Locale normalizationLocale;
		@Nonnull
		List<CustomColumnMapper> customColumnMappers;
		@Nonnull
		Boolean planCachingEnabled;
		@Nonnull
		Integer planCacheCapacity;
		@Nonnull
		Integer preferredColumnMapperCacheCapacity;

		private Builder() {
			this.normalizationLocale = Locale.ROOT;
			this.customColumnMappers = List.of();
			this.planCachingEnabled = true;
			this.planCacheCapacity = DEFAULT_PLAN_CACHE_CAPACITY;
			this.preferredColumnMapperCacheCapacity = DEFAULT_PREFERRED_COLUMN_MAPPER_CACHE_CAPACITY;
		}

		/**
		 * Specifies the locale to use when massaging JDBC column names for matching against Java property names.
		 *
		 * @param normalizationLocale the locale to use when massaging JDBC column names for matching against Java property names
		 * @return this {@code Builder}, for chaining
		 */
		@Nonnull
		public Builder normalizationLocale(@Nonnull Locale normalizationLocale) {
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
		@Nonnull
		public Builder customColumnMappers(@Nonnull List<CustomColumnMapper> customColumnMappers) {
			requireNonNull(customColumnMappers);
			this.customColumnMappers = customColumnMappers;
			return this;
		}

		/**
		 * Specifies whether an internal "mapping plan" cache should be used to speed up {@link ResultSet} mapping.
		 *
		 * @param planCachingEnabled whether an internal "mapping plan" cache should be used to speed up {@link ResultSet} mapping
		 * @return this {@code Builder}, for chaining
		 */
		@Nonnull
		public Builder planCachingEnabled(@Nonnull Boolean planCachingEnabled) {
			requireNonNull(planCachingEnabled);
			this.planCachingEnabled = planCachingEnabled;
			return this;
		}

		/**
		 * Specifies the maximum number of row-mapping plans to cache when plan caching is enabled.
		 * <p>
		 * Use {@code 0} for an unbounded cache.
		 *
		 * @param planCacheCapacity maximum number of cached plans, or {@code 0} for unbounded. Defaults to {@link #DEFAULT_PLAN_CACHE_CAPACITY}.
		 * @return this {@code Builder}, for chaining
		 */
		@Nonnull
		public Builder planCacheCapacity(@Nonnull Integer planCacheCapacity) {
			requireNonNull(planCacheCapacity);
			if (planCacheCapacity < 0)
				throw new IllegalArgumentException("Plan cache capacity must be >= 0");
			this.planCacheCapacity = planCacheCapacity;
			return this;
		}

		/**
		 * Specifies the maximum number of cached preferred custom column mappers.
		 * <p>
		 * Use {@code 0} for an unbounded cache.
		 *
		 * @param preferredColumnMapperCacheCapacity maximum number of cached preferred custom column mappers, or {@code 0} for unbounded.
		 *                                           Defaults to {@link #DEFAULT_PREFERRED_COLUMN_MAPPER_CACHE_CAPACITY}.
		 * @return this {@code Builder}, for chaining
		 */
		@Nonnull
		public Builder preferredColumnMapperCacheCapacity(@Nonnull Integer preferredColumnMapperCacheCapacity) {
			requireNonNull(preferredColumnMapperCacheCapacity);
			if (preferredColumnMapperCacheCapacity < 0)
				throw new IllegalArgumentException("Preferred column mapper cache capacity must be >= 0");
			this.preferredColumnMapperCacheCapacity = preferredColumnMapperCacheCapacity;
			return this;
		}

		/**
		 * Constructs a default {@code ResultSetMapper} instance.
		 * <p>
		 * The constructed instance is thread-safe.
		 *
		 * @return a {@code ResultSetMapper} instance
		 */
		@Nonnull
		public ResultSetMapper build() {
			return new DefaultResultSetMapper(this);
		}
	}
}
