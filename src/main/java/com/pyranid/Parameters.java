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
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Fluent interface for acquiring instances of specialized parameter types.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 2.1.0
 */
@ThreadSafe
public final class Parameters {
	private Parameters() {
		// Prevents instantiation
	}

	/**
	 * Acquires a SQL ARRAY parameter for a {@link List} given an appropriate <a href="https://docs.oracle.com/en/java/javase/24/docs/api/java.sql/java/sql/Array.html#getBaseTypeName()" target="_blank">{@code java.sql.Array#getBaseTypeName()}</a>.
	 * <p>
	 * You may determine available {@code baseTypeName} values for your database by examining metadata exposed via {@link Database#examineDatabaseMetaData(DatabaseMetaDataExaminer)}.
	 *
	 * @param baseTypeName the SQL ARRAY element type, e.g. {@code "text"}, {@code "uuid"}, {@code "float4"}, {@code "float8"} ...
	 * @param list         the list whose elements will be used to populate the SQL ARRAY
	 * @param <E>          the element type of the Java list ({@code List<E>}); each element must be bindable to {@code baseTypeName} by the active {@link PreparedStatementBinder}.
	 * @return a SQL ARRAY parameter for the given list
	 */
	@Nonnull
	public static <E> ArrayParameter<E> arrayOf(@Nonnull String baseTypeName,
																							@Nullable List<E> list) {
		requireNonNull(baseTypeName);
		return new DefaultArrayParameter(baseTypeName, list == null ? null : list.toArray());
	}

	/**
	 * Acquires a SQL ARRAY parameter for a native Java array given an appropriate <a href="https://docs.oracle.com/en/java/javase/24/docs/api/java.sql/java/sql/Array.html#getBaseTypeName()" target="_blank">{@code java.sql.Array#getBaseTypeName()}</a>.
	 * <p>
	 * You may determine available {@code baseTypeName} values for your database by examining metadata exposed via {@link Database#examineDatabaseMetaData(DatabaseMetaDataExaminer)}.
	 *
	 * @param baseTypeName the SQL ARRAY element type, e.g. {@code "text"}, {@code "uuid"}, {@code "float4"}, {@code "float8"} ...
	 * @param array        the native Java array whose elements will be used to populate the SQL ARRAY
	 * @param <E>          the element type of the Java array ({@code T[]}); each element must be bindable to {@code baseTypeName} by the active {@link PreparedStatementBinder}.
	 * @return a SQL ARRAY parameter for the given Java array
	 */
	public static <E> ArrayParameter<E> arrayOf(@Nonnull String baseTypeName,
																							@Nullable E[] array) {
		requireNonNull(baseTypeName);
		return new DefaultArrayParameter(baseTypeName, array);
	}

	/**
	 * Default package-private implementation of {@link ArrayParameter}.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 2.1.0
	 */
	@ThreadSafe
	static class DefaultArrayParameter implements ArrayParameter {
		@Nonnull
		private final String baseTypeName; // e.g. "text", "uuid", "integer", ...
		@Nullable
		private final Object[] elements;

		DefaultArrayParameter(@Nonnull String baseTypeName,
													@Nullable Object[] elements) {
			requireNonNull(baseTypeName);

			this.baseTypeName = baseTypeName;
			this.elements = elements == null ? null : elements.clone(); // Always perform a defensive copy
		}

		/**
		 * Gets the element type of this SQL ARRAY, which corresponds to the value of <a href="https://docs.oracle.com/en/java/javase/24/docs/api/java.sql/java/sql/Array.html#getBaseTypeName()" target="_blank">{@code java.sql.Array#getBaseTypeName()}</a>
		 * and is database-specific.
		 *
		 * @return the element type of this SQL ARRAY
		 */
		@Nonnull
		@Override
		public String getBaseTypeName() {
			return this.baseTypeName;
		}

		/**
		 * Gets the elements of this SQL ARRAY.
		 *
		 * @return the elements of this SQL ARRAY
		 */
		@Nonnull
		@Override
		public Optional<Object[]> getElements() {
			// Defensive copy
			return this.elements == null ? Optional.empty() : Optional.of(this.elements.clone());
		}
	}
}