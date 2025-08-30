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
import java.math.BigDecimal;
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


	/**
	 * Acquires a vector parameter for an array of {@code double}.
	 *
	 * @param elements the elements of the vector parameter
	 * @return the vector parameter
	 */
	@Nonnull
	public static VectorParameter vectorOfDoubles(@Nullable double[] elements) {
		return new DefaultVectorParameter(elements);
	}

	/**
	 * Acquires a vector parameter for a {@link List} of {@link Double}.
	 *
	 * @param elements the elements of the vector parameter
	 * @return the vector parameter
	 */
	@Nonnull
	public static VectorParameter vectorOfDoubles(@Nullable List<Double> elements) {
		if (elements == null)
			return new DefaultVectorParameter(null);

		double[] doubles = new double[elements.size()];
		for (int i = 0; i < doubles.length; i++) doubles[i] = requireNonNull(elements.get(i));
		return new DefaultVectorParameter(doubles);
	}

	/**
	 * Acquires a vector parameter for an array of {@code float}.
	 *
	 * @param elements the elements of the vector parameter
	 * @return the vector parameter
	 */
	@Nonnull
	public static VectorParameter vectorOfFloats(@Nullable float[] elements) {
		if (elements == null)
			return new DefaultVectorParameter(null);

		double[] doubles = new double[elements.length];
		for (int i = 0; i < elements.length; i++) doubles[i] = elements[i];
		return new DefaultVectorParameter(doubles);
	}

	/**
	 * Acquires a vector parameter for a {@link List} of {@link Float}.
	 *
	 * @param elements the elements of the vector parameter
	 * @return the vector parameter
	 */
	@Nonnull
	public static VectorParameter vectorOfFloats(@Nullable List<Float> elements) {
		if (elements == null)
			return new DefaultVectorParameter(null);

		double[] doubles = new double[elements.size()];
		for (int i = 0; i < doubles.length; i++) doubles[i] = requireNonNull(elements.get(i));
		return new DefaultVectorParameter(doubles);
	}

	/**
	 * Acquires a vector parameter for a {@link List} of {@link BigDecimal}.
	 *
	 * @param elements the elements of the vector parameter
	 * @return the vector parameter
	 */
	@Nonnull
	public static VectorParameter vectorOfBigDecimals(@Nullable List<BigDecimal> elements) {
		if (elements == null)
			return new DefaultVectorParameter(null);

		double[] d = new double[elements.size()];
		for (int i = 0; i < d.length; i++) d[i] = requireNonNull(elements.get(i)).doubleValue();
		return new DefaultVectorParameter(d);
	}

	/**
	 * Default package-private implementation of {@link VectorParameter}.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 2.1.0
	 */
	@ThreadSafe
	static class DefaultVectorParameter implements VectorParameter {
		@Nullable
		private final double[] elements;

		private DefaultVectorParameter(@Nullable double[] elements) {
			if (elements == null) {
				this.elements = null;
				return;
			}

			if (elements.length == 0)
				throw new IllegalArgumentException("Vector parameters must have at least 1 element");

			for (double d : elements)
				if (!Double.isFinite(d))
					throw new IllegalArgumentException("Vector parameter elements must be finite (no NaN/Infinity)");

			// Always defensive copy
			this.elements = elements.clone();
		}

		/**
		 * Gets the elements of this vector.
		 *
		 * @return the elements of this vector
		 */
		@Nonnull
		@Override
		public Optional<double[]> getElements() {
			// Defensive copy
			return this.elements == null ? Optional.empty() : Optional.of(this.elements.clone());
		}
	}
}