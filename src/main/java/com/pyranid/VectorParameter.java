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
import javax.annotation.concurrent.ThreadSafe;
import java.math.BigDecimal;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Encapsulates prepared-statement parameter data meant to be bound to a vector type (e.g. PostgreSQL's <a href="https://github.com/pgvector/pgvector" target="_blank">{@code pgvector}</a>) by {@link PreparedStatementBinder}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 2.0.2
 */
@ThreadSafe
public final class VectorParameter {
	@Nonnull
	private final double[] elements;

	private VectorParameter(@Nonnull double[] elements) {
		requireNonNull(elements);

		if (elements.length == 0)
			throw new IllegalArgumentException("Vector parameters must have at least 1 element");

		for (double d : elements)
			if (!Double.isFinite(d))
				throw new IllegalArgumentException("Vector parameter elements must be finite (no NaN/Infinity)");

		// Always defensive copy
		this.elements = elements.clone();
	}

	/**
	 * Acquires a vector parameter for an array of {@code double}.
	 *
	 * @param elements the elements of the vector parameter
	 * @return the vector parameter
	 */
	@Nonnull
	public static VectorParameter ofDoubles(@Nonnull double[] elements) {
		requireNonNull(elements);
		return new VectorParameter(elements);
	}

	/**
	 * Acquires a vector parameter for a {@link List} of {@link Double}.
	 *
	 * @param elements the elements of the vector parameter
	 * @return the vector parameter
	 */
	@Nonnull
	public static VectorParameter ofDoubles(@Nonnull List<Double> elements) {
		requireNonNull(elements);

		double[] doubles = new double[elements.size()];
		for (int i = 0; i < doubles.length; i++) doubles[i] = requireNonNull(elements.get(i));
		return new VectorParameter(doubles);
	}

	/**
	 * Acquires a vector parameter for an array of {@code float}.
	 *
	 * @param elements the elements of the vector parameter
	 * @return the vector parameter
	 */
	@Nonnull
	public static VectorParameter ofFloats(@Nonnull float[] elements) {
		requireNonNull(elements);

		double[] doubles = new double[elements.length];
		for (int i = 0; i < elements.length; i++) doubles[i] = elements[i];
		return new VectorParameter(doubles);
	}

	/**
	 * Acquires a vector parameter for a {@link List} of {@link Float}.
	 *
	 * @param elements the elements of the vector parameter
	 * @return the vector parameter
	 */
	@Nonnull
	public static VectorParameter ofFloats(@Nonnull List<Float> elements) {
		requireNonNull(elements);

		double[] doubles = new double[elements.size()];
		for (int i = 0; i < doubles.length; i++) doubles[i] = requireNonNull(elements.get(i));
		return new VectorParameter(doubles);
	}

	/**
	 * Acquires a vector parameter for a {@link List} of {@link BigDecimal}.
	 *
	 * @param elements the elements of the vector parameter
	 * @return the vector parameter
	 */
	@Nonnull
	public static VectorParameter ofBigDecimals(@Nonnull List<BigDecimal> elements) {
		requireNonNull(elements);

		double[] d = new double[elements.size()];
		for (int i = 0; i < d.length; i++) d[i] = requireNonNull(elements.get(i)).doubleValue();
		return new VectorParameter(d);
	}

	/**
	 * Gets the elements of this vector.
	 *
	 * @return the elements of this vector
	 */
	@Nonnull
	public double[] getElements() {
		// Defensive copy
		return this.elements.clone();
	}
}