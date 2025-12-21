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

import com.pyranid.JsonParameter.BindingPreference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Fluent interface for acquiring instances of specialized parameter types.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 3.0.0
 */
@ThreadSafe
public final class Parameters {
	private Parameters() {
		// Prevents instantiation
	}

	/**
	 * Acquires a SQL ARRAY parameter for a {@link List} given an appropriate <a href="https://docs.oracle.com/en/java/javase/24/docs/api/java.sql/java/sql/Array.html#getBaseTypeName()" target="_blank">{@code java.sql.Array#getBaseTypeName()}</a>.
	 * <p>
	 * You may determine available {@code baseTypeName} values for your database by examining metadata exposed via {@link Database#readDatabaseMetaData(DatabaseMetaDataReader)}.
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
	 * You may determine available {@code baseTypeName} values for your database by examining metadata exposed via {@link Database#readDatabaseMetaData(DatabaseMetaDataReader)}.
	 *
	 * @param baseTypeName the SQL ARRAY element type, e.g. {@code "text"}, {@code "uuid"}, {@code "float4"}, {@code "float8"} ...
	 * @param array        the native Java array whose elements will be used to populate the SQL ARRAY
	 * @param <E>          the element type of the Java array ({@code T[]}); each element must be bindable to {@code baseTypeName} by the active {@link PreparedStatementBinder}.
	 * @return a SQL ARRAY parameter for the given Java array
	 */
	@Nonnull
	public static <E> ArrayParameter<E> arrayOf(@Nonnull String baseTypeName,
																							@Nullable E[] array) {
		requireNonNull(baseTypeName);
		return new DefaultArrayParameter(baseTypeName, array);
	}

	/**
	 * Default package-private implementation of {@link ArrayParameter}.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 3.0.0
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
	 * Acquires a parameter for SQL {@code IN} list expansion using a {@link Collection}.
	 *
	 * @param elements the elements to expand into {@code ?} placeholders
	 * @param <E>      the element type
	 * @return an IN-list parameter for the given elements
	 */
	@Nonnull
	public static <E> InListParameter inList(@Nonnull Collection<E> elements) {
		requireNonNull(elements);
		return new DefaultInListParameter(elements.toArray());
	}

	/**
	 * Acquires a parameter for SQL {@code IN} list expansion using a Java array.
	 *
	 * @param elements the elements to expand into {@code ?} placeholders
	 * @param <E>      the element type
	 * @return an IN-list parameter for the given elements
	 */
	@Nonnull
	public static <E> InListParameter inList(@Nonnull E[] elements) {
		requireNonNull(elements);
		return new DefaultInListParameter(elements);
	}

	/**
	 * Acquires a parameter for SQL {@code IN} list expansion using a {@code byte[]} array.
	 *
	 * @param elements the elements to expand into {@code ?} placeholders
	 * @return an IN-list parameter for the given elements
	 */
	@Nonnull
	public static InListParameter inList(@Nonnull byte[] elements) {
		requireNonNull(elements);
		Object[] boxed = new Object[elements.length];
		for (int i = 0; i < elements.length; i++)
			boxed[i] = elements[i];
		return new DefaultInListParameter(boxed);
	}

	/**
	 * Acquires a parameter for SQL {@code IN} list expansion using a {@code short[]} array.
	 *
	 * @param elements the elements to expand into {@code ?} placeholders
	 * @return an IN-list parameter for the given elements
	 */
	@Nonnull
	public static InListParameter inList(@Nonnull short[] elements) {
		requireNonNull(elements);
		Object[] boxed = new Object[elements.length];
		for (int i = 0; i < elements.length; i++)
			boxed[i] = elements[i];
		return new DefaultInListParameter(boxed);
	}

	/**
	 * Acquires a parameter for SQL {@code IN} list expansion using an {@code int[]} array.
	 *
	 * @param elements the elements to expand into {@code ?} placeholders
	 * @return an IN-list parameter for the given elements
	 */
	@Nonnull
	public static InListParameter inList(@Nonnull int[] elements) {
		requireNonNull(elements);
		Object[] boxed = new Object[elements.length];
		for (int i = 0; i < elements.length; i++)
			boxed[i] = elements[i];
		return new DefaultInListParameter(boxed);
	}

	/**
	 * Acquires a parameter for SQL {@code IN} list expansion using a {@code long[]} array.
	 *
	 * @param elements the elements to expand into {@code ?} placeholders
	 * @return an IN-list parameter for the given elements
	 */
	@Nonnull
	public static InListParameter inList(@Nonnull long[] elements) {
		requireNonNull(elements);
		Object[] boxed = new Object[elements.length];
		for (int i = 0; i < elements.length; i++)
			boxed[i] = elements[i];
		return new DefaultInListParameter(boxed);
	}

	/**
	 * Acquires a parameter for SQL {@code IN} list expansion using a {@code float[]} array.
	 *
	 * @param elements the elements to expand into {@code ?} placeholders
	 * @return an IN-list parameter for the given elements
	 */
	@Nonnull
	public static InListParameter inList(@Nonnull float[] elements) {
		requireNonNull(elements);
		Object[] boxed = new Object[elements.length];
		for (int i = 0; i < elements.length; i++)
			boxed[i] = elements[i];
		return new DefaultInListParameter(boxed);
	}

	/**
	 * Acquires a parameter for SQL {@code IN} list expansion using a {@code double[]} array.
	 *
	 * @param elements the elements to expand into {@code ?} placeholders
	 * @return an IN-list parameter for the given elements
	 */
	@Nonnull
	public static InListParameter inList(@Nonnull double[] elements) {
		requireNonNull(elements);
		Object[] boxed = new Object[elements.length];
		for (int i = 0; i < elements.length; i++)
			boxed[i] = elements[i];
		return new DefaultInListParameter(boxed);
	}

	/**
	 * Acquires a parameter for SQL {@code IN} list expansion using a {@code boolean[]} array.
	 *
	 * @param elements the elements to expand into {@code ?} placeholders
	 * @return an IN-list parameter for the given elements
	 */
	@Nonnull
	public static InListParameter inList(@Nonnull boolean[] elements) {
		requireNonNull(elements);
		Object[] boxed = new Object[elements.length];
		for (int i = 0; i < elements.length; i++)
			boxed[i] = elements[i];
		return new DefaultInListParameter(boxed);
	}

	/**
	 * Acquires a parameter for SQL {@code IN} list expansion using a {@code char[]} array.
	 *
	 * @param elements the elements to expand into {@code ?} placeholders
	 * @return an IN-list parameter for the given elements
	 */
	@Nonnull
	public static InListParameter inList(@Nonnull char[] elements) {
		requireNonNull(elements);
		Object[] boxed = new Object[elements.length];
		for (int i = 0; i < elements.length; i++)
			boxed[i] = elements[i];
		return new DefaultInListParameter(boxed);
	}

	/**
	 * Default package-private implementation of {@link InListParameter}.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 4.0.0
	 */
	@ThreadSafe
	static class DefaultInListParameter implements InListParameter {
		@Nonnull
		private final Object[] elements;

		DefaultInListParameter(@Nonnull Object[] elements) {
			requireNonNull(elements);
			this.elements = elements.clone(); // Always perform a defensive copy
		}

		@Nonnull
		@Override
		public Optional<Object[]> getElements() {
			// Defensive copy
			return Optional.of(this.elements.clone());
		}
	}

	/**
	 * Acquires a parameter of array type, preserving element type information so it is accessible at runtime.
	 * <p>
	 * This is useful when you want to bind an array via a {@link CustomParameterBinder} and need the
	 * component type to be preserved for {@link TargetType} matching.
	 * <p>
	 * <strong>Note:</strong> this kind of parameter requires a corresponding {@link CustomParameterBinder}
	 * to be registered; otherwise, binding will fail-fast.
	 *
	 * @param elementType the element type of the array
	 * @param array       the array value to wrap; may be {@code null}
	 * @param <E>         the element type of the array
	 * @return a {@link TypedParameter} representing a {@code E[]} suitable for use with custom binders
	 */
	@Nonnull
	public static <E> TypedParameter typedArrayOf(@Nonnull Class<E> elementType,
																								@Nullable E[] array) {
		requireNonNull(elementType);
		return typedArrayOf((Class<?>) elementType, array);
	}

	/**
	 * Acquires a parameter of array type, preserving element type information so it is accessible at runtime.
	 * <p>
	 * This overload supports primitive arrays by passing the primitive component class
	 * (for example, {@code int.class}) and the corresponding primitive array.
	 * <p>
	 * <strong>Note:</strong> this kind of parameter requires a corresponding {@link CustomParameterBinder}
	 * to be registered; otherwise, binding will fail-fast.
	 *
	 * @param elementType the element type of the array (may be primitive)
	 * @param array       the array value to wrap; may be {@code null}
	 * @return a {@link TypedParameter} representing an array suitable for use with custom binders
	 */
	@Nonnull
	public static TypedParameter typedArrayOf(@Nonnull Class<?> elementType,
																						@Nullable Object array) {
		requireNonNull(elementType);

		if (array != null) {
			Class<?> arrayClass = array.getClass();

			if (!arrayClass.isArray())
				throw new IllegalArgumentException("Array parameter is not an array");

			Class<?> componentType = arrayClass.getComponentType();

			if (elementType.isPrimitive()) {
				if (!componentType.equals(elementType))
					throw new IllegalArgumentException("Array parameter component type does not match elementType");
			} else if (!elementType.isAssignableFrom(componentType)) {
				throw new IllegalArgumentException("Array parameter component type is not assignable to elementType");
			}
		}

		Class<?> arrayType = Array.newInstance(elementType, 0).getClass();
		return new DefaultTypedParameter(arrayType, array);
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
	 * @since 3.0.0
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

	/**
	 * Acquires a JSON parameter for "stringified" JSON, using {@link BindingPreference#BINARY}.
	 *
	 * @param json the stringified JSON for this parameter
	 * @return the JSON parameter
	 */
	@Nonnull
	public static JsonParameter json(@Nullable String json) {
		return new DefaultJsonParameter(json, BindingPreference.BINARY);
	}

	/**
	 * Acquires a JSON parameter for "stringified" JSON.
	 *
	 * @param json              the stringified JSON for this parameter
	 * @param bindingPreference how the JSON parameter should be bound to a {@link java.sql.PreparedStatement}
	 * @return the JSON parameter
	 */
	@Nonnull
	public static JsonParameter json(@Nullable String json,
																	 @Nonnull BindingPreference bindingPreference) {
		requireNonNull(bindingPreference);

		return new DefaultJsonParameter(json, bindingPreference);
	}

	/**
	 * Default package-private implementation of {@link JsonParameter}.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 3.0.0
	 */
	@ThreadSafe
	static class DefaultJsonParameter implements JsonParameter {
		@Nullable
		private final String json;
		@Nonnull
		private final BindingPreference bindingPreference;

		private DefaultJsonParameter(@Nullable String json,
																 @Nonnull BindingPreference bindingPreference) {
			requireNonNull(bindingPreference);

			this.json = json;
			this.bindingPreference = bindingPreference;
		}

		@Nonnull
		@Override
		public Optional<String> getJson() {
			return Optional.ofNullable(this.json);
		}

		@Nonnull
		@Override
		public BindingPreference getBindingPreference() {
			return this.bindingPreference;
		}
	}

	/**
	 * Acquires a parameter of type {@link List}, preserving type information so it is accessible at runtime.
	 * <p>
	 * This is useful when you want to bind a parameterized collection such as {@code List<UUID>} or
	 * {@code List<String>} and need the generic type argument (e.g. {@code UUID.class}) to be preserved.
	 * <p>
	 * The resulting {@link TypedParameter} carries both the runtime value and its generic type so that
	 * {@link CustomParameterBinder#appliesTo(TargetType)} can match against the element type.
	 * <p>
	 * <strong>Note:</strong> this kind of parameter requires a corresponding {@link CustomParameterBinder}
	 * to be registered; otherwise, binding will fail-fast.
	 *
	 * @param elementType the {@link Class} representing the type of elements contained in the list;
	 *                    used to preserve generic type information
	 * @param list        the list value to wrap; may be {@code null}
	 * @param <E>         the element type of the list
	 * @return a {@link TypedParameter} representing a {@code List<E>} suitable for use with custom binders
	 */
	@Nonnull
	public static <E> TypedParameter listOf(@Nonnull Class<E> elementType,
																					@Nullable List<E> list) {
		requireNonNull(elementType);

		Type listOfE = new DefaultParameterizedType(List.class, new Type[]{elementType}, null);
		return new DefaultTypedParameter(listOfE, list);
	}

	/**
	 * Acquires a parameter of type {@link Set}, preserving type information so it is accessible at runtime.
	 * <p>
	 * This is useful when you want to bind a parameterized collection such as {@code Set<UUID>} or
	 * {@code Set<String>} and need the generic type argument (e.g. {@code UUID.class}) to be preserved.
	 * <p>
	 * The resulting {@link TypedParameter} carries both the runtime value and its generic type so that
	 * {@link CustomParameterBinder#appliesTo(TargetType)} can match against the element type.
	 * <p>
	 * <strong>Note:</strong> this kind of parameter requires a corresponding {@link CustomParameterBinder}
	 * to be registered; otherwise, binding will fail-fast.
	 *
	 * @param elementType the {@link Class} representing the type of elements contained in the set;
	 *                    used to preserve generic type information
	 * @param set         the set value to wrap; may be {@code null}
	 * @param <E>         the element type of the set
	 * @return a {@link TypedParameter} representing a {@code Set<E>} suitable for use with custom binders
	 */
	@Nonnull
	public static <E> TypedParameter setOf(@Nonnull Class<E> elementType,
																				 @Nullable Set<E> set) {
		requireNonNull(elementType);

		Type setOfE = new DefaultParameterizedType(Set.class, new Type[]{elementType}, null);
		return new DefaultTypedParameter(setOfE, set);
	}

	/**
	 * Acquires a parameter of type {@link Map}, preserving key and value type information
	 * so they are accessible at runtime.
	 * <p>
	 * This is useful when you want to bind a parameterized collection such as {@code Map<UUID, Integer>}
	 * and need the generic type arguments (e.g. {@code UUID.class} and {@code Integer.class}) to be preserved.
	 * <p>
	 * The resulting {@link TypedParameter} carries both the runtime value and its generic type so that
	 * {@link CustomParameterBinder#appliesTo(TargetType)} can match against the element type.
	 * <p>
	 * <strong>Note:</strong> this kind of parameter requires a corresponding {@link CustomParameterBinder}
	 * to be registered; otherwise, binding will fail-fast.
	 *
	 * @param keyType   the type of the map keys
	 * @param valueType the type of the map values
	 * @param map       the map value; may be {@code null}
	 * @param <K>       the key type
	 * @param <V>       the value type
	 * @return a {@link TypedParameter} representing {@code Map<K,V>}
	 */
	@Nonnull
	public static <K, V> TypedParameter mapOf(@Nonnull Class<K> keyType,
																						@Nonnull Class<V> valueType,
																						@Nullable Map<K, V> map) {
		requireNonNull(keyType);
		requireNonNull(valueType);

		Type mapOfKV = new DefaultParameterizedType(Map.class, new Type[]{keyType, valueType}, null);
		return new DefaultTypedParameter(mapOfKV, map);
	}
}
