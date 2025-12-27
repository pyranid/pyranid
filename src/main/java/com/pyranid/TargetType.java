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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * A developer-friendly view over a reflective {@link java.lang.reflect.Type} used by the {@link ResultSet}-mapping pipeline for standard {@link ResultSetMapper} instances.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @see java.lang.reflect.Type
 * @since 3.0.0
 */
public interface TargetType {
	/**
	 * The original reflective type ({@code Class}, {@code ParameterizedType}, etc.)
	 */
	@NonNull
	Type getType();

	/**
	 * Raw class, with erasure ({@code List.class} for {@code List<UUID>}, etc.)
	 */
	@NonNull
	Class<?> getRawClass();

	/**
	 * @return {@code true} if {@link #getRawClass()} matches the provided {@code rawClass} (no subtype/parameter checks), {@code false} otherwise.
	 */
	@NonNull
	default Boolean matchesClass(@NonNull Class<?> rawClass) {
		requireNonNull(rawClass);
		return getRawClass().equals(rawClass);
	}

	/**
	 * Does this instance match the given raw class and its parameterized type arguments?
	 * <p>
	 * For example, invoke {@code matchesParameterizedType(List.class, UUID.class)} to determine "is this type a {@code List<UUID>}?"
	 */
	@NonNull
	default Boolean matchesParameterizedType(@NonNull Class<?> rawClass,
																					 Class<?> @Nullable ... typeArguments) {
		requireNonNull(rawClass);

		if (typeArguments == null || typeArguments.length == 0)
			return matchesClass(rawClass);

		if (!(getType() instanceof ParameterizedType parameterizedType) || !rawClass.equals(parameterizedType.getRawType()))
			return false;

		Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();

		if (actualTypeArguments.length != typeArguments.length)
			return false;

		for (int i = 0; i < actualTypeArguments.length; i++)
			if (!(actualTypeArguments[i] instanceof Class<?> actualClass) || !actualClass.equals(typeArguments[i]))
				return false;

		return true;
	}

	@NonNull
	default Boolean isList() {
		return matchesClass(List.class);
	}

	@NonNull
	default Optional<TargetType> getListElementType() {
		return matchesClass(List.class) ? getFirstTargetTypeArgument() : Optional.empty();
	}

	@NonNull
	default Boolean isSet() {
		return matchesClass(Set.class);
	}

	@NonNull
	default Optional<TargetType> getSetElementType() {
		return matchesClass(Set.class) ? getFirstTargetTypeArgument() : Optional.empty();
	}

	@NonNull
	default Boolean isMap() {
		return matchesClass(Map.class);
	}

	@NonNull
	default Optional<TargetType> getMapKeyType() {
		return matchesClass(Map.class) ? getTargetTypeArgumentAtIndex(0) : Optional.empty();
	}

	@NonNull
	default Optional<TargetType> getMapValueType() {
		return matchesClass(Map.class) ? getTargetTypeArgumentAtIndex(1) : Optional.empty();
	}

	@NonNull
	default Boolean isArray() {
		return getRawClass().isArray() || getType() instanceof GenericArrayType;
	}

	@NonNull
	default Optional<TargetType> getArrayComponentType() {
		if (getRawClass().isArray())
			return Optional.of(TargetType.of(getRawClass().getComponentType()));

		if (getType() instanceof GenericArrayType genericArrayType)
			return Optional.of(TargetType.of(genericArrayType.getGenericComponentType()));

		return Optional.empty();
	}

	/**
	 * All type arguments wrapped (empty for raw/non-parameterized).
	 */
	@NonNull
	List<TargetType> getTypeArguments();

	@NonNull
	static TargetType of(@NonNull Type type) {
		requireNonNull(type);
		return new DefaultTargetType(type);
	}

	@NonNull
	private Optional<TargetType> getFirstTargetTypeArgument() {
		return getTargetTypeArgumentAtIndex(0);
	}

	@NonNull
	private Optional<TargetType> getTargetTypeArgumentAtIndex(@NonNull Integer index) {
		requireNonNull(index);

		List<TargetType> targetTypeArguments = getTypeArguments();
		return index < targetTypeArguments.size() ? Optional.of(targetTypeArguments.get(index)) : Optional.empty();
	}
}
