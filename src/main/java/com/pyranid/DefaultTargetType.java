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

import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Package-private default implementation of {@link TargetType}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 3.0.0
 */
@ThreadSafe
class DefaultTargetType implements TargetType {
	@NonNull
	private final Type type;

	DefaultTargetType(@NonNull Type type) {
		requireNonNull(type);
		this.type = requireNonNull(type);
	}

	@Override
	public boolean equals(@Nullable Object object) {
		if (object == null || getClass() != object.getClass())
			return false;

		DefaultTargetType that = (DefaultTargetType) object;
		return Objects.equals(getType(), that.getType());
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(getType());
	}

	@Override
	@NonNull
	public Type getType() {
		return this.type;
	}

	@Override
	@NonNull
	public Class<?> getRawClass() {
		if (type instanceof Class<?> rawClass)
			return rawClass;

		if (type instanceof ParameterizedType parameterizedType)
			return (Class<?>) parameterizedType.getRawType();

		if (type instanceof GenericArrayType genericArrayType) {
			Class<?> componentClass = TargetType.of(genericArrayType.getGenericComponentType()).getRawClass();
			return Array.newInstance(componentClass, 0).getClass();
		}

		if (type instanceof TypeVariable<?> typeVariable) {
			Type[] bounds = typeVariable.getBounds();
			return bounds.length == 0 ? Object.class : TargetType.of(bounds[0]).getRawClass();
		}

		if (type instanceof WildcardType wildcardType) {
			Type[] uppers = wildcardType.getUpperBounds();
			return uppers.length == 0 ? Object.class : TargetType.of(uppers[0]).getRawClass();
		}

		return Object.class;
	}

	@NonNull
	public List<@NonNull TargetType> getTypeArguments() {
		if (getType() instanceof ParameterizedType parameterizedType) {
			Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
			List<TargetType> targetTypes = new ArrayList<>(actualTypeArguments.length);

			for (Type actualTypeArgument : actualTypeArguments)
				targetTypes.add(TargetType.of(actualTypeArgument));

			return Collections.unmodifiableList(targetTypes);
		}

		return List.of();
	}

	@Override
	public String toString() {
		return format("%s{type=%s}", getClass().getSimpleName(), getType().getTypeName());
	}
}
