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

import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Package-private implementation of {@link ParameterizedType}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 3.0.0
 */
@ThreadSafe
class DefaultParameterizedType implements ParameterizedType {
	@NonNull
	private final Type rawType;
	@NonNull
	private final Type @NonNull [] typeArguments;
	@Nullable
	private final Type ownerType;

	DefaultParameterizedType(@NonNull Type rawType,
													 Type @NonNull [] typeArguments,
													 @Nullable Type ownerType) {
		requireNonNull(rawType);
		requireNonNull(typeArguments);

		this.rawType = rawType;
		this.typeArguments = typeArguments;
		this.ownerType = ownerType;
	}

	@Override
	public boolean equals(@Nullable Object object) {
		if (this == object)
			return true;

		if (!(object instanceof ParameterizedType))
			return false;

		ParameterizedType other = (ParameterizedType) object;
		return Objects.equals(getRawType(), other.getRawType())
				&& Objects.equals(getOwnerType(), other.getOwnerType())
				&& Arrays.equals(getActualTypeArguments(), other.getActualTypeArguments());
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(getActualTypeArguments());
		result = 31 * result + Objects.hashCode(getRawType());
		result = 31 * result + Objects.hashCode(getOwnerType());
		return result;
	}

	@Override
	@NonNull
	public Type[] getActualTypeArguments() {
		return this.typeArguments.clone();
	}

	@Override
	@NonNull
	public Type getRawType() {
		return this.rawType;
	}

	@Override
	@Nullable
	public Type getOwnerType() {
		return this.ownerType;
	}

	@Override
	@NonNull
	public String toString() {
		StringBuilder sb = new StringBuilder();
		appendParameterizedType(sb, this);
		return sb.toString();
	}

	private void appendParameterizedType(@NonNull StringBuilder sb,
																			 @NonNull ParameterizedType pt) {
		requireNonNull(sb);
		requireNonNull(pt);

		Type raw = pt.getRawType();
		Type owner = pt.getOwnerType();

		// If we have an owner (nested class), prefer "owner.SimpleName" for readability.
		if (owner != null && raw instanceof Class<?> rc) {
			sb.append(typeToString(owner)).append('.').append(rc.getSimpleName());
		} else {
			sb.append(typeToString(raw));
		}

		Type[] args = pt.getActualTypeArguments();
		if (args.length > 0) {
			sb.append('<');
			for (int i = 0; i < args.length; i++) {
				if (i > 0) sb.append(", ");
				sb.append(typeToString(args[i]));
			}
			sb.append('>');
		}
	}

	@NonNull
	private String typeToString(Type t) {
		requireNonNull(t);

		if (t instanceof Class<?> c) {
			return classToString(c);
		}
		if (t instanceof ParameterizedType p) {
			StringBuilder sb = new StringBuilder();
			appendParameterizedType(sb, p);
			return sb.toString();
		}
		if (t instanceof GenericArrayType ga) {
			return typeToString(ga.getGenericComponentType()) + "[]";
		}
		if (t instanceof WildcardType w) {
			StringBuilder sb = new StringBuilder("?");
			Type[] lowers = w.getLowerBounds();
			Type[] uppers = w.getUpperBounds();

			if (lowers.length > 0) {
				sb.append(" super ").append(joinTypes(lowers));
			} else if (!(uppers.length == 0 || (uppers.length == 1 && uppers[0] == Object.class))) {
				// Only print "extends" when it's not the implicit Object bound
				sb.append(" extends ").append(joinTypes(uppers));
			}
			return sb.toString();
		}
		if (t instanceof TypeVariable<?> v) {
			// Standard behavior is just the variable name (bounds usually omitted in toString)
			return v.getName();
		}
		// Fallback
		return String.valueOf(t);
	}

	@NonNull
	private String classToString(Class<?> c) {
		requireNonNull(c);

		if (c.isArray())
			return classToString(c.getComponentType()) + "[]";

		// Use dotted form for nested types (e.g., java.util.Map.Entry)
		Class<?> declaring = c.getDeclaringClass();

		if (declaring != null)
			return declaring.getName() + '.' + c.getSimpleName();

		return c.getName();
	}

	@NonNull
	private String joinTypes(Type @NonNull [] types) {
		requireNonNull(types);

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < types.length; i++) {
			if (i > 0) sb.append(" & ");
			sb.append(typeToString(types[i]));
		}

		return sb.toString();
	}
}
