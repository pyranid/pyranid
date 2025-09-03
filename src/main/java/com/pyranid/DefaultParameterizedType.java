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
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;

import static java.util.Objects.requireNonNull;

/**
 * Package-private implementation of {@link ParameterizedType}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 3.0.0
 */
@ThreadSafe
class DefaultParameterizedType implements ParameterizedType {
	@Nonnull
	private final Type rawType;
	@Nonnull
	private final Type[] typeArguments;
	@Nullable
	private final Type ownerType;

	DefaultParameterizedType(@Nonnull Type rawType,
													 @Nonnull Type[] typeArguments,
													 @Nullable Type ownerType) {
		requireNonNull(rawType);
		requireNonNull(typeArguments);

		this.rawType = rawType;
		this.typeArguments = typeArguments;
		this.ownerType = ownerType;
	}

	@Override
	@Nonnull
	public Type[] getActualTypeArguments() {
		return this.typeArguments.clone();
	}

	@Override
	@Nonnull
	public Type getRawType() {
		return this.rawType;
	}

	@Override
	@Nullable
	public Type getOwnerType() {
		return this.ownerType;
	}

	@Override
	@Nonnull
	public String toString() {
		StringBuilder sb = new StringBuilder();
		appendParameterizedType(sb, this);
		return sb.toString();
	}

	private void appendParameterizedType(@Nonnull StringBuilder sb,
																			 @Nonnull ParameterizedType pt) {
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

	@Nonnull
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

	@Nonnull
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

	@Nonnull
	private String joinTypes(@Nonnull Type[] types) {
		requireNonNull(types);

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < types.length; i++) {
			if (i > 0) sb.append(" & ");
			sb.append(typeToString(types[i]));
		}

		return sb.toString();
	}
}