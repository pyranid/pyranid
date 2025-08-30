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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

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
}