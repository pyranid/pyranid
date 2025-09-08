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
import java.lang.reflect.Type;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Package-private implementation of {@link TypedParameter}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 3.0.0
 */
@ThreadSafe
class DefaultTypedParameter implements TypedParameter {
	@Nonnull
	private final Type explicitType;
	@Nullable
	private final Object value;

	DefaultTypedParameter(@Nonnull Type explicitType,
												@Nullable Object value) {
		requireNonNull(explicitType);

		this.explicitType = explicitType;
		this.value = value;
	}

	@Nonnull
	@Override
	public Type getExplicitType() {
		return this.explicitType;
	}

	@Nonnull
	@Override
	public Optional<Object> getValue() {
		return Optional.ofNullable(this.value);
	}
}