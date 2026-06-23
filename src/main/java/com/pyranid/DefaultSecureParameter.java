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
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Package-private implementation of {@link SecureParameter}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.4.0
 */
@ThreadSafe
final class DefaultSecureParameter implements SecureParameter {
	@Nullable
	private final Object value;
	@NonNull
	private final String mask;

	DefaultSecureParameter(@Nullable Object value,
												 @NonNull String mask) {
		requireNonNull(mask);

		this.value = value;
		this.mask = mask;
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.value, getMask());
	}

	@Override
	public boolean equals(@Nullable Object object) {
		if (this == object)
			return true;

		if (!(object instanceof DefaultSecureParameter defaultSecureParameter))
			return false;

		return Objects.equals(this.value, defaultSecureParameter.value)
				&& Objects.equals(getMask(), defaultSecureParameter.getMask());
	}

	@Override
	public String toString() {
		return getMask();
	}

	@NonNull
	@Override
	public Optional<Object> getValue() {
		return Optional.ofNullable(this.value);
	}

	@NonNull
	@Override
	public String getMask() {
		return this.mask;
	}
}
