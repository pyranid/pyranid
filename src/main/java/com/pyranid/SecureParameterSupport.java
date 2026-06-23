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
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Internal helpers for secure parameter handling.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.4.0
 */
@ThreadSafe
final class SecureParameterSupport {
	static final int MAX_NESTING_DEPTH = 64;
	@NonNull
	static final String DEFAULT_MASK = "<redacted>";

	private SecureParameterSupport() {
		// Prevents instantiation
	}

	@Nullable
	static Object unwrapSecureAndOptionalParameter(@Nullable Object parameter) {
		return unwrapSecureAndOptionalParameterWithMetadata(parameter).value();
	}

	@NonNull
	static SecureParameterUnwrapResult unwrapSecureAndOptionalParameterWithMetadata(@Nullable Object parameter) {
		Object unwrappedParameter = parameter;
		SecureParameter secureParameter = null;
		int secureParameterDepth = 0;

		while (true) {
			if (unwrappedParameter instanceof SecureParameter currentSecureParameter) {
				if (secureParameterDepth >= MAX_NESTING_DEPTH)
					throw secureParameterNestingDepthExceeded();

				++secureParameterDepth;

				if (secureParameter == null)
					secureParameter = currentSecureParameter;

				unwrappedParameter = currentSecureParameter.getValue().orElse(null);
				continue;
			}

			Object optionalValue = unwrapOptionalValue(unwrappedParameter);

			if (optionalValue == unwrappedParameter)
				return new SecureParameterUnwrapResult(secureParameter, unwrappedParameter);

			unwrappedParameter = optionalValue;
		}
	}

	@NonNull
	static String maskOf(@NonNull SecureParameter secureParameter) {
		requireNonNull(secureParameter);
		return requireNonNull(secureParameter.getMask());
	}

	@Nullable
	static SecureParameter displaySecureParameter(@Nullable Object parameter) {
		Object displayParameter = parameter;

		while (true) {
			if (displayParameter instanceof SecureParameter secureParameter)
				return secureParameter;

			Object optionalValue = unwrapOptionalValue(displayParameter);

			if (optionalValue == displayParameter)
				return null;

			displayParameter = optionalValue;
		}
	}

	@NonNull
	private static IllegalArgumentException secureParameterNestingDepthExceeded() {
		return new IllegalArgumentException(format("SecureParameter nesting exceeds the maximum depth of %s", MAX_NESTING_DEPTH));
	}

	record SecureParameterUnwrapResult(@Nullable SecureParameter secureParameter,
																		 @Nullable Object value) {}

	@Nullable
	private static Object unwrapOptionalValue(@Nullable Object value) {
		if (value == null)
			return null;

		if (value instanceof Optional<?> optional)
			return optional.orElse(null);
		if (value instanceof OptionalInt optionalInt)
			return optionalInt.isPresent() ? optionalInt.getAsInt() : null;
		if (value instanceof OptionalLong optionalLong)
			return optionalLong.isPresent() ? optionalLong.getAsLong() : null;
		if (value instanceof OptionalDouble optionalDouble)
			return optionalDouble.isPresent() ? optionalDouble.getAsDouble() : null;

		return value;
	}
}
