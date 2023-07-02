/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2023 Revetware LLC.
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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Basic implementation of {@link InstanceProvider}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public class DefaultInstanceProvider implements InstanceProvider {
	@Override
	@Nonnull
	public <T> T provide(@Nonnull StatementContext<T> statementContext,
											 @Nonnull Class<T> instanceType) {
		requireNonNull(statementContext);
		requireNonNull(instanceType);

		try {
			return instanceType.getDeclaredConstructor().newInstance();
		} catch (Exception e) {
			throw new RuntimeException(format(
					"Unable to create an instance of %s. Please verify that %s has a public no-argument constructor",
					instanceType, instanceType.getSimpleName()), e);
		}
	}
}