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
import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Contract for a factory that creates instances given a type.
 * <p>
 * Useful for resultset mapping, where each row in the resultset might require a new instance.
 * <p>
 * Implementors are suggested to employ application-specific strategies, such as having a DI container handle instance
 * creation.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
public interface InstanceProvider {
	/**
	 * Provides an instance of the given {@code instanceType}.
	 * <p>
	 * Whether the instance is new every time or shared/reused is implementation-dependent.
	 *
	 * @param <T>           instance type token
	 * @param instanceType the type of instance to create
	 * @return an instance of the given {@code instanceType}
	 */
	@Nonnull
	<T> T provide(@Nonnull StatementContext<T> statementContext,
								@Nonnull Class<T> instanceType);

	/**
	 * Provides an instance of the given {@code recordType}.
	 * <p>
	 * Whether the instance is new every time or shared/reused is implementation-dependent.
	 *
	 * @param <T>         instance type token
	 * @param recordType the type of instance to create (must be a record)
	 * @param initargs    values used to construct the record instance
	 * @return an instance of the given {@code recordType}
	 * @since 2.0.0
	 */
	@Nonnull
	default <T extends Record> T provideRecord(@Nonnull StatementContext<T> statementContext,
																						 @Nonnull Class<T> recordType,
																						 @Nullable Object... initargs) {
		requireNonNull(statementContext);
		requireNonNull(recordType);

		try {
			// Find the canonical constructor for the record.
			// Hat tip to https://stackoverflow.com/a/67127067
			Class<?>[] componentTypes = Arrays.stream(recordType.getRecordComponents())
					.map(rc -> rc.getType())
					.toArray(Class<?>[]::new);

			Constructor<T> constructor = recordType.getDeclaredConstructor(componentTypes);
			return constructor.newInstance(initargs);
		} catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
						 IllegalArgumentException | InvocationTargetException e) {
			throw new DatabaseException(String.format("Unable to instantiate Record type %s with args %s", recordType,
					initargs == null ? "[none]" : Arrays.asList(initargs)), e);
		}
	}
}