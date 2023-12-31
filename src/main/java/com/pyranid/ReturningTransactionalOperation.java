/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2024 Revetware LLC.
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
import java.util.Optional;

/**
 * Represents a transactional operation capable of returning a value.
 * <p>
 * See {@link TransactionalOperation} for a variant with a {@code void} return type.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@FunctionalInterface
public interface ReturningTransactionalOperation<T> {
	/**
	 * Executes a transactional operation.
	 *
	 * @return the result of operation execution
	 * @throws Exception if an error occurs while executing the transactional operation
	 */
	@Nonnull
	Optional<T> perform() throws Exception;
}