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

import java.sql.Connection;
import java.util.Optional;

/**
 * Performs raw JDBC work with a {@link Connection} managed by {@link Database#useRawConnection(RawConnectionOperation)}.
 * <p>
 * The connection is valid only for the duration of the callback. Do not close it, retain it, or manage transaction lifecycle
 * directly from it.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
@FunctionalInterface
public interface RawConnectionOperation<T> {
	/**
	 * Performs raw JDBC work with the provided connection.
	 *
	 * @param connection connection to use
	 * @return the operation result
	 * @throws Exception if the operation fails
	 */
	@NonNull
	Optional<T> perform(@NonNull Connection connection) throws Exception;
}
