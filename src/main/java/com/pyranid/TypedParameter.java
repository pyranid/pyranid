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
import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Special "type carrier" which avoids generic type erasure at runtime when {@link PreparedStatementBinder} binds parameters to {@link java.sql.PreparedStatement}.
 * <p>
 * Examples of where this is used:
 * <ul>
 *   <li>{@link Parameters#listOf(Class, List)}</li>
 *   <li>{@link Parameters#setOf(Class, Set)}</li>
 *   <li>{@link Parameters#mapOf(Class, Class, Map)}</li>
 *   <li>{@link Parameters#arrayOf(Class, Object)}</li>
 * </ul>
 * Implementations should be threadsafe.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 3.0.0
 */
@ThreadSafe
public interface TypedParameter {
	/**
	 * Gets the full type information for this parameter, including how it's parameterized.
	 *
	 * @return the full type information for this parameter
	 */
	@Nonnull
	Type getExplicitType();

	/**
	 * Gets the value of this parameter.
	 *
	 * @return the value of this parameter, or {@link Optional#empty()} if none
	 */
	@Nonnull
	Optional<Object> getValue();
}
