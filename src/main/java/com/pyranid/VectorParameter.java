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
import java.util.List;
import java.util.Optional;

/**
 * Encapsulates {@link java.sql.PreparedStatement} parameter data meant to be bound to a vector type (for example, PostgreSQL's <a href="https://github.com/pgvector/pgvector" target="_blank">{@code pgvector}</a>), by {@link PreparedStatementBinder}.
 * <p>
 * Stardard instances may be constructed via the following factory methods:
 * <ul>
 *   <li>{@link Parameters#vectorOfFloats(float[])}</li>
 *   <li>{@link Parameters#vectorOfFloats(List)}</li>
 *   <li>{@link Parameters#vectorOfDoubles(double[])}</li>
 *   <li>{@link Parameters#vectorOfDoubles(List)}</li>
 *   <li>{@link Parameters#vectorOfBigDecimals(List)}</li>
 * </ul>
 * Implementations should be threadsafe.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 3.0.0
 */
@ThreadSafe
public interface VectorParameter {
	/**
	 * Gets the elements of this vector.
	 *
	 * @return the elements of this vector
	 */
	@Nonnull
	Optional<double[]> getElements();
}