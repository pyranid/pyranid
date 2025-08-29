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
 * Encapsulates {@link java.sql.PreparedStatement} parameter data meant to be bound to a formal <a href="https://docs.oracle.com/en/java/javase/24/docs/api/java.sql/java/sql/Array.html" target="_blank">{@code java.sql.Array}</a> type by {@link PreparedStatementBinder}.
 * <p>
 * The {@code baseTypeName} corresponds to the value of <a href="https://docs.oracle.com/en/java/javase/24/docs/api/java.sql/java/sql/Array.html#getBaseTypeName()" target="_blank">{@code java.sql.Array#getBaseTypeName()}</a>
 * and is database-specific.
 * <p>
 * You may determine available {@code baseTypeName} values for your database by examining metadata exposed via {@link Database#examineDatabaseMetaData(DatabaseMetaDataExaminer)}.
 * <p>
 * Stardard instances may be constructed via {@link Parameters#arrayOf(String, Object[])} and {@link Parameters#arrayOf(String, List)}.
 * <p>
 * Implementations should be threadsafe.
 *
 * @param <E> the Java element type of the array; each element must be bindable to the SQL element type named by {@link #getBaseTypeName()} by the active {@link PreparedStatementBinder}.
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 2.1.0
 */
@ThreadSafe
public interface ArrayParameter<E> {
	/**
	 * Gets the element type of this SQL ARRAY, which corresponds to the value of <a href="https://docs.oracle.com/en/java/javase/24/docs/api/java.sql/java/sql/Array.html#getBaseTypeName()" target="_blank">{@code java.sql.Array#getBaseTypeName()}</a>
	 * and is database-specific.
	 *
	 * @return the element type of this SQL ARRAY
	 */
	@Nonnull
	String getBaseTypeName();

	/**
	 * Gets the elements of this SQL ARRAY.
	 *
	 * @return the elements of this SQL ARRAY
	 */
	@Nonnull
	Optional<E[]> getElements();
}