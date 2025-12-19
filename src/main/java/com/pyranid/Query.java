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
import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Fluent builder for SQL statements.
 * <p>
 * Obtain instances via {@link Database#query(String)}.
 * Positional parameters via {@code ?} are not supported; use named parameters (e.g. {@code :id}) and {@link #bind(String, Object)}.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Query returning one row
 * Optional<Employee> employee = database.query("SELECT * FROM employee WHERE id = :id")
 *   .bind("id", 42)
 *   .fetchObject(Employee.class);
 *
 * // Query returning multiple rows
 * List<Employee> employees = database.query("SELECT * FROM employee WHERE dept = :dept")
 *   .bind("dept", "Engineering")
 *   .fetchList(Employee.class);
 *
 * // DML with no result
 * long rowsAffected = database.query("UPDATE employee SET active = :active WHERE id = :id")
 *   .bind("id", 42)
 *   .bind("active", false)
 *   .execute();
 *
 * // DML with RETURNING clause
 * Optional<Employee> updated = database.query("UPDATE employee SET salary = :salary WHERE id = :id RETURNING *")
 *   .bind("id", 42)
 *   .bind("salary", new BigDecimal("150000"))
 *   .executeForObject(Employee.class);
 * }</pre>
 * <p>
 * Implementations of this interface are intended for use by a single thread.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @see Database#query(String)
 * @since 3.1.0
 */
@NotThreadSafe
public interface Query {
	/**
	 * Binds a named parameter to a value.
	 *
	 * @param name  the parameter name (without the leading {@code :})
	 * @param value the value to bind (may be {@code null})
	 * @return this builder, for chaining
	 */
	@Nonnull
	Query bind(@Nonnull String name,
						 @Nullable Object value);

	/**
	 * Binds all entries from the given map as named parameters.
	 *
	 * @param parameters map of parameter names to values
	 * @return this builder, for chaining
	 */
	@Nonnull
	Query bindAll(@Nonnull Map<String, Object> parameters);

	/**
	 * Associates an identifier with this query for logging/diagnostics.
	 * <p>
	 * If not called, a default ID will be generated.
	 *
	 * @param id the identifier
	 * @return this builder, for chaining
	 */
	@Nonnull
	Query id(@Nullable Object id);

	/**
	 * Executes the query and returns a single result.
	 *
	 * @param resultType the type to marshal each row to
	 * @param <T>        the result type
	 * @return the single result, or empty if no rows
	 * @throws DatabaseException if more than one row is returned
	 */
	@Nonnull
	<T> Optional<T> fetchObject(@Nonnull Class<T> resultType);

	/**
	 * Executes the query and returns all results as a list.
	 *
	 * @param resultType the type to marshal each row to
	 * @param <T>        the result type
	 * @return list of results (empty if no rows)
	 */
	@Nonnull
	<T> List<T> fetchList(@Nonnull Class<T> resultType);

	/**
	 * Executes a DML statement (INSERT, UPDATE, DELETE) with no resultset.
	 *
	 * @return the number of rows affected
	 */
	@Nonnull
	Long execute();

	/**
	 * Executes a DML statement that returns a single row (e.g., with {@code RETURNING} clause).
	 *
	 * @param resultType the type to marshal the row to
	 * @param <T>        the result type
	 * @return the single result, or empty if no rows
	 * @throws DatabaseException if more than one row is returned
	 */
	@Nonnull
	<T> Optional<T> executeForObject(@Nonnull Class<T> resultType);

	/**
	 * Executes a DML statement that returns multiple rows (e.g., with {@code RETURNING} clause).
	 *
	 * @param resultType the type to marshal each row to
	 * @param <T>        the result type
	 * @return list of results
	 */
	@Nonnull
	<T> List<T> executeForList(@Nonnull Class<T> resultType);
}
