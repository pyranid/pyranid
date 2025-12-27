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

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

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
 * @since 4.0.0
 */
@NotThreadSafe
public interface Query {
	/**
	 * Binds a named parameter to a value.
	 *
	 * @param name  the parameter name (without the leading {@code :})
	 * @param value the value to bind (may be {@code null}). Raw {@link java.util.Collection} and array
	 *              values are not expanded; use {@link Parameters#inList(java.util.Collection)},
	 *              {@link Parameters#sqlArrayOf(String, Object[])}, or {@link Parameters#arrayOf(Class, Object)}
	 *              as appropriate.
	 * @return this builder, for chaining
	 */
	@NonNull
	Query bind(@NonNull String name,
						 @Nullable Object value);

	/**
	 * Binds all entries from the given map as named parameters.
	 *
	 * @param parameters map of parameter names to values
	 * @return this builder, for chaining
	 */
	@NonNull
	Query bindAll(@NonNull Map<String, Object> parameters);

	/**
	 * Associates an identifier with this query for logging/diagnostics.
	 * <p>
	 * If not called, a default ID will be generated.
	 *
	 * @param id the identifier
	 * @return this builder, for chaining
	 */
	@NonNull
	Query id(@Nullable Object id);

	/**
	 * Customizes the {@link java.sql.PreparedStatement} before execution.
	 * <p>
	 * If called multiple times, the most recent customizer wins.
	 *
	 * @param preparedStatementCustomizer customization callback
	 * @return this builder, for chaining
	 */
	@NonNull
	Query customize(@NonNull PreparedStatementCustomizer preparedStatementCustomizer);

	/**
	 * Executes the query and returns a single result.
	 *
	 * @param resultType the type to marshal each row to
	 * @param <T>        the result type
	 * @return the single result, or empty if no rows
	 * @throws DatabaseException if more than one row is returned
	 */
	@NonNull
	<T> Optional<T> fetchObject(@NonNull Class<T> resultType);

	/**
	 * Executes the query and returns all results as a list.
	 *
	 * @param resultType the type to marshal each row to
	 * @param <T>        the result type
	 * @return list of results (empty if no rows)
	 */
	@NonNull
	<T> List<T> fetchList(@NonNull Class<T> resultType);

	/**
	 * Executes the query and provides a {@link Stream} backed by the underlying {@link java.sql.ResultSet}.
	 * <p>
	 * This approach is useful for processing very large resultsets (e.g. millions of rows), where it's impractical to load all rows into memory at once.
	 * <p>
	 * JDBC resources are closed automatically when {@code streamFunction} returns (or throws), so the stream must be fully consumed
	 * within that callback. Do not escape the stream from the function.
	 * <p>
	 * The stream must be consumed within the scope of the transaction or connection that created it.
	 *
	 * @param resultType     the type to marshal each row to
	 * @param streamFunction function that consumes the stream and returns a result
	 * @param <T>            the result type
	 * @param <R>            the return type
	 * @return the value returned by {@code streamFunction}
	 */
	@Nullable
	<T, R> R fetchStream(@NonNull Class<T> resultType,
											 @NonNull Function<Stream<T>, R> streamFunction);


	/**
	 * Executes a DML statement (INSERT, UPDATE, DELETE) with no resultset.
	 *
	 * @return the number of rows affected
	 */
	@NonNull
	Long execute();

	/**
	 * Executes a DML statement in batch over groups of named parameters.
	 * <p>
	 * Any parameters already bound on this {@code Query} apply to all groups; group values override them.
	 *
	 * @param parameterGroups groups of named parameter values (without the leading {@code :})
	 * @return the number of rows affected by the SQL statement per-group
	 */
	@NonNull
	List<Long> executeBatch(@NonNull List<Map<String, Object>> parameterGroups);

	/**
	 * Executes a DML statement that returns a single row (e.g., with {@code RETURNING} clause).
	 *
	 * @param resultType the type to marshal the row to
	 * @param <T>        the result type
	 * @return the single result, or empty if no rows
	 * @throws DatabaseException if more than one row is returned
	 */
	@NonNull
	<T> Optional<T> executeForObject(@NonNull Class<T> resultType);

	/**
	 * Executes a DML statement that returns multiple rows (e.g., with {@code RETURNING} clause).
	 *
	 * @param resultType the type to marshal each row to
	 * @param <T>        the result type
	 * @return list of results
	 */
	@NonNull
	<T> List<T> executeForList(@NonNull Class<T> resultType);
}
