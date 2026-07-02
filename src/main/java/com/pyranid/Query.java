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
import java.time.Duration;
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
 * Parameter-looking text inside SQL string literals, quoted identifiers, comments, PostgreSQL dollar-quoted strings,
 * and SQL Server-style bracket-quoted identifiers is ignored. PostgreSQL JSONB/hstore {@code ?}, {@code ?|}, and
 * {@code ?&} operators are supported and are escaped automatically for pgjdbc when the {@link Database} is configured
 * or detected as PostgreSQL.
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
	Query bindAll(@NonNull Map<@NonNull String, @Nullable Object> parameters);

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
	 * Configures the JDBC query timeout for this query.
	 * <p>
	 * This maps to {@link java.sql.Statement#setQueryTimeout(int)}. {@code null} leaves the timeout unset so any
	 * {@link Database.Builder#queryTimeout(Duration)} default applies. {@link Duration#ZERO} disables the JDBC timeout.
	 * Positive sub-second durations are rounded up to one second because JDBC accepts whole seconds.
	 *
	 * @param queryTimeout timeout to apply, or {@code null} to inherit the database default
	 * @return this builder, for chaining
	 * @since 4.2.0
	 */
	@NonNull
	default Query queryTimeout(@Nullable Duration queryTimeout) {
		throw new UnsupportedOperationException("queryTimeout is not supported by this Query implementation");
	}

	/**
	 * Configures the JDBC fetch size for this query.
	 * <p>
	 * This maps to {@link java.sql.Statement#setFetchSize(int)}. {@code null} leaves the fetch size unset so any
	 * {@link Database.Builder#fetchSize(Integer)} default applies. A value of {@code 0} uses the driver's default
	 * fetch-size behavior.
	 *
	 * @param fetchSize fetch size to apply, or {@code null} to inherit the database default
	 * @return this builder, for chaining
	 * @since 4.2.0
	 */
	@NonNull
	default Query fetchSize(@Nullable Integer fetchSize) {
		throw new UnsupportedOperationException("fetchSize is not supported by this Query implementation");
	}

	/**
	 * Configures the JDBC maximum row count for this query.
	 * <p>
	 * This maps to {@link java.sql.Statement#setMaxRows(int)}. {@code null} leaves the maximum row count unset so any
	 * {@link Database.Builder#maxRows(Integer)} default applies. A value of {@code 0} disables the JDBC row limit.
	 *
	 * @param maxRows maximum rows to apply, or {@code null} to inherit the database default
	 * @return this builder, for chaining
	 * @since 4.2.0
	 */
	@NonNull
	default Query maxRows(@Nullable Integer maxRows) {
		throw new UnsupportedOperationException("maxRows is not supported by this Query implementation");
	}

	/**
	 * Configures the maximum number of parameter groups to send in each JDBC batch execution.
	 * <p>
	 * This setting applies only to {@link #executeBatch(List)}. {@code null} leaves batch execution unchunked,
	 * preserving the default behavior of sending all parameter groups in one JDBC batch. A positive value causes
	 * Pyranid to execute multiple JDBC batches as needed and flatten the returned update counts in input order. If this
	 * setting is specified and a non-batch terminal operation is invoked, Pyranid throws {@link IllegalStateException}.
	 * <p>
	 * Chunking is performed by Pyranid. JDBC drivers still execute each chunk as a normal JDBC batch.
	 * If a later chunk fails outside an explicit transaction, earlier chunks may already be committed depending on
	 * autocommit and driver behavior. Wrap chunked batches in {@link Database#transaction(TransactionalOperation)} when
	 * all-or-nothing behavior is required.
	 *
	 * @param batchChunkSize maximum parameter groups per JDBC batch execution, or {@code null} to execute one batch
	 * @return this builder, for chaining
	 * @throws IllegalArgumentException if {@code batchChunkSize} is less than or equal to {@code 0}
	 * @since 4.2.0
	 */
	@NonNull
	default Query batchChunkSize(@Nullable Integer batchChunkSize) {
		throw new UnsupportedOperationException("batchChunkSize is not supported by this Query implementation");
	}

	/**
	 * Overrides the {@link Database}-wide {@link ResultSetMapper} for this query only.
	 * <p>
	 * This enables per-query inline mapping — for example, projecting an ad-hoc join or computed columns —
	 * without configuring a database-wide mapper. {@link ResultSetMapper} is a functional interface, so a
	 * lambda works: <pre>{@code  database.query("SELECT name, COUNT(*) AS total FROM employee GROUP BY name")
	 *   .resultSetMapper((ctx, rs, type, ip) -> Optional.of(type.cast(new NameCount(rs.getString(1), rs.getLong(2)))))
	 *   .fetchList(NameCount.class);}</pre>
	 * The override applies to every row this query maps (including {@link #fetchStream} rows and DML-returning
	 * results). Other queries on the same {@link Database} are unaffected. Metrics, statement logging, and
	 * exception diagnostics behave identically with an override present.
	 *
	 * @param resultSetMapper the mapper to use for this query, or {@code null} to inherit the database-wide mapper
	 * @return this builder, for chaining
	 * @since 4.4.1
	 */
	@NonNull
	default Query resultSetMapper(@Nullable ResultSetMapper resultSetMapper) {
		throw new UnsupportedOperationException("resultSetMapper is not supported by this Query implementation");
	}

	/**
	 * Overrides the {@link Database}-wide {@link PreparedStatementBinder} for this query only.
	 * <p>
	 * The override receives every non-null parameter for this query — including expanded IN-list elements and
	 * each batch group's parameters. As with the database-wide SPI, {@link SecureParameter} and {@link java.util.Optional}
	 * wrappers are unwrapped by Pyranid <em>before</em> the binder is invoked, so custom binders receive bound-ready
	 * raw values and need no unwrap logic. {@code null} parameters never reach the binder; Pyranid binds them via
	 * {@link java.sql.PreparedStatement#setNull(int, int)} even when an override is present.
	 * <p>
	 * Other queries on the same {@link Database} are unaffected.
	 *
	 * @param preparedStatementBinder the binder to use for this query, or {@code null} to inherit the database-wide binder
	 * @return this builder, for chaining
	 * @since 4.4.1
	 */
	@NonNull
	default Query preparedStatementBinder(@Nullable PreparedStatementBinder preparedStatementBinder) {
		throw new UnsupportedOperationException("preparedStatementBinder is not supported by this Query implementation");
	}

	/**
	 * Customizes the {@link java.sql.PreparedStatement} before execution.
	 * <p>
	 * If called multiple times, the most recent customizer wins. The customizer runs after database-wide statement
	 * settings and this query's {@link #queryTimeout(Duration)}, {@link #fetchSize(Integer)}, and
	 * {@link #maxRows(Integer)} settings, so it can override them when needed.
	 * <p>
	 * For dialect-managed streams, Pyranid may apply driver-specific stream settings after this callback
	 * unless this query explicitly configured {@link #fetchSize(Integer)}.
	 * <p>
	 * For driver-specific cancellation beyond {@link #queryTimeout(Duration)}, application code may capture the
	 * {@link java.sql.PreparedStatement} here and call {@link java.sql.Statement#cancel()} from its own cancellation path.
	 * Cancellation behavior is JDBC-driver-specific.
	 *
	 * @param preparedStatementCustomizer customization callback
	 * @return this builder, for chaining
	 */
	@NonNull
	Query customize(@NonNull PreparedStatementCustomizer preparedStatementCustomizer);

	/**
	 * Acquires a result type token for fetching rows as insertion-ordered {@code Map<String, Object>} instances,
	 * usable anywhere a result type token is accepted.
	 * <p>
	 * Java's type erasure means there is no {@code Map<String, Object>.class} literal — a raw {@code Map.class}
	 * token can only ever infer the raw {@code Map} type at fetch sites. This method returns the same runtime
	 * {@code Map.class} token, statically typed as {@code Class<Map<String, Object>>} so results are properly
	 * parameterized without caller-side casts:
	 * <pre>{@code  List<Map<String, Object>> rows = database.query("SELECT * FROM car")
	 *   .fetchList(Query.mapRowType());}</pre>
	 * This is safe because Pyranid's default mapping produces {@code LinkedHashMap<String, Object>} rows for this
	 * token. Note that a custom {@link ResultSetMapper} receiving this token observes the raw {@code Map.class}.
	 *
	 * @return a {@code Map<String, Object>} result type token
	 * @since 4.4.1
	 */
	@NonNull
	@SuppressWarnings("unchecked")
	static Class<Map<String, Object>> mapRowType() {
		return (Class<Map<String, Object>>) (Class<?>) Map.class;
	}

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
	<T> List<@Nullable T> fetchList(@NonNull Class<T> resultType);

	/**
	 * Executes the query and provides a {@link Stream} backed by the underlying {@link java.sql.ResultSet}.
	 * <p>
	 * This approach is useful for processing very large resultsets (e.g. millions of rows), where it's impractical to load all rows into memory at once.
	 * <p>
	 * JDBC resources are closed automatically when {@code streamFunction} returns (or throws), so the stream must be fully consumed
	 * within that callback. Do not escape the stream from the function.
	 * <p>
	 * The stream must be consumed within the scope of the transaction or connection that created it.
	 * If the stream participates in a Pyranid transaction, it must also be closed by the thread that opened it.
	 * <p>
	 * Supported dialects apply driver-specific streaming setup automatically. PostgreSQL streams use a positive JDBC
	 * fetch size and an autocommit-disabled connection when no Pyranid transaction is active; MySQL streams use
	 * forward-only/read-only statements and the MySQL streaming fetch-size sentinel; MariaDB streams use
	 * forward-only/read-only statements without the MySQL sentinel. Use {@link #fetchSize(Integer)} to override
	 * fetch-size behavior when needed.
	 *
	 * @param resultType     the type to marshal each row to
	 * @param streamFunction function that consumes the stream and returns a result
	 * @param <T>            the result type
	 * @param <R>            the return type
	 * @return the value returned by {@code streamFunction}
	 */
	@Nullable
	<T, R> R fetchStream(@NonNull Class<T> resultType,
											 @NonNull Function<Stream<@Nullable T>, R> streamFunction);


	/**
	 * Executes a DML statement (INSERT, UPDATE, DELETE) with no resultset.
	 *
	 * @return the number of rows affected
	 */
	@NonNull
	Long execute();

	/**
	 * Executes a DML statement and maps a single JDBC-generated key row.
	 * <p>
	 * This uses JDBC {@link java.sql.Statement#RETURN_GENERATED_KEYS}. It is intended for database-generated values such
	 * as identity/auto-increment primary keys. If your SQL returns rows directly via database syntax such as PostgreSQL
	 * {@code RETURNING} or SQL Server {@code OUTPUT}, use {@link #executeForObject(Class)} instead.
	 * <p>
	 * For SQL Server multi-row identity inserts, use {@code OUTPUT} with {@link #executeForList(Class)} instead of JDBC
	 * generated keys. For MySQL {@code INSERT ... ON DUPLICATE KEY UPDATE}, use the {@code LAST_INSERT_ID(id)} idiom if
	 * you need the existing row's ID returned on the update path.
	 * <p>
	 * Oracle requires explicit generated-key column names; use {@link #executeReturningGeneratedKey(Class, String...)}
	 * instead.
	 *
	 * @param resultType the type to marshal the generated-key row to
	 * @param <T>        the result type
	 * @return the single generated key row, or empty if the driver returns no generated keys
	 * @throws DatabaseException if more than one generated-key row is returned
	 * @since 4.2.0
	 */
	@NonNull
	default <T> Optional<T> executeReturningGeneratedKey(@NonNull Class<T> resultType) {
		throw new UnsupportedOperationException("executeReturningGeneratedKey is not supported by this Query implementation");
	}

	/**
	 * Executes a DML statement and maps a single JDBC-generated key row.
	 * <p>
	 * This uses JDBC {@link java.sql.Connection#prepareStatement(String, String[])} with the supplied key column names.
	 * Some drivers require column names to return generated keys for specific columns, especially when more than one
	 * generated value is available. If {@code keyColumnNames} is empty, this behaves like
	 * {@link #executeReturningGeneratedKey(Class)}, except for dialects such as Oracle which require explicit names.
	 * <p>
	 * For SQL Server multi-row identity inserts, use {@code OUTPUT} with {@link #executeForList(Class)} instead of JDBC
	 * generated keys. For MySQL {@code INSERT ... ON DUPLICATE KEY UPDATE}, use the {@code LAST_INSERT_ID(id)} idiom if
	 * you need the existing row's ID returned on the update path.
	 *
	 * @param resultType      the type to marshal the generated-key row to
	 * @param keyColumnNames generated-key column names requested from the driver
	 * @param <T>             the result type
	 * @return the single generated key row, or empty if the driver returns no generated keys
	 * @throws DatabaseException if more than one generated-key row is returned
	 * @since 4.2.0
	 */
	@NonNull
	default <T> Optional<T> executeReturningGeneratedKey(@NonNull Class<T> resultType,
																											 @NonNull String @NonNull ... keyColumnNames) {
		throw new UnsupportedOperationException("executeReturningGeneratedKey is not supported by this Query implementation");
	}

	/**
	 * Executes a DML statement and maps all JDBC-generated key rows.
	 * <p>
	 * This uses JDBC {@link java.sql.Statement#RETURN_GENERATED_KEYS}. It is intended for database-generated values such
	 * as identity/auto-increment primary keys. If your SQL returns rows directly via database syntax such as PostgreSQL
	 * {@code RETURNING} or SQL Server {@code OUTPUT}, use {@link #executeForList(Class)} instead.
	 * <p>
	 * For SQL Server multi-row identity inserts, use {@code OUTPUT} with {@link #executeForList(Class)} instead of JDBC
	 * generated keys. For MySQL {@code INSERT ... ON DUPLICATE KEY UPDATE}, use the {@code LAST_INSERT_ID(id)} idiom if
	 * you need the existing row's ID returned on the update path.
	 * <p>
	 * Oracle requires explicit generated-key column names; use {@link #executeReturningGeneratedKeys(Class, String...)}
	 * instead.
	 *
	 * @param resultType the type to marshal each generated-key row to
	 * @param <T>        the result type
	 * @return list of generated key rows
	 * @since 4.2.0
	 */
	@NonNull
	default <T> List<@Nullable T> executeReturningGeneratedKeys(@NonNull Class<T> resultType) {
		throw new UnsupportedOperationException("executeReturningGeneratedKeys is not supported by this Query implementation");
	}

	/**
	 * Executes a DML statement and maps all JDBC-generated key rows.
	 * <p>
	 * This uses JDBC {@link java.sql.Connection#prepareStatement(String, String[])} with the supplied key column names.
	 * Some drivers require column names to return generated keys for specific columns, especially when more than one
	 * generated value is available. If {@code keyColumnNames} is empty, this behaves like
	 * {@link #executeReturningGeneratedKeys(Class)}, except for dialects such as Oracle which require explicit names.
	 * <p>
	 * For SQL Server multi-row identity inserts, use {@code OUTPUT} with {@link #executeForList(Class)} instead of JDBC
	 * generated keys. For MySQL {@code INSERT ... ON DUPLICATE KEY UPDATE}, use the {@code LAST_INSERT_ID(id)} idiom if
	 * you need the existing row's ID returned on the update path.
	 *
	 * @param resultType      the type to marshal each generated-key row to
	 * @param keyColumnNames generated-key column names requested from the driver
	 * @param <T>             the result type
	 * @return list of generated key rows
	 * @since 4.2.0
	 */
	@NonNull
	default <T> List<@Nullable T> executeReturningGeneratedKeys(@NonNull Class<T> resultType,
																														 @NonNull String @NonNull ... keyColumnNames) {
		throw new UnsupportedOperationException("executeReturningGeneratedKeys is not supported by this Query implementation");
	}

	/**
	 * Executes a DML statement in batch over groups of named parameters.
	 * <p>
	 * Any parameters already bound on this {@code Query} apply to all groups; group values override them.
	 * Each group must provide a complete set of parameter values after merging; groups must be non-null and
	 * expand to the same number of JDBC parameters (for example, IN-list sizes must match).
	 *
	 * @param parameterGroups groups of named parameter values (without the leading {@code :})
	 * @return the number of rows affected by the SQL statement per-group
	 */
	@NonNull
	List<Long> executeBatch(@NonNull List<@NonNull Map<@NonNull String, @Nullable Object>> parameterGroups);

	/**
	 * Executes a DML statement that returns a single row (for example, with PostgreSQL/SQLite/MariaDB
	 * {@code RETURNING} or SQL Server {@code OUTPUT}).
	 *
	 * @param resultType the type to marshal the row to
	 * @param <T>        the result type
	 * @return the single result, or empty if no rows
	 * @throws DatabaseException if more than one row is returned
	 */
	@NonNull
	<T> Optional<T> executeForObject(@NonNull Class<T> resultType);

	/**
	 * Executes a DML statement that returns multiple rows (for example, with PostgreSQL/SQLite/MariaDB
	 * {@code RETURNING} or SQL Server {@code OUTPUT}).
	 *
	 * @param resultType the type to marshal each row to
	 * @param <T>        the result type
	 * @return list of results
	 */
	@NonNull
	<T> List<@Nullable T> executeForList(@NonNull Class<T> resultType);
}
