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

import org.jspecify.annotations.NonNull;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Contract for binding parameters to SQL prepared statements.
 * <p>
 * A production-ready concrete implementation is available via the following static methods:
 * <ul>
 *   <li>{@link #withDefaultConfiguration()}</li>
 *   <li>{@link #withCustomParameterBinders(List)}</li>
 * </ul>
 * How to acquire an instance:
 * <pre>{@code  // With out-of-the-box defaults
 * PreparedStatementBinder default = PreparedStatementBinder.withDefaultConfiguration();
 *
 * // Customized
 * PreparedStatementBinder custom = PreparedStatementBinder.withCustomParameterBinders(List.of(...));}</pre>
 * Or, implement your own: <pre>{@code  PreparedStatementBinder myImpl = new PreparedStatementBinder() {
 *   @Override
 *   <T> void bindParameter(
 *     @NonNull StatementContext<T> statementContext,
 *     @NonNull PreparedStatement preparedStatement,
 *     @NonNull Integer parameterIndex,
 *     @NonNull Object parameter
 *   ) throws SQLException {
 *     // TODO: your own code that binds the parameter at the specified index to the PreparedStatement
 *   }
 * };}</pre>
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@FunctionalInterface
public interface PreparedStatementBinder {
	/**
	 * Binds a single parameter to a SQL prepared statement.
	 * <p>
	 * This function is only invoked when {@code parameter} is non-null.
	 *
	 * @param preparedStatement the prepared statement to bind to
	 * @param statementContext  current SQL context
	 * @param parameterIndex    the index of the parameter we are binding
	 * @param parameter         the parameter we are binding to the {@link PreparedStatement}, if any
	 * @throws SQLException if an error occurs during binding
	 */
	<T> void bindParameter(@NonNull StatementContext<T> statementContext,
												 @NonNull PreparedStatement preparedStatement,
												 @NonNull Integer parameterIndex,
												 @NonNull Object parameter) throws SQLException;

	/**
	 * Acquires a concrete implementation of this interface with out-of-the-box defaults.
	 * <p>
	 * The returned instance is thread-safe.
	 *
	 * @return a concrete implementation of this interface with out-of-the-box defaults
	 */
	@NonNull
	static PreparedStatementBinder withDefaultConfiguration() {
		return new DefaultPreparedStatementBinder();
	}

	/**
	 * Acquires a concrete implementation of this interface, specifying "surgical" per-type custom parameter binders.
	 * <p>
	 * The returned instance is thread-safe.
	 *
	 * @param customParameterBinders the "surgical" per-type custom parameter binders
	 * @return a concrete implementation of this interface
	 */
	@NonNull
	static PreparedStatementBinder withCustomParameterBinders(@NonNull List<CustomParameterBinder> customParameterBinders) {
		requireNonNull(customParameterBinders);
		return new DefaultPreparedStatementBinder(customParameterBinders);
	}
}