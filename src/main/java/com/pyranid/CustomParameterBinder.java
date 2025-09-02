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
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Enables {@link java.sql.PreparedStatement} parameter binding customization via {@link PreparedStatementBinder}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 3.0.0
 */
public interface CustomParameterBinder {
	/**
	 * Performs custom binding of a {@link PreparedStatement} value given a {@code value} and its {@code index} when {@link #appliesTo(TargetType)} is {@code true}.
	 * <p>
	 * This function is only invoked when {@code parameter} is non-null.
	 *
	 * @param statementContext  current SQL context
	 * @param preparedStatement the prepared statement to bind to
	 * @param parameterIndex    1-based parameter index at which to perform the binding
	 * @param parameter         the parameter to bind at the specified index
	 * @return {@link BindingResult#HANDLED} if the custom binding was performed, or {@link BindingResult#FALLBACK} to fall back to default {@link PreparedStatementBinder} behavior
	 * @throws SQLException if an error occurs during binding
	 */
	@Nonnull
	BindingResult bind(@Nonnull StatementContext<?> statementContext,
										 @Nonnull PreparedStatement preparedStatement,
										 @Nonnull Integer parameterIndex,
										 @Nonnull Object parameter) throws SQLException;

	/**
	 * Specifies which types this custom binder should handle.
	 * <p>
	 * For example, if this binder should apply when binding {@code MyCustomType}, this method could return {@code targetType.matchesClass(MyCustomType.class)}.
	 * <p>
	 * For parameterized types like {@code List<UUID>}, this method could return {@code targetType.matchesParameterizedType(List.class, UUID.class)}.
	 *
	 * @param targetType the target type to evaluate - should this custom binder handle it or not?
	 * @return {@code true} if this binder should handle the type, {@code false} otherwise.
	 */
	@Nonnull
	Boolean appliesTo(@Nonnull TargetType targetType);

	/**
	 * Result of a custom parameter binding attempt.
	 * <p>
	 * Use {@link #HANDLED} to indicate a successfully-bound value or {@link #FALLBACK} to indicate "didn't bind; fall back to the registered {@link PreparedStatementBinder} behavior".</p>
	 */
	@ThreadSafe
	enum BindingResult {
		HANDLED,
		FALLBACK
	}
}