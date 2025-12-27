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
	 * @return {@link BindingResult#handled()} if the custom binding was performed, or {@link BindingResult#fallback()} to fall back to default {@link PreparedStatementBinder} behavior
	 * @throws SQLException if an error occurs during binding
	 */
	@NonNull
	BindingResult bind(@NonNull StatementContext<?> statementContext,
										 @NonNull PreparedStatement preparedStatement,
										 @NonNull Integer parameterIndex,
										 @NonNull Object parameter) throws SQLException;

	/**
	 * Performs custom binding of a null value given its {@code index} and {@code targetType}.
	 * <p>
	 * This function is only invoked for {@link TypedParameter} values that are {@code null}.
	 *
	 * @param statementContext  current SQL context
	 * @param preparedStatement the prepared statement to bind to
	 * @param parameterIndex    1-based parameter index at which to perform the binding
	 * @param targetType        explicit target type from {@link TypedParameter}
	 * @param sqlType           JDBC SQL type hint (may be {@link java.sql.Types#NULL})
	 * @return {@link BindingResult#handled()} if the custom binding was performed, or {@link BindingResult#fallback()} to fall back to default behavior
	 * @throws SQLException if an error occurs during binding
	 */
	@NonNull
	default BindingResult bindNull(@NonNull StatementContext<?> statementContext,
																 @NonNull PreparedStatement preparedStatement,
																 @NonNull Integer parameterIndex,
																 @NonNull TargetType targetType,
																 @NonNull Integer sqlType) throws SQLException {
		return BindingResult.fallback();
	}

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
	@NonNull
	Boolean appliesTo(@NonNull TargetType targetType);

	/**
	 * Result of a custom parameter binding attempt.
	 * <p>
	 * Use {@link #handled()} to indicate a successfully-bound value or {@link #fallback()} to indicate "didn't bind; fall back to the registered {@link PreparedStatementBinder} behavior".</p>
	 */
	@ThreadSafe
	sealed abstract class BindingResult permits BindingResult.Handled, BindingResult.Fallback {
		private BindingResult() {}

		/**
		 * Indicates that this mapper successfully bound a custom value.
		 *
		 * @return a result which indicates that this binder successfully bound a custom value
		 */
		@NonNull
		public static BindingResult handled() {
			return Handled.INSTANCE;
		}

		/**
		 * Indicates that this mapper did not bind a custom value and prefers to fall back to the behavior of the registered {@link PreparedStatementBinder}.
		 *
		 * @return a result which indicates that this binder did not bind a custom value
		 */
		@NonNull
		public static BindingResult fallback() {
			return Fallback.INSTANCE;
		}

		@ThreadSafe
		static final class Handled extends BindingResult {
			static final Handled INSTANCE = new Handled();

			private Handled() {}
		}

		@ThreadSafe
		static final class Fallback extends BindingResult {
			static final Fallback INSTANCE = new Fallback();

			private Fallback() {}
		}
	}
}
