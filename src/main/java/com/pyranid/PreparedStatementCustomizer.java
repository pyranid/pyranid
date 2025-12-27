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

/**
 * Customizes a {@link PreparedStatement} before execution.
 * <p>
 * Used by {@link Query#customize(PreparedStatementCustomizer)}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.0.0
 */
@FunctionalInterface
public interface PreparedStatementCustomizer {
	/**
	 * Applies customization to the prepared statement before execution.
	 *
	 * @param statementContext  current SQL context
	 * @param preparedStatement the prepared statement to customize
	 * @throws SQLException if customization fails
	 */
	void customize(@NonNull StatementContext<?> statementContext,
								 @NonNull PreparedStatement preparedStatement) throws SQLException;
}
