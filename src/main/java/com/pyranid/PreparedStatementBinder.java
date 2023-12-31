/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2024 Revetware LLC.
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
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Contract for binding parameters to SQL prepared statements.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@FunctionalInterface
public interface PreparedStatementBinder {
	/**
	 * Binds parameters to a SQL prepared statement.
	 *
	 * @param preparedStatement the prepared statement to bind to
	 * @param statementContext  current SQL context
	 * @param parameters        the parameters to bind to the {@link PreparedStatement}, if any
	 * @throws DatabaseException if an error occurs during binding
	 */
	<T> void bind(@Nonnull StatementContext<T> statementContext,
								@Nonnull PreparedStatement preparedStatement,
								@Nonnull List<Object> parameters);
}