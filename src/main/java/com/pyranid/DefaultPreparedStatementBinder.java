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
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Basic implementation of {@link PreparedStatementBinder}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
class DefaultPreparedStatementBinder implements PreparedStatementBinder {
	@Nonnull
	private final PreparedStatementParameterBinder preparedStatementParameterBinder;

	DefaultPreparedStatementBinder(@Nonnull PreparedStatementParameterBinder preparedStatementParameterBinder) {
		requireNonNull(preparedStatementParameterBinder);
		this.preparedStatementParameterBinder = preparedStatementParameterBinder;
	}

	@Override
	public <T> void bind(@Nonnull StatementContext<T> statementContext,
											 @Nonnull PreparedStatement preparedStatement,
											 @Nonnull List<Object> parameters) {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);
		requireNonNull(parameters);

		try {
			for (int i = 0; i < parameters.size(); ++i) {
				Object parameter = parameters.get(i);

				if (parameter != null)
					getPreparedStatementParameterBinder().bind(statementContext, preparedStatement, parameter, i + 1);
				else
					preparedStatement.setObject(i + 1, parameter);
			}
		} catch (Exception e) {
			throw new DatabaseException(e);
		}
	}

	@Nonnull
	protected PreparedStatementParameterBinder getPreparedStatementParameterBinder() {
		return this.preparedStatementParameterBinder;
	}
}