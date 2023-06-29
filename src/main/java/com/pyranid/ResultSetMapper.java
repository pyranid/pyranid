/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2023 Revetware LLC.
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
import java.sql.ResultSet;
import java.util.Optional;

/**
 * Contract for mapping a {@link ResultSet} row to a different type.
 *
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
@FunctionalInterface
public interface ResultSetMapper {
	/**
	 * Maps the current row of {@code resultSet} to the result class indicated by {@code statementContext}.
	 *
	 * @param <T>              result instance type token
	 * @param statementContext current SQL context
	 * @param resultSet        provides raw row data to pull from*
	 * @param resultSetRowType the type to which the {@link ResultSet} row should be marshaled
	 * @param instanceProvider instance-creation factory, used to instantiate {@code resultSetRowType} row objects
	 * @return an instance of the given {@code resultClass}
	 * @throws DatabaseException if an error occurs during mapping
	 */
	@Nonnull
	<T> Optional<T> map(@Nonnull StatementContext<T> statementContext,
											@Nonnull ResultSet resultSet,
											@Nonnull Class<T> resultSetRowType,
											@Nonnull InstanceProvider instanceProvider);
}