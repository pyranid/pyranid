/*
 * Copyright 2015-2018 Transmogrify LLC.
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

import java.sql.ResultSet;

/**
 * Contract for mapping a {@link ResultSet} row to a different type.
 * 
 * @author <a href="http://revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
public interface ResultSetMapper {
  /**
   * Maps the current row of {@code resultSet} into an instance of {@code resultClass}.
   * 
   * @param <T>
   *          result instance type token
   * @param resultSet
   *          provides raw row data to pull from
   * @param resultClass
   *          the type of instance to map to
   * @return an instance of the given {@code resultClass}
   * @throws DatabaseException
   *           if an error occurs during mapping
   */
  <T> T map(ResultSet resultSet, Class<T> resultClass);
}