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

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Identifies different types of databases, which allows for special platform-specific handling.
 * 
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
public enum DatabaseType {
  /**
   * A database which requires no special handling.
   */
  GENERIC,
  /**
   * An Oracle database.
   */
  ORACLE;

  /**
   * Determines the type of database to which the given {@code dataSource} connects.
   * <p>
   * Note: this will establish a {@link Connection} to the database.
   * 
   * @param dataSource
   *          the database connection factory
   * @return the type of database
   * @throws DatabaseException
   *           if an exception occurs while attempting to read database metadata
   */
  public static DatabaseType fromDataSource(DataSource dataSource) {
    requireNonNull(dataSource);

    DatabaseType databaseType = DatabaseType.GENERIC;

    try {
      try (Connection connection = dataSource.getConnection()) {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        String databaseProductName = databaseMetaData.getDatabaseProductName();
        if (databaseProductName != null && databaseProductName.startsWith("Oracle"))
          databaseType = DatabaseType.ORACLE;
      }
    } catch (SQLException e) {
      throw new DatabaseException("Unable to connect to database to determine its type", e);
    }

    return databaseType;
  }
}