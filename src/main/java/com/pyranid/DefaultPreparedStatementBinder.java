/*
 * Copyright 2015 Transmogrify LLC.
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

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Basic implementation of {@link PreparedStatementBinder}.
 * 
 * @author <a href="http://revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
public class DefaultPreparedStatementBinder implements PreparedStatementBinder {
  private final DatabaseType databaseType;

  /**
   * Creates a {@code PreparedStatementBinder} for the given {@code databaseType}.
   * 
   * @param databaseType
   *          the type of database we're working with
   */
  public DefaultPreparedStatementBinder(DatabaseType databaseType) {
    this.databaseType = requireNonNull(databaseType);
  }

  @Override
  public void bind(PreparedStatement preparedStatement, List<Object> parameters) {
    requireNonNull(preparedStatement);
    requireNonNull(parameters);

    try {
      for (int i = 0; i < parameters.size(); ++i)
        preparedStatement.setObject(i + 1, normalizeParameter(parameters.get(i)));
    } catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  /**
   * Massages a parameter into a JDBC-friendly format if needed.
   * <p>
   * For example, we need to do special work to prepare a {@link UUID} for Oracle.
   * 
   * @param parameter
   *          the parameter to (possibly) massage
   * @return the result of the massaging process
   */
  protected Object normalizeParameter(Object parameter) {
    if (parameter == null)
      return null;

    if (parameter instanceof Date)
      return new Timestamp(((Date) parameter).getTime());
    if (parameter instanceof Instant)
      return new Timestamp(((Instant) parameter).toEpochMilli());
    if (parameter instanceof Locale)
      return ((Locale) parameter).toLanguageTag();
    if (parameter instanceof Enum)
      return ((Enum<?>) parameter).name();

    // Special handling for Oracle
    if (databaseType() == DatabaseType.ORACLE) {
      if (parameter instanceof java.util.UUID) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(((UUID) parameter).getMostSignificantBits());
        byteBuffer.putLong(((UUID) parameter).getLeastSignificantBits());
        return byteBuffer.array();
      }

      // Other massaging here if needed...
    }

    return parameter;
  }

  /**
   * What kind of database are we working with?
   * 
   * @return the kind of database we're working with
   */
  protected DatabaseType databaseType() {
    return this.databaseType;
  }
}