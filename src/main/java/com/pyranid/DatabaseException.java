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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Thrown when an error occurs when interacting with a {@link Database}.
 * <p>
 * If the {@code cause} of this exception is a {@link SQLException}, the {@link #errorCode()} and {@link #sqlState()}
 * accessors are shorthand for retrieving the corresponding {@link SQLException} values.
 *
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
public class DatabaseException extends RuntimeException {
  private final Optional<Integer> errorCode;
  private final Optional<String> sqlState;

  // Additional metadata
  private final Optional<String> column;
  private final Optional<String> constraint;
  private final Optional<String> datatype;
  private final Optional<String> detail;
  private final Optional<String> file;
  private final Optional<String> hint;
  private final Optional<Integer> internalPosition;
  private final Optional<String> internalQuery;
  private final Optional<Integer> line;
  private final Optional<String> dbmsMessage;
  private final Optional<Integer> position;
  private final Optional<String> routine;
  private final Optional<String> schema;
  private final Optional<String> severity;
  private final Optional<String> table;
  private final Optional<String> where;

  /**
   * Creates a {@code DatabaseException} with the given {@code message}.
   *
   * @param message a message describing this exception
   */
  public DatabaseException(String message) {
    this(message, null);
  }

  /**
   * Creates a {@code DatabaseException} which wraps the given {@code cause}.
   *
   * @param cause the cause of this exception
   */
  public DatabaseException(Throwable cause) {
    this(cause == null ? null : cause.getMessage(), cause);
  }

  /**
   * Creates a {@code DatabaseException} which wraps the given {@code cause}.
   *
   * @param message a message describing this exception
   * @param cause   the cause of this exception
   */
  public DatabaseException(String message, Throwable cause) {
    super(message, cause);

    Optional<Integer> errorCode = Optional.empty();
    Optional<String> sqlState = Optional.empty();
    Optional<String> column = Optional.empty();
    Optional<String> constraint = Optional.empty();
    Optional<String> datatype = Optional.empty();
    Optional<String> detail = Optional.empty();
    Optional<String> file = Optional.empty();
    Optional<String> hint = Optional.empty();
    Optional<Integer> internalPosition = Optional.empty();
    Optional<String> internalQuery = Optional.empty();
    Optional<Integer> line = Optional.empty();
    Optional<String> dbmsMessage = Optional.empty();
    Optional<Integer> position = Optional.empty();
    Optional<String> routine = Optional.empty();
    Optional<String> schema = Optional.empty();
    Optional<String> severity = Optional.empty();
    Optional<String> table = Optional.empty();
    Optional<String> where = Optional.empty();

    if(cause != null) {
      // Special handling for Postgres
      if ("org.postgresql.util.PSQLException".equals(cause.getClass().getName())) {
        org.postgresql.util.PSQLException psqlException = (org.postgresql.util.PSQLException) cause;
        org.postgresql.util.ServerErrorMessage serverErrorMessage = psqlException.getServerErrorMessage();

        if(serverErrorMessage != null) {
          errorCode = Optional.ofNullable(psqlException.getErrorCode());
          column = Optional.ofNullable(serverErrorMessage.getColumn());
          constraint = Optional.ofNullable(serverErrorMessage.getConstraint());
          datatype = Optional.ofNullable(serverErrorMessage.getDatatype());
          detail = Optional.ofNullable(serverErrorMessage.getDetail());
          file = Optional.ofNullable(serverErrorMessage.getFile());
          hint = Optional.ofNullable(serverErrorMessage.getHint());
          internalQuery = Optional.ofNullable(serverErrorMessage.getInternalQuery());
          dbmsMessage = Optional.ofNullable(serverErrorMessage.getMessage());
          routine = Optional.ofNullable(serverErrorMessage.getRoutine());
          schema = Optional.ofNullable(serverErrorMessage.getSchema());
          severity = Optional.ofNullable(serverErrorMessage.getSeverity());
          sqlState = Optional.ofNullable(serverErrorMessage.getSQLState());
          table = Optional.ofNullable(serverErrorMessage.getTable());
          where = Optional.ofNullable(serverErrorMessage.getWhere());
          internalPosition = Optional.ofNullable(serverErrorMessage.getInternalPosition());
          line = Optional.ofNullable(serverErrorMessage.getLine());
          position = Optional.ofNullable(serverErrorMessage.getPosition());
        }
      } else if(cause instanceof SQLException) {
        SQLException sqlException = (SQLException) cause;
        errorCode = Optional.ofNullable(sqlException.getErrorCode());
        sqlState = Optional.ofNullable(sqlException.getSQLState());
      }
    }

    this.errorCode = errorCode;
    this.sqlState = sqlState;
    this.column = column;
    this.constraint = constraint;
    this.datatype = datatype;
    this.detail = detail;
    this.file = file;
    this.hint = hint;
    this.internalPosition = internalPosition;
    this.internalQuery = internalQuery;
    this.line = line;
    this.dbmsMessage = dbmsMessage;
    this.position = position;
    this.routine = routine;
    this.schema = schema;
    this.severity = severity;
    this.table = table;
    this.where = where;
  }

  @Override
  public String toString() {
    List<String> components = new ArrayList<>(20);

    if(getMessage() != null && getMessage().trim().length() > 0)
      components.add(format("message=%s", getMessage()));

    if(errorCode().isPresent())
      components.add(format("errorCode=%s", errorCode().get()));
    if(sqlState().isPresent())
      components.add(format("sqlState=%s", sqlState().get()));
    if(column().isPresent())
      components.add(format("column=%s", column().get()));
    if(constraint().isPresent())
      components.add(format("constraint=%s", constraint().get()));
    if(datatype().isPresent())
      components.add(format("datatype=%s", datatype().get()));
    if(detail().isPresent())
      components.add(format("detail=%s", detail().get()));
    if(file().isPresent())
      components.add(format("file=%s", file().get()));
    if(hint().isPresent())
      components.add(format("hint=%s", hint().get()));
    if(internalPosition().isPresent())
      components.add(format("internalPosition=%s", internalPosition().get()));
    if(internalQuery().isPresent())
      components.add(format("internalQuery=%s", internalQuery().get()));
    if(line().isPresent())
      components.add(format("line=%s", line().get()));
    if(dbmsMessage().isPresent())
      components.add(format("dbmsMessage=%s", dbmsMessage().get()));
    if(position().isPresent())
      components.add(format("position=%s", position().get()));
    if(routine().isPresent())
      components.add(format("routine=%s", routine().get()));
    if(schema().isPresent())
      components.add(format("schema=%s", schema().get()));
    if(severity().isPresent())
      components.add(format("severity=%s", severity().get()));
    if(table().isPresent())
      components.add(format("table=%s", table().get()));
    if(where().isPresent())
      components.add(format("where=%s", where().get()));

    return format("%s{%s}", getClass().getSimpleName(), components.stream().collect(Collectors.joining(", ")));
  }

  /**
   * Shorthand for {@link SQLException#getErrorCode()} if this exception was caused by a {@link SQLException}.
   *
   * @return the value of {@link SQLException#getErrorCode()}, or empty if not available
   */
  public Optional<Integer> errorCode() {
    return errorCode;
  }

  /**
   * Shorthand for {@link SQLException#getSQLState()} if this exception was caused by a {@link SQLException}.
   *
   * @return the value of {@link SQLException#getSQLState()}, or empty if not available
   */
  public Optional<String> sqlState() {
    return sqlState;
  }

  /**
   * @return the value of the offending {@code column}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> column() {
    return column;
  }

  /**
   * @return the value of the offending {@code constraint}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> constraint() {
    return constraint;
  }

  /**
   * @return the value of the offending {@code datatype}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> datatype() {
    return datatype;
  }

  /**
   * @return the value of the offending {@code detail}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> detail() {
    return detail;
  }

  /**
   * @return the value of the offending {@code file}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> file() {
    return file;
  }

  /**
   * @return the value of the error {@code hint}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> hint() {
    return hint;
  }

  /**
   * @return the value of the offending {@code internalPosition}, or empty if not available
   * @since 1.0.12
   */
  public Optional<Integer> internalPosition() {
    return internalPosition;
  }

  /**
   * @return the value of the offending {@code internalQuery}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> internalQuery() {
    return internalQuery;
  }

  /**
   * @return the value of the offending {@code line}, or empty if not available
   * @since 1.0.12
   */
  public Optional<Integer> line() {
    return line;
  }

  /**
   * @return the value of the error {@code dbmsMessage}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> dbmsMessage() {
    return dbmsMessage;
  }

  /**
   * @return the value of the offending {@code position}, or empty if not available
   * @since 1.0.12
   */
  public Optional<Integer> position() {
    return position;
  }

  /**
   * @return the value of the offending {@code routine}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> routine() {
    return routine;
  }

  /**
   * @return the value of the offending {@code schema}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> schema() {
    return schema;
  }

  /**
   * @return the error {@code severity}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> severity() {
    return severity;
  }

  /**
   * @return the value of the offending {@code table}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> table() {
    return table;
  }

  /**
   * @return the value of the offending {@code where}, or empty if not available
   * @since 1.0.12
   */
  public Optional<String> where() {
    return where;
  }
}