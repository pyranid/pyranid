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

import java.sql.SQLException;
import java.util.Optional;

/**
 * Thrown when an error occurs when interacting with a {@link Database}.
 * <p>
 * If the {@code cause} of this exception is a {@link SQLException}, the {@link #errorCode()} and {@link #sqlState()}
 * accessors are shorthand for retrieving the corresponding {@link SQLException} values.
 * 
 * @author <a href="http://revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
public class DatabaseException extends RuntimeException {
  private final Optional<Integer> errorCode;
  private final Optional<String> sqlState;

  /**
   * Creates a {@code DatabaseException} with the given {@code message}.
   * 
   * @param message
   *          a message describing this exception
   */
  public DatabaseException(String message) {
    super(message);
    this.errorCode = Optional.empty();
    this.sqlState = Optional.empty();
  }

  /**
   * Creates a {@code DatabaseException} which wraps the given {@link SQLException}.
   * 
   * @param cause
   *          the cause of this exception
   */
  public DatabaseException(SQLException cause) {
    super(cause);
    this.errorCode = Optional.ofNullable(cause.getErrorCode());
    this.sqlState = Optional.ofNullable(cause.getSQLState());
  }

  /**
   * Creates a {@code DatabaseException} which wraps the given {@code cause}.
   * 
   * @param cause
   *          the cause of this exception
   */
  public DatabaseException(Throwable cause) {
    super(cause);
    this.errorCode = Optional.empty();
    this.sqlState = Optional.empty();
  }

  /**
   * Creates a {@code DatabaseException} which wraps the given {@link SQLException}.
   * 
   * @param message
   *          a message describing this exception
   * @param cause
   *          the cause of this exception
   */
  public DatabaseException(String message, SQLException cause) {
    super(message, cause);
    this.errorCode = Optional.ofNullable(cause.getErrorCode());
    this.sqlState = Optional.ofNullable(cause.getSQLState());
  }

  /**
   * Creates a {@code DatabaseException} which wraps the given {@code cause}.
   * 
   * @param message
   *          a message describing this exception
   * @param cause
   *          the cause of this exception
   */
  public DatabaseException(String message, Throwable cause) {
    super(message, cause);
    this.errorCode = Optional.empty();
    this.sqlState = Optional.empty();
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
}