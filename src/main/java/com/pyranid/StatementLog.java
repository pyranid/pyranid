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

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A collection of SQL statement execution diagnostics.
 * <p>
 * Created via builder, for example
 * 
 * <pre>
 * StatementLog statementLog = StatementLog.forSql(&quot;SELECT * FROM car WHERE id=?&quot;).parameters(singletonList(123)).build();
 * </pre>
 * 
 * @author <a href="http://revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
public class StatementLog implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Optional<Long> connectionAcquisitionTime;
  private final Optional<Long> preparationTime;
  private final Optional<Long> executionTime;
  private final Optional<Long> resultSetMappingTime;
  private final String sql;
  private final List<Object> parameters;
  private final Optional<Integer> batchSize;
  private final Optional<Exception> exception;

  /**
   * Creates a {@code StatementLog} for the given {@code builder}.
   * 
   * @param builder
   *          the builder used to construct this {@code StatementLog}
   */
  private StatementLog(Builder builder) {
    requireNonNull(builder);
    this.connectionAcquisitionTime = builder.connectionAcquisitionTime;
    this.preparationTime = builder.preparationTime;
    this.executionTime = builder.executionTime;
    this.resultSetMappingTime = builder.resultSetMappingTime;
    this.sql = requireNonNull(builder.sql);
    this.parameters = requireNonNull(builder.parameters);
    this.batchSize = requireNonNull(builder.batchSize);
    this.exception = requireNonNull(builder.exception);
  }

  @Override
  public String toString() {
    return format(
      "%s{connectionAcquisitionTime=%s, preparationTime=%s, executionTime=%s, resultSetMappingTime=%s, sql=%s, "
          + "parameters=%s, batchSize=%s, exception=%s}", getClass().getSimpleName(), connectionAcquisitionTime(),
      preparationTime(), executionTime(), resultSetMappingTime(), sql(), parameters(), batchSize(), exception());
  }

  @Override
  public boolean equals(Object object) {
    if (this == object)
      return true;

    if (!(object instanceof StatementLog))
      return false;

    StatementLog statementLog = (StatementLog) object;

    return Objects.equals(connectionAcquisitionTime(), statementLog.connectionAcquisitionTime())
        && Objects.equals(preparationTime(), statementLog.preparationTime())
        && Objects.equals(executionTime(), statementLog.executionTime())
        && Objects.equals(resultSetMappingTime(), statementLog.resultSetMappingTime())
        && Objects.equals(sql(), statementLog.sql()) && Objects.equals(parameters(), statementLog.parameters())
        && Objects.equals(batchSize(), statementLog.batchSize())
        && Objects.equals(exception(), statementLog.exception());
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionAcquisitionTime(), preparationTime(), executionTime(), resultSetMappingTime(), sql(),
      parameters(), batchSize(), exception());
  }

  /**
   * Creates a {@link StatementLog} builder for the given {@code sql}.
   * 
   * @param sql
   *          the SQL statement
   * @return a {@link StatementLog} builder
   */
  public static Builder forSql(String sql) {
    return new Builder(requireNonNull(sql));
  }

  /**
   * How long did it take to acquire a {@link java.sql.Connection} from the {@link javax.sql.DataSource}, in
   * nanoseconds?
   * 
   * @return how long it took to acquire a {@link java.sql.Connection}, if available
   */
  public Optional<Long> connectionAcquisitionTime() {
    return connectionAcquisitionTime;
  }

  /**
   * How long did it take to bind data to the {@link java.sql.PreparedStatement}, in nanoseconds?
   * 
   * @return how long it took to bind data to the {@link java.sql.PreparedStatement}, if available
   */
  public Optional<Long> preparationTime() {
    return preparationTime;
  }

  /**
   * How long did it take to execute the SQL statement, in nanoseconds?
   * 
   * @return how long it took to execute the SQL statement, if available
   */
  public Optional<Long> executionTime() {
    return executionTime;
  }

  /**
   * How long did it take to extract data from the {@link java.sql.ResultSet}, in nanoseconds?
   * 
   * @return how long it took to extract data from the {@link java.sql.ResultSet}, if available
   */
  public Optional<Long> resultSetMappingTime() {
    return resultSetMappingTime;
  }

  /**
   * How long did it take to perform the database operation in total?
   * <p>
   * This is the sum of {@link #connectionAcquisitionTime()} + {@link #preparationTime()} +
   * {@link #executionTime()} + {@link #resultSetMappingTime()}.
   *
   * @return how long the database operation took in total
   */
  public Long totalTime() {
    return connectionAcquisitionTime().orElse(0L)
        + preparationTime().orElse(0L)
        + executionTime().orElse(0L)
        + resultSetMappingTime().orElse(0L);
  }

  /**
   * The SQL statement that was executed.
   * 
   * @return the SQL statement that was executed.
   */
  public String sql() {
    return sql;
  }

  /**
   * The parameters bound to the SQL statement that was executed.
   * 
   * @return the parameters bound to the SQL statement that was executed, or an empty {@code List} if none
   */
  public List<Object> parameters() {
    return parameters;
  }

  /**
   * The size of the batch operation.
   * 
   * @return how many records were processed as part of the batch operation, if available
   */
  public Optional<Integer> batchSize() {
    return batchSize;
  }

  /**
   * The exception that occurred during SQL statement execution.
   * 
   * @return the exception that occurred during SQL statement execution, if available
   */
  public Optional<Exception> exception() {
    return exception;
  }

  /**
   * Builder for {@link StatementLog} instances.
   * <p>
   * Created via {@link StatementLog#forSql(String)}, for example
   * 
   * <pre>
   * StatementLog.Builder builder = StatementLog.forSql(&quot;SELECT * FROM car WHERE id=?&quot;).parameters(singletonList(123));
   * StatementLog statementLog = builder.build();
   * </pre>
   * 
   * @author <a href="http://revetkn.com">Mark Allen</a>
   * @since 1.0.0
   */
  public static class Builder {
    private final String sql;
    private Optional<Long> connectionAcquisitionTime = Optional.empty();
    private Optional<Long> preparationTime = Optional.empty();
    private Optional<Long> executionTime = Optional.empty();
    private Optional<Long> resultSetMappingTime = Optional.empty();
    private List<Object> parameters = emptyList();
    private Optional<Integer> batchSize = Optional.empty();
    private Optional<Exception> exception = Optional.empty();

    /**
     * Creates a {@code Builder} for the given {@code sql}.
     * 
     * @param sql
     *          the SQL statement
     */
    private Builder(String sql) {
      this.sql = requireNonNull(sql);
    }

    /**
     * Specifies how long it took to acquire a {@link java.sql.Connection} from the {@link javax.sql.DataSource}, in
     * nanoseconds.
     * 
     * @param connectionAcquisitionTime
     *          how long it took to acquire a {@link java.sql.Connection}, if available
     * @return this {@code Builder}, for chaining
     */
    public Builder connectionAcquisitionTime(Optional<Long> connectionAcquisitionTime) {
      this.connectionAcquisitionTime = requireNonNull(connectionAcquisitionTime);
      return this;
    }

    /**
     * Specifies how long it took to bind data to a {@link java.sql.PreparedStatement}, in nanoseconds.
     * 
     * @param preparationTime
     *          how long it took to bind data to a {@link java.sql.PreparedStatement}, if available
     * @return this {@code Builder}, for chaining
     */
    public Builder preparationTime(Optional<Long> preparationTime) {
      this.preparationTime = requireNonNull(preparationTime);
      return this;
    }

    /**
     * Specifies how long it took to execute a SQL statement, in nanoseconds.
     * 
     * @param executionTime
     *          how long it took to execute a SQL statement, if available
     * @return this {@code Builder}, for chaining
     */
    public Builder executionTime(Optional<Long> executionTime) {
      this.executionTime = requireNonNull(executionTime);
      return this;
    }

    /**
     * Specifies how long it took to extract data from a {@link java.sql.ResultSet}, in nanoseconds.
     * 
     * @param resultSetMappingTime
     *          how long it took to extract data from a {@link java.sql.ResultSet}, if available
     * @return this {@code Builder}, for chaining
     */
    public Builder resultSetMappingTime(Optional<Long> resultSetMappingTime) {
      this.resultSetMappingTime = requireNonNull(resultSetMappingTime);
      return this;
    }

    /**
     * The parameters bound to the SQL statement that was executed.
     * 
     * @param parameters
     *          the parameters bound to the SQL statement that was executed, or an empty {@code List} if none
     * @return this {@code Builder}, for chaining
     */
    public Builder parameters(List<Object> parameters) {
      this.parameters = unmodifiableList(new ArrayList<>(requireNonNull(parameters)));
      return this;
    }

    /**
     * Specifies the size of the batch operation.
     * 
     * @param batchSize
     *          how many records were processed as part of the batch operation, if available
     * @return this {@code Builder}, for chaining
     */
    public Builder batchSize(Optional<Integer> batchSize) {
      this.batchSize = requireNonNull(batchSize);
      return this;
    }

    /**
     * Specifies the exception that occurred during SQL statement execution.
     * 
     * @param exception
     *          the exception that occurred during SQL statement execution, if available
     * @return this {@code Builder}, for chaining
     */
    public Builder exception(Optional<Exception> exception) {
      this.exception = requireNonNull(exception);
      return this;
    }

    /**
     * Constructs a {@code StatementLog} instance.
     * 
     * @return a {@code StatementLog} instance
     */
    public StatementLog build() {
      return new StatementLog(this);
    }
  }
}