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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.logging.Level.WARNING;

/**
 * @author <a href="http://revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
public class Database {
  private static final ThreadLocal<Deque<Transaction>> TRANSACTION_STACK_HOLDER = ThreadLocal
      .withInitial(() -> new ArrayDeque<>());

  private final DataSource dataSource;
  private final InstanceProvider instanceProvider;
  private final PreparedStatementBinder preparedStatementBinder;
  private final ResultSetMapper resultSetMapper;
  private final StatementLogger statementLogger;
  private final Logger logger = Logger.getLogger(Database.class.getName());

  public static Builder forDataSource(DataSource dataSource) {
    return new Builder(requireNonNull(dataSource));
  }

  protected Database(Builder builder) {
    requireNonNull(builder);
    this.dataSource = requireNonNull(builder.dataSource);
    this.instanceProvider = requireNonNull(builder.instanceProvider);
    this.preparedStatementBinder = requireNonNull(builder.preparedStatementBinder);
    this.resultSetMapper = requireNonNull(builder.resultSetMapper);
    this.statementLogger = requireNonNull(builder.statementLogger);
  }

  public static class Builder {
    private final DataSource dataSource;
    private final DatabaseType databaseType;
    private InstanceProvider instanceProvider;
    private PreparedStatementBinder preparedStatementBinder;
    private ResultSetMapper resultSetMapper;
    private StatementLogger statementLogger;

    // See build() method for explanation of why we keep track of whether these fields have changed
    private final InstanceProvider initialInstanceProvider;
    private final ResultSetMapper initialResultSetMapper;

    private Builder(DataSource dataSource) {
      this.dataSource = requireNonNull(dataSource);
      this.databaseType = DatabaseType.fromDataSource(dataSource);

      this.preparedStatementBinder = new DefaultPreparedStatementBinder(this.databaseType);
      this.statementLogger = new DefaultStatementLogger();

      this.instanceProvider = new DefaultInstanceProvider();
      this.initialInstanceProvider = this.instanceProvider;

      this.resultSetMapper = new DefaultResultSetMapper(this.databaseType, this.instanceProvider);
      this.initialResultSetMapper = resultSetMapper;
    }

    public Builder instanceProvider(InstanceProvider instanceProvider) {
      this.instanceProvider = requireNonNull(instanceProvider);
      return this;
    }

    public Builder preparedStatementBinder(PreparedStatementBinder preparedStatementBinder) {
      this.preparedStatementBinder = requireNonNull(preparedStatementBinder);
      return this;
    }

    public Builder resultSetMapper(ResultSetMapper resultSetMapper) {
      this.resultSetMapper = requireNonNull(resultSetMapper);
      return this;
    }

    public Builder statementLogger(StatementLogger statementLogger) {
      this.statementLogger = requireNonNull(statementLogger);
      return this;
    }

    public Database build() {
      // A little sleight-of-hand to make the 99% case easier for users...
      // If at build time the InstanceProvider has been changed but the ResultSetMapper is unchanged,
      // wire the custom InstanceProvider into the DefaultResultSetMapper
      if (this.instanceProvider != this.initialInstanceProvider && this.resultSetMapper == this.initialResultSetMapper)
        this.resultSetMapper = new DefaultResultSetMapper(this.databaseType, this.instanceProvider);

      return new Database(this);
    }
  }

  public Optional<Transaction> currentTransaction() {
    Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();
    return Optional.ofNullable(transactionStack.size() == 0 ? null : transactionStack.peek());
  }

  public void transaction(TransactionalOperation transactionalOperation) {
    requireNonNull(transactionalOperation);

    transaction(() -> {
      transactionalOperation.perform();
      return null;
    });
  }

  public <T> T transaction(ReturningTransactionalOperation<T> transactionalOperation) {
    requireNonNull(transactionalOperation);
    return transaction(TransactionIsolation.DEFAULT, transactionalOperation);
  }

  public <T> T transaction(TransactionIsolation transactionIsolation,
                           ReturningTransactionalOperation<T> transactionalOperation) {
    requireNonNull(transactionIsolation);
    requireNonNull(transactionalOperation);

    Transaction transaction = new Transaction(dataSource, transactionIsolation);
    TRANSACTION_STACK_HOLDER.get().push(transaction);
    boolean committed = false;
    boolean rolledBack = false;

    try {
      T returnValue = transactionalOperation.perform();

      if (transaction.isRollbackOnly()) {
        transaction.rollback();
        rolledBack = true;
      } else {
        transaction.commit();
        committed = true;
      }

      return returnValue;
    } catch (RuntimeException e) {
      try {
        transaction.rollback();
        rolledBack = true;
      } catch (Exception rollbackException) {
        logger.log(WARNING, "Unable to roll back transaction", rollbackException);
      }

      throw e;
    } catch (Throwable t) {
      try {
        transaction.rollback();
        rolledBack = true;
      } catch (Exception rollbackException) {
        logger.log(WARNING, "Unable to roll back transaction", rollbackException);
      }

      throw new RuntimeException(t);
    } finally {
      TRANSACTION_STACK_HOLDER.get().pop();

      try {
        try {
          if (transaction.initialAutoCommit().isPresent() && transaction.initialAutoCommit().get())
            // Autocommit was true initially, so restoring to true now that transaction has completed
            transaction.setAutoCommit(true);
        } finally {
          if (transaction.hasConnection())
            closeConnection(transaction.connection());
        }
      } finally {
        // Execute any user-supplied post-execution hooks
        if (committed) {
          for (Runnable postCommitOperation : transaction.postCommitOperations())
            postCommitOperation.run();
        } else if (rolledBack) {
          for (Runnable postRollbackOperation : transaction.postRollbackOperations())
            postRollbackOperation.run();
        }
      }
    }
  }

  protected void closeConnection(Connection connection) {
    requireNonNull(connection);

    try {
      connection.close();
    } catch (SQLException e) {
      throw new DatabaseException("Unable to close database connection", e);
    }
  }

  public void participate(Transaction transaction, TransactionalOperation transactionalOperation) {
    requireNonNull(transaction);
    requireNonNull(transactionalOperation);

    participate(transaction, () -> {
      transactionalOperation.perform();
      return null;
    });
  }

  public <T> T participate(Transaction transaction, ReturningTransactionalOperation<T> transactionalOperation) {
    requireNonNull(transaction);
    requireNonNull(transactionalOperation);

    TRANSACTION_STACK_HOLDER.get().push(transaction);

    try {
      return transactionalOperation.perform();
    } catch (RuntimeException e) {
      transaction.setRollbackOnly(true);
      throw e;
    } catch (Throwable t) {
      transaction.setRollbackOnly(true);
      throw new RuntimeException(t);
    } finally {
      TRANSACTION_STACK_HOLDER.get().pop();
    }
  }

  public <T> Optional<T> queryForObject(String sql, Class<T> objectType, Object... parameters) {
    requireNonNull(sql);
    requireNonNull(objectType);

    return queryForObject(sql, null, objectType, parameters);
  }

  public <T> Optional<T> queryForObject(String sql, StatementMetadata statementMetadata, Class<T> objectType, Object... parameters) {
    requireNonNull(sql);
    requireNonNull(objectType);

    List<T> list = queryForList(sql, statementMetadata, objectType, parameters);

    if (list.size() > 1)
      throw new DatabaseException(format("Expected 1 row in resultset but got %s instead", list.size()));

    return Optional.ofNullable(list.size() == 0 ? null : list.get(0));
  }

  protected static class DatabaseOperationResult {
    private final Optional<Long> executionTime;
    private final Optional<Long> resultSetMappingTime;

    public DatabaseOperationResult(Optional<Long> executionTime, Optional<Long> resultSetMappingTime) {
      this.executionTime = requireNonNull(executionTime);
      this.resultSetMappingTime = requireNonNull(resultSetMappingTime);
    }

    public Optional<Long> executionTime() {
      return executionTime;
    }

    public Optional<Long> resultSetMappingTime() {
      return resultSetMappingTime;
    }
  }

  @FunctionalInterface
  protected static interface DatabaseOperation {
    DatabaseOperationResult perform(PreparedStatement preparedStatement) throws Exception;
  }

  @FunctionalInterface
  protected static interface PreparedStatementBindingOperation {
    void perform(PreparedStatement preparedStatement) throws Exception;
  }

  protected void performDatabaseOperation(String sql, StatementMetadata statementMetadata, Object[] parameters, DatabaseOperation databaseOperation) {
    requireNonNull(sql);
    requireNonNull(databaseOperation);

    performDatabaseOperation(sql, statementMetadata, parameters, (preparedStatement) -> {
      if (parameters != null && parameters.length > 0)
        preparedStatementBinder().bind(preparedStatement, Arrays.asList(parameters));
    }, databaseOperation);
  }

  protected void performDatabaseOperation(String sql, StatementMetadata statementMetadata, Object[] parameters,
                                          PreparedStatementBindingOperation preparedStatementBindingOperation, DatabaseOperation databaseOperation) {
    requireNonNull(sql);
    requireNonNull(preparedStatementBindingOperation);
    requireNonNull(databaseOperation);

    long startTime = nanoTime();
    Optional<Long> connectionAcquisitionTime = Optional.empty();
    Optional<Long> preparationTime = Optional.empty();
    Optional<Long> executionTime = Optional.empty();
    Optional<Long> resultSetMappingTime = Optional.empty();
    Optional<Exception> exception = Optional.empty();

    Connection connection = null;

    try {
      boolean alreadyHasConnection = currentTransaction().isPresent() && currentTransaction().get().hasConnection();
      connection = acquireConnection();
      connectionAcquisitionTime = alreadyHasConnection ? Optional.empty() : Optional.of(nanoTime() - startTime);
      startTime = nanoTime();

      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        preparedStatementBindingOperation.perform(preparedStatement);
        preparationTime = Optional.of(nanoTime() - startTime);

        DatabaseOperationResult databaseOperationResult = databaseOperation.perform(preparedStatement);
        executionTime = databaseOperationResult.executionTime();
        resultSetMappingTime = databaseOperationResult.resultSetMappingTime();
      }
    } catch (DatabaseException e) {
      exception = Optional.of(e);
      throw e;
    } catch (Exception e) {
      exception = Optional.of(e);
      throw new DatabaseException(e);
    } finally {
      try {
        // If this was a single-shot operation (not in a transaction), close the connection
        if (connection != null && !currentTransaction().isPresent())
          closeConnection(connection);
      } finally {
        StatementLog statementLog =
            StatementLog.forSql(sql)
                .parameters(parameters == null ? emptyList() : Arrays.asList(parameters))
                .connectionAcquisitionTime(connectionAcquisitionTime)
                .preparationTime(preparationTime)
                .executionTime(executionTime)
                .resultSetMappingTime(resultSetMappingTime)
                .exception(exception)
                .statementMetadata(Optional.ofNullable(statementMetadata))
                .build();

        statementLogger().log(statementLog);
      }
    }
  }

  public <T> List<T> queryForList(String sql, Class<T> elementType, Object... parameters) {
    requireNonNull(sql);
    requireNonNull(elementType);

    return queryForList(sql, null, elementType, parameters);
  }

  public <T> List<T> queryForList(String sql, StatementMetadata statementMetadata, Class<T> elementType, Object... parameters) {
    requireNonNull(sql);
    requireNonNull(elementType);

    List<T> list = new ArrayList<>();

    performDatabaseOperation(sql, statementMetadata, parameters, (PreparedStatement preparedStatement) -> {
      long startTime = nanoTime();

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        Long executionTime = nanoTime() - startTime;
        startTime = nanoTime();

        while (resultSet.next()) {
          T listElement = resultSetMapper().map(resultSet, elementType);
          list.add(listElement);
        }

        Long resultSetMappingTime = nanoTime() - startTime;
        return new DatabaseOperationResult(Optional.of(executionTime), Optional.of(resultSetMappingTime));
      }
    });

    return list;
  }

  private static class ResultHolder<T> {
    T value;
  }

  public long execute(String sql, Object... parameters) {
    requireNonNull(sql);
    return execute(sql, null, parameters);
  }

  public long execute(String sql, StatementMetadata statementMetadata, Object... parameters) {
    requireNonNull(sql);

    ResultHolder<Long> resultHolder = new ResultHolder<>();

    performDatabaseOperation(sql, statementMetadata, parameters, (PreparedStatement preparedStatement) -> {
      long startTime = nanoTime();
      // TODO: allow users to specify that they want support for executeLargeUpdate()
      // Not everyone implements it currently
      resultHolder.value = (long) preparedStatement.executeUpdate();
      return new DatabaseOperationResult(Optional.of(nanoTime() - startTime), Optional.empty());
    });

    return resultHolder.value;
  }

  public <T> Optional<T> executeReturning(String sql, Class<T> returnType, Object... parameters) {
    requireNonNull(sql);
    requireNonNull(returnType);

    return executeReturning(sql, null, returnType, parameters);
  }

  public <T> Optional<T> executeReturning(String sql, StatementMetadata statementMetadata, Class<T> returnType, Object... parameters) {
    requireNonNull(sql);
    requireNonNull(returnType);

    ResultHolder<T> resultHolder = new ResultHolder<>();

    performDatabaseOperation(sql, statementMetadata, parameters, (PreparedStatement preparedStatement) -> {
      long startTime = nanoTime();

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        Long executionTime = nanoTime() - startTime;
        startTime = nanoTime();

        if (resultSet.next())
          resultHolder.value = resultSetMapper().map(resultSet, returnType);

        Long resultSetMappingTime = nanoTime() - startTime;
        return new DatabaseOperationResult(Optional.of(executionTime), Optional.of(resultSetMappingTime));
      }
    });

    return Optional.ofNullable(resultHolder.value);
  }

  public long[] executeBatch(String sql, List<List<Object>> parameterGroups) {
    requireNonNull(sql);
    requireNonNull(parameterGroups);

    return executeBatch(sql, null, parameterGroups);
  }

  public long[] executeBatch(String sql, StatementMetadata statementMetadata, List<List<Object>> parameterGroups) {
    requireNonNull(sql);
    requireNonNull(parameterGroups);

    ResultHolder<long[]> resultHolder = new ResultHolder<>();

    performDatabaseOperation(sql, statementMetadata, parameterGroups.toArray(), (preparedStatement) -> {
      for (List<Object> parameterGroup : parameterGroups) {
        if (parameterGroup != null && parameterGroup.size() > 0)
          preparedStatementBinder().bind(preparedStatement, parameterGroup);

        preparedStatement.addBatch();
      }
    }, (PreparedStatement preparedStatement) -> {
      long startTime = nanoTime();
      // TODO: allow users to specify that they want support for executeLargeBatch()
      // Not everyone implements it currently
      int[] result = preparedStatement.executeBatch();
      long[] longResult = new long[result.length];

      for (int i = 0; i < result.length; ++i)
        longResult[i] = result[i];

      resultHolder.value = longResult;
      return new DatabaseOperationResult(Optional.of(nanoTime() - startTime), Optional.empty());
    });

    return resultHolder.value;
  }

  protected Connection acquireConnection() {
    Optional<Transaction> transaction = currentTransaction();

    if (transaction.isPresent())
      return transaction.get().connection();

    try {
      return dataSource.getConnection();
    } catch (SQLException e) {
      throw new DatabaseException("Unable to acquire database connection", e);
    }
  }

  protected DataSource dataSource() {
    return dataSource;
  }

  protected InstanceProvider instanceProvider() {
    return instanceProvider;
  }

  protected PreparedStatementBinder preparedStatementBinder() {
    return preparedStatementBinder;
  }

  protected ResultSetMapper resultSetMapper() {
    return resultSetMapper;
  }

  protected StatementLogger statementLogger() {
    return statementLogger;
  }
}