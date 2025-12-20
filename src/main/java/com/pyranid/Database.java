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
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.logging.Level.WARNING;

/**
 * Main class for performing database access operations.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public final class Database {
	@Nonnull
	private static final ThreadLocal<Deque<Transaction>> TRANSACTION_STACK_HOLDER;

	static {
		TRANSACTION_STACK_HOLDER = ThreadLocal.withInitial(() -> new ArrayDeque<>());
	}

	@Nonnull
	private final DataSource dataSource;
	@Nonnull
	private final DatabaseType databaseType;
	@Nonnull
	private final ZoneId timeZone;
	@Nonnull
	private final InstanceProvider instanceProvider;
	@Nonnull
	private final PreparedStatementBinder preparedStatementBinder;
	@Nonnull
	private final ResultSetMapper resultSetMapper;
	@Nonnull
	private final StatementLogger statementLogger;
	@Nonnull
	private final AtomicInteger defaultIdGenerator;
	@Nonnull
	private final Logger logger;

	@Nonnull
	private volatile DatabaseOperationSupportStatus executeLargeBatchSupported;
	@Nonnull
	private volatile DatabaseOperationSupportStatus executeLargeUpdateSupported;

	protected Database(@Nonnull Builder builder) {
		requireNonNull(builder);

		this.dataSource = requireNonNull(builder.dataSource);
		this.databaseType = builder.databaseType == null ? DatabaseType.fromDataSource(builder.dataSource) : builder.databaseType;
		this.timeZone = builder.timeZone == null ? ZoneId.systemDefault() : builder.timeZone;
		this.instanceProvider = builder.instanceProvider == null ? new InstanceProvider() {} : builder.instanceProvider;
		this.preparedStatementBinder = builder.preparedStatementBinder == null ? PreparedStatementBinder.withDefaultConfiguration() : builder.preparedStatementBinder;
		this.resultSetMapper = builder.resultSetMapper == null ? ResultSetMapper.withDefaultConfiguration() : builder.resultSetMapper;
		this.statementLogger = builder.statementLogger == null ? (statementLog) -> {} : builder.statementLogger;
		this.defaultIdGenerator = new AtomicInteger();
		this.logger = Logger.getLogger(getClass().getName());
		this.executeLargeBatchSupported = DatabaseOperationSupportStatus.UNKNOWN;
		this.executeLargeUpdateSupported = DatabaseOperationSupportStatus.UNKNOWN;
	}

	/**
	 * Provides a {@link Database} builder for the given {@link DataSource}.
	 *
	 * @param dataSource data source used to create the {@link Database} builder
	 * @return a {@link Database} builder
	 */
	@Nonnull
	public static Builder withDataSource(@Nonnull DataSource dataSource) {
		requireNonNull(dataSource);
		return new Builder(dataSource);
	}

	/**
	 * Gets a reference to the current transaction, if any.
	 *
	 * @return the current transaction
	 */
	@Nonnull
	public Optional<Transaction> currentTransaction() {
		Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();
		return Optional.ofNullable(transactionStack.isEmpty() ? null : transactionStack.peek());
	}

	/**
	 * Performs an operation transactionally.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 *
	 * @param transactionalOperation the operation to perform transactionally
	 */
	public void transaction(@Nonnull TransactionalOperation transactionalOperation) {
		requireNonNull(transactionalOperation);

		transaction(() -> {
			transactionalOperation.perform();
			return Optional.empty();
		});
	}

	/**
	 * Performs an operation transactionally with the given isolation level.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 *
	 * @param transactionIsolation   the desired database transaction isolation level
	 * @param transactionalOperation the operation to perform transactionally
	 */
	public void transaction(@Nonnull TransactionIsolation transactionIsolation,
													@Nonnull TransactionalOperation transactionalOperation) {
		requireNonNull(transactionIsolation);
		requireNonNull(transactionalOperation);

		transaction(transactionIsolation, () -> {
			transactionalOperation.perform();
			return Optional.empty();
		});
	}

	/**
	 * Performs an operation transactionally and optionally returns a value.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 *
	 * @param transactionalOperation the operation to perform transactionally
	 * @param <T>                    the type to be returned
	 * @return the result of the transactional operation
	 */
	@Nonnull
	public <T> Optional<T> transaction(@Nonnull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transactionalOperation);
		return transaction(TransactionIsolation.DEFAULT, transactionalOperation);
	}

	/**
	 * Performs an operation transactionally with the given isolation level, optionally returning a value.
	 * <p>
	 * The transaction will be automatically rolled back if an exception bubbles out of {@code transactionalOperation}.
	 *
	 * @param transactionIsolation   the desired database transaction isolation level
	 * @param transactionalOperation the operation to perform transactionally
	 * @param <T>                    the type to be returned
	 * @return the result of the transactional operation
	 */
	@Nonnull
	public <T> Optional<T> transaction(@Nonnull TransactionIsolation transactionIsolation,
																		 @Nonnull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transactionIsolation);
		requireNonNull(transactionalOperation);

		Transaction transaction = new Transaction(dataSource, transactionIsolation);
		TRANSACTION_STACK_HOLDER.get().push(transaction);
		boolean committed = false;
		Throwable thrown = null;

		try {
			Optional<T> returnValue = transactionalOperation.perform();

			// Safeguard in case user code accidentally returns null instead of Optional.empty()
			if (returnValue == null)
				returnValue = Optional.empty();

			if (transaction.isRollbackOnly()) {
				transaction.rollback();
			} else {
				transaction.commit();
				committed = true;
			}

			return returnValue;
		} catch (RuntimeException e) {
			thrown = e;
			try {
				transaction.rollback();
			} catch (Exception rollbackException) {
				logger.log(WARNING, "Unable to roll back transaction", rollbackException);
			}

			throw e;
		} catch (Throwable t) {
			RuntimeException wrapped = new RuntimeException(t);
			thrown = wrapped;
			try {
				transaction.rollback();
			} catch (Exception rollbackException) {
				logger.log(WARNING, "Unable to roll back transaction", rollbackException);
			}

			throw wrapped;
		} finally {
			Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();

			transactionStack.pop();

			// Ensure txn stack is fully cleaned up
			if (transactionStack.isEmpty())
				TRANSACTION_STACK_HOLDER.remove();

			Throwable cleanupFailure = null;

			try {
				try {
					transaction.restoreTransactionIsolationIfNeeded();

					if (transaction.getInitialAutoCommit().isPresent() && transaction.getInitialAutoCommit().get())
						// Autocommit was true initially, so restoring to true now that transaction has completed
						transaction.setAutoCommit(true);
				} catch (Throwable cleanupException) {
					cleanupFailure = cleanupException;
				} finally {
					if (transaction.hasConnection()) {
						try {
							closeConnection(transaction.getConnection());
						} catch (Throwable cleanupException) {
							if (cleanupFailure == null)
								cleanupFailure = cleanupException;
							else
								cleanupFailure.addSuppressed(cleanupException);
						}
					}
				}
			} finally {
				// Execute any user-supplied post-execution hooks
				for (Consumer<TransactionResult> postTransactionOperation : transaction.getPostTransactionOperations()) {
					try {
						postTransactionOperation.accept(committed ? TransactionResult.COMMITTED : TransactionResult.ROLLED_BACK);
					} catch (Throwable cleanupException) {
						if (cleanupFailure == null)
							cleanupFailure = cleanupException;
						else
							cleanupFailure.addSuppressed(cleanupException);
					}
				}
			}

			if (cleanupFailure != null) {
				if (thrown != null) {
					thrown.addSuppressed(cleanupFailure);
				} else if (cleanupFailure instanceof RuntimeException) {
					throw (RuntimeException) cleanupFailure;
				} else if (cleanupFailure instanceof Error) {
					throw (Error) cleanupFailure;
				} else {
					throw new RuntimeException(cleanupFailure);
				}
			}
		}
	}

	protected void closeConnection(@Nonnull Connection connection) {
		requireNonNull(connection);

		try {
			connection.close();
		} catch (SQLException e) {
			throw new DatabaseException("Unable to close database connection", e);
		}
	}

	/**
	 * Performs an operation in the context of a pre-existing transaction.
	 * <p>
	 * No commit or rollback on the transaction will occur when {@code transactionalOperation} completes.
	 * <p>
	 * However, if an exception bubbles out of {@code transactionalOperation}, the transaction will be marked as rollback-only.
	 *
	 * @param transaction            the transaction in which to participate
	 * @param transactionalOperation the operation that should participate in the transaction
	 */
	public void participate(@Nonnull Transaction transaction,
													@Nonnull TransactionalOperation transactionalOperation) {
		requireNonNull(transaction);
		requireNonNull(transactionalOperation);

		participate(transaction, () -> {
			transactionalOperation.perform();
			return Optional.empty();
		});
	}

	/**
	 * Performs an operation in the context of a pre-existing transaction, optionall returning a value.
	 * <p>
	 * No commit or rollback on the transaction will occur when {@code transactionalOperation} completes.
	 * <p>
	 * However, if an exception bubbles out of {@code transactionalOperation}, the transaction will be marked as rollback-only.
	 *
	 * @param transaction            the transaction in which to participate
	 * @param transactionalOperation the operation that should participate in the transaction
	 * @param <T>                    the type to be returned
	 * @return the result of the transactional operation
	 */
	@Nonnull
	public <T> Optional<T> participate(@Nonnull Transaction transaction,
																		 @Nonnull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transaction);
		requireNonNull(transactionalOperation);

		Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();
		transactionStack.push(transaction);

		try {
			Optional<T> returnValue = transactionalOperation.perform();
			return returnValue == null ? Optional.empty() : returnValue;
		} catch (RuntimeException e) {
			transaction.setRollbackOnly(true);
			throw e;
		} catch (Throwable t) {
			transaction.setRollbackOnly(true);
			throw new RuntimeException(t);
		} finally {
			transactionStack.pop();
			if (transactionStack.isEmpty())
				TRANSACTION_STACK_HOLDER.remove();
		}
	}

	/**
	 * Creates a fluent builder for executing SQL.
	 * <p>
	 * Named parameters use the {@code :paramName} syntax and are bound via {@link Query#bind(String, Object)}.
	 * Positional parameters via {@code ?} are not supported.
	 * <p>
	 * Example:
	 * <pre>{@code
	 * Optional<Employee> employee = database.query("SELECT * FROM employee WHERE id = :id")
	 *   .bind("id", 42)
	 *   .fetchObject(Employee.class);
	 * }</pre>
	 *
	 * @param sql SQL containing {@code :paramName} placeholders
	 * @return a fluent builder for binding parameters and executing
	 * @since 4.0.0
	 */
	@Nonnull
	public Query query(@Nonnull String sql) {
		requireNonNull(sql);
		return new DefaultQuery(this, sql);
	}

	/**
	 * Default internal implementation of {@link Query}.
	 * <p>
	 * This class is intended for use by a single thread.
	 */
	@NotThreadSafe
	private static final class DefaultQuery implements Query {
		@Nonnull
		private final Database database;
		@Nonnull
		private final String originalSql;
		@Nonnull
		private final List<String> sqlFragments;
		@Nonnull
		private final List<String> parameterNames;
		@Nonnull
		private final Set<String> distinctParameterNames;
		@Nonnull
		private final Map<String, Object> bindings;
		@Nullable
		private Object id;

		private DefaultQuery(@Nonnull Database database,
												 @Nonnull String sql) {
			requireNonNull(database);
			requireNonNull(sql);

			this.database = database;
			this.originalSql = sql;

			ParsedSql parsedSql = parseNamedParameterSql(sql);
			this.sqlFragments = parsedSql.sqlFragments;
			this.parameterNames = parsedSql.parameterNames;
			this.distinctParameterNames = parsedSql.distinctParameterNames;

			this.bindings = new LinkedHashMap<>(Math.max(8, this.distinctParameterNames.size()));
		}

		@Nonnull
		@Override
		public Query bind(@Nonnull String name,
											@Nullable Object value) {
			requireNonNull(name);

			if (!this.distinctParameterNames.contains(name))
				throw new IllegalArgumentException(format("Unknown named parameter '%s' for SQL: %s", name, this.originalSql));

			this.bindings.put(name, value);
			return this;
		}

		@Nonnull
		@Override
		public Query bindAll(@Nonnull Map<String, Object> parameters) {
			requireNonNull(parameters);

			for (Map.Entry<String, Object> entry : parameters.entrySet())
				bind(entry.getKey(), entry.getValue());

			return this;
		}

		@Nonnull
		@Override
		public Query id(@Nullable Object id) {
			this.id = id;
			return this;
		}

		@Nonnull
		@Override
		public <T> Optional<T> fetchObject(@Nonnull Class<T> resultType) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.queryForObject(preparedQuery.statement, resultType, preparedQuery.parameters);
		}

		@Nonnull
		@Override
		public <T> List<T> fetchList(@Nonnull Class<T> resultType) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.queryForList(preparedQuery.statement, resultType, preparedQuery.parameters);
		}

		@Nonnull
		@Override
		public Long execute() {
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.execute(preparedQuery.statement, preparedQuery.parameters);
		}

		@Nonnull
		@Override
		public List<Long> executeBatch(@Nonnull List<Map<String, Object>> parameterGroups) {
			requireNonNull(parameterGroups);

			List<List<Object>> parametersAsList = new ArrayList<>(parameterGroups.size());
			Object statementId = this.id == null ? this.database.generateId() : this.id;
			Statement statement = null;
			String expandedSql = null;

			for (Map<String, Object> parameterGroup : parameterGroups) {
				requireNonNull(parameterGroup);

				for (String parameterName : parameterGroup.keySet())
					if (!this.distinctParameterNames.contains(parameterName))
						throw new IllegalArgumentException(format("Unknown named parameter '%s' for SQL: %s", parameterName, this.originalSql));

				Map<String, Object> mergedBindings;
				if (this.bindings.isEmpty()) {
					mergedBindings = parameterGroup;
				} else if (parameterGroup.isEmpty()) {
					mergedBindings = this.bindings;
				} else {
					Map<String, Object> combinedBindings = new LinkedHashMap<>(this.bindings);
					combinedBindings.putAll(parameterGroup);
					mergedBindings = combinedBindings;
				}

				PreparedQuery preparedQuery = prepare(mergedBindings, statementId);

				if (expandedSql == null) {
					expandedSql = preparedQuery.statement.getSql();
					statement = preparedQuery.statement;
				} else if (!expandedSql.equals(preparedQuery.statement.getSql())) {
					throw new IllegalArgumentException(format(
							"Inconsistent SQL after expanding parameters for batch execution; ensure collection sizes are consistent. SQL: %s",
							this.originalSql));
				}

				parametersAsList.add(Arrays.asList(preparedQuery.parameters));
			}

			if (statement == null)
				statement = Statement.of(statementId, buildPlaceholderSql());

			return this.database.executeBatch(statement, parametersAsList);
		}

		@Nonnull
		@Override
		public <T> Optional<T> executeForObject(@Nonnull Class<T> resultType) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.executeForObject(preparedQuery.statement, resultType, preparedQuery.parameters);
		}

		@Nonnull
		@Override
		public <T> List<T> executeForList(@Nonnull Class<T> resultType) {
			requireNonNull(resultType);
			PreparedQuery preparedQuery = prepare(this.bindings);
			return this.database.executeForList(preparedQuery.statement, resultType, preparedQuery.parameters);
		}

		@Nonnull
		private PreparedQuery prepare(@Nonnull Map<String, Object> bindings) {
			Object statementId = this.id == null ? this.database.generateId() : this.id;
			return prepare(bindings, statementId);
		}

		@Nonnull
		private PreparedQuery prepare(@Nonnull Map<String, Object> bindings,
																	@Nonnull Object statementId) {
			requireNonNull(bindings);
			requireNonNull(statementId);

			if (this.parameterNames.isEmpty())
				return new PreparedQuery(Statement.of(statementId, this.originalSql), new Object[0]);

			StringBuilder sql = new StringBuilder(this.originalSql.length() + this.parameterNames.size() * 2);
			List<String> missingParameterNames = null;
			List<Object> parameters = new ArrayList<>(this.parameterNames.size());

			for (int i = 0; i < this.parameterNames.size(); ++i) {
				String parameterName = this.parameterNames.get(i);
				sql.append(this.sqlFragments.get(i));

				if (!bindings.containsKey(parameterName)) {
					if (missingParameterNames == null)
						missingParameterNames = new ArrayList<>();

					missingParameterNames.add(parameterName);
					sql.append('?');
					continue;
				}

				Object value = bindings.get(parameterName);

				if (value instanceof InListParameter inListParameter) {
					Object[] elements = inListParameter.getElements().orElse(null);

					if (elements == null)
						throw new IllegalArgumentException(format("IN-list parameter '%s' for SQL: %s is null", parameterName, this.originalSql));
					if (elements.length == 0)
						throw new IllegalArgumentException(format("IN-list parameter '%s' for SQL: %s is empty", parameterName, this.originalSql));

					appendPlaceholders(sql, elements.length);
					parameters.addAll(Arrays.asList(elements));
				} else {
					sql.append('?');
					parameters.add(value);
				}
			}

			sql.append(this.sqlFragments.get(this.sqlFragments.size() - 1));

			if (missingParameterNames != null)
				throw new IllegalArgumentException(format("Missing required named parameters %s for SQL: %s", missingParameterNames, this.originalSql));

			return new PreparedQuery(Statement.of(statementId, sql.toString()), parameters.toArray());
		}

		@Nonnull
		private String buildPlaceholderSql() {
			if (this.parameterNames.isEmpty())
				return this.originalSql;

			StringBuilder sql = new StringBuilder(this.originalSql.length() + this.parameterNames.size() * 2);

			for (int i = 0; i < this.parameterNames.size(); ++i)
				sql.append(this.sqlFragments.get(i)).append('?');

			sql.append(this.sqlFragments.get(this.sqlFragments.size() - 1));
			return sql.toString();
		}

		private void appendPlaceholders(@Nonnull StringBuilder sql,
																		int count) {
			requireNonNull(sql);

			for (int i = 0; i < count; ++i) {
				if (i > 0)
					sql.append(", ");
				sql.append('?');
			}
		}

		private static final class PreparedQuery {
			@Nonnull
			private final Statement statement;
			@Nonnull
			private final Object[] parameters;

			private PreparedQuery(@Nonnull Statement statement,
														@Nonnull Object[] parameters) {
				this.statement = requireNonNull(statement);
				this.parameters = requireNonNull(parameters);
			}
		}

		private static final class ParsedSql {
			@Nonnull
			private final List<String> sqlFragments;
			@Nonnull
			private final List<String> parameterNames;
			@Nonnull
			private final Set<String> distinctParameterNames;

			private ParsedSql(@Nonnull List<String> sqlFragments,
												@Nonnull List<String> parameterNames,
												@Nonnull Set<String> distinctParameterNames) {
				requireNonNull(sqlFragments);
				requireNonNull(parameterNames);
				requireNonNull(distinctParameterNames);

				this.sqlFragments = sqlFragments;
				this.parameterNames = parameterNames;
				this.distinctParameterNames = distinctParameterNames;
			}
		}

		@Nonnull
		private static ParsedSql parseNamedParameterSql(@Nonnull String sql) {
			requireNonNull(sql);

			List<String> sqlFragments = new ArrayList<>();
			StringBuilder sqlFragment = new StringBuilder(sql.length());
			List<String> parameterNames = new ArrayList<>();
			Set<String> distinctParameterNames = new HashSet<>();

			boolean inSingleQuote = false;
			boolean inDoubleQuote = false;
			boolean inBacktickQuote = false;
			boolean inBracketQuote = false;
			boolean inLineComment = false;
			boolean inBlockComment = false;
			String dollarQuoteDelimiter = null;

			for (int i = 0; i < sql.length(); ) {
				if (dollarQuoteDelimiter != null) {
					if (sql.startsWith(dollarQuoteDelimiter, i)) {
						sqlFragment.append(dollarQuoteDelimiter);
						i += dollarQuoteDelimiter.length();
						dollarQuoteDelimiter = null;
					} else {
						sqlFragment.append(sql.charAt(i));
						++i;
					}

					continue;
				}

				char c = sql.charAt(i);

				if (inLineComment) {
					sqlFragment.append(c);
					++i;

					if (c == '\n' || c == '\r')
						inLineComment = false;

					continue;
				}

				if (inBlockComment) {
					sqlFragment.append(c);

					if (c == '*' && i + 1 < sql.length() && sql.charAt(i + 1) == '/') {
						sqlFragment.append('/');
						i += 2;
						inBlockComment = false;
					} else {
						++i;
					}

					continue;
				}

				if (inSingleQuote) {
					sqlFragment.append(c);

					if (c == '\'') {
						// Escaped quote: ''
						if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
							sqlFragment.append('\'');
							i += 2;
							continue;
						}

						inSingleQuote = false;
					}

					++i;
					continue;
				}

				if (inDoubleQuote) {
					sqlFragment.append(c);

					if (c == '"') {
						// Escaped quote: ""
						if (i + 1 < sql.length() && sql.charAt(i + 1) == '"') {
							sqlFragment.append('"');
							i += 2;
							continue;
						}

						inDoubleQuote = false;
					}

					++i;
					continue;
				}

				if (inBacktickQuote) {
					sqlFragment.append(c);

					if (c == '`')
						inBacktickQuote = false;

					++i;
					continue;
				}

				if (inBracketQuote) {
					sqlFragment.append(c);

					if (c == ']')
						inBracketQuote = false;

					++i;
					continue;
				}

				// Not inside string/comment
				if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
					sqlFragment.append("--");
					i += 2;
					inLineComment = true;
					continue;
				}

				if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
					sqlFragment.append("/*");
					i += 2;
					inBlockComment = true;
					continue;
				}

				if (c == '\'') {
					inSingleQuote = true;
					sqlFragment.append(c);
					++i;
					continue;
				}

				if (c == '"') {
					inDoubleQuote = true;
					sqlFragment.append(c);
					++i;
					continue;
				}

				if (c == '`') {
					inBacktickQuote = true;
					sqlFragment.append(c);
					++i;
					continue;
				}

				if (c == '[') {
					inBracketQuote = true;
					sqlFragment.append(c);
					++i;
					continue;
				}

				if (c == '$') {
					String delimiter = parseDollarQuoteDelimiter(sql, i);

					if (delimiter != null) {
						sqlFragment.append(delimiter);
						i += delimiter.length();
						dollarQuoteDelimiter = delimiter;
						continue;
					}
				}

				if (c == '?')
					throw new IllegalArgumentException(format("Positional parameters ('?') are not supported. Use named parameters (e.g. ':id') and %s#bind. SQL: %s",
							Query.class.getSimpleName(), sql));

				if (c == ':' && i + 1 < sql.length() && sql.charAt(i + 1) == ':') {
					// Postgres type-cast operator (::), do not treat second ':' as a parameter prefix.
					sqlFragment.append("::");
					i += 2;
					continue;
				}

				if (c == ':' && i + 1 < sql.length() && Character.isJavaIdentifierStart(sql.charAt(i + 1))) {
					int nameStartIndex = i + 1;
					int nameEndIndex = nameStartIndex + 1;

					while (nameEndIndex < sql.length() && Character.isJavaIdentifierPart(sql.charAt(nameEndIndex)))
						++nameEndIndex;

					String parameterName = sql.substring(nameStartIndex, nameEndIndex);
					parameterNames.add(parameterName);
					distinctParameterNames.add(parameterName);
					sqlFragments.add(sqlFragment.toString());
					sqlFragment.setLength(0);
					i = nameEndIndex;
					continue;
				}

				sqlFragment.append(c);
				++i;
			}

			sqlFragments.add(sqlFragment.toString());

			return new ParsedSql(List.copyOf(sqlFragments), List.copyOf(parameterNames), Set.copyOf(distinctParameterNames));
		}

		@Nullable
		private static String parseDollarQuoteDelimiter(@Nonnull String sql,
																										int startIndex) {
			requireNonNull(sql);

			if (startIndex < 0 || startIndex >= sql.length())
				return null;

			if (sql.charAt(startIndex) != '$')
				return null;

			int i = startIndex + 1;

			while (i < sql.length()) {
				char c = sql.charAt(i);

				if (c == '$')
					return sql.substring(startIndex, i + 1);

				if (Character.isLetterOrDigit(c) || c == '_') {
					++i;
					continue;
				}

				return null;
			}

			return null;
		}
	}

	/**
	 * Performs a SQL query that is expected to return 0 or 1 result rows.
	 *
	 * @param sql              the SQL query to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a single result (or no result)
	 * @throws DatabaseException if > 1 row is returned
	 */
	@Nonnull
	private <T> Optional<T> queryForObject(@Nonnull String sql,
																				 @Nonnull Class<T> resultSetRowType,
																				 @Nullable Object... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return queryForObject(Statement.of(generateId(), sql), resultSetRowType, parameters);
	}

	/**
	 * Performs a SQL query that is expected to return 0 or 1 result rows.
	 *
	 * @param statement        the SQL statement to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a single result (or no result)
	 * @throws DatabaseException if > 1 row is returned
	 */
	private <T> Optional<T> queryForObject(@Nonnull Statement statement,
																				 @Nonnull Class<T> resultSetRowType,
																				 @Nullable Object... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		ResultHolder<Optional<T>> resultHolder = new ResultHolder<>();
		StatementContext<T> statementContext = StatementContext.<T>with(statement, this)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);

		performDatabaseOperation(statementContext, parametersAsList, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();

			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
				startTime = nanoTime();

				Optional<T> result = Optional.empty();

				if (resultSet.next()) {
					try {
						T value = getResultSetMapper().map(statementContext, resultSet, statementContext.getResultSetRowType().get(), getInstanceProvider()).orElse(null);
						result = Optional.ofNullable(value);
					} catch (SQLException e) {
						throw new DatabaseException(format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), statementContext.getResultSetRowType().get()), e);
					}

					if (resultSet.next())
						throw new DatabaseException("Expected 1 row in resultset but got more than 1 instead");
				}

				resultHolder.value = result;
				Duration resultSetMappingDuration = Duration.ofNanos(nanoTime() - startTime);
				return new DatabaseOperationResult(executionDuration, resultSetMappingDuration);
			}
		});

		return resultHolder.value;
	}

	/**
	 * Performs a SQL query that is expected to return any number of result rows.
	 *
	 * @param sql              the SQL query to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a list of results
	 */
	@Nonnull
	private <T> List<T> queryForList(@Nonnull String sql,
																	 @Nonnull Class<T> resultSetRowType,
																	 @Nullable Object... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return queryForList(Statement.of(generateId(), sql), resultSetRowType, parameters);
	}

	/**
	 * Performs a SQL query that is expected to return any number of result rows.
	 *
	 * @param statement        the SQL statement to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a list of results
	 */
	@Nonnull
	private <T> List<T> queryForList(@Nonnull Statement statement,
																	 @Nonnull Class<T> resultSetRowType,
																	 @Nullable Object... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		List<T> list = new ArrayList<>();
		StatementContext<T> statementContext = StatementContext.<T>with(statement, this)
				.resultSetRowType(resultSetRowType)
				.parameters(parameters)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);

		performDatabaseOperation(statementContext, parametersAsList, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();

			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
				startTime = nanoTime();

				while (resultSet.next()) {
					try {
						T listElement = getResultSetMapper().map(statementContext, resultSet, statementContext.getResultSetRowType().get(), getInstanceProvider()).orElse(null);
						list.add(listElement);
					} catch (SQLException e) {
						throw new DatabaseException(format("Unable to map JDBC %s row to %s", ResultSet.class.getSimpleName(), statementContext.getResultSetRowType().get()), e);
					}
				}

				Duration resultSetMappingDuration = Duration.ofNanos(nanoTime() - startTime);
				return new DatabaseOperationResult(executionDuration, resultSetMappingDuration);
			}
		});

		return list;
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE};
	 * or a SQL statement that returns nothing, such as a DDL statement.
	 *
	 * @param sql        the SQL to execute
	 * @param parameters {@link PreparedStatement} parameters, if any
	 * @return the number of rows affected by the SQL statement
	 */
	@Nonnull
	private Long execute(@Nonnull String sql,
											 @Nullable Object... parameters) {
		requireNonNull(sql);
		return execute(Statement.of(generateId(), sql), parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE};
	 * or a SQL statement that returns nothing, such as a DDL statement.
	 *
	 * @param statement  the SQL statement to execute
	 * @param parameters {@link PreparedStatement} parameters, if any
	 * @return the number of rows affected by the SQL statement
	 */
	@Nonnull
	private Long execute(@Nonnull Statement statement,
											 @Nullable Object... parameters) {
		requireNonNull(statement);

		ResultHolder<Long> resultHolder = new ResultHolder<>();
		StatementContext<Void> statementContext = StatementContext.with(statement, this)
				.parameters(parameters)
				.build();

		List<Object> parametersAsList = parameters == null ? List.of() : Arrays.asList(parameters);

		performDatabaseOperation(statementContext, parametersAsList, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();

			DatabaseOperationSupportStatus executeLargeUpdateSupported = getExecuteLargeUpdateSupported();

			// Use the appropriate "large" value if we know it.
			// If we don't know it, detect it and store it.
			if (executeLargeUpdateSupported == DatabaseOperationSupportStatus.YES) {
				resultHolder.value = preparedStatement.executeLargeUpdate();
			} else if (executeLargeUpdateSupported == DatabaseOperationSupportStatus.NO) {
				resultHolder.value = (long) preparedStatement.executeUpdate();
			} else {
				// If the driver doesn't support executeLargeUpdate, then UnsupportedOperationException is thrown.
				try {
					resultHolder.value = preparedStatement.executeLargeUpdate();
					setExecuteLargeUpdateSupported(DatabaseOperationSupportStatus.YES);
				} catch (UnsupportedOperationException e) {
					setExecuteLargeUpdateSupported(DatabaseOperationSupportStatus.NO);
					resultHolder.value = (long) preparedStatement.executeUpdate();
				}
			}

			Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
			return new DatabaseOperationResult(executionDuration, null);
		});

		return resultHolder.value;
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns 0 or 1 rows, e.g. with Postgres/Oracle's {@code RETURNING} clause.
	 *
	 * @param sql              the SQL query to execute
	 * @param resultSetRowType the type to which the {@link ResultSet} row should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a single result (or no result)
	 * @throws DatabaseException if > 1 row is returned
	 */
	@Nonnull
	private <T> Optional<T> executeForObject(@Nonnull String sql,
																					 @Nonnull Class<T> resultSetRowType,
																					 @Nullable Object... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return executeForObject(Statement.of(generateId(), sql), resultSetRowType, parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns 0 or 1 rows, e.g. with Postgres/Oracle's {@code RETURNING} clause.
	 *
	 * @param statement        the SQL statement to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a single result (or no result)
	 * @throws DatabaseException if > 1 row is returned
	 */
	private <T> Optional<T> executeForObject(@Nonnull Statement statement,
																					 @Nonnull Class<T> resultSetRowType,
																					 @Nullable Object... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		// Ultimately we just delegate to queryForObject.
		// Having `executeForList` is to allow for users to explicitly express intent
		// and make static analysis of code easier (e.g. maybe you'd like to hook all of your "execute" statements for
		// logging, or delegation to a writable master as opposed to a read replica)
		return queryForObject(statement, resultSetRowType, parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns any number of rows, e.g. with Postgres/Oracle's {@code RETURNING} clause.
	 *
	 * @param sql              the SQL to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a list of results
	 */
	@Nonnull
	private <T> List<T> executeForList(@Nonnull String sql,
																		 @Nonnull Class<T> resultSetRowType,
																		 @Nullable Object... parameters) {
		requireNonNull(sql);
		requireNonNull(resultSetRowType);

		return executeForList(Statement.of(generateId(), sql), resultSetRowType, parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE},
	 * which returns any number of rows, e.g. with Postgres/Oracle's {@code RETURNING} clause.
	 *
	 * @param statement        the SQL statement to execute
	 * @param resultSetRowType the type to which {@link ResultSet} rows should be marshaled
	 * @param parameters       {@link PreparedStatement} parameters, if any
	 * @param <T>              the type to be returned
	 * @return a list of results
	 */
	@Nonnull
	private <T> List<T> executeForList(@Nonnull Statement statement,
																		 @Nonnull Class<T> resultSetRowType,
																		 @Nullable Object... parameters) {
		requireNonNull(statement);
		requireNonNull(resultSetRowType);

		// Ultimately we just delegate to queryForList.
		// Having `executeForList` is to allow for users to explicitly express intent
		// and make static analysis of code easier (e.g. maybe you'd like to hook all of your "execute" statements for
		// logging, or delegation to a writable master as opposed to a read replica)
		return queryForList(statement, resultSetRowType, parameters);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE}
	 * in "batch" over a set of parameter groups.
	 * <p>
	 * Useful for bulk-inserting or updating large amounts of data.
	 *
	 * @param sql             the SQL to execute
	 * @param parameterGroups Groups of {@link PreparedStatement} parameters
	 * @return the number of rows affected by the SQL statement per-group
	 */
	@Nonnull
	private List<Long> executeBatch(@Nonnull String sql,
																	@Nonnull List<List<Object>> parameterGroups) {
		requireNonNull(sql);
		requireNonNull(parameterGroups);

		return executeBatch(Statement.of(generateId(), sql), parameterGroups);
	}

	/**
	 * Executes a SQL Data Manipulation Language (DML) statement, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE}
	 * in "batch" over a set of parameter groups.
	 * <p>
	 * Useful for bulk-inserting or updating large amounts of data.
	 *
	 * @param statement       the SQL statement to execute
	 * @param parameterGroups Groups of {@link PreparedStatement} parameters
	 * @return the number of rows affected by the SQL statement per-group
	 */
	@Nonnull
	private List<Long> executeBatch(@Nonnull Statement statement,
																	@Nonnull List<List<Object>> parameterGroups) {
		requireNonNull(statement);
		requireNonNull(parameterGroups);

		ResultHolder<List<Long>> resultHolder = new ResultHolder<>();
		StatementContext<List<Long>> statementContext = StatementContext.with(statement, this)
				.parameters((List) parameterGroups)
				.resultSetRowType(List.class)
				.build();

		performDatabaseOperation(statementContext, (preparedStatement) -> {
			for (List<Object> parameterGroup : parameterGroups) {
				if (parameterGroup != null && parameterGroup.size() > 0)
					performPreparedStatementBinding(statementContext, preparedStatement, parameterGroup);

				preparedStatement.addBatch();
			}
		}, (PreparedStatement preparedStatement) -> {
			long startTime = nanoTime();
			List<Long> result;

			DatabaseOperationSupportStatus executeLargeBatchSupported = getExecuteLargeBatchSupported();

			// Use the appropriate "large" value if we know it.
			// If we don't know it, detect it and store it.
			if (executeLargeBatchSupported == DatabaseOperationSupportStatus.YES) {
				long[] resultArray = preparedStatement.executeLargeBatch();
				result = Arrays.stream(resultArray).boxed().collect(Collectors.toList());
			} else if (executeLargeBatchSupported == DatabaseOperationSupportStatus.NO) {
				int[] resultArray = preparedStatement.executeBatch();
				result = Arrays.stream(resultArray).asLongStream().boxed().collect(Collectors.toList());
			} else {
				// If the driver doesn't support executeLargeBatch, then UnsupportedOperationException is thrown.
				try {
					long[] resultArray = preparedStatement.executeLargeBatch();
					result = Arrays.stream(resultArray).boxed().collect(Collectors.toList());
					setExecuteLargeBatchSupported(DatabaseOperationSupportStatus.YES);
				} catch (UnsupportedOperationException e) {
					setExecuteLargeBatchSupported(DatabaseOperationSupportStatus.NO);
					int[] resultArray = preparedStatement.executeBatch();
					result = Arrays.stream(resultArray).asLongStream().boxed().collect(Collectors.toList());
				}
			}

			resultHolder.value = result;
			Duration executionDuration = Duration.ofNanos(nanoTime() - startTime);
			return new DatabaseOperationResult(executionDuration, null);
		}, parameterGroups.size());

		return resultHolder.value;
	}

	/**
	 * Exposes a temporary handle to JDBC {@link DatabaseMetaData}, which provides comprehensive vendor-specific information about this database as a whole.
	 * <p>
	 * This method acquires {@link DatabaseMetaData} on its own newly-borrowed connection, which it manages internally.
	 * <p>
	 * It does <strong>not</strong> participate in the active transaction, if one exists.
	 * <p>
	 * The connection is closed as soon as {@link DatabaseMetaDataReader#read(DatabaseMetaData)} completes.
	 * <p>
	 * See <a href="https://docs.oracle.com/en/java/javase/24/docs/api/java.sql/java/sql/DatabaseMetaData.html">{@code DatabaseMetaData} Javadoc</a> for details.
	 */
	public void readDatabaseMetaData(@Nonnull DatabaseMetaDataReader databaseMetaDataReader) {
		requireNonNull(databaseMetaDataReader);

		performRawConnectionOperation((connection -> {
			databaseMetaDataReader.read(connection.getMetaData());
			return Optional.empty();
		}), false);
	}

	protected <T> void performDatabaseOperation(@Nonnull StatementContext<T> statementContext,
																							@Nonnull List<Object> parameters,
																							@Nonnull DatabaseOperation databaseOperation) {
		requireNonNull(statementContext);
		requireNonNull(parameters);
		requireNonNull(databaseOperation);

		performDatabaseOperation(statementContext, (preparedStatement) -> {
			if (parameters.size() > 0)
				performPreparedStatementBinding(statementContext, preparedStatement, parameters);
		}, databaseOperation);
	}

	protected <T> void performPreparedStatementBinding(@Nonnull StatementContext<T> statementContext,
																										 @Nonnull PreparedStatement preparedStatement,
																										 @Nonnull List<Object> parameters) {
		requireNonNull(statementContext);
		requireNonNull(preparedStatement);
		requireNonNull(parameters);

		try {
			for (int i = 0; i < parameters.size(); ++i) {
				Object parameter = parameters.get(i);

				if (parameter != null) {
					getPreparedStatementBinder().bindParameter(statementContext, preparedStatement, i + 1, parameter);
				} else {
					try {
						ParameterMetaData parameterMetaData = preparedStatement.getParameterMetaData();

						if (parameterMetaData != null) {
							preparedStatement.setNull(i + 1, parameterMetaData.getParameterType(i + 1));
						} else {
							preparedStatement.setNull(i + 1, Types.NULL);
						}
					} catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
						preparedStatement.setNull(i + 1, Types.NULL);
					}
				}
			}
		} catch (Exception e) {
			throw new DatabaseException(e);
		}
	}

	@FunctionalInterface
	protected interface RawConnectionOperation<R> {
		@Nonnull
		Optional<R> perform(@Nonnull Connection connection) throws Exception;
	}

	/**
	 * @since 3.0.0
	 */
	@Nonnull
	public DatabaseType getDatabaseType() {
		return this.databaseType;
	}

	/**
	 * @since 3.0.0
	 */
	@Nonnull
	public ZoneId getTimeZone() {
		return this.timeZone;
	}

	/**
	 * Useful for single-shot "utility" calls that operate outside of normal query operations, e.g. pulling DB metadata.
	 * <p>
	 * Example: {@link #readDatabaseMetaData(DatabaseMetaDataReader)}.
	 */
	@Nonnull
	protected <R> Optional<R> performRawConnectionOperation(@Nonnull RawConnectionOperation<R> rawConnectionOperation,
																													@Nonnull Boolean shouldParticipateInExistingTransactionIfPossible) {
		requireNonNull(rawConnectionOperation);
		requireNonNull(shouldParticipateInExistingTransactionIfPossible);

		if (shouldParticipateInExistingTransactionIfPossible) {
			Optional<Transaction> transaction = currentTransaction();
			ReentrantLock connectionLock = transaction.isPresent() ? transaction.get().getConnectionLock() : null;
			// Try to participate in txn if it's available
			Connection connection = null;
			Throwable thrown = null;

			if (connectionLock != null)
				connectionLock.lock();

			try {
				connection = transaction.isPresent() ? transaction.get().getConnection() : acquireConnection();
				return rawConnectionOperation.perform(connection);
			} catch (DatabaseException e) {
				thrown = e;
				throw e;
			} catch (Exception e) {
				DatabaseException wrapped = new DatabaseException(e);
				thrown = wrapped;
				throw wrapped;
			} finally {
				Throwable cleanupFailure = null;

				try {
					// If this was a single-shot operation (not in a transaction), close the connection
					if (connection != null && !transaction.isPresent()) {
						try {
							closeConnection(connection);
						} catch (Throwable cleanupException) {
							cleanupFailure = cleanupException;
						}
					}
				} finally {
					if (connectionLock != null)
						connectionLock.unlock();

					if (cleanupFailure != null) {
						if (thrown != null) {
							thrown.addSuppressed(cleanupFailure);
						} else if (cleanupFailure instanceof RuntimeException) {
							throw (RuntimeException) cleanupFailure;
						} else if (cleanupFailure instanceof Error) {
							throw (Error) cleanupFailure;
						} else {
							throw new RuntimeException(cleanupFailure);
						}
					}
				}
			}
		} else {
			boolean acquiredConnection = false;
			Connection connection = null;
			Throwable thrown = null;

			// Always get a fresh connection no matter what and close it afterwards
			try {
				connection = getDataSource().getConnection();
				acquiredConnection = true;
				return rawConnectionOperation.perform(connection);
			} catch (DatabaseException e) {
				thrown = e;
				throw e;
			} catch (Exception e) {
				DatabaseException wrapped = acquiredConnection
						? new DatabaseException(e)
						: new DatabaseException("Unable to acquire database connection", e);
				thrown = wrapped;
				throw wrapped;
			} finally {
				if (connection != null) {
					try {
						closeConnection(connection);
					} catch (Throwable cleanupException) {
						if (thrown != null) {
							thrown.addSuppressed(cleanupException);
						} else if (cleanupException instanceof RuntimeException) {
							throw (RuntimeException) cleanupException;
						} else if (cleanupException instanceof Error) {
							throw (Error) cleanupException;
						} else {
							throw new RuntimeException(cleanupException);
						}
					}
				}
			}
		}
	}

	protected <T> void performDatabaseOperation(@Nonnull StatementContext<T> statementContext,
																							@Nonnull PreparedStatementBindingOperation preparedStatementBindingOperation,
																							@Nonnull DatabaseOperation databaseOperation) {
		performDatabaseOperation(statementContext, preparedStatementBindingOperation, databaseOperation, null);
	}

	protected <T> void performDatabaseOperation(@Nonnull StatementContext<T> statementContext,
																							@Nonnull PreparedStatementBindingOperation preparedStatementBindingOperation,
																							@Nonnull DatabaseOperation databaseOperation,
																							@Nullable Integer batchSize) {
		requireNonNull(statementContext);
		requireNonNull(preparedStatementBindingOperation);
		requireNonNull(databaseOperation);

		long startTime = nanoTime();
		Duration connectionAcquisitionDuration = null;
		Duration preparationDuration = null;
		Duration executionDuration = null;
		Duration resultSetMappingDuration = null;
		Exception exception = null;
		Throwable thrown = null;
		Connection connection = null;
		Optional<Transaction> transaction = currentTransaction();
		ReentrantLock connectionLock = transaction.isPresent() ? transaction.get().getConnectionLock() : null;

		if (connectionLock != null)
			connectionLock.lock();

		try {
			boolean alreadyHasConnection = transaction.isPresent() && transaction.get().hasConnection();
			connection = transaction.isPresent() ? transaction.get().getConnection() : acquireConnection();
			connectionAcquisitionDuration = alreadyHasConnection ? null : Duration.ofNanos(nanoTime() - startTime);
			startTime = nanoTime();

			try (PreparedStatement preparedStatement = connection.prepareStatement(statementContext.getStatement().getSql())) {
				preparedStatementBindingOperation.perform(preparedStatement);
				preparationDuration = Duration.ofNanos(nanoTime() - startTime);

				DatabaseOperationResult databaseOperationResult = databaseOperation.perform(preparedStatement);
				executionDuration = databaseOperationResult.getExecutionDuration().orElse(null);
				resultSetMappingDuration = databaseOperationResult.getResultSetMappingDuration().orElse(null);
			}
		} catch (DatabaseException e) {
			exception = e;
			thrown = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			DatabaseException wrapped = new DatabaseException(e);
			thrown = wrapped;
			throw wrapped;
		} finally {
			Throwable cleanupFailure = null;

			try {
				// If this was a single-shot operation (not in a transaction), close the connection
				if (connection != null && !transaction.isPresent()) {
					try {
						closeConnection(connection);
					} catch (Throwable cleanupException) {
						cleanupFailure = cleanupException;
					}
				}
			} finally {
				if (connectionLock != null)
					connectionLock.unlock();

				StatementLog statementLog =
						StatementLog.withStatementContext(statementContext)
								.connectionAcquisitionDuration(connectionAcquisitionDuration)
								.preparationDuration(preparationDuration)
								.executionDuration(executionDuration)
								.resultSetMappingDuration(resultSetMappingDuration)
								.batchSize(batchSize)
								.exception(exception)
								.build();

				try {
					getStatementLogger().log(statementLog);
				} catch (Throwable cleanupException) {
					if (cleanupFailure == null)
						cleanupFailure = cleanupException;
					else
						cleanupFailure.addSuppressed(cleanupException);
				}
			}

			if (cleanupFailure != null) {
				if (thrown != null) {
					thrown.addSuppressed(cleanupFailure);
				} else if (cleanupFailure instanceof RuntimeException) {
					throw (RuntimeException) cleanupFailure;
				} else if (cleanupFailure instanceof Error) {
					throw (Error) cleanupFailure;
				} else {
					throw new RuntimeException(cleanupFailure);
				}
			}
		}
	}

	@Nonnull
	protected Connection acquireConnection() {
		Optional<Transaction> transaction = currentTransaction();

		if (transaction.isPresent())
			return transaction.get().getConnection();

		try {
			return getDataSource().getConnection();
		} catch (SQLException e) {
			throw new DatabaseException("Unable to acquire database connection", e);
		}
	}

	@Nonnull
	protected DataSource getDataSource() {
		return this.dataSource;
	}

	@Nonnull
	protected InstanceProvider getInstanceProvider() {
		return this.instanceProvider;
	}

	@Nonnull
	protected PreparedStatementBinder getPreparedStatementBinder() {
		return this.preparedStatementBinder;
	}

	@Nonnull
	protected ResultSetMapper getResultSetMapper() {
		return this.resultSetMapper;
	}

	@Nonnull
	protected StatementLogger getStatementLogger() {
		return this.statementLogger;
	}

	@Nonnull
	protected DatabaseOperationSupportStatus getExecuteLargeBatchSupported() {
		return this.executeLargeBatchSupported;
	}

	protected void setExecuteLargeBatchSupported(@Nonnull DatabaseOperationSupportStatus executeLargeBatchSupported) {
		requireNonNull(executeLargeBatchSupported);
		this.executeLargeBatchSupported = executeLargeBatchSupported;
	}

	@Nonnull
	protected DatabaseOperationSupportStatus getExecuteLargeUpdateSupported() {
		return this.executeLargeUpdateSupported;
	}

	protected void setExecuteLargeUpdateSupported(@Nonnull DatabaseOperationSupportStatus executeLargeUpdateSupported) {
		requireNonNull(executeLargeUpdateSupported);
		this.executeLargeUpdateSupported = executeLargeUpdateSupported;
	}

	@Nonnull
	protected Object generateId() {
		// "Unique" keys
		return format("com.pyranid.%s", this.defaultIdGenerator.incrementAndGet());
	}

	@FunctionalInterface
	protected interface DatabaseOperation {
		@Nonnull
		DatabaseOperationResult perform(@Nonnull PreparedStatement preparedStatement) throws Exception;
	}

	@FunctionalInterface
	protected interface PreparedStatementBindingOperation {
		void perform(@Nonnull PreparedStatement preparedStatement) throws Exception;
	}

	/**
	 * Builder used to construct instances of {@link Database}.
	 * <p>
	 * This class is intended for use by a single thread.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 1.0.0
	 */
	@NotThreadSafe
	public static class Builder {
		@Nonnull
		private final DataSource dataSource;
		@Nullable
		private DatabaseType databaseType;
		@Nullable
		private ZoneId timeZone;
		@Nullable
		private InstanceProvider instanceProvider;
		@Nullable
		private PreparedStatementBinder preparedStatementBinder;
		@Nullable
		private ResultSetMapper resultSetMapper;
		@Nullable
		private StatementLogger statementLogger;

		private Builder(@Nonnull DataSource dataSource) {
			this.dataSource = requireNonNull(dataSource);
			this.databaseType = null;
		}

		/**
		 * Overrides automatic database type detection.
		 *
		 * @param databaseType the database type to use (null to enable auto-detection)
		 * @return this {@code Builder}, for chaining
		 * @since 4.0.0
		 */
		@Nonnull
		public Builder databaseType(@Nullable DatabaseType databaseType) {
			this.databaseType = databaseType;
			return this;
		}

		@Nonnull
		public Builder timeZone(@Nullable ZoneId timeZone) {
			this.timeZone = timeZone;
			return this;
		}

		@Nonnull
		public Builder instanceProvider(@Nullable InstanceProvider instanceProvider) {
			this.instanceProvider = instanceProvider;
			return this;
		}

		@Nonnull
		public Builder preparedStatementBinder(@Nullable PreparedStatementBinder preparedStatementBinder) {
			this.preparedStatementBinder = preparedStatementBinder;
			return this;
		}

		@Nonnull
		public Builder resultSetMapper(@Nullable ResultSetMapper resultSetMapper) {
			this.resultSetMapper = resultSetMapper;
			return this;
		}

		@Nonnull
		public Builder statementLogger(@Nullable StatementLogger statementLogger) {
			this.statementLogger = statementLogger;
			return this;
		}

		@Nonnull
		public Database build() {
			return new Database(this);
		}
	}

	@ThreadSafe
	static class DatabaseOperationResult {
		@Nullable
		private final Duration executionDuration;
		@Nullable
		private final Duration resultSetMappingDuration;

		public DatabaseOperationResult(@Nullable Duration executionDuration,
																	 @Nullable Duration resultSetMappingDuration) {
			this.executionDuration = executionDuration;
			this.resultSetMappingDuration = resultSetMappingDuration;
		}

		@Nonnull
		public Optional<Duration> getExecutionDuration() {
			return Optional.ofNullable(this.executionDuration);
		}

		@Nonnull
		public Optional<Duration> getResultSetMappingDuration() {
			return Optional.ofNullable(this.resultSetMappingDuration);
		}
	}

	@NotThreadSafe
	static class ResultHolder<T> {
		T value;
	}

	enum DatabaseOperationSupportStatus {
		UNKNOWN,
		YES,
		NO
	}
}
