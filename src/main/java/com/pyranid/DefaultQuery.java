/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.NotThreadSafe;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import com.pyranid.SqlCompiler.ParsedSql;
import com.pyranid.SqlCompiler.ParsedSqlVariants;
import static com.pyranid.Database.*;

/**
 * Default internal implementation of {@link Query}.
 * <p>
 * This class is intended for use by a single thread.
 */
@NotThreadSafe
final class DefaultQuery implements Query {
	@NonNull
	private final Database database;
	private final StatementExecutor executor;
	@NonNull
	private final String originalSql;
	@NonNull
	private final ParsedSqlVariants parsedSqlVariants;
	@NonNull
	private final Map<String, Object> bindings;
	@Nullable
	private PreparedStatementCustomizer preparedStatementCustomizer;
	@Nullable
	private Duration queryTimeout;
	@Nullable
	private Integer fetchSize;
	@Nullable
	private Integer maxRows;
	@Nullable
	private Integer batchChunkSize;
	@Nullable
	private Object id;
	@Nullable
	private ResultSetMapper resultSetMapper;
	@Nullable
	private PreparedStatementBinder preparedStatementBinder;

	DefaultQuery(@NonNull Database database,
											 @NonNull String sql, @NonNull SqlCompiler compiler, @NonNull StatementExecutor executor) {
		requireNonNull(database);
		requireNonNull(sql);

		this.database = database;
		this.executor = requireNonNull(executor);
		this.originalSql = sql;
		this.parsedSqlVariants = compiler.compile(sql);

		this.bindings = new LinkedHashMap<>(Math.max(8, this.parsedSqlVariants.distinctParameterNames.size()));
		this.preparedStatementCustomizer = null;
		this.queryTimeout = null;
		this.fetchSize = null;
		this.maxRows = null;
		this.batchChunkSize = null;
	}

	@NonNull
	@Override
	public Query bind(@NonNull String name,
										@Nullable Object value) {
		requireNonNull(name);

		if (!this.parsedSqlVariants.distinctParameterNames.contains(name))
			throw new IllegalArgumentException(format("Unknown named parameter '%s' for SQL: %s", name, this.originalSql));

		this.bindings.put(name, value);
		return this;
	}

	@NonNull
	@Override
	public Query bindAll(@NonNull Map<@NonNull String, @Nullable Object> parameters) {
		requireNonNull(parameters);

		for (Map.Entry<@NonNull String, @Nullable Object> entry : parameters.entrySet())
			bind(entry.getKey(), entry.getValue());

		return this;
	}

	@NonNull
	@Override
	public Query id(@Nullable Object id) {
		this.id = id;
		return this;
	}

	@NonNull
	@Override
	public Query queryTimeout(@Nullable Duration queryTimeout) {
		this.queryTimeout = validateQueryTimeout(queryTimeout);
		return this;
	}

	@NonNull
	@Override
	public Query fetchSize(@Nullable Integer fetchSize) {
		this.fetchSize = validateNonNegativeStatementSetting("fetchSize", fetchSize);
		return this;
	}

	@NonNull
	@Override
	public Query maxRows(@Nullable Integer maxRows) {
		this.maxRows = validateNonNegativeStatementSetting("maxRows", maxRows);
		return this;
	}

	@NonNull
	@Override
	public Query batchChunkSize(@Nullable Integer batchChunkSize) {
		this.batchChunkSize = validatePositiveQuerySetting("batchChunkSize", batchChunkSize);
		return this;
	}

	@NonNull
	@Override
	public Query resultSetMapper(@Nullable ResultSetMapper resultSetMapper) {
		this.resultSetMapper = resultSetMapper;
		return this;
	}

	@NonNull
	@Override
	public Query preparedStatementBinder(@Nullable PreparedStatementBinder preparedStatementBinder) {
		this.preparedStatementBinder = preparedStatementBinder;
		return this;
	}

	private StatementContext.@Nullable SpiOverrides spiOverrides() {
		return this.resultSetMapper == null && this.preparedStatementBinder == null
				? null
				: new StatementContext.SpiOverrides(this.resultSetMapper, this.preparedStatementBinder);
	}

	@NonNull
	@Override
	public Query customize(@NonNull PreparedStatementCustomizer preparedStatementCustomizer) {
		requireNonNull(preparedStatementCustomizer);
		this.preparedStatementCustomizer = preparedStatementCustomizer;

		return this;
	}

	@NonNull
	@Override
	public <T> Optional<T> fetchObject(@NonNull Class<T> resultType) {
		validateBatchChunkSizeNotSet();
		requireNonNull(resultType);
		PreparedQuery preparedQuery = prepare(this.bindings);
		return this.executor.queryForObject(preparedQuery.statement, resultType, effectivePreparedStatementCustomizer(), spiOverrides(), preparedQuery.parameters);
	}

	@NonNull
	@Override
	public <T> List<@Nullable T> fetchList(@NonNull Class<T> resultType) {
		validateBatchChunkSizeNotSet();
		requireNonNull(resultType);
		PreparedQuery preparedQuery = prepare(this.bindings);
		return this.executor.queryForList(preparedQuery.statement, resultType, effectivePreparedStatementCustomizer(), spiOverrides(), preparedQuery.parameters);
	}

	@Nullable
	@Override
	public <T, R> R fetchStream(@NonNull Class<T> resultType,
															@NonNull Function<Stream<@Nullable T>, R> streamFunction) {
		validateBatchChunkSizeNotSet();
		requireNonNull(resultType);
		requireNonNull(streamFunction);
		PreparedQuery preparedQuery = prepare(this.bindings);
		return this.executor.queryForStream(preparedQuery.statement, resultType, effectivePreparedStatementCustomizer(),
				this.fetchSize != null, streamFunction, spiOverrides(), preparedQuery.parameters);
	}


	@NonNull
	@Override
	public Long execute() {
		validateBatchChunkSizeNotSet();
		PreparedQuery preparedQuery = prepare(this.bindings);
		return this.executor.execute(preparedQuery.statement, effectivePreparedStatementCustomizer(), spiOverrides(), preparedQuery.parameters);
	}

	@NonNull
	@Override
	public <T> Optional<T> executeReturningGeneratedKey(@NonNull Class<T> resultType) {
		validateBatchChunkSizeNotSet();
		requireNonNull(resultType);
		PreparedQuery preparedQuery = prepare(this.bindings);
		return this.executor.executeReturningGeneratedKey(preparedQuery.statement, resultType,
				effectivePreparedStatementCustomizer(), new String[0], spiOverrides(), preparedQuery.parameters);
	}

	@NonNull
	@Override
	public <T> Optional<T> executeReturningGeneratedKey(@NonNull Class<T> resultType,
																											@NonNull String @NonNull ... keyColumnNames) {
		validateBatchChunkSizeNotSet();
		requireNonNull(resultType);
		PreparedQuery preparedQuery = prepare(this.bindings);
		return this.executor.executeReturningGeneratedKey(preparedQuery.statement, resultType,
				effectivePreparedStatementCustomizer(), keyColumnNames, spiOverrides(), preparedQuery.parameters);
	}

	@NonNull
	@Override
	public <T> List<@Nullable T> executeReturningGeneratedKeys(@NonNull Class<T> resultType) {
		validateBatchChunkSizeNotSet();
		requireNonNull(resultType);
		PreparedQuery preparedQuery = prepare(this.bindings);
		return this.executor.executeReturningGeneratedKeys(preparedQuery.statement, resultType,
				effectivePreparedStatementCustomizer(), new String[0], spiOverrides(), preparedQuery.parameters);
	}

	@NonNull
	@Override
	public <T> List<@Nullable T> executeReturningGeneratedKeys(@NonNull Class<T> resultType,
																												 @NonNull String @NonNull ... keyColumnNames) {
		validateBatchChunkSizeNotSet();
		requireNonNull(resultType);
		PreparedQuery preparedQuery = prepare(this.bindings);
		return this.executor.executeReturningGeneratedKeys(preparedQuery.statement, resultType,
				effectivePreparedStatementCustomizer(), keyColumnNames, spiOverrides(), preparedQuery.parameters);
	}

	@NonNull
	@Override
	public List<Long> executeBatch(@NonNull List<@NonNull Map<@NonNull String, @Nullable Object>> parameterGroups) {
		requireNonNull(parameterGroups);
		if (parameterGroups.isEmpty())
			return List.of();

		List<List<Object>> parametersAsList = new ArrayList<>(parameterGroups.size());
		Object statementId = this.id == null ? this.database.generateId() : this.id;
		Statement statement = null;
		String expandedSql = null;

		for (Map<@NonNull String, @Nullable Object> parameterGroup : parameterGroups) {
			requireNonNull(parameterGroup);

			for (String parameterName : parameterGroup.keySet())
				if (!this.parsedSqlVariants.distinctParameterNames.contains(parameterName))
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

		return this.executor.executeBatch(statement, parametersAsList, effectivePreparedStatementCustomizer(), this.batchChunkSize, spiOverrides());
	}

	@NonNull
	@Override
	public <T> Optional<T> executeForObject(@NonNull Class<T> resultType) {
		validateBatchChunkSizeNotSet();
		requireNonNull(resultType);
		PreparedQuery preparedQuery = prepare(this.bindings);
		return this.executor.executeForObject(preparedQuery.statement, resultType, effectivePreparedStatementCustomizer(), spiOverrides(), preparedQuery.parameters);
	}

	@NonNull
	@Override
	public <T> List<@Nullable T> executeForList(@NonNull Class<T> resultType) {
		validateBatchChunkSizeNotSet();
		requireNonNull(resultType);
		PreparedQuery preparedQuery = prepare(this.bindings);
		return this.executor.executeForList(preparedQuery.statement, resultType, effectivePreparedStatementCustomizer(), spiOverrides(), preparedQuery.parameters);
	}

	private void validateBatchChunkSizeNotSet() {
		if (this.batchChunkSize != null)
			throw new IllegalStateException("batchChunkSize applies only to executeBatch(...)");
	}

	@Nullable
	private PreparedStatementCustomizer effectivePreparedStatementCustomizer() {
		if (!this.database.hasDefaultPreparedStatementSettings()
				&& !hasQueryPreparedStatementSettings()
				&& this.preparedStatementCustomizer == null)
			return null;

		return (statementContext, preparedStatement) -> {
			this.database.applyDefaultPreparedStatementSettings(preparedStatement);
			applyPreparedStatementSettings(preparedStatement, this.queryTimeout, this.fetchSize, this.maxRows);

			if (this.preparedStatementCustomizer != null)
				this.preparedStatementCustomizer.customize(statementContext, preparedStatement);
		};
	}

	private boolean hasQueryPreparedStatementSettings() {
		return this.queryTimeout != null || this.fetchSize != null || this.maxRows != null;
	}

	@NonNull
	private PreparedQuery prepare(@NonNull Map<String, Object> bindings) {
		Object statementId = this.id == null ? this.database.generateId() : this.id;
		return prepare(bindings, statementId);
	}

	@NonNull
	private PreparedQuery prepare(@NonNull Map<String, Object> bindings,
																@NonNull Object statementId) {
		requireNonNull(bindings);
		requireNonNull(statementId);

		ParsedSql parsedSql = parsedSqlForDatabaseType();
		List<String> parameterNames = parsedSql.parameterNames;
		List<String> sqlFragments = sqlFragmentsForDatabaseType(parsedSql);
		validateBindingsForParsedSql(bindings, parsedSql);

		if (parameterNames.isEmpty())
			return new PreparedQuery(Statement.of(statementId, sqlFragments.get(0)), new Object[0]);

		StringBuilder sql = new StringBuilder(this.originalSql.length() + parameterNames.size() * 2);
		List<String> missingParameterNames = null;
		List<Object> parameters = new ArrayList<>(parameterNames.size());

		for (int i = 0; i < parameterNames.size(); ++i) {
			String parameterName = parameterNames.get(i);
			sql.append(sqlFragments.get(i));

			if (!bindings.containsKey(parameterName)) {
				if (missingParameterNames == null)
					missingParameterNames = new ArrayList<>();

				missingParameterNames.add(parameterName);
				sql.append('?');
				continue;
			}

			SecureParameterSupport.SecureParameterUnwrapResult secureParameterUnwrapResult =
					SecureParameterSupport.unwrapSecureAndOptionalParameterWithMetadata(bindings.get(parameterName));
			SecureParameter secureParameter = secureParameterUnwrapResult.secureParameter();
			Object value = secureParameterUnwrapResult.value();

			if (value instanceof InListParameter inListParameter) {
				Object[] elements = inListParameter.getElements();

				if (elements.length == 0)
					throw new IllegalArgumentException(format("IN-list parameter '%s' for SQL: %s is empty", parameterName, this.originalSql));

				appendPlaceholders(sql, elements.length);

				for (int j = 0; j < elements.length; ++j) {
					SecureParameterSupport.SecureParameterUnwrapResult elementUnwrapResult =
							SecureParameterSupport.unwrapSecureAndOptionalParameterWithMetadata(elements[j]);
					Object element = elementUnwrapResult.value();

					if (element == null)
						throw new IllegalArgumentException(format(
								"IN-list parameter '%s' for SQL: %s contains null element at index %d. "
										+ "SQL IN does not match NULL values; use an explicit IS NULL predicate instead.",
								parameterName, this.originalSql, j));

					SecureParameter effectiveSecureParameter = secureParameter == null
							? elementUnwrapResult.secureParameter()
							: secureParameter;
					parameters.add(effectiveSecureParameter == null
							? element
							: Parameters.secure(element, SecureParameterSupport.maskOf(effectiveSecureParameter)));
				}
			} else if (value instanceof Collection<?>) {
				throw new IllegalArgumentException(format(
						"Collection parameter '%s' for SQL: %s must be wrapped with %s.inList(...) or %s.listOf/%s.setOf(...)",
						parameterName, this.originalSql,
						Parameters.class.getSimpleName(),
						Parameters.class.getSimpleName(), Parameters.class.getSimpleName()));
			} else if (value != null && value.getClass().isArray() && !(value instanceof byte[])) {
				throw new IllegalArgumentException(format(
						"Array parameter '%s' for SQL: %s must be wrapped with %s.inList(...), %s.sqlArrayOf(...), or %s.arrayOf(Class, ...)",
						parameterName, this.originalSql,
						Parameters.class.getSimpleName(), Parameters.class.getSimpleName(), Parameters.class.getSimpleName()));
			} else {
				sql.append('?');
				parameters.add(secureParameter == null ? value : secureParameter);
			}
		}

		sql.append(sqlFragments.get(sqlFragments.size() - 1));

		if (missingParameterNames != null)
			throw new IllegalArgumentException(format("Missing required named parameters %s for SQL: %s", missingParameterNames, this.originalSql));

		return new PreparedQuery(Statement.of(statementId, sql.toString()), parameters.toArray());
	}

	@NonNull
	private String buildPlaceholderSql() {
		ParsedSql parsedSql = parsedSqlForDatabaseType();
		List<String> parameterNames = parsedSql.parameterNames;
		List<String> sqlFragments = sqlFragmentsForDatabaseType(parsedSql);

		if (parameterNames.isEmpty())
			return sqlFragments.get(0);

		StringBuilder sql = new StringBuilder(this.originalSql.length() + parameterNames.size() * 2);

		for (int i = 0; i < parameterNames.size(); ++i)
			sql.append(sqlFragments.get(i)).append('?');

		sql.append(sqlFragments.get(sqlFragments.size() - 1));
		return sql.toString();
	}

	@NonNull
	private List<String> sqlFragmentsForDatabaseType(@NonNull ParsedSql parsedSql) {
		requireNonNull(parsedSql);

		if (!parsedSql.hasQuestionMarkOperators)
			return parsedSql.sqlFragments;

		return this.database.getDatabaseDialect().sqlFragmentsForOperators(
				parsedSql.hasQuestionMarkOperators,
				parsedSql.sqlFragments,
				parsedSql.questionMarkOperatorFragmentIndexes);
	}

	@NonNull
	private ParsedSql parsedSqlForDatabaseType() {
		if (!this.parsedSqlVariants.requiresDatabaseType)
			return this.parsedSqlVariants.standardParsedSql;

		DatabaseType databaseType;
		Optional<Transaction> transaction = this.database.currentTransactionForDatabaseOperation();

		if (transaction.isPresent())
			databaseType = this.database.getDatabaseType(transaction.get().getConnection());
		else
			databaseType = this.database.getDatabaseType();

		if (databaseType == DatabaseType.MYSQL || databaseType == DatabaseType.MARIA_DB) {
			if (this.parsedSqlVariants.mysqlParseFailureMessage != null)
				throw new IllegalArgumentException(this.parsedSqlVariants.mysqlParseFailureMessage);

			return this.parsedSqlVariants.mysqlParsedSql;
		}

		if (databaseType == DatabaseType.DUCK_DB) {
			if (this.parsedSqlVariants.duckDbParseFailureMessage != null)
				throw new IllegalArgumentException(this.parsedSqlVariants.duckDbParseFailureMessage);

			return this.parsedSqlVariants.duckDbParsedSql;
		}

		if (databaseType == DatabaseType.SQL_SERVER || databaseType == DatabaseType.SQLITE) {
			if (this.parsedSqlVariants.bracketIdentifierParseFailureMessage != null)
				throw new IllegalArgumentException(this.parsedSqlVariants.bracketIdentifierParseFailureMessage);

			return this.parsedSqlVariants.bracketIdentifierParsedSql;
		}

		if (this.parsedSqlVariants.standardParseFailureMessage != null)
			throw new IllegalArgumentException(this.parsedSqlVariants.standardParseFailureMessage);

		return this.parsedSqlVariants.standardParsedSql;
	}

	private void validateBindingsForParsedSql(@NonNull Map<String, Object> bindings,
																				 @NonNull ParsedSql parsedSql) {
		requireNonNull(bindings);
		requireNonNull(parsedSql);

		for (String bindingName : bindings.keySet())
			if (!parsedSql.distinctParameterNames.contains(bindingName))
				throw new IllegalArgumentException(format(
						"Named parameter '%s' is not valid for the detected database SQL syntax. SQL: %s",
						bindingName, this.originalSql));
	}

	private void appendPlaceholders(@NonNull StringBuilder sql,
																	int count) {
		requireNonNull(sql);

		for (int i = 0; i < count; ++i) {
			if (i > 0)
				sql.append(", ");
			sql.append('?');
		}
	}

	private static final class PreparedQuery {
		@NonNull
		private final Statement statement;
		@NonNull
		private final Object @NonNull [] parameters;

		private PreparedQuery(@NonNull Statement statement,
													Object @NonNull [] parameters) {
			this.statement = requireNonNull(statement);
			this.parameters = requireNonNull(parameters);
		}
	}

}

