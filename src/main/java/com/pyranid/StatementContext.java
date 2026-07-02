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
import javax.annotation.concurrent.ThreadSafe;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Data that represents a SQL statement.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 2.0.0
 */
@ThreadSafe
public final class StatementContext<T> {
	@NonNull
	private final Statement statement;
	@NonNull
	private final List<@Nullable Object> parameters;
	@Nullable
	private final Class<T> resultSetRowType;
	@NonNull
	private final Supplier<@NonNull DatabaseType> databaseTypeSupplier;
	@NonNull
	private final Supplier<@NonNull DatabaseDialect> databaseDialectSupplier;
	@Nullable
	private volatile DatabaseDialect databaseDialect;
	@NonNull
	private final Supplier<@NonNull DatabaseType> diagnosticDatabaseTypeSupplier;
	@NonNull
	private final ZoneId timeZone;
	@NonNull
	private final AmbiguousTimestampBindingStrategy ambiguousTimestampBindingStrategy;
	@NonNull
	private final ParameterRedactor parameterRedactor;
	private final boolean batchParameterGroups;
	@Nullable
	private final SpiOverrides spiOverrides;
	@NonNull
	private final Queue<@NonNull AutoCloseable> cleanupOperations;

	/**
	 * Per-query overrides of the {@link Database}-wide mapping/binding SPIs, carried on the context so
	 * execution internals can resolve them without additional plumbing. Deliberately excluded from
	 * {@link #equals(Object)}/{@link #hashCode()}/{@link #toString()} — overrides are functional
	 * identity, not value identity.
	 */
	record SpiOverrides(@Nullable ResultSetMapper resultSetMapper,
											@Nullable PreparedStatementBinder preparedStatementBinder) {}

	protected StatementContext(@NonNull Builder builder) {
		requireNonNull(builder);

		this.statement = builder.statement;
		this.parameters = builder.parameters == null
				? List.of()
				: Collections.unmodifiableList(new ArrayList<>(builder.parameters));
		this.resultSetRowType = builder.resultSetRowType;
		this.databaseTypeSupplier = builder.databaseTypeSupplier;
		this.databaseDialectSupplier = builder.databaseDialectSupplier;
		this.diagnosticDatabaseTypeSupplier = builder.diagnosticDatabaseTypeSupplier;
		this.timeZone = builder.timeZone;
		this.ambiguousTimestampBindingStrategy = builder.ambiguousTimestampBindingStrategy;
		this.parameterRedactor = builder.parameterRedactor;
		this.batchParameterGroups = builder.batchParameterGroups;
		this.spiOverrides = builder.spiOverrides;
		this.cleanupOperations = new ConcurrentLinkedQueue<>();
	}

	@Nullable
	ResultSetMapper getResultSetMapperOverride() {
		return this.spiOverrides == null ? null : this.spiOverrides.resultSetMapper();
	}

	@Nullable
	PreparedStatementBinder getPreparedStatementBinderOverride() {
		return this.spiOverrides == null ? null : this.spiOverrides.preparedStatementBinder();
	}

	@Override
	public int hashCode() {
		return Objects.hash(getStatement(), getParameters(), getResultSetRowType(), getDiagnosticDatabaseType(), getTimeZone(), getAmbiguousTimestampBindingStrategy());
	}

	@Override
	public boolean equals(Object object) {
		if (this == object)
			return true;

		if (!(object instanceof StatementContext))
			return false;

		StatementContext statementContext = (StatementContext) object;

		return Objects.equals(statementContext.getStatement(), getStatement())
				&& Objects.equals(statementContext.getParameters(), getParameters())
				&& Objects.equals(statementContext.getResultSetRowType(), getResultSetRowType())
				&& Objects.equals(statementContext.getDiagnosticDatabaseType(), getDiagnosticDatabaseType())
				&& Objects.equals(statementContext.getTimeZone(), getTimeZone())
				&& Objects.equals(statementContext.getAmbiguousTimestampBindingStrategy(), getAmbiguousTimestampBindingStrategy());
	}

	@Override
	public String toString() {
		List<String> components = new ArrayList<>(3);

		components.add(format("statement=%s", getStatement()));

		if (getParameters().size() > 0)
			components.add(format("parameters=%s", getRedactedParameters()));

		Class<T> resultSetRowType = getResultSetRowType().orElse(null);

		if (resultSetRowType != null)
			components.add(format("resultSetRowType=%s", resultSetRowType));

		components.add(format("databaseType=%s", getDiagnosticDatabaseType().name()));
		components.add(format("timeZone=%s", getTimeZone().getId()));
		components.add(format("ambiguousTimestampBindingStrategy=%s", getAmbiguousTimestampBindingStrategy().name()));

		return format("%s{%s}", getClass().getSimpleName(), components.stream().collect(Collectors.joining(", ")));
	}

	@NonNull
	public Statement getStatement() {
		return this.statement;
	}

	@NonNull
	public List<@Nullable Object> getParameters() {
		return this.parameters;
	}

	/**
	 * Gets this statement's parameters rendered for diagnostics.
	 * <p>
	 * {@link SecureParameter} values render as their masks. Other non-batch values are rendered through the configured
	 * {@link ParameterRedactor}. Batch executions render a bounded summary instead of individual group values.
	 *
	 * @return parameters rendered for diagnostics
	 * @since 4.4.0
	 */
	@NonNull
	public List<@Nullable Object> getRedactedParameters() {
		if (this.batchParameterGroups)
			return List.of(batchParameterSummary());

		List<@Nullable Object> redactedParameters = new ArrayList<>(getParameters().size());

		for (int i = 0; i < getParameters().size(); ++i) {
			Object parameter = getParameters().get(i);
			SecureParameter secureParameter = SecureParameterSupport.displaySecureParameter(parameter);

			if (secureParameter != null)
				redactedParameters.add(SecureParameterSupport.maskOf(secureParameter));
			else
				redactedParameters.add(this.parameterRedactor.redactParameter(this, i, parameter));
		}

		return Collections.unmodifiableList(redactedParameters);
	}

	@NonNull
	private String batchParameterSummary() {
		List<@Nullable Object> parameters = getParameters();
		int groupCount = parameters.size();

		if (groupCount == 0)
			return "<batch: 0 groups x 0 parameters>";

		Integer expectedParameterCount = null;
		boolean mixedParameterCounts = false;

		for (Object parameterGroup : parameters) {
			int parameterCount = parameterGroup instanceof List<?> list ? list.size() : 0;

			if (expectedParameterCount == null) {
				expectedParameterCount = parameterCount;
			} else if (expectedParameterCount != parameterCount) {
				mixedParameterCounts = true;
				break;
			}
		}

		if (mixedParameterCounts)
			return format("<batch: %s groups x mixed parameter counts>", groupCount);

		return format("<batch: %s groups x %s parameters>", groupCount, expectedParameterCount);
	}

	@NonNull
	public Optional<Class<T>> getResultSetRowType() {
		return Optional.ofNullable(this.resultSetRowType);
	}

	/**
	 * Gets the database type for this statement.
	 * <p>
	 * If automatic database type detection is enabled and the type has not already been detected, this method may acquire a
	 * connection and inspect {@link java.sql.DatabaseMetaData}. Diagnostic methods such as {@link #toString()},
	 * {@link #equals(Object)}, and {@link #hashCode()} use a non-detecting database-type value instead.
	 *
	 * @return the database type
	 * @throws DatabaseException if automatic database type detection fails
	 * @since 3.0.0
	 */
	@NonNull
	public DatabaseType getDatabaseType() {
		return this.databaseTypeSupplier.get();
	}

	@NonNull
	DatabaseDialect getDatabaseDialect() {
		DatabaseDialect cachedDatabaseDialect = this.databaseDialect;

		if (cachedDatabaseDialect != null)
			return cachedDatabaseDialect;

		DatabaseDialect databaseDialect = this.databaseDialectSupplier.get();
		this.databaseDialect = databaseDialect;
		return databaseDialect;
	}

	@NonNull
	private DatabaseType getDiagnosticDatabaseType() {
		return this.diagnosticDatabaseTypeSupplier.get();
	}

	@NonNull
	public ZoneId getTimeZone() {
		return this.timeZone;
	}

	/**
	 * How should Pyranid bind {@link java.time.Instant} and {@link java.time.OffsetDateTime} parameters when JDBC
	 * parameter metadata cannot identify whether the target is {@code TIMESTAMP} or {@code TIMESTAMP WITH TIME ZONE}?
	 *
	 * @return behavior to use when timestamp target metadata is unavailable or non-identifying
	 * @since 4.2.0
	 */
	@NonNull
	public AmbiguousTimestampBindingStrategy getAmbiguousTimestampBindingStrategy() {
		return this.ambiguousTimestampBindingStrategy;
	}

	void addCleanupOperation(@NonNull AutoCloseable cleanupOperation) {
		requireNonNull(cleanupOperation);
		this.cleanupOperations.add(cleanupOperation);
	}

	@NonNull
	Queue<@NonNull AutoCloseable> getCleanupOperations() {
		return this.cleanupOperations;
	}

	@NonNull
	public static <T> Builder<T> with(@NonNull Statement statement,
																		@NonNull Database database) {
		requireNonNull(statement);
		requireNonNull(database);

		return new Builder<>(statement, database);
	}

	/**
	 * Builder used to construct instances of {@link StatementContext}.
	 * <p>
	 * This class is intended for use by a single thread.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 2.0.0
	 */
	@NotThreadSafe
	public static class Builder<T> {
		@NonNull
		private final Statement statement;
		@NonNull
		private final Supplier<@NonNull DatabaseType> databaseTypeSupplier;
		@NonNull
		private final Supplier<@NonNull DatabaseDialect> databaseDialectSupplier;
		@NonNull
		private final Supplier<@NonNull DatabaseType> diagnosticDatabaseTypeSupplier;
		@NonNull
		private final ZoneId timeZone;
		@NonNull
		private final AmbiguousTimestampBindingStrategy ambiguousTimestampBindingStrategy;
		@NonNull
		private final ParameterRedactor parameterRedactor;
		@Nullable
		private List<@Nullable Object> parameters;
		@Nullable
		private Class<T> resultSetRowType;
		private boolean batchParameterGroups;
		@Nullable
		private SpiOverrides spiOverrides;

		private Builder(@NonNull Statement statement,
										@NonNull Database database) {
			requireNonNull(statement);
			requireNonNull(database);

			this.statement = statement;
			this.databaseTypeSupplier = database::getDatabaseType;
			this.databaseDialectSupplier = database::getDatabaseDialect;
			this.diagnosticDatabaseTypeSupplier = database::peekDatabaseType;
			this.timeZone = database.getTimeZone();
			this.ambiguousTimestampBindingStrategy = database.getAmbiguousTimestampBindingStrategy();
			this.parameterRedactor = database.getParameterRedactor();
		}

		@NonNull
		public Builder parameters(@Nullable List<@Nullable Object> parameters) {
			this.parameters = parameters;
			return this;
		}

		@NonNull
		public Builder parameters(Object @Nullable ... parameters) {
			this.parameters = parameters == null ? null : Arrays.asList(parameters);
			return this;
		}

		@NonNull
		public Builder resultSetRowType(Class<T> resultSetRowType) {
			this.resultSetRowType = resultSetRowType;
			return this;
		}

		@NonNull
		Builder batchParameterGroups(boolean batchParameterGroups) {
			this.batchParameterGroups = batchParameterGroups;
			return this;
		}

		@NonNull
		Builder spiOverrides(@Nullable SpiOverrides spiOverrides) {
			this.spiOverrides = spiOverrides;
			return this;
		}

		@NonNull
		public StatementContext build() {
			return new StatementContext<>(this);
		}
	}
}
