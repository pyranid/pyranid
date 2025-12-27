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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A collection of SQL statement execution diagnostics.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public final class StatementLog<T> {
	@NonNull
	private final StatementContext<T> statementContext;
	@NonNull
	private final Duration totalDuration;
	@Nullable
	private final Duration connectionAcquisitionDuration;
	@Nullable
	private final Duration preparationDuration;
	@Nullable
	private final Duration executionDuration;
	@Nullable
	private final Duration resultSetMappingDuration;
	@Nullable
	private final Integer batchSize;
	@Nullable
	private final Exception exception;

	/**
	 * Creates a {@code StatementLog} for the given {@code builder}.
	 *
	 * @param builder the builder used to construct this {@code StatementLog}
	 */
	private StatementLog(@NonNull Builder builder) {
		requireNonNull(builder);

		this.statementContext = requireNonNull(builder.statementContext);
		this.connectionAcquisitionDuration = builder.connectionAcquisitionDuration;
		this.preparationDuration = builder.preparationDuration;
		this.executionDuration = builder.executionDuration;
		this.resultSetMappingDuration = builder.resultSetMappingDuration;
		this.batchSize = builder.batchSize;
		this.exception = builder.exception;

		Duration totalDuration = Duration.ZERO;

		if (this.connectionAcquisitionDuration != null)
			totalDuration = totalDuration.plus(this.connectionAcquisitionDuration);

		if (this.preparationDuration != null)
			totalDuration = totalDuration.plus(this.preparationDuration);

		if (this.executionDuration != null)
			totalDuration = totalDuration.plus(this.executionDuration);

		if (this.resultSetMappingDuration != null)
			totalDuration = totalDuration.plus(this.resultSetMappingDuration);

		this.totalDuration = totalDuration;
	}

	/**
	 * Creates a {@link StatementLog} builder for the given {@code statementContext}.
	 *
	 * @param statementContext current SQL context
	 * @return a {@link StatementLog} builder
	 */
	@NonNull
	public static <T> Builder withStatementContext(@NonNull StatementContext<T> statementContext) {
		requireNonNull(statementContext);
		return new Builder(statementContext);
	}

	@Override
	public String toString() {
		List<String> components = new ArrayList<>(8);

		components.add(format("statementContext=%s", getStatementContext()));
		components.add(format("totalDuration=%s", getTotalDuration()));

		Duration connectionAcquisitionDuration = getConnectionAcquisitionDuration().orElse(null);

		if (connectionAcquisitionDuration != null)
			components.add(format("connectionAcquisitionDuration=%s", connectionAcquisitionDuration));

		Duration preparationDuration = getPreparationDuration().orElse(null);

		if (preparationDuration != null)
			components.add(format("preparationDuration=%s", preparationDuration));

		Duration executionDuration = getExecutionDuration().orElse(null);

		if (executionDuration != null)
			components.add(format("executionDuration=%s", executionDuration));

		Duration resultSetMappingDuration = getResultSetMappingDuration().orElse(null);

		if (resultSetMappingDuration != null)
			components.add(format("resultSetMappingDuration=%s", resultSetMappingDuration));

		Integer batchSize = getBatchSize().orElse(null);

		if (batchSize != null)
			components.add(format("batchSize=%s", batchSize));

		Exception exception = getException().orElse(null);

		if (exception != null)
			components.add(format("exception=%s", exception));

		return format("%s{%s}", getClass().getSimpleName(), components.stream().collect(Collectors.joining(", ")));
	}

	@Override
	public boolean equals(Object object) {
		if (this == object)
			return true;

		if (!(object instanceof StatementLog))
			return false;

		StatementLog statementLog = (StatementLog) object;

		return Objects.equals(getStatementContext(), statementLog.getStatementContext())
				&& Objects.equals(getConnectionAcquisitionDuration(), statementLog.getConnectionAcquisitionDuration())
				&& Objects.equals(getPreparationDuration(), statementLog.getPreparationDuration())
				&& Objects.equals(getExecutionDuration(), statementLog.getExecutionDuration())
				&& Objects.equals(getResultSetMappingDuration(), statementLog.getResultSetMappingDuration())
				&& Objects.equals(getBatchSize(), statementLog.getBatchSize())
				&& Objects.equals(getException(), statementLog.getException());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getStatementContext(), getConnectionAcquisitionDuration(), getPreparationDuration(),
				getExecutionDuration(), getResultSetMappingDuration(), getBatchSize(), getException());
	}

	/**
	 * How long did it take to acquire a {@link java.sql.Connection} from the {@link javax.sql.DataSource}?
	 *
	 * @return how long it took to acquire a {@link java.sql.Connection}, if available
	 */
	@NonNull
	public Optional<Duration> getConnectionAcquisitionDuration() {
		return Optional.ofNullable(this.connectionAcquisitionDuration);
	}

	/**
	 * How long did it take to bind data to the {@link java.sql.PreparedStatement}?
	 *
	 * @return how long it took to bind data to the {@link java.sql.PreparedStatement}, if available
	 */
	@NonNull
	public Optional<Duration> getPreparationDuration() {
		return Optional.ofNullable(this.preparationDuration);
	}

	/**
	 * How long did it take to execute the SQL statement?
	 *
	 * @return how long it took to execute the SQL statement, if available
	 */
	public Optional<Duration> getExecutionDuration() {
		return Optional.ofNullable(this.executionDuration);
	}

	/**
	 * How long did it take to extract data from the {@link java.sql.ResultSet}?
	 *
	 * @return how long it took to extract data from the {@link java.sql.ResultSet}, if available
	 */
	public Optional<Duration> getResultSetMappingDuration() {
		return Optional.ofNullable(this.resultSetMappingDuration);
	}

	/**
	 * How long did it take to perform the database operation in total?
	 * <p>
	 * This is the sum of {@link #getConnectionAcquisitionDuration()} + {@link #getPreparationDuration()} +
	 * {@link #getExecutionDuration()} + {@link #getResultSetMappingDuration()}.
	 *
	 * @return how long the database operation took in total
	 */
	@NonNull
	public Duration getTotalDuration() {
		return this.totalDuration;
	}

	/**
	 * The SQL statement that was executed.
	 *
	 * @return the SQL statement that was executed.
	 */
	@NonNull
	public StatementContext<T> getStatementContext() {
		return this.statementContext;
	}

	/**
	 * The size of the batch operation.
	 *
	 * @return how many records were processed as part of the batch operation, if available
	 */
	@NonNull
	public Optional<Integer> getBatchSize() {
		return Optional.ofNullable(this.batchSize);
	}

	/**
	 * The exception that occurred during SQL statement execution.
	 *
	 * @return the exception that occurred during SQL statement execution, if available
	 */
	@NonNull
	public Optional<Exception> getException() {
		return Optional.ofNullable(this.exception);
	}

	/**
	 * Builder used to construct instances of {@link StatementLog}.
	 * <p>
	 * This class is intended for use by a single thread.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 1.0.0
	 */
	@NotThreadSafe
	public static class Builder<T> {
		@NonNull
		private final StatementContext<T> statementContext;
		@Nullable
		private Duration connectionAcquisitionDuration;
		@Nullable
		private Duration preparationDuration;
		@Nullable
		private Duration executionDuration;
		@Nullable
		private Duration resultSetMappingDuration;
		@Nullable
		private Integer batchSize;
		@Nullable
		private Exception exception;

		/**
		 * Creates a {@code Builder} for the given {@code statementContext}.
		 *
		 * @param statementContext current SQL context
		 */
		private Builder(@NonNull StatementContext<T> statementContext) {
			requireNonNull(statementContext);
			this.statementContext = statementContext;
		}

		/**
		 * Specifies how long it took to acquire a {@link java.sql.Connection} from the {@link javax.sql.DataSource}.
		 *
		 * @param connectionAcquisitionDuration how long it took to acquire a {@link java.sql.Connection}, if available
		 * @return this {@code Builder}, for chaining
		 */
		@NonNull
		public Builder connectionAcquisitionDuration(@Nullable Duration connectionAcquisitionDuration) {
			this.connectionAcquisitionDuration = connectionAcquisitionDuration;
			return this;
		}

		/**
		 * Specifies how long it took to bind data to a {@link java.sql.PreparedStatement}.
		 *
		 * @param preparationDuration how long it took to bind data to a {@link java.sql.PreparedStatement}, if available
		 * @return this {@code Builder}, for chaining
		 */
		public Builder preparationDuration(@Nullable Duration preparationDuration) {
			this.preparationDuration = preparationDuration;
			return this;
		}

		/**
		 * Specifies how long it took to execute a SQL statement.
		 *
		 * @param executionDuration how long it took to execute a SQL statement, if available
		 * @return this {@code Builder}, for chaining
		 */
		public Builder executionDuration(@Nullable Duration executionDuration) {
			this.executionDuration = executionDuration;
			return this;
		}

		/**
		 * Specifies how long it took to extract data from a {@link java.sql.ResultSet}.
		 *
		 * @param resultSetMappingDuration how long it took to extract data from a {@link java.sql.ResultSet}, if available
		 * @return this {@code Builder}, for chaining
		 */
		public Builder resultSetMappingDuration(@Nullable Duration resultSetMappingDuration) {
			this.resultSetMappingDuration = resultSetMappingDuration;
			return this;
		}

		/**
		 * Specifies the size of the batch operation.
		 *
		 * @param batchSize how many records were processed as part of the batch operation, if available
		 * @return this {@code Builder}, for chaining
		 */
		public Builder batchSize(@Nullable Integer batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		/**
		 * Specifies the exception that occurred during SQL statement execution.
		 *
		 * @param exception the exception that occurred during SQL statement execution, if available
		 * @return this {@code Builder}, for chaining
		 */
		public Builder exception(@Nullable Exception exception) {
			this.exception = exception;
			return this;
		}

		/**
		 * Constructs a {@code StatementLog} instance.
		 *
		 * @return a {@code StatementLog} instance
		 */
		@NonNull
		public StatementLog build() {
			return new StatementLog(this);
		}
	}
}