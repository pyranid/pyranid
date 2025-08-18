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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
public class StatementContext<T> {
	@Nonnull
	private final Statement statement;
	@Nonnull
	private final List<Object> parameters;
	@Nullable
	private final Class<T> resultSetRowType;
	@Nonnull
	private final DatabaseType databaseType;
	@Nonnull
	private final ZoneId timeZone;

	protected StatementContext(@Nonnull Builder builder) {
		requireNonNull(builder);

		this.statement = builder.statement;
		this.parameters = builder.parameters == null ? List.of() : Collections.unmodifiableList(builder.parameters);
		this.resultSetRowType = builder.resultSetRowType;
		this.databaseType = builder.databaseType;
		this.timeZone = builder.timeZone;
	}

	@Override
	public int hashCode() {
		return Objects.hash(getStatement(), getParameters(), getResultSetRowType(), getDatabaseType(), getTimeZone());
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
				&& Objects.equals(statementContext.getDatabaseType(), getDatabaseType())
				&& Objects.equals(statementContext.getTimeZone(), getTimeZone());
	}

	@Override
	public String toString() {
		List<String> components = new ArrayList<>(3);

		components.add(format("statement=%s", getStatement()));

		if (getParameters().size() > 0)
			components.add(format("parameters=%s", getParameters()));

		Class<T> resultSetRowType = getResultSetRowType().orElse(null);

		if (resultSetRowType != null)
			components.add(format("resultSetRowType=%s", resultSetRowType));

		components.add(format("databaseType=%s", getDatabaseType().name()));
		components.add(format("timeZone=%s", getTimeZone().getId()));

		return format("%s{%s}", getClass().getSimpleName(), components.stream().collect(Collectors.joining(", ")));
	}

	@Nonnull
	public Statement getStatement() {
		return this.statement;
	}

	@Nonnull
	public List<Object> getParameters() {
		return this.parameters;
	}

	@Nonnull
	public Optional<Class<T>> getResultSetRowType() {
		return Optional.ofNullable(this.resultSetRowType);
	}

	@Nonnull
	public DatabaseType getDatabaseType() {
		return this.databaseType;
	}

	@Nonnull
	public ZoneId getTimeZone() {
		return this.timeZone;
	}

	@Nonnull
	public static <T> Builder<T> with(@Nonnull Statement statement,
																		@Nonnull Database database) {
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
		@Nonnull
		private final Statement statement;
		@Nonnull
		private final DatabaseType databaseType;
		@Nonnull
		private final ZoneId timeZone;
		@Nullable
		private List<Object> parameters;
		@Nullable
		private Class<T> resultSetRowType;

		private Builder(@Nonnull Statement statement,
										@Nonnull Database database) {
			requireNonNull(statement);
			requireNonNull(database);

			this.statement = statement;
			this.databaseType = database.getDatabaseType();
			this.timeZone = database.getTimeZone();
		}

		@Nonnull
		public Builder parameters(@Nullable List<Object> parameters) {
			this.parameters = parameters;
			return this;
		}

		@Nonnull
		public Builder parameters(@Nullable Object... parameters) {
			this.parameters = parameters == null ? null : Arrays.asList(parameters);
			return this;
		}

		@Nonnull
		public Builder resultSetRowType(Class<T> resultSetRowType) {
			this.resultSetRowType = resultSetRowType;
			return this;
		}

		@Nonnull
		public StatementContext build() {
			return new StatementContext(this);
		}
	}
}