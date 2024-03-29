/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2024 Revetware LLC.
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

	protected StatementContext(@Nonnull Builder builder) {
		requireNonNull(builder);

		this.statement = builder.statement;
		this.parameters = builder.parameters == null ? List.of() : Collections.unmodifiableList(builder.parameters);
		this.resultSetRowType = builder.resultSetRowType;
	}

	@Override
	public int hashCode() {
		return Objects.hash(getStatement(), getParameters(), getResultSetRowType());
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
				&& Objects.equals(statementContext.getResultSetRowType(), getResultSetRowType());
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
		@Nullable
		private List<Object> parameters;
		@Nullable
		private Class<T> resultSetRowType;

		public Builder(@Nonnull Statement statement) {
			requireNonNull(statement);
			this.statement = statement;
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