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
import javax.annotation.concurrent.ThreadSafe;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Represents a SQL statement and an identifier for it.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 2.0.0
 */
@ThreadSafe
public final class Statement {
	@Nonnull
	private final Object id;
	@Nonnull
	private final String sql;

	private Statement(@Nonnull Object id,
										@Nonnull String sql) {
		requireNonNull(id);
		requireNonNull(sql);

		this.id = id;
		this.sql = sql;
	}

	/**
	 * Factory method for providing {@link Statement} instances.
	 *
	 * @param id  the statment's identifier
	 * @param sql the SQL being identified
	 * @return a statement instance
	 */
	@Nonnull
	public static Statement of(@Nonnull Object id,
														 @Nonnull String sql) {
		requireNonNull(id);
		requireNonNull(sql);

		return new Statement(id, sql);
	}

	@Override
	public int hashCode() {
		return Objects.hash(getId(), getSql());
	}

	@Override
	public boolean equals(Object object) {
		if (this == object)
			return true;

		if (!(object instanceof Statement))
			return false;

		Statement statement = (Statement) object;

		return Objects.equals(statement.getId(), getId())
				&& Objects.equals(statement.getSql(), getSql());
	}

	@Override
	@Nonnull
	public String toString() {
		// Strip out newlines for more compact SQL representation
		return format("%s{id=%s, sql=%s}", getClass().getSimpleName(),
				getId(), getSql().replaceAll("\n+", " ").trim());
	}

	@Nonnull
	public Object getId() {
		return this.id;
	}

	@Nonnull
	public String getSql() {
		return this.sql;
	}
}