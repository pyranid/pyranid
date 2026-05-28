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

import javax.annotation.concurrent.ThreadSafe;

import static java.util.Objects.requireNonNull;

/**
 * Statement row-count information observed by Pyranid.
 * <p>
 * {@code rowsReturned} counts rows materialized through a JDBC {@link java.sql.ResultSet}. {@code rowsAffected} counts
 * rows changed by a JDBC update-count operation. Some statements have neither value, and some database features can
 * logically have both even if Pyranid can only observe one of them through the current JDBC execution path.
 *
 * @param rowsReturned rows returned through a result set, or {@code null} if unavailable
 * @param rowsAffected rows affected by a DML/update operation, or {@code null} if unavailable
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
@ThreadSafe
public record StatementResult(@Nullable Long rowsReturned,
															@Nullable Long rowsAffected) {
	@NonNull
	private static final StatementResult EMPTY;

	static {
		EMPTY = new StatementResult(null, null);
	}

	public StatementResult {
		if (rowsReturned != null && rowsReturned < 0L)
			throw new IllegalArgumentException("rowsReturned must be nonnegative or null, got: " + rowsReturned);

		if (rowsAffected != null && rowsAffected < 0L)
			throw new IllegalArgumentException("rowsAffected must be nonnegative or null, got: " + rowsAffected);
	}

	/**
	 * A singleton result for statements with no row-count information.
	 *
	 * @return an empty result
	 */
	@NonNull
	public static StatementResult empty() {
		return EMPTY;
	}

	/**
	 * Creates a result with returned-row information.
	 *
	 * @param rowsReturned returned-row count
	 * @return a statement result
	 */
	@NonNull
	public static StatementResult ofRowsReturned(@NonNull Long rowsReturned) {
		requireNonNull(rowsReturned);
		return new StatementResult(rowsReturned, null);
	}

	/**
	 * Creates a result with affected-row information.
	 *
	 * @param rowsAffected affected-row count
	 * @return a statement result
	 */
	@NonNull
	public static StatementResult ofRowsAffected(@NonNull Long rowsAffected) {
		requireNonNull(rowsAffected);
		return new StatementResult(null, rowsAffected);
	}
}
