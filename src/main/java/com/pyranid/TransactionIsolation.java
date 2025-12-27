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
import java.sql.Connection;
import java.util.Optional;

/**
 * Strategies for database locking during transactional operations.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
public enum TransactionIsolation {
	/**
	 * Default isolation (DBMS-specific).
	 */
	DEFAULT(null),

	/**
	 * Maps to JDBC value {@link Connection#TRANSACTION_READ_COMMITTED}.
	 */
	READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),

	/**
	 * Maps to JDBC value {@link Connection#TRANSACTION_READ_UNCOMMITTED}.
	 */
	READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),

	/**
	 * Maps to JDBC value {@link Connection#TRANSACTION_REPEATABLE_READ}.
	 */
	REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),

	/**
	 * Maps to JDBC value {@link Connection#TRANSACTION_SERIALIZABLE}.
	 */
	SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

	@Nullable
	private final Integer jdbcLevel;

	TransactionIsolation(@Nullable Integer jdbcLevel) {
		this.jdbcLevel = jdbcLevel;
	}

	@NonNull
	Optional<Integer> getJdbcLevel() {
		return Optional.ofNullable(this.jdbcLevel);
	}
}