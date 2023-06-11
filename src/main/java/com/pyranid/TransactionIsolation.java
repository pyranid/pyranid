/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2023 Revetware LLC.
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

import java.sql.Connection;
import java.util.Optional;

/**
 * Strategies for database locking during transactional operations.
 *
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
public enum TransactionIsolation {
	/**
	 * Default isolation (DBMS-specific).
	 */
	DEFAULT(Optional.empty()),

	/**
	 * Maps to JDBC value {@link Connection#TRANSACTION_READ_COMMITTED}.
	 */
	READ_COMMITTED(Optional.of(Connection.TRANSACTION_READ_COMMITTED)),

	/**
	 * Maps to JDBC value {@link Connection#TRANSACTION_READ_UNCOMMITTED}.
	 */
	READ_UNCOMMITTED(Optional.of(Connection.TRANSACTION_READ_UNCOMMITTED)),

	/**
	 * Maps to JDBC value {@link Connection#TRANSACTION_REPEATABLE_READ}.
	 */
	REPEATABLE_READ(Optional.of(Connection.TRANSACTION_REPEATABLE_READ)),

	/**
	 * Maps to JDBC value {@link Connection#TRANSACTION_SERIALIZABLE}.
	 */
	SERIALIZABLE(Optional.of(Connection.TRANSACTION_SERIALIZABLE));

	private final Optional<Integer> jdbcLevel;

	private TransactionIsolation(Optional<Integer> jdbcLevel) {
		this.jdbcLevel = jdbcLevel;
	}

	Optional<Integer> jdbcLevel() {
		return jdbcLevel;
	}
}