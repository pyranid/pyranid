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

/**
 * Identifies different types of databases, which allows for special platform-specific handling.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
public enum DatabaseType {
	/**
	 * A database which requires no special handling.
	 */
	GENERIC,
	/**
	 * A PostgreSQL database.
	 */
	POSTGRESQL,
	/**
	 * An Oracle database.
	 */
	ORACLE,
	/**
	 * A MySQL database.
	 *
	 * @since 4.3.0
	 */
	MYSQL,
	/**
	 * A MariaDB database.
	 *
	 * @since 4.3.0
	 */
	MARIA_DB,
	/**
	 * A SQLite database.
	 *
	 * @since 4.3.0
	 */
	SQLITE,
	/**
	 * A Microsoft SQL Server database.
	 *
	 * @since 4.3.0
	 */
	SQL_SERVER,
	/**
	 * A DuckDB database.
	 *
	 * @since 4.7.0
	 */
	DUCK_DB;

	@NonNull
	DatabaseDialect dialect() {
		return switch (this) {
			case POSTGRESQL -> PostgresDialect.INSTANCE;
			case ORACLE -> OracleDialect.INSTANCE;
			case MYSQL -> MySqlDialect.INSTANCE;
			case MARIA_DB -> MariaDbDialect.INSTANCE;
			case SQLITE -> SqliteDialect.INSTANCE;
			case SQL_SERVER -> SqlServerDialect.INSTANCE;
			case DUCK_DB -> DuckDbDialect.INSTANCE;
			case GENERIC -> GenericDialect.INSTANCE;
		};
	}

}
