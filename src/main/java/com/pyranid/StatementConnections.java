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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.sql.DataSource;

/** Narrow connection and configured dialect services shared by statement leases. */
final class StatementConnections {
	private final DataSource dataSource;
	private final Supplier<Optional<Transaction>> currentTransaction;
	private final Consumer<Connection> closeConnection;
	private final DatabaseType databaseType;
	private final DatabaseDialect dialect;

	StatementConnections(DataSource dataSource, Supplier<Optional<Transaction>> currentTransaction,
			Consumer<Connection> closeConnection, DatabaseType databaseType, DatabaseDialect dialect) {
		this.dataSource = dataSource;
		this.currentTransaction = currentTransaction;
		this.closeConnection = closeConnection;
		this.dialect = dialect;
		this.databaseType = databaseType;
	}

	Optional<Transaction> currentTransaction() { return this.currentTransaction.get(); }
	Connection acquire() throws SQLException { return this.dataSource.getConnection(); }
	void release(Connection connection) { this.closeConnection.accept(connection); }
	DatabaseType databaseType() { return this.databaseType; }
	DatabaseDialect dialect() { return this.dialect; }
}
