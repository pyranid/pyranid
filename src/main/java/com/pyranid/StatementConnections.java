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
import java.util.function.Function;
import java.util.function.Supplier;
import javax.sql.DataSource;

/** Narrow connection and metadata services shared by statement leases. */
final class StatementConnections {
	private final DataSource dataSource;
	private final Supplier<Optional<Transaction>> currentTransaction;
	private final Consumer<Connection> closeConnection;
	private final ThreadLocal<Connection> metadataConnection;
	private final Function<Connection, DatabaseDialect> dialect;
	private final Supplier<DatabaseType> cachedType;

	StatementConnections(DataSource dataSource, Supplier<Optional<Transaction>> currentTransaction,
			Consumer<Connection> closeConnection, ThreadLocal<Connection> metadataConnection,
			Function<Connection, DatabaseDialect> dialect, Supplier<DatabaseType> cachedType) {
		this.dataSource = dataSource;
		this.currentTransaction = currentTransaction;
		this.closeConnection = closeConnection;
		this.metadataConnection = metadataConnection;
		this.dialect = dialect;
		this.cachedType = cachedType;
	}

	Optional<Transaction> currentTransaction() { return this.currentTransaction.get(); }
	Connection acquire() throws SQLException { return this.dataSource.getConnection(); }
	void release(Connection connection) { this.closeConnection.accept(connection); }
	DatabaseType cachedType() { return this.cachedType.get(); }
	DatabaseDialect dialect(Connection connection) { return this.dialect.apply(connection); }

	MetadataScope metadataScope(Connection connection) {
		Connection previous = this.metadataConnection.get();
		this.metadataConnection.set(connection);
		return new MetadataScope(previous);
	}

	final class MetadataScope implements AutoCloseable {
		private final Connection previous;
		private boolean closed;
		private MetadataScope(Connection previous) { this.previous = previous; }
		@Override public void close() {
			if (this.closed) return;
			this.closed = true;
			if (this.previous == null) metadataConnection.remove();
			else metadataConnection.set(this.previous);
		}
	}
}
