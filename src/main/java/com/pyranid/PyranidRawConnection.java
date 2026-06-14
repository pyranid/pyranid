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

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.ShardingKey;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Guarded raw JDBC connection handle exposed by {@link Database#useRawConnection(RawConnectionOperation)}.
 */
@NotThreadSafe
final class PyranidRawConnection implements Connection {
	@NonNull
	private final Connection connection;
	private volatile boolean released;

	PyranidRawConnection(@NonNull Connection connection) {
		this.connection = requireNonNull(connection);
	}

	void release() {
		this.released = true;
	}

	@Override
	public Statement createStatement() throws SQLException {
		assertUsable();
		return this.connection.createStatement();
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		assertUsable();
		return this.connection.prepareStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		assertUsable();
		return this.connection.prepareCall(sql);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		assertUsable();
		return this.connection.nativeSQL(sql);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) {
		throw guardedOperation("setAutoCommit");
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		assertUsable();
		return this.connection.getAutoCommit();
	}

	@Override
	public void commit() {
		throw guardedOperation("commit");
	}

	@Override
	public void rollback() {
		throw guardedOperation("rollback");
	}

	@Override
	public void close() {
		throw guardedOperation("close");
	}

	@Override
	public boolean isClosed() throws SQLException {
		assertUsable();
		return this.connection.isClosed();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		assertUsable();
		return this.connection.getMetaData();
	}

	@Override
	public void setReadOnly(boolean readOnly) {
		throw guardedOperation("setReadOnly");
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		assertUsable();
		return this.connection.isReadOnly();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		assertUsable();
		this.connection.setCatalog(catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		assertUsable();
		return this.connection.getCatalog();
	}

	@Override
	public void setTransactionIsolation(int level) {
		throw guardedOperation("setTransactionIsolation");
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		assertUsable();
		return this.connection.getTransactionIsolation();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		assertUsable();
		return this.connection.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		assertUsable();
		this.connection.clearWarnings();
	}

	@Override
	public Statement createStatement(int resultSetType,
																	 int resultSetConcurrency) throws SQLException {
		assertUsable();
		return this.connection.createStatement(resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
																						int resultSetType,
																						int resultSetConcurrency) throws SQLException {
		assertUsable();
		return this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public CallableStatement prepareCall(String sql,
																			 int resultSetType,
																			 int resultSetConcurrency) throws SQLException {
		assertUsable();
		return this.connection.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		assertUsable();
		return this.connection.getTypeMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		assertUsable();
		this.connection.setTypeMap(map);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		assertUsable();
		this.connection.setHoldability(holdability);
	}

	@Override
	public int getHoldability() throws SQLException {
		assertUsable();
		return this.connection.getHoldability();
	}

	@Override
	public Savepoint setSavepoint() {
		throw guardedOperation("setSavepoint");
	}

	@Override
	public Savepoint setSavepoint(String name) {
		throw guardedOperation("setSavepoint");
	}

	@Override
	public void rollback(Savepoint savepoint) {
		throw guardedOperation("rollback");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) {
		throw guardedOperation("releaseSavepoint");
	}

	@Override
	public Statement createStatement(int resultSetType,
																	 int resultSetConcurrency,
																	 int resultSetHoldability) throws SQLException {
		assertUsable();
		return this.connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
																						int resultSetType,
																						int resultSetConcurrency,
																						int resultSetHoldability) throws SQLException {
		assertUsable();
		return this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public CallableStatement prepareCall(String sql,
																			 int resultSetType,
																			 int resultSetConcurrency,
																			 int resultSetHoldability) throws SQLException {
		assertUsable();
		return this.connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
																						int autoGeneratedKeys) throws SQLException {
		assertUsable();
		return this.connection.prepareStatement(sql, autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
																						int[] columnIndexes) throws SQLException {
		assertUsable();
		return this.connection.prepareStatement(sql, columnIndexes);
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
																						String[] columnNames) throws SQLException {
		assertUsable();
		return this.connection.prepareStatement(sql, columnNames);
	}

	@Override
	public Clob createClob() throws SQLException {
		assertUsable();
		return this.connection.createClob();
	}

	@Override
	public Blob createBlob() throws SQLException {
		assertUsable();
		return this.connection.createBlob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		assertUsable();
		return this.connection.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		assertUsable();
		return this.connection.createSQLXML();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		assertUsable();
		return this.connection.isValid(timeout);
	}

	@Override
	public void setClientInfo(String name,
														String value) throws SQLClientInfoException {
		assertUsable();
		this.connection.setClientInfo(name, value);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		assertUsable();
		this.connection.setClientInfo(properties);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		assertUsable();
		return this.connection.getClientInfo(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		assertUsable();
		return this.connection.getClientInfo();
	}

	@Override
	public Array createArrayOf(String typeName,
														 Object[] elements) throws SQLException {
		assertUsable();
		return this.connection.createArrayOf(typeName, elements);
	}

	@Override
	public Struct createStruct(String typeName,
														 Object[] attributes) throws SQLException {
		assertUsable();
		return this.connection.createStruct(typeName, attributes);
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		assertUsable();
		this.connection.setSchema(schema);
	}

	@Override
	public String getSchema() throws SQLException {
		assertUsable();
		return this.connection.getSchema();
	}

	@Override
	public void abort(Executor executor) {
		throw guardedOperation("abort");
	}

	@Override
	public void setNetworkTimeout(Executor executor,
																int milliseconds) throws SQLException {
		assertUsable();
		this.connection.setNetworkTimeout(executor, milliseconds);
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		assertUsable();
		return this.connection.getNetworkTimeout();
	}

	@Override
	public void beginRequest() throws SQLException {
		assertUsable();
		this.connection.beginRequest();
	}

	@Override
	public void endRequest() throws SQLException {
		assertUsable();
		this.connection.endRequest();
	}

	@Override
	public boolean setShardingKeyIfValid(ShardingKey shardingKey,
																			 ShardingKey superShardingKey,
																			 int timeout) throws SQLException {
		assertUsable();
		return this.connection.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
	}

	@Override
	public boolean setShardingKeyIfValid(ShardingKey shardingKey,
																			 int timeout) throws SQLException {
		assertUsable();
		return this.connection.setShardingKeyIfValid(shardingKey, timeout);
	}

	@Override
	public void setShardingKey(ShardingKey shardingKey,
														 ShardingKey superShardingKey) throws SQLException {
		assertUsable();
		this.connection.setShardingKey(shardingKey, superShardingKey);
	}

	@Override
	public void setShardingKey(ShardingKey shardingKey) throws SQLException {
		assertUsable();
		this.connection.setShardingKey(shardingKey);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		assertUsable();

		if (iface == null)
			throw new SQLException("Expected a JDBC wrapper interface");

		if (iface.isInstance(this))
			return iface.cast(this);

		if (Connection.class.isAssignableFrom(iface))
			throw new SQLException(format(
					"Cannot unwrap Pyranid-managed Connection to %s because that would bypass Pyranid connection lifecycle management",
					iface.getName()));

		if (iface.isInstance(this.connection))
			return iface.cast(this.connection);

		return this.connection.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		assertUsable();

		if (iface == null)
			throw new SQLException("Expected a JDBC wrapper interface");

		if (iface.isInstance(this))
			return true;

		if (Connection.class.isAssignableFrom(iface))
			return false;

		return iface.isInstance(this.connection) || this.connection.isWrapperFor(iface);
	}

	@Override
	public String toString() {
		return "Pyranid-managed Connection";
	}

	private void assertUsable() {
		if (this.released)
			throw new IllegalStateException("Pyranid-managed Connection handle is no longer valid outside Database.useRawConnection(...) callback");
	}

	@NonNull
	private IllegalStateException guardedOperation(@NonNull String methodName) {
		requireNonNull(methodName);
		assertUsable();
		return new IllegalStateException(format(
				"Cannot call Connection.%s(...) on a Pyranid-managed Connection; use Pyranid transaction APIs instead",
				methodName));
	}
}
