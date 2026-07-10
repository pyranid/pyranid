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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
		return guardedStatement(this.connection.createStatement(), Statement.class);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		assertUsable();
		return guardedStatement(this.connection.prepareStatement(sql), PreparedStatement.class);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		assertUsable();
		return guardedStatement(this.connection.prepareCall(sql), CallableStatement.class);
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
		return guardedDatabaseMetaData(this.connection.getMetaData());
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
	public void setCatalog(String catalog) {
		throw guardedOperation("setCatalog");
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
		return guardedStatement(this.connection.createStatement(resultSetType, resultSetConcurrency), Statement.class);
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
																						int resultSetType,
																						int resultSetConcurrency) throws SQLException {
		assertUsable();
		return guardedStatement(this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency), PreparedStatement.class);
	}

	@Override
	public CallableStatement prepareCall(String sql,
																			 int resultSetType,
																			 int resultSetConcurrency) throws SQLException {
		assertUsable();
		return guardedStatement(this.connection.prepareCall(sql, resultSetType, resultSetConcurrency), CallableStatement.class);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		assertUsable();
		return this.connection.getTypeMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) {
		throw guardedOperation("setTypeMap");
	}

	@Override
	public void setHoldability(int holdability) {
		throw guardedOperation("setHoldability");
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
		return guardedStatement(this.connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), Statement.class);
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
																						int resultSetType,
																						int resultSetConcurrency,
																						int resultSetHoldability) throws SQLException {
		assertUsable();
		return guardedStatement(this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability), PreparedStatement.class);
	}

	@Override
	public CallableStatement prepareCall(String sql,
																			 int resultSetType,
																			 int resultSetConcurrency,
																			 int resultSetHoldability) throws SQLException {
		assertUsable();
		return guardedStatement(this.connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability), CallableStatement.class);
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
																						int autoGeneratedKeys) throws SQLException {
		assertUsable();
		return guardedStatement(this.connection.prepareStatement(sql, autoGeneratedKeys), PreparedStatement.class);
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
																						int[] columnIndexes) throws SQLException {
		assertUsable();
		return guardedStatement(this.connection.prepareStatement(sql, columnIndexes), PreparedStatement.class);
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
																						String[] columnNames) throws SQLException {
		assertUsable();
		return guardedStatement(this.connection.prepareStatement(sql, columnNames), PreparedStatement.class);
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
														String value) {
		throw guardedOperation("setClientInfo");
	}

	@Override
	public void setClientInfo(Properties properties) {
		throw guardedOperation("setClientInfo");
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
	public void setSchema(String schema) {
		throw guardedOperation("setSchema");
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
																int milliseconds) {
		throw guardedOperation("setNetworkTimeout");
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		assertUsable();
		return this.connection.getNetworkTimeout();
	}

	@Override
	public void beginRequest() {
		throw guardedOperation("beginRequest");
	}

	@Override
	public void endRequest() {
		throw guardedOperation("endRequest");
	}

	@Override
	public boolean setShardingKeyIfValid(ShardingKey shardingKey,
																			 ShardingKey superShardingKey,
																			 int timeout) {
		throw guardedOperation("setShardingKeyIfValid");
	}

	@Override
	public boolean setShardingKeyIfValid(ShardingKey shardingKey,
																			 int timeout) {
		throw guardedOperation("setShardingKeyIfValid");
	}

	@Override
	public void setShardingKey(ShardingKey shardingKey,
														 ShardingKey superShardingKey) {
		throw guardedOperation("setShardingKey");
	}

	@Override
	public void setShardingKey(ShardingKey shardingKey) {
		throw guardedOperation("setShardingKey");
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

		Object unwrapped = iface.isInstance(this.connection)
				? this.connection
				: this.connection.unwrap(iface);

		if (!(unwrapped instanceof Connection))
			return iface.cast(unwrapped);

		if (!iface.isInterface())
			throw new SQLException(format(
					"Cannot safely unwrap Pyranid-managed Connection to concrete type %s", iface.getName()));

		return guardedVendorConnectionInterface(unwrapped, iface);
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

	@NonNull
	private <T> T guardedStatement(@NonNull T statement,
																 @NonNull Class<T> statementInterface) {
		requireNonNull(statement);
		requireNonNull(statementInterface);

		return statementInterface.cast(Proxy.newProxyInstance(
				statementInterface.getClassLoader(),
				new Class<?>[]{statementInterface},
				(proxy, method, args) -> invokeGuardedJdbcObject(proxy, statement, method, args)));
	}

	@NonNull
	private <T> T guardedVendorConnectionInterface(@NonNull Object target,
																						 @NonNull Class<T> vendorInterface) {
		requireNonNull(target);
		requireNonNull(vendorInterface);

		Object proxy = Proxy.newProxyInstance(
				vendorInterface.getClassLoader(),
				new Class<?>[]{vendorInterface},
				(proxyInstance, method, args) -> invokeGuardedVendorConnection(
						proxyInstance, target, method, args));
		return vendorInterface.cast(proxy);
	}

	private Object invokeGuardedVendorConnection(@NonNull Object proxy,
																				@NonNull Object target,
																				@NonNull Method method,
																				Object[] args) throws Throwable {
		requireNonNull(proxy);
		requireNonNull(target);
		requireNonNull(method);

		if (method.getDeclaringClass() == Object.class) {
			return switch (method.getName()) {
				case "equals" -> proxy == (args == null ? null : args[0]);
				case "hashCode" -> System.identityHashCode(proxy);
				case "toString" -> format("Pyranid-managed %s", target);
				default -> invoke(method, target, args);
			};
		}

		assertUsable();

		if ("unwrap".equals(method.getName()) && args != null && args.length == 1 && args[0] instanceof Class<?> iface) {
			if (iface.isInstance(proxy))
				return iface.cast(proxy);

			throw new SQLException(format(
					"Cannot unwrap Pyranid-managed vendor connection to %s because that could bypass Pyranid connection lifecycle management",
					iface.getName()));
		}

		if ("isWrapperFor".equals(method.getName()) && args != null && args.length == 1 && args[0] instanceof Class<?> iface)
			return iface.isInstance(proxy);

		if (isGuardedConnectionOperation(method.getName()))
			throw guardedOperation(method.getName());

		Object result = invoke(method, target, args);

		if (result == target && method.getReturnType().isInstance(proxy))
			return proxy;

		if (result instanceof Connection) {
			if (method.getReturnType().isInstance(this))
				return this;

			throw new SQLException(format(
					"Vendor connection method %s returned a Connection type that cannot be exposed safely",
					method.getName()));
		}

		return result;
	}

	private boolean isGuardedConnectionOperation(@NonNull String methodName) {
		requireNonNull(methodName);

		return switch (methodName) {
			case "setAutoCommit", "commit", "rollback", "close", "setReadOnly", "setCatalog",
					"setTransactionIsolation", "setTypeMap", "setHoldability", "setSavepoint",
					"releaseSavepoint", "setClientInfo", "setSchema", "abort", "setNetworkTimeout",
					"beginRequest", "endRequest", "setShardingKeyIfValid", "setShardingKey" -> true;
			default -> false;
		};
	}

	@NonNull
	private DatabaseMetaData guardedDatabaseMetaData(@NonNull DatabaseMetaData databaseMetaData) {
		requireNonNull(databaseMetaData);

		return (DatabaseMetaData) Proxy.newProxyInstance(
				DatabaseMetaData.class.getClassLoader(),
				new Class<?>[]{DatabaseMetaData.class},
				(proxy, method, args) -> invokeGuardedJdbcObject(proxy, databaseMetaData, method, args));
	}

	private Object invokeGuardedJdbcObject(@NonNull Object proxy,
																				 @NonNull Object target,
																				 @NonNull Method method,
																				 Object[] args) throws Throwable {
		requireNonNull(proxy);
		requireNonNull(target);
		requireNonNull(method);

		if (method.getDeclaringClass() == Object.class) {
			return switch (method.getName()) {
				case "equals" -> proxy == (args == null ? null : args[0]);
				case "hashCode" -> System.identityHashCode(proxy);
				case "toString" -> format("Pyranid-managed %s", target);
				default -> invoke(method, target, args);
			};
		}

		assertUsable();

		if ("getConnection".equals(method.getName()) && method.getParameterCount() == 0
				&& Connection.class.isAssignableFrom(method.getReturnType()))
			return this;

		if ("unwrap".equals(method.getName()) && args != null && args.length == 1 && args[0] instanceof Class<?> iface) {
			if (iface.isInstance(proxy))
				return iface.cast(proxy);

			throw new SQLException(format(
					"Cannot unwrap Pyranid-managed JDBC object to %s because that could bypass Pyranid connection lifecycle management",
					iface.getName()));
		}

		if ("isWrapperFor".equals(method.getName()) && args != null && args.length == 1 && args[0] instanceof Class<?> iface)
			return iface.isInstance(proxy);

		Object result = invoke(method, target, args);

		if (result instanceof ResultSet resultSet)
			return guardedResultSet(resultSet, proxy instanceof Statement statement ? statement : null);

		return result;
	}

	@NonNull
	private ResultSet guardedResultSet(@NonNull ResultSet resultSet,
																		 Statement statement) {
		requireNonNull(resultSet);

		return (ResultSet) Proxy.newProxyInstance(
				ResultSet.class.getClassLoader(),
				new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> invokeGuardedResultSet(proxy, resultSet, statement, method, args));
	}

	private Object invokeGuardedResultSet(@NonNull Object proxy,
																				@NonNull ResultSet resultSet,
																				Statement statement,
																				@NonNull Method method,
																				Object[] args) throws Throwable {
		requireNonNull(proxy);
		requireNonNull(resultSet);
		requireNonNull(method);

		if (method.getDeclaringClass() == Object.class) {
			return switch (method.getName()) {
				case "equals" -> proxy == (args == null ? null : args[0]);
				case "hashCode" -> System.identityHashCode(proxy);
				case "toString" -> format("Pyranid-managed %s", resultSet);
				default -> invoke(method, resultSet, args);
			};
		}

		assertUsable();

		if ("getStatement".equals(method.getName()) && method.getParameterCount() == 0) {
			if (statement != null)
				return statement;

			Statement resultSetStatement = (Statement) invoke(method, resultSet, args);
			return resultSetStatement == null ? null : guardedStatement(resultSetStatement, Statement.class);
		}

		if ("unwrap".equals(method.getName()) && args != null && args.length == 1 && args[0] instanceof Class<?> iface) {
			if (iface.isInstance(proxy))
				return iface.cast(proxy);

			throw new SQLException(format(
					"Cannot unwrap Pyranid-managed JDBC object to %s because that could bypass Pyranid connection lifecycle management",
					iface.getName()));
		}

		if ("isWrapperFor".equals(method.getName()) && args != null && args.length == 1 && args[0] instanceof Class<?> iface)
			return iface.isInstance(proxy);

		return invoke(method, resultSet, args);
	}

	private Object invoke(@NonNull Method method,
												@NonNull Object target,
												Object[] args) throws Throwable {
		requireNonNull(method);
		requireNonNull(target);

		try {
			return method.invoke(target, args);
		} catch (InvocationTargetException e) {
			throw e.getCause();
		}
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
