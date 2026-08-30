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

package com.pyranid.testdriver.jdbc;

/**
 * Stands in for a PostgreSQL JDBC driver fork's connection class, mirroring the
 * {@code <root>.jdbc.PgConnection} shape that pgjdbc and its forks share (YugabyteDB's smart driver ships
 * {@code com.yugabyte.jdbc.PgConnection}). Only its package matters: Pyranid resolves the driver's
 * {@code PGobject} by walking up from this class's package, which finds
 * {@code com.pyranid.testdriver.util.PGobject}. Every method is unsupported.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
public class ForkConnection implements java.sql.Connection {
	public boolean isClosed() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void rollback() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void rollback(java.sql.Savepoint a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void setTransactionIsolation(int a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public int getTransactionIsolation() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.DatabaseMetaData getMetaData() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.Statement createStatement() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.Statement createStatement(int a0, int a1, int a2) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.Statement createStatement(int a0, int a1) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.PreparedStatement prepareStatement(java.lang.String a0, int a1, int a2) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.PreparedStatement prepareStatement(java.lang.String a0, java.lang.String[] a1) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.PreparedStatement prepareStatement(java.lang.String a0, int[] a1) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.PreparedStatement prepareStatement(java.lang.String a0, int a1, int a2, int a3) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.PreparedStatement prepareStatement(java.lang.String a0, int a1) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.PreparedStatement prepareStatement(java.lang.String a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.CallableStatement prepareCall(java.lang.String a0, int a1, int a2, int a3) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.CallableStatement prepareCall(java.lang.String a0, int a1, int a2) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.CallableStatement prepareCall(java.lang.String a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.lang.String nativeSQL(java.lang.String a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void setAutoCommit(boolean a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public boolean getAutoCommit() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void setCatalog(java.lang.String a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.lang.String getCatalog() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.SQLWarning getWarnings() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void clearWarnings() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.util.Map<java.lang.String, java.lang.Class<?>> getTypeMap() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void setTypeMap(java.util.Map<java.lang.String, java.lang.Class<?>> a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void setHoldability(int a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public int getHoldability() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.Savepoint setSavepoint() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.Savepoint setSavepoint(java.lang.String a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void releaseSavepoint(java.sql.Savepoint a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.Clob createClob() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.Blob createBlob() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.NClob createNClob() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.SQLXML createSQLXML() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void setClientInfo(java.lang.String a0, java.lang.String a1) throws java.sql.SQLClientInfoException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void setClientInfo(java.util.Properties a0) throws java.sql.SQLClientInfoException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.util.Properties getClientInfo() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.lang.String getClientInfo(java.lang.String a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.Array createArrayOf(java.lang.String a0, java.lang.Object[] a1) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.sql.Struct createStruct(java.lang.String a0, java.lang.Object[] a1) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void setSchema(java.lang.String a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public java.lang.String getSchema() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void setNetworkTimeout(java.util.concurrent.Executor a0, int a1) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public int getNetworkTimeout() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void commit() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void close() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void setReadOnly(boolean a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public boolean isReadOnly() throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public boolean isValid(int a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public void abort(java.util.concurrent.Executor a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public boolean isWrapperFor(java.lang.Class<?> a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

	public <T> T unwrap(java.lang.Class<T> a0) throws java.sql.SQLException {
		throw new UnsupportedOperationException("fixture connection");
	}

}
