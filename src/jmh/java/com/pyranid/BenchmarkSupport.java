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

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Locale;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class BenchmarkSupport {
	private static final DataSource THROWING_DATA_SOURCE = new ThrowingDataSource();

	private BenchmarkSupport() {}

	static DataSource throwingDataSource() {
		return THROWING_DATA_SOURCE;
	}

	static ResultSet resultSet(String[] columnLabels,
														 int[] columnTypes,
														 String[] columnTypeNames,
														 Object[] values) {
		requireNonNull(columnLabels);
		requireNonNull(columnTypes);
		requireNonNull(columnTypeNames);
		requireNonNull(values);

		if (columnLabels.length != columnTypes.length
				|| columnLabels.length != columnTypeNames.length
				|| columnLabels.length != values.length) {
			throw new IllegalArgumentException("Column metadata and value arrays must have the same length");
		}

		ResultSetMetaData resultSetMetaData = resultSetMetaData(columnLabels, columnTypes, columnTypeNames);

		return (ResultSet) Proxy.newProxyInstance(
				ResultSet.class.getClassLoader(),
				new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "equals" -> proxy == args[0];
					case "hashCode" -> System.identityHashCode(proxy);
					case "toString" -> "BenchmarkResultSet";
					case "getMetaData" -> resultSetMetaData;
					case "findColumn" -> columnIndex(columnLabels, (String) args[0]) + 1;
					case "getObject" -> valueAt(columnLabels, values, args[0]);
					case "getString" -> stringValue(valueAt(columnLabels, values, args[0]));
					case "getInt" -> intValue(valueAt(columnLabels, values, args[0]));
					case "getLong" -> longValue(valueAt(columnLabels, values, args[0]));
					case "getBoolean" -> booleanValue(valueAt(columnLabels, values, args[0]));
					case "wasNull" -> false;
					default -> defaultValue(method.getReturnType());
				});
	}

	private static ResultSetMetaData resultSetMetaData(String[] columnLabels,
																										 int[] columnTypes,
																										 String[] columnTypeNames) {
		return (ResultSetMetaData) Proxy.newProxyInstance(
				ResultSetMetaData.class.getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "equals" -> proxy == args[0];
					case "hashCode" -> System.identityHashCode(proxy);
					case "toString" -> "BenchmarkResultSetMetaData";
					case "getColumnCount" -> columnLabels.length;
					case "getColumnLabel", "getColumnName" -> columnLabels[(Integer) args[0] - 1];
					case "getColumnType" -> columnTypes[(Integer) args[0] - 1];
					case "getColumnTypeName" -> columnTypeNames[(Integer) args[0] - 1];
					default -> defaultValue(method.getReturnType());
				});
	}

	private static Object valueAt(String[] columnLabels,
																Object[] values,
																Object columnIdentifier) throws SQLException {
		if (columnIdentifier instanceof Integer)
			return values[(Integer) columnIdentifier - 1];

		if (columnIdentifier instanceof String)
			return values[columnIndex(columnLabels, (String) columnIdentifier)];

		throw new SQLException(format("Unsupported column identifier %s", columnIdentifier));
	}

	private static int columnIndex(String[] columnLabels,
																 String columnLabel) throws SQLException {
		for (int i = 0; i < columnLabels.length; ++i) {
			if (columnLabels[i].equalsIgnoreCase(columnLabel))
				return i;

			if (columnLabels[i].replace("_", "").toLowerCase(Locale.ROOT).equals(
					columnLabel.replace("_", "").toLowerCase(Locale.ROOT))) {
				return i;
			}
		}

		throw new SQLException(format("Unknown column '%s'", columnLabel));
	}

	private static String stringValue(Object value) {
		return value == null ? null : value.toString();
	}

	private static Integer intValue(Object value) {
		return value instanceof Number ? ((Number) value).intValue() : 0;
	}

	private static Long longValue(Object value) {
		return value instanceof Number ? ((Number) value).longValue() : 0L;
	}

	private static Boolean booleanValue(Object value) {
		return value instanceof Boolean ? (Boolean) value : false;
	}

	private static Object defaultValue(Class<?> returnType) {
		if (!returnType.isPrimitive())
			return null;
		if (returnType == boolean.class)
			return false;
		if (returnType == byte.class)
			return (byte) 0;
		if (returnType == short.class)
			return (short) 0;
		if (returnType == int.class)
			return 0;
		if (returnType == long.class)
			return 0L;
		if (returnType == float.class)
			return 0.0f;
		if (returnType == double.class)
			return 0.0d;
		if (returnType == char.class)
			return '\0';
		return null;
	}

	private static final class ThrowingDataSource implements DataSource {
		@Override
		public Connection getConnection() throws SQLException {
			throw new SQLException("Benchmark fixture does not provide JDBC connections");
		}

		@Override
		public Connection getConnection(String username,
																		String password) throws SQLException {
			throw new SQLException("Benchmark fixture does not provide JDBC connections");
		}

		@Override
		public PrintWriter getLogWriter() {
			return null;
		}

		@Override
		public void setLogWriter(PrintWriter out) {}

		@Override
		public void setLoginTimeout(int seconds) {}

		@Override
		public int getLoginTimeout() {
			return 0;
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			throw new SQLFeatureNotSupportedException("Benchmark fixture does not provide a parent logger");
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			throw new SQLException(format("Benchmark fixture does not wrap %s", iface));
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) {
			return false;
		}
	}
}
