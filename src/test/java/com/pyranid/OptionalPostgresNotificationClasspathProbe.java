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
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Child-process probe whose constant pool deliberately contains no pgjdbc notification type.
 */
public final class OptionalPostgresNotificationClasspathProbe {
	private OptionalPostgresNotificationClasspathProbe() {}

	public static void main(String[] arguments) throws Exception {
		try {
			Class.forName("org.postgresql.PGConnection");
			throw new AssertionError("pgjdbc unexpectedly remained on the isolated classpath");
		} catch (ClassNotFoundException expected) {
			// Expected.
		}

		List<String> boundValues = new ArrayList<>();
		Database database = Database.withDataSource(dataSource(boundValues))
				.databaseType(DatabaseType.POSTGRESQL)
				.build();

		if (database.isNotificationListeningSupported())
			throw new AssertionError("Receive support must be false without pgjdbc");

		database.sendNotification("classpath_probe", "payload");

		if (!boundValues.equals(List.of("classpath_probe", "payload")))
			throw new AssertionError("Pure-SQL notification send did not bind expected values: " + boundValues);
	}

	private static DataSource dataSource(List<String> boundValues) {
		Connection connection = (Connection) Proxy.newProxyInstance(
				OptionalPostgresNotificationClasspathProbe.class.getClassLoader(),
				new Class<?>[]{Connection.class},
				(proxy, method, arguments) -> {
					if (method.getDeclaringClass() == Object.class)
						return objectMethod(proxy, method, arguments);

					return switch (method.getName()) {
						case "prepareStatement" -> preparedStatement(boundValues);
						case "close" -> null;
						case "isClosed" -> false;
						default -> defaultValue(method.getReturnType());
					};
				});

		return new DataSource() {
			@Override
			public Connection getConnection() {
				return connection;
			}

			@Override
			public Connection getConnection(String username, String password) {
				return connection;
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
				throw new SQLFeatureNotSupportedException();
			}

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				if (iface.isInstance(this))
					return iface.cast(this);

				throw new SQLException("No wrapper for " + iface);
			}

			@Override
			public boolean isWrapperFor(Class<?> iface) {
				return iface.isInstance(this);
			}
		};
	}

	private static PreparedStatement preparedStatement(List<String> boundValues) {
		return (PreparedStatement) Proxy.newProxyInstance(
				OptionalPostgresNotificationClasspathProbe.class.getClassLoader(),
				new Class<?>[]{PreparedStatement.class},
				(proxy, method, arguments) -> {
					if (method.getDeclaringClass() == Object.class)
						return objectMethod(proxy, method, arguments);

					return switch (method.getName()) {
						case "setString", "setObject" -> {
							boundValues.add((String) arguments[1]);
							yield null;
						}
						case "execute" -> false;
						case "getUpdateCount" -> -1;
						case "close" -> null;
						default -> defaultValue(method.getReturnType());
					};
				});
	}

	private static Object objectMethod(Object proxy, Method method, Object[] arguments) {
		return switch (method.getName()) {
			case "toString" -> "optionalPostgresNotificationProbe";
			case "hashCode" -> System.identityHashCode(proxy);
			case "equals" -> proxy == arguments[0];
			default -> throw new UnsupportedOperationException(method.getName());
		};
	}

	private static Object defaultValue(Class<?> returnType) {
		if (!returnType.isPrimitive() || returnType == void.class)
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
			return 0.0F;

		if (returnType == double.class)
			return 0.0D;

		if (returnType == char.class)
			return '\0';

		throw new IllegalArgumentException("Unsupported primitive type " + returnType);
	}
}
