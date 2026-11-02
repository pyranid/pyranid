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
import org.sqlite.SQLiteConnectionConfig;
import org.sqlite.jdbc4.JDBC4Connection;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Properties;

/** A pool/tracing-style wrapper that keeps the borrowed JDBC handle chain wrapped. */
final class SqliteDateFormatFixtures {
	private SqliteDateFormatFixtures() {}

	static DataSource wrapped(DataSource source, AtomicInteger unwraps) {
		return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
				new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
					Object value = invoke(source, method, args);
					return value instanceof Connection connection ? connection(connection, unwraps) : value;
				});
	}

	static ResultSet borrowedResultSet(Connection connection) {
		Statement statement = (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(), new Class<?>[]{Statement.class},
				(proxy, method, args) -> {
					if (method.getName().equals("getConnection")) return connection;
					throw new AssertionError(method);
				});
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> {
					if (method.getName().equals("getStatement")) return statement;
					throw new AssertionError(method);
				});
	}

	static final class FailingConfigConnection extends JDBC4Connection {
		Throwable failure;

		FailingConfigConnection() throws SQLException {
			super("jdbc:sqlite::memory:", ":memory:", new Properties());
		}

		@Override
		public SQLiteConnectionConfig getConnectionConfig() {
			if (this.failure != null) return sneakyThrow(this.failure);
			return super.getConnectionConfig();
		}

		@SuppressWarnings("unchecked")
		private static <T, E extends Throwable> T sneakyThrow(Throwable failure) throws E {
			throw (E) failure;
		}
	}

	private static Connection connection(Connection nativeConnection, AtomicInteger unwraps) {
		return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
				new Class<?>[]{Connection.class}, (proxy, method, args) -> {
					if (method.getName().equals("isWrapperFor"))
						return ((Class<?>) args[0]).isInstance(nativeConnection) || nativeConnection.isWrapperFor((Class<?>) args[0]);
					if (method.getName().equals("unwrap")) {
						unwraps.incrementAndGet();
						Class<?> type = (Class<?>) args[0];
						return type.isInstance(nativeConnection) ? type.cast(nativeConnection) : nativeConnection.unwrap(type);
					}
					Object value = invoke(nativeConnection, method, args);
					return value instanceof Statement statement ? statement(statement, (Connection) proxy) : value;
				});
	}

	private static Statement statement(Statement nativeStatement, Connection connection) {
		Class<?> type = nativeStatement instanceof PreparedStatement ? PreparedStatement.class : Statement.class;
		return (Statement) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
			if (method.getName().equals("getConnection")) return connection;
			Object value = invoke(nativeStatement, method, args);
			return value instanceof ResultSet resultSet ? resultSet(resultSet, (Statement) proxy) : value;
		});
	}

	private static ResultSet resultSet(ResultSet nativeResultSet, Statement statement) {
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> method.getName().equals("getStatement") ? statement : invoke(nativeResultSet, method, args));
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try {
			return method.invoke(target, args);
		} catch (InvocationTargetException failure) {
			throw failure.getTargetException();
		}
	}
}
