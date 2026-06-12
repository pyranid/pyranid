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

import org.hsqldb.jdbc.JDBCDataSource;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.1.0
 */
public class ParameterMetadataReductionTests {
	@Test
	public void testScalarBindsDoNotCallParameterMetaData() {
		AtomicInteger parameterMetaDataAccessCount = new AtomicInteger();
		Database db = Database.withDataSource(new ParameterMetadataCountingDataSource(
				createInMemoryDataSource("scalar_binds_no_parameter_metadata"), parameterMetaDataAccessCount)).build();

		db.query("CREATE TABLE t (id INT, name VARCHAR(100), active BOOLEAN)").execute();
		parameterMetaDataAccessCount.set(0);

		db.query("INSERT INTO t(id, name, active) VALUES (:id, :name, :active)")
				.bind("id", 1)
				.bind("name", "Ada")
				.bind("active", true)
				.execute();

		Assertions.assertEquals(0, parameterMetaDataAccessCount.get(),
				"Ordinary scalar binds should not inspect JDBC parameter metadata");
	}

	@Test
	public void testInstantBindCallsParameterMetaDataForTimestampTargetDetection() {
		AtomicInteger parameterMetaDataAccessCount = new AtomicInteger();
		Database db = Database.withDataSource(new ParameterMetadataCountingDataSource(
				createInMemoryDataSource("instant_bind_parameter_metadata"), parameterMetaDataAccessCount)).build();

		db.query("CREATE TABLE t (created_at TIMESTAMP)").execute();
		parameterMetaDataAccessCount.set(0);

		db.query("INSERT INTO t(created_at) VALUES (:createdAt)")
				.bind("createdAt", Instant.parse("2020-01-02T03:04:05Z"))
				.execute();

		Assertions.assertTrue(parameterMetaDataAccessCount.get() >= 1,
				"Instant binds should inspect parameter metadata to distinguish TIMESTAMP targets");
	}

	@Test
	public void testNullBindStillCallsParameterMetaDataForSqlTypeHint() {
		AtomicInteger parameterMetaDataAccessCount = new AtomicInteger();
		Database db = Database.withDataSource(new ParameterMetadataCountingDataSource(
				createInMemoryDataSource("null_bind_parameter_metadata"), parameterMetaDataAccessCount)).build();

		db.query("CREATE TABLE t (id INT)").execute();
		parameterMetaDataAccessCount.set(0);

		db.query("INSERT INTO t(id) VALUES (:id)")
				.bind("id", null)
				.execute();

		Assertions.assertTrue(parameterMetaDataAccessCount.get() >= 1,
				"Null binds should still inspect parameter metadata when available");
	}

	@Test
	public void testParameterMetaDataIsFetchedAtMostOncePerStatementExecution() {
		AtomicInteger parameterMetaDataAccessCount = new AtomicInteger();
		Database db = Database.withDataSource(new ParameterMetadataCountingDataSource(
				createInMemoryDataSource("parameter_metadata_once_per_execution"), parameterMetaDataAccessCount)).build();

		db.query("CREATE TABLE t (id INT, created_at TIMESTAMP, updated_at TIMESTAMP)").execute();
		parameterMetaDataAccessCount.set(0);

		db.query("INSERT INTO t(id, created_at, updated_at) VALUES (:id, :createdAt, :updatedAt)")
				.bind("id", null)
				.bind("createdAt", Instant.parse("2020-01-02T03:04:05Z"))
				.bind("updatedAt", Instant.parse("2020-01-02T03:04:06Z"))
				.execute();

		Assertions.assertEquals(1, parameterMetaDataAccessCount.get(),
				"Pyranid should reuse one ParameterMetaData instance across all built-in binding paths for an execution");
	}

	@NonNull
	private static DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}

	private static final class ParameterMetadataCountingDataSource implements DataSource {
		@NonNull
		private final DataSource delegate;
		@NonNull
		private final AtomicInteger parameterMetaDataAccessCount;

		private ParameterMetadataCountingDataSource(@NonNull DataSource delegate,
																							 @NonNull AtomicInteger parameterMetaDataAccessCount) {
			this.delegate = requireNonNull(delegate);
			this.parameterMetaDataAccessCount = requireNonNull(parameterMetaDataAccessCount);
		}

		@Override
		public Connection getConnection() throws SQLException {
			return connectionThatCountsParameterMetaDataAccess(this.delegate.getConnection());
		}

		@Override
		public Connection getConnection(String username,
																		String password) throws SQLException {
			return connectionThatCountsParameterMetaDataAccess(this.delegate.getConnection(username, password));
		}

		@Override
		public PrintWriter getLogWriter() throws SQLException {
			return this.delegate.getLogWriter();
		}

		@Override
		public void setLogWriter(PrintWriter out) throws SQLException {
			this.delegate.setLogWriter(out);
		}

		@Override
		public void setLoginTimeout(int seconds) throws SQLException {
			this.delegate.setLoginTimeout(seconds);
		}

		@Override
		public int getLoginTimeout() throws SQLException {
			return this.delegate.getLoginTimeout();
		}

		@Override
		public Logger getParentLogger() {
			return Logger.getLogger(DataSource.class.getName());
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			return this.delegate.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return this.delegate.isWrapperFor(iface);
		}

		@NonNull
		private Connection connectionThatCountsParameterMetaDataAccess(@NonNull Connection connection) {
			requireNonNull(connection);

			return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> {
						if ("prepareStatement".equals(method.getName()) && method.getParameterCount() > 0) {
							try {
								PreparedStatement preparedStatement = (PreparedStatement) method.invoke(connection, args);
								return preparedStatementThatCountsParameterMetaDataAccess(preparedStatement);
							} catch (InvocationTargetException e) {
								throw e.getCause();
							}
						}

						try {
							return method.invoke(connection, args);
						} catch (InvocationTargetException e) {
							throw e.getCause();
						}
					});
		}

		@NonNull
		private PreparedStatement preparedStatementThatCountsParameterMetaDataAccess(@NonNull PreparedStatement preparedStatement) {
			requireNonNull(preparedStatement);

			return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
					new Class<?>[]{PreparedStatement.class},
					(proxy, method, args) -> {
						if ("getParameterMetaData".equals(method.getName()) && method.getParameterCount() == 0)
							this.parameterMetaDataAccessCount.incrementAndGet();

						try {
							return method.invoke(preparedStatement, args);
						} catch (InvocationTargetException e) {
							throw e.getCause();
						}
					});
		}
	}
}
