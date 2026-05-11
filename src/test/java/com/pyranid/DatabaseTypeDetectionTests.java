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
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.1.0
 */
public class DatabaseTypeDetectionTests {
	@Test
	public void testBuildDoesNotAcquireConnectionForAutomaticDatabaseTypeDetection() {
		DataSource dataSource = new ThrowingDataSource();

		Database db = Assertions.assertDoesNotThrow(() -> Database.withDataSource(dataSource).build());

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, db::getDatabaseType);
		Assertions.assertTrue(ex.getMessage().contains("databaseType"),
				"Expected automatic detection failure to tell callers how to configure databaseType explicitly");
	}

	@Test
	public void testExplicitDatabaseTypeSkipsAutomaticDetection() {
		DataSource dataSource = new ThrowingDataSource();

		Database db = Database.withDataSource(dataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.build();

		Assertions.assertEquals(DatabaseType.POSTGRESQL, db.getDatabaseType());
	}

	@Test
	public void testGenericStatementExecutionDoesNotForceDatabaseTypeDetection() {
		AtomicInteger metadataAccessCount = new AtomicInteger();
		DataSource dataSource = new MetadataThrowingDataSource(createInMemoryDataSource("generic_statement_no_detection"),
				metadataAccessCount);
		Database db = Database.withDataSource(dataSource).build();

		Assertions.assertDoesNotThrow(() -> db.query("CREATE TABLE t (id INT)").execute());
		Assertions.assertEquals(0, metadataAccessCount.get(), "Generic execution should not inspect database metadata");
	}

	@Test
	public void testGenericSelectWithScalarBindDoesNotForceDatabaseTypeDetection() {
		AtomicInteger metadataAccessCount = new AtomicInteger();
		DataSource dataSource = new MetadataThrowingDataSource(createInMemoryDataSource("select_bind_no_detection"),
				metadataAccessCount);
		Database db = Database.withDataSource(dataSource).build();

		Optional<Integer> value = Assertions.assertDoesNotThrow(() ->
				db.query("SELECT CAST(:value AS INTEGER) FROM (VALUES (0)) AS t(x)")
						.bind("value", 42)
						.fetchObject(Integer.class));

		Assertions.assertEquals(Optional.of(42), value);
		Assertions.assertEquals(0, metadataAccessCount.get(),
				"Scalar bind execution should not inspect database metadata");
	}

	@Test
	public void testConcurrentFirstUseProducesConsistentDatabaseType() throws Exception {
		AtomicInteger metadataAccessCount = new AtomicInteger();
		Database db = Database.withDataSource(new MetadataCountingDataSource(
				createInMemoryDataSource("concurrent_database_type_detection"), metadataAccessCount)).build();
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		CountDownLatch ready = new CountDownLatch(2);
		CountDownLatch start = new CountDownLatch(1);

		try {
			Future<DatabaseType> first = executorService.submit(() -> detectDatabaseTypeWhenReleased(db, ready, start));
			Future<DatabaseType> second = executorService.submit(() -> detectDatabaseTypeWhenReleased(db, ready, start));

			Assertions.assertTrue(ready.await(5, TimeUnit.SECONDS), "Detection workers did not start");
			start.countDown();

			Assertions.assertEquals(DatabaseType.GENERIC, first.get(5, TimeUnit.SECONDS));
			Assertions.assertEquals(DatabaseType.GENERIC, second.get(5, TimeUnit.SECONDS));
			Assertions.assertEquals(DatabaseType.GENERIC, db.getDatabaseType());
			Assertions.assertTrue(metadataAccessCount.get() >= 1,
					"Expected automatic detection to inspect database metadata");
		} finally {
			executorService.shutdownNow();
		}
	}

	@NonNull
	private DatabaseType detectDatabaseTypeWhenReleased(@NonNull Database db,
																										 @NonNull CountDownLatch ready,
																										 @NonNull CountDownLatch start) throws InterruptedException {
		requireNonNull(db);
		requireNonNull(ready);
		requireNonNull(start);

		ready.countDown();
		Assertions.assertTrue(start.await(5, TimeUnit.SECONDS), "Timed out waiting to start detection");
		return db.getDatabaseType();
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

	private static final class ThrowingDataSource implements DataSource {
		@Override
		public Connection getConnection() throws SQLException {
			throw new SQLException("connection should not be acquired");
		}

		@Override
		public Connection getConnection(String username,
																		String password) throws SQLException {
			throw new SQLException("connection should not be acquired");
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
		public Logger getParentLogger() {
			return Logger.getLogger(DataSource.class.getName());
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			throw new SQLException("Not a wrapper");
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) {
			return false;
		}
	}

	private static final class MetadataThrowingDataSource implements DataSource {
		@NonNull
		private final DataSource delegate;
		@NonNull
		private final AtomicInteger metadataAccessCount;

		private MetadataThrowingDataSource(@NonNull DataSource delegate,
																			 @NonNull AtomicInteger metadataAccessCount) {
			this.delegate = requireNonNull(delegate);
			this.metadataAccessCount = requireNonNull(metadataAccessCount);
		}

		@Override
		public Connection getConnection() throws SQLException {
			return connectionThatThrowsOnMetadataAccess(this.delegate.getConnection());
		}

		@Override
		public Connection getConnection(String username,
																		String password) throws SQLException {
			return connectionThatThrowsOnMetadataAccess(this.delegate.getConnection(username, password));
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
		private Connection connectionThatThrowsOnMetadataAccess(@NonNull Connection connection) {
			requireNonNull(connection);

			return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> {
						if ("getMetaData".equals(method.getName()) && method.getParameterCount() == 0) {
							this.metadataAccessCount.incrementAndGet();
							throw new SQLException("metadata should not be accessed");
						}

						try {
							return method.invoke(connection, args);
						} catch (InvocationTargetException e) {
							throw e.getCause();
						}
					});
		}
	}

	private static final class MetadataCountingDataSource implements DataSource {
		@NonNull
		private final DataSource delegate;
		@NonNull
		private final AtomicInteger metadataAccessCount;

		private MetadataCountingDataSource(@NonNull DataSource delegate,
																			 @NonNull AtomicInteger metadataAccessCount) {
			this.delegate = requireNonNull(delegate);
			this.metadataAccessCount = requireNonNull(metadataAccessCount);
		}

		@Override
		public Connection getConnection() throws SQLException {
			return connectionThatCountsMetadataAccess(this.delegate.getConnection());
		}

		@Override
		public Connection getConnection(String username,
																		String password) throws SQLException {
			return connectionThatCountsMetadataAccess(this.delegate.getConnection(username, password));
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
		private Connection connectionThatCountsMetadataAccess(@NonNull Connection connection) {
			requireNonNull(connection);

			return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> {
						if ("getMetaData".equals(method.getName()) && method.getParameterCount() == 0)
							this.metadataAccessCount.incrementAndGet();

						try {
							return method.invoke(connection, args);
						} catch (InvocationTargetException e) {
							throw e.getCause();
						}
					});
		}
	}
}
