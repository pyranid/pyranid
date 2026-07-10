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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.time.Duration;
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
	public void testDatabaseTypeDetectionRecognizesPostgreSqlSignatures() {
		assertDetectedDatabaseType(DatabaseType.POSTGRESQL, "PostgreSQL", null, null, null);
		assertDetectedDatabaseType(DatabaseType.POSTGRESQL, "Postgres", null, null, null);
		assertDetectedDatabaseType(DatabaseType.POSTGRESQL, "Unknown", null, "jdbc:postgresql://localhost/pyranid", null);
		assertDetectedDatabaseType(DatabaseType.POSTGRESQL, "Unknown", null, null, "PostgreSQL JDBC Driver");
	}

	@Test
	public void testDatabaseTypeDetectionRecognizesOracleSignatures() {
		assertDetectedDatabaseType(DatabaseType.ORACLE, "Oracle", null, null, null);
		assertDetectedDatabaseType(DatabaseType.ORACLE, "Unknown", null, "jdbc:oracle:thin:@localhost:1521/freepdb1", null);
		assertDetectedDatabaseType(DatabaseType.ORACLE, "Unknown", null, "jdbc:oracle:oci:@pyranid", null);
		assertDetectedDatabaseType(DatabaseType.ORACLE, "Unknown", null, null, "Oracle JDBC driver");
	}

	@Test
	public void testDatabaseTypeDetectionRecognizesMysqlAndMariadbSignatures() {
		assertDetectedDatabaseType(DatabaseType.MYSQL, "MySQL", "8.4.0", null, "MySQL Connector/J");
		assertDetectedDatabaseType(DatabaseType.MYSQL, "MySQL", null, null, "MySQL Connector/J", true);
		assertDetectedDatabaseType(DatabaseType.MYSQL, "Unknown", null, "jdbc:mysql://localhost/pyranid", null);
		assertDetectedDatabaseType(DatabaseType.MYSQL, "Unknown", null, "jdbc:mysql:loadbalance://localhost/pyranid", null);
		assertDetectedDatabaseType(DatabaseType.MYSQL, "Unknown", null, "jdbc:mysql+srv://example.com/pyranid", null);
		assertDetectedDatabaseType(DatabaseType.MYSQL, "Unknown", null, null, "MySQL Connector/J");
		assertDetectedDatabaseType(DatabaseType.MARIA_DB, "MariaDB", "11.4.3-MariaDB", null, null);
		assertDetectedDatabaseType(DatabaseType.MARIA_DB, "MySQL", "11.4.3-MariaDB", "jdbc:mysql://localhost/pyranid", "MySQL Connector/J");
		assertDetectedDatabaseType(DatabaseType.MARIA_DB, "Unknown", null, "jdbc:mariadb://localhost/pyranid", null);
		assertDetectedDatabaseType(DatabaseType.MARIA_DB, "Unknown", null, "jdbc:mysql://localhost/pyranid", "MariaDB Connector/J");
	}

	@Test
	public void testDatabaseTypeDetectionRecognizesSqliteSignatures() {
		assertDetectedDatabaseType(DatabaseType.SQLITE, "SQLite", null, null, null);
		assertDetectedDatabaseType(DatabaseType.SQLITE, "Unknown", null, "jdbc:sqlite:/tmp/pyranid.db", null);
		assertDetectedDatabaseType(DatabaseType.SQLITE, "Unknown", null, null, "SQLite JDBC");
	}

	@Test
	public void testDatabaseTypeDetectionRecognizesSqlServerSignatures() {
		assertDetectedDatabaseType(DatabaseType.SQL_SERVER, "Microsoft SQL Server", null, null, null);
		assertDetectedDatabaseType(DatabaseType.SQL_SERVER, "Unknown", null, "jdbc:sqlserver://localhost:1433", null);
		assertDetectedDatabaseType(DatabaseType.SQL_SERVER, "Unknown", null, "jdbc:jtds:sqlserver://localhost:1433/pyranid", null);
		assertDetectedDatabaseType(DatabaseType.SQL_SERVER, "Unknown", null, null, "Microsoft JDBC Driver 13.4 for SQL Server");
		assertDetectedDatabaseType(DatabaseType.SQL_SERVER, "Unknown", null, null, "jTDS Type 4 JDBC Driver for MS SQL Server");
	}

	@Test
	public void testDatabaseTypeDetectionFallsBackToGeneric() {
		assertDetectedDatabaseType(DatabaseType.GENERIC, "HSQL Database Engine", "2.7.4", "jdbc:hsqldb:mem:pyranid", "HSQL Database Engine Driver");
	}

	@Test
	public void testStatementContextDiagnosticsDoNotForceDatabaseTypeDetection() {
		DataSource dataSource = new ThrowingDataSource();
		Database db = Database.withDataSource(dataSource).build();
		Statement statement = Statement.of("diagnostic", "SELECT :value");
		StatementContext<Integer> statementContext = StatementContext.<Integer>with(statement, db)
				.parameters("secret")
				.resultSetRowType(Integer.class)
				.build();
		StatementContext<Integer> sameStatementContext = StatementContext.<Integer>with(statement, db)
				.parameters("secret")
				.resultSetRowType(Integer.class)
				.build();
		StatementLog<?> statementLog = StatementLog.withStatementContext(statementContext)
				.executionDuration(Duration.ofNanos(1))
				.build();

		Assertions.assertDoesNotThrow(() -> {
			statementContext.hashCode();
			statementContext.equals(sameStatementContext);
			statementContext.toString();
			statementLog.hashCode();
			statementLog.equals(StatementLog.withStatementContext(sameStatementContext)
					.executionDuration(Duration.ofNanos(1))
					.build());
			statementLog.toString();
		});
		Assertions.assertTrue(statementContext.toString().contains("databaseType=GENERIC"),
				"Cold database type diagnostics should use the non-I/O GENERIC sentinel");
		Assertions.assertThrows(DatabaseException.class, statementContext::getDatabaseType);
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
	public void testFirstStatementPreparationFailureUsesBorrowedConnectionForTypeDetection() {
		AtomicInteger connectionAcquisitionCount = new AtomicInteger();
		DatabaseMetaData databaseMetaData = metadata("PostgreSQL", "17", "jdbc:postgresql://localhost/test",
				"PostgreSQL JDBC Driver");
		Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
				new Class<?>[]{Connection.class}, (proxy, method, args) -> switch (method.getName()) {
					case "prepareStatement" -> throw new SQLException("prepare failed", "42601");
					case "getMetaData" -> databaseMetaData;
					case "close" -> null;
					case "toString" -> "single-borrow-connection";
					default -> throw new UnsupportedOperationException(method.getName());
				});
		DataSource dataSource = new DataSource() {
			@Override
			public Connection getConnection() throws SQLException {
				if (connectionAcquisitionCount.incrementAndGet() > 1)
					throw new SQLException("second connection acquisition would block");
				return connection;
			}

			@Override
			public Connection getConnection(String username, String password) throws SQLException {
				return getConnection();
			}

			@Override
			public PrintWriter getLogWriter() { return null; }

			@Override
			public void setLogWriter(PrintWriter out) {}

			@Override
			public void setLoginTimeout(int seconds) {}

			@Override
			public int getLoginTimeout() { return 0; }

			@Override
			public Logger getParentLogger() { return Logger.getLogger(DataSource.class.getName()); }

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("Not a wrapper"); }

			@Override
			public boolean isWrapperFor(Class<?> iface) { return false; }
		};
		Database db = Database.withDataSource(dataSource).build();

		DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT :value").bind("value", 1).execute());

		Assertions.assertEquals(1, connectionAcquisitionCount.get());
		Assertions.assertEquals(DatabaseType.POSTGRESQL, db.getDatabaseType());
		Assertions.assertEquals("42601", exception.getSqlState().orElseThrow());
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

	private static void assertDetectedDatabaseType(@NonNull DatabaseType expectedDatabaseType,
																								 @Nullable String databaseProductName,
																								 @Nullable String databaseProductVersion,
																								 @Nullable String url,
																								 @Nullable String driverName) {
		assertDetectedDatabaseType(expectedDatabaseType, databaseProductName, databaseProductVersion, url, driverName, false);
	}

	private static void assertDetectedDatabaseType(@NonNull DatabaseType expectedDatabaseType,
																								 @Nullable String databaseProductName,
																								 @Nullable String databaseProductVersion,
																								 @Nullable String url,
																								 @Nullable String driverName,
																								 boolean throwOnProductVersion) {
		requireNonNull(expectedDatabaseType);

		DatabaseType databaseType = DatabaseType.fromConnection(metadataConnection(
				databaseProductName, databaseProductVersion, url, driverName, throwOnProductVersion));

		Assertions.assertEquals(expectedDatabaseType, databaseType);
	}

	@NonNull
	private static Connection metadataConnection(@Nullable String databaseProductName,
																							 @Nullable String databaseProductVersion,
																							 @Nullable String url,
																							 @Nullable String driverName) {
		return metadataConnection(databaseProductName, databaseProductVersion, url, driverName, false);
	}

	@NonNull
	private static Connection metadataConnection(@Nullable String databaseProductName,
																							 @Nullable String databaseProductVersion,
																							 @Nullable String url,
																							 @Nullable String driverName,
																							 boolean throwOnProductVersion) {
		DatabaseMetaData databaseMetaData = metadata(databaseProductName, databaseProductVersion, url, driverName, throwOnProductVersion);

		return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
				(proxy, method, args) -> {
					if ("getMetaData".equals(method.getName()) && method.getParameterCount() == 0)
						return databaseMetaData;

					if ("toString".equals(method.getName()) && method.getParameterCount() == 0)
						return "metadataConnection";

					throw new UnsupportedOperationException("Unexpected connection method: " + method.getName());
				});
	}

	@NonNull
	private static DatabaseMetaData metadata(@Nullable String databaseProductName,
																					 @Nullable String databaseProductVersion,
																					 @Nullable String url,
																					 @Nullable String driverName) {
		return metadata(databaseProductName, databaseProductVersion, url, driverName, false);
	}

	@NonNull
	private static DatabaseMetaData metadata(@Nullable String databaseProductName,
																					 @Nullable String databaseProductVersion,
																					 @Nullable String url,
																					 @Nullable String driverName,
																					 boolean throwOnProductVersion) {
		return (DatabaseMetaData) Proxy.newProxyInstance(DatabaseMetaData.class.getClassLoader(), new Class<?>[]{DatabaseMetaData.class},
				(proxy, method, args) -> {
					if ("getDatabaseProductName".equals(method.getName()) && method.getParameterCount() == 0)
						return databaseProductName;

					if ("getDatabaseProductVersion".equals(method.getName()) && method.getParameterCount() == 0) {
						if (throwOnProductVersion)
							throw new SQLException("product version unavailable");

						return databaseProductVersion;
					}

					if ("getURL".equals(method.getName()) && method.getParameterCount() == 0)
						return url;

					if ("getDriverName".equals(method.getName()) && method.getParameterCount() == 0)
						return driverName;

					if ("toString".equals(method.getName()) && method.getParameterCount() == 0)
						return "metadata";

					throw new UnsupportedOperationException("Unexpected metadata method: " + method.getName());
				});
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
