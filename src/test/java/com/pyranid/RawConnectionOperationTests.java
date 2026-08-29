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

import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for guarded and driver-typed raw connection operations.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
public class RawConnectionOperationTests {
	private interface VendorConnection extends Connection {
		String driverFeature();
	}

	private interface UnsupportedConnection extends Connection {}

	@Test
	public void testGuardedOverloadAcceptsGeneralizedOperation() {
		Database database = database("rawConnectionGuardedGeneric");
		RawConnectionOperation<Connection, String> operation = connection -> {
			Assertions.assertInstanceOf(PyranidRawConnection.class, connection);
			return Optional.of(connection.getMetaData().getDatabaseProductName());
		};

		Assertions.assertEquals(Optional.of("HSQL Database Engine"), database.useRawConnection(operation));
	}

	@Test
	public void testTypedOverloadExposesConcreteConnectionAndClosesItAfterCallback() throws Exception {
		Database database = database("rawConnectionConcrete");
		AtomicReference<JDBCConnection> retainedConnection = new AtomicReference<>();

		Optional<String> result = database.useRawConnection(JDBCConnection.class, connection -> {
			retainedConnection.set(connection);
			Assertions.assertNotEquals(PyranidRawConnection.class, connection.getClass());
			Assertions.assertFalse(connection.isClosed());
			return Optional.of(connection.getMetaData().getDatabaseProductName());
		});

		Assertions.assertEquals(Optional.of("HSQL Database Engine"), result);
		Assertions.assertTrue(retainedConnection.get().isClosed(),
				"Pyranid should close a typed connection borrowed outside a transaction");
	}

	@Test
	public void testTypedOverloadUnwrapsRequestedConnectionType() {
		AtomicInteger managedCloseCount = new AtomicInteger();
		VendorConnection vendorConnection = vendorConnection();
		Connection managedConnection = managedConnectionWrapper(vendorConnection, managedCloseCount);
		Database database = Database.withDataSource(dataSource(managedConnection))
				.databaseType(DatabaseType.GENERIC)
				.build();

		Optional<String> result = database.useRawConnection(VendorConnection.class, connection -> {
			Assertions.assertSame(vendorConnection, connection);
			return Optional.of(connection.driverFeature());
		});

		Assertions.assertEquals(Optional.of("driver-specific"), result);
		Assertions.assertEquals(1, managedCloseCount.get());
	}

	@Test
	public void testTypedOverloadAcceptsOperationForConnectionSupertype() {
		Database database = database("rawConnectionTypedContravariance");
		RawConnectionOperation<Connection, String> operation = connection ->
				Optional.of(connection.getMetaData().getDatabaseProductName());

		Assertions.assertEquals(Optional.of("HSQL Database Engine"),
				database.useRawConnection(JDBCConnection.class, operation));
	}

	@Test
	public void testTypedOverloadParticipatesInTransaction() {
		Database database = database("rawConnectionTypedTransaction");
		database.query("CREATE TABLE raw_connection_transaction (id INTEGER)").execute();

		Assertions.assertThrows(IllegalStateException.class, () -> database.transaction(() -> {
			database.useRawConnection(JDBCConnection.class, connection -> {
				try (PreparedStatement preparedStatement =
							 connection.prepareStatement("INSERT INTO raw_connection_transaction VALUES (?)")) {
					preparedStatement.setInt(1, 1);
					preparedStatement.executeUpdate();
				}

				return Optional.empty();
			});

			throw new IllegalStateException("roll back");
		}));

		Assertions.assertEquals(Long.valueOf(0), database.query("SELECT COUNT(*) FROM raw_connection_transaction")
				.fetchObject(Long.class)
				.orElseThrow());
	}

	@Test
	public void testTypedOverloadNormalizesNullResult() {
		Database database = database("rawConnectionTypedNullResult");

		Optional<String> result = database.useRawConnection(JDBCConnection.class, connection -> null);

		Assertions.assertEquals(Optional.empty(), result);
	}

	@Test
	public void testTypedOverloadPreservesDatabaseException() {
		Database database = database("rawConnectionTypedDatabaseException");
		DatabaseException cause = new DatabaseException("expected");

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.useRawConnection(JDBCConnection.class, connection -> {
					throw cause;
				}));

		Assertions.assertSame(cause, thrown);
	}

	@Test
	public void testTypedOverloadWrapsCheckedException() {
		Database database = database("rawConnectionTypedCheckedException");
		Exception cause = new Exception("expected");

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.useRawConnection(JDBCConnection.class, connection -> {
					throw cause;
				}));

		Assertions.assertSame(cause, thrown.getCause());
	}

	@Test
	public void testTypedOverloadWrapsUnwrapFailure() {
		Database database = database("rawConnectionTypedUnwrapFailure");

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.useRawConnection(UnsupportedConnection.class, connection -> Optional.empty()));

		Assertions.assertInstanceOf(SQLException.class, thrown.getCause());
	}

	@Test
	public void testTypedOverloadRejectsNullArguments() {
		Database database = database("rawConnectionTypedNullArguments");

		Assertions.assertThrows(NullPointerException.class,
				() -> database.useRawConnection(null, connection -> Optional.empty()));
		Assertions.assertThrows(NullPointerException.class,
				() -> database.useRawConnection(JDBCConnection.class, null));
	}

	private Database database(String name) {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl("jdbc:hsqldb:mem:" + name);
		dataSource.setUser("sa");
		dataSource.setPassword("");
		return Database.withDataSource(dataSource)
				.databaseType(DatabaseType.GENERIC)
				.build();
	}

	private VendorConnection vendorConnection() {
		AtomicBoolean closed = new AtomicBoolean();

		return (VendorConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
				new Class<?>[]{VendorConnection.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "driverFeature" -> "driver-specific";
					case "close" -> {
						closed.set(true);
						yield null;
					}
					case "isClosed" -> closed.get();
					case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
					case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
					default -> defaultValue(method.getReturnType());
				});
	}

	private Connection managedConnectionWrapper(VendorConnection vendorConnection,
																					 AtomicInteger closeCount) {
		return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "close" -> {
						closeCount.incrementAndGet();
						yield null;
					}
					case "isClosed" -> closeCount.get() > 0;
					case "isWrapperFor" -> args[0] == VendorConnection.class;
					case "unwrap" -> {
						if (args[0] == VendorConnection.class)
							yield vendorConnection;

						throw new SQLException("Unsupported connection type " + args[0]);
					}
					default -> defaultValue(method.getReturnType());
				});
	}

	private DataSource dataSource(Connection connection) {
		return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getConnection" -> connection;
					default -> defaultValue(method.getReturnType());
				});
	}

	private Object defaultValue(Class<?> type) {
		if (!type.isPrimitive())
			return null;
		if (type == boolean.class)
			return false;
		if (type == byte.class)
			return (byte) 0;
		if (type == short.class)
			return (short) 0;
		if (type == int.class)
			return 0;
		if (type == long.class)
			return 0L;
		if (type == float.class)
			return 0.0F;
		if (type == double.class)
			return 0.0D;
		if (type == char.class)
			return '\0';

		return null;
	}
}
