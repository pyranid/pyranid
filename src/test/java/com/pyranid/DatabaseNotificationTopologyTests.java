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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

/**
 * Database-level topology, ownership, capability, and entry-guard tests for notification sessions.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@ThreadSafe
public class DatabaseNotificationTopologyTests {
	@Test
	public void explicitAndAutomaticallyDetectedCapabilityReportWithoutListenerCheckout() {
		SequenceDataSource explicitDataSource = new SequenceDataSource(List.of());
		Database explicitPostgres = Database.withDataSource(explicitDataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.build();

		Assertions.assertTrue(explicitPostgres.isNotificationListeningSupported());
		Assertions.assertEquals(0, explicitDataSource.checkouts());

		assertAutomaticallyDetectedCapability("PostgreSQL", true);
		assertAutomaticallyDetectedCapability("Oracle", false);
		assertAutomaticallyDetectedCapability("Unknown database", false);
	}

	@Test
	public void concurrentTopLevelSessionsUseIndependentConnectionsAndMetrics() throws InterruptedException {
		ListenerConnection firstConnection = new ListenerConnection("first");
		ListenerConnection secondConnection = new ListenerConnection("second");
		SequenceDataSource dataSource =
				new SequenceDataSource(List.of(firstConnection.connection(), secondConnection.connection()));
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(dataSource, metricsCollector);
		CountDownLatch callbacksEntered = new CountDownLatch(2);
		CountDownLatch releaseCallbacks = new CountDownLatch(1);
		AtomicReference<Throwable> failure = new AtomicReference<>();

		Thread firstThread = sessionThread(
				database, "first_channel", callbacksEntered, releaseCallbacks, failure);
		Thread secondThread = sessionThread(
				database, "second_channel", callbacksEntered, releaseCallbacks, failure);

		firstThread.start();
		secondThread.start();

		Assertions.assertTrue(callbacksEntered.await(2, TimeUnit.SECONDS),
				"Both independent notification sessions should reach their callbacks");
		Assertions.assertEquals(2, dataSource.checkouts());
		MetricsCollector.NotificationSnapshot activeSnapshot = notificationSnapshot(metricsCollector);
		Assertions.assertEquals(2L, activeSnapshot.sessionsOpened());
		Assertions.assertEquals(0L, activeSnapshot.sessionsCallbackReturned());

		releaseCallbacks.countDown();
		join(firstThread);
		join(secondThread);

		Assertions.assertNull(failure.get());
		Assertions.assertEquals(1, firstConnection.closeCalls());
		Assertions.assertEquals(1, secondConnection.closeCalls());
		MetricsCollector.NotificationSnapshot completedSnapshot = notificationSnapshot(metricsCollector);
		Assertions.assertEquals(2L, completedSnapshot.sessionsOpened());
		Assertions.assertEquals(2L, completedSnapshot.sessionsCallbackReturned());
		Assertions.assertEquals(0L, completedSnapshot.sessionsFailed());
	}

	@Test
	public void nestedSessionsHaveIndependentLifetimesAndOuterSessionResumes() throws InterruptedException {
		ListenerConnection outerConnection = new ListenerConnection("outer");
		ListenerConnection innerConnection = new ListenerConnection("inner");
		SequenceDataSource dataSource =
				new SequenceDataSource(List.of(outerConnection.connection(), innerConnection.connection()));
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(dataSource, metricsCollector);
		AtomicReference<NotificationSession> outerSession = new AtomicReference<>();
		AtomicReference<NotificationSession> innerSession = new AtomicReference<>();

		database.withNotificationSession("outer_channel", outer -> {
			outerSession.set(outer);
			Assertions.assertEquals(List.of(), outer.drainNotifications());

			database.withNotificationSession("inner_channel", inner -> {
				innerSession.set(inner);
				Assertions.assertEquals(List.of(), inner.drainNotifications());
				Assertions.assertEquals(List.of(), outer.drainNotifications(),
						"The outer session should remain active during a nested session");
			});

			Assertions.assertThrows(IllegalStateException.class,
					() -> requireNonNull(innerSession.get()).drainNotifications());
			Assertions.assertEquals(List.of(), outer.drainNotifications(),
					"The outer session should resume after the inner session closes");
		});

		Assertions.assertThrows(IllegalStateException.class,
				() -> requireNonNull(outerSession.get()).drainNotifications());
		Assertions.assertEquals(2, dataSource.checkouts());
		Assertions.assertEquals(1, outerConnection.closeCalls());
		Assertions.assertEquals(1, innerConnection.closeCalls());
		MetricsCollector.NotificationSnapshot snapshot = notificationSnapshot(metricsCollector);
		Assertions.assertEquals(2L, snapshot.sessionsOpened());
		Assertions.assertEquals(2L, snapshot.sessionsCallbackReturned());
		Assertions.assertEquals(0L, snapshot.sessionsFailed());
	}

	@Test
	public void eachDatabaseOwnsItsNotificationMetrics() throws InterruptedException {
		MetricsCollector firstCollector = MetricsCollector.inMemoryInstance();
		MetricsCollector secondCollector = MetricsCollector.inMemoryInstance();
		Database firstDatabase = postgresDatabase(
				new SequenceDataSource(List.of(new ListenerConnection("first").connection())), firstCollector);
		Database secondDatabase = postgresDatabase(
				new SequenceDataSource(List.of(new ListenerConnection("second").connection())), secondCollector);

		firstDatabase.withNotificationSession("first_channel", session -> {});

		Assertions.assertEquals(1L, notificationSnapshot(firstCollector).sessionsOpened());
		Assertions.assertEquals(0L, notificationSnapshot(secondCollector).sessionsOpened());

		secondDatabase.withNotificationSession("second_channel", session -> {});

		Assertions.assertEquals(1L, notificationSnapshot(firstCollector).sessionsOpened());
		Assertions.assertEquals(1L, notificationSnapshot(secondCollector).sessionsOpened());
	}

	@Test
	public void twoDatabasesCanIntentionallyShareOneNotificationMetricsCollector() throws InterruptedException {
		MetricsCollector sharedCollector = MetricsCollector.inMemoryInstance();
		Database firstDatabase = postgresDatabase(
				new SequenceDataSource(List.of(new ListenerConnection("first").connection())), sharedCollector);
		Database secondDatabase = postgresDatabase(
				new SequenceDataSource(List.of(new ListenerConnection("second").connection())), sharedCollector);

		firstDatabase.withNotificationSession("first_channel", session -> {});
		secondDatabase.withNotificationSession("second_channel", session -> {});

		MetricsCollector.NotificationSnapshot snapshot = notificationSnapshot(sharedCollector);
		Assertions.assertEquals(2L, snapshot.sessionsStarted());
		Assertions.assertEquals(2L, snapshot.sessionsOpened());
		Assertions.assertEquals(2L, snapshot.sessionsCallbackReturned());
		Assertions.assertEquals(0L, snapshot.sessionsFailed());
	}

	@Test
	public void publicEntryValidationFailsBeforeConnectionCheckout() {
		SequenceDataSource dataSource = new SequenceDataSource(List.of());
		Database database = postgresDatabase(dataSource, null);
		Set<String> channelsWithNull = new HashSet<>();
		channelsWithNull.add("valid");
		channelsWithNull.add(null);

		Assertions.assertThrows(NullPointerException.class,
				() -> database.withNotificationSession((Set<String>) null, session -> {}));
		Assertions.assertThrows(NullPointerException.class,
				() -> database.withNotificationSession(Set.of("valid"), null));
		Assertions.assertThrows(NullPointerException.class,
				() -> database.withNotificationSession((String) null, session -> {}));
		Assertions.assertThrows(NullPointerException.class,
				() -> database.withNotificationSession(channelsWithNull, session -> {}));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> database.withNotificationSession(Set.of(), session -> {}));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> database.withNotificationSession(" \t", session -> {}));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> database.withNotificationSession("a".repeat(64), session -> {}));
		Assertions.assertThrows(NullPointerException.class,
				() -> database.sendNotification(null));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> database.sendNotification(" \t"));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> database.sendNotification("valid", "payload\0"));

		Assertions.assertEquals(0, dataSource.checkouts());
	}

	@Test
	public void anyDatabaseTransactionRejectsPublicSessionEntryBeforeCheckout() {
		SequenceDataSource sharedDataSource = new SequenceDataSource(List.of());
		Database transactionDatabase = Database.withDataSource(sharedDataSource)
				.databaseType(DatabaseType.GENERIC)
				.build();
		Database sameDataSourceListener = postgresDatabase(sharedDataSource, null);
		SequenceDataSource differentDataSource = new SequenceDataSource(List.of());
		Database differentDataSourceListener = postgresDatabase(differentDataSource, null);

		Assertions.assertThrows(IllegalStateException.class, () ->
				transactionDatabase.transaction(() ->
						sameDataSourceListener.withNotificationSession("shared", session -> {})));
		Assertions.assertThrows(IllegalStateException.class, () ->
				transactionDatabase.transaction(() ->
						differentDataSourceListener.withNotificationSession("different", session -> {})));

		Assertions.assertEquals(0, sharedDataSource.checkouts());
		Assertions.assertEquals(0, differentDataSource.checkouts());
	}

	private static void assertAutomaticallyDetectedCapability(@NonNull String productName,
			boolean expectedCapability) {
		MetadataDataSource dataSource = new MetadataDataSource(productName);
		Database database = Database.withDataSource(dataSource).build();

		Assertions.assertEquals(expectedCapability, database.isNotificationListeningSupported());
		Assertions.assertEquals(expectedCapability, database.isNotificationListeningSupported(),
				"The cached database type should return the same capability");
		Assertions.assertEquals(1, dataSource.checkouts(),
				"Capability detection should acquire only the metadata connection");
		Assertions.assertEquals(1, dataSource.closeCalls());
	}

	@NonNull
	private static Database postgresDatabase(@NonNull DataSource dataSource,
			MetricsCollector metricsCollector) {
		Database.Builder builder = Database.withDataSource(requireNonNull(dataSource))
				.databaseType(DatabaseType.POSTGRESQL);

		if (metricsCollector != null)
			builder.metricsCollector(metricsCollector);

		return builder.build();
	}

	private static MetricsCollector.@NonNull NotificationSnapshot notificationSnapshot(
			@NonNull MetricsCollector metricsCollector) {
		return metricsCollector.notificationSnapshot().orElseThrow();
	}

	@NonNull
	private static Thread sessionThread(@NonNull Database database,
			@NonNull String channel,
			@NonNull CountDownLatch callbacksEntered,
			@NonNull CountDownLatch releaseCallbacks,
			@NonNull AtomicReference<Throwable> failure) {
		return new Thread(() -> {
			try {
				database.withNotificationSession(channel, session -> {
					callbacksEntered.countDown();

					if (!releaseCallbacks.await(2, TimeUnit.SECONDS))
						throw new AssertionError("Timed out waiting to release notification callbacks");
				});
			} catch (Throwable throwable) {
				failure.compareAndSet(null, throwable);
			}
		});
	}

	private static void join(@NonNull Thread thread) throws InterruptedException {
		thread.join(2_000L);

		if (thread.isAlive()) {
			thread.interrupt();
			thread.join(2_000L);
			Assertions.fail("Notification session thread did not terminate");
		}
	}

	private static Object objectMethod(Object proxy,
			@NonNull Method method,
			Object[] args,
			@NonNull String text) {
		return switch (method.getName()) {
			case "toString" -> text;
			case "hashCode" -> System.identityHashCode(proxy);
			case "equals" -> proxy == args[0];
			default -> throw new UnsupportedOperationException(method.getName());
		};
	}

	private static Object defaultValue(@NonNull Class<?> returnType) {
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
			return 0.0F;
		if (returnType == double.class)
			return 0.0D;
		if (returnType == char.class)
			return '\0';

		throw new IllegalArgumentException("Unsupported primitive type " + returnType);
	}

	private static final class ListenerConnection {
		@NonNull
		private final String name;
		@NonNull
		private final Connection connection;
		@NonNull
		private final AtomicInteger closeCalls;
		@NonNull
		private final AtomicBoolean closed;

		private ListenerConnection(@NonNull String name) {
			this.name = requireNonNull(name);
			this.closeCalls = new AtomicInteger();
			this.closed = new AtomicBoolean();
			this.connection = (Connection) Proxy.newProxyInstance(
					DatabaseNotificationTopologyTests.class.getClassLoader(),
					new Class<?>[]{Connection.class, PGConnection.class},
					this::invokeConnection);
		}

		@NonNull
		private Connection connection() {
			return this.connection;
		}

		private int closeCalls() {
			return this.closeCalls.get();
		}

		private Object invokeConnection(Object proxy,
				@NonNull Method method,
				Object[] args) {
			if (method.getDeclaringClass() == Object.class)
				return objectMethod(proxy, method, args, this.name + "NotificationConnection");

			return switch (method.getName()) {
				case "getAutoCommit" -> true;
				case "createStatement" -> statement();
				case "getNotifications" -> new PGNotification[0];
				case "getNetworkTimeout" -> 0;
				case "setNetworkTimeout" -> null;
				case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
				case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
				case "isClosed" -> this.closed.get();
				case "abort", "close" -> {
					this.closed.set(true);
					this.closeCalls.incrementAndGet();
					yield null;
				}
				default -> defaultValue(method.getReturnType());
			};
		}

		@NonNull
		private Statement statement() {
			return (Statement) Proxy.newProxyInstance(
					DatabaseNotificationTopologyTests.class.getClassLoader(),
					new Class<?>[]{Statement.class},
					(proxy, method, args) -> {
						if (method.getDeclaringClass() == Object.class)
							return objectMethod(proxy, method, args, this.name + "NotificationStatement");

						return switch (method.getName()) {
							case "execute" -> false;
							case "close" -> null;
							default -> defaultValue(method.getReturnType());
						};
					});
		}
	}

	private static class BaseDataSource implements DataSource {
		@Override
		public Connection getConnection() throws SQLException {
			throw new SQLException("No connection configured");
		}

		@Override
		public Connection getConnection(String username,
				String password) throws SQLException {
			return getConnection();
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

	private static final class SequenceDataSource extends BaseDataSource {
		@NonNull
		private final List<Connection> connections;
		@NonNull
		private final AtomicInteger checkouts;

		private SequenceDataSource(@NonNull List<Connection> connections) {
			this.connections = List.copyOf(requireNonNull(connections));
			this.checkouts = new AtomicInteger();
		}

		@NonNull
		@Override
		public Connection getConnection() throws SQLException {
			int index = this.checkouts.getAndIncrement();

			if (index >= this.connections.size())
				throw new SQLException("Unexpected connection checkout " + (index + 1));

			return this.connections.get(index);
		}

		private int checkouts() {
			return this.checkouts.get();
		}
	}

	private static final class MetadataDataSource extends BaseDataSource {
		@NonNull
		private final Connection connection;
		@NonNull
		private final AtomicInteger checkouts;
		@NonNull
		private final AtomicInteger closeCalls;

		private MetadataDataSource(@NonNull String productName) {
			this.checkouts = new AtomicInteger();
			this.closeCalls = new AtomicInteger();
			DatabaseMetaData metadata = metadata(productName);
			this.connection = (Connection) Proxy.newProxyInstance(
					DatabaseNotificationTopologyTests.class.getClassLoader(),
					new Class<?>[]{Connection.class},
					(proxy, method, args) -> {
						if (method.getDeclaringClass() == Object.class)
							return objectMethod(proxy, method, args, productName + "MetadataConnection");

						return switch (method.getName()) {
							case "getMetaData" -> metadata;
							case "close" -> {
								this.closeCalls.incrementAndGet();
								yield null;
							}
							default -> defaultValue(method.getReturnType());
						};
					});
		}

		@NonNull
		@Override
		public Connection getConnection() {
			this.checkouts.incrementAndGet();
			return this.connection;
		}

		private int checkouts() {
			return this.checkouts.get();
		}

		private int closeCalls() {
			return this.closeCalls.get();
		}

		@NonNull
		private static DatabaseMetaData metadata(@NonNull String productName) {
			return (DatabaseMetaData) Proxy.newProxyInstance(
					DatabaseNotificationTopologyTests.class.getClassLoader(),
					new Class<?>[]{DatabaseMetaData.class},
					(proxy, method, args) -> {
						if (method.getDeclaringClass() == Object.class)
							return objectMethod(proxy, method, args, productName + "Metadata");

						return switch (method.getName()) {
							case "getDatabaseProductName" -> productName;
							case "getDatabaseProductVersion", "getURL", "getDriverName" -> null;
							default -> defaultValue(method.getReturnType());
						};
					});
		}
	}
}
