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

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

/**
 * Child-process probe whose constant pool deliberately contains no pgjdbc or YugabyteDB driver type.
 */
public final class YugabyteNotificationAdapterClasspathProbe {
	private YugabyteNotificationAdapterClasspathProbe() {}

	public static void main(String[] arguments) throws Exception {
		if (arguments.length != 1)
			throw new AssertionError("Expected the isolated driver-classes directory");

		assertApplicationLoaderCannotSee("org.postgresql.PGConnection");
		assertApplicationLoaderCannotSee("com.yugabyte.PGConnection");

		ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
		URL driverClassesUrl = Path.of(arguments[0]).toUri().toURL();

		try (URLClassLoader driverClassLoader = new URLClassLoader(
				new URL[]{driverClassesUrl}, ClassLoader.getPlatformClassLoader())) {
			Thread.currentThread().setContextClassLoader(driverClassLoader);

			if (!PostgresNotificationAdapterLoader.isAvailable())
				throw new AssertionError("YugabyteDB receive API was not discovered through the context class loader");

			DriverFixture mappingFixture = new DriverFixture(driverClassLoader);
			mappingFixture.notifications = mappingFixture.notificationArray("car_changed", "17");
			NotificationTransport mappingTransport = PostgresNotificationAdapterLoader.open(
					mappingFixture.platformConnection);
			List<Notification> notifications = mappingTransport.receive(Duration.ofMillis(5));

			if (!notifications.equals(List.of(Notification.of("car_changed", "17"))))
				throw new AssertionError("Unexpected mapped notifications: " + notifications);

			if (mappingFixture.unwrappedApi == null
					|| !"com.yugabyte.PGConnection".equals(mappingFixture.unwrappedApi.getName())
					|| mappingFixture.unwrappedApi.getClassLoader() != driverClassLoader)
				throw new AssertionError("The platform wrapper was not unwrapped to the child-loaded YugabyteDB API");

			if (mappingTransport.isConnectionUncertain())
				throw new AssertionError("Successful notification mapping marked the connection uncertain");

			SQLException sqlFailure = new SQLException("receive failed", "08006");
			DriverFixture sqlFixture = new DriverFixture(driverClassLoader);
			sqlFixture.receiveFailure = sqlFailure;
			NotificationTransport sqlTransport = PostgresNotificationAdapterLoader.open(sqlFixture.platformConnection);

			try {
				sqlTransport.receive(Duration.ofMillis(5));
				throw new AssertionError("Expected the driver's SQLException");
			} catch (SQLException thrown) {
				if (thrown != sqlFailure)
					throw new AssertionError("SQLException identity was not preserved", thrown);
			}

			if (!sqlTransport.isConnectionUncertain())
				throw new AssertionError("Driver SQLException did not mark the connection uncertain");

			AssertionError errorFailure = new AssertionError("driver error");
			DriverFixture errorFixture = new DriverFixture(driverClassLoader);
			errorFixture.receiveFailure = errorFailure;
			NotificationTransport errorTransport = PostgresNotificationAdapterLoader.open(errorFixture.platformConnection);

			try {
				errorTransport.receive(Duration.ofMillis(5));
				throw new AssertionError("Expected the driver's Error");
			} catch (AssertionError thrown) {
				if (thrown != errorFailure)
					throw new AssertionError("Error identity was not preserved", thrown);
			}

			if (!errorTransport.isConnectionUncertain())
				throw new AssertionError("Driver Error did not mark the connection uncertain");
		} finally {
			Thread.currentThread().setContextClassLoader(originalContextClassLoader);
		}
	}

	private static void assertApplicationLoaderCannotSee(String className) throws Exception {
		try {
			Class.forName(className, false, YugabyteNotificationAdapterClasspathProbe.class.getClassLoader());
			throw new AssertionError(className + " unexpectedly remained on the isolated application classpath");
		} catch (ClassNotFoundException expected) {
			// Expected.
		}
	}

	private static final class DriverFixture {
		private final ClassLoader driverClassLoader;
		private final Class<?> notificationApi;
		private final Object driverConnection;
		private final Connection platformConnection;
		private Object notifications;
		private Throwable receiveFailure;
		private Class<?> unwrappedApi;

		private DriverFixture(ClassLoader driverClassLoader) throws Exception {
			this.driverClassLoader = driverClassLoader;
			Class<?> connectionApi = Class.forName("com.yugabyte.PGConnection", true, driverClassLoader);
			this.notificationApi = Class.forName("com.yugabyte.PGNotification", true, driverClassLoader);
			this.notifications = Array.newInstance(this.notificationApi, 0);
			this.driverConnection = Proxy.newProxyInstance(
					driverClassLoader, new Class<?>[]{Connection.class, connectionApi}, this::invokeDriverConnection);
			this.platformConnection = (Connection) Proxy.newProxyInstance(
					Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, this::invokePlatformConnection);
		}

		private Object notificationArray(String channel,
													 String payload) {
			Object notification = Proxy.newProxyInstance(
					this.driverClassLoader, new Class<?>[]{this.notificationApi}, (proxy, method, arguments) ->
							switch (method.getName()) {
								case "getName" -> channel;
								case "getParameter" -> payload;
								default -> objectMethod(proxy, method, arguments);
							});
			Object notificationArray = Array.newInstance(this.notificationApi, 1);
			Array.set(notificationArray, 0, notification);
			return notificationArray;
		}

		private Object invokeDriverConnection(Object proxy,
																	Method method,
																	Object[] arguments) throws Throwable {
			if (method.getDeclaringClass() == Object.class)
				return objectMethod(proxy, method, arguments);

			if ("getNotifications".equals(method.getName())) {
				if (this.receiveFailure != null)
					throw this.receiveFailure;

				return this.notifications;
			}

			return defaultValue(method.getReturnType());
		}

		private Object invokePlatformConnection(Object proxy,
																		Method method,
																		Object[] arguments) throws SQLException {
			if (method.getDeclaringClass() == Object.class)
				return objectMethod(proxy, method, arguments);

			return switch (method.getName()) {
				case "isWrapperFor" -> ((Class<?>) arguments[0]).isInstance(this.driverConnection);
				case "unwrap" -> {
					Class<?> requestedApi = (Class<?>) arguments[0];
					this.unwrappedApi = requestedApi;
					yield requestedApi.cast(this.driverConnection);
				}
				case "getNetworkTimeout" -> 0;
				case "setNetworkTimeout" -> null;
				case "isClosed" -> false;
				default -> defaultValue(method.getReturnType());
			};
		}
	}

	private static Object objectMethod(Object proxy,
															 Method method,
															 Object[] arguments) {
		return switch (method.getName()) {
			case "toString" -> "yugabyteNotificationAdapterProbe";
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
