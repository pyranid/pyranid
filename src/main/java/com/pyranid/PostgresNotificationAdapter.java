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
import org.jspecify.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Reflective access to a PostgreSQL JDBC driver's {@code PGConnection} notification API.
 * <p>
 * The driver's types are resolved by name at runtime rather than linked statically, so a repackaged fork of
 * pgjdbc works on its own terms - YugabyteDB's "smart driver", for example, ships
 * {@code com.yugabyte.PGConnection} and {@code com.yugabyte.PGNotification}. Candidate interfaces are always
 * reached through {@link Connection#isWrapperFor(Class)} and {@link Connection#unwrap(Class)}, never by
 * inspecting the connection's own class, because pooled connections are wrappers whose package says nothing
 * about the driver beneath them. Only public interface members are reflected over; nothing here calls
 * {@code setAccessible}.
 * <p>
 * Driver failures raised through reflection are unwrapped back to their original type before they reach
 * {@link PostgresNotificationTransport}, whose connection-uncertainty handling distinguishes
 * {@link SQLException}, {@link java.sql.SQLFeatureNotSupportedException}, {@link RuntimeException} and
 * {@link AbstractMethodError} from one another.
 */
final class PostgresNotificationAdapter {
	/**
	 * Well-known driver roots tried before falling back to the connection's own package. pgjdbc and
	 * YugabyteDB's "smart driver" ship the same API under their respective roots.
	 */
	@NonNull
	private static final List<@NonNull String> CANDIDATE_DRIVER_ROOT_PACKAGES = List.of(
			"org.postgresql", "com.yugabyte");
	@NonNull
	private static final String CONNECTION_SIMPLE_CLASS_NAME = "PGConnection";
	@NonNull
	private static final String NOTIFICATION_SIMPLE_CLASS_NAME = "PGNotification";

	@NonNull
	private final Object driverConnection;
	@NonNull
	private final Method timedReceiveMethod;
	@NonNull
	private final Method receiveMethod;
	@NonNull
	private final Method notificationNameMethod;
	@NonNull
	private final Method notificationPayloadMethod;

	private PostgresNotificationAdapter(@NonNull Object driverConnection,
																			@NonNull Method timedReceiveMethod,
																			@NonNull Method receiveMethod,
																			@NonNull Method notificationNameMethod,
																			@NonNull Method notificationPayloadMethod) {
		this.driverConnection = requireNonNull(driverConnection);
		this.timedReceiveMethod = requireNonNull(timedReceiveMethod);
		this.receiveMethod = requireNonNull(receiveMethod);
		this.notificationNameMethod = requireNonNull(notificationNameMethod);
		this.notificationPayloadMethod = requireNonNull(notificationPayloadMethod);
	}

	/**
	 * Resolves the notification API of the driver that owns {@code connection} and unwraps the connection to it.
	 *
	 * @throws NotificationReceiveUnsupportedException if the driver exposes no usable notification API
	 * @throws SQLException if unwrapping fails for a reason other than lack of support
	 */
	@NonNull
	static PostgresNotificationAdapter forConnection(@NonNull Connection connection)
			throws SQLException, NotificationReceiveUnsupportedException {
		requireNonNull(connection);

		// Every candidate is reached through isWrapperFor/unwrap, never by inspecting the connection's own
		// class. Pooled connections are wrappers - HikariCP hands out HikariProxyConnection - so the driver
		// interface is only reachable by unwrapping, and a connection's own package says nothing about the
		// driver underneath it.
		for (Class<?> connectionApi : candidateConnectionApis(connection)) {
			Class<?> notificationApi = resolveSibling(connectionApi, NOTIFICATION_SIMPLE_CLASS_NAME);

			if (notificationApi == null)
				continue;

			Method timedReceiveMethod = notificationArrayMethod(connectionApi, notificationApi, int.class);
			Method receiveMethod = notificationArrayMethod(connectionApi, notificationApi);
			Method notificationNameMethod = stringMethod(notificationApi, "getName");
			Method notificationPayloadMethod = stringMethod(notificationApi, "getParameter");

			if (timedReceiveMethod == null || receiveMethod == null
					|| notificationNameMethod == null || notificationPayloadMethod == null)
				continue;

			if (!connection.isWrapperFor(connectionApi))
				continue;

			Object driverConnection = connection.unwrap(connectionApi);

			if (driverConnection == null)
				throw new NotificationReceiveUnsupportedException(
						"JDBC connection returned a null PostgreSQL notification receive adapter", null);

			return new PostgresNotificationAdapter(driverConnection, timedReceiveMethod, receiveMethod,
					notificationNameMethod, notificationPayloadMethod);
		}

		throw new NotificationReceiveUnsupportedException(
				"JDBC connection does not expose PostgreSQL notification receive support", null);
	}

	/**
	 * Driver notification interfaces to try, in order: the well-known roots first, then - for an unwrapped
	 * connection belonging to a fork this build has never heard of - a sibling of the connection's own
	 * package. Missing classes are simply skipped.
	 */
	@NonNull
	private static List<@NonNull Class<?>> candidateConnectionApis(@NonNull Connection connection) {
		requireNonNull(connection);

		List<Class<?>> candidates = new ArrayList<>();

		for (ClassLoader classLoader : candidateClassLoaders(connection))
			for (String driverRootPackage : CANDIDATE_DRIVER_ROOT_PACKAGES) {
				Class<?> candidate = loadClass(
						driverRootPackage + "." + CONNECTION_SIMPLE_CLASS_NAME, classLoader);

				if (candidate != null && candidate.isInterface() && !candidates.contains(candidate))
					candidates.add(candidate);
			}

		// Last resort for a fork this build has never heard of, and only meaningful for an unwrapped
		// connection: a sibling of the connection's own package.
		Class<?> resolvedFromConnection = resolveDriverClass(connection.getClass(), CONNECTION_SIMPLE_CLASS_NAME);

		if (resolvedFromConnection != null && !candidates.contains(resolvedFromConnection))
			candidates.add(resolvedFromConnection);

		return candidates;
	}

	/**
	 * Class loaders to search, most reliable first.
	 * <p>
	 * Pyranid's own loader comes first because it is the one guaranteed to see the driver an application
	 * put on Pyranid's classpath. The connection's loader is NOT sufficient on its own: a connection may be
	 * a {@link java.lang.reflect.Proxy} created with {@code Connection.class.getClassLoader()} - the platform
	 * loader, which cannot see application classes at all - which is exactly what tracing and instrumentation
	 * wrappers do. The context loader covers container deployments that isolate the driver from Pyranid.
	 */
	@NonNull
	private static List<@NonNull ClassLoader> candidateClassLoaders(@NonNull Connection connection) {
		requireNonNull(connection);

		List<ClassLoader> classLoaders = new ArrayList<>(3);

		for (ClassLoader classLoader : new ClassLoader[]{
				PostgresNotificationAdapter.class.getClassLoader(),
				connection.getClass().getClassLoader(),
				currentThreadContextClassLoader()})
			if (classLoader != null && !classLoaders.contains(classLoader))
				classLoaders.add(classLoader);

		return classLoaders;
	}

	@Nullable
	private static ClassLoader currentThreadContextClassLoader() {
		try {
			return Thread.currentThread().getContextClassLoader();
		} catch (SecurityException e) {
			return null;
		}
	}

	/**
	 * Resolves {@code simpleClassName} from the same package as {@code sibling}.
	 */
	@Nullable
	private static Class<?> resolveSibling(@NonNull Class<?> sibling,
																				 @NonNull String simpleClassName) {
		requireNonNull(sibling);
		requireNonNull(simpleClassName);

		Class<?> resolved = loadClass(
				sibling.getPackageName() + "." + simpleClassName, sibling.getClassLoader());
		return resolved != null && resolved.isInterface() ? resolved : null;
	}

	/**
	 * Receives pending notifications, blocking up to {@code waitMilliseconds}.
	 */
	Object @Nullable [] receive(int waitMilliseconds) throws SQLException {
		return invokeReceive(this.timedReceiveMethod, waitMilliseconds);
	}

	/**
	 * Receives already-buffered notifications without blocking.
	 */
	Object @Nullable [] receive() throws SQLException {
		return invokeReceive(this.receiveMethod);
	}

	@Nullable
	String notificationName(@NonNull Object notification) {
		requireNonNull(notification);
		return invokeNotificationAccessor(this.notificationNameMethod, notification);
	}

	@Nullable
	String notificationPayload(@NonNull Object notification) {
		requireNonNull(notification);
		return invokeNotificationAccessor(this.notificationPayloadMethod, notification);
	}

	private Object @Nullable [] invokeReceive(@NonNull Method method,
																						@NonNull Object @NonNull ... arguments) throws SQLException {
		requireNonNull(method);
		requireNonNull(arguments);

		try {
			return (Object[]) method.invoke(this.driverConnection, arguments);
		} catch (InvocationTargetException e) {
			throw rethrowDriverFailure(e.getTargetException());
		} catch (IllegalAccessException | IllegalArgumentException e) {
			throw new NotificationReceiveUnsupportedException(
					"Unable to invoke the driver's PostgreSQL notification receive API", e);
		}
	}

	@Nullable
	private String invokeNotificationAccessor(@NonNull Method method,
																						@NonNull Object notification) {
		requireNonNull(method);
		requireNonNull(notification);

		try {
			return (String) method.invoke(notification);
		} catch (InvocationTargetException e) {
			Throwable targetException = e.getTargetException();

			if (targetException instanceof RuntimeException runtimeException)
				throw runtimeException;

			if (targetException instanceof Error error)
				throw error;

			throw new NotificationReceiveUnsupportedException(
					"Unable to read a notification from the driver", targetException);
		} catch (IllegalAccessException | IllegalArgumentException e) {
			throw new NotificationReceiveUnsupportedException("Unable to read a notification from the driver", e);
		}
	}

	/**
	 * Rethrows a driver failure with its original type preserved, so the caller's connection-uncertainty
	 * classification behaves exactly as it does under a statically linked driver. Always throws.
	 */
	@NonNull
	private SQLException rethrowDriverFailure(@Nullable Throwable targetException) throws SQLException {
		if (targetException instanceof SQLException sqlException)
			throw sqlException;

		if (targetException instanceof RuntimeException runtimeException)
			throw runtimeException;

		if (targetException instanceof Error error)
			throw error;

		throw new NotificationReceiveUnsupportedException(
				"PostgreSQL notification receive adapter failed", targetException);
	}

	/**
	 * Finds {@code simpleClassName} in the driver's root package by walking up from {@code driverType}'s package.
	 * {@code org.postgresql.jdbc.PgConnection} resolves {@code org.postgresql.PGConnection};
	 * {@code com.yugabyte.jdbc.PgConnection} resolves {@code com.yugabyte.PGConnection}.
	 */
	@Nullable
	private static Class<?> resolveDriverClass(@NonNull Class<?> driverType,
																						 @NonNull String simpleClassName) {
		requireNonNull(driverType);
		requireNonNull(simpleClassName);

		ClassLoader classLoader = driverType.getClassLoader();
		String packageName = driverType.getPackageName();

		while (!packageName.isEmpty()) {
			Class<?> candidate = loadClass(packageName + "." + simpleClassName, classLoader);

			if (candidate != null && candidate.isInterface())
				return candidate;

			int lastSeparatorIndex = packageName.lastIndexOf('.');

			if (lastSeparatorIndex < 0)
				break;

			packageName = packageName.substring(0, lastSeparatorIndex);
		}

		return null;
	}

	@Nullable
	private static Class<?> loadClass(@NonNull String className,
																		@Nullable ClassLoader classLoader) {
		requireNonNull(className);

		try {
			return Class.forName(className, false, classLoader);
		} catch (ClassNotFoundException | LinkageError e) {
			return null;
		}
	}

	/**
	 * A public {@code getNotifications(...)} returning an array of the driver's notification type.
	 */
	@Nullable
	private static Method notificationArrayMethod(@NonNull Class<?> connectionApi,
																								@NonNull Class<?> notificationApi,
																								@NonNull Class<?>... parameterTypes) {
		requireNonNull(connectionApi);
		requireNonNull(notificationApi);
		requireNonNull(parameterTypes);

		try {
			Method method = connectionApi.getMethod("getNotifications", parameterTypes);
			Class<?> returnType = method.getReturnType();
			return returnType.isArray() && returnType.getComponentType() == notificationApi ? method : null;
		} catch (NoSuchMethodException e) {
			return null;
		}
	}

	@Nullable
	private static Method stringMethod(@NonNull Class<?> declaringApi,
																		 @NonNull String methodName) {
		requireNonNull(declaringApi);
		requireNonNull(methodName);

		try {
			Method method = declaringApi.getMethod(methodName);
			return method.getReturnType() == String.class ? method : null;
		} catch (NoSuchMethodException e) {
			return null;
		}
	}
}
