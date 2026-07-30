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

import static java.util.Objects.requireNonNull;

/**
 * Loads the optional PostgreSQL receive adapter without statically linking its driver-specific types.
 */
final class PostgresNotificationAdapterLoader {
	@NonNull
	private static final String CONNECTION_CLASS_NAME = "org.postgresql.PGConnection";
	@NonNull
	private static final String NOTIFICATION_CLASS_NAME = "org.postgresql.PGNotification";
	@NonNull
	private static final String ADAPTER_CLASS_NAME = "com.pyranid.PostgresNotificationTransport";
	@NonNull
	private static final String OPEN_METHOD_NAME = "open";

	private PostgresNotificationAdapterLoader() {}

	static boolean isAvailable() {
		try {
			loadAdapterClass(false);
			return true;
		} catch (ReflectiveOperationException | LinkageError | SecurityException ignored) {
			return false;
		}
	}

	@NonNull
	static NotificationTransport open(@NonNull Connection connection)
			throws SQLException, NotificationReceiveUnsupportedException {
		requireNonNull(connection);

		try {
			Class<?> adapterClass = loadAdapterClass(true);
			Method openMethod = adapterClass.getDeclaredMethod(OPEN_METHOD_NAME, Connection.class);
			Object transport = openMethod.invoke(null, connection);

			if (!(transport instanceof NotificationTransport))
				throw unsupported("PostgreSQL notification receive adapter returned an incompatible transport", null);

			return (NotificationTransport) transport;
		} catch (InvocationTargetException exception) {
			Throwable cause = exception.getCause();

			if (cause instanceof NotificationReceiveUnsupportedException)
				throw (NotificationReceiveUnsupportedException) cause;

			if (cause instanceof SQLException)
				throw (SQLException) cause;

			if (cause instanceof LinkageError)
				throw unsupported("PostgreSQL notification receive adapter is unavailable", cause);

			if (cause instanceof RuntimeException)
				throw (RuntimeException) cause;

			if (cause instanceof Error)
				throw (Error) cause;

			throw unsupported("Unable to initialize PostgreSQL notification receive adapter", cause);
		} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException |
						 LinkageError | SecurityException exception) {
			throw unsupported("PostgreSQL notification receive adapter is unavailable", exception);
		}
	}

	@NonNull
	private static Class<?> loadAdapterClass(boolean initializeAdapter)
			throws ClassNotFoundException, NoSuchMethodException {
		ClassLoader classLoader = PostgresNotificationAdapterLoader.class.getClassLoader();
		Class<?> connectionClass = Class.forName(CONNECTION_CLASS_NAME, false, classLoader);
		Class<?> notificationClass = Class.forName(NOTIFICATION_CLASS_NAME, false, classLoader);

		requireNotificationArrayReturnType(
				connectionClass.getMethod("getNotifications"), notificationClass);
		requireNotificationArrayReturnType(
				connectionClass.getMethod("getNotifications", int.class), notificationClass);
		requireReturnType(notificationClass.getMethod("getName"), String.class);
		requireReturnType(notificationClass.getMethod("getParameter"), String.class);

		return Class.forName(ADAPTER_CLASS_NAME, initializeAdapter, classLoader);
	}

	private static void requireNotificationArrayReturnType(@NonNull Method method,
																												 @NonNull Class<?> notificationClass)
			throws NoSuchMethodException {
		requireNonNull(method);
		requireNonNull(notificationClass);

		Class<?> returnType = method.getReturnType();

		if (!returnType.isArray() || returnType.getComponentType() != notificationClass)
			throw new NoSuchMethodException(method.toString());
	}

	private static void requireReturnType(@NonNull Method method,
																			 @NonNull Class<?> returnType) throws NoSuchMethodException {
		requireNonNull(method);
		requireNonNull(returnType);

		if (method.getReturnType() != returnType)
			throw new NoSuchMethodException(method.toString());
	}

	@NonNull
	private static NotificationReceiveUnsupportedException unsupported(@NonNull String message,
																																		 @Nullable Throwable cause) {
		return new NotificationReceiveUnsupportedException(message, cause);
	}
}
