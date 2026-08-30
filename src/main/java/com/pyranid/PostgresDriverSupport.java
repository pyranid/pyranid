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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Reflective bridge to the PostgreSQL JDBC driver's {@code PGobject} and {@code PSQLException} types.
 * <p>
 * Pyranid does not depend on pgjdbc at runtime and explicitly supports both pgjdbc and YugabyteDB's smart
 * driver, which ships the same classes under
 * {@code com.yugabyte.util} rather than {@code org.postgresql.util}. Referring to those classes by their
 * pgjdbc names would fail on such a driver with {@link NoClassDefFoundError} when pgjdbc is absent, or bind
 * a {@code PGobject} the driver does not recognize when both are on the classpath.
 * <p>
 * Types are therefore resolved at runtime. Which driver a connection belongs to is established by asking it,
 * through {@link Connection#isWrapperFor(Class)}, rather than by inspecting its class - a pooled or traced
 * connection is a wrapper whose package says nothing about the driver beneath it. A returned value's own
 * class is authoritative, and that lookup is cached per class.
 */
final class PostgresDriverSupport {
	@NonNull
	private static final List<@NonNull String> CANDIDATE_DRIVER_ROOT_PACKAGES = List.of(
			"org.postgresql", "com.yugabyte");
	@NonNull
	private static final String PG_CONNECTION_SIMPLE_CLASS_NAME = "PGConnection";
	@NonNull
	private static final String PG_OBJECT_SIMPLE_CLASS_NAME = "PGobject";
	@NonNull
	private static final String PSQL_EXCEPTION_SIMPLE_CLASS_NAME = "PSQLException";
	@NonNull
	private static final String UTIL_PACKAGE_SEGMENT = "util";

	/**
	 * A driver's {@code PGobject} class to everything needed to build one. Keyed on the resolved
	 * {@code PGobject} class rather than the connection class: one wrapper type (a pool's proxy) can front
	 * different drivers in the same application, so the connection class is not a sound cache key.
	 */
	@NonNull
	private static final ClassValue<PgObjectFactory> PG_OBJECT_FACTORIES = new ClassValue<>() {
		@Override
		@Nullable
		protected PgObjectFactory computeValue(@NonNull Class<?> pgObjectClass) {
			try {
				return new PgObjectFactory(
						pgObjectClass.getConstructor(),
						pgObjectClass.getMethod("setType", String.class),
						pgObjectClass.getMethod("setValue", String.class));
			} catch (NoSuchMethodException e) {
				return null;
			}
		}
	};

	private record PgObjectFactory(@NonNull Constructor<?> constructor,
																 @NonNull Method typeSetter,
																 @NonNull Method valueSetter) {}

	/**
	 * Value class to its {@code getValue()} method when the class is a driver {@code PGobject}, else {@code null}.
	 */
	@NonNull
	private static final ClassValue<Method> PG_OBJECT_VALUE_READERS = new ClassValue<>() {
		@Override
		@Nullable
		protected Method computeValue(@NonNull Class<?> valueType) {
			if (!isDriverClass(valueType, PG_OBJECT_SIMPLE_CLASS_NAME))
				return null;

			try {
				Method method = valueType.getMethod("getValue");
				return method.getReturnType() == String.class ? method : null;
			} catch (NoSuchMethodException e) {
				return null;
			}
		}
	};

	private PostgresDriverSupport() {}

	/**
	 * Binds {@code value} as a driver {@code PGobject} of SQL type {@code typeName}, using the {@code PGobject}
	 * class belonging to the driver that owns {@code connection}.
	 *
	 * @throws SQLException if the driver exposes no usable {@code PGobject}, or construction fails
	 */
	@NonNull
	static Object newPgObject(@NonNull Connection connection,
														@NonNull String typeName,
														@NonNull String value) throws SQLException {
		requireNonNull(connection);
		requireNonNull(typeName);
		requireNonNull(value);

		Class<?> pgObjectClass = resolvePgObjectClass(connection);
		PgObjectFactory factory = pgObjectClass == null ? null : PG_OBJECT_FACTORIES.get(pgObjectClass);

		if (factory == null)
			throw new SQLException(format(
					"Unable to locate or unambiguously select a PGobject class for the PostgreSQL driver in use (%s). "
							+ "Binding JSON and vector parameters requires pgjdbc or the supported YugabyteDB smart driver "
							+ "with a usable PGobject implementation.", connection.getClass().getName()));

		try {
			Object pgObject = factory.constructor().newInstance();
			factory.typeSetter().invoke(pgObject, typeName);
			factory.valueSetter().invoke(pgObject, value);
			return pgObject;
		} catch (InvocationTargetException e) {
			Throwable targetException = e.getTargetException();

			if (targetException instanceof SQLException sqlException)
				throw sqlException;

			if (targetException instanceof RuntimeException runtimeException)
				throw runtimeException;

			if (targetException instanceof Error error)
				throw error;

			throw new SQLException(format("Unable to construct a %s parameter", typeName), targetException);
		} catch (ReflectiveOperationException | RuntimeException e) {
			throw new SQLException(format("Unable to construct a %s parameter", typeName), e);
		}
	}

	/**
	 * Determines which driver's {@code PGobject} to build for {@code connection}.
	 * <p>
	 * The connection's own class is NOT a reliable signal: a pool or instrumentation layer hands out a
	 * wrapper whose package says nothing about the driver beneath it, and may be a {@link java.lang.reflect.Proxy}
	 * loaded by a class loader that cannot see the driver at all. So the driver is identified the same way
	 * {@link PostgresNotificationAdapter} identifies it - by asking the connection, through
	 * {@link Connection#isWrapperFor(Class)}, which driver's {@code PGConnection} it can produce.
	 */
	@Nullable
	private static Class<?> resolvePgObjectClass(@NonNull Connection connection) {
		requireNonNull(connection);

		List<ClassLoader> classLoaders = candidateClassLoaders(connection);

		// 1. Ask the connection which driver it belongs to, seeing through pool and tracing wrappers.
		for (ClassLoader classLoader : classLoaders)
			for (String driverRootPackage : CANDIDATE_DRIVER_ROOT_PACKAGES) {
				Class<?> driverConnectionApi = loadClass(
						driverRootPackage + "." + PG_CONNECTION_SIMPLE_CLASS_NAME, classLoader);

				if (driverConnectionApi == null || !driverConnectionApi.isInterface())
					continue;

				if (!isWrapperFor(connection, driverConnectionApi))
					continue;

				Class<?> pgObjectClass = loadClass(
						driverRootPackage + "." + UTIL_PACKAGE_SEGMENT + "." + PG_OBJECT_SIMPLE_CLASS_NAME, classLoader);

				if (pgObjectClass != null)
					return pgObjectClass;
			}

		// 2. An unwrapped connection from a fork this build has never heard of: walk its own package.
		Class<?> resolvedFromPackage = resolveFromConnectionPackage(connection.getClass());

		if (resolvedFromPackage != null)
			return resolvedFromPackage;

		// 3. A wrapper that refuses to unwrap. Preserve the legacy single-driver fallback, but do not guess
		// when multiple usable drivers are visible: binding one driver's PGobject through another driver is
		// worse than rejecting the bind explicitly.
		List<Class<?>> visiblePgObjectClasses = new ArrayList<>();

		for (ClassLoader classLoader : classLoaders)
			for (String driverRootPackage : CANDIDATE_DRIVER_ROOT_PACKAGES) {
				Class<?> pgObjectClass = loadClass(
						driverRootPackage + "." + UTIL_PACKAGE_SEGMENT + "." + PG_OBJECT_SIMPLE_CLASS_NAME, classLoader);

				if (pgObjectClass != null && PG_OBJECT_FACTORIES.get(pgObjectClass) != null
						&& !visiblePgObjectClasses.contains(pgObjectClass))
					visiblePgObjectClasses.add(pgObjectClass);
			}

		return visiblePgObjectClasses.size() == 1 ? visiblePgObjectClasses.get(0) : null;
	}

	private static boolean isWrapperFor(@NonNull Connection connection,
																			@NonNull Class<?> driverConnectionApi) {
		requireNonNull(connection);
		requireNonNull(driverConnectionApi);

		try {
			return connection.isWrapperFor(driverConnectionApi);
		} catch (SQLException | RuntimeException | AbstractMethodError e) {
			return false;
		}
	}

	/**
	 * Class loaders to search, most reliable first. Pyranid's own loader sees the driver an application put
	 * on Pyranid's classpath; a proxied connection's loader may see nothing at all.
	 */
	@NonNull
	private static List<@NonNull ClassLoader> candidateClassLoaders(@NonNull Connection connection) {
		requireNonNull(connection);

		List<ClassLoader> classLoaders = new ArrayList<>(3);

		for (ClassLoader classLoader : new ClassLoader[]{
				PostgresDriverSupport.class.getClassLoader(),
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
	 * Extracts the text of a driver {@code PGobject}, or returns {@code null} when {@code value} is not one or its
	 * SQL value is {@code null}.
	 */
	@Nullable
	static String pgObjectValue(@NonNull Object value) {
		requireNonNull(value);

		Method valueReader = PG_OBJECT_VALUE_READERS.get(value.getClass());

		if (valueReader == null)
			return null;

		try {
			return (String) valueReader.invoke(value);
		} catch (InvocationTargetException e) {
			Throwable targetException = e.getTargetException();

			if (targetException instanceof RuntimeException runtimeException)
				throw runtimeException;

			if (targetException instanceof Error error)
				throw error;

			throw new DatabaseException("Unable to extract PostgreSQL PGobject value", targetException);
		} catch (IllegalAccessException | IllegalArgumentException e) {
			throw new DatabaseException("Unable to extract PostgreSQL PGobject value", e);
		}
	}

	/**
	 * Whether {@code value} is a driver {@code PGobject}, regardless of which fork supplied it.
	 */
	@NonNull
	static Boolean isPgObject(@NonNull Object value) {
		requireNonNull(value);
		return PG_OBJECT_VALUE_READERS.get(value.getClass()) != null;
	}

	/**
	 * Whether {@code throwable} is a driver {@code PSQLException}, regardless of which fork supplied it.
	 */
	@NonNull
	static Boolean isPsqlException(@Nullable Throwable throwable) {
		return throwable != null && isDriverClass(throwable.getClass(), PSQL_EXCEPTION_SIMPLE_CLASS_NAME);
	}

	/**
	 * Reads a driver {@code PSQLException}'s {@code ServerErrorMessage}, or {@code null} when unavailable.
	 */
	@Nullable
	static Object serverErrorMessage(@NonNull Throwable throwable) {
		requireNonNull(throwable);

		try {
			return throwable.getClass().getMethod("getServerErrorMessage").invoke(throwable);
		} catch (ReflectiveOperationException | RuntimeException e) {
			return null;
		}
	}

	/**
	 * Invokes a no-argument accessor on a driver {@code ServerErrorMessage}, or returns {@code null}.
	 */
	@Nullable
	static <T> T serverErrorMessageAttribute(@NonNull Object serverErrorMessage,
																					 @NonNull String accessorName,
																					 @NonNull Class<T> attributeType) {
		requireNonNull(serverErrorMessage);
		requireNonNull(accessorName);
		requireNonNull(attributeType);

		try {
			Object attribute = serverErrorMessage.getClass().getMethod(accessorName).invoke(serverErrorMessage);
			return attributeType.isInstance(attribute) ? attributeType.cast(attribute) : null;
		} catch (ReflectiveOperationException | RuntimeException e) {
			return null;
		}
	}

	/**
	 * Resolves the {@code PGobject} class belonging to the driver that owns {@code connectionType}, by walking up
	 * that class's package and looking for a {@code util.PGobject} sibling. {@code org.postgresql.jdbc.PgConnection}
	 * resolves {@code org.postgresql.util.PGobject}; {@code com.yugabyte.jdbc.PgConnection} resolves
	 * {@code com.yugabyte.util.PGobject}.
	 */
	@Nullable
	private static Class<?> resolveFromConnectionPackage(@NonNull Class<?> connectionType) {
		requireNonNull(connectionType);

		ClassLoader classLoader = connectionType.getClassLoader();
		String packageName = connectionType.getPackageName();

		while (!packageName.isEmpty()) {
			Class<?> candidate = loadClass(
					packageName + "." + UTIL_PACKAGE_SEGMENT + "." + PG_OBJECT_SIMPLE_CLASS_NAME, classLoader);

			if (candidate != null)
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
	 * Whether {@code type} has the PostgreSQL-driver utility-class shape used by pgjdbc and the explicitly
	 * supported YugabyteDB smart driver.
	 */
	private static boolean isDriverClass(@NonNull Class<?> type,
																			 @NonNull String simpleClassName) {
		requireNonNull(type);
		requireNonNull(simpleClassName);

		return simpleClassName.equals(type.getSimpleName())
				&& type.getPackageName().endsWith("." + UTIL_PACKAGE_SEGMENT);
	}
}
