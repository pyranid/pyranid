/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Optional Connector/J inspection without a runtime driver dependency or retained driver classes. */
final class MySqlDriverSupport {
	private MySqlDriverSupport() {}

	/** Only positive driver evidence permits a positive fetch size instead of row-by-row streaming. */
	static boolean cursorFetchEnabled(PreparedStatement statement) throws SQLException {
		Connection connection = statement.getConnection();
		if (connection == null) return false;
		List<ClassLoader> loaders = new ArrayList<>(4);
		addLoader(loaders, statement.getClass().getClassLoader());
		addLoader(loaders, connection.getClass().getClassLoader());
		addLoader(loaders, MySqlDriverSupport.class.getClassLoader());
		try { addLoader(loaders, Thread.currentThread().getContextClassLoader()); }
		catch (Throwable unavailable) { rethrowTerminal(unavailable); }

		for (ClassLoader loader : loaders) {
			try {
				Class<?> serverStatementApi = Class.forName("com.mysql.cj.jdbc.ServerPreparedStatement", false, loader);
				Object serverStatement = serverStatementApi.isInstance(statement) ? statement
						: statement.isWrapperFor(serverStatementApi) ? statement.unwrap(serverStatementApi) : null;
				if (!serverStatementApi.isInstance(serverStatement)) continue;
				Class<?> api = Class.forName("com.mysql.cj.jdbc.JdbcConnection", false, loader);
				if (!api.isInterface()) continue;
				Object driver = api.isInstance(connection) ? connection
						: connection.isWrapperFor(api) ? connection.unwrap(api) : null;
				if (!api.isInstance(driver)) continue;
				Method propertiesReader = api.getMethod("getPropertySet");
				Object properties = propertiesReader.invoke(driver);
				if (properties == null) return false;
				Method propertyReader = propertiesReader.getReturnType().getMethod("getBooleanProperty", String.class);
				Object property = propertyReader.invoke(properties, "useCursorFetch");
				if (property == null) return false;
				Object value = propertyReader.getReturnType().getMethod("getValue").invoke(property);
				return Boolean.TRUE.equals(value);
			} catch (Throwable unavailable) {
				Throwable failure = unavailable instanceof InvocationTargetException invocation
						? invocation.getTargetException() : unavailable;
				rethrowTerminal(failure);
				// An opaque/older wrapper or an unavailable optional property keeps the bounded sentinel path.
			}
		}
		return false;
	}

	private static void addLoader(List<ClassLoader> loaders, ClassLoader loader) {
		if (loader != null && !loaders.contains(loader)) loaders.add(loader);
	}

	private static void rethrowTerminal(Throwable failure) throws SQLException {
		if (!JdbcFallbacks.isTerminal(failure)) return;
		if (failure instanceof SQLException sqlFailure) throw sqlFailure;
		if (failure instanceof RuntimeException runtimeFailure) throw runtimeFailure;
		if (failure instanceof Error error) throw error;
		throw new SQLException("Unable to inspect MySQL cursor configuration", failure);
	}
}
