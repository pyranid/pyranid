/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.pyranid;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;

/** Child-loaded application fixture; its callers retain only JDK values and weak loader references. */
public final class MappingRetentionApplication {
	public record Row(Long id) {}

	public static String exercise(String mode, DataSource source) throws Exception {
		if (mode.equals("binder")) {
			Database[] owner = new Database[1];
			CustomParameterBinder callback = new CustomParameterBinder() {
				@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(String.class); }
				@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement,
						Integer index, Object value) throws java.sql.SQLException {
					if (owner[0] == null) throw new AssertionError("Custom binder lost its Database");
					statement.setString(index, value.toString());
					return BindingResult.handled();
				}
			};
			PreparedStatementBinder binder = PreparedStatementBinder.withCustomParameterBinders(List.of(callback));
			Database database = Database.with(source, DatabaseType.GENERIC).preparedStatementBinder(binder).build();
			owner[0] = database;
			bind(binder, database);
			return mode;
		}
		if (mode.equals("pgobject")) {
			Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
					new Class<?>[]{Connection.class}, (proxy, method, arguments) -> {
						if (method.getName().equals("isWrapperFor"))
							return ((Class<?>) arguments[0]).getName().equals("org.postgresql.PGConnection");
						throw new AssertionError("Unexpected driver access: " + method);
					});
			Object value = PostgresDriverSupport.newPgObject(connection, "jsonb", "{}");
			if (value.getClass().getClassLoader() == MappingRetentionApplication.class.getClassLoader())
				throw new AssertionError("The driver must remain parent-loaded for this retention control");
			if (!"{}".equals(value.toString())) throw new AssertionError("PGobject construction failed");
			return mode;
		}
		ResultSetMapper mapper = mode.equals("failure")
				? ResultSetMapper.withCustomColumnMappers(List.of(new CustomColumnMapper() {
					@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(Long.class); }
					@Override public MappingResult map(StatementContext<?> context, ResultSet result, Object value,
							TargetType type, Integer index, String label, InstanceProvider instances) {
						throw new IllegalStateException("expected mapping failure");
					}
				})).build()
				: ResultSetMapper.withDefaultConfiguration();
		Database database = Database.with(source, DatabaseType.GENERIC).resultSetMapper(mapper).build();
		if (mode.equals("construct")) return mode;
		if (mode.equals("long")) map(database, Long.class, 1, "id");
		else if (mode.equals("string")) map(database, String.class, "value", "id");
		else if (mode.equals("record")) {
			Row row = map(database, Row.class, 1, "id");
			// Record.equals would populate the JDK ObjectMethods singleton's last MethodHandle adaptation cache.
			if (!Long.valueOf(1L).equals(row.id())) throw new AssertionError("Record conversion failed");
		} else if (mode.equals("failure")) {
			try {
				map(database, Row.class, 1, "id");
				throw new AssertionError("Expected mapper failure");
			} catch (DatabaseException expected) {
				if (!(expected.getCause() instanceof IllegalStateException)) throw expected;
			}
		} else throw new AssertionError("Unknown retention mode " + mode);
		return mode;
	}

	static void bind(PreparedStatementBinder binder, Database database) throws Exception {
		StatementContext<?> context = StatementContext.with(Statement.of("binder-retention", "SELECT :value"), database).build();
		try (PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
				new Class<?>[]{PreparedStatement.class}, (proxy, method, arguments) -> switch (method.getName()) {
					case "setString" -> {
						if (!"value".equals(arguments[1])) throw new AssertionError("Incorrect bound value");
						yield null;
					}
					case "close" -> null;
					default -> throw new AssertionError("Unexpected binding call: " + method);
				})) {
			binder.bindParameter(context, statement, 1, "value");
		}
	}

	private static <T> T map(Database database, Class<T> type, Object value, String label) throws Exception {
		StatementContext<T> context = StatementContext.<T>with(Statement.of("retention", "SELECT " + label), database)
				.resultSetRowType(type).build();
		try (ResultSet result = cursor(value, label)) {
			return database.getResultSetMapper().map(context, result, type, database.getInstanceProvider()).orElseThrow();
		}
	}

	static ResultSet cursor(Object value, String label) {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class}, (proxy, method, arguments) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnLabel", "getColumnName" -> label;
					case "getColumnType" -> value instanceof String ? Types.VARCHAR : Types.INTEGER;
					case "getColumnTypeName" -> value instanceof String ? "VARCHAR" : "INTEGER";
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == arguments[0];
					case "toString" -> "retention metadata";
					default -> throw new AssertionError("Unexpected metadata access: " + method);
				});
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, arguments) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject" -> value;
					case "getLong" -> ((Number) value).longValue();
					case "getString" -> value.toString();
					case "wasNull" -> false;
					case "close" -> null;
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == arguments[0];
					case "toString" -> "retention cursor";
					default -> throw new AssertionError("Unexpected cursor access: " + method);
				});
	}
}
