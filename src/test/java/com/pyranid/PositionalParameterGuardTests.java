/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PositionalParameterGuardTests {
	@Test
	void everyNonPostgresDialectRejectsStrayPlaceholdersBeforeAcquiringAConnection() {
		for (DatabaseType type : DatabaseType.values()) {
			if (type == DatabaseType.POSTGRESQL) continue;
			Capture capture = new Capture();
			Database database = Database.with(capture.source(), type).build();
			for (int repeat = 0; repeat < 2; repeat++) {
				for (String sql : List.of(
						"SELECT 'Hello, ' || ? AS greeting, :id AS user_id",
						"UPDATE work SET label = 'Dr. ' || ? || label WHERE id=:id",
						"UPDATE work SET label=:label WHERE label LIKE '%' || ? || '%' AND id=:id",
						"INSERT INTO work (id, label, note) VALUES (:id, 'x' || ? || 'y', :label)",
						"SELECT data ? :id FROM work", "SELECT data ?| ARRAY[:id] FROM work")) {
					IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> {
						Query query = database.query(sql).bind("id", 1);
						if (sql.contains(":label")) query.bind("label", "value");
						query.execute();
					}, type + " " + sql);
					assertTrue(failure.getMessage().contains("Positional parameters"));
					assertTrue(failure.getMessage().contains("named parameters"));
				}
			}
			assertEquals(0, capture.connections.get(), type.name());
			assertTrue(capture.sql.isEmpty());
		}
	}

	@Test
	void questionMarksInsideProtectedTextDoNotCountAsPositionalParameters() {
		for (DatabaseType type : DatabaseType.values()) {
			Capture capture = new Capture();
			Database database = Database.with(capture.source(), type).build();
			String sql = "UPDATE work SET label='? :ignored '' still?', \"column?\"=:label /* ? :ignored */ WHERE id=:id -- ? :ignored\n";
			assertEquals(1L, database.query(sql).bind("label", "value").bind("id", 7).execute());
			assertEquals("UPDATE work SET label='? :ignored '' still?', \"column?\"=? /* ? :ignored */ WHERE id=? -- ? :ignored\n", capture.sql.get(0));
			assertEquals(List.of("value", 7), capture.values);
		}
	}

	@Test
	void guardUsesTheSelectedLexicalVariantForCommentsAndQuotedIdentifiers() {
		for (DatabaseType type : List.of(DatabaseType.MYSQL, DatabaseType.MARIA_DB)) {
			Capture capture = new Capture();
			Database database = Database.with(capture.source(), type).build();
			assertEquals(1L, database.query("UPDATE work SET label=:label # ? :ignored\n WHERE id=:id")
					.bind("label", "value").bind("id", 7).execute());
			assertEquals(List.of("value", 7), capture.values);
		}
		for (DatabaseType type : List.of(DatabaseType.SQL_SERVER, DatabaseType.SQLITE)) {
			Capture capture = new Capture();
			Database database = Database.with(capture.source(), type).build();
			assertEquals(1L, database.query("UPDATE [work? :ignored] SET [label?]=:label WHERE id=:id")
					.bind("label", "value").bind("id", 7).execute());
			assertEquals(List.of("value", 7), capture.values);
		}
		Capture capture = new Capture();
		Database database = Database.with(capture.source(), DatabaseType.ORACLE).build();
		assertEquals(1L, database.query("UPDATE work SET label=q'[? :ignored]' WHERE id=:id").bind("id", 7).execute());
		assertEquals(List.of(7), capture.values);
	}

	@Test
	void postgresQuestionMarkOperatorsAreEscapedOnceWhileNamedSlotsKeepTheirOrder() {
		Capture capture = new Capture();
		Database database = Database.with(capture.source(), DatabaseType.POSTGRESQL).build();
		String sql = "SELECT :data::jsonb /* ? in comment */ ? :key, :data::jsonb ?| ARRAY[:first], :data::jsonb ?& ARRAY[:second], '?'";
		for (int repeat = 0; repeat < 2; repeat++) {
			capture.values.clear();
			assertEquals(1L, database.query(sql).bind("data", "{\"a\":1}").bind("key", "a")
					.bind("first", "a").bind("second", "a").execute());
			assertEquals("SELECT ?::jsonb /* ? in comment */ ?? ?, ?::jsonb ??| ARRAY[?], ?::jsonb ??& ARRAY[?], '?'", capture.sql.get(repeat));
			assertEquals(List.of("{\"a\":1}", "a", "{\"a\":1}", "a", "{\"a\":1}", "a"), capture.values);
		}
	}

	@Test
	void batchAndReturningEntrypointsRejectBeforeJdbcAndDoNotRunCustomizers() {
		Capture capture = new Capture();
		Database database = Database.with(capture.source(), DatabaseType.GENERIC).build();
		AtomicInteger customizers = new AtomicInteger();
		String sql = "UPDATE work SET label='x' || ? || :label WHERE id=:id";
		assertThrows(IllegalArgumentException.class, () -> database.query(sql).customize((context, statement) -> customizers.incrementAndGet())
				.executeBatch(List.of(Map.of("label", "value", "id", 1))));
		assertThrows(IllegalArgumentException.class, () -> database.query(sql + " RETURNING id").bind("label", "value").bind("id", 1)
				.executeForObject(Integer.class));
		assertEquals(0, capture.connections.get());
		assertEquals(0, customizers.get());
	}

	private static final class Capture {
		final AtomicInteger connections = new AtomicInteger();
		final List<String> sql = new ArrayList<>();
		final List<Object> values = new ArrayList<>();
		DataSource source() {
			return proxy(DataSource.class, (method, args) -> {
				if (!method.getName().equals("getConnection")) return null;
				connections.incrementAndGet();
				return proxy(Connection.class, (jdbc, parameters) -> {
					if (!jdbc.getName().equals("prepareStatement")) return null;
					sql.add((String) parameters[0]);
					return proxy(PreparedStatement.class, (call, supplied) -> {
						if (call.getName().equals("setObject") || call.getName().equals("setString")) values.add(supplied[1]);
						if (call.getName().equals("executeLargeUpdate")) return 1L;
						if (call.getName().equals("executeUpdate")) return 1;
						return null;
					});
				});
			});
		}
	}
	@FunctionalInterface private interface Call { Object invoke(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
			Object result = call.invoke(method, args);
			if (result != null || !method.getReturnType().isPrimitive()) return result;
			if (method.getReturnType() == boolean.class) return false;
			if (method.getReturnType() == int.class) return 0;
			if (method.getReturnType() == long.class) return 0L;
			return null;
		}));
	}
}
