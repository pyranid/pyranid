/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PositionalParameterGuardIT {
	@TempDir Path directory;

	@Test
	void sqliteRejectsStraySlotsWithoutExecutingOrShiftingNamedBindings() {
		verify("jdbc:sqlite:" + directory.resolve("positional.sqlite"), DatabaseType.SQLITE);
	}

	@Test
	void duckDbRejectsStraySlotsWithoutExecutingOrShiftingNamedBindings() {
		verify("jdbc:duckdb:" + directory.resolve("positional.duckdb"), DatabaseType.DUCK_DB);
	}

	private static void verify(String url, DatabaseType type) {
		DataSource physical = new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(url, null, null);
		AtomicInteger connections = new AtomicInteger();
		DataSource counted = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
			if (method.getName().equals("getConnection")) connections.incrementAndGet();
			try { return method.invoke(physical, args); }
			catch (InvocationTargetException failure) { throw failure.getCause(); }
		});
		Database database = Database.with(counted, type).build();
		database.query("CREATE TABLE work (id INTEGER PRIMARY KEY, name VARCHAR(100))").execute();
		database.query("INSERT INTO work VALUES (1,'Ada'),(2,'Grace')").execute();
		int before = connections.get();

		assertThrows(IllegalArgumentException.class, () -> database.query("UPDATE work SET name='Dr. ' || ? || name WHERE id=:id")
				.bind("id", 1).execute());
		assertThrows(IllegalArgumentException.class, () -> database.query("INSERT INTO work VALUES (:id, 'x' || ? || :suffix)")
				.bind("id", 3).bind("suffix", "y").execute());
		assertThrows(IllegalArgumentException.class, () -> database.query("UPDATE work SET name='Dr. ' || ? || name WHERE id=:id")
				.executeBatch(List.of(Map.of("id", 1), Map.of("id", 2))));
		assertThrows(IllegalArgumentException.class, () -> database.query("SELECT 'Hello, ' || ? AS greeting, :id AS user_id")
				.bind("id", 1).fetchList(Query.mapRowType()));
		assertEquals(before, connections.get(), "Rejection must occur before acquiring a JDBC connection");
		assertEquals(List.of(1, 2), database.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class));
		assertEquals(List.of("Ada", "Grace"), database.query("SELECT name FROM work ORDER BY id").fetchList(String.class));

		assertEquals(1L, database.query("UPDATE work SET name=:name WHERE id=:id")
				.bind("name", "Dr. Ada").bind("id", 1).execute());
		assertEquals("Dr. Ada", database.query("SELECT name FROM work WHERE id=:id").bind("id", 1).fetchObject(String.class).orElseThrow());
		assertEquals("? Dr. Ada", database.query("SELECT '? ' || name FROM work WHERE id=:id /* ? :ignored */")
				.bind("id", 1).fetchObject(String.class).orElseThrow());
	}
}
