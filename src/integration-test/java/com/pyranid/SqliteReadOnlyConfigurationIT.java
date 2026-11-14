/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** The default xerial connection cannot change its open-mode read-only flag after checkout. */
class SqliteReadOnlyConfigurationIT {
	@TempDir Path directory;

	@Test
	void readOnlyTransactionOptionOnDefaultReadWriteSourceFailsBeforePreparingSelect() {
		SQLiteDataSource source = source(this.directory.resolve("default.sqlite"), false);
		Database setup = Database.with(source, DatabaseType.SQLITE).build();
		setup.query("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)").execute();
		setup.query("INSERT INTO items VALUES (1, 'available')").execute();
		AtomicInteger preparations = new AtomicInteger();
		Database database = Database.with(tracking(source, preparations), DatabaseType.SQLITE).build();
		TransactionOutcome<String> outcome = database.transactionWithOutcome(TransactionOptions.withReadOnly(true).build(),
				() -> database.query("SELECT value FROM items WHERE id=1").fetchObject(String.class));
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		DatabaseException failure = assertInstanceOf(DatabaseException.class, outcome.getFailure().orElseThrow());
		assertTrue(failure.getMessage().contains("read-only"), failure::getMessage);
		assertTrue(outcome.getAttemptedValue().isEmpty());
		assertEquals(0, preparations.get(), "The driver rejects setReadOnly before any application SELECT is prepared");
		assertEquals("available", setup.query("SELECT value FROM items WHERE id=1").fetchObject(String.class).orElseThrow());
	}

	@Test
	void separateReadOnlySourceInheritsItsOpenModeAndRejectsWrites() throws Exception {
		Path file = this.directory.resolve("read-only.sqlite");
		Database writer = Database.with(source(file, false), DatabaseType.SQLITE).build();
		writer.query("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)").execute();
		writer.query("INSERT INTO items VALUES (1, 'available')").execute();
		SQLiteDataSource readOnlySource = source(file, true);
		try (Connection connection = readOnlySource.getConnection()) { assertTrue(connection.isReadOnly()); }
		Database reader = Database.with(readOnlySource, DatabaseType.SQLITE).build();
		assertTrue(TransactionOptions.DEFAULT.getReadOnly().isEmpty(), "Default options inherit the source's connection state");
		TransactionOutcome<String> read = reader.transactionWithOutcome(
				() -> reader.query("SELECT value FROM items WHERE id=1").fetchObject(String.class));
		assertEquals(TransactionResult.COMMITTED, read.getResult());
		assertEquals(Optional.of("available"), read.getAttemptedValue());
		assertTrue(read.getFailure().isEmpty());
		TransactionOutcome<Void> write = reader.transactionWithOutcome(() -> {
			reader.query("INSERT INTO items VALUES (2, 'forbidden')").execute();
		});
		assertEquals(TransactionResult.ROLLED_BACK, write.getResult());
		DatabaseException failure = assertInstanceOf(DatabaseException.class, write.getFailure().orElseThrow());
		assertEquals(8, failure.getErrorCode().orElseThrow() & 0xff, "SQLITE_READONLY must be enforced by the opened connection");
		assertEquals(1L, writer.query("SELECT COUNT(*) FROM items").fetchObject(Long.class).orElseThrow());
		assertEquals("available", reader.query("SELECT value FROM items WHERE id=1").fetchObject(String.class).orElseThrow());
	}

	private static SQLiteDataSource source(Path file, boolean readOnly) {
		SQLiteDataSource source = new SQLiteDataSource(); source.setUrl("jdbc:sqlite:" + file); source.setReadOnly(readOnly); return source;
	}

	private static DataSource tracking(DataSource source, AtomicInteger preparations) {
		return (DataSource) Proxy.newProxyInstance(SqliteReadOnlyConfigurationIT.class.getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> {
					if (!method.getName().equals("getConnection")) throw new AssertionError(method.getName());
					Connection connection = source.getConnection();
					return Proxy.newProxyInstance(SqliteReadOnlyConfigurationIT.class.getClassLoader(), new Class<?>[]{Connection.class},
							(borrowed, operation, parameters) -> {
								if (operation.getName().equals("prepareStatement")) preparations.incrementAndGet();
								try { return operation.invoke(connection, parameters); }
								catch (InvocationTargetException failure) { throw failure.getCause(); }
							});
				});
	}
}
