/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

import java.nio.file.Path;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Uses actual Xerial result codes; messages and the broad constraint code are not uniqueness evidence. */
class SqliteRowIdClassificationIT {
	@TempDir Path directory;
	public record Row(Long rowId, String value) {}

	@Test
	void implicitRowIdInsertAndUpdateCollisionsAreUniqueAndPreserveExistingRows() {
		Database database = database();
		database.query("CREATE TABLE entries (value TEXT NOT NULL)").execute();
		database.query("INSERT INTO entries (rowid, value) VALUES (1, 'one'), (2, 'two')").execute();
		List<Row> expected = List.of(new Row(1L, "one"), new Row(2L, "two"));
		for (String alias : List.of("rowid", "oid", "_rowid_")) {
			DatabaseException insert = assertThrows(DatabaseException.class, () -> database.query(
					"INSERT INTO entries (" + alias + ", value) VALUES (:id, :value)")
					.bind("id", 1L).bind("value", "not stored").execute());
			assertRowIdViolation(insert);
			assertEquals(expected, rows(database));
			DatabaseException update = assertThrows(DatabaseException.class, () -> database.query(
					"UPDATE entries SET " + alias + " = :newId WHERE " + alias + " = :oldId")
					.bind("newId", 1L).bind("oldId", 2L).execute());
			assertRowIdViolation(update);
			assertEquals(expected, rows(database));
		}
	}

	@Test
	void unhandledRowIdCollisionRollsBackAttemptedRowsAndLeavesTheDatabaseUsable() {
		Database database = database();
		database.query("CREATE TABLE entries (value TEXT NOT NULL)").execute();
		database.query("INSERT INTO entries (rowid, value) VALUES (1, 'existing')").execute();
		DatabaseException failure = assertThrows(DatabaseException.class, () -> database.transaction(() -> {
			database.query("INSERT INTO entries (rowid, value) VALUES (3, 'attempted')").execute();
			database.query("INSERT INTO entries (rowid, value) VALUES (1, 'duplicate')").execute();
		}));
		assertRowIdViolation(failure);
		assertEquals(List.of(new Row(1L, "existing")), rows(database));
		database.query("INSERT INTO entries (rowid, value) VALUES (2, 'later')").execute();
		assertEquals(List.of(new Row(1L, "existing"), new Row(2L, "later")), rows(database));
	}

	@Test
	void handledRowIdCollisionDoesNotChangeSqlitesUsableTransactionBehavior() {
		Database database = database();
		database.query("CREATE TABLE entries (value TEXT NOT NULL)").execute();
		database.query("INSERT INTO entries (rowid, value) VALUES (1, 'existing')").execute();
		database.transaction(() -> {
			database.query("INSERT INTO entries (rowid, value) VALUES (2, 'before')").execute();
			assertRowIdViolation(assertThrows(DatabaseException.class,
					() -> database.query("INSERT INTO entries (rowid, value) VALUES (1, 'duplicate')").execute()));
			database.query("INSERT INTO entries (rowid, value) VALUES (3, 'after')").execute();
		});
		assertEquals(List.of(new Row(1L, "existing"), new Row(2L, "before"), new Row(3L, "after")), rows(database));
	}

	@Test
	void realRowIdResultCodesSurviveCauseNextAndCyclicJdbcWrappers() {
		assertUnique(classify(new SQLiteException("rowid collision", SQLiteErrorCode.SQLITE_CONSTRAINT_ROWID)));
		assertUnique(classify(new SQLException("wrapper", null, 19,
				new SQLiteException("rowid collision", SQLiteErrorCode.SQLITE_CONSTRAINT_ROWID))));
		assertUnique(classify(new IllegalStateException("wrapper",
				new SQLiteException("rowid collision", SQLiteErrorCode.SQLITE_CONSTRAINT_ROWID))));
		BatchUpdateException batch = new BatchUpdateException("batch wrapper", null, 19, new int[0]);
		SQLiteException rowId = new SQLiteException("rowid collision", SQLiteErrorCode.SQLITE_CONSTRAINT_ROWID);
		batch.setNextException(rowId);
		rowId.setNextException(batch);
		assertUnique(classify(batch));
	}

	@Test
	void unrelatedConstraintCodesProseAndSuppressedCleanupDoNotBecomeUniqueViolations() {
		for (SQLiteErrorCode code : List.of(SQLiteErrorCode.SQLITE_CONSTRAINT, SQLiteErrorCode.SQLITE_CONSTRAINT_CHECK,
				SQLiteErrorCode.SQLITE_CONSTRAINT_NOTNULL, SQLiteErrorCode.SQLITE_CONSTRAINT_FOREIGNKEY,
				SQLiteErrorCode.SQLITE_CONSTRAINT_TRIGGER, SQLiteErrorCode.SQLITE_CONSTRAINT_DATATYPE)) {
			DatabaseException failure = classify(new SQLiteException("SQLITE_CONSTRAINT_ROWID: UNIQUE constraint failed", code));
			assertFalse(failure.isUniqueConstraintViolation(), code.name());
			assertEquals(code == SQLiteErrorCode.SQLITE_CONSTRAINT_FOREIGNKEY, failure.isForeignKeyViolation(), code.name());
		}
		for (int code : new int[]{19, SQLiteErrorCode.SQLITE_CONSTRAINT_ROWID.code})
			assertFalse(classify(new SQLException("SQLITE_CONSTRAINT_ROWID: UNIQUE constraint failed", null, code))
					.isUniqueConstraintViolation(), "Generic numeric codes/prose are not a driver result enum");
		SQLiteException check = new SQLiteException("check violation", SQLiteErrorCode.SQLITE_CONSTRAINT_CHECK);
		check.addSuppressed(new SQLiteException("cleanup", SQLiteErrorCode.SQLITE_CONSTRAINT_ROWID));
		assertFalse(classify(check).isUniqueConstraintViolation());
		for (SQLiteErrorCode code : List.of(SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY, SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE))
			assertUnique(classify(new SQLiteException("existing unique classification", code)));
	}

	@Test
	void realCheckNotNullAndForeignKeyErrorsRetainTheirSpecificClassifications() {
		Database database = database();
		database.query("CREATE TABLE parent (id INTEGER PRIMARY KEY)").execute();
		database.query("INSERT INTO parent VALUES (1)").execute();
		database.query("CREATE TABLE guarded (id INTEGER PRIMARY KEY, required TEXT NOT NULL, "
				+ "flag INTEGER CHECK (flag IN (0, 1)), parent_id INTEGER REFERENCES parent(id))").execute();
		database.query("INSERT INTO guarded VALUES (1, 'existing', 1, 1)").execute();
		List<String> statements = List.of("INSERT INTO guarded VALUES (2, NULL, 1, 1)",
				"INSERT INTO guarded VALUES (2, 'check', 2, 1)", "INSERT INTO guarded VALUES (2, 'foreign', 1, 99)");
		List<SQLiteErrorCode> expected = List.of(SQLiteErrorCode.SQLITE_CONSTRAINT_NOTNULL,
				SQLiteErrorCode.SQLITE_CONSTRAINT_CHECK, SQLiteErrorCode.SQLITE_CONSTRAINT_FOREIGNKEY);
		for (int index = 0; index < statements.size(); index++) {
			String sql = statements.get(index);
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query(sql).execute());
			assertEquals(expected.get(index), assertInstanceOf(SQLiteException.class, failure.getCause()).getResultCode());
			assertFalse(failure.isUniqueConstraintViolation());
			assertEquals(index == 2, failure.isForeignKeyViolation());
			assertEquals(List.of(1L), database.query("SELECT id FROM guarded").fetchList(Long.class));
		}
	}

	private Database database() {
		SQLiteConfig config = new SQLiteConfig();
		config.enforceForeignKeys(true);
		SQLiteDataSource source = new SQLiteDataSource(config);
		source.setUrl("jdbc:sqlite:" + directory.resolve(UUID.randomUUID() + ".db"));
		return Database.with(source, DatabaseType.SQLITE).build();
	}

	private static List<Row> rows(Database database) {
		return database.query("SELECT rowid AS row_id, value FROM entries ORDER BY rowid").fetchList(Row.class);
	}

	private static DatabaseException classify(Throwable cause) {
		return new DatabaseException("SQLite operation failed", cause, DatabaseType.SQLITE.dialect());
	}

	private static void assertRowIdViolation(DatabaseException failure) {
		assertEquals(SQLiteErrorCode.SQLITE_CONSTRAINT_ROWID,
				assertInstanceOf(SQLiteException.class, failure.getCause()).getResultCode());
		assertUnique(failure);
	}

	private static void assertUnique(DatabaseException failure) {
		assertTrue(failure.isUniqueConstraintViolation());
		assertFalse(failure.isForeignKeyViolation());
		assertFalse(failure.isTransient());
		assertFalse(failure.isSerializationFailure());
		assertFalse(failure.isDeadlock());
		assertFalse(failure.isTimeout());
	}
}
