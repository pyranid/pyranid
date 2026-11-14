/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ScriptExecutionRecipesIT {
	@TempDir Path directory;

	static void executeCommands(Database database, List<String> commands) {
		database.transaction(() -> database.useRawConnection(connection -> {
			try (Statement statement = connection.createStatement()) {
				for (String command : commands) statement.executeUpdate(command);
			}
			return Optional.empty();
		}));
	}

	@Test
	void sqliteQueryIsNotAScriptRunnerAndExecutesOnlyTheFirstCommand() {
		Database database = database(DatabaseType.SQLITE);
		database.query("CREATE TABLE item (id INTEGER PRIMARY KEY, label TEXT)").execute();
		database.query("INSERT INTO item VALUES (1, 'first'); INSERT INTO item VALUES (2, 'second');").execute();
		assertEquals(List.of(1), database.query("SELECT id FROM item ORDER BY id").fetchList(Integer.class));
	}

	@Test
	void completeCommandListPreservesLiteralSemicolonsAndRollsBackOnFailure() {
		for (DatabaseType type : List.of(DatabaseType.SQLITE, DatabaseType.DUCK_DB)) {
			Database database = database(type);
			database.query("CREATE TABLE item (id INTEGER PRIMARY KEY, label VARCHAR(50))").execute();
			executeCommands(database, List.of("INSERT INTO item VALUES (1, 'literal;semicolon')", "INSERT INTO item VALUES (2, 'second')"));
			assertEquals(List.of("literal;semicolon", "second"), database.query("SELECT label FROM item ORDER BY id").fetchList(String.class));
			assertThrows(DatabaseException.class, () -> executeCommands(database,
					List.of("INSERT INTO item VALUES (3, 'rolled back')", "INSERT INTO item VALUES (1, 'duplicate')")));
			assertEquals(List.of(1, 2), database.query("SELECT id FROM item ORDER BY id").fetchList(Integer.class));
			executeCommands(database, List.of());
		}
	}

	@Test
	void sqliteTriggerBodyIsOneCompleteCommandEvenThoughItContainsSemicolons() {
		Database database = database(DatabaseType.SQLITE);
		executeCommands(database, List.of("CREATE TABLE item (id INTEGER PRIMARY KEY)", "CREATE TABLE audit (value TEXT)", """
				CREATE TRIGGER audit_insert AFTER INSERT ON item BEGIN
				  INSERT INTO audit VALUES ('first;entry');
				  INSERT INTO audit VALUES ('second');
				END
				""", "INSERT INTO item VALUES (1)"));
		assertEquals(List.of("first;entry", "second"), database.query("SELECT value FROM audit ORDER BY rowid").fetchList(String.class));
	}

	private Database database(DatabaseType type) {
		String path = directory.resolve(UUID.randomUUID().toString()).toString();
		String url = type == DatabaseType.SQLITE ? "jdbc:sqlite:" + path : "jdbc:duckdb:" + path;
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(url, null, null), type).build();
	}
}
