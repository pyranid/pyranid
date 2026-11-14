/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import javax.sql.DataSource;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class SqlCompilerIntegrationSupport {
	public record QuotedRow(String textValue, Integer id) {}
	private SqlCompilerIntegrationSupport() {}

	static void verifyFlatComments(DataSource source, DatabaseType type) {
		String suffix = type == DatabaseType.ORACLE ? " FROM dual" : "";
		for (int capacity : new int[]{0, 16}) {
			Database database = Database.with(source, type).parsedSqlCacheCapacity(capacity).build();
			for (int pass = 0; pass < 2; pass++)
				assertEquals(7 + pass, database.query("SELECT :left /* outer /* inner :ignored ? */ - :right" + suffix)
						.bind("left", 10 + pass).bind("right", 3).fetchObject(Integer.class).orElseThrow());
		}
	}

	static void verifyNestedComments(DataSource source, DatabaseType type) {
		for (int capacity : new int[]{0, 16}) {
			Database database = Database.with(source, type).parsedSqlCacheCapacity(capacity).build();
			for (int pass = 0; pass < 2; pass++)
				assertEquals(7 + pass, database.query("SELECT :left /* outer :ignored /* inner ? :hidden */ still :ignored */ - :right")
						.bind("left", 10 + pass).bind("right", 3).fetchObject(Integer.class).orElseThrow());
		}
	}

	static void verifyMySqlQuotes(DataSource source, DatabaseType type) {
		for (int capacity : new int[]{0, 16}) {
			Database database = Database.with(source, type).parsedSqlCacheCapacity(capacity).build();
			String mode = database.query("SELECT @@SESSION.sql_mode").fetchObject(String.class).orElseThrow();
			assertFalse(mode.contains("ANSI_QUOTES"), "This regression checks the driver's default double-quoted strings");
			assertFalse(mode.contains("NO_BACKSLASH_ESCAPES"), "This regression checks default backslash escaping");
			for (String[] sample : List.of(new String[]{"\"x\\\"y :ignored ? /*\"", "x\"y :ignored ? /*"},
					new String[]{"\"x\"\"y :ignored ?\"", "x\"y :ignored ?"},
					new String[]{"\"path" + "\\\\" + "\"", "path\\"})) {
				for (int pass = 0; pass < 2; pass++)
					assertEquals(new QuotedRow(sample[1], 41 + pass), database.query("SELECT " + sample[0] + " AS text_value, :id AS id")
							.bind("id", 41 + pass).fetchObject(QuotedRow.class).orElseThrow());
			}
		}
	}

	static void verifyOracleNationalQuotes(DataSource source) {
		for (int capacity : new int[]{0, 16}) {
			Database database = Database.with(source, DatabaseType.ORACLE).parsedSqlCacheCapacity(capacity).build();
			for (String literal : List.of("nq'[it's :ignored ? /* café]'", "NQ'{it's :ignored ? /* café}'",
					"nQ'(it's :ignored ? /* café)'", "Nq'<it's :ignored ? /* café>'", "nq'!it's :ignored ? /* café!'"))
				for (int pass = 0; pass < 2; pass++)
					assertEquals(new QuotedRow("it's :ignored ? /* café", 41 + pass), database.query("SELECT " + literal + " AS text_value, :id AS id FROM dual")
							.bind("id", 41 + pass).fetchObject(QuotedRow.class).orElseThrow());
		}
	}
}
