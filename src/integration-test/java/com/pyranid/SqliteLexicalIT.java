/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SqliteLexicalIT {
	@TempDir Path directory;
	@Test
	void flatCommentsAndBracketIdentifiersPreserveTheirSeparateBoundaries() {
		DataSource source = new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:sqlite:" + directory.resolve("lexical.db"), null, null);
		SqlCompilerIntegrationSupport.verifyFlatComments(source, DatabaseType.SQLITE);
		for (int capacity : new int[]{0, 16}) {
			Database database = Database.with(source, DatabaseType.SQLITE).parsedSqlCacheCapacity(capacity).build();
			assertEquals(42, database.query("SELECT [value:ignored] FROM (SELECT :value AS [value:ignored]) "
					+ "/* outer /* inner */ WHERE [value:ignored]=:value").bind("value", 42).fetchObject(Integer.class).orElseThrow());
		}
	}
}
