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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbLexicalIT {
	@TempDir Path directory;
	@Test
	void nestedCommentsAndListSyntaxKeepActualParametersVisible() {
		DataSource source = new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:duckdb:" + directory.resolve("lexical.duckdb"), null, null);
		SqlCompilerIntegrationSupport.verifyNestedComments(source, DatabaseType.DUCK_DB);
		for (int capacity : new int[]{0, 16}) {
			Database database = Database.with(source, DatabaseType.DUCK_DB).parsedSqlCacheCapacity(capacity).build();
			assertEquals(List.of(3, 7), database.query("SELECT unnest([:first, /* outer /* inner :ignored */ still :hidden */ :second])")
					.bind("first", 3).bind("second", 7).fetchList(Integer.class));
			assertEquals(List.of(1, 3), database.query("SELECT i FROM range(4) AS series(i) WHERE i IN (:ids) ORDER BY i")
					.bind("ids", Parameters.inList(List.of(1, 3))).fetchList(Integer.class));
		}
	}
}
