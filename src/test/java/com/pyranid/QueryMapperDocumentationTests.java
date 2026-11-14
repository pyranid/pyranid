/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class QueryMapperDocumentationTests {

	@TempDir Path directory;

	@Test
	void actualPerQueryMapperJavadocCompilesOnJava17AndMapsTheJoinProjection() throws Exception {
		String source = Files.readString(Path.of("src/main/java/com/pyranid/Query.java"));
		int method = source.indexOf("Overrides the {@link Database}-wide {@link ResultSetMapper}");
		int start = source.indexOf("<pre>{@code", method) + "<pre>{@code".length();
		int end = source.indexOf("}</pre>", start);
		assertTrue(method >= 0 && start > method && end > start, "The mapper example must remain present");
		String example = source.substring(start, end).replaceAll("(?m)^\\s*\\* ?", "").strip();
		String compilationUnit = """
				import com.pyranid.*;
				import java.sql.*;
				import java.util.*;
				public class QueryMapperExample {
				  public record NameCount(String name, long total) {}
				  public static List<NameCount> run(Database database) {
				    return %s
				  }
				}
				""".formatted(example);
		Path file = directory.resolve("QueryMapperExample.java");
		Files.writeString(file, compilationUnit);
		ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();
		int exit = ToolProvider.getSystemJavaCompiler().run(null, diagnostics, diagnostics,
				"--release", "17", "-classpath", System.getProperty("java.class.path"), "-d", directory.toString(), file.toString());
		assertEquals(0, exit, diagnostics::toString);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl("jdbc:hsqldb:mem:query_mapper_doc_" + UUID.randomUUID());
		dataSource.setUser("sa"); dataSource.setPassword("");
		Database database = Database.with(dataSource, DatabaseType.GENERIC).build();
		database.query("CREATE TABLE employee (name VARCHAR(40))").execute();
		database.query("INSERT INTO employee VALUES ('Ada'), ('Ada'), ('Grace')").execute();
		try (URLClassLoader loader = new URLClassLoader(new java.net.URL[]{directory.toUri().toURL()}, getClass().getClassLoader())) {
			Class<?> compiled = loader.loadClass("QueryMapperExample");
			List<?> rows = (List<?>) compiled.getMethod("run", Database.class).invoke(null, database);
			assertEquals(2, rows.size());
			for (Object row : rows) {
				String name = (String) row.getClass().getMethod("name").invoke(row);
				long total = (long) row.getClass().getMethod("total").invoke(row);
				assertTrue(name.equals("Ada") || name.equals("Grace"));
				assertEquals(name.equals("Ada") ? 2 : 1, total);
			}
		}
		assertEquals(3L, database.query("SELECT COUNT(*) FROM employee").fetchObject(Long.class).orElseThrow());
	}
}
