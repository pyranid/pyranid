/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SqlCompilerDialectTests {
	@Test
	void blockCommentsEndAtTheFirstCloserOnlyInNonNestingModes() {
		String sql = "SELECT /* outer /* inner :ignored ? */ :value";
		for (SqlCompiler.SqlLexicalMode mode : List.of(SqlCompiler.SqlLexicalMode.MYSQL,
				SqlCompiler.SqlLexicalMode.NON_NESTING, SqlCompiler.SqlLexicalMode.NON_NESTING_BRACKET_IDENTIFIERS))
			assertEquals(List.of("value"), SqlCompiler.parseNamedParameterSql(sql, mode).parameterNames, mode.name());
		for (SqlCompiler.SqlLexicalMode mode : List.of(SqlCompiler.SqlLexicalMode.STANDARD,
				SqlCompiler.SqlLexicalMode.DUCK_DB, SqlCompiler.SqlLexicalMode.BRACKET_IDENTIFIERS)) {
			IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> SqlCompiler.parseNamedParameterSql(sql, mode));
			assertTrue(failure.getMessage().contains("Unterminated block comment"));
		}
	}

	@Test
	void configuredDatabasesSelectTheRightCommentVariantWithAndWithoutCaching() {
		String sql = "UPDATE work SET value=:value /* outer /* inner :ignored ? */ WHERE id=:id";
		for (int capacity : new int[]{0, 16}) {
			for (DatabaseType type : DatabaseType.values()) {
				Capture capture = new Capture();
				Database database = Database.with(capture.source(), type).parsedSqlCacheCapacity(capacity).build();
				for (int pass = 0; pass < 2; pass++) {
					if (List.of(DatabaseType.POSTGRESQL, DatabaseType.DUCK_DB, DatabaseType.SQL_SERVER).contains(type)) {
						IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
								() -> database.query(sql).bind("value", "manual").bind("id", 7).execute());
						assertTrue(failure.getMessage().contains("Unterminated block comment"));
						assertEquals(0, capture.connections.get());
					} else {
						capture.values.clear();
						assertEquals(1L, database.query(sql).bind("value", "manual").bind("id", 7).execute());
						assertEquals(List.of("manual", 7), capture.values, type.name());
						assertEquals("UPDATE work SET value=? /* outer /* inner :ignored ? */ WHERE id=?", capture.sql.get(pass));
					}
				}
			}
		}
	}

	@Test
	void variantsKeepDistinctNamedBindingsAndDoNotHidePlaceholdersAfterAFlatComment() {
		String sql = "UPDATE work SET value=:value /* outer /* inner */ , other=:flat -- */\n WHERE id=:id";
		for (int capacity : new int[]{0, 16}) {
			SqlCompiler compiler = new SqlCompiler(capacity);
			for (int pass = 0; pass < 2; pass++) {
				SqlCompiler.ParsedSqlVariants parsed = compiler.compile(sql);
				assertEquals(List.of("value", "id"), parsed.standardParsedSql.parameterNames);
				assertEquals(List.of("value", "flat", "id"), parsed.nonNestingParsedSql.parameterNames);
				assertEquals(List.of("value", "flat", "id"), parsed.mysqlParsedSql.parameterNames);
			}
			Capture capture = new Capture();
			Database database = Database.with(capture.source(), DatabaseType.POSTGRESQL).parsedSqlCacheCapacity(capacity).build();
			assertThrows(IllegalArgumentException.class,
					() -> database.query(sql).bind("value", "v").bind("flat", "not-selected").bind("id", 1).execute());
			assertEquals(0, capture.connections.get());
		}
		for (DatabaseType type : List.of(DatabaseType.GENERIC, DatabaseType.MYSQL, DatabaseType.MARIA_DB, DatabaseType.ORACLE, DatabaseType.SQLITE)) {
			Capture capture = new Capture();
			Database database = Database.with(capture.source(), type).build();
			assertThrows(IllegalArgumentException.class,
					() -> database.query("UPDATE work SET value=:value /* outer /* inner */ WHERE id=?").bind("value", "v").execute());
			assertEquals(0, capture.connections.get(), type.name());
		}
	}

	@Test
	void sqliteKeepsBracketIdentifiersAlongsideItsNonNestingCommentRules() {
		Capture capture = new Capture();
		Database database = Database.with(capture.source(), DatabaseType.SQLITE).build();
		String sql = "UPDATE [work/* :ignored] SET [value?]=:value /* outer /* inner */ WHERE [key:ignored]=:id";
		assertEquals(1L, database.query(sql).bind("value", "v").bind("id", 3).execute());
		assertEquals(List.of("v", 3), capture.values);
		assertEquals("UPDATE [work/* :ignored] SET [value?]=? /* outer /* inner */ WHERE [key:ignored]=?", capture.sql.get(0));
	}

	@Test
	void genericHsqlExecutionUsesTheFirstCommentCloser() {
		for (int capacity : new int[]{0, 16}) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:flat_comment_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
			Database database = Database.with(source, DatabaseType.GENERIC).parsedSqlCacheCapacity(capacity).build();
			assertEquals(42, database.query("VALUES /* outer /* inner :ignored ? */ (CAST(:value AS INTEGER))").bind("value", 42).fetchObject(Integer.class).orElseThrow());
		}
	}

	@Test
	void mysqlDoubleQuotedStringsSkipEscapedQuotesWithoutExposingLiteralParameters() {
		String sql = "UPDATE work SET value=\"x\\\"y :ignored ? /*\" WHERE id=:id";
		for (int capacity : new int[]{0, 16}) {
			for (DatabaseType type : List.of(DatabaseType.MYSQL, DatabaseType.MARIA_DB)) {
				Capture capture = new Capture();
				Database database = Database.with(capture.source(), type).parsedSqlCacheCapacity(capacity).build();
				for (int pass = 0; pass < 2; pass++) {
					capture.values.clear();
					assertEquals(1L, database.query(sql).bind("id", 11).execute());
					assertEquals(List.of(11), capture.values);
					assertEquals("UPDATE work SET value=\"x\\\"y :ignored ? /*\" WHERE id=?", capture.sql.get(pass));
				}
			}
			Capture capture = new Capture();
			Database database = Database.with(capture.source(), DatabaseType.POSTGRESQL).parsedSqlCacheCapacity(capacity).build();
			assertThrows(IllegalArgumentException.class, () -> database.query(sql).bind("id", 11).execute());
			assertEquals(0, capture.connections.get());
		}
	}

	@Test
	void mysqlDoubleQuoteHandlingKeepsDoubledQuotesAndEvenBackslashes() {
		for (String sql : List.of("SELECT \"x\"\"y :ignored ?\", :id", "SELECT \"path" + "\\\\" + "\", :id"))
			assertEquals(List.of("id"), SqlCompiler.parseNamedParameterSql(sql, SqlCompiler.SqlLexicalMode.MYSQL).parameterNames);
	}

	@Test
	void oracleNationalAlternativeQuotesProtectAllDelimitersAndMixedCasePrefixes() {
		for (String literal : List.of("nq'[it's :ignored ? /*]'", "NQ'{it's :ignored ?}'", "nQ'(it's :ignored ?)'",
				"Nq'<it's :ignored ?>'", "nq'!it's :ignored ?!'")) {
			for (SqlCompiler.SqlLexicalMode mode : SqlCompiler.SqlLexicalMode.values())
				assertEquals(List.of("id"), SqlCompiler.parseNamedParameterSql("SELECT " + literal + ", :id", mode).parameterNames);
			Capture capture = new Capture();
			Database database = Database.with(capture.source(), DatabaseType.ORACLE).build();
			assertEquals(1L, database.query("UPDATE work SET value=" + literal + " WHERE id=:id").bind("id", 19).execute());
			assertEquals(List.of(19), capture.values);
		}
		for (SqlCompiler.SqlLexicalMode mode : SqlCompiler.SqlLexicalMode.values())
			assertEquals(List.of("id"), SqlCompiler.parseNamedParameterSql("SELECT xnq'[first' || :id || 'last]'", mode).parameterNames);
	}

	@Test
	void alreadyEscapedPostgresOperatorsAreRenderedOnceWithStableBindingOrder() {
		String sql = "SELECT data ?? 'a', data ? 'b', :doc::jsonb ?? :key, :doc::jsonb ??| ARRAY[:first], "
				+ ":doc::jsonb ??& ARRAY[:second], :doc::jsonb @?? :path, :doc::jsonb @? :path";
		String expected = "SELECT data ?? 'a', data ?? 'b', ?::jsonb ?? ?, ?::jsonb ??| ARRAY[?], "
				+ "?::jsonb ??& ARRAY[?], ?::jsonb @?? ?, ?::jsonb @?? ?";
		for (int capacity : new int[]{0, 16}) {
			Capture capture = new Capture();
			Database database = Database.with(capture.source(), DatabaseType.POSTGRESQL).parsedSqlCacheCapacity(capacity).build();
			for (int pass = 0; pass < 2; pass++) {
				capture.values.clear();
				assertEquals(1L, database.query(sql).bind("doc", "{}").bind("key", "a").bind("first", "a").bind("second", "a").bind("path", "$.a").execute());
				assertEquals(expected, capture.sql.get(pass));
				assertEquals(List.of("{}", "a", "{}", "a", "{}", "a", "{}", "$.a", "{}", "$.a"), capture.values);
			}
		}
	}

	@Test
	void preEscapedArrayOperatorsStillAcceptNamedParametersSpelledLikeKeywords() {
		Capture capture = new Capture();
		Database database = Database.with(capture.source(), DatabaseType.POSTGRESQL).build();
		String sql = "SELECT :where ??| ARRAY[:and], :select ??& ARRAY[:group], :where ?? :and, :select ? :group";
		assertEquals(1L, database.query(sql).bind("where", "{}").bind("and", "a").bind("select", "{}").bind("group", "b").execute());
		assertEquals("SELECT ? ??| ARRAY[?], ? ??& ARRAY[?], ? ?? ?, ? ?? ?", capture.sql.get(0));
		assertEquals(List.of("{}", "a", "{}", "b", "{}", "a", "{}", "b"), capture.values);
	}

	@Test
	void escapedOperatorsStayProtectedInLiteralTextAndCannotEnablePositionalBinding() {
		Capture capture = new Capture();
		Database database = Database.with(capture.source(), DatabaseType.POSTGRESQL).build();
		String sql = "SELECT '?? :ignored', \"??\", $$??$$, nq'[??]' /* ?? */ WHERE data ?? :key -- ??\n";
		assertEquals(1L, database.query(sql).bind("key", "a").execute());
		assertEquals(sql.replace(":key", "?"), capture.sql.get(0));
		assertEquals(1L, database.query("SELECT data ???? :key").bind("key", "a").execute());
		assertEquals("SELECT data ???? ?", capture.sql.get(1), "Two escaped question marks must survive as two operator characters");
		for (DatabaseType type : DatabaseType.values()) {
			Capture rejected = new Capture();
			Database other = Database.with(rejected.source(), type).build();
			for (String positional : List.of("SELECT ??", "UPDATE work SET value=?? WHERE id=:id"))
				assertThrows(IllegalArgumentException.class, () -> other.query(positional).bind("id", 1).execute());
			if (type != DatabaseType.POSTGRESQL)
				assertThrows(IllegalArgumentException.class, () -> other.query("SELECT data ?? :key").bind("key", "a").execute());
			assertEquals(0, rejected.connections.get(), type.name());
		}
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
