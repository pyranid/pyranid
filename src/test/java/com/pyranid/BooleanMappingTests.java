/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BooleanMappingTests {
	private static final String SQL = "SELECT CAST(:value AS VARCHAR(128)) AS flag FROM (VALUES (0))";
	private static final List<String> TRUE_TEXT = List.of("true", "tru", "tr", "t", "yes", "ye", "y", "on", "1");
	private static final List<String> FALSE_TEXT = List.of("false", "fals", "fal", "fa", "f", "no", "n", "off", "of", "0");
	public record Flag(Boolean flag) {}
	public record PrimitiveFlag(boolean flag) {}
	public record Flags(List<Boolean> flag) {}
	public static class FlagBean {
		private Boolean flag;
		public Boolean getFlag() { return flag; }
		public void setFlag(Boolean flag) { this.flag = flag; }
	}
	public static class PrimitiveFlagBean {
		private boolean flag;
		public boolean getFlag() { return flag; }
		public void setFlag(boolean flag) { this.flag = flag; }
	}

	@Test
	void postgresBooleanWordsAndUniquePrefixesHaveOneMeaningAcrossAllRowShapes() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			for (String text : TRUE_TEXT) assertShapes(database, text, true);
			for (String text : FALSE_TEXT) assertShapes(database, text, false);
			for (String text : List.of("TrUe", "YES", "ON", "FALSE", "No", "OFF", "\t t\n", "\r f\t", " 1 ", "\n0\r"))
				assertShapes(database, text, TRUE_TEXT.contains(text.strip().toLowerCase(Locale.ROOT)));
		}
	}

	@Test
	void unrecognizedOrAmbiguousTextFailsInsteadOfBecomingFalseWithoutRevealingTheValue() {
		String privateValue = "private-invalid-boolean-8427";
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			for (String value : List.of("", " ", "o", "2", "-1", "0.0", "truth", "falsehood", "1yes", "\u0000true", privateValue)) {
				for (Class<?> target : List.of(Boolean.class, boolean.class, Flag.class, PrimitiveFlag.class, FlagBean.class, PrimitiveFlagBean.class)) {
					DatabaseException failure = assertThrows(DatabaseException.class,
							() -> database.query(SQL).bind("value", Parameters.secure(value)).fetchObject(target), target.getName() + " " + value.length());
					assertTrue(failure.getMessage().contains("boolean"));
					if (value.equals(privateValue)) {
						assertFalse(failure.getMessage().contains(privateValue));
						assertFalse(failure.toString().contains(privateValue));
					}
				}
			}
		}
	}

	@Test
	void nativeNumericAndNullSemanticsRemainConsistentWithExistingContracts() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			for (String literal : List.of("TRUE", "FALSE", "CAST(0 AS DECIMAL(10,2))", "CAST(0.5 AS DECIMAL(10,2))", "-2")) {
				boolean expected = !literal.equals("FALSE") && !literal.equals("CAST(0 AS DECIMAL(10,2))");
				String sql = "SELECT " + literal + " AS flag FROM (VALUES (0))";
				assertEquals(expected, database.query(sql).fetchObject(Boolean.class).orElseThrow());
				assertEquals(new Flag(expected), database.query(sql).fetchObject(Flag.class).orElseThrow());
				assertEquals(expected, database.query(sql).fetchObject(FlagBean.class).orElseThrow().getFlag());
			}
			String nullSql = "SELECT CAST(NULL AS VARCHAR(32)) AS flag FROM (VALUES (0))";
			assertTrue(database.query(nullSql).fetchObject(Boolean.class).isEmpty());
			assertTrue(database.query(nullSql).fetchObject(boolean.class).isEmpty());
			assertEquals(new Flag(null), database.query(nullSql).fetchObject(Flag.class).orElseThrow());
			assertNull(database.query(nullSql).fetchObject(FlagBean.class).orElseThrow().getFlag());
			assertThrows(DatabaseException.class, () -> database.query(nullSql).fetchObject(PrimitiveFlag.class));
			assertThrows(DatabaseException.class, () -> database.query(nullSql).fetchObject(PrimitiveFlagBean.class));
		}
	}

	@Test
	void typedBooleanArraysAndListsUseTheSameParserAndRetainNullElementRules() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			String sql = "SELECT ARRAY[CAST('t' AS VARCHAR(16)), 'off', NULL, 'ye'] AS flag FROM (VALUES (0))";
			assertArrayEquals(new Boolean[]{true, false, null, true}, database.query(sql).fetchObject(Boolean[].class).orElseThrow());
			assertEquals(Arrays.asList(true, false, null, true), database.query(sql).fetchObject(Flags.class).orElseThrow().flag());
			assertThrows(DatabaseException.class, () -> database.query(sql).fetchObject(boolean[].class));
			String nonNull = "SELECT ARRAY[CAST('tr' AS VARCHAR(16)), 'of'] AS flag FROM (VALUES (0))";
			assertArrayEquals(new boolean[]{true, false}, database.query(nonNull).fetchObject(boolean[].class).orElseThrow());
			String invalid = "SELECT ARRAY[CAST('t' AS VARCHAR(16)), 'o'] AS flag FROM (VALUES (0))";
			assertThrows(DatabaseException.class, () -> database.query(invalid).fetchObject(Boolean[].class));
			assertThrows(DatabaseException.class, () -> database.query(invalid).fetchObject(Flags.class));
		}
	}

	@Test
	void customMappersRetainRawInputsAndPrecedenceWhileNativeResultsStayStrings() {
		for (boolean cached : new boolean[]{false, true}) {
			CustomColumnMapper custom = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.getRawClass() == Boolean.class || type.getRawClass() == boolean.class; }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object value, TargetType target,
						Integer column, String label, InstanceProvider provider) {
					assertInstanceOf(String.class, value);
					return value.equals("application-true") ? MappingResult.of(true) : MappingResult.fallback();
				}
			};
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(custom)).build());
			assertShapes(database, "application-true", true);
			assertShapes(database, "off", false);
			assertEquals("application-true", database.query(SQL).bind("value", "application-true").fetchObject(Object.class).orElseThrow());
			assertEquals(Map.of("flag", "application-true"), database.query(SQL).bind("value", "application-true").fetchObject(Map.class).orElseThrow());
		}
	}

	@Test
	void scalarTextConversionNeverUsesTheDriverBooleanGetterOrValueRendering() throws Exception {
		Database database = database(ResultSetMapper.withDefaultConfiguration());
		ResultSetMapper mapper = database.getResultSetMapper();
		assertTrue(map(mapper, database, rows(" yes "), Boolean.class));
		assertFalse(map(mapper, database, rows("off"), boolean.class));
		Object opaque = new Object() {
			@Override public String toString() { throw new AssertionError("A mapping failure must not render the row value"); }
		};
		DatabaseException failure = assertThrows(DatabaseException.class, () -> map(mapper, database, rows(opaque), Boolean.class));
		assertTrue(failure.getMessage().contains(opaque.getClass().getName()));
		assertSame(opaque, map(mapper, database, rows(opaque), Object.class));
	}

	private static void assertShapes(Database database, String value, boolean expected) {
		assertEquals(expected, database.query(SQL).bind("value", value).fetchObject(Boolean.class).orElseThrow());
		assertEquals(expected, database.query(SQL).bind("value", value).fetchObject(boolean.class).orElseThrow());
		assertEquals(new Flag(expected), database.query(SQL).bind("value", value).fetchObject(Flag.class).orElseThrow());
		assertEquals(new PrimitiveFlag(expected), database.query(SQL).bind("value", value).fetchObject(PrimitiveFlag.class).orElseThrow());
		assertEquals(expected, database.query(SQL).bind("value", value).fetchObject(FlagBean.class).orElseThrow().getFlag());
		assertEquals(expected, database.query(SQL).bind("value", value).fetchObject(PrimitiveFlagBean.class).orElseThrow().getFlag());
	}
	private static Database database(ResultSetMapper mapper) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:boolean_mapping_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).resultSetMapper(mapper).build();
	}
	private static <T> T map(ResultSetMapper mapper, Database database, ResultSet rows, Class<T> type) throws Exception {
		StatementContext<T> context = StatementContext.<T>with(Statement.of("boolean-row", "SELECT flag"), database).build();
		return mapper.map(context, rows, type, new InstanceProvider() {}).orElseThrow();
	}
	private static ResultSet rows(Object value) {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnType" -> Types.VARCHAR;
					case "getColumnTypeName" -> "VARCHAR";
					case "getColumnClassName" -> String.class.getName();
					case "getColumnLabel", "getColumnName" -> "flag";
					default -> throw new AssertionError(method.getName());
				});
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class}, (proxy, method, args) -> switch (method.getName()) {
			case "getMetaData" -> metadata;
			case "getObject" -> value;
			case "getBoolean", "wasNull" -> throw new AssertionError("Boolean conversion must use the original value");
			default -> throw new AssertionError(method.getName());
		});
	}
}
