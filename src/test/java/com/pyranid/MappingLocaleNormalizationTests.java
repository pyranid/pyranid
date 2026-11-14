/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ResourceLock("java.util.Locale.default")
class MappingLocaleNormalizationTests {
	private static final List<Locale> LOCALES = List.of(Locale.ROOT, Locale.ENGLISH,
			Locale.forLanguageTag("tr"), Locale.forLanguageTag("az"));
	public record Identifiers(Integer id, String itemId, String ipAddress, @DatabaseColumn("INDICATOR") String state) {}
	public record Identifier(Integer id) {}
	public static class IdentifierBean {
		private Integer id;
		private String itemId, ipAddress;
		@DatabaseColumn("INDICATOR") private String state;
		public Integer getId() { return id; }
		public void setId(Integer id) { this.id = id; }
		public String getItemId() { return itemId; }
		public void setItemId(String itemId) { this.itemId = itemId; }
		public String getIpAddress() { return ipAddress; }
		public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }
		public String getState() { return state; }
		public void setState(String state) { this.state = state; }
	}

	@Test
	void asciiColumnLabelsAndAnnotationAliasesRemainStableAcrossConfiguredLocalesAndCaches() {
		for (boolean cached : new boolean[]{false, true}) {
			for (Locale locale : LOCALES) {
				Database database = database(locale, cached);
				for (String labels : List.of("1 AS ID, 'item' AS ITEM_ID, 'ip' AS IP_ADDRESS, 'active' AS INDICATOR",
						"1 AS \"id\", 'item' AS \"item_id\", 'ip' AS \"ip_address\", 'active' AS \"indicator\"")) {
					String sql = "SELECT " + labels + " FROM (VALUES (0))";
					for (int repeat = 0; repeat < 2; repeat++) {
						assertEquals(new Identifiers(1, "item", "ip", "active"), database.query(sql).fetchObject(Identifiers.class).orElseThrow(), locale.toString());
						IdentifierBean bean = database.query(sql).fetchObject(IdentifierBean.class).orElseThrow();
						assertEquals(1, bean.getId());
						assertEquals("item", bean.getItemId());
						assertEquals("ip", bean.getIpAddress());
						assertEquals("active", bean.getState());
					}
				}
				assertEquals(Map.of("id", 1, "item_id", "item"), database.query(
						"SELECT 1 AS ID, 'item' AS ITEM_ID FROM (VALUES (0))").fetchObject(Map.class).orElseThrow());
			}
		}
	}

	@Test
	void duplicateAsciiLabelsStillFailAfterLocaleIndependentNormalization() {
		for (boolean cached : new boolean[]{false, true}) {
			for (Locale locale : LOCALES) {
				Database database = database(locale, cached);
				for (Class<?> type : List.of(Map.class, Identifier.class, IdentifierBean.class)) {
					DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query(
							"SELECT 1 AS \"ID\", 2 AS \"id\" FROM (VALUES (0))").fetchObject(type));
					assertTrue(failure.getMessage().contains("Duplicate column label"), failure.getMessage());
				}
				DefaultResultSetMapper mapper = (DefaultResultSetMapper) database.getResultSetMapper();
				StatementContext<?> context = StatementContext.with(Statement.of("locale-struct", "SELECT payload"), database).build();
				DatabaseException duplicate = assertThrows(DatabaseException.class, () -> mapper.convertResultSetValueToTargetType(
						context, Map.of("ID", 1, "id", 2), TargetType.of(Identifier.class)));
				assertTrue(duplicate.getMessage().contains("Duplicate STRUCT attribute name"));
			}
		}
	}

	@Test
	void nonAsciiLettersRetainConfiguredLocaleAndUnicodeContextRules() {
		for (Locale locale : LOCALES) {
			DefaultResultSetMapper mapper = (DefaultResultSetMapper) ResultSetMapper.withNormalizationLocale(locale).build();
			assertEquals("id", mapper.normalizeColumnLabel("ID"));
			assertEquals("id", mapper.normalizeColumnLabel("id"));
			assertEquals("invoice_id", mapper.normalizeColumnLabel("INVOICE_ID"));
			assertEquals("ä_id", mapper.normalizeColumnLabel("Ä_ID"));
			assertEquals("σος", mapper.normalizeColumnLabel("ΣΟΣ"));
			assertEquals("σi", mapper.normalizeColumnLabel("ΣI"));
			assertEquals("i\u0307", mapper.normalizeColumnLabel("I\u0307"));
			boolean turkic = locale.getLanguage().equals("tr") || locale.getLanguage().equals("az");
			assertEquals(turkic ? "id" : "i\u0307d", mapper.normalizeColumnLabel("İD"));
		}
		Database root = database(Locale.ROOT, true), turkish = database(Locale.forLanguageTag("tr"), true);
		assertEquals(Map.of("i\u0307d", 7), root.query("SELECT 7 AS \"İD\" FROM (VALUES (0))").fetchObject(Map.class).orElseThrow());
		assertEquals(Map.of("id", 7), turkish.query("SELECT 7 AS \"İD\" FROM (VALUES (0))").fetchObject(Map.class).orElseThrow());
		assertThrows(DatabaseException.class, () -> turkish.query(
				"SELECT 1 AS \"İD\", 2 AS \"ID\" FROM (VALUES (0))").fetchObject(Map.class));
	}

	@Test
	void jvmDefaultLocaleDoesNotChangeAsciiMappingWithEitherDefaultOrExplicitMapperLocales() {
		Locale original = Locale.getDefault();
		Locale originalDisplay = Locale.getDefault(Locale.Category.DISPLAY);
		Locale originalFormat = Locale.getDefault(Locale.Category.FORMAT);
		try {
			for (Locale jvmLocale : List.of(Locale.ENGLISH, Locale.forLanguageTag("tr"), Locale.forLanguageTag("az"))) {
				Locale.setDefault(jvmLocale);
				for (boolean cached : new boolean[]{false, true}) {
					for (Locale configured : LOCALES) {
						Database database = database(configured, cached);
						assertEquals(new Identifier(7), database.query("SELECT 7 AS ID FROM (VALUES (0))").fetchObject(Identifier.class).orElseThrow());
					}
					ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
					assertEquals(new Identifier(7), database(mapper).query("SELECT 7 AS ID FROM (VALUES (0))").fetchObject(Identifier.class).orElseThrow());
				}
			}
		} finally {
			Locale.setDefault(original);
			Locale.setDefault(Locale.Category.DISPLAY, originalDisplay);
			Locale.setDefault(Locale.Category.FORMAT, originalFormat);
		}
	}

	private static Database database(Locale locale, boolean cached) {
		return database(ResultSetMapper.withPlanCachingEnabled(cached).normalizationLocale(locale).build());
	}
	private static Database database(ResultSetMapper mapper) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:mapping_locale_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).resultSetMapper(mapper).build();
	}
}
