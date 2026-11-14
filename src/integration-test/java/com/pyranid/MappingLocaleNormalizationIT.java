/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MappingLocaleNormalizationIT {
	@TempDir Path directory;
	private static final List<Locale> LOCALES = List.of(Locale.ROOT, Locale.ENGLISH,
			Locale.forLanguageTag("tr"), Locale.forLanguageTag("az"));
	public record Identifier(Integer id) {}
	public record Identifiers(Integer id, String itemId, @DatabaseColumn("INDICATOR") String state) {}
	public record Payload(Identifier payload) {}
	public static class IdentifierBean {
		private Integer id;
		private String itemId;
		@DatabaseColumn("INDICATOR") private String state;
		public Integer getId() { return id; }
		public void setId(Integer id) { this.id = id; }
		public String getItemId() { return itemId; }
		public void setItemId(String itemId) { this.itemId = itemId; }
		public String getState() { return state; }
		public void setState(String state) { this.state = state; }
	}

	@Test
	void sqliteAsciiAliasesAndDuplicateDetectionDoNotDependOnTurkicCaseRules() {
		verify(DatabaseType.SQLITE);
	}

	@Test
	void duckDbAsciiAliasesAndDuplicateDetectionDoNotDependOnTurkicCaseRules() {
		verify(DatabaseType.DUCK_DB);
	}

	@Test
	void duckDbStructAttributeNormalizationUsesTheSameAsciiPolicy() {
		for (boolean cached : new boolean[]{false, true}) {
			for (Locale locale : LOCALES) {
				Database database = database(DatabaseType.DUCK_DB, locale, cached, "struct");
				assertEquals(new Identifier(7), database.query("SELECT {'ID': 7} AS payload").fetchObject(Identifier.class).orElseThrow());
				assertEquals(new Payload(new Identifier(9)), database.query("SELECT {'ID': 9} AS payload").fetchObject(Payload.class).orElseThrow());
			}
		}
	}

	private void verify(DatabaseType type) {
		for (boolean cached : new boolean[]{false, true}) {
			for (Locale locale : LOCALES) {
				Database database = database(type, locale, cached, "rows");
				for (int repeat = 0; repeat < 2; repeat++) {
					String sql = "SELECT 7 AS \"ID\", 'item' AS \"ITEM_ID\", 'active' AS \"INDICATOR\"";
					assertEquals(new Identifiers(7, "item", "active"), database.query(sql).fetchObject(Identifiers.class).orElseThrow());
					IdentifierBean bean = database.query(sql).fetchObject(IdentifierBean.class).orElseThrow();
					assertEquals(7, bean.getId()); assertEquals("item", bean.getItemId()); assertEquals("active", bean.getState());
					assertEquals(Map.of("id", 7, "item_id", "item", "indicator", "active"), database.query(sql).fetchObject(Map.class).orElseThrow());
				}
				for (Class<?> target : List.of(Map.class, Identifier.class, IdentifierBean.class))
					assertThrows(DatabaseException.class, () -> database.query("SELECT 1 AS \"ID\", 2 AS \"id\"").fetchObject(target));
				boolean turkic = locale.getLanguage().equals("tr") || locale.getLanguage().equals("az");
				assertEquals(Map.of(turkic ? "id" : "i\u0307d", 7), database.query("SELECT 7 AS \"İD\"").fetchObject(Map.class).orElseThrow());
			}
		}
	}

	private Database database(DatabaseType type, Locale locale, boolean cached, String suffix) {
		String name = type + "_" + locale.toLanguageTag() + "_" + cached + "_" + suffix;
		String url = (type == DatabaseType.SQLITE ? "jdbc:sqlite:" : "jdbc:duckdb:")
				+ directory.resolve(name + (type == DatabaseType.SQLITE ? ".sqlite" : ".duckdb"));
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(url, null, null), type)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).normalizationLocale(locale).build()).build();
	}
}
