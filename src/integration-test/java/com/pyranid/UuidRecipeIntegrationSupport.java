/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Common MySQL-family recipe: the normal UUID text binding enters an explicit RFC-order SQL conversion. */
final class UuidRecipeIntegrationSupport {
	private UuidRecipeIntegrationSupport() {}
	public record UuidRow(UUID binaryId, UUID textId) {}

	static void verifyRfcBinaryRecipe(Database database) {
		String table = "recipe_binary_uuid_" + UUID.randomUUID().toString().replace("-", "");
		UUID first = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
		UUID second = UUID.fromString("01234567-89ab-cdef-8012-3456789abcde");
		database.query("CREATE TABLE " + table + " (seq INTEGER PRIMARY KEY, binary_id BINARY(16), text_id CHAR(36), marker INTEGER DEFAULT 0)").execute();
		try {
			String insert = "INSERT INTO " + table + " (seq, binary_id, text_id) VALUES (:seq, UNHEX(REPLACE(:id, '-', '')), :id)";
			assertEquals(1L, database.query(insert).bind("seq", 1).bind("id", first).execute());
			assertEquals(List.of(1L, 1L), database.query(insert).executeBatch(List.of(row(2, second), row(3, null))));
			assertEquals(List.of(new UuidRow(first, first), new UuidRow(second, second), new UuidRow(null, null)),
					database.query("SELECT binary_id, text_id FROM " + table + " ORDER BY seq").fetchList(UuidRow.class));
			assertEquals(first.toString(), database.query("SELECT text_id FROM " + table + " WHERE seq=1").fetchObject(String.class).orElseThrow());
			assertEquals(first.toString().replace("-", "").toUpperCase(Locale.ROOT),
					database.query("SELECT HEX(binary_id) FROM " + table + " WHERE seq=1").fetchObject(String.class).orElseThrow());
			assertArrayEquals(bytes(first), database.query("SELECT binary_id FROM " + table + " WHERE seq=1").fetchObject(byte[].class).orElseThrow());
			assertEquals(first, database.query("SELECT binary_id FROM " + table + " WHERE seq=1").fetchObject(UUID.class).orElseThrow());
			assertTrue(database.query("SELECT binary_id FROM " + table + " WHERE seq=3").fetchObject(UUID.class).isEmpty());
			assertEquals(List.of(1), database.query("SELECT seq FROM " + table + " WHERE binary_id=UNHEX(REPLACE(:id, '-', ''))")
					.bind("id", first).fetchList(Integer.class));
			assertEquals(0L, database.query("SELECT COUNT(*) FROM " + table + " WHERE binary_id=:id").bind("id", first).fetchObject(Long.class).orElseThrow(),
					"The default MySQL-family UUID parameter is text, not the stored sixteen bytes");
			assertEquals(1L, database.query("UPDATE " + table + " SET marker=1 WHERE binary_id=UNHEX(REPLACE(:id, '-', ''))")
					.bind("id", first).execute());
			assertEquals(1, database.query("SELECT marker FROM " + table + " WHERE seq=1").fetchObject(Integer.class).orElseThrow());
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	static byte[] bytes(UUID uuid) {
		return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array();
	}
	private static Map<String, Object> row(int sequence, UUID id) {
		Map<String, Object> values = new HashMap<>(); values.put("seq", sequence); values.put("id", id); return values;
	}
}
