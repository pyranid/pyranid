/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class OracleUuidBindingIT {
	@Container
	private static final OracleContainer ORACLE = new OracleContainer(DockerImageName.parse(
			System.getProperty("oracle.integration.image", "gvenzl/oracle-free:23-slim-faststart"))
			.asCompatibleSubstituteFor("gvenzl/oracle-free")).withUsername("pyranid").withPassword("pyranid");

	@Test
	void uuidParametersStoreCanonicalTextOrRawForEachPositionAcrossQueriesAndBatches() {
		Database database = database();
		String table = "uuid_binding_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, text_id VARCHAR2(36), char_id CHAR(36), "
				+ "ntext_id NVARCHAR2(36), nchar_id NCHAR(36), clob_id CLOB, nclob_id NCLOB, raw_id RAW(16), writes NUMBER(10))").execute();
		try {
			UUID first = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
			UUID second = UUID.fromString("11223344-5566-7788-99aa-bbccddeeff00");
			UUID third = UUID.fromString("99aabbcc-ddee-ff00-1122-334455667788");
			String insert = "INSERT INTO " + table + " VALUES (:id, :uuid, :uuid, :uuid, :uuid, :uuid, :uuid, :uuid, 0)";
			database.query(insert).bind("id", 1).bind("uuid", first).execute();
			assertEquals(3, database.query(insert).batchChunkSize(2).executeBatch(List.of(
					Map.of("id", 2, "uuid", second), Map.of("id", 3, "uuid", third), Map.of("id", 4, "uuid", Optional.empty()))).size());

			for (String column : List.of("text_id", "char_id", "ntext_id", "nchar_id")) {
				assertEquals(List.of(first.toString(), second.toString(), third.toString()), database.query(
						"SELECT " + column + " FROM " + table + " WHERE " + column + " IS NOT NULL ORDER BY id").fetchList(String.class));
				assertEquals(List.of(36, 36, 36), database.query("SELECT LENGTH(" + column + ") FROM " + table
						+ " WHERE " + column + " IS NOT NULL ORDER BY id").fetchList(Integer.class));
				assertEquals(first, database.query("SELECT " + column + " FROM " + table + " WHERE id = 1").fetchObject(UUID.class).orElseThrow());
			}
			for (String column : List.of("clob_id", "nclob_id")) {
				assertEquals(List.of(first.toString(), second.toString(), third.toString()), database.query(
						"SELECT TO_CHAR(" + column + ") FROM " + table + " WHERE " + column + " IS NOT NULL ORDER BY id").fetchList(String.class));
				assertEquals(List.of(36, 36, 36), database.query("SELECT DBMS_LOB.GETLENGTH(" + column + ") FROM " + table
						+ " WHERE " + column + " IS NOT NULL ORDER BY id").fetchList(Integer.class));
			}
			assertArrayEquals(ByteBuffer.allocate(16).putLong(first.getMostSignificantBits()).putLong(first.getLeastSignificantBits()).array(),
					database.query("SELECT raw_id FROM " + table + " WHERE id = 1").fetchObject(byte[].class).orElseThrow());
			assertEquals(first, database.query("SELECT raw_id FROM " + table + " WHERE id = 1").fetchObject(UUID.class).orElseThrow());
			assertEquals(List.of(16, 16, 16), database.query("SELECT UTL_RAW.LENGTH(raw_id) FROM " + table + " WHERE raw_id IS NOT NULL ORDER BY id")
					.fetchList(Integer.class));
			assertEquals(1L, database.query("SELECT COUNT(*) FROM " + table + " WHERE text_id = :uuid AND char_id = :uuid "
					+ "AND ntext_id = :uuid AND nchar_id = :uuid AND raw_id = :uuid").bind("uuid", first).fetchObject(Long.class).orElseThrow());
			assertEquals(List.of(1, 2), database.query("SELECT id FROM " + table + " WHERE text_id IN (:ids) AND raw_id IN (:ids) ORDER BY id")
					.bind("ids", Parameters.inList(List.of(first, second))).fetchList(Integer.class));
			assertEquals(1L, database.query("UPDATE " + table + " SET text_id = :new, raw_id = :new, writes = writes + 1 "
					+ "WHERE text_id = :old AND raw_id = :old").bind("new", second).bind("old", first).execute());
			assertEquals(second.toString(), database.query("SELECT text_id FROM " + table + " WHERE id = 1").fetchObject(String.class).orElseThrow());
			assertEquals(second, database.query("SELECT raw_id FROM " + table + " WHERE id = 1").fetchObject(UUID.class).orElseThrow());
			assertEquals(1L, database.query("SELECT COUNT(*) FROM " + table + " WHERE text_id IS NULL AND char_id IS NULL AND ntext_id IS NULL "
					+ "AND nchar_id IS NULL AND clob_id IS NULL AND nclob_id IS NULL AND raw_id IS NULL").fetchObject(Long.class).orElseThrow());
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	@Test
	void legacyRawBoundHexTextNeedsWideningAndNormalizationBeforeCanonicalUuidBinding() {
		Database database = database();
		String table = "uuid_upgrade_" + UUID.randomUUID().toString().replace("-", "");
		UUID uuid = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
		byte[] bytes = ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array();
		database.query("CREATE TABLE " + table + " (id NUMBER(10) PRIMARY KEY, text_id VARCHAR2(32), char_id CHAR(32), raw_id RAW(16))").execute();
		try {
			// Reproduce the pre-4.9 Oracle binder: UUID bytes are bound as RAW even for character targets.
			database.useRawConnection(connection -> {
				try (var statement = connection.prepareStatement("INSERT INTO " + table + " VALUES (1, ?, ?, ?)")) {
					statement.setBytes(1, bytes);
					statement.setBytes(2, bytes);
					statement.setBytes(3, bytes);
					assertEquals(1, statement.executeUpdate());
				}
				return Optional.empty();
			});
			String hex = uuid.toString().replace("-", "").toUpperCase(Locale.ROOT);
			assertEquals(hex, database.query("SELECT text_id FROM " + table + " WHERE id=1").fetchObject(String.class).orElseThrow());
			assertEquals(hex, database.query("SELECT char_id FROM " + table + " WHERE id=1").fetchObject(String.class).orElseThrow());
			for (String column : List.of("text_id", "char_id")) {
				assertEquals(0L, database.query("SELECT COUNT(*) FROM " + table + " WHERE " + column + "=:uuid")
						.bind("uuid", uuid).fetchObject(Long.class).orElseThrow());
				DatabaseException tooWide = assertThrows(DatabaseException.class, () -> database.query("INSERT INTO " + table
						+ " (id, " + column + ") VALUES (2, :uuid)").bind("uuid", uuid).execute());
				assertEquals(Optional.of(12899), tooWide.getErrorCode(), "Canonical UUID text requires 36 character positions");
			}
			// This sample has no text-key dependencies; real schemas must migrate their dependent keys too.
			database.query("ALTER TABLE " + table + " MODIFY (text_id VARCHAR2(36), char_id CHAR(36))").execute();
			for (String column : List.of("text_id", "char_id")) {
				String value = "TRIM(" + column + ")";
				database.query("UPDATE " + table + " SET " + column + "=LOWER(SUBSTR(" + value + ",1,8)||'-'||SUBSTR(" + value
						+ ",9,4)||'-'||SUBSTR(" + value + ",13,4)||'-'||SUBSTR(" + value + ",17,4)||'-'||SUBSTR(" + value + ",21,12))").execute();
				assertEquals(uuid.toString(), database.query("SELECT " + column + " FROM " + table + " WHERE id=1").fetchObject(String.class).orElseThrow());
			}
			assertEquals(1L, database.query("SELECT COUNT(*) FROM " + table + " WHERE text_id=:uuid AND char_id=:uuid AND raw_id=:uuid")
					.bind("uuid", uuid).fetchObject(Long.class).orElseThrow());
			database.query("INSERT INTO " + table + " VALUES (2, :uuid, :uuid, :uuid)").bind("uuid", uuid).execute();
			assertEquals(List.of(16, 16), database.query("SELECT UTL_RAW.LENGTH(raw_id) FROM " + table + " ORDER BY id").fetchList(Integer.class));
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	private static Database database() {
		DataSource source = new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
				ORACLE.getJdbcUrl(), ORACLE.getUsername(), ORACLE.getPassword());
		return Database.with(source, DatabaseType.ORACLE).build();
	}
}
