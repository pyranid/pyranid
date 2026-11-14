/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class MariaDbUuidRecipesIT {
	@Container
	private static final MariaDBContainer MARIA_DB = new MariaDBContainer(DockerImageName.parse(
			System.getProperty("mariadb.integration.image", "mariadb:11.4")).asCompatibleSubstituteFor("mariadb"))
			.withDatabaseName("uuid_recipes").withUsername("pyranid").withPassword("pyranid");

	@Test
	void rfcBinaryUuidRecipeSupportsStorageReadsPredicatesNullsAndBatches() {
		UuidRecipeIntegrationSupport.verifyRfcBinaryRecipe(database());
	}

	@Test
	void mariaDbNativeUuidAcceptsCanonicalTextWithoutBinaryConversion() {
		Database database = database();
		String table = "recipe_native_uuid_" + UUID.randomUUID().toString().replace("-", "");
		UUID uuid = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
		database.query("CREATE TABLE " + table + " (seq INTEGER PRIMARY KEY, id UUID)").execute();
		try {
			database.query("INSERT INTO " + table + " VALUES (1, :id), (2, NULL)").bind("id", uuid).execute();
			assertEquals(uuid.toString(), database.query("SELECT CAST(id AS CHAR(36)) FROM " + table + " WHERE seq=1").fetchObject(String.class).orElseThrow());
			assertEquals(uuid, database.query("SELECT id FROM " + table + " WHERE seq=1").fetchObject(UUID.class).orElseThrow());
			assertEquals(1L, database.query("SELECT COUNT(*) FROM " + table + " WHERE id=:id").bind("id", uuid).fetchObject(Long.class).orElseThrow());
			assertTrue(database.query("SELECT id FROM " + table + " WHERE seq=2").fetchObject(UUID.class).isEmpty());
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	private static Database database() {
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
				MARIA_DB.getJdbcUrl(), MARIA_DB.getUsername(), MARIA_DB.getPassword()), DatabaseType.MARIA_DB).build();
	}
}
