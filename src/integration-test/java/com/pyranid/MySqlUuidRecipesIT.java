/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class MySqlUuidRecipesIT {
	@Container
	private static final MySQLContainer MYSQL = new MySQLContainer(DockerImageName.parse(
			System.getProperty("mysql.integration.image", "mysql:8.4")).asCompatibleSubstituteFor("mysql"))
			.withDatabaseName("uuid_recipes").withUsername("pyranid").withPassword("pyranid");

	@Test
	void rfcBinaryUuidRecipeSupportsStorageReadsPredicatesNullsAndBatches() {
		UuidRecipeIntegrationSupport.verifyRfcBinaryRecipe(database());
	}

	@Test
	void mysqlUuidFunctionsRequireMatchingSwapFlagsAndExplicitDecodeForSwappedStorage() {
		Database database = database();
		String table = "recipe_uuid_swap_" + UUID.randomUUID().toString().replace("-", "");
		UUID uuid = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
		database.query("CREATE TABLE " + table + " (normal_id BINARY(16), swapped_id BINARY(16))").execute();
		try {
			database.query("INSERT INTO " + table + " VALUES (UUID_TO_BIN(:id, 0), UUID_TO_BIN(:id, 1))").bind("id", uuid).execute();
			assertArrayEquals(UuidRecipeIntegrationSupport.bytes(uuid), database.query("SELECT normal_id FROM " + table).fetchObject(byte[].class).orElseThrow());
			assertEquals(uuid, database.query("SELECT normal_id FROM " + table).fetchObject(UUID.class).orElseThrow());
			assertEquals(uuid, database.query("SELECT BIN_TO_UUID(normal_id, 0) FROM " + table).fetchObject(UUID.class).orElseThrow());
			assertEquals("11D07DECF81D4FAEA76500A0C91E6BF6", database.query("SELECT HEX(swapped_id) FROM " + table).fetchObject(String.class).orElseThrow());
			assertEquals(UUID.fromString("11d07dec-f81d-4fae-a765-00a0c91e6bf6"), database.query("SELECT swapped_id FROM " + table).fetchObject(UUID.class).orElseThrow());
			assertNotEquals(uuid, database.query("SELECT swapped_id FROM " + table).fetchObject(UUID.class).orElseThrow(),
					"Raw sixteen-byte mapping does not undo MySQL's time-part swap");
			assertEquals(uuid, database.query("SELECT BIN_TO_UUID(swapped_id, 1) FROM " + table).fetchObject(UUID.class).orElseThrow());
			assertEquals(1L, database.query("SELECT COUNT(*) FROM " + table + " WHERE normal_id=UUID_TO_BIN(:id, 0)").bind("id", uuid).fetchObject(Long.class).orElseThrow());
			assertEquals(1L, database.query("SELECT COUNT(*) FROM " + table + " WHERE swapped_id=UUID_TO_BIN(:id, 1)").bind("id", uuid).fetchObject(Long.class).orElseThrow());
			assertEquals(0L, database.query("SELECT COUNT(*) FROM " + table + " WHERE swapped_id=UUID_TO_BIN(:id, 0)").bind("id", uuid).fetchObject(Long.class).orElseThrow());
		} finally { database.query("DROP TABLE " + table).execute(); }
	}

	private static Database database() {
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
				MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword()), DatabaseType.MYSQL).build();
	}
}
