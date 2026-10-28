/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class LobMappingIntegrationIT {
	public record BinaryRow(byte[] value) {}
	public record NestedRow(BinaryRow payload) {}
	public record ArrayRow(List<byte[]> values) {}
	public record NativeMapRow(Map<String, Object> payload) {}
	public record NativeArrayRow(List<Object> values) {}

	@Test
	void duckDbLobLimitsApplyBeforeDialectUnwrappingIncludingNestedValues() {
		for (boolean cached : new boolean[]{false, true}) {
			Database small = duckDb(ResultSetMapper.withPlanCachingEnabled(cached).maxLobBytes(4L).build());
			assertThrows(DatabaseException.class, () -> small.query("SELECT 'hello'::BLOB AS value").fetchObject(byte[].class));
			assertThrows(DatabaseException.class, () -> small.query("SELECT 'hello'::BLOB AS value").fetchObject(BinaryRow.class));
			assertThrows(DatabaseException.class, () -> small.query("SELECT {'value': 'hello'::BLOB} AS payload").fetchObject(NestedRow.class));
			assertThrows(DatabaseException.class, () -> small.query("SELECT ['hello'::BLOB] AS values").fetchObject(ArrayRow.class));
			Database enough = duckDb(ResultSetMapper.withPlanCachingEnabled(cached).maxLobBytes(5L).build());
			byte[] expected = "hello".getBytes(StandardCharsets.UTF_8);
			assertArrayEquals(expected, enough.query("SELECT 'hello'::BLOB AS value").fetchObject(byte[].class).orElseThrow());
			assertArrayEquals(expected, enough.query("SELECT 'hello'::BLOB AS value").fetchObject(BinaryRow.class).orElseThrow().value());
			assertArrayEquals(expected, enough.query("SELECT {'value': 'hello'::BLOB} AS payload").fetchObject(NestedRow.class).orElseThrow().payload().value());
			assertArrayEquals(expected, enough.query("SELECT ['hello'::BLOB] AS values").fetchObject(ArrayRow.class).orElseThrow().values().get(0));
		}
	}

	@Test
	void duckDbNativeMapAndCollectionValuesKeepDialectNormalization() {
		Database database = duckDb(ResultSetMapper.withDefaultConfiguration());
		Object mapValue = database.query("SELECT MAP {'value': 'hello'::BLOB} AS payload")
				.fetchObject(NativeMapRow.class).orElseThrow().payload().get("value");
		assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), assertInstanceOf(byte[].class, mapValue));
		Object arrayValue = database.query("SELECT ['hello'::BLOB] AS values")
				.fetchObject(NativeArrayRow.class).orElseThrow().values().get(0);
		assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), assertInstanceOf(byte[].class, arrayValue));
	}

	@Test
	void duckDbScalarBlobToStringRetainsDriverGetterConversion() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = duckDb(ResultSetMapper.withPlanCachingEnabled(cached).maxLobCharacters(1L).maxLobBytes(1L).build());
			for (int execution = 0; execution < 2; execution++) {
				assertEquals("hello", database.query("SELECT 'hello'::BLOB AS value").fetchObject(String.class).orElseThrow());
				assertTrue(database.query("SELECT NULL::BLOB AS value").fetchObject(String.class).isEmpty());
			}
		}
	}

	private Database duckDb(ResultSetMapper mapper) {
		return Database.withDataSource(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:duckdb:", null, null))
				.databaseType(DatabaseType.DUCK_DB).resultSetMapper(mapper).build();
	}
}
