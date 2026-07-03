/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Verifies the 4.5.0 {@code Map}/{@code LinkedHashMap} row target.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 */
public class MapRowTargetTests {
	@Test
	public void testMapRowTargetWithLowercaseKeysAndColumnOrder() {
		Database db = createDatabase("map_basic");
		db.query("CREATE TABLE car (id INTEGER, MAKE VARCHAR(64), model VARCHAR(64))").execute();
		db.query("INSERT INTO car (id, MAKE, model) VALUES (:id, :make, :model)")
				.bind("id", 1).bind("make", "Toyota").bind("model", "Camry").execute();

		Optional<Map> row = db.query("SELECT id, MAKE, model FROM car").fetchObject(Map.class);

		Assertions.assertTrue(row.isPresent());
		// HSQLDB reports unquoted labels uppercase; keys must be normalized lowercase regardless
		Assertions.assertEquals(List.of("id", "make", "model"), new ArrayList<>(row.get().keySet()),
				"Expected normalized-lowercase keys in column order");
		Assertions.assertEquals("Toyota", row.get().get("make"));
		Assertions.assertEquals(1, row.get().get("id"));
	}

	@Test
	public void testAliasRespectedAndQuotedMixedCaseAliasFoldsToLowercase() {
		Database db = createDatabase("map_alias");

		Optional<Map> row = db.query("SELECT 42 AS answer, 7 AS \"MixedCase\" FROM (VALUES (0)) AS t(x)")
				.fetchObject(Map.class);

		Assertions.assertTrue(row.isPresent());
		Assertions.assertEquals(List.of("answer", "mixedcase"), new ArrayList<>(row.get().keySet()),
				"Expected aliases (including quoted mixed-case) folded to lowercase");
		Assertions.assertEquals(42, row.get().get("answer"));
		Assertions.assertEquals(7, row.get().get("mixedcase"));
	}

	@Test
	public void testNullColumnPresentWithNullValue() {
		Database db = createDatabase("map_null");

		Optional<Map> row = db.query("SELECT 1 AS a, CAST(NULL AS VARCHAR(8)) AS b FROM (VALUES (0)) AS t(x)")
				.fetchObject(Map.class);

		Assertions.assertTrue(row.isPresent());
		Assertions.assertTrue(row.get().containsKey("b"), "Expected the NULL column present as a key");
		Assertions.assertNull(row.get().get("b"));
	}

	@Test
	public void testDuplicateColumnLabelsThrowWithAliasAdvice() {
		Database db = createDatabase("map_dup");

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT 1 AS x, 2 AS x FROM (VALUES (0)) AS t(x)").fetchObject(Map.class));

		Assertions.assertTrue(ex.getMessage().contains("Duplicate column label"),
				format("Expected duplicate-label diagnostics but got: %s", ex.getMessage()));
		Assertions.assertTrue(ex.getMessage().contains("aliases"), "Expected alias advice in the error");
	}

	@Test
	public void testLinkedHashMapTokenSupportedAndInstanceIsLinkedHashMap() {
		Database db = createDatabase("map_lhm");

		Optional<LinkedHashMap> row = db.query("SELECT 1 AS a, 2 AS b FROM (VALUES (0)) AS t(x)")
				.fetchObject(LinkedHashMap.class);

		Assertions.assertTrue(row.isPresent());
		Assertions.assertEquals(List.of("a", "b"), new ArrayList<>(row.get().keySet()));
	}

	@Test
	public void testWellKnownJdkMapTokensRejectedWithClearError() {
		Database db = createDatabase("map_rejected");

		for (Class<?> rejectedType : List.of(HashMap.class, TreeMap.class)) {
			DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
					db.query("SELECT 1 AS a FROM (VALUES (0)) AS t(x)").fetchObject(rejectedType));
			Assertions.assertTrue(ex.getMessage().contains("not a supported Map row type"),
					format("Expected a clear rejection for %s but got: %s", rejectedType, ex.getMessage()));
			Assertions.assertTrue(ex.getMessage().contains("LinkedHashMap.class"),
					"Expected the error to name the supported tokens");
		}
	}

	@Test
	public void testUserClassImplementingMapKeepsBeanPath() {
		Database db = createDatabase("map_user_bean");

		Optional<MapExtendingBean> row = db.query("SELECT 'hello' AS label FROM (VALUES (0)) AS t(x)")
				.fetchObject(MapExtendingBean.class);

		Assertions.assertTrue(row.isPresent());
		Assertions.assertEquals("hello", row.get().getLabel(),
				"Expected a user Map-implementing class to map via the JavaBean path");
	}

	@Test
	public void testFetchListAndScalarSingleColumn() {
		Database db = createDatabase("map_list");
		db.query("CREATE TABLE nums (n INTEGER)").execute();
		db.query("INSERT INTO nums VALUES (:a)").bind("a", 1).execute();
		db.query("INSERT INTO nums VALUES (:a)").bind("a", 2).execute();

		List<Map> rows = db.query("SELECT n FROM nums ORDER BY n").fetchList(Map.class);

		Assertions.assertEquals(2, rows.size());
		Assertions.assertEquals(1, rows.get(0).get("n"));
		Assertions.assertEquals(2, rows.get(1).get("n"));
	}

	@Test
	public void testFetchStreamWithMapTarget() {
		Database db = createDatabase("map_stream");
		db.query("CREATE TABLE items (v VARCHAR(16))").execute();
		db.query("INSERT INTO items VALUES (:v)").bind("v", "a").execute();
		db.query("INSERT INTO items VALUES (:v)").bind("v", "b").execute();

		List<Object> values = db.query("SELECT v FROM items ORDER BY v")
				.fetchStream(Map.class, stream -> stream.map(row -> row.get("v")).toList());

		Assertions.assertEquals(List.of("a", "b"), values);
	}

	@Test
	public void testMapRowTypeTokenYieldsParameterizedResultsWithoutCasts() {
		Database db = createDatabase("map_result_token");
		db.query("CREATE TABLE tokens (k VARCHAR(16), n INTEGER)").execute();
		db.query("INSERT INTO tokens VALUES (:k, :n)").bind("k", "a").bind("n", 1).execute();
		db.query("INSERT INTO tokens VALUES (:k, :n)").bind("k", "b").bind("n", 2).execute();

		// Every assignment below compiles without caller-side casts or @SuppressWarnings
		Optional<Map<String, Object>> row = db.query("SELECT k, n FROM tokens WHERE k = :k").bind("k", "a")
				.fetchObject(Query.mapRowType());
		Assertions.assertEquals(1, row.orElseThrow().get("n"));

		List<Map<String, Object>> rows = db.query("SELECT k, n FROM tokens ORDER BY k").fetchList(Query.mapRowType());
		Assertions.assertEquals(2, rows.size());
		Assertions.assertEquals("b", rows.get(1).get("k"));

		List<Object> streamedKeys = db.query("SELECT k FROM tokens ORDER BY k")
				.fetchStream(Query.mapRowType(), stream -> stream.map(r -> r.get("k")).toList());
		Assertions.assertEquals(List.of("a", "b"), streamedKeys);

		Optional<Map<String, Object>> returned = db.query("SELECT 7 AS seven FROM (VALUES (0)) AS t(x)")
				.executeForObject(Query.mapRowType());
		Assertions.assertEquals(7, returned.orElseThrow().get("seven"));

		// The token is the raw Map.class at runtime - identical mapping behavior to passing Map.class
		Assertions.assertSame(Map.class, Query.mapRowType());
	}

	@Test
	public void testExecuteForObjectWithMapTarget() {
		Database db = createDatabase("map_execute_for");

		// executeForObject shares the query-for-object machinery; verify the Map target flows through it
		Optional<Map> row = db.query("SELECT 5 AS five FROM (VALUES (0)) AS t(x)").executeForObject(Map.class);

		Assertions.assertTrue(row.isPresent());
		Assertions.assertEquals(5, row.get().get("five"));
	}

	/**
	 * A user class that happens to implement {@link Map} (by extension) - must keep bean-path mapping.
	 */
	public static class MapExtendingBean extends HashMap<String, Object> {
		private String label;

		public String getLabel() {
			return this.label;
		}

		public void setLabel(String label) {
			this.label = label;
		}
	}

	@NonNull
	private Database createDatabase(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return Database.withDataSource(dataSource).build();
	}
}
