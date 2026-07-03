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

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Verifies the 4.5.0 vector-literal read-back: text like {@code [1.5,2.5]} (how drivers surface
 * pgvector columns after PGobject unwrapping) parses into {@code float[]}/{@code double[]} targets.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 */
public class VectorReadBackTests {
	public record EmbeddingRow(Long documentId, float[] embedding) {}

	@Test
	public void testVectorLiteralToFloatArray() {
		Database db = createDatabase("vector_floats");

		float[] vector = db.query("SELECT '[1.5,2.5,3.5]' FROM (VALUES (0)) AS t(x)")
				.fetchObject(float[].class)
				.orElseThrow();

		Assertions.assertArrayEquals(new float[]{1.5f, 2.5f, 3.5f}, vector);
	}

	@Test
	public void testVectorLiteralToDoubleArray() {
		Database db = createDatabase("vector_doubles");

		double[] vector = db.query("SELECT '[0.25, -1.5, 2e-3]' FROM (VALUES (0)) AS t(x)")
				.fetchObject(double[].class)
				.orElseThrow();

		Assertions.assertArrayEquals(new double[]{0.25d, -1.5d, 0.002d}, vector);
	}

	@Test
	public void testWhitespaceAndEmptyVectors() {
		Database db = createDatabase("vector_edges");

		float[] spaced = db.query("SELECT '[ 1.0 , 2.0 ]' FROM (VALUES (0)) AS t(x)")
				.fetchObject(float[].class)
				.orElseThrow();
		Assertions.assertArrayEquals(new float[]{1.0f, 2.0f}, spaced);

		float[] empty = db.query("SELECT '[]' FROM (VALUES (0)) AS t(x)")
				.fetchObject(float[].class)
				.orElseThrow();
		Assertions.assertEquals(0, empty.length);
	}

	@Test
	public void testRecordComponentVectorMapped() {
		Database db = createDatabase("vector_record");
		db.query("CREATE TABLE document (document_id BIGINT, embedding VARCHAR(256))").execute();
		db.query("INSERT INTO document VALUES (:id, :embedding)")
				.bind("id", 7L)
				.bind("embedding", "[0.1,0.2,0.3]")
				.execute();

		EmbeddingRow row = db.query("SELECT document_id, embedding FROM document")
				.fetchObject(EmbeddingRow.class)
				.orElseThrow();

		Assertions.assertEquals(7L, row.documentId());
		Assertions.assertArrayEquals(new float[]{0.1f, 0.2f, 0.3f}, row.embedding());
	}

	@Test
	public void testMalformedVectorElementFailsClearly() {
		Database db = createDatabase("vector_malformed");

		DatabaseException failure = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT '[1.0,abc,3.0]' FROM (VALUES (0)) AS t(x)").fetchObject(float[].class));

		Assertions.assertTrue(failure.getMessage().contains("vector literal"),
				format("Expected a vector-literal diagnostic but got: %s", failure.getMessage()));
	}

	@Test
	public void testTrailingAndDegenerateCommasFail() {
		Database db = createDatabase("vector_commas");

		for (String malformed : new String[]{"'[1,]'", "'[,1]'", "'[,]'"}) {
			Assertions.assertThrows(DatabaseException.class, () ->
							db.query("SELECT " + malformed + " FROM (VALUES (0)) AS t(x)").fetchObject(float[].class),
					format("Expected %s to be rejected", malformed));
		}
	}

	@Test
	public void testNonFiniteAndOverflowingElementsFail() {
		Database db = createDatabase("vector_nonfinite");

		// pgvector never emits NaN/Infinity; parseFloat would otherwise accept them (and silently
		// overflow 1e60 to Infinity), poisoning downstream distance math
		for (String malformed : new String[]{"'[NaN]'", "'[Infinity]'", "'[1e60]'"}) {
			Assertions.assertThrows(DatabaseException.class, () ->
							db.query("SELECT " + malformed + " FROM (VALUES (0)) AS t(x)").fetchObject(float[].class),
					format("Expected %s to be rejected", malformed));
		}
	}

	@Test
	public void testNonVectorStringToFloatArrayStillFails() {
		Database db = createDatabase("vector_nonvector");

		// A string with no bracket delimiters is NOT treated as a vector; normal conversion rules apply
		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT 'hello' FROM (VALUES (0)) AS t(x)").fetchObject(float[].class));
	}

	@Test
	public void testNullVectorColumn() {
		Database db = createDatabase("vector_null");
		db.query("CREATE TABLE vec (v VARCHAR(64))").execute();
		db.query("INSERT INTO vec VALUES (:v)").bind("v", null).execute();

		Optional<float[]> vector = db.query("SELECT v FROM vec").fetchObject(float[].class);
		Assertions.assertTrue(vector.isEmpty(), "Expected NULL to map to empty Optional");
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
