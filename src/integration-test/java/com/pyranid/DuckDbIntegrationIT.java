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

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DuckDB integration coverage: in-process file database, so no container support is required.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
// DuckDB legitimately cannot run five portable contract tests: read-only transactions (driver rejects
// per-connection setReadOnly), isolation options (setter silently no-ops), the SQL ARRAY guard (arrays ARE
// supported), JDBC generated keys (unsupported; RETURNING is guarded instead), and setMaxRows (silently
// ignored). 21 portable tests - 5 capability skips = 16.
@PortableContractCoverageFloor(16)
public class DuckDbIntegrationIT extends AbstractPortableJdbcIntegrationTests {
	public record Person(String name, Integer age, String emailAddress) {}

	public record Point(Integer x, Integer y) {}

	public record Shape(Point origin, String label) {}

	public record ShapeRow(Long id, Shape shape) {}

	public record TeamRow(Long id, List<Person> people) {}

	public record BlobRow(byte[] payload) {}

	public record ArrayRow(List<Integer> vals) {}

	@TempDir
	private Path tempDir;

	@Test
	public void testDuckDbStructColumnsMapToRecords() {
		Database db = database();
		String table = "pyranid_duckdb_struct_people";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "person STRUCT(name VARCHAR, age INTEGER, email_address VARCHAR)"
				+ ")");

		db.query("INSERT INTO " + table + " VALUES (1, {'name': 'Ada', 'age': 36, 'email_address': 'ada@example.com'})")
				.execute();

		Person person = db.query("SELECT person FROM " + table)
				.fetchObject(Person.class)
				.orElseThrow();

		Assertions.assertEquals(new Person("Ada", 36, "ada@example.com"), person);
	}

	@Test
	public void testDuckDbNestedStructsMapToNestedRecords() {
		Database db = database();
		String table = "pyranid_duckdb_nested_shapes";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "shape STRUCT(origin STRUCT(x INTEGER, y INTEGER), label VARCHAR)"
				+ ")");

		db.query("INSERT INTO " + table + " VALUES (1, {'origin': {'x': 3, 'y': 4}, 'label': 'pt'}), (2, NULL)")
				.execute();

		ShapeRow row = db.query("SELECT id, shape FROM " + table + " WHERE id = 1")
				.fetchObject(ShapeRow.class)
				.orElseThrow();

		Assertions.assertEquals(new ShapeRow(1L, new Shape(new Point(3, 4), "pt")), row);

		ShapeRow nullShapeRow = db.query("SELECT id, shape FROM " + table + " WHERE id = 2")
				.fetchObject(ShapeRow.class)
				.orElseThrow();

		Assertions.assertEquals(new ShapeRow(2L, null), nullShapeRow);

		// Single NULL STRUCT column maps to an empty Optional
		Assertions.assertTrue(db.query("SELECT shape FROM " + table + " WHERE id = 2")
				.fetchObject(Shape.class)
				.isEmpty());
	}

	@Test
	public void testDuckDbListOfStructsMapsToRecordList() {
		Database db = database();
		String table = "pyranid_duckdb_struct_teams";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "people STRUCT(name VARCHAR, age INTEGER, email_address VARCHAR)[]"
				+ ")");

		db.query("INSERT INTO " + table + " VALUES (1, ["
				+ "{'name': 'Ada', 'age': 36, 'email_address': 'ada@example.com'}, "
				+ "{'name': 'Grace', 'age': 45, 'email_address': 'grace@example.com'}])")
				.execute();

		TeamRow row = db.query("SELECT id, people FROM " + table)
				.fetchObject(TeamRow.class)
				.orElseThrow();

		Assertions.assertEquals(List.of(
						new Person("Ada", 36, "ada@example.com"),
						new Person("Grace", 45, "grace@example.com")),
				row.people());
	}

	@Test
	public void testDuckDbStructProjectionMapsToRecord() {
		Database db = database();

		// Struct literals use ':' - a space after it (or struct_pack) keeps Pyranid's named-parameter
		// parsing out of the way, and bound parameters still work inside the literal
		Person person = db.query("SELECT {'name': :name, 'age': :age, 'email_address': :emailAddress} AS person")
				.bind("name", "Edsger")
				.bind("age", 72)
				.bind("emailAddress", "edsger@example.com")
				.fetchObject(Person.class)
				.orElseThrow();

		Assertions.assertEquals(new Person("Edsger", 72, "edsger@example.com"), person);
	}

	@Test
	public void testDuckDbMapRowsContainNoDriverTypes() {
		Database db = database();
		String table = "pyranid_duckdb_map_rows";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "person STRUCT(name VARCHAR, tags INTEGER[]), "
				+ "doc JSON"
				+ ")");

		db.query("INSERT INTO " + table + " VALUES (1, {'name': 'Ada', 'tags': [7, 8]}, '{\"kind\":\"it\"}')")
				.execute();

		Map<String, Object> row = db.query("SELECT id, person, doc, "
						+ "[{'name': 'Grace'}, {'name': 'Edsger'}] AS people, "
						+ "['{\"n\":1}'::JSON, '{\"n\":2}'::JSON] AS docs FROM " + table)
				.fetchObject(Map.class)
				.map(m -> {
					@SuppressWarnings("unchecked")
					Map<String, Object> typed = (Map<String, Object>) m;
					return typed;
				})
				.orElseThrow();

		Object person = row.get("person");
		Assertions.assertInstanceOf(Map.class, person, "STRUCT map-row values should surface as name-to-value maps");

		@SuppressWarnings("unchecked")
		Map<String, Object> personMap = (Map<String, Object>) person;
		Assertions.assertEquals("Ada", personMap.get("name"));

		Object tags = personMap.get("tags");
		Assertions.assertInstanceOf(Object[].class, tags, "Nested STRUCT lists should be extracted to Java arrays");
		Assertions.assertArrayEquals(new Object[]{7, 8}, (Object[]) tags);

		Assertions.assertEquals("{\"kind\":\"it\"}", row.get("doc"), "JSON map-row values should surface as String");

		// Elements of top-level LIST columns must be unwrapped too - no driver types anywhere in a Map row
		Object[] people = Assertions.assertInstanceOf(Object[].class, row.get("people"));
		Assertions.assertEquals(2, people.length);
		Assertions.assertInstanceOf(Map.class, people[0], "STRUCT[] map-row elements should surface as maps");
		Assertions.assertEquals("Grace", ((Map<?, ?>) people[0]).get("name"));

		Object[] docs = Assertions.assertInstanceOf(Object[].class, row.get("docs"));
		Assertions.assertEquals("{\"n\":1}", docs[0], "JSON[] map-row elements should surface as Strings");
		Assertions.assertEquals("{\"n\":2}", docs[1]);
	}

	@Test
	public void testDuckDbUuidNativeRoundTrip() {
		Database db = database();
		String table = "pyranid_duckdb_uuid_items";
		UUID id = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id UUID PRIMARY KEY, "
				+ "name VARCHAR NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (id, name) VALUES (:id, :name)")
				.bind("id", id)
				.bind("name", "native uuid")
				.execute();

		Assertions.assertEquals(id, db.query("SELECT id FROM " + table)
				.fetchObject(UUID.class)
				.orElseThrow());
		Assertions.assertEquals(id.toString(), db.query("SELECT id FROM " + table)
				.fetchObject(String.class)
				.orElseThrow());
		Assertions.assertEquals("native uuid", db.query("SELECT name FROM " + table + " WHERE id = :id")
				.bind("id", id)
				.fetchObject(String.class)
				.orElseThrow());
	}

	@Test
	public void testDuckDbHugeintMapsToBigInteger() {
		Database db = database();
		BigInteger expected = new BigInteger("170141183460469231731687303715884105727");

		Assertions.assertEquals(expected, db.query("SELECT 170141183460469231731687303715884105727::HUGEINT")
				.fetchObject(BigInteger.class)
				.orElseThrow());
	}

	@Test
	public void testDuckDbJsonParameterRoundTrip() {
		Database db = database();
		String table = "pyranid_duckdb_json_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "doc JSON NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (id, doc) VALUES (:id, :doc)")
				.bind("id", 1L)
				.bind("doc", Parameters.json("{\"kind\":\"integration\",\"count\":3}"))
				.execute();

		String doc = db.query("SELECT doc FROM " + table)
				.fetchObject(String.class)
				.orElseThrow();
		Assertions.assertTrue(doc.contains("\"kind\":\"integration\""), "Unexpected JSON round trip: " + doc);

		Assertions.assertEquals("integration", db.query("SELECT doc->>'kind' FROM " + table)
				.fetchObject(String.class)
				.orElseThrow());
	}

	@Test
	public void testDuckDbVectorBindingAndSimilarity() {
		Database db = database();
		String table = "pyranid_duckdb_vector_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "embedding FLOAT[3]"
				+ ")");

		db.query("INSERT INTO " + table + " (id, embedding) VALUES (:id, :embedding)")
				.bind("id", 1L)
				.bind("embedding", Parameters.vectorOfFloats(new float[]{1.0f, 0.0f, 0.0f}))
				.execute();
		db.query("INSERT INTO " + table + " (id, embedding) VALUES (:id, :embedding)")
				.bind("id", 2L)
				.bind("embedding", Parameters.vectorOfDoubles(new double[]{0.0, 1.0, 0.0}))
				.execute();
		db.query("INSERT INTO " + table + " (id, embedding) VALUES (:id, :embedding)")
				.bind("id", 3L)
				.bind("embedding", Parameters.vectorOfFloats((float[]) null))
				.execute();

		float[] embedding = db.query("SELECT embedding FROM " + table + " WHERE id = 1")
				.fetchObject(float[].class)
				.orElseThrow();
		Assertions.assertArrayEquals(new float[]{1.0f, 0.0f, 0.0f}, embedding);

		Assertions.assertTrue(db.query("SELECT embedding FROM " + table + " WHERE id = 3")
				.fetchObject(float[].class)
				.isEmpty());

		// Similarity comparisons require an explicit cast of the bound vector to the fixed-size ARRAY type
		Long nearestId = db.query("SELECT id FROM " + table + " WHERE embedding IS NOT NULL "
						+ "ORDER BY array_cosine_distance(embedding, CAST(:probe AS FLOAT[3])) LIMIT 1")
				.bind("probe", Parameters.vectorOfFloats(new float[]{0.9f, 0.1f, 0.0f}))
				.fetchObject(Long.class)
				.orElseThrow();

		Assertions.assertEquals(1L, nearestId);
	}

	@Test
	public void testDuckDbSqlArrayParameterRoundTrip() {
		Database db = database();
		String table = "pyranid_duckdb_array_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "vals INTEGER[]"
				+ ")");

		db.query("INSERT INTO " + table + " (id, vals) VALUES (:id, :vals)")
				.bind("id", 1L)
				.bind("vals", Parameters.sqlArrayOf("INTEGER", List.of(7, 8, 9)))
				.execute();

		Assertions.assertEquals(List.of(7, 8, 9), db.query("SELECT vals FROM " + table)
				.fetchObject(ArrayRow.class)
				.orElseThrow()
				.vals());
	}

	@Test
	public void testDuckDbReturningWithSequenceMapsMultipleRows() {
		Database db = database();
		String table = "pyranid_duckdb_returning_items";
		db.query("DROP TABLE IF EXISTS " + table).execute();
		db.query("DROP SEQUENCE IF EXISTS " + table + "_seq").execute();
		db.query("CREATE SEQUENCE " + table + "_seq").execute();
		db.query("CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY DEFAULT nextval('" + table + "_seq'), "
				+ "name VARCHAR NOT NULL"
				+ ")").execute();

		List<Long> ids = db.query("INSERT INTO " + table + " (name) VALUES (:firstName), (:secondName) RETURNING id")
				.bind("firstName", "Ada")
				.bind("secondName", "Grace")
				.executeForList(Long.class);

		Assertions.assertEquals(2, ids.size());
		Assertions.assertTrue(ids.get(1) > ids.get(0));
		Assertions.assertEquals(List.of("Ada", "Grace"), db.query("SELECT name FROM " + table + " WHERE id IN (:ids) ORDER BY id")
				.bind("ids", Parameters.inList(ids))
				.fetchList(String.class));
	}

	@Test
	public void testDuckDbGeneratedKeysGuardThrowsClearException() {
		Database db = database();
		String table = "pyranid_duckdb_generated_key_guard";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "name VARCHAR NOT NULL"
				+ ")");

		DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO " + table + " (id, name) VALUES (1, 'guarded')")
						.executeReturningGeneratedKey(Long.class, "id"));

		Assertions.assertTrue(exception.getMessage().contains("DatabaseType.DUCK_DB"),
				"Unexpected message: " + exception.getMessage());
		Assertions.assertTrue(exception.getMessage().contains("RETURNING"),
				"Guard should point users at INSERT ... RETURNING; message was: " + exception.getMessage());
	}

	@Test
	public void testDuckDbBlobMapsToByteArray() {
		Database db = database();
		String table = "pyranid_duckdb_blob_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "payload BLOB NOT NULL"
				+ ")");

		byte[] payload = new byte[]{1, 2, 3, -4};

		db.query("INSERT INTO " + table + " (id, payload) VALUES (:id, :payload)")
				.bind("id", 1L)
				.bind("payload", payload)
				.execute();

		Assertions.assertArrayEquals(payload, db.query("SELECT payload FROM " + table)
				.fetchObject(byte[].class)
				.orElseThrow());
		Assertions.assertArrayEquals(payload, db.query("SELECT payload FROM " + table)
				.fetchObject(BlobRow.class)
				.orElseThrow()
				.payload());
	}

	@Test
	public void testDuckDbTimestampWithTimeZoneRoundTrip() {
		Database db = database();
		String table = "pyranid_duckdb_tstz_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "occurred_at TIMESTAMPTZ NOT NULL"
				+ ")");

		OffsetDateTime occurredAt = OffsetDateTime.of(2026, 8, 27, 10, 30, 15, 123456000, ZoneOffset.UTC);

		db.query("INSERT INTO " + table + " (id, occurred_at) VALUES (:id, :occurredAt)")
				.bind("id", 1L)
				.bind("occurredAt", occurredAt)
				.execute();

		OffsetDateTime fetched = db.query("SELECT occurred_at FROM " + table)
				.fetchObject(OffsetDateTime.class)
				.orElseThrow();

		Assertions.assertEquals(occurredAt.toInstant(), fetched.toInstant());

		// Instant binds via the OffsetDateTime conversion path and round-trips with microsecond precision
		Instant instant = Instant.parse("2026-08-27T14:30:15.123456Z");

		db.query("INSERT INTO " + table + " (id, occurred_at) VALUES (:id, :occurredAt)")
				.bind("id", 2L)
				.bind("occurredAt", instant)
				.execute();

		Assertions.assertEquals(instant, db.query("SELECT occurred_at FROM " + table + " WHERE id = 2")
				.fetchObject(Instant.class)
				.orElseThrow());
	}

	@Test
	public void testDuckDbOffsetTimeBindsAsStringAndRoundTrips() {
		Database db = database();
		String table = "pyranid_duckdb_timetz_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "occurred_at TIMETZ NOT NULL"
				+ ")");

		// The driver accepts setObject(OffsetTime) but fails at execute time, so the dialect binds the
		// ISO-8601 string form, which casts cleanly to TIMETZ
		OffsetTime occurredAt = OffsetTime.of(10, 30, 15, 0, ZoneOffset.ofHours(-4));

		db.query("INSERT INTO " + table + " (id, occurred_at) VALUES (:id, :occurredAt)")
				.bind("id", 1L)
				.bind("occurredAt", occurredAt)
				.execute();

		OffsetTime fetched = db.query("SELECT occurred_at FROM " + table)
				.fetchObject(OffsetTime.class)
				.orElseThrow();

		Assertions.assertTrue(occurredAt.isEqual(fetched), "Expected " + occurredAt + " but fetched " + fetched);
	}

	@Test
	public void testDuckDbReadOnlyTransactionsAreRejectedByDriver() {
		// Documents driver behavior backing the supportsReadOnlyTransactions(false) capability flag:
		// DuckDB's driver rejects per-connection setReadOnly outright (read-only is a database-open option)
		Database db = database();

		Assertions.assertThrows(DatabaseException.class, () ->
				db.transaction(TransactionOptions.withReadOnly(true).build(), () ->
						db.query("SELECT 1").fetchObject(Long.class)));
	}

	@Test
	public void testDuckDbSavepointsAreUnsupportedByDriver() {
		// Documents driver behavior: DuckDB's driver throws SQLFeatureNotSupportedException for savepoints
		Database db = database();
		String table = "pyranid_duckdb_savepoint_items";
		recreateTable(db, table, "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY)");

		Assertions.assertThrows(DatabaseException.class, () ->
				db.transaction(() -> {
					Transaction transaction = db.currentTransaction().orElseThrow();
					transaction.withSavepoint(() ->
							db.query("INSERT INTO " + table + " (id) VALUES (1)").execute());
				}));
	}

	@Test
	public void testDuckDbMaxRowsIsIgnoredByDriver() {
		// Documents driver behavior backing the supportsMaxRows(false) capability flag: the driver accepts
		// setMaxRows but does not enforce it. If this test starts failing, the driver gained enforcement -
		// flip the capability flag and update the README/statement-settings guidance.
		Database db = database();
		String table = "pyranid_duckdb_max_rows_items";
		recreateTable(db, table, "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY)");
		db.query("INSERT INTO " + table + " (id) VALUES (1), (2), (3)").execute();

		List<Long> ids = db.query("SELECT id FROM " + table + " ORDER BY id")
				.maxRows(2)
				.fetchList(Long.class);

		Assertions.assertEquals(3, ids.size(), "DuckDB's driver currently ignores setMaxRows; use SQL LIMIT");
	}

	@Test
	public void testDuckDbQueryTimeoutClassifiesAsTimeout() {
		Database db = database();

		DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT count(*) FROM range(2000000000) r1, range(1000) r2")
						.queryTimeout(java.time.Duration.ofSeconds(1))
						.fetchObject(Long.class));

		Assertions.assertTrue(exception.isTimeout(),
				"DuckDB query timeouts should surface as SQLTimeoutException and classify as timeouts; message was: "
						+ exception.getMessage());
	}

	@Test
	public void testDuckDbWriteWriteConflictClassifiesAsSerializationFailure() {
		Database db = database();
		Database otherDb = Database.withDataSource(new DriverManagerDataSource(jdbcUrl(), null, null))
				.databaseType(DatabaseType.DUCK_DB)
				.build();
		String table = "pyranid_duckdb_conflict_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "val BIGINT NOT NULL"
				+ ")");
		db.query("INSERT INTO " + table + " (id, val) VALUES (1, 0)").execute();

		AtomicReference<DatabaseException> conflictException = new AtomicReference<>();

		db.transaction(() -> {
			db.query("UPDATE " + table + " SET val = 1 WHERE id = 1").execute();

			// A concurrent writer conflicts under DuckDB's optimistic concurrency control. Run it on
			// another thread so it does not join this thread's ambient transaction.
			Thread writer = new Thread(() -> {
				try {
					otherDb.query("UPDATE " + table + " SET val = 2 WHERE id = 1").execute();
				} catch (DatabaseException e) {
					conflictException.set(e);
				}
			});
			writer.start();

			try {
				writer.join();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Interrupted waiting for the conflicting writer", e);
			}
		});

		DatabaseException exception = conflictException.get();

		Assertions.assertNotNull(exception, "Concurrent write should conflict under optimistic concurrency control");
		Assertions.assertTrue(exception.isSerializationFailure(),
				"DuckDB write-write conflicts should classify as retryable serialization failures; message was: "
						+ exception.getMessage());
		Assertions.assertTrue(exception.isTransient());
		Assertions.assertFalse(exception.isDeadlock());
	}

	@NonNull
	private String jdbcUrl() {
		return "jdbc:duckdb:" + this.tempDir.resolve("pyranid.duckdb");
	}

	@NonNull
	@Override
	protected DataSource dataSource() {
		return new DriverManagerDataSource(jdbcUrl(), null, null);
	}

	@NonNull
	@Override
	protected DatabaseType expectedDatabaseType() {
		return DatabaseType.DUCK_DB;
	}

	@NonNull
	@Override
	protected DialectProfile dialectProfile() {
		return new DialectProfile() {
			@NonNull
			@Override
			String timestampWithFractionalSeconds() {
				// DuckDB TIMESTAMP has microsecond precision; no precision argument is supported
				return "TIMESTAMP";
			}
		};
	}

	@NonNull
	@Override
	protected CapabilityFlags capabilityFlags() {
		return CapabilityFlags.builder()
				.supportsReadOnlyTransactions(false)
				.supportsTransactionIsolationOptions(false)
				.supportsSqlArrays(true)
				.supportsNativeJson(true)
				.supportsJdbcGeneratedKeys(false)
				.supportsMaxRows(false)
				.supportsReturningClause(true)
				.supportsNativeUuid(true)
				.reportsTimestampWithTimeZoneTypeName(true)
				.build();
	}
}
