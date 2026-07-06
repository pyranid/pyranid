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
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.1.0
 */
@Testcontainers
public class PostgreSqlIntegrationIT extends AbstractPortableJdbcIntegrationTests {
	public record JsonbAndArrayRow(String payload, String[] tagsArray, List<String> tagsList, Set<String> tagsSet) {}
	public record PgvectorRow(Long documentId, float[] embeddingFloats, double[] embeddingDoubles) {}

	private static final String POSTGRES_IMAGE_NAME =
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17");
	private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse(POSTGRES_IMAGE_NAME)
			.asCompatibleSubstituteFor("postgres");

	@Container
	private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(POSTGRES_IMAGE)
			.withDatabaseName("pyranid")
			.withUsername("pyranid")
			.withPassword("pyranid");

	@Test
	public void testPostgreSqlDatabaseTypeDetection() {
		Database db = Database.withDataSource(dataSource()).build();

		Assertions.assertEquals(DatabaseType.POSTGRESQL, db.getDatabaseType());
	}

	@Test
	public void testFetchStreamConfiguresPostgreSqlCursorOutsideTransaction() {
		TrackingPostgreSqlStreamDataSource trackingDataSource = new TrackingPostgreSqlStreamDataSource(dataSource());
		Database db = Database.withDataSource(trackingDataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.build();
		AtomicBoolean sawAutoCommitDisabled = new AtomicBoolean(false);

		List<Integer> values = db.query("SELECT generate_series(1, 3)")
				.customize((statementContext, preparedStatement) -> {
					sawAutoCommitDisabled.set(!preparedStatement.getConnection().getAutoCommit());
				})
				.fetchStream(Integer.class, stream -> stream.toList());

		Assertions.assertEquals(List.of(1, 2, 3), values);
		Assertions.assertTrue(sawAutoCommitDisabled.get());
		Assertions.assertEquals(256, trackingDataSource.fetchSizeAtExecute.get());
		Assertions.assertTrue(trackingDataSource.autoCommitDisabled.get());
		Assertions.assertTrue(trackingDataSource.committed.get());
		Assertions.assertTrue(trackingDataSource.autoCommitRestored.get());
	}

	@Test
	public void testFetchStreamUsesPostgreSqlCursorFetchSizeWhenDatabaseDefaultIsZero() {
		TrackingPostgreSqlStreamDataSource trackingDataSource = new TrackingPostgreSqlStreamDataSource(dataSource());
		Database db = Database.withDataSource(trackingDataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.fetchSize(0)
				.build();

		List<Integer> values = db.query("SELECT generate_series(1, 3)")
				.fetchStream(Integer.class, stream -> stream.toList());

		Assertions.assertEquals(List.of(1, 2, 3), values);
		Assertions.assertEquals(256, trackingDataSource.fetchSizeAtExecute.get());
		Assertions.assertTrue(trackingDataSource.autoCommitDisabled.get());
		Assertions.assertTrue(trackingDataSource.committed.get());
		Assertions.assertTrue(trackingDataSource.autoCommitRestored.get());
	}

	@Test
	public void testFetchStreamHonorsQueryFetchSizeForPostgreSqlCursorOutsideTransaction() {
		TrackingPostgreSqlStreamDataSource trackingDataSource = new TrackingPostgreSqlStreamDataSource(dataSource());
		Database db = Database.withDataSource(trackingDataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.build();

		List<Integer> values = db.query("SELECT generate_series(1, 3)")
				.fetchSize(12)
				.fetchStream(Integer.class, stream -> stream.toList());

		Assertions.assertEquals(List.of(1, 2, 3), values);
		Assertions.assertEquals(12, trackingDataSource.fetchSizeAtExecute.get());
		Assertions.assertTrue(trackingDataSource.autoCommitDisabled.get());
		Assertions.assertTrue(trackingDataSource.committed.get());
		Assertions.assertTrue(trackingDataSource.autoCommitRestored.get());
	}

	@Test
	public void testJsonbParameterAndReturningRoundTrip() {
		Database db = Database.withDataSource(dataSource()).build();

		db.query("CREATE TABLE IF NOT EXISTS pyranid_jsonb_test (id BIGSERIAL PRIMARY KEY, payload JSONB NOT NULL)")
				.execute();

		String kind = db.query("INSERT INTO pyranid_jsonb_test(payload) VALUES (:payload) RETURNING payload->>'kind'")
				.bind("payload", Parameters.json("{\"kind\":\"integration\",\"count\":3}"))
				.fetchObject(String.class)
				.orElseThrow();

		Assertions.assertEquals("integration", kind);
	}

	@Test
	public void testPostgreSqlReturningMapsMultipleGeneratedRows() {
		Database db = database();
		String table = "pyranid_pg_returning_keys";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGSERIAL PRIMARY KEY, "
				+ "name TEXT NOT NULL"
				+ ")");

		List<Long> ids = db.query("INSERT INTO " + table + " (name) VALUES (:firstName), (:secondName) RETURNING id")
				.bind("firstName", "Ada")
				.bind("secondName", "Grace")
				.executeForList(Long.class);

		Assertions.assertEquals(2, ids.size());
		Assertions.assertTrue(ids.get(0) > 0L);
		Assertions.assertTrue(ids.get(1) > ids.get(0));
		Assertions.assertEquals(List.of("Ada", "Grace"), db.query("SELECT name FROM " + table + " WHERE id IN (:ids) ORDER BY id")
				.bind("ids", Parameters.inList(ids))
				.fetchList(String.class));
	}

	@Test
	public void testPostgreSqlNativeUuidRoundTrip() {
		Database db = database();
		String table = "pyranid_pg_uuid_items";
		UUID id = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id UUID PRIMARY KEY, "
				+ "name TEXT NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (id, name) VALUES (:id, :name)")
				.bind("id", id)
				.bind("name", "native uuid")
				.execute();

		Assertions.assertEquals(id, db.query("SELECT id FROM " + table)
				.fetchObject(UUID.class)
				.orElseThrow());
		Assertions.assertEquals(id.toString(), db.query("SELECT id::text FROM " + table)
				.fetchObject(String.class)
				.orElseThrow());
	}

	@Test
	public void testJsonbQuestionMarkOperatorsRoundTrip() {
		Database db = Database.withDataSource(dataSource())
				.databaseType(DatabaseType.POSTGRESQL)
				.build();

		db.query("CREATE TABLE IF NOT EXISTS pyranid_jsonb_operator_test (id BIGSERIAL PRIMARY KEY, payload JSONB NOT NULL)")
				.execute();
		db.query("TRUNCATE TABLE pyranid_jsonb_operator_test").execute();

		Long id = db.query("INSERT INTO pyranid_jsonb_operator_test(payload) VALUES (:payload) RETURNING id")
				.bind("payload", Parameters.json("{\"kind\":\"integration\",\"count\":3}"))
				.fetchObject(Long.class)
				.orElseThrow();

		Assertions.assertTrue(db.query("SELECT '{\"kind\":1}'::jsonb ? 'kind'")
				.fetchObject(Boolean.class)
				.orElseThrow());
		Assertions.assertTrue(db.query("SELECT payload ? 'kind' FROM pyranid_jsonb_operator_test WHERE id = :id")
				.bind("id", id)
				.fetchObject(Boolean.class)
				.orElseThrow());
		Assertions.assertTrue(db.query("SELECT payload ?| ARRAY['missing', 'kind'] FROM pyranid_jsonb_operator_test WHERE id = :id")
				.bind("id", id)
				.fetchObject(Boolean.class)
				.orElseThrow());
		Assertions.assertTrue(db.query("SELECT payload ?& ARRAY['kind', 'count'] FROM pyranid_jsonb_operator_test WHERE id = :id")
				.bind("id", id)
				.fetchObject(Boolean.class)
				.orElseThrow());
	}

	@Test
	public void testTextArrayParameterAndReturningRoundTrip() {
		Database db = Database.withDataSource(dataSource()).build();

		db.query("CREATE TABLE IF NOT EXISTS pyranid_array_test (id BIGSERIAL PRIMARY KEY, tags TEXT[] NOT NULL)")
				.execute();

		String secondTag = db.query("INSERT INTO pyranid_array_test(tags) VALUES (:tags) RETURNING tags[2]")
				.bind("tags", Parameters.sqlArrayOf("text", List.of("alpha", "beta", "gamma")))
				.fetchObject(String.class)
				.orElseThrow();

		Assertions.assertEquals("beta", secondTag);
	}

	@Test
	public void testJsonbAndTextArrayResultMapping() {
		for (Boolean planCachingEnabled : List.of(false, true)) {
			Database db = Database.withDataSource(dataSource())
					.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(planCachingEnabled).build())
					.build();

			db.query("""
					CREATE TABLE IF NOT EXISTS pyranid_jsonb_array_result_test (
						id BIGSERIAL PRIMARY KEY,
						payload JSONB NOT NULL,
						tags TEXT[] NOT NULL
					)
					""").execute();
			db.query("TRUNCATE TABLE pyranid_jsonb_array_result_test").execute();
			db.query("INSERT INTO pyranid_jsonb_array_result_test(payload, tags) VALUES (:payload, :tags)")
					.bind("payload", Parameters.json("{\"kind\":\"integration\",\"count\":3}"))
					.bind("tags", Parameters.sqlArrayOf("text", List.of("alpha", "beta", "alpha", "gamma")))
					.execute();

			JsonbAndArrayRow row = db.query("""
					SELECT payload, tags AS tags_array, tags AS tags_list, tags AS tags_set
					FROM pyranid_jsonb_array_result_test
					FETCH FIRST ROW ONLY
					""")
					.fetchObject(JsonbAndArrayRow.class)
					.orElseThrow();

			Assertions.assertTrue(row.payload().contains("\"kind\""));
			Assertions.assertTrue(row.payload().contains("\"integration\""));
			Assertions.assertArrayEquals(new String[]{"alpha", "beta", "alpha", "gamma"}, row.tagsArray());
			Assertions.assertEquals(List.of("alpha", "beta", "alpha", "gamma"), row.tagsList());
			Assertions.assertEquals(new LinkedHashSet<>(List.of("alpha", "beta", "gamma")), row.tagsSet());

			String[] scalarTags = db.query("SELECT tags FROM pyranid_jsonb_array_result_test FETCH FIRST ROW ONLY")
					.fetchObject(String[].class)
					.orElseThrow();
			Assertions.assertArrayEquals(new String[]{"alpha", "beta", "alpha", "gamma"}, scalarTags);
		}
	}

	@Test
	public void testPgvectorParameterAndReadBackRoundTrip() {
		Database db = Database.withDataSource(dataSource())
				.databaseType(DatabaseType.POSTGRESQL)
				.build();

		db.query("CREATE EXTENSION IF NOT EXISTS vector").execute();
		db.query("DROP TABLE IF EXISTS pyranid_pgvector_test").execute();
		db.query("""
				CREATE TABLE pyranid_pgvector_test (
					document_id BIGINT PRIMARY KEY,
					embedding vector(3) NOT NULL
				)
				""").execute();
		db.query("INSERT INTO pyranid_pgvector_test (document_id, embedding) VALUES (:id, :embedding)")
				.bind("id", 7L)
				.bind("embedding", Parameters.vectorOfFloats(new float[]{0.1f, 0.2f, 0.3f}))
				.execute();

		Assertions.assertEquals("vector", db.query("SELECT pg_typeof(embedding)::text FROM pyranid_pgvector_test")
				.fetchObject(String.class)
				.orElseThrow());

		float[] embeddingAsFloats = db.query("SELECT embedding FROM pyranid_pgvector_test")
				.fetchObject(float[].class)
				.orElseThrow();
		double[] embeddingAsDoubles = db.query("SELECT embedding FROM pyranid_pgvector_test")
				.fetchObject(double[].class)
				.orElseThrow();
		PgvectorRow row = db.query("""
				SELECT document_id,
				       embedding AS embedding_floats,
				       embedding AS embedding_doubles
				FROM pyranid_pgvector_test
				""")
				.fetchObject(PgvectorRow.class)
				.orElseThrow();
		Long nearestDocumentId = db.query("""
				SELECT document_id
				FROM pyranid_pgvector_test
				ORDER BY embedding <-> :query
				LIMIT 1
				""")
				.bind("query", Parameters.vectorOfDoubles(new double[]{0.1d, 0.19d, 0.31d}))
				.fetchObject(Long.class)
				.orElseThrow();

		Assertions.assertArrayEquals(new float[]{0.1f, 0.2f, 0.3f}, embeddingAsFloats);
		Assertions.assertArrayEquals(new double[]{0.1d, 0.2d, 0.3d}, embeddingAsDoubles);
		Assertions.assertEquals(7L, row.documentId());
		Assertions.assertArrayEquals(new float[]{0.1f, 0.2f, 0.3f}, row.embeddingFloats());
		Assertions.assertArrayEquals(new double[]{0.1d, 0.2d, 0.3d}, row.embeddingDoubles());
		Assertions.assertEquals(7L, nearestDocumentId);
	}

	@Test
	public void testTimestampAndTimestampWithTimeZoneRoundTrip() {
		Database db = Database.withDataSource(dataSource())
				.timeZone(ZoneId.of("UTC"))
				.build();
		Instant instant = Instant.parse("2020-01-02T03:04:05.123456Z");
		OffsetDateTime offsetDateTime = OffsetDateTime.parse("2020-01-02T05:04:05.123456+02:00");
		Instant expectedInstant = instant;
		Instant expectedOffsetInstant = offsetDateTime.toInstant();

		db.query("""
				CREATE TABLE IF NOT EXISTS pyranid_temporal_test (
					id BIGSERIAL PRIMARY KEY,
					instant_ts TIMESTAMP NOT NULL,
					instant_tstz TIMESTAMPTZ NOT NULL,
					offset_ts TIMESTAMP NOT NULL,
					offset_tstz TIMESTAMPTZ NOT NULL
				)
				""").execute();
		db.query("TRUNCATE TABLE pyranid_temporal_test").execute();
		db.query("""
				INSERT INTO pyranid_temporal_test(instant_ts, instant_tstz, offset_ts, offset_tstz)
				VALUES (:instantTs, :instantTstz, :offsetTs, :offsetTstz)
				""")
				.bind("instantTs", instant)
				.bind("instantTstz", instant)
				.bind("offsetTs", offsetDateTime)
				.bind("offsetTstz", offsetDateTime)
				.execute();

		Assertions.assertEquals(expectedInstant, db.query("SELECT instant_ts FROM pyranid_temporal_test")
				.fetchObject(Instant.class)
				.orElseThrow());
		Assertions.assertEquals(expectedInstant, db.query("SELECT instant_tstz FROM pyranid_temporal_test")
				.fetchObject(Instant.class)
				.orElseThrow());
		Assertions.assertEquals(expectedOffsetInstant, db.query("SELECT offset_ts FROM pyranid_temporal_test")
				.fetchObject(Instant.class)
				.orElseThrow());
		Assertions.assertEquals(expectedOffsetInstant, db.query("SELECT offset_tstz FROM pyranid_temporal_test")
				.fetchObject(Instant.class)
				.orElseThrow());
		Assertions.assertEquals(expectedOffsetInstant, db.query("SELECT offset_tstz FROM pyranid_temporal_test")
				.fetchObject(OffsetDateTime.class)
				.orElseThrow()
				.toInstant());
	}

	@Test
	public void testPostgreSqlExceptionMetadataExtraction() {
		Database db = Database.withDataSource(dataSource()).build();

		db.query("CREATE TABLE IF NOT EXISTS pyranid_unique_test (id BIGSERIAL PRIMARY KEY, email TEXT UNIQUE)")
				.execute();
		db.query("TRUNCATE TABLE pyranid_unique_test").execute();
		db.query("INSERT INTO pyranid_unique_test(email) VALUES (:email)")
				.bind("email", "ada@example.com")
				.execute();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO pyranid_unique_test(email) VALUES (:email)")
						.bind("email", "ada@example.com")
						.execute());

		Assertions.assertEquals("23505", ex.getSqlState().orElse(null));
		Assertions.assertTrue(ex.getConstraint().orElse("").contains("email"),
				"Expected PostgreSQL constraint metadata to be extracted");
	}

	@Test
	public void testSecureParameterValueScrubbedFromDriverEchoedDiagnostics() {
		// PostgreSQL echoes bound values in constraint-violation detail ("Key (email)=(...) already exists").
		// A SecureParameter value must be scrubbed from every Pyranid-rendered diagnostic surface while the
		// raw driver exception remains intact as the cause.
		String secret = "leak-test@secret.example";
		Database db = Database.withDataSource(dataSource()).build();

		db.query("CREATE TABLE IF NOT EXISTS pyranid_secure_leak_test (id BIGSERIAL PRIMARY KEY, email TEXT UNIQUE)")
				.execute();
		db.query("TRUNCATE TABLE pyranid_secure_leak_test").execute();
		db.query("INSERT INTO pyranid_secure_leak_test(email) VALUES (:email)")
				.bind("email", Parameters.secure(secret))
				.execute();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO pyranid_secure_leak_test(email) VALUES (:email)")
						.bind("email", Parameters.secure(secret))
						.execute());

		Assertions.assertEquals("23505", ex.getSqlState().orElse(null));
		Assertions.assertFalse(ex.getMessage().contains(secret),
				"Expected the secure value absent from DatabaseException.getMessage()");
		Assertions.assertFalse(ex.toString().contains(secret),
				"Expected the secure value absent from DatabaseException.toString()");
		Assertions.assertFalse(ex.getDetail().orElse("").contains(secret),
				"Expected the secure value absent from getDetail()");
		Assertions.assertFalse(ex.getDbmsMessage().orElse("").contains(secret),
				"Expected the secure value absent from getDbmsMessage()");
		Assertions.assertTrue(ex.getMessage().contains("<redacted>"),
				"Expected the mask present in the scrubbed message");

		// The raw driver exception is deliberately preserved: the cause chain must be treated as sensitive
		Throwable cause = ex.getCause();
		Assertions.assertNotNull(cause);
		Assertions.assertTrue(String.valueOf(cause.getMessage()).contains(secret),
				"Expected the raw driver exception (the cause) to remain unsanitized");
	}

	@Test
	public void testStatementLogScrubbedAgainstRealDriverMessage() {
		String secret = "statement-log-leak@secret.example";
		java.util.concurrent.atomic.AtomicReference<StatementLog<?>> loggedStatementLog = new java.util.concurrent.atomic.AtomicReference<>();
		Database db = Database.withDataSource(dataSource())
				.statementLogger(loggedStatementLog::set)
				.build();

		db.query("CREATE TABLE IF NOT EXISTS pyranid_log_leak_test (id BIGSERIAL PRIMARY KEY, email TEXT UNIQUE)")
				.execute();
		db.query("TRUNCATE TABLE pyranid_log_leak_test").execute();
		db.query("INSERT INTO pyranid_log_leak_test(email) VALUES (:email)")
				.bind("email", Parameters.secure(secret))
				.execute();

		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO pyranid_log_leak_test(email) VALUES (:email)")
						.bind("email", Parameters.secure(secret))
						.execute());

		StatementLog<?> statementLog = loggedStatementLog.get();
		Assertions.assertNotNull(statementLog, "Expected a StatementLog for the failed statement");
		Assertions.assertTrue(statementLog.getException().orElseThrow() instanceof DatabaseException,
				"Expected the wrapped exception stored in the StatementLog");
		Assertions.assertFalse(statementLog.toString().contains(secret),
				"Expected the secure value absent from StatementLog.toString() against a real driver message");
	}

	@Test
	public void testMapRowValuesUnwrapDriverObjects() {
		// PGobject-wrapped columns (JSONB, vector) must surface in map rows as their String form,
		// matching record/bean mapping - never as raw org.postgresql.util.PGobject
		Database db = Database.withDataSource(dataSource()).build();

		java.util.Map row = db.query("SELECT '{\"a\":1}'::jsonb AS payload, 42 AS n FROM (VALUES (0)) AS t(x)")
				.fetchObject(Query.mapRowType())
				.orElseThrow();

		Assertions.assertTrue(row.get("payload") instanceof String,
				"Expected JSONB to unwrap to String in map rows, got: " + row.get("payload").getClass().getName());
		Assertions.assertTrue(((String) row.get("payload")).contains("\"a\""));
		Assertions.assertEquals(42, row.get("n"));
	}

	@Test
	public void testMapRowTargetThroughDmlReturning() {
		Database db = Database.withDataSource(dataSource()).build();

		db.query("CREATE TABLE IF NOT EXISTS pyranid_map_returning (id BIGSERIAL PRIMARY KEY, MixedCaseName TEXT)")
				.execute();
		db.query("TRUNCATE TABLE pyranid_map_returning").execute();

		java.util.Map row = db.query("INSERT INTO pyranid_map_returning (MixedCaseName) VALUES (:name) RETURNING id, MixedCaseName")
				.bind("name", "Ada")
				.executeForObject(java.util.Map.class)
				.orElseThrow();

		Assertions.assertEquals(java.util.List.of("id", "mixedcasename"), java.util.List.copyOf(row.keySet()),
				"Expected lowercase keys in column order through the DML-returning path");
		Assertions.assertEquals("Ada", row.get("mixedcasename"));
	}

	@Test
	public void testTransactionWithRetryRecoversFromSerializationConflict() throws Exception {
		Database db = Database.withDataSource(dataSource())
				.databaseType(DatabaseType.POSTGRESQL)
				.build();
		String table = "pyranid_retry_serialization_conflict";
		db.query("DROP TABLE IF EXISTS " + table).execute();
		db.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, val INT NOT NULL)").execute();
		db.query("INSERT INTO " + table + " (id, val) VALUES (1, 0)").execute();

		TransactionOptions repeatableRead = TransactionOptions.withIsolation(TransactionIsolation.REPEATABLE_READ).build();
		CountDownLatch retrierHasRead = new CountDownLatch(1);
		CountDownLatch conflicterCommitted = new CountDownLatch(1);
		AtomicInteger attempts = new AtomicInteger();
		AtomicReference<Boolean> firstFailureWasSerialization = new AtomicReference<>();

		ExecutorService executor = Executors.newSingleThreadExecutor();

		try {
			// Concurrent committer: updates and commits the same row after the retrier has taken its snapshot.
			Future<?> conflicter = executor.submit(() -> {
				awaitLatch(retrierHasRead);
				db.transaction(() ->
						db.query("UPDATE " + table + " SET val = val + 1 WHERE id = 1").execute());
				conflicterCommitted.countDown();
				return null;
			});

			RetryPolicy retryPolicy = RetryPolicy.ofMaxAttempts(5,
					RetryPolicy.Backoff.fixed(Duration.ofMillis(25)),
					failure -> {
						firstFailureWasSerialization.compareAndSet(null, failure.isSerializationFailure());
						return failure.isSerializationFailure() || failure.isDeadlock();
					});

			TransactionRetryResult<Integer> retryResult = db.transactionWithRetry(retryPolicy, repeatableRead, () -> {
				int attempt = attempts.incrementAndGet();
				// Establish this attempt's REPEATABLE READ snapshot by reading the row.
				int current = db.query("SELECT val FROM " + table + " WHERE id = 1")
						.fetchObject(Integer.class)
						.orElseThrow();

				if (attempt == 1) {
					// Allow the concurrent transaction to update and commit the same row...
					retrierHasRead.countDown();
					// ...then proceed so our UPDATE collides with a committed concurrent change (SQLState 40001).
					awaitLatch(conflicterCommitted);
				}

				db.query("UPDATE " + table + " SET val = :val WHERE id = 1")
						.bind("val", current + 100)
						.execute();

				return Optional.of(current + 100);
			});
			Integer finalValue = retryResult.getValue().orElseThrow();

			conflicter.get(30, TimeUnit.SECONDS);

			Assertions.assertEquals(2, attempts.get(),
					"Expected exactly one serialization failure followed by a successful retry");
			Assertions.assertEquals(1, retryResult.getFailures().size());
			Assertions.assertEquals(2, retryResult.getAttemptCount());
			Assertions.assertTrue(retryResult.wasRetried());
			Assertions.assertEquals(Boolean.TRUE, firstFailureWasSerialization.get(),
					"First failed attempt should be classified as a serialization failure");
			Assertions.assertEquals(101, finalValue,
					"Retry should read the committed concurrent value (1) and write 101");
			Assertions.assertEquals(101, db.query("SELECT val FROM " + table + " WHERE id = 1")
					.fetchObject(Integer.class)
					.orElseThrow());
		} finally {
			executor.shutdownNow();
		}
	}

	@Test
	public void testStatementTimeoutClassifiedAsTimeout() {
		Database db = database();

		DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT pg_sleep(3)")
						.queryTimeout(Duration.ofMillis(300))
						.fetchObject(String.class));

		Assertions.assertTrue(exception.isTimeout(),
				"PostgreSQL statement cancellation (57014) should classify as a timeout");
		Assertions.assertEquals("57014", exception.getSqlState().orElse(null));
		Assertions.assertFalse(exception.isSerializationFailure());
	}

	@NonNull
	@Override
	protected DataSource dataSource() {
		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setUrl(POSTGRES.getJdbcUrl());
		dataSource.setUser(POSTGRES.getUsername());
		dataSource.setPassword(POSTGRES.getPassword());
		return dataSource;
	}

	@NonNull
	@Override
	protected DatabaseType expectedDatabaseType() {
		return DatabaseType.POSTGRESQL;
	}

	@NonNull
	@Override
	protected CapabilityFlags capabilityFlags() {
		return CapabilityFlags.builder()
				.supportsServerSideStreaming(true)
				.supportsSqlArrays(true)
				.supportsNativeJson(true)
				.supportsReturningClause(true)
				.supportsNativeUuid(true)
				.reportsTimestampWithTimeZoneTypeName(true)
				.build();
	}

	private static final class TrackingPostgreSqlStreamDataSource implements DataSource {
		@NonNull
		private final DataSource delegate;
		@NonNull
		private final AtomicBoolean autoCommitDisabled;
		@NonNull
		private final AtomicBoolean autoCommitRestored;
		@NonNull
		private final AtomicBoolean committed;
		@NonNull
		private final AtomicInteger fetchSizeAtExecute;

		private TrackingPostgreSqlStreamDataSource(@NonNull DataSource delegate) {
			this.delegate = requireNonNull(delegate);
			this.autoCommitDisabled = new AtomicBoolean(false);
			this.autoCommitRestored = new AtomicBoolean(false);
			this.committed = new AtomicBoolean(false);
			this.fetchSizeAtExecute = new AtomicInteger(-1);
		}

		@Override
		public Connection getConnection() throws SQLException {
			return wrapConnection(this.delegate.getConnection());
		}

		@Override
		public Connection getConnection(String username, String password) throws SQLException {
			return wrapConnection(this.delegate.getConnection(username, password));
		}

		@Override
		public PrintWriter getLogWriter() throws SQLException {
			return this.delegate.getLogWriter();
		}

		@Override
		public void setLogWriter(PrintWriter out) throws SQLException {
			this.delegate.setLogWriter(out);
		}

		@Override
		public void setLoginTimeout(int seconds) throws SQLException {
			this.delegate.setLoginTimeout(seconds);
		}

		@Override
		public int getLoginTimeout() throws SQLException {
			return this.delegate.getLoginTimeout();
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			return this.delegate.getParentLogger();
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			if (iface.isInstance(this))
				return iface.cast(this);

			return this.delegate.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return iface.isInstance(this) || this.delegate.isWrapperFor(iface);
		}

		private Connection wrapConnection(@NonNull Connection connection) {
			requireNonNull(connection);

			return (Connection) Proxy.newProxyInstance(
					Connection.class.getClassLoader(),
					new Class<?>[]{Connection.class},
					(proxy, method, args) -> {
						Object result = invoke(method, connection, args);
						String methodName = method.getName();

						if ("prepareStatement".equals(methodName) && result instanceof PreparedStatement preparedStatement) {
							return wrapPreparedStatement(preparedStatement);
						} else if ("setAutoCommit".equals(methodName) && args != null && args.length == 1) {
							if (Boolean.FALSE.equals(args[0]))
								this.autoCommitDisabled.set(true);
							else if (Boolean.TRUE.equals(args[0]))
								this.autoCommitRestored.set(true);
						} else if ("commit".equals(methodName)) {
							this.committed.set(true);
						}

						return result;
					});
		}

		private PreparedStatement wrapPreparedStatement(@NonNull PreparedStatement preparedStatement) {
			requireNonNull(preparedStatement);

			return (PreparedStatement) Proxy.newProxyInstance(
					PreparedStatement.class.getClassLoader(),
					new Class<?>[]{PreparedStatement.class},
					(proxy, method, args) -> {
						if ("executeQuery".equals(method.getName()) && (args == null || args.length == 0))
							this.fetchSizeAtExecute.set(preparedStatement.getFetchSize());

						return invoke(method, preparedStatement, args);
					});
		}

		private Object invoke(@NonNull Method method,
													@NonNull Object target,
													Object[] args) throws Throwable {
			requireNonNull(method);
			requireNonNull(target);

			try {
				return method.invoke(target, args);
			} catch (InvocationTargetException e) {
				throw e.getCause();
			}
		}
	}
}
