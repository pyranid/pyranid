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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Verifies the 4.4.1 per-query {@link ResultSetMapper} / {@link PreparedStatementBinder} overrides.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 */
public class QuerySpiOverrideTests {
	@Test
	public void testPerQueryResultSetMapperAppliesToFetchObjectListAndStream() {
		Database db = createDatabase("spi_mapper_shapes");
		db.query("CREATE TABLE words (w VARCHAR(16))").execute();
		db.query("INSERT INTO words VALUES (:w)").bind("w", "one").execute();
		db.query("INSERT INTO words VALUES (:w)").bind("w", "two").execute();

		ResultSetMapper upperCaseMapper = new ResultSetMapper() {
			@NonNull
			@Override
			public <T> Optional<T> map(@NonNull StatementContext<T> statementContext,
																 @NonNull ResultSet resultSet,
																 @NonNull Class<T> resultSetRowType,
																 @NonNull InstanceProvider instanceProvider) throws SQLException {
				return Optional.of(resultSetRowType.cast(resultSet.getString(1).toUpperCase()));
			}
		};

		Optional<String> one = db.query("SELECT w FROM words WHERE w = :w").bind("w", "one")
				.resultSetMapper(upperCaseMapper)
				.fetchObject(String.class);
		Assertions.assertEquals("ONE", one.orElseThrow(), "Expected the override applied on fetchObject");

		List<String> all = db.query("SELECT w FROM words ORDER BY w DESC")
				.resultSetMapper(upperCaseMapper)
				.fetchList(String.class);
		Assertions.assertEquals(List.of("TWO", "ONE"), all, "Expected the override applied per row on fetchList");

		List<String> streamed = db.query("SELECT w FROM words ORDER BY w")
				.resultSetMapper(upperCaseMapper)
				.fetchStream(String.class, stream -> stream.toList());
		Assertions.assertEquals(List.of("ONE", "TWO"), streamed, "Expected the override applied on fetchStream rows");
	}

	@Test
	public void testOverrideIsPerQueryOnlyAndNullRestoresDefault() {
		Database db = createDatabase("spi_mapper_isolation");
		db.query("CREATE TABLE vals (v VARCHAR(16))").execute();
		db.query("INSERT INTO vals VALUES (:v)").bind("v", "plain").execute();

		ResultSetMapper markerMapper = new ResultSetMapper() {
			@NonNull
			@Override
			public <T> Optional<T> map(@NonNull StatementContext<T> statementContext,
																 @NonNull ResultSet resultSet,
																 @NonNull Class<T> resultSetRowType,
																 @NonNull InstanceProvider instanceProvider) {
				return Optional.of(resultSetRowType.cast("MARKER"));
			}
		};

		Assertions.assertEquals("MARKER",
				db.query("SELECT v FROM vals").resultSetMapper(markerMapper).fetchObject(String.class).orElseThrow());

		// A different query on the same Database is unaffected
		Assertions.assertEquals("plain",
				db.query("SELECT v FROM vals").fetchObject(String.class).orElseThrow(),
				"Expected the database-wide default mapper for a query without an override");

		// Setting then clearing the override restores the default
		Assertions.assertEquals("plain",
				db.query("SELECT v FROM vals").resultSetMapper(markerMapper).resultSetMapper(null)
						.fetchObject(String.class).orElseThrow(),
				"Expected null to restore the database-wide mapper");
	}

	@Test
	public void testPerQueryBinderInvokedWithUnwrappedNonNullValues() {
		Database db = createDatabase("spi_binder_basic");
		db.query("CREATE TABLE pairs (a VARCHAR(32), b VARCHAR(32))").execute();

		List<Object> boundParameters = new CopyOnWriteArrayList<>();
		PreparedStatementBinder recordingBinder = new PreparedStatementBinder() {
			@Override
			public <T> void bindParameter(@NonNull StatementContext<T> statementContext,
																		@NonNull PreparedStatement preparedStatement,
																		@NonNull Integer parameterIndex,
																		@NonNull Object parameter) throws SQLException {
				boundParameters.add(parameter);
				preparedStatement.setObject(parameterIndex, parameter);
			}
		};

		db.query("INSERT INTO pairs (a, b) VALUES (:a, :b)")
				.bind("a", Parameters.secure("secret-bind-value"))
				.bind("b", "plain-value")
				.preparedStatementBinder(recordingBinder)
				.execute();

		Assertions.assertEquals(2, boundParameters.size(), "Expected the override to bind both parameters");
		Assertions.assertTrue(boundParameters.contains("secret-bind-value"),
				"Expected the secure parameter unwrapped before reaching the binder");
		Assertions.assertFalse(boundParameters.stream().anyMatch(p -> p instanceof SecureParameter),
				"The binder must never see a SecureParameter wrapper");

		// Verify the bind actually landed
		Assertions.assertEquals("secret-bind-value",
				db.query("SELECT a FROM pairs").fetchObject(String.class).orElseThrow());
	}

	@Test
	public void testNullParametersNeverReachOverrideBinder() {
		Database db = createDatabase("spi_binder_null");
		db.query("CREATE TABLE nullable_vals (v VARCHAR(32))").execute();

		AtomicInteger binderInvocations = new AtomicInteger();
		PreparedStatementBinder countingBinder = new PreparedStatementBinder() {
			@Override
			public <T> void bindParameter(@NonNull StatementContext<T> statementContext,
																		@NonNull PreparedStatement preparedStatement,
																		@NonNull Integer parameterIndex,
																		@NonNull Object parameter) throws SQLException {
				binderInvocations.incrementAndGet();
				preparedStatement.setObject(parameterIndex, parameter);
			}
		};

		db.query("INSERT INTO nullable_vals (v) VALUES (:v)")
				.bind("v", null)
				.preparedStatementBinder(countingBinder)
				.execute();

		Assertions.assertEquals(0, binderInvocations.get(),
				"Expected Pyranid's setNull handling for null parameters even with an override present");
		Assertions.assertEquals(1, (long) db.query("SELECT COUNT(*) FROM nullable_vals").fetchObject(Long.class).orElseThrow());
	}

	@Test
	public void testPerQueryBinderAppliesToBatchGroupsAndInListElements() {
		Database db = createDatabase("spi_binder_batch");
		db.query("CREATE TABLE batched (v INTEGER)").execute();

		AtomicInteger binderInvocations = new AtomicInteger();
		PreparedStatementBinder countingBinder = new PreparedStatementBinder() {
			@Override
			public <T> void bindParameter(@NonNull StatementContext<T> statementContext,
																		@NonNull PreparedStatement preparedStatement,
																		@NonNull Integer parameterIndex,
																		@NonNull Object parameter) throws SQLException {
				binderInvocations.incrementAndGet();
				preparedStatement.setObject(parameterIndex, parameter);
			}
		};

		db.query("INSERT INTO batched (v) VALUES (:v)")
				.preparedStatementBinder(countingBinder)
				.executeBatch(List.of(Map.of("v", 1), Map.of("v", 2), Map.of("v", 3)));

		Assertions.assertEquals(3, binderInvocations.get(), "Expected the override to bind every batch group");

		binderInvocations.set(0);
		List<Integer> matched = db.query("SELECT v FROM batched WHERE v IN (:vs) ORDER BY v")
				.bind("vs", Parameters.inList(new int[]{1, 3}))
				.preparedStatementBinder(countingBinder)
				.fetchList(Integer.class);

		Assertions.assertEquals(List.of(1, 3), matched);
		Assertions.assertEquals(2, binderInvocations.get(), "Expected the override to bind every expanded IN-list element");
	}

	@Test
	public void testPerQueryBinderAppliesToChunkedBatchesAndStreamPath() {
		Database db = createDatabase("spi_binder_chunk_stream");
		db.query("CREATE TABLE chunked (v INTEGER)").execute();

		AtomicInteger binderInvocations = new AtomicInteger();
		PreparedStatementBinder countingBinder = new PreparedStatementBinder() {
			@Override
			public <T> void bindParameter(@NonNull StatementContext<T> statementContext,
																		@NonNull PreparedStatement preparedStatement,
																		@NonNull Integer parameterIndex,
																		@NonNull Object parameter) throws SQLException {
				binderInvocations.incrementAndGet();
				preparedStatement.setObject(parameterIndex, parameter);
			}
		};

		// Chunked batch: 5 groups with chunk size 2 → 3 JDBC batches, all through the override
		db.query("INSERT INTO chunked (v) VALUES (:v)")
				.batchChunkSize(2)
				.preparedStatementBinder(countingBinder)
				.executeBatch(List.of(Map.of("v", 1), Map.of("v", 2), Map.of("v", 3), Map.of("v", 4), Map.of("v", 5)));

		Assertions.assertEquals(5, binderInvocations.get(),
				"Expected the override to bind every group across chunked JDBC batches");

		// Stream path: bound parameter flows through the override
		binderInvocations.set(0);
		List<Integer> streamed = db.query("SELECT v FROM chunked WHERE v > :min ORDER BY v")
				.bind("min", 3)
				.preparedStatementBinder(countingBinder)
				.fetchStream(Integer.class, stream -> stream.toList());

		Assertions.assertEquals(List.of(4, 5), streamed);
		Assertions.assertEquals(1, binderInvocations.get(), "Expected the override to bind the stream query's parameter");
	}

	@Test
	public void testStatementContextEqualityUnaffectedByOverrides() {
		Database db = createDatabase("spi_context_equality");
		Statement statement = Statement.of("equality-test", "SELECT 1");

		ResultSetMapper markerMapper = new ResultSetMapper() {
			@NonNull
			@Override
			public <T> Optional<T> map(@NonNull StatementContext<T> statementContext,
																 @NonNull ResultSet resultSet,
																 @NonNull Class<T> resultSetRowType,
																 @NonNull InstanceProvider instanceProvider) {
				return Optional.empty();
			}
		};

		StatementContext<String> withoutOverrides = StatementContext.<String>with(statement, db)
				.resultSetRowType(String.class)
				.parameters(List.of("p"))
				.build();
		StatementContext<String> withOverrides = StatementContext.<String>with(statement, db)
				.resultSetRowType(String.class)
				.parameters(List.of("p"))
				.spiOverrides(new StatementContext.SpiOverrides(markerMapper, null))
				.build();

		Assertions.assertEquals(withoutOverrides, withOverrides,
				"SPI overrides are functional identity and must not participate in equals()");
		Assertions.assertEquals(withoutOverrides.hashCode(), withOverrides.hashCode(),
				"SPI overrides must not participate in hashCode()");
		Assertions.assertEquals(withoutOverrides.toString(), withOverrides.toString(),
				"SPI overrides must not appear in toString()");
	}

	@Test
	public void testPerQueryMapperWinsOverMapRowTarget() {
		Database db = createDatabase("spi_mapper_map_target");

		ResultSetMapper markerMapper = new ResultSetMapper() {
			@NonNull
			@Override
			public <T> Optional<T> map(@NonNull StatementContext<T> statementContext,
																 @NonNull ResultSet resultSet,
																 @NonNull Class<T> resultSetRowType,
																 @NonNull InstanceProvider instanceProvider) {
				return Optional.of(resultSetRowType.cast(Map.of("marker", "custom")));
			}
		};

		Optional<Map> row = db.query("SELECT 1 AS a FROM (VALUES (0)) AS t(x)")
				.resultSetMapper(markerMapper)
				.fetchObject(Map.class);

		Assertions.assertEquals("custom", row.orElseThrow().get("marker"),
				"Expected a per-query mapper to take precedence over the built-in Map row target");
	}

	@Test
	public void testStatementLoggingUnaffectedByOverrides() {
		AtomicReference<StatementLog<?>> loggedStatementLog = new AtomicReference<>();
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl("jdbc:hsqldb:mem:spi_logging");
		dataSource.setUser("sa");
		dataSource.setPassword("");
		Database db = Database.withDataSource(dataSource)
				.statementLogger(loggedStatementLog::set)
				.build();

		ResultSetMapper markerMapper = new ResultSetMapper() {
			@NonNull
			@Override
			public <T> Optional<T> map(@NonNull StatementContext<T> statementContext,
																 @NonNull ResultSet resultSet,
																 @NonNull Class<T> resultSetRowType,
																 @NonNull InstanceProvider instanceProvider) {
				return Optional.of(resultSetRowType.cast("MARKER"));
			}
		};

		db.query("SELECT 1 FROM (VALUES (0)) AS t(x)").resultSetMapper(markerMapper).fetchObject(String.class);

		Assertions.assertNotNull(loggedStatementLog.get(), "Expected statement logging to fire with an override present");
		Assertions.assertTrue(loggedStatementLog.get().getException().isEmpty());
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
