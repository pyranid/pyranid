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
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.1.0
 */
@Testcontainers
public class PostgreSQLIntegrationIT {
	private static final String POSTGRES_IMAGE_NAME =
			System.getProperty("postgres.integration.image", "postgres:17-alpine");
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
		AtomicInteger fetchSize = new AtomicInteger(-1);

		List<Integer> values = db.query("SELECT generate_series(1, 3)")
				.customize((statementContext, preparedStatement) -> {
					sawAutoCommitDisabled.set(!preparedStatement.getConnection().getAutoCommit());
					fetchSize.set(preparedStatement.getFetchSize());
				})
				.fetchStream(Integer.class, stream -> stream.toList());

		Assertions.assertEquals(List.of(1, 2, 3), values);
		Assertions.assertTrue(sawAutoCommitDisabled.get());
		Assertions.assertEquals(256, fetchSize.get());
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

	private DataSource dataSource() {
		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setUrl(POSTGRES.getJdbcUrl());
		dataSource.setUser(POSTGRES.getUsername());
		dataSource.setPassword(POSTGRES.getPassword());
		return dataSource;
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

		private TrackingPostgreSqlStreamDataSource(@NonNull DataSource delegate) {
			this.delegate = requireNonNull(delegate);
			this.autoCommitDisabled = new AtomicBoolean(false);
			this.autoCommitRestored = new AtomicBoolean(false);
			this.committed = new AtomicBoolean(false);
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

						if ("setAutoCommit".equals(methodName) && args != null && args.length == 1) {
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
