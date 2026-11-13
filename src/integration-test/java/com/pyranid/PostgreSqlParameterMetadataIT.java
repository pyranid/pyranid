/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.jdbc.AutoSave;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlParameterMetadataIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_metadata").withUsername("pyranid").withPassword("pyranid");
	private static final Instant NOW = Instant.parse("2024-06-15T12:34:56.123456Z");

	@Test
	void validTemporalArithmeticWorksBeforeAndAfterEarlierWorkWithDriverAutosaveModes() {
		for (AutoSave autosave : List.of(AutoSave.NEVER, AutoSave.ALWAYS, AutoSave.CONSERVATIVE)) {
			for (boolean precedingWrite : new boolean[]{false, true}) {
				Fixture fixture = new Fixture(autosave, ZoneId.of("UTC"));
				TransactionOutcome<Long> outcome = fixture.database.transactionWithOutcome(() -> {
					if (precedingWrite) fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (2)").execute();
					return Optional.of(fixture.database.query("UPDATE " + fixture.table
							+ " SET zoned = :now WHERE id = 1 AND last_login < :cutoff - interval '30 days'")
							.bind("now", NOW).bind("cutoff", NOW).execute());
				});
				assertEquals(TransactionResult.COMMITTED, outcome.getResult(), autosave + " preceding=" + precedingWrite);
				assertTrue(outcome.getFailure().isEmpty());
				assertEquals(Optional.of(1L), outcome.getAttemptedValue());
				assertEquals(NOW, fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id = 1")
						.fetchObject(Instant.class).orElseThrow());
				assertEquals(precedingWrite ? List.of(1, 2) : List.of(1), fixture.ids());
				assertEquals(1, fixture.descriptions.get());
				assertEquals(1, fixture.savepoints.get());
				assertEquals(1, fixture.rollbacks.get());
				assertEquals(1, fixture.releases.get());
			}
		}
	}

	@Test
	void timestampInferenceStillUsesTheConfiguredZoneAndPreservesTypedNulls() {
		ZoneId zone = ZoneId.of("America/Los_Angeles");
		Fixture fixture = new Fixture(AutoSave.NEVER, zone);
		fixture.database.transaction(() -> {
			fixture.database.query("UPDATE " + fixture.table + " SET last_login = last_login WHERE id = 1").execute();
			fixture.database.query("INSERT INTO " + fixture.table + " (id, plain, zoned, nullable_integer) VALUES (:id, :plain, :zoned, :nil)")
					.bind("id", 2).bind("plain", NOW).bind("zoned", NOW).bind("nil", Optional.empty()).execute();
		});
		assertEquals(LocalDateTime.ofInstant(NOW, zone), fixture.inspection.query("SELECT plain FROM " + fixture.table + " WHERE id = 2")
				.fetchObject(LocalDateTime.class).orElseThrow());
		assertEquals(NOW, fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id = 2")
				.fetchObject(Instant.class).orElseThrow());
		assertTrue(fixture.inspection.query("SELECT nullable_integer IS NULL FROM " + fixture.table + " WHERE id = 2")
				.fetchObject(Boolean.class).orElseThrow());
		assertEquals(1, fixture.descriptions.get());
		assertEquals(3, fixture.savepoints.get());
		assertEquals(0, fixture.rollbacks.get());
		assertEquals(3, fixture.releases.get());
	}

	@Test
	void metadataAndProtectionCallsAreBoundedByParametersInsteadOfBatchRowsAndChunks() {
		for (Integer chunkSize : new Integer[]{null, 4}) {
			Fixture fixture = new Fixture(AutoSave.NEVER, ZoneId.of("UTC"));
			List<Map<String, Object>> rows = new ArrayList<>();
			for (int index = 2; index <= 26; index++)
				rows.add(Map.of("id", index, "plain", Optional.empty(), "zoned", NOW));
			fixture.database.transaction(() -> {
				Query query = fixture.database.query("INSERT INTO " + fixture.table + " (id, plain, zoned) VALUES (:id, :plain, :zoned)");
				if (chunkSize != null) query.batchChunkSize(chunkSize);
				assertEquals(25, query.executeBatch(rows).size());
			});
			assertEquals(26, fixture.ids().size());
			assertEquals(1, fixture.descriptions.get());
			assertEquals(2, fixture.savepoints.get());
			assertEquals(0, fixture.rollbacks.get());
			assertEquals(2, fixture.releases.get());
		}
	}

	@Test
	void rawBinderRecoversDescribeFailureWithoutExposingRawTransactionControls() {
		Fixture fixture = new Fixture(AutoSave.NEVER, ZoneId.of("UTC"));
		String sql = "UPDATE " + fixture.table + " SET zoned = ? WHERE id = 1 AND last_login < ? - interval '30 days'";
		StatementContext<?> context = StatementContext.with(Statement.of("raw-described-update", sql), fixture.database).build();
		fixture.database.transaction(() -> {
			fixture.database.query("INSERT INTO " + fixture.table + " (id) VALUES (2)").execute();
			fixture.database.useRawConnection(connection -> {
				assertThrows(IllegalStateException.class, connection::setSavepoint);
				try (PreparedStatement statement = connection.prepareStatement(sql)) {
					PreparedStatementBinder binder = PreparedStatementBinder.withDefaultConfiguration();
					binder.bindParameter(context, statement, 1, NOW);
					binder.bindParameter(context, statement, 2, NOW);
					assertEquals(1, statement.executeUpdate());
				}
				return Optional.empty();
			});
		});
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(NOW, fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id = 1")
				.fetchObject(Instant.class).orElseThrow());
		// Direct binder calls remain independently scoped; both ambiguous describes are safely recovered.
		assertEquals(2, fixture.descriptions.get());
		assertEquals(2, fixture.savepoints.get());
		assertEquals(2, fixture.rollbacks.get());
		assertEquals(2, fixture.releases.get());
	}

	@Test
	void autocommitDescribeFallbackStillExecutesTheValidTemporalStatement() {
		Fixture fixture = new Fixture(AutoSave.NEVER, ZoneId.of("UTC"));
		assertEquals(1L, fixture.database.query("UPDATE " + fixture.table
				+ " SET zoned = :now WHERE id = 1 AND last_login < :cutoff - interval '30 days'")
				.bind("now", NOW).bind("cutoff", NOW).execute());
		assertEquals(1, fixture.descriptions.get());
		assertEquals(0, fixture.savepoints.get());
		assertEquals(NOW, fixture.inspection.query("SELECT zoned FROM " + fixture.table + " WHERE id = 1")
				.fetchObject(Instant.class).orElseThrow());
	}

	private static final class Fixture {
		final Database database, inspection;
		final String table = "metadata_" + UUID.randomUUID().toString().replace("-", "");
		final AtomicInteger descriptions = new AtomicInteger(), savepoints = new AtomicInteger(),
				rollbacks = new AtomicInteger(), releases = new AtomicInteger();

		Fixture(AutoSave autoSave, ZoneId zone) {
			PGSimpleDataSource source = new PGSimpleDataSource();
			source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
			source.setOptions("-c TimeZone=UTC"); source.setAutosave(autoSave);
			this.inspection = Database.with(source, DatabaseType.POSTGRESQL).build();
			this.inspection.query("CREATE TABLE " + this.table
					+ " (id INT PRIMARY KEY, last_login TIMESTAMPTZ, plain TIMESTAMP, zoned TIMESTAMPTZ, nullable_integer INT)").execute();
			this.inspection.query("INSERT INTO " + this.table + " (id, last_login) VALUES (1, TIMESTAMPTZ '2020-01-02T00:00:00Z')").execute();
			DataSource counted = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(source, method, args);
				if (!(result instanceof Connection connection)) return result;
				Connection[] handle = new Connection[1];
				handle[0] = proxy(Connection.class, (jdbc, values) -> {
					String name = jdbc.getName();
					if (name.equals("setSavepoint")) this.savepoints.incrementAndGet();
					if (name.equals("rollback") && values != null && values.length == 1) this.rollbacks.incrementAndGet();
					if (name.equals("releaseSavepoint")) this.releases.incrementAndGet();
					Object value = invoke(connection, jdbc, values);
					if (!(value instanceof PreparedStatement statement)) return value;
					return proxy(PreparedStatement.class, (call, parameters) -> {
						if (call.getName().equals("getConnection")) return handle[0];
						if (call.getName().equals("getParameterMetaData")) this.descriptions.incrementAndGet();
						return invoke(statement, call, parameters);
					});
				});
				return handle[0];
			});
			this.database = Database.with(counted, DatabaseType.POSTGRESQL).timeZone(zone).build();
		}

		List<Integer> ids() { return this.inspection.query("SELECT id FROM " + this.table + " ORDER BY id").fetchList(Integer.class); }
	}

	@FunctionalInterface private interface Call { Object run(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> call.run(method, args)));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
