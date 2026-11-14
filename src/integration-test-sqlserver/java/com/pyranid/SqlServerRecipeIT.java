/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/** Executes the SQL Server guide's complete batch and isolation recipes against a real server. */
@Testcontainers
@Timeout(value = 45, unit = TimeUnit.SECONDS)
class SqlServerRecipeIT {
	@Container
	private static final MSSQLServerContainer SQL_SERVER = new MSSQLServerContainer(DockerImageName.parse(
			System.getProperty("sqlserver.integration.image", "mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04"))
			.asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server")).acceptLicense();
	private static final String DATABASE_NAME = "recipe_" + UUID.randomUUID().toString().replace("-", "");
	private static final String ISOLATION_LEVEL_SQL = "SELECT CAST(transaction_isolation_level AS INT) "
			+ "FROM sys.dm_exec_sessions WHERE session_id = @@SPID";

	@BeforeAll
	static void createSnapshotEnabledDatabase() {
		SQLServerDataSource source = source(false, false);
		Database admin = Database.with(source, DatabaseType.SQL_SERVER).build();
		admin.query("CREATE DATABASE " + DATABASE_NAME).execute();
		admin.query("ALTER DATABASE " + DATABASE_NAME + " SET ALLOW_SNAPSHOT_ISOLATION ON").execute();
	}

	@Test
	void singleBatchOutputIntoReturnsExactIdsAndTriggerEffectsInAndOutOfTransactions() {
		Database database = database(false);
		for (boolean transaction : new boolean[]{false, true}) {
			try (Fixture fixture = new Fixture(database)) {
				List<Long> ids = transaction
						? database.transaction(() -> java.util.Optional.of(insertWithOutput(database, fixture))).orElseThrow()
						: insertWithOutput(database, fixture);
				assertEquals(List.of(2L, 3L), ids);
				assertEquals(List.of(1L, 2L, 3L), fixture.ids());
				assertEquals(List.of("Ada", "Existing", "Grace"), fixture.names());
				assertEquals(fixture.ids(), fixture.auditIds());
				assertEquals(fixture.names(), fixture.auditNames());
			}
		}
	}

	@Test
	void outputRowsAndTriggerEffectsRollBackWithTheirOwningTransaction() {
		Database database = database(false);
		try (Fixture fixture = new Fixture(database)) {
			database.transaction(() -> {
				assertEquals(List.of(2L, 3L), insertWithOutput(database, fixture));
				assertEquals(List.of(1L, 2L, 3L), fixture.auditIds());
				database.currentTransaction().orElseThrow().setRollbackOnly(true);
			});
			assertEquals(List.of(1L), fixture.ids());
			assertEquals(List.of("Existing"), fixture.names());
			assertEquals(List.of(1L), fixture.auditIds());
			assertEquals(List.of("Existing"), fixture.auditNames());
		}
	}

	@Test
	void separatePreparedTempTableAndIsolationStatementsDoNotEscapeTheirDynamicScope() {
		Database database = database(false);
		try (Fixture fixture = new Fixture(database)) {
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.transaction(() -> {
				database.query("CREATE TABLE #employee_insert_result (employee_id BIGINT NOT NULL)").execute();
				database.query("INSERT INTO " + fixture.table + " (name) OUTPUT inserted.employee_id "
						+ "INTO #employee_insert_result VALUES (:firstName), (:secondName)")
						.bind("firstName", "Ada").bind("secondName", "Grace").execute();
			}));
			assertEquals(208, failure.getErrorCode().orElseThrow());
			assertEquals(List.of(1L), fixture.ids());
			assertEquals(List.of("Existing"), fixture.names());
			assertEquals(List.of(1L), fixture.auditIds());
		}
		database.transaction(() -> {
			database.query("SET TRANSACTION ISOLATION LEVEL SNAPSHOT").execute();
			assertEquals(2, level(database), "The separate prepared SET returned to READ COMMITTED");
		});
		database.transaction(() -> {
			assertEquals(5, database.query("SET TRANSACTION ISOLATION LEVEL SNAPSHOT; " + ISOLATION_LEVEL_SQL)
					.fetchObject(Integer.class).orElseThrow(), "The setting is visible inside its own batch");
			assertEquals(2, level(database), "The later prepared batch still uses READ COMMITTED");
		});
		database.transaction(TransactionOptions.withIsolation(TransactionIsolation.SERIALIZABLE).build(), () ->
				assertEquals(4, level(database), "TransactionOptions configures JDBC isolation before the work"));
	}

	@Test
	void dedicatedSnapshotSourceAndDefaultOptionsPreserveReadsAcrossAConcurrentCommit() throws Exception {
		Database ordinary = database(false);
		String table = "snapshot_rows_" + UUID.randomUUID().toString().replace("-", "");
		ordinary.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, value INT NOT NULL)").execute();
		ordinary.query("INSERT INTO " + table + " VALUES (1, 10)").execute();
		SQLServerDataSource snapshotSource = source(true, true);
		try (Connection connection = snapshotSource.getConnection()) {
			assertEquals(SQLServerConnection.TRANSACTION_SNAPSHOT, connection.getTransactionIsolation());
		}
		Database snapshot = Database.with(snapshotSource, DatabaseType.SQL_SERVER).build();
		ExecutorService writer = Executors.newSingleThreadExecutor();
		try {
			snapshot.transaction(TransactionOptions.withIsolation(TransactionIsolation.DEFAULT).build(), () -> {
				assertEquals(DATABASE_NAME, snapshot.query("SELECT DB_NAME()").fetchObject(String.class).orElseThrow());
				assertEquals(5, level(snapshot));
				assertEquals(10, snapshot.query("SELECT value FROM " + table + " WHERE id = 1").fetchObject(Integer.class).orElseThrow());
				assertEquals(1L, await(writer.submit(() -> ordinary.query("UPDATE " + table + " SET value = 20 WHERE id = 1")
						.queryTimeout(Duration.ofSeconds(5)).execute())));
				assertEquals(10, snapshot.query("SELECT value FROM " + table + " WHERE id = 1").fetchObject(Integer.class).orElseThrow(),
						"The snapshot must retain its first committed view across subsequent reads");
				assertEquals(5, level(snapshot));
			});
			assertEquals(20, ordinary.query("SELECT value FROM " + table + " WHERE id = 1").fetchObject(Integer.class).orElseThrow());
			snapshot.transaction(TransactionOptions.withIsolation(TransactionIsolation.SERIALIZABLE).build(), () -> assertEquals(4, level(snapshot)));
		} finally {
			writer.shutdownNow();
			assertTrue(writer.awaitTermination(10, TimeUnit.SECONDS), "Concurrent writer did not stop before cleanup");
			ordinary.query("DROP TABLE " + table).queryTimeout(Duration.ofSeconds(5)).execute();
		}
	}

	private static List<Long> insertWithOutput(Database database, Fixture fixture) {
		return database.query("""
			SET NOCOUNT ON;
			DECLARE @employee_insert_result TABLE (employee_id BIGINT NOT NULL);
			INSERT INTO %s (name)
			OUTPUT inserted.employee_id INTO @employee_insert_result
			VALUES (:firstName), (:secondName);
			SELECT employee_id FROM @employee_insert_result ORDER BY employee_id;
			""".formatted(fixture.table)).bind("firstName", "Ada").bind("secondName", "Grace").executeForList(Long.class);
	}

	private static int level(Database database) {
		return database.query(ISOLATION_LEVEL_SQL).fetchObject(Integer.class).orElseThrow();
	}

	private static Database database(boolean snapshot) {
		return Database.with(source(snapshot, true), DatabaseType.SQL_SERVER).build();
	}

	private static SQLServerDataSource source(boolean snapshot, boolean testDatabase) {
		SQLServerDataSource source = new SQLServerDataSource();
		source.setURL(SQL_SERVER.getJdbcUrl());
		source.setUser(SQL_SERVER.getUsername());
		source.setPassword(SQL_SERVER.getPassword());
		if (testDatabase) source.setDatabaseName(DATABASE_NAME);
		source.setDefaultTransactionIsolation(snapshot ? "SNAPSHOT" : "READ_COMMITTED");
		return source;
	}

	private static <T> T await(Future<T> future) {
		try {
			return future.get(10, TimeUnit.SECONDS);
		} catch (InterruptedException failure) {
			Thread.currentThread().interrupt();
			throw new AssertionError("Concurrent writer was interrupted", failure);
		} catch (ExecutionException | TimeoutException failure) {
			throw new AssertionError("Concurrent writer failed to commit", failure);
		}
	}

	private static final class Fixture implements AutoCloseable {
		private final Database database;
		private final String table = "recipe_employee_" + UUID.randomUUID().toString().replace("-", "");
		private final String audit = "recipe_audit_" + UUID.randomUUID().toString().replace("-", "");

		private Fixture(Database database) {
			this.database = database;
			database.query("CREATE TABLE " + this.table + " (employee_id BIGINT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(64) NOT NULL)").execute();
			database.query("CREATE TABLE " + this.audit + " (employee_id BIGINT NOT NULL, name VARCHAR(64) NOT NULL)").execute();
			database.query("CREATE TRIGGER " + this.table + "_ai ON " + this.table + " AFTER INSERT AS BEGIN SET NOCOUNT ON; "
					+ "INSERT INTO " + this.audit + " (employee_id, name) SELECT employee_id, name FROM inserted; END").execute();
			database.query("INSERT INTO " + this.table + " (name) VALUES ('Existing')").execute();
		}

		private List<Long> ids() { return this.database.query("SELECT employee_id FROM " + this.table + " ORDER BY employee_id").fetchList(Long.class); }
		private List<String> names() { return this.database.query("SELECT name FROM " + this.table + " ORDER BY name").fetchList(String.class); }
		private List<Long> auditIds() { return this.database.query("SELECT employee_id FROM " + this.audit + " ORDER BY employee_id").fetchList(Long.class); }
		private List<String> auditNames() { return this.database.query("SELECT name FROM " + this.audit + " ORDER BY name").fetchList(String.class); }
		@Override public void close() {
			this.database.query("DROP TABLE " + this.table).execute();
			this.database.query("DROP TABLE " + this.audit).execute();
		}
	}
}
