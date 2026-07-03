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
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.3.0
 */
@Testcontainers
public class SqlServerIntegrationIT extends AbstractPortableJdbcIntegrationTests {
	public record MergeActionRow(String mergeAction, Long id, String name) {}

	private static final String SQL_SERVER_IMAGE_NAME =
			System.getProperty("sqlserver.integration.image", "mcr.microsoft.com/mssql/server:2022-CU25-ubuntu-22.04");
	private static final DockerImageName SQL_SERVER_IMAGE = DockerImageName.parse(SQL_SERVER_IMAGE_NAME)
			.asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server");

	@Container
	private static final MSSQLServerContainer<?> SQL_SERVER = new MSSQLServerContainer<>(SQL_SERVER_IMAGE)
			.acceptLicense();

	@Test
	public void testSqlServerOutputMapsMultipleGeneratedRows() {
		Database db = database();
		String table = "pyranid_sqlserver_output_keys";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT IDENTITY(1,1) PRIMARY KEY, "
				+ "name VARCHAR(64) NOT NULL"
				+ ")");

		List<Long> ids = db.query("INSERT INTO " + table + " (name) OUTPUT inserted.id VALUES (:firstName), (:secondName)")
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
	public void testSqlServerOutputIntoWorksForTriggerTable() {
		Database db = database();
		String table = "pyranid_sqlserver_trigger_output";
		String outputTable = "pyranid_sqlserver_trigger_output_ids";
		String logTable = "pyranid_sqlserver_trigger_output_log";
		recreateTable(db, outputTable, "CREATE TABLE " + outputTable + " (id BIGINT NOT NULL)");
		recreateTable(db, logTable, "CREATE TABLE " + logTable + " (name VARCHAR(64) NOT NULL)");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT IDENTITY(1,1) PRIMARY KEY, "
				+ "name VARCHAR(64) NOT NULL"
				+ ")");
		db.query("CREATE TRIGGER " + table + "_ai ON " + table + " AFTER INSERT AS "
						+ "INSERT INTO " + logTable + " (name) SELECT name FROM inserted")
				.execute();

		DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO " + table + " (name) OUTPUT inserted.id VALUES (:name)")
						.bind("name", "plain-output")
						.executeForList(Long.class));

		Assertions.assertTrue(exception.getMessage().contains("enabled triggers"),
				"SQL Server should reject plain OUTPUT against a table with enabled triggers");
		Assertions.assertEquals(Long.valueOf(0L), db.query("SELECT COUNT(*) FROM " + table)
				.fetchObject(Long.class)
				.orElseThrow());

		db.query("INSERT INTO " + table + " (name) OUTPUT inserted.id INTO " + outputTable + " VALUES (:firstName), (:secondName)")
				.bind("firstName", "Ada")
				.bind("secondName", "Grace")
				.execute();

		List<Long> ids = db.query("SELECT id FROM " + outputTable + " ORDER BY id")
				.fetchList(Long.class);
		Assertions.assertEquals(2, ids.size());
		Assertions.assertTrue(ids.get(0) > 0L);
		Assertions.assertTrue(ids.get(1) > ids.get(0));
		Assertions.assertEquals(List.of("Ada", "Grace"), db.query("SELECT name FROM " + table + " ORDER BY id")
				.fetchList(String.class));
		Assertions.assertEquals(List.of("Ada", "Grace"), db.query("SELECT name FROM " + logTable + " ORDER BY name")
				.fetchList(String.class));
	}

	@Test
	public void testSqlServerMergeOutputsActionAndRows() {
		Database db = database();
		String table = "pyranid_sqlserver_merge_items";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id BIGINT IDENTITY(1,1) PRIMARY KEY, "
				+ "email VARCHAR(100) NOT NULL UNIQUE, "
				+ "name VARCHAR(64) NOT NULL"
				+ ")");
		Long existingId = db.query("INSERT INTO " + table + " (email, name) OUTPUT inserted.id VALUES (:email, :name)")
				.bind("email", "ada@example.com")
				.bind("name", "Ada")
				.executeForObject(Long.class)
				.orElseThrow();

		List<MergeActionRow> rows = db.query("MERGE " + table + " AS target "
						+ "USING (VALUES (:existingEmail, :updatedName), (:newEmail, :newName)) AS source(email, name) "
						+ "ON target.email = source.email "
						+ "WHEN MATCHED THEN UPDATE SET name = source.name "
						+ "WHEN NOT MATCHED THEN INSERT (email, name) VALUES (source.email, source.name) "
						+ "OUTPUT $action AS merge_action, inserted.id, inserted.name;")
				.bind("existingEmail", "ada@example.com")
				.bind("updatedName", "Ada Updated")
				.bind("newEmail", "grace@example.com")
				.bind("newName", "Grace")
				.executeForList(MergeActionRow.class);

		Assertions.assertEquals(2, rows.size());
		Assertions.assertTrue(rows.stream().anyMatch(row ->
				"UPDATE".equals(row.mergeAction()) && existingId.equals(row.id()) && "Ada Updated".equals(row.name())));
		Assertions.assertTrue(rows.stream().anyMatch(row ->
				"INSERT".equals(row.mergeAction()) && row.id() > existingId && "Grace".equals(row.name())));
		Assertions.assertEquals(List.of("Ada Updated", "Grace"), db.query("SELECT name FROM " + table + " ORDER BY email")
				.fetchList(String.class));
	}

	@Test
	public void testSqlServerUniqueIdentifierUuidRoundTrip() {
		Database db = database();
		String table = "pyranid_sqlserver_uuid_items";
		UUID id = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id UNIQUEIDENTIFIER PRIMARY KEY, "
				+ "name VARCHAR(64) NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (id, name) VALUES (:id, :name)")
				.bind("id", id)
				.bind("name", "native uuid")
				.execute();

		Assertions.assertEquals(id, db.query("SELECT id FROM " + table)
				.fetchObject(UUID.class)
				.orElseThrow());
		Assertions.assertEquals(id.toString(), db.query("SELECT LOWER(CONVERT(varchar(36), id)) FROM " + table)
				.fetchObject(String.class)
				.orElseThrow());
	}

	@Test
	public void testSqlServerDatetimeOffsetRoundTrip() {
		Database db = Database.withDataSource(dataSource())
				.timeZone(ZoneId.of("UTC"))
				.build();
		String table = "pyranid_sqlserver_temporal_items";
		OffsetDateTime eventAt = OffsetDateTime.parse("2020-11-01T01:30:15.123456700-04:00");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id INT PRIMARY KEY, "
				+ "event_at DATETIMEOFFSET(7) NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (id, event_at) VALUES (:id, :eventAt)")
				.bind("id", 1)
				.bind("eventAt", eventAt)
				.execute();

		Assertions.assertEquals(eventAt, db.query("SELECT event_at FROM " + table)
				.fetchObject(OffsetDateTime.class)
				.orElseThrow());
		Assertions.assertEquals(eventAt.toInstant(), db.query("SELECT event_at FROM " + table)
				.fetchObject(Instant.class)
				.orElseThrow());
	}

	@NonNull
	@Override
	protected DataSource dataSource() {
		return new DriverManagerDataSource(SQL_SERVER.getJdbcUrl(), SQL_SERVER.getUsername(), SQL_SERVER.getPassword());
	}

	@NonNull
	@Override
	protected DatabaseType expectedDatabaseType() {
		return DatabaseType.SQL_SERVER;
	}

	@NonNull
	@Override
	protected DialectProfile dialectProfile() {
		return new DialectProfile() {
			@NonNull
			@Override
			String integer() {
				return "INT";
			}

			@NonNull
			@Override
			String doublePrecision() {
				return "FLOAT";
			}

			@NonNull
			@Override
			String timestamp() {
				return "DATETIME2";
			}

			@NonNull
			@Override
			String timestampWithFractionalSeconds() {
				return "DATETIME2(6)";
			}

			@NonNull
			@Override
			String autoIncrementPrimaryKey(@NonNull String columnName) {
				return columnName + " BIGINT IDENTITY(1,1) PRIMARY KEY";
			}
		};
	}

	@NonNull
	@Override
	protected CapabilityFlags capabilityFlags() {
		return CapabilityFlags.builder()
				.supportsReadOnlyTransactions(false)
				.supportsOutputClause(true)
				.build();
	}
}
