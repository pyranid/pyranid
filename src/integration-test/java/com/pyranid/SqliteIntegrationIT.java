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
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
public class SqliteIntegrationIT extends AbstractPortableJdbcIntegrationTests {
	public record DecimalTextRow(BigDecimal amount) {}

	@TempDir
	private Path tempDir;

	@Test
	public void testSqliteReturningMapsMultipleGeneratedRows() {
		Database db = database();
		String table = "pyranid_sqlite_returning_keys";
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id INTEGER PRIMARY KEY AUTOINCREMENT, "
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
	public void testSqliteUuidTextRoundTrip() {
		Database db = database();
		String table = "pyranid_sqlite_uuid_items";
		UUID id = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id TEXT PRIMARY KEY, "
				+ "name TEXT NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (id, name) VALUES (:id, :name)")
				.bind("id", id)
				.bind("name", "text uuid")
				.execute();

		Assertions.assertEquals(id, db.query("SELECT id FROM " + table)
				.fetchObject(UUID.class)
				.orElseThrow());
		Assertions.assertEquals(id.toString(), db.query("SELECT id FROM " + table)
				.fetchObject(String.class)
				.orElseThrow());
	}

	@Test
	public void testSqliteDecimalTextPreservesBigDecimalPrecision() {
		Database db = database();
		String table = "pyranid_sqlite_decimal_text_items";
		BigDecimal amount = new BigDecimal("12345678901234567890.123456789012345678");
		recreateTable(db, table, "CREATE TABLE " + table + " ("
				+ "id INTEGER PRIMARY KEY AUTOINCREMENT, "
				+ "amount TEXT NOT NULL"
				+ ")");

		db.query("INSERT INTO " + table + " (amount) VALUES (:amount)")
				.bind("amount", amount.toPlainString())
				.execute();

		Assertions.assertEquals(0, amount.compareTo(db.query("SELECT amount FROM " + table)
				.fetchObject(BigDecimal.class)
				.orElseThrow()));
		Assertions.assertEquals(0, amount.compareTo(db.query("SELECT amount FROM " + table)
				.fetchObject(DecimalTextRow.class)
				.orElseThrow()
				.amount()));
		Assertions.assertEquals(amount.toPlainString(), db.query("SELECT amount FROM " + table)
				.fetchObject(String.class)
				.orElseThrow());
	}

	@NonNull
	@Override
	protected DataSource dataSource() {
		return new DriverManagerDataSource("jdbc:sqlite:" + this.tempDir.resolve("pyranid.db"), null, null);
	}

	@NonNull
	@Override
	protected DatabaseType expectedDatabaseType() {
		return DatabaseType.SQLITE;
	}

	@NonNull
	@Override
	protected DialectProfile dialectProfile() {
		return new DialectProfile() {
			@NonNull
			@Override
			String autoIncrementPrimaryKey(@NonNull String columnName) {
				return columnName + " INTEGER PRIMARY KEY AUTOINCREMENT";
			}
		};
	}

	@NonNull
	@Override
	protected CapabilityFlags capabilityFlags() {
		return CapabilityFlags.builder()
				.supportsReadOnlyTransactions(false)
				.supportsTransactionIsolationOptions(false)
				.build();
	}
}
