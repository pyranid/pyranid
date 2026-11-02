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
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
public class SqliteIntegrationIT extends AbstractPortableJdbcIntegrationTests {
	public record DecimalTextRow(BigDecimal amount) {}

	public record InstantRow(Instant value) {}

	public static class InstantBean {
		private Instant value;
		public Instant getValue() { return this.value; }
		public void setValue(Instant value) { this.value = value; }
	}

	public record OffsetRow(OffsetDateTime value) {}

	public static class OffsetBean {
		private OffsetDateTime value;
		public OffsetDateTime getValue() { return this.value; }
		public void setValue(OffsetDateTime value) { this.value = value; }
	}

	public record LocalTimestampRow(LocalDateTime value) {}

	public static class LocalTimestampBean {
		private LocalDateTime value;
		public LocalDateTime getValue() { return this.value; }
		public void setValue(LocalDateTime value) { this.value = value; }
	}

	public record LocalDateRow(LocalDate value) {}

	public static class LocalDateBean {
		private LocalDate value;
		public LocalDate getValue() { return this.value; }
		public void setValue(LocalDate value) { this.value = value; }
	}

	public record SqlDateRow(java.sql.Date value) {}

	public static class SqlDateBean {
		private java.sql.Date value;
		public java.sql.Date getValue() { return this.value; }
		public void setValue(java.sql.Date value) { this.value = value; }
	}

	public record TimestampRow(Timestamp value) {}

	public static class TimestampBean {
		private Timestamp value;
		public Timestamp getValue() { return this.value; }
		public void setValue(Timestamp value) { this.value = value; }
	}

	public record ZonedRow(ZonedDateTime value) {}

	public static class ZonedBean {
		private ZonedDateTime value;
		public ZonedDateTime getValue() { return this.value; }
		public void setValue(ZonedDateTime value) { this.value = value; }
	}

	public record DateRow(Date value) {}

	public static class DateBean {
		private Date value;
		public Date getValue() { return this.value; }
		public void setValue(Date value) { this.value = value; }
	}

	@TempDir
	private Path tempDir;

	@Test
	public void testBracketIdentifiersDoNotBecomeNamedParameters() {
		Database db = database();
		Assertions.assertEquals(1, db.query("SELECT 1 [a:b]").fetchObject(Integer.class).orElseThrow());
		Assertions.assertEquals(1, db.query("SELECT [a:b] FROM (SELECT 1 AS [a:b]) WHERE [a:b] = :value")
				.bind("value", 1).fetchObject(Integer.class).orElseThrow());
		Assertions.assertEquals(1, db.query("SELECT CASE WHEN [a:b] = :value THEN 1 ELSE 0 END FROM (SELECT 1 AS [a:b])")
				.bind("value", 1).fetchObject(Integer.class).orElseThrow());
	}

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


	@Test
	public void testSqliteInstantsRoundTripAcrossTemporalRowShapes() {
		for (boolean cached : new boolean[]{false, true}) {
			for (ZoneId zone : List.of(ZoneOffset.UTC, ZoneId.of("Asia/Tokyo"))) {
				Database db = temporalDatabase(zone, cached);
				recreateTemporalTable(db);
				int id = 0;
				for (Instant value : List.of(Instant.parse("2024-03-10T13:30:00Z"),
						Instant.parse("2024-03-10T13:30:12.123456789Z"))) {
					insertTemporal(db, ++id, value);
					assertTemporalTargets(db, id, value, value.atOffset(ZoneOffset.UTC),
							value.atZone(zone).toLocalDateTime(), zone, true);
				}
			}
		}
	}

	@Test
	public void testSqliteOffsetTimestampsPreserveExplicitOffsetsAndLateDstOverlap() {
		ZoneId zone = ZoneId.of("America/New_York");
		for (boolean cached : new boolean[]{false, true}) {
			Database db = temporalDatabase(zone, cached);
			recreateTemporalTable(db);
			int id = 0;
			for (OffsetDateTime value : List.of(
					OffsetDateTime.parse("2024-03-10T13:30:12.123456789+05:45"),
					OffsetDateTime.parse("2024-11-03T01:30:00.123456789-05:00"),
					OffsetDateTime.parse("2024-11-03T06:30:00.123456789Z"))) {
				insertTemporal(db, ++id, value);
				assertTemporalTargets(db, id, value.toInstant(), value,
						value.atZoneSameInstant(zone).toLocalDateTime(), zone, true);
			}
		}
	}

	@Test
	public void testSqliteTimestampTextPreservesLongFractionsAndConfiguredLocalTime() {
		ZoneId zone = ZoneId.of("Asia/Tokyo");
		for (boolean cached : new boolean[]{false, true}) {
			Database db = temporalDatabase(zone, cached);
			recreateTemporalTable(db);
			int id = 0;
			for (String text : List.of("2024-03-10", "2024-03-10 13:30:00", "2024-03-10 13:30:00.1",
					"2024-03-10 13:30:00.123", "2024-03-10 13:30:00.123456",
					"2024-03-10 13:30:00.123456789", "2024-03-10T13:30:00.123456789")) {
				LocalDateTime local = text.length() == 10 ? LocalDate.parse(text).atStartOfDay()
						: LocalDateTime.parse(text.replace(' ', 'T'));
				OffsetDateTime expected = local.atZone(zone).toOffsetDateTime();
				insertTemporal(db, ++id, text);
				assertTemporalTargets(db, id, expected.toInstant(), expected, local, zone, false);
			}
		}
	}

	@Test
	public void testSqliteTimestampNullsStayNullAcrossTemporalRowShapes() {
		for (boolean cached : new boolean[]{false, true}) {
			Database db = temporalDatabase(ZoneOffset.UTC, cached);
			recreateTemporalTable(db);
			insertTemporal(db, 1, Instant.parse("2024-03-10T13:30:12.123456789Z"));
			insertTemporal(db, 2, null);
			for (int id : new int[]{1, 2}) {
				if (id == 1) {
					Instant value = Instant.parse("2024-03-10T13:30:12.123456789Z");
					assertTemporalTargets(db, id, value, value.atOffset(ZoneOffset.UTC),
							LocalDateTime.ofInstant(value, ZoneOffset.UTC), ZoneOffset.UTC, true);
					continue;
				}
				Assertions.assertTrue(temporalQuery(db, id).fetchObject(Instant.class).isEmpty());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(InstantRow.class).orElseThrow().value());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(InstantBean.class).orElseThrow().getValue());
				Assertions.assertTrue(temporalQuery(db, id).fetchObject(OffsetDateTime.class).isEmpty());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(OffsetRow.class).orElseThrow().value());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(OffsetBean.class).orElseThrow().getValue());
				Assertions.assertTrue(temporalQuery(db, id).fetchObject(LocalDateTime.class).isEmpty());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(LocalTimestampRow.class).orElseThrow().value());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(LocalTimestampBean.class).orElseThrow().getValue());
				Assertions.assertTrue(temporalQuery(db, id).fetchObject(LocalDate.class).isEmpty());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(LocalDateRow.class).orElseThrow().value());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(LocalDateBean.class).orElseThrow().getValue());
				Assertions.assertTrue(temporalQuery(db, id).fetchObject(java.sql.Date.class).isEmpty());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(SqlDateRow.class).orElseThrow().value());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(SqlDateBean.class).orElseThrow().getValue());
				Assertions.assertTrue(temporalQuery(db, id).fetchObject(Timestamp.class).isEmpty());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(TimestampRow.class).orElseThrow().value());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(TimestampBean.class).orElseThrow().getValue());
				Assertions.assertTrue(temporalQuery(db, id).fetchObject(ZonedDateTime.class).isEmpty());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(ZonedRow.class).orElseThrow().value());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(ZonedBean.class).orElseThrow().getValue());
				Assertions.assertTrue(temporalQuery(db, id).fetchObject(Date.class).isEmpty());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(DateRow.class).orElseThrow().value());
				Assertions.assertNull(temporalQuery(db, id).fetchObject(DateBean.class).orElseThrow().getValue());
			}
		}
	}

	@Test
	public void testSqliteTimestampRejectsInvalidCalendarValuesAndTrailingText() {
		for (boolean cached : new boolean[]{false, true}) {
			Database db = temporalDatabase(ZoneOffset.UTC, cached);
			recreateTemporalTable(db);
			int id = 0;
			for (String text : List.of("2024-02-30", "2024-02-30 12:34:56.123", "2024-02-30T12:34:56.123Z",
					"2024-03-10 24:00:00.000", "2024-03-10 13:30:00.1234567890",
					"2024-03-10 13:30:00.123 trailing", "2024-03-10T13:30Z trailing")) {
				int valueId = ++id;
				insertTemporal(db, valueId, text);
				for (Class<?> target : List.of(Instant.class, OffsetDateTime.class, LocalDateTime.class,
						LocalDate.class, java.sql.Date.class, Timestamp.class, ZonedDateTime.class, Date.class,
						InstantRow.class, OffsetRow.class, LocalTimestampRow.class, LocalDateRow.class,
						SqlDateRow.class, TimestampRow.class, ZonedRow.class, DateRow.class,
						InstantBean.class, OffsetBean.class, LocalTimestampBean.class, LocalDateBean.class,
						SqlDateBean.class, TimestampBean.class, ZonedBean.class, DateBean.class)) {
					Assertions.assertThrows(DatabaseException.class,
							() -> temporalQuery(db, valueId).fetchObject(target), text + " -> " + target.getSimpleName());
				}
			}
		}
	}

	@Test
	public void testSqliteDateOnlyAndNumericTimestampControlsKeepDriverSemantics() {
		ZoneId zone = ZoneId.systemDefault();
		for (boolean cached : new boolean[]{false, true}) {
			Database db = temporalDatabase(zone, cached);
			recreateTemporalTable(db);
			Instant value = Instant.parse("2020-01-02T03:04:05.123Z");
			insertTemporal(db, 1, value.toEpochMilli());
			assertTemporalTargets(db, 1, value, value.atZone(zone).toOffsetDateTime(),
					value.atZone(zone).toLocalDateTime(), zone, false);
			recreateTable(db, "pyranid_sqlite_dates", "CREATE TABLE pyranid_sqlite_dates (value DATE)");
			LocalDate date = LocalDate.of(2024, 3, 10);
			db.query("INSERT INTO pyranid_sqlite_dates VALUES (:date)").bind("date", date).execute();
			Assertions.assertEquals(date, db.query("SELECT value FROM pyranid_sqlite_dates").fetchObject(LocalDate.class).orElseThrow());
			Assertions.assertEquals(date, db.query("SELECT value FROM pyranid_sqlite_dates").fetchObject(LocalDateRow.class).orElseThrow().value());
			Assertions.assertEquals(date, db.query("SELECT value FROM pyranid_sqlite_dates").fetchObject(LocalDateBean.class).orElseThrow().getValue());
			Assertions.assertEquals(java.sql.Date.valueOf(date), db.query("SELECT value FROM pyranid_sqlite_dates").fetchObject(java.sql.Date.class).orElseThrow());
			Assertions.assertEquals(java.sql.Date.valueOf(date), db.query("SELECT value FROM pyranid_sqlite_dates").fetchObject(SqlDateRow.class).orElseThrow().value());
			Assertions.assertEquals(java.sql.Date.valueOf(date), db.query("SELECT value FROM pyranid_sqlite_dates").fetchObject(SqlDateBean.class).orElseThrow().getValue());
		}
	}

	@Test
	public void testSharedMapperCachesKeepTimestampReadersDialectAware() {
		ZoneId zone = ZoneId.of("America/New_York");
		LocalDateTime local = LocalDateTime.parse("2020-01-02T03:04:05.123456789");
		OffsetDateTime overlap = OffsetDateTime.parse("2024-11-03T01:30:00.123456789-05:00");
		for (boolean sqliteFirst : new boolean[]{false, true}) {
			ResultSetMapper shared = ResultSetMapper.withPlanCachingEnabled(true).build();
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:shared_temporal_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			Database hsql = Database.with(source, DatabaseType.GENERIC)
					.timeZone(zone).resultSetMapper(shared).build();
			hsql.query("CREATE TABLE pyranid_sqlite_temporals (id INTEGER PRIMARY KEY, value TIMESTAMP(9))").execute();
			insertTemporal(hsql, 1, local);
			Database sqlite = Database.with(dataSource(), DatabaseType.SQLITE)
					.timeZone(zone).resultSetMapper(shared).build();
			recreateTemporalTable(sqlite);
			insertTemporal(sqlite, 1, overlap);
			for (boolean useSqlite : new boolean[]{sqliteFirst, !sqliteFirst, sqliteFirst}) {
				if (useSqlite)
					assertTemporalTargets(sqlite, 1, overlap.toInstant(), overlap,
							overlap.atZoneSameInstant(zone).toLocalDateTime(), zone, true);
				else
					assertTemporalTargets(hsql, 1, local.atZone(zone).toInstant(),
							local.atZone(zone).toOffsetDateTime(), local, zone, false);
			}
		}
	}

	@Test
	public void testSqliteCustomDateFormatsRetainDriverSemanticsThroughWrappedConnections() throws Exception {
		ZoneId zone = ZoneId.of("Asia/Tokyo");
		LocalDateTime local = LocalDateTime.of(2024, 11, 3, 13, 30, 0, 123_000_000);
		for (String format : List.of("MM/dd/yyyy HH:mm:ss.SSS", "yyyy-dd-MM HH:mm:ss.SSS",
				"yyyy-dd-MM'T'HH:mm:ss.SSS")) {
			for (boolean wrapped : new boolean[]{false, true}) {
				for (boolean cached : new boolean[]{false, true}) {
					SQLiteConfig config = new SQLiteConfig();
					config.setDateClass("TEXT");
					config.setDateStringFormat(format);
					SQLiteDataSource nativeSource = new SQLiteDataSource(config);
					nativeSource.setUrl("jdbc:sqlite:" + this.tempDir.resolve("custom-" + UUID.randomUUID() + ".db"));
					AtomicInteger unwraps = new AtomicInteger();
					DataSource source = wrapped ? SqliteDateFormatFixtures.wrapped(nativeSource, unwraps) : nativeSource;
					Database db = Database.with(source, DatabaseType.SQLITE).timeZone(zone)
							.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
					recreateTemporalTable(db);
					// JDBC's legacy setter deliberately writes the configured format, unlike ISO java.time binding.
					try (Connection connection = nativeSource.getConnection();
							 PreparedStatement statement = connection.prepareStatement("INSERT INTO pyranid_sqlite_temporals VALUES (1, ?)")) {
						statement.setTimestamp(1, Timestamp.valueOf(local));
						statement.executeUpdate();
					}
					Assertions.assertEquals(local.format(DateTimeFormatter.ofPattern(format)),
							temporalQuery(db, 1).fetchObject(String.class).orElseThrow());
					// Both space and T custom formats can look like ISO: ISO parsing would silently swap month/day.
					assertTemporalTargets(db, 1, local.atZone(zone).toInstant(), local.atZone(zone).toOffsetDateTime(),
							local, zone, false);
					if (wrapped) Assertions.assertTrue(unwraps.get() > 0, "format lookup must unwrap the borrowed connection");
				}
			}
		}
	}

	@Test
	public void testSqliteDateFormatReflectionPreservesTargetFailuresWithoutClosingBorrowedConnection() throws Exception {
		try (SqliteDateFormatFixtures.FailingConfigConnection connection = new SqliteDateFormatFixtures.FailingConfigConnection()) {
			try {
				Assertions.assertEquals("yyyy-MM-dd HH:mm:ss.SSS",
						SqliteDialect.configuredDateStringFormat(SqliteDateFormatFixtures.borrowedResultSet(connection)));
				for (Throwable failure : new Throwable[]{new SQLException("rollback", "40001"),
						new IllegalStateException("config failed"), new AssertionError("fatal"),
						new SQLFeatureNotSupportedException("cancelled", new InterruptedException("cancelled"))}) {
					connection.failure = failure;
					Assertions.assertSame(failure, Assertions.assertThrows(Throwable.class,
							() -> SqliteDialect.configuredDateStringFormat(SqliteDateFormatFixtures.borrowedResultSet(connection))));
					Assertions.assertFalse(connection.isClosed());
				}
				connection.failure = new UnsupportedOperationException("optional config unavailable");
				Assertions.assertNull(SqliteDialect.configuredDateStringFormat(SqliteDateFormatFixtures.borrowedResultSet(connection)));
				connection.failure.addSuppressed(new SQLException("rollback", "40001"));
				Assertions.assertSame(connection.failure, Assertions.assertThrows(UnsupportedOperationException.class,
						() -> SqliteDialect.configuredDateStringFormat(SqliteDateFormatFixtures.borrowedResultSet(connection))));
				Assertions.assertFalse(connection.isClosed());
			} finally {
				connection.failure = null;
			}
		}
	}

	private Database temporalDatabase(ZoneId zone, boolean cached) {
		return Database.with(dataSource(), DatabaseType.SQLITE).timeZone(zone)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}

	private void recreateTemporalTable(Database db) {
		recreateTable(db, "pyranid_sqlite_temporals", "CREATE TABLE pyranid_sqlite_temporals (id INTEGER PRIMARY KEY, value TIMESTAMP)");
	}

	private void insertTemporal(Database db, int id, Object value) {
		db.query("INSERT INTO pyranid_sqlite_temporals VALUES (:id, :value)").bind("id", id).bind("value", value).execute();
	}

	private Query temporalQuery(Database db, int id) {
		return db.query("SELECT value FROM pyranid_sqlite_temporals WHERE id = :id").bind("id", id);
	}

	private void assertTemporalTargets(Database db, int id, Instant instant, OffsetDateTime offset,
			LocalDateTime local, ZoneId zone, boolean offsetBearing) {
		Assertions.assertEquals(instant, temporalQuery(db, id).fetchObject(Instant.class).orElseThrow());
		Assertions.assertEquals(instant, temporalQuery(db, id).fetchObject(InstantRow.class).orElseThrow().value());
		Assertions.assertEquals(instant, temporalQuery(db, id).fetchObject(InstantBean.class).orElseThrow().getValue());
		Assertions.assertEquals(offset, temporalQuery(db, id).fetchObject(OffsetDateTime.class).orElseThrow());
		Assertions.assertEquals(offset, temporalQuery(db, id).fetchObject(OffsetRow.class).orElseThrow().value());
		Assertions.assertEquals(offset, temporalQuery(db, id).fetchObject(OffsetBean.class).orElseThrow().getValue());
		Assertions.assertEquals(local, temporalQuery(db, id).fetchObject(LocalDateTime.class).orElseThrow());
		Assertions.assertEquals(local, temporalQuery(db, id).fetchObject(LocalTimestampRow.class).orElseThrow().value());
		Assertions.assertEquals(local, temporalQuery(db, id).fetchObject(LocalTimestampBean.class).orElseThrow().getValue());
		Assertions.assertEquals(local.toLocalDate(), temporalQuery(db, id).fetchObject(LocalDate.class).orElseThrow());
		Assertions.assertEquals(local.toLocalDate(), temporalQuery(db, id).fetchObject(LocalDateRow.class).orElseThrow().value());
		Assertions.assertEquals(local.toLocalDate(), temporalQuery(db, id).fetchObject(LocalDateBean.class).orElseThrow().getValue());
		Assertions.assertEquals(java.sql.Date.valueOf(local.toLocalDate()), temporalQuery(db, id).fetchObject(java.sql.Date.class).orElseThrow());
		Assertions.assertEquals(java.sql.Date.valueOf(local.toLocalDate()), temporalQuery(db, id).fetchObject(SqlDateRow.class).orElseThrow().value());
		Assertions.assertEquals(java.sql.Date.valueOf(local.toLocalDate()), temporalQuery(db, id).fetchObject(SqlDateBean.class).orElseThrow().getValue());
		Assertions.assertEquals(offsetBearing ? Timestamp.from(instant) : Timestamp.valueOf(local), temporalQuery(db, id).fetchObject(Timestamp.class).orElseThrow());
		Assertions.assertEquals(offsetBearing ? Timestamp.from(instant) : Timestamp.valueOf(local), temporalQuery(db, id).fetchObject(TimestampRow.class).orElseThrow().value());
		Assertions.assertEquals(offsetBearing ? Timestamp.from(instant) : Timestamp.valueOf(local), temporalQuery(db, id).fetchObject(TimestampBean.class).orElseThrow().getValue());
		Assertions.assertEquals(instant.atZone(zone), temporalQuery(db, id).fetchObject(ZonedDateTime.class).orElseThrow());
		Assertions.assertEquals(instant.atZone(zone), temporalQuery(db, id).fetchObject(ZonedRow.class).orElseThrow().value());
		Assertions.assertEquals(instant.atZone(zone), temporalQuery(db, id).fetchObject(ZonedBean.class).orElseThrow().getValue());
		Assertions.assertEquals(Date.from(instant), temporalQuery(db, id).fetchObject(Date.class).orElseThrow());
		Assertions.assertEquals(Date.from(instant), temporalQuery(db, id).fetchObject(DateRow.class).orElseThrow().value());
		Assertions.assertEquals(Date.from(instant), temporalQuery(db, id).fetchObject(DateBean.class).orElseThrow().getValue());
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
