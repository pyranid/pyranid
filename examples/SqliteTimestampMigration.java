/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Properties;

/**
 * Application migration example for the exact event_stamp schema in docs/sqlite-timestamp-migration.md.
 * Adapt the declared column semantics and unique-key validation to the application before running it.
 * Requires Java 17 and sqlite-jdbc; no Pyranid runtime is needed by this one-time migration.
 */
public final class SqliteTimestampMigration {
    private static final DateTimeFormatter LOCAL_TEXT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSS", Locale.ROOT);
    private static final DateTimeFormatter INSTANT_TEXT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'", Locale.ROOT);

    private SqliteTimestampMigration() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1)
            throw new IllegalArgumentException("Usage: SqliteTimestampMigration /path/to/existing.sqlite");
        System.out.println("Rows normalized: " + migrate(Path.of(arguments[0])));
    }

    /** Owns a fresh, dedicated connection and transaction. Never call while old-format writers are active. */
    public static int migrate(Path databaseFile) throws Exception {
        Path existing = databaseFile.toRealPath();
        if (!Files.isRegularFile(existing)) throw new IllegalArgumentException("Expected an existing database file");
        Properties properties = new Properties();
        properties.setProperty("transaction_mode", "IMMEDIATE");
        properties.setProperty("foreign_keys", "true");
        properties.setProperty("busy_timeout", "5000");
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + existing.toUri(), properties)) {
            // sqlite-jdbc starts BEGIN IMMEDIATE here, reserving the writer before the first read.
            connection.setAutoCommit(false);
            try {
                stage(connection);
                rejectCollisions(connection);
                int changed;
                try (Statement update = connection.createStatement()) {
                    changed = update.executeUpdate("""
                            UPDATE event_stamp
                            SET local_at = (SELECT s.local_at FROM migration_event_stamp s WHERE s.id = event_stamp.id),
                                instant_at = (SELECT s.instant_at FROM migration_event_stamp s WHERE s.id = event_stamp.id)
                            WHERE local_at IS NOT (SELECT s.local_at FROM migration_event_stamp s WHERE s.id = event_stamp.id)
                               OR instant_at IS NOT (SELECT s.instant_at FROM migration_event_stamp s WHERE s.id = event_stamp.id)
                            """);
                }
                connection.commit();
                return changed;
            } catch (SQLException | RuntimeException | Error failure) {
                try { connection.rollback(); }
                catch (SQLException | RuntimeException | Error cleanup) {
                    if (cleanup != failure) failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }
    }

    private static void stage(Connection connection) throws SQLException {
        try (Statement create = connection.createStatement()) {
            create.executeUpdate("""
                    CREATE TEMP TABLE migration_event_stamp (
                        id INTEGER PRIMARY KEY,
                        account_id INTEGER NOT NULL,
                        local_at TEXT,
                        instant_at TEXT
                    )
                    """);
        }
        // Stage first: no source row is updated until every value and declared unique key has been checked.
        try (Statement select = connection.createStatement();
             ResultSet rows = select.executeQuery("""
                     SELECT id, account_id, local_at, instant_at,
                            typeof(id), typeof(account_id), typeof(local_at), typeof(instant_at)
                     FROM event_stamp ORDER BY id
                     """);
             PreparedStatement insert = connection.prepareStatement("INSERT INTO migration_event_stamp VALUES (?, ?, ?, ?)")) {
            while (rows.next()) {
                long id = rows.getLong(1);
                if (!"integer".equals(rows.getString(5)) || !"integer".equals(rows.getString(6)))
                    throw new SQLException("Expected integer id and account_id in event_stamp", "22023");
                String local = timestampText(rows, 3, 7, id, "local_at");
                String instant = timestampText(rows, 4, 8, id, "instant_at");
                insert.setLong(1, id);
                insert.setLong(2, rows.getLong(2));
                insert.setString(3, normalizeLocal(local, id));
                insert.setString(4, normalizeInstant(instant, id));
                insert.executeUpdate();
            }
        }
    }

    private static String timestampText(ResultSet rows, int column, int typeColumn, long id, String name) throws SQLException {
        String storage = rows.getString(typeColumn);
        if (!"text".equals(storage) && !"null".equals(storage))
            throw new SQLException("Row " + id + ", " + name + " requires TEXT or NULL; convert other storage explicitly", "22023");
        return rows.getString(column);
    }

    private static String normalizeLocal(String value, long id) throws SQLException {
        if (value == null) return null;
        try {
            // No zone is inferred: even a DST gap remains exactly the application's local wall-clock value.
            LocalDateTime local = LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            checkYear(local.getYear());
            return LOCAL_TEXT.format(local);
        } catch (DateTimeException invalid) {
            throw new SQLException("Row " + id + ", local_at requires an ISO local timestamp in years 0000-9999", "22007");
        }
    }

    private static String normalizeInstant(String value, long id) throws SQLException {
        if (value == null) return null;
        try {
            // Offset text is mandatory. An offset-free value in this column needs an explicit business decision.
            LocalDateTime utc = OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                    .withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
            checkYear(utc.getYear());
            return INSTANT_TEXT.format(utc);
        } catch (DateTimeException invalid) {
            throw new SQLException("Row " + id + ", instant_at requires an ISO offset timestamp with UTC year 0000-9999", "22007");
        }
    }

    private static void checkYear(int year) {
        if (year < 0 || year > 9999) throw new DateTimeException("Expanded years require a separate migration policy");
    }

    private static void rejectCollisions(Connection connection) throws SQLException {
        // This is the sample schema's exact unique key. Adapt this check for every actual unique key.
        // SQLite UNIQUE allows multiple NULL values, so only non-null instants participate.
        try (Statement select = connection.createStatement();
             ResultSet duplicate = select.executeQuery("""
                     SELECT MIN(id), MAX(id)
                     FROM migration_event_stamp
                     WHERE instant_at IS NOT NULL
                     GROUP BY account_id, instant_at
                     HAVING COUNT(*) > 1
                     LIMIT 1
                     """)) {
            if (duplicate.next())
                throw new SQLException("Normalization collides on UNIQUE(account_id, instant_at), rows "
                        + duplicate.getLong(1) + " and " + duplicate.getLong(2) + "; resolve before migration", "23505");
        }
    }
}
