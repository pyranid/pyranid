/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqliteTimestampMigrationIT {
    @TempDir Path directory;
    private static URLClassLoader recipeLoader;
    private static Method migrate;

    @BeforeAll
    static void compileExactApplicationRecipe(@TempDir Path classes) throws Exception {
        Path source = Path.of(System.getProperty("basedir", "."), "examples", "SqliteTimestampMigration.java").toAbsolutePath();
        ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();
        assertNotNull(ToolProvider.getSystemJavaCompiler(), "Recipe verification requires the development JDK");
        int exit = ToolProvider.getSystemJavaCompiler().run(null, diagnostics, diagnostics,
                "--release", "17", "-d", classes.toString(), source.toString());
        assertEquals(0, exit, diagnostics.toString(StandardCharsets.UTF_8));
        recipeLoader = new URLClassLoader(new java.net.URL[]{classes.toUri().toURL()}, SqliteTimestampMigrationIT.class.getClassLoader());
        migrate = recipeLoader.loadClass("SqliteTimestampMigration").getMethod("migrate", Path.class);
    }

    @AfterAll
    static void closeRecipeLoader() throws Exception {
        if (recipeLoader != null) recipeLoader.close();
    }

    @Test
    void normalizationRestoresEqualityUniquenessJoinsAndOptimisticUpdatesWithoutLosingNanoseconds() throws Exception {
        Path file = directory.resolve("legacy stamps.sqlite");
        create(file);
        insert(file, 1, 7, "2024-03-10T02:30", "2024-03-10T06:30:15Z");
        insert(file, 2, 7, "2024-11-03T01:30:00.123456789", "2024-03-10T15:30:16.123456789+08:00");
        insert(file, 3, 8, "2024-03-10T02:30:00.000000000", "2024-03-10T06:30:15.000000000Z");
        insert(file, 4, 7, null, null);
        insert(file, 5, 7, null, null);
        Database database = database(file);
        LocalDateTime local = database.query("SELECT local_at FROM event_stamp WHERE id=1").fetchObject(LocalDateTime.class).orElseThrow();
        Instant instant = database.query("SELECT instant_at FROM event_stamp WHERE id=1").fetchObject(Instant.class).orElseThrow();
        assertEquals(LocalDateTime.of(2024, 3, 10, 2, 30), local, "Local gaps must not be resolved through a time zone");
        assertEquals(ZoneOffset.ofHours(8), database.query("SELECT instant_at FROM event_stamp WHERE id=2")
                .fetchObject(OffsetDateTime.class).orElseThrow().getOffset());
        assertEquals(0L, count(database, "SELECT COUNT(*) FROM event_stamp WHERE account_id=7 AND local_at=:value", local));
        assertEquals(0L, count(database, "SELECT COUNT(*) FROM event_stamp WHERE account_id=7 AND instant_at=:value", instant));
        assertEquals(0L, count(database, "SELECT COUNT(*) FROM event_stamp WHERE id=1 AND local_at>=:value", local));
        assertEquals(0L, database.query("UPDATE event_stamp SET account_id=account_id WHERE id=1 AND local_at=:value")
                .bind("value", local).execute());

        // A new canonical spelling evades the old text key even though it denotes the same instant.
        database.transaction(() -> {
            assertEquals(1L, database.query("INSERT INTO event_stamp VALUES (6, 7, NULL, :value)").bind("value", instant).execute());
            database.currentTransaction().orElseThrow().setRollbackOnly(true);
        });
        assertEquals(5L, database.query("SELECT COUNT(*) FROM event_stamp").fetchObject(Long.class).orElseThrow());
        assertEquals(3L, database.query("SELECT COUNT(DISTINCT instant_at) FROM event_stamp").fetchObject(Long.class).orElseThrow());
        database.query("CREATE TABLE lookup_time (id INTEGER PRIMARY KEY, instant_at TEXT)").execute();
        database.query("INSERT INTO lookup_time VALUES (1, '2024-03-10T06:30:15.000000000Z')").execute();
        assertEquals(0L, database.query("SELECT COUNT(*) FROM event_stamp e JOIN lookup_time l ON e.instant_at=l.instant_at WHERE e.account_id=7")
                .fetchObject(Long.class).orElseThrow());

        assertEquals(2, normalize(file));
        assertEquals(0, normalize(file), "An already normalized file must produce no updates");
        assertEquals("2024-03-10T02:30:00.000000000", text(database, "local_at", 1));
        assertEquals("2024-11-03T01:30:00.123456789", text(database, "local_at", 2));
        assertEquals("2024-03-10T06:30:15.000000000Z", text(database, "instant_at", 1));
        assertEquals("2024-03-10T07:30:16.123456789Z", text(database, "instant_at", 2));
        assertEquals(Instant.parse("2024-03-10T07:30:16.123456789Z"), database.query("SELECT instant_at FROM event_stamp WHERE id=2")
                .fetchObject(Instant.class).orElseThrow());
        assertEquals(ZoneOffset.UTC, database.query("SELECT instant_at FROM event_stamp WHERE id=2")
                .fetchObject(OffsetDateTime.class).orElseThrow().getOffset());
        assertEquals(2L, database.query("SELECT COUNT(*) FROM event_stamp WHERE local_at IS NULL AND instant_at IS NULL")
                .fetchObject(Long.class).orElseThrow());
        assertEquals(1L, count(database, "SELECT COUNT(*) FROM event_stamp WHERE account_id=7 AND local_at=:value", local));
        assertEquals(1L, count(database, "SELECT COUNT(*) FROM event_stamp WHERE account_id=7 AND instant_at=:value", instant));
        assertEquals(1L, count(database, "SELECT COUNT(*) FROM event_stamp WHERE id=1 AND local_at>=:value", local));
        assertEquals(1L, database.query("UPDATE event_stamp SET account_id=account_id WHERE id=1 AND local_at=:value")
                .bind("value", local).execute());
        DatabaseException duplicate = assertThrows(DatabaseException.class,
                () -> database.query("INSERT INTO event_stamp VALUES (6, 7, NULL, :value)").bind("value", instant).execute());
        assertTrue(duplicate.isUniqueConstraintViolation());
        assertEquals(2L, database.query("SELECT COUNT(DISTINCT instant_at) FROM event_stamp").fetchObject(Long.class).orElseThrow());
        assertEquals(1L, database.query("SELECT COUNT(*) FROM event_stamp e JOIN lookup_time l ON e.instant_at=l.instant_at WHERE e.account_id=7")
                .fetchObject(Long.class).orElseThrow());
        assertEquals(1, database.query("INSERT INTO event_stamp VALUES (6, 7, :local, :instant) "
                        + "ON CONFLICT(account_id, instant_at) DO UPDATE SET local_at=excluded.local_at RETURNING id")
                .bind("local", local).bind("instant", instant).fetchObject(Integer.class).orElseThrow());
        assertEquals(5L, database.query("SELECT COUNT(*) FROM event_stamp").fetchObject(Long.class).orElseThrow());
    }

    @Test
    void logicallyDuplicateUniqueKeysAreRejectedBeforeAnySourceUpdate() throws Exception {
        Path file = directory.resolve("collision.sqlite");
        create(file);
        insert(file, 1, 7, "2024-03-10T02:30", "2024-03-10T06:30:15Z");
        insert(file, 2, 7, "2024-03-10T02:31", "2024-03-10T14:30:15.000000000+08:00");
        Database database = database(file);
        database.query("CREATE TABLE audit (id INTEGER)").execute();
        database.query("CREATE TRIGGER observe_migration AFTER UPDATE ON event_stamp BEGIN INSERT INTO audit VALUES (new.id); END").execute();
        List<String> before = snapshot(file);
        SQLException collision = assertThrows(SQLException.class, () -> normalize(file));
        assertEquals("23505", collision.getSQLState());
        assertTrue(collision.getMessage().contains("UNIQUE(account_id, instant_at)"));
        assertEquals(before, snapshot(file));
        assertEquals(0L, database.query("SELECT COUNT(*) FROM audit").fetchObject(Long.class).orElseThrow());
    }

    @Test
    void malformedAmbiguousAndNonTextValuesFailBeforeUpdatingAnySourceRow() throws Exception {
        for (String malformed : List.of("2024-02-30T02:30Z", "2024-03-10T06:30:15", " 2024-03-10T06:30:15Z",
                "2024-03-10T06:30:15.1234567891Z", "1700000000", "+10000-01-01T00:00:00Z")) {
            Path file = directory.resolve("bad_instant_" + java.util.UUID.randomUUID() + ".sqlite");
            create(file);
            insert(file, 1, 7, "2024-03-10T02:30", "2024-03-10T06:30:15Z");
            insert(file, 2, 7, null, malformed);
            List<String> before = snapshot(file);
            SQLException failure = assertThrows(SQLException.class, () -> normalize(file));
            assertEquals("22007", failure.getSQLState());
            assertEquals(before, snapshot(file));
        }
        Path zonedLocal = directory.resolve("zoned_local.sqlite");
        create(zonedLocal);
        insert(zonedLocal, 1, 7, "2024-03-10T02:30Z", null);
        assertEquals("22007", assertThrows(SQLException.class, () -> normalize(zonedLocal)).getSQLState());

        Path blob = directory.resolve("blob.sqlite");
        create(blob);
        database(blob).query("INSERT INTO event_stamp VALUES (1, 7, NULL, X'000102')").execute();
        List<String> before = snapshot(blob);
        assertEquals("22023", assertThrows(SQLException.class, () -> normalize(blob)).getSQLState());
        assertEquals(before, snapshot(blob));
    }

    @Test
    void aFailureDuringSourceUpdatesRollsBackEveryRow() throws Exception {
        Path file = directory.resolve("update_failure.sqlite");
        create(file);
        insert(file, 1, 7, "2024-03-10T02:30", "2024-03-10T06:30:15Z");
        insert(file, 2, 7, "2024-03-10T02:31", "2024-03-10T06:30:16Z");
        Database database = database(file);
        database.query("CREATE TRIGGER fail_second BEFORE UPDATE ON event_stamp WHEN old.id=2 "
                + "BEGIN SELECT RAISE(ABORT, 'injected migration update failure'); END").execute();
        List<String> before = snapshot(file);
        assertThrows(SQLException.class, () -> normalize(file));
        assertEquals(before, snapshot(file));
        database.query("DROP TRIGGER fail_second").execute();
        assertEquals(2, normalize(file));
        assertEquals(0, normalize(file));
    }

    private static int normalize(Path file) throws Exception {
        try { return (Integer) migrate.invoke(null, file); }
        catch (InvocationTargetException failure) {
            if (failure.getCause() instanceof Exception exception) throw exception;
            if (failure.getCause() instanceof Error error) throw error;
            throw new AssertionError(failure.getCause());
        }
    }

    private static void create(Path file) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + file);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE event_stamp (id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, "
                    + "local_at TEXT, instant_at TEXT, UNIQUE(account_id, instant_at))");
        }
    }

    private static void insert(Path file, long id, long account, String local, String instant) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + file);
             PreparedStatement statement = connection.prepareStatement("INSERT INTO event_stamp VALUES (?, ?, ?, ?)")) {
            statement.setLong(1, id); statement.setLong(2, account); statement.setString(3, local); statement.setString(4, instant);
            assertEquals(1, statement.executeUpdate());
        }
    }

    private static List<String> snapshot(Path file) throws SQLException {
        List<String> values = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + file);
             var statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT id, account_id, quote(local_at), quote(instant_at) FROM event_stamp ORDER BY id")) {
            while (rows.next()) values.add(rows.getLong(1) + ":" + rows.getLong(2) + ":" + rows.getString(3) + ":" + rows.getString(4));
        }
        return values;
    }

    private static Database database(Path file) {
        return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:sqlite:" + file, null, null),
                DatabaseType.SQLITE).timeZone(ZoneId.of("Asia/Tokyo")).build();
    }

    private static long count(Database database, String sql, Object value) {
        return database.query(sql).bind("value", value).fetchObject(Long.class).orElseThrow();
    }

    private static String text(Database database, String column, int id) {
        return database.query("SELECT CAST(" + column + " AS TEXT) FROM event_stamp WHERE id=:id")
                .bind("id", id).fetchObject(String.class).orElseThrow();
    }
}
