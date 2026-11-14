/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Native CALL output rows and callback-scoped raw JDBC procedure/cursor recipes. */
@Testcontainers
class PostgreSqlProcedureRecipesIT {

	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("procedure_recipes").withUsername("pyranid").withPassword("pyranid");

	private static final String CURSOR = "pyranid_recipe_cursor";
	private static final String FETCH = "FETCH ALL FROM pyranid_recipe_cursor";
	public record CallOutput(Integer doubled, String echoed) {}
	public record CursorRow(int item, String label, int backendPid) {}
	public record CursorResult(int backendPid, List<CursorRow> rows) {}

	@Test
	void nativeCallUsesLiteralNullPlaceholdersAndReturnsTheOutParameterRow() {
		Database database = database(false);
		String schema = createSchema(database);
		try {
			createProcedure(database, schema);
			String sql = "CALL " + schema + ".calculate(:amount, :label, NULL, NULL)";
			String label = "bound label '); DROP SCHEMA public CASCADE; --";
			assertEquals(new CallOutput(42, label), database.query(sql).bind("amount", 21).bind("label", label)
					.fetchObject(CallOutput.class).orElseThrow());
			assertEquals(new CallOutput(null, null), database.query(sql).bind("amount", null).bind("label", null)
					.fetchObject(CallOutput.class).orElseThrow());
			DatabaseException missingOutArguments = assertThrows(DatabaseException.class,
					() -> database.query("CALL " + schema + ".calculate(:amount, :label)").bind("amount", 21).bind("label", "x")
							.fetchObject(CallOutput.class));
			assertEquals("42883", missingOutArguments.getSqlState().orElseThrow());
		} finally { dropSchema(database, schema); }
	}

	@Test
	void jdbcEscapeCallNeedsProcedureModeAndRegisteredOutParameters() {
		Database database = database(true);
		String schema = createSchema(database);
		try {
			createProcedure(database, schema);
			String escapeSql = "{call " + schema + ".calculate(?, ?, ?, ?)}";
			assertEquals(new CallOutput(42, "JDBC output"), callableOutput(database, escapeSql, 21, "JDBC output"));
			assertEquals(new CallOutput(null, null), callableOutput(database, escapeSql, null, null));
			// The pgjdbc default rewrites JDBC escapes as function SELECT calls; native CALL above does not need this property.
			DatabaseException defaultMode = assertThrows(DatabaseException.class, () -> callableOutput(database(false), escapeSql, 21, "x"));
			assertTrue(List.of("42883", "42809").contains(defaultMode.getSqlState().orElseThrow()));
		} finally { dropSchema(database, schema); }
	}

	private static CallOutput callableOutput(Database database, String escapeSql, Integer amount, String label) {
		return database.useRawConnection(connection -> {
			try (CallableStatement call = connection.prepareCall(escapeSql)) {
				if (amount == null) call.setNull(1, Types.INTEGER); else call.setInt(1, amount);
				call.setString(2, label);
				call.registerOutParameter(3, Types.INTEGER); call.registerOutParameter(4, Types.VARCHAR);
				call.execute();
				Integer doubled = call.getInt(3);
				if (call.wasNull()) doubled = null;
				return Optional.of(new CallOutput(doubled, call.getString(4)));
			}
		}).orElseThrow();
	}

	@Test
	void refcursorIsFetchedAndMappedOnItsOwningTransactionConnectionBeforeCommit() {
		Database database = database(false);
		String schema = createSchema(database);
		try {
			createCursorFunction(database, schema);
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(true).build();
			InstanceProvider instances = new InstanceProvider() {};
			String label = "untrusted ' cursor label; --";
			CursorResult result = database.transaction(() -> Optional.of(fetchCursor(database, schema, label, mapper, instances))).orElseThrow();
			assertEquals(List.of(new CursorRow(1, label, result.backendPid()), new CursorRow(2, label, result.backendPid())), result.rows());
			assertEquals(1, database.query("SELECT 1").fetchObject(Integer.class).orElseThrow());
			// Only detached Java values escape the callback; all statements/resultsets close before it returns.
			assertEquals(2, result.rows().size());
		} finally { dropSchema(database, schema); }
	}

	@Test
	void refcursorCannotBeFetchedAfterAnAutocommitFunctionCall() {
		Database database = database(false);
		String schema = createSchema(database);
		try {
			createCursorFunction(database, schema);
			DatabaseException expired = assertThrows(DatabaseException.class,
					() -> fetchCursor(database, schema, "autocommit", ResultSetMapper.withDefaultConfiguration(), new InstanceProvider() {}));
			assertEquals("34000", expired.getSqlState().orElseThrow());
		} finally { dropSchema(database, schema); }
	}

	@Test
	void explicitCloseEndsTheCursorAndGuardedRawWorkDoesNotOwnCommit() {
		Database database = database(false);
		String schema = createSchema(database);
		try {
			createCursorFunction(database, schema);
			database.transaction(() -> database.useRawConnection(connection -> {
				assertFalse(connection.getAutoCommit());
				assertThrows(IllegalStateException.class, connection::commit);
				try (PreparedStatement open = connection.prepareStatement("SELECT " + schema + ".open_cursor(?)")) {
					open.setString(1, "transaction scoped");
					try (ResultSet cursor = open.executeQuery()) {
						assertTrue(cursor.next()); assertEquals(CURSOR, cursor.getString(1));
					}
				}
				try (java.sql.Statement close = connection.createStatement()) { close.execute("CLOSE pyranid_recipe_cursor"); }
				// Closed explicitly while this callback still owns its JDBC scope.
				try (java.sql.Statement inspect = connection.createStatement();
						 ResultSet cursors = inspect.executeQuery("SELECT COUNT(*) FROM pg_cursors WHERE name='pyranid_recipe_cursor'")) {
					assertTrue(cursors.next()); assertEquals(0, cursors.getLong(1));
				}
				return Optional.empty();
			}));
			assertEquals(1, database.query("SELECT 1").fetchObject(Integer.class).orElseThrow());
		} finally { dropSchema(database, schema); }
	}

	/** Uses known application-owned SQL identifiers; values are bound, and a server-provided identifier is never interpolated. */
	static CursorResult fetchCursor(Database database, String schema, String label, ResultSetMapper mapper, InstanceProvider instances) {
		return database.useRawConnection(connection -> {
			int backendPid;
			try (java.sql.Statement pid = connection.createStatement(); ResultSet row = pid.executeQuery("SELECT pg_backend_pid()")) {
				assertTrue(row.next()); backendPid = row.getInt(1);
			}
			try (PreparedStatement open = connection.prepareStatement("SELECT " + schema + ".open_cursor(?)")) {
				open.setString(1, label);
				try (ResultSet cursor = open.executeQuery()) {
					if (!cursor.next() || !CURSOR.equals(cursor.getString(1))) throw new SQLException("Unexpected application cursor name");
				}
			}
			StatementContext<CursorRow> context = StatementContext.<CursorRow>with(Statement.of("recipe.fetch-cursor", FETCH), database)
					.resultSetRowType(CursorRow.class).build();
			List<CursorRow> rows = new ArrayList<>();
			try (java.sql.Statement fetch = connection.createStatement(); ResultSet resultSet = fetch.executeQuery(FETCH)) {
				while (resultSet.next()) rows.add(mapper.map(context, resultSet, CursorRow.class, instances).orElseThrow());
			}
			try (java.sql.Statement close = connection.createStatement()) { close.execute("CLOSE pyranid_recipe_cursor"); }
			return Optional.of(new CursorResult(backendPid, List.copyOf(rows)));
		}).orElseThrow();
	}

	private static void createProcedure(Database database, String schema) {
		database.query("CREATE PROCEDURE " + schema + ".calculate(IN amount INTEGER, IN label TEXT, OUT doubled INTEGER, OUT echoed TEXT) "
				+ "LANGUAGE plpgsql AS $$ BEGIN doubled := amount * 2; echoed := label; END $$").execute();
	}

	private static void createCursorFunction(Database database, String schema) {
		database.query("CREATE FUNCTION " + schema + ".open_cursor(label TEXT) RETURNS refcursor LANGUAGE plpgsql AS $$ "
				+ "DECLARE cursor_name refcursor := 'pyranid_recipe_cursor'; BEGIN OPEN cursor_name FOR "
				+ "SELECT item, label, pg_backend_pid() AS backend_pid FROM generate_series(1, 2) AS rows(item) ORDER BY item; "
				+ "RETURN cursor_name; END $$").execute();
	}

	private static String createSchema(Database database) {
		String schema = "procedure_recipe_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE SCHEMA " + schema).execute(); return schema;
	}
	private static void dropSchema(Database database, String schema) { database.query("DROP SCHEMA " + schema + " CASCADE").execute(); }
	private static Database database(boolean callIfNoReturn) {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		if (callIfNoReturn) source.setEscapeSyntaxCallMode("callIfNoReturn");
		return Database.with(source, DatabaseType.POSTGRESQL).build();
	}
}
