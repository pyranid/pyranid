/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the tenant recipe as an ordinary application role, rather than an RLS-bypassing owner. */
@Testcontainers
class PostgreSqlTenantContextIT {
	@Container private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("tenant_context").withUsername("pyranid").withPassword("pyranid");
	private static final UUID A = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
	private static final UUID B = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
	public record Account(int accountId, UUID orgId, int balance) {}

	@Test
	void nonPrivilegedRoleFailsClosedAndIsolatesReadsAndWrites() {
		try (Fixture fixture = new Fixture(2)) {
			Database database = fixture.database;
			assertFalse(database.query("SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user")
					.fetchObject(Boolean.class).orElseThrow());
			assertTrue(database.query("SELECT pg_catalog.row_security_active(CAST(:table AS regclass))")
					.bind("table", fixture.table).fetchObject(Boolean.class).orElseThrow());
			assertEquals(List.of(), fixture.accounts());
			DatabaseException missing = assertThrows(DatabaseException.class, () -> fixture.insert(3, A));
			assertEquals("42501", missing.getSqlState().orElseThrow());

			TransactionOutcome<Void> accepted = database.transactionWithOutcome(() -> {
				fixture.setTenant(A);
				database.query("SET LOCAL lock_timeout = '2s'").execute();
				database.query("SET LOCAL statement_timeout = '5s'").execute();
				assertEquals("2s", database.query("SHOW lock_timeout").fetchObject(String.class).orElseThrow());
				assertEquals("5s", database.query("SHOW statement_timeout").fetchObject(String.class).orElseThrow());
				assertEquals(List.of(new Account(1, A, 10)), fixture.accounts());
				assertEquals(0L, database.query("UPDATE " + fixture.table + " SET balance=99 WHERE account_id=2").execute());
				DatabaseException denied = assertThrows(DatabaseException.class,
						() -> database.currentTransaction().orElseThrow().withSavepoint(() -> fixture.insert(3, B)));
				assertEquals("42501", denied.getSqlState().orElseThrow());
				assertEquals(List.of(new Account(1, A, 10)), fixture.accounts(), "A handled savepoint error must not expose another tenant");
				fixture.insert(3, A);
			});
			assertEquals(TransactionResult.COMMITTED, accepted.getResult());
			assertTrue(accepted.getFailure().isEmpty());
			database.transaction(() -> {
				fixture.setTenant(B);
				assertEquals(List.of(new Account(2, B, 20)), fixture.accounts());
			});
			assertEquals(List.of(), fixture.accounts());
			assertEquals(List.of(1, 2, 3), fixture.admin.query("SELECT account_id FROM " + fixture.table + " ORDER BY account_id")
					.fetchList(Integer.class));
		}
	}

	@Test
	void localSettingsDisappearAfterCommitRollbackAndAutocommitOnTheSamePhysicalSession() {
		try (Fixture fixture = new Fixture(1)) {
			Database database = fixture.database;
			int pid = fixture.pid();
			for (boolean rollback : new boolean[]{false, true}) {
				TransactionOutcome<Void> outcome = database.transactionWithOutcome(() -> {
					assertEquals(pid, fixture.pid());
					fixture.setTenant(A);
					assertEquals(List.of(new Account(1, A, 10)), fixture.accounts());
					if (rollback) database.currentTransaction().orElseThrow().setRollbackOnly(true);
				});
				assertEquals(rollback ? TransactionResult.ROLLED_BACK : TransactionResult.COMMITTED, outcome.getResult());
				assertEquals(pid, fixture.pid());
				assertTrue(fixture.tenant().isEmpty());
				assertEquals(List.of(), fixture.accounts());
			}
			// A transaction-local setter in autocommit completes before the following application query.
			assertEquals(A.toString(), database.query("SELECT pg_catalog.set_config('app.org_id', :org, true)")
					.bind("org", A.toString()).fetchObject(String.class).orElseThrow());
			assertEquals(pid, fixture.pid());
			assertTrue(fixture.tenant().isEmpty());
			assertEquals(List.of(), fixture.accounts());
			TransactionOutcome<Void> invalid = database.transactionWithOutcome(() -> {
				database.query("SELECT pg_catalog.set_config('app.org_id', :org, true)").bind("org", "invalid-uuid")
						.fetchObject(String.class).orElseThrow();
				fixture.accounts();
			});
			assertEquals(TransactionResult.ROLLED_BACK, invalid.getResult());
			assertEquals("22P02", assertInstanceOf(DatabaseException.class, invalid.getFailure().orElseThrow()).getSqlState().orElseThrow());
			assertTrue(fixture.tenant().isEmpty());
		}
	}

	@Test
	void savepointsIndependentTransactionsAndCompletionHooksHaveExplicitContextBoundaries() {
		try (Fixture fixture = new Fixture(2)) {
			Database database = fixture.database;
			AtomicReference<List<Account>> innerHook = new AtomicReference<>(), outerHook = new AtomicReference<>();
			database.transaction(() -> {
				fixture.setTenant(A);
				int outerPid = fixture.pid();
				IllegalStateException rejected = new IllegalStateException("reject savepoint work");
				assertSame(rejected, assertThrows(IllegalStateException.class, () -> database.currentTransaction().orElseThrow().withSavepoint(() -> {
					fixture.setTenant(B);
					assertEquals(List.of(new Account(2, B, 20)), fixture.accounts());
					throw rejected;
				})));
				assertEquals(Optional.of(A.toString()), fixture.tenant());
				database.transaction(() -> {
					assertNotEquals(outerPid, fixture.pid());
					assertTrue(fixture.tenant().isEmpty());
					assertEquals(List.of(), fixture.accounts());
					fixture.setTenant(B);
					assertEquals(List.of(new Account(2, B, 20)), fixture.accounts());
					database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> innerHook.set(fixture.accounts()));
				});
				assertEquals(List.of(new Account(1, A, 10)), innerHook.get(), "An enclosing scope is restored before nested completion hooks");
				assertEquals(Optional.of(A.toString()), fixture.tenant());
				database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> {
					assertTrue(database.currentTransaction().isEmpty());
					outerHook.set(fixture.accounts());
				});
			});
			assertEquals(List.of(), outerHook.get(), "A top-level hook cannot retain the completed transaction's local tenant");
			assertTrue(fixture.tenant().isEmpty());
		}
	}

	private static final class Fixture implements AutoCloseable {
		private final String suffix = UUID.randomUUID().toString().replace("-", "");
		private final String schema = "tenant_" + this.suffix, role = "tenant_app_" + this.suffix;
		private final String table = this.schema + ".account";
		private final Database admin;
		private final HikariDataSource pool;
		private final Database database;

		private Fixture(int size) {
			PGSimpleDataSource adminSource = source(POSTGRES.getUsername(), POSTGRES.getPassword());
			this.admin = Database.with(adminSource, DatabaseType.POSTGRESQL).build();
			this.admin.query("CREATE ROLE " + this.role + " LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD 'tenant_recipe_password'").execute();
			this.admin.query("CREATE SCHEMA " + this.schema).execute();
			this.admin.query("CREATE TABLE " + this.table + " (account_id INTEGER PRIMARY KEY, org_id UUID NOT NULL, balance INTEGER NOT NULL)").execute();
			this.admin.query("INSERT INTO " + this.table + " VALUES (1, :a, 10), (2, :b, 20)").bind("a", A).bind("b", B).execute();
			this.admin.query("CREATE FUNCTION " + this.schema + ".current_org_id() RETURNS UUID LANGUAGE SQL STABLE AS $$ "
					+ "SELECT NULLIF(pg_catalog.current_setting('app.org_id', true), '')::uuid $$").execute();
			this.admin.query("ALTER TABLE " + this.table + " ENABLE ROW LEVEL SECURITY").execute();
			this.admin.query("ALTER TABLE " + this.table + " FORCE ROW LEVEL SECURITY").execute();
			this.admin.query("CREATE POLICY application_access ON " + this.table + " TO " + this.role + " USING (true) WITH CHECK (true)").execute();
			this.admin.query("CREATE POLICY tenant_isolation ON " + this.table + " AS RESTRICTIVE TO " + this.role + " USING (org_id=" + this.schema
					+ ".current_org_id()) WITH CHECK (org_id=" + this.schema + ".current_org_id())").execute();
			this.admin.query("GRANT USAGE ON SCHEMA " + this.schema + " TO " + this.role).execute();
			this.admin.query("GRANT SELECT, INSERT, UPDATE, DELETE ON " + this.table + " TO " + this.role).execute();
			HikariConfig config = new HikariConfig();
			config.setDataSource(source(this.role, "tenant_recipe_password"));
			config.setMaximumPoolSize(size); config.setMinimumIdle(0);
			this.pool = new HikariDataSource(config);
			this.database = Database.with(this.pool, DatabaseType.POSTGRESQL).build();
		}

		private List<Account> accounts() {
			return this.database.query("SELECT account_id, org_id, balance FROM " + this.table + " ORDER BY account_id").fetchList(Account.class);
		}
		private void insert(int id, UUID tenant) {
			this.database.query("INSERT INTO " + this.table + " VALUES (:id, :org, 30)").bind("id", id).bind("org", tenant).execute();
		}
		private void setTenant(UUID tenant) {
			assertTrue(this.database.currentTransaction().isPresent());
			assertEquals(tenant.toString(), this.database.query("SELECT pg_catalog.set_config('app.org_id', :org, true)")
					.bind("org", tenant.toString()).fetchObject(String.class).orElseThrow());
		}
		private Optional<String> tenant() {
			return this.database.query("SELECT NULLIF(pg_catalog.current_setting('app.org_id', true), '')").fetchObject(String.class);
		}
		private int pid() { return this.database.query("SELECT pg_backend_pid()").fetchObject(Integer.class).orElseThrow(); }
		@Override public void close() {
			this.pool.close();
			this.admin.query("DROP SCHEMA " + this.schema + " CASCADE").execute();
			this.admin.query("DROP ROLE " + this.role).execute();
		}
	}

	private static PGSimpleDataSource source(String username, String password) {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(username); source.setPassword(password); return source;
	}
}
