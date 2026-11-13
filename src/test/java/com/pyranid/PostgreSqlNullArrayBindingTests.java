/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLRecoverableException;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PostgreSqlNullArrayBindingTests {
	@AfterEach
	void clearInterruption() { Thread.interrupted(); }

	@Test
	void postgresUsesTheDriversTypedNullArrayWithoutDescribingTheStatement() throws Exception {
		for (String elementType : List.of("text", "integer", "\"Array Schema\".\"Enum.Name\"", "domain_type")) {
			Fixture fixture = new Fixture(DatabaseType.POSTGRESQL);
			fixture.bind(elementType);
			assertEquals(elementType, fixture.createdElementType);
			assertNull(fixture.createdElements);
			assertSame(fixture.array, fixture.boundArray);
			assertEquals(1, fixture.creations.get());
			assertEquals(1, fixture.arrayBindings.get());
			assertEquals(0, fixture.metadataReads.get());
			assertEquals(0, fixture.nullBindings.get());
			assertEquals(0, fixture.frees.get());
			assertEquals(1, fixture.context.getCleanupOperations().size());
			fixture.drainCleanup();
			assertEquals(1, fixture.frees.get());
			fixture.drainCleanup();
			assertEquals(1, fixture.frees.get());
		}
	}

	@Test
	void genericAndDuckDbKeepTheirExistingTypedSetNullPath() throws Exception {
		for (DatabaseType type : List.of(DatabaseType.GENERIC, DatabaseType.DUCK_DB)) {
			Fixture fixture = new Fixture(type);
			fixture.bind("custom_element");
			assertEquals(0, fixture.creations.get());
			assertEquals(0, fixture.arrayBindings.get());
			assertEquals(1, fixture.metadataReads.get());
			assertEquals(1, fixture.nullBindings.get());
			assertArrayEquals(new Object[]{1, Types.ARRAY, "custom_element"}, fixture.nullArguments);
			assertTrue(fixture.context.getCleanupOperations().isEmpty());
		}
	}

	@Test
	void creationFailuresNeverFallBackToAnUntypedNullOrExecuteTheStatement() {
		for (Throwable failure : failures()) {
			Fixture fixture = new Fixture(DatabaseType.POSTGRESQL);
			fixture.creationFailure = failure;
			assertSame(failure, assertThrows(Throwable.class, () -> fixture.bind("text")));
			assertEquals(1, fixture.creations.get());
			assertEquals(0, fixture.arrayBindings.get());
			assertEquals(0, fixture.nullBindings.get());
			assertEquals(0, fixture.metadataReads.get());
			assertEquals(0, fixture.executions.get());
			assertEquals(0, fixture.frees.get());
			assertTrue(fixture.context.getCleanupOperations().isEmpty());
		}
	}

	@Test
	void bindingFailuresFreeTheCreatedArrayOnceAndPreserveThePrimaryAndCleanupFailures() {
		for (Throwable failure : failures()) {
			Fixture fixture = new Fixture(DatabaseType.POSTGRESQL);
			fixture.bindingFailure = failure;
			SQLException cleanup = new SQLException("typed null array cleanup failed", "HY000");
			fixture.freeFailure = cleanup;
			assertSame(failure, assertThrows(Throwable.class, () -> fixture.bind("text")));
			assertArrayEquals(new Throwable[]{cleanup}, failure.getSuppressed());
			assertEquals(1, fixture.creations.get());
			assertEquals(1, fixture.arrayBindings.get());
			assertEquals(1, fixture.frees.get());
			assertEquals(0, fixture.nullBindings.get());
			assertEquals(0, fixture.executions.get());
			assertTrue(fixture.context.getCleanupOperations().isEmpty());
		}
	}

	@Test
	void successfulBindingDefersItsCleanupFailureWithoutRegisteringDuplicateOwnership() throws Exception {
		Fixture fixture = new Fixture(DatabaseType.POSTGRESQL);
		SQLException cleanup = new SQLException("typed null array cleanup failed", "HY000");
		fixture.freeFailure = cleanup;
		fixture.bind("text");
		assertSame(cleanup, assertThrows(SQLException.class, fixture::drainCleanup));
		fixture.drainCleanup();
		assertEquals(1, fixture.frees.get());
	}

	@Test
	void aDriverReturningNoArrayCannotLoseTheExplicitType() {
		Fixture fixture = new Fixture(DatabaseType.POSTGRESQL);
		fixture.returnNullArray = true;
		SQLException failure = assertThrows(SQLException.class, () -> fixture.bind("text"));
		assertTrue(failure.getMessage().contains("typed null SQL ARRAY"));
		assertEquals(0, fixture.arrayBindings.get());
		assertEquals(0, fixture.nullBindings.get());
		assertEquals(0, fixture.executions.get());
		assertTrue(fixture.context.getCleanupOperations().isEmpty());
	}

	@Test
	void escapingFactoryFailuresUseExistingStatementDiagnosticScrubbing() {
		Fixture fixture = new Fixture(DatabaseType.POSTGRESQL);
		SQLException driver = new SQLException("Unknown type; echoed secret-owner-987", "42704");
		fixture.creationFailure = driver;
		DatabaseException failure = assertThrows(DatabaseException.class, () -> fixture.database.query(
				"INSERT INTO work (owner, tags) VALUES (:owner, :tags)")
				.bind("owner", Parameters.secure("secret-owner-987"))
				.bind("tags", Parameters.sqlArrayOf("missing_type", (Object[]) null)).execute());
		assertSame(driver, failure.getCause());
		assertEquals("42704", failure.getSqlState().orElseThrow());
		assertFalse(failure.getMessage().contains("secret-owner-987"));
		assertTrue(failure.getMessage().contains("<redacted>"));
		assertEquals(0, fixture.executions.get());
		assertEquals(0, fixture.nullBindings.get());
	}

	@Test
	void managedExecutionDrainsTypedNullArrayOwnershipOnceOnSuccessAndFailure() {
		for (boolean failing : new boolean[]{false, true}) {
			Fixture fixture = new Fixture(DatabaseType.POSTGRESQL);
			SQLException execution = new SQLException("execution failed", "40001");
			SQLException cleanup = new SQLException("array cleanup failed", "HY000");
			if (failing) {
				fixture.executionFailure = execution;
				fixture.freeFailure = cleanup;
			}
			Query query = fixture.database.query("INSERT INTO work (payload) VALUES (:payload)")
					.bind("payload", Parameters.sqlArrayOf("text", (Object[]) null));
			if (failing) {
				DatabaseException failure = assertThrows(DatabaseException.class, query::execute);
				assertSame(execution, failure.getCause());
				assertTrue(ThrowableGraph.inspect(failure, current -> current == cleanup ? 1 : 0, 0) != 0);
			} else assertEquals(1L, query.execute());
			assertEquals(1, fixture.creations.get());
			assertEquals(1, fixture.arrayBindings.get());
			assertEquals(1, fixture.executions.get());
			assertEquals(1, fixture.frees.get());
			assertEquals(0, fixture.metadataReads.get());
			assertEquals(0, fixture.nullBindings.get());
		}
	}

	private static List<Throwable> failures() {
		return List.of(new SQLException("unknown array type", "42704"),
				new SQLFeatureNotSupportedException("typed array unavailable", "0A000"),
				new SQLRecoverableException("connection lost", "08006"),
				new SQLException("cancelled", "57014", new InterruptedException("cancelled")),
				new AbstractMethodError("typed array unavailable"));
	}

	private static final class Fixture {
		final AtomicInteger creations = new AtomicInteger(), arrayBindings = new AtomicInteger(),
				nullBindings = new AtomicInteger(), metadataReads = new AtomicInteger(),
				frees = new AtomicInteger(), executions = new AtomicInteger();
		final Array array;
		final Connection connection;
		PreparedStatement statement;
		final Database database;
		final StatementContext<?> context;
		String createdElementType;
		Object createdElements;
		Object[] nullArguments;
		Array boundArray;
		Throwable creationFailure, bindingFailure, freeFailure, executionFailure;
		boolean returnNullArray;

		Fixture(DatabaseType type) {
			this.array = proxy(Array.class, (method, args) -> {
				if (method.getName().equals("free")) {
					this.frees.incrementAndGet();
					if (this.freeFailure != null) throw this.freeFailure;
					return null;
				}
				throw new AssertionError("Unexpected array access: " + method.getName());
			});
			this.connection = proxy(Connection.class, (method, args) -> {
				if (method.getName().equals("createArrayOf")) {
					this.creations.incrementAndGet();
					this.createdElementType = (String) args[0];
					this.createdElements = args[1];
					if (this.creationFailure != null) throw this.creationFailure;
					return this.returnNullArray ? null : this.array;
				}
				if (method.getName().equals("prepareStatement")) return this.statement;
				if (method.getName().equals("close")) return null;
				throw new AssertionError("Unexpected connection access: " + method.getName());
			});
			this.statement = proxy(PreparedStatement.class, (method, args) -> {
				switch (method.getName()) {
					case "getConnection": return this.connection;
					case "getParameterMetaData": this.metadataReads.incrementAndGet(); return null;
					case "setNull": this.nullBindings.incrementAndGet(); this.nullArguments = args.clone(); return null;
					case "setArray":
						this.arrayBindings.incrementAndGet(); this.boundArray = (Array) args[1];
						if (this.bindingFailure != null) throw this.bindingFailure;
						return null;
					case "executeLargeUpdate":
						this.executions.incrementAndGet();
						if (this.executionFailure != null) throw this.executionFailure;
						return 1L;
					case "setObject", "close": return null;
					default: throw new AssertionError("Unexpected statement access: " + method.getName());
				}
			});
			DataSource source = proxy(DataSource.class, (method, args) -> {
				if (method.getName().equals("getConnection")) return this.connection;
				throw new AssertionError("Unexpected source access: " + method.getName());
			});
			this.database = Database.with(source, type).build();
			this.context = StatementContext.with(Statement.of("typed-null-array", "SELECT ?"), this.database).build();
		}

		void bind(String elementType) throws SQLException {
			PreparedStatementBinder.withDefaultConfiguration().bindParameter(this.context, this.statement, 1,
					Parameters.sqlArrayOf(elementType, (Object[]) null));
		}

		void drainCleanup() throws Exception {
			AutoCloseable operation;
			while ((operation = this.context.getCleanupOperations().poll()) != null) operation.close();
		}
	}

	@FunctionalInterface private interface Call { Object invoke(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> api, Call call) {
		return api.cast(Proxy.newProxyInstance(api.getClassLoader(), new Class<?>[]{api},
				(proxy, method, args) -> call.invoke(method, args)));
	}
}
