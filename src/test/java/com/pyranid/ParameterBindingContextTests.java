/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ParameterBindingContextTests {
	@Test
	void onlyTheExactActiveContextAndStatementMatchWithoutCallingDriverEquality() {
		Database database = Database.with(new JDBCDataSource(), DatabaseType.GENERIC).build();
		Statement statement = Statement.of("same", "SELECT ?");
		StatementContext<?> first = StatementContext.with(statement, database).parameters(1).build();
		StatementContext<?> equal = StatementContext.with(statement, database).parameters(1).build();
		assertEquals(first, equal); assertNotSame(first, equal);
		AtomicInteger equalityCalls = new AtomicInteger();
		PreparedStatement one = opaqueStatement(equalityCalls), two = opaqueStatement(equalityCalls);
		var resolver = new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(first, one);
		assertNull(ParameterBindingContext.current(first, one));
		try (var ignored = ParameterBindingContext.activate(first, one, resolver)) {
			assertSame(resolver, ParameterBindingContext.current(first, one));
			assertNull(ParameterBindingContext.current(equal, one));
			assertNull(ParameterBindingContext.current(first, two));
			assertNull(ParameterBindingContext.current(equal, two));
		}
		assertNull(ParameterBindingContext.current(first, one));
		assertEquals(0, equalityCalls.get());
	}

	@Test
	void nestedScopesShadowMatchingIdentitiesAndRestoreTheOuterResolverAfterFailure() {
		Database database = Database.with(new JDBCDataSource(), DatabaseType.GENERIC).build();
		StatementContext<?> context = StatementContext.with(Statement.of("nested", "SELECT ?"), database).build();
		PreparedStatement statement = opaqueStatement(new AtomicInteger());
		var outer = new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(context, statement);
		var inner = new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(context, statement);
		RuntimeException failure = new IllegalStateException("nested binding failure");
		try (var ignored = ParameterBindingContext.activate(context, statement, outer)) {
			assertSame(failure, assertThrows(RuntimeException.class, () -> {
				try (var nested = ParameterBindingContext.activate(context, statement, inner)) {
					assertSame(inner, ParameterBindingContext.current(context, statement));
					throw failure;
				}
			}));
			assertSame(outer, ParameterBindingContext.current(context, statement));
		}
		assertNull(ParameterBindingContext.current(context, statement));
	}

	@Test
	void activeResolversAreThreadConfined() throws Exception {
		Database database = Database.with(new JDBCDataSource(), DatabaseType.GENERIC).build();
		StatementContext<?> context = StatementContext.with(Statement.of("thread", "SELECT ?"), database).build();
		PreparedStatement statement = opaqueStatement(new AtomicInteger());
		var resolver = new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(context, statement);
		AtomicReference<Throwable> failure = new AtomicReference<>();
		try (var ignored = ParameterBindingContext.activate(context, statement, resolver)) {
			Thread worker = new Thread(() -> {
				try { assertNull(ParameterBindingContext.current(context, statement)); }
				catch (Throwable thrown) { failure.set(thrown); }
			});
			worker.start(); worker.join(5_000); assertFalse(worker.isAlive());
			assertNull(failure.get());
			assertSame(resolver, ParameterBindingContext.current(context, statement));
		}
	}

	@Test
	void delegatedPublicBindingSharesOneResolverThroughAllBatchChunksAndStartsFreshNextExecution() {
		Fixture fixture = new Fixture();
		fixture.database.query("INSERT INTO work VALUES (:id, :created)").batchChunkSize(2).executeBatch(List.of(
				Map.of("id", 1, "created", Instant.EPOCH), Map.of("id", 2, "created", Instant.EPOCH),
				Map.of("id", 3, "created", Instant.EPOCH), Map.of("id", 4, "created", Instant.EPOCH),
				Map.of("id", 5, "created", Instant.EPOCH)));
		assertEquals(10, fixture.resolvers.size());
		Object first = fixture.resolvers.get(0);
		for (Object resolver : fixture.resolvers) assertSame(first, resolver);
		assertEquals(1, fixture.descriptions.get(), "A public default-binder delegate must share statement metadata across chunks");
		fixture.assertInactive();
		fixture.database.query("INSERT INTO work VALUES (:id, :created)").bind("id", 6).bind("created", Instant.EPOCH).execute();
		assertEquals(12, fixture.resolvers.size());
		assertNotSame(first, fixture.resolvers.get(10), "A subsequent execution needs a fresh statement resolver");
		assertSame(fixture.resolvers.get(10), fixture.resolvers.get(11));
		assertEquals(2, fixture.descriptions.get());
		fixture.assertInactive();
	}

	@Test
	void standaloneBindingEntriesDoNotReuseAResolverEvenWhenContextAndStatementObjectsAreReused() throws Exception {
		Fixture fixture = new Fixture();
		Field field = Database.class.getDeclaredField("statementExecutor"); field.setAccessible(true);
		StatementExecutor executor = (StatementExecutor) field.get(fixture.database);
		StatementContext<?> context = StatementContext.with(Statement.of("reused", "INSERT INTO work VALUES (?, ?)"), fixture.database)
				.parameters(1, Instant.EPOCH).build();
		try (Connection connection = fixture.source.getConnection(); PreparedStatement statement = connection.prepareStatement(context.getStatement().getSql())) {
			executor.performPreparedStatementBinding(context, statement, List.of(1, Instant.EPOCH));
			fixture.assertInactive();
			executor.performPreparedStatementBinding(context, statement, List.of(2, Instant.EPOCH));
			assertSame(fixture.resolvers.get(0), fixture.resolvers.get(1));
			assertSame(fixture.resolvers.get(2), fixture.resolvers.get(3));
			assertNotSame(fixture.resolvers.get(0), fixture.resolvers.get(2));
			assertEquals(2, fixture.descriptions.get());
			fixture.assertInactive();
		}
	}

	@Test
	void nestedQueryBindingRestoresTheOuterResolverAndPreservesCallbackOrder() {
		Fixture fixture = new Fixture();
		fixture.nested = true;
		fixture.database.query("INSERT INTO work VALUES (:id, :created)").bind("id", 1).bind("created", Instant.EPOCH).execute();
		assertEquals(List.of("outer:1", "inner:1", "outer-restored", "outer:2"), fixture.callbacks);
		assertEquals(3, fixture.resolvers.size());
		assertSame(fixture.resolvers.get(0), fixture.resolvers.get(2));
		assertNotSame(fixture.resolvers.get(0), fixture.resolvers.get(1));
		fixture.assertInactive();
	}

	@Test
	void streamCustomizersAndDelegatedParametersShareTheSameOpeningResolver() {
		Fixture fixture = new Fixture();
		AtomicReference<Object> customized = new AtomicReference<>();
		PreparedStatementBinder delegate = PreparedStatementBinder.withDefaultConfiguration();
		List<Long> rows = fixture.database.query("SELECT COUNT(*) FROM work WHERE created = :created AND id = :id")
				.customize((context, statement) -> {
					Object resolver = ParameterBindingContext.current(context, statement);
					assertNotNull(resolver); customized.set(resolver); fixture.callbacks.add("customizer");
					delegate.bindParameter(context, statement, 1, Instant.EPOCH);
				}).bind("created", Instant.EPOCH).bind("id", 1).fetchStream(Long.class, stream -> stream.toList());
		assertEquals(List.of(0L), rows);
		assertEquals(List.of("customizer", "outer:1", "outer:2"), fixture.callbacks);
		assertEquals(1, fixture.descriptions.get());
		for (Object resolver : fixture.resolvers) assertSame(customized.get(), resolver);
		fixture.assertInactive();
	}

	@Test
	void customizerFailuresClearNormalAndStreamScopesWithoutReplacingTheOriginalFailure() {
		for (boolean streamed : new boolean[]{false, true}) {
			for (Throwable primary : List.of(new SQLException("customizer failed", "22001"), new AssertionError("customizer fatal"))) {
				Fixture fixture = new Fixture();
				AtomicReference<StatementContext<?>> context = new AtomicReference<>();
				AtomicReference<PreparedStatement> statement = new AtomicReference<>();
				Query query = fixture.database.query("SELECT COUNT(*) FROM work WHERE id = :id").bind("id", 1).customize((c, s) -> {
					assertNotNull(ParameterBindingContext.current(c, s)); context.set(c); statement.set(s);
					if (primary instanceof SQLException sql) throw sql;
					throw (Error) primary;
				});
				Throwable thrown = assertThrows(Throwable.class, () -> {
					if (streamed) query.fetchStream(Long.class, stream -> stream.toList());
					else query.fetchObject(Long.class);
				});
				if (primary instanceof Error) assertSame(primary, thrown);
				else {
					while (thrown != primary && thrown.getCause() != null) thrown = thrown.getCause();
					assertSame(primary, thrown);
				}
				assertNotNull(context.get());
				assertNull(ParameterBindingContext.current(context.get(), statement.get()));
				assertTrue(fixture.resolvers.isEmpty(), "A failing customizer must not dispatch parameter callbacks");
			}
		}
	}

	@Test
	void callbackFailuresNeverLeaveAResolverActiveAndErrorsKeepTheirIdentity() {
		for (Throwable primary : List.of(new SQLException("binding failure", "22001"),
				new IllegalStateException("binding runtime"), new AssertionError("binding fatal"))) {
			Fixture fixture = new Fixture(); fixture.bindingFailure = primary;
			Throwable thrown = assertThrows(Throwable.class, () -> fixture.database.query("INSERT INTO work VALUES (:id, :created)")
					.bind("id", 1).bind("created", Instant.EPOCH).execute());
			if (primary instanceof Error) assertSame(primary, thrown);
			else {
				while (thrown != primary && thrown.getCause() != null) thrown = thrown.getCause();
				assertSame(primary, thrown);
			}
			fixture.assertInactive();
			Object failed = fixture.resolvers.get(0);
			fixture.bindingFailure = null;
			fixture.database.query("INSERT INTO work VALUES (:id, :created)").bind("id", 2).bind("created", Instant.EPOCH).execute();
			assertNotSame(failed, fixture.resolvers.get(1));
			fixture.assertInactive();
		}
	}

	@Test
	void closingARetainedScopeReleasesItsContextStatementAndResolver() throws Exception {
		Retention retained = closedScope();
		for (int attempt = 0; attempt < 40 && (retained.context.get() != null || retained.statement.get() != null || retained.resolver.get() != null); attempt++) {
			System.gc(); Thread.sleep(5);
		}
		assertNull(retained.context.get()); assertNull(retained.statement.get()); assertNull(retained.resolver.get());
		retained.scope.close(); // Re-closing cannot restore an old execution.
		Reference.reachabilityFence(retained.scope);
	}

	private static Retention closedScope() {
		Database database = Database.with(new JDBCDataSource(), DatabaseType.GENERIC).build();
		StatementContext<?> context = StatementContext.with(Statement.of("retention", "SELECT ?"), database).build();
		PreparedStatement statement = opaqueStatement(new AtomicInteger());
		var resolver = new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(context, statement);
		ParameterBindingContext.Scope scope = ParameterBindingContext.activate(context, statement, resolver);
		try { assertSame(resolver, ParameterBindingContext.current(context, statement)); }
		finally { scope.close(); }
		return new Retention(scope, new WeakReference<>(context), new WeakReference<>(statement), new WeakReference<>(resolver));
	}
	private record Retention(ParameterBindingContext.Scope scope, WeakReference<?> context, WeakReference<?> statement, WeakReference<?> resolver) {}

	private static PreparedStatement opaqueStatement(AtomicInteger equalityCalls) {
		return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "equals" -> { equalityCalls.incrementAndGet(); yield true; }
					case "hashCode" -> { equalityCalls.incrementAndGet(); yield 0; }
					default -> throw new AssertionError(method);
				});
	}

	private static Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
		try { return method.invoke(target, arguments); }
		catch (InvocationTargetException failure) { throw failure.getTargetException(); }
	}

	private static final class Fixture {
		final Database database;
		final DataSource source;
		final AtomicInteger descriptions = new AtomicInteger();
		final List<Object> resolvers = new ArrayList<>();
		final List<String> callbacks = new ArrayList<>();
		final List<StatementContext<?>> contexts = new ArrayList<>();
		final List<PreparedStatement> statements = new ArrayList<>();
		boolean nested, insideNested;
		Throwable bindingFailure;
		Fixture() {
			JDBCDataSource real = new JDBCDataSource(); real.setUrl("jdbc:hsqldb:mem:binding_scope_" + UUID.randomUUID());
			source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> {
						Object value = invoke(real, method, args);
						if (!method.getName().equals("getConnection")) return value;
						Connection connection = (Connection) value;
						return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (handle, call, values) -> {
							Object result = invoke(connection, call, values);
							if (!(result instanceof PreparedStatement statement)) return result;
							return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
									(prepared, action, parameters) -> {
										if (action.getName().equals("getParameterMetaData")) descriptions.incrementAndGet();
										return invoke(statement, action, parameters);
									});
						});
					});
			PreparedStatementBinder delegate = PreparedStatementBinder.withDefaultConfiguration();
			PreparedStatementBinder decorator = new PreparedStatementBinder() {
				@Override public <T> void bindParameter(StatementContext<T> context, PreparedStatement statement, Integer index, Object parameter) throws SQLException {
					Object resolver = ParameterBindingContext.current(context, statement);
					assertNotNull(resolver, "The executor must expose the actual in-flight resolver before dispatching a callback");
					resolvers.add(resolver); contexts.add(context); statements.add(statement);
					callbacks.add((insideNested ? "inner:" : "outer:") + index);
					if (bindingFailure != null) {
						if (bindingFailure instanceof SQLException sql) throw sql;
						if (bindingFailure instanceof RuntimeException runtime) throw runtime;
						if (bindingFailure instanceof Error error) throw error;
					}
					if (nested && !insideNested && index == 1) {
						insideNested = true;
						try { database.query("SELECT COUNT(*) FROM work WHERE id = :id").bind("id", -1).fetchObject(Long.class); }
						finally { insideNested = false; }
						assertSame(resolver, ParameterBindingContext.current(context, statement)); callbacks.add("outer-restored");
					}
					delegate.bindParameter(context, statement, index, parameter);
				}
			};
			database = Database.with(source, DatabaseType.GENERIC).timeZone(ZoneOffset.UTC).preparedStatementBinder(decorator).build();
			database.query("CREATE TABLE work (id INT, created TIMESTAMP)").execute();
		}
		void assertInactive() {
			for (int index = 0; index < contexts.size(); index++) assertNull(ParameterBindingContext.current(contexts.get(index), statements.get(index)));
		}
	}
}
