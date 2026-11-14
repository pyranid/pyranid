/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ParameterNameRedactionTests {
	private record Call(int index, String name, Object parameter) {}
	private enum Status { READY }

	@Test
	void repeatedAndExpandedBindingsExposeNamesInActualJdbcSlotOrder() {
		for (int capacity : new int[]{0, 16}) {
			List<Call> calls = new ArrayList<>();
			AtomicReference<StatementContext<?>> captured = new AtomicReference<>();
			Database database = Database.with(source(), DatabaseType.GENERIC).parsedSqlCacheCapacity(capacity)
					.parameterRedactor(named(calls)).statementLogger(log -> captured.set(log.getStatementContext())).build();
			for (int pass = 0; pass < 2; ++pass) {
				String result = database.query("SELECT CAST(:visible AS VARCHAR(80)) || CAST(:password AS VARCHAR(80)) || CAST(:visible AS VARCHAR(80)) "
						+ "FROM (VALUES(0)) WHERE 1 IN (:ids)")
						.bind("password", "private-password-5127").bind("ids", Parameters.inList(new int[]{1, 2}))
						.bind("visible", "public").fetchObject(String.class).orElseThrow();
				assertEquals("publicprivate-password-5127public", result, "Redaction must not affect binding");
				StatementContext<?> context = captured.get();
				assertEquals(List.of("visible", "password", "visible", "ids", "ids"), context.getParameterNames());
				assertEquals(List.of("public", "<named-password>", "public", 1, 2), context.getRedactedParameters());
				assertEquals(List.of(0, 1, 2, 3, 4), calls.stream().map(Call::index).toList());
				assertEquals(context.getParameterNames(), calls.stream().map(Call::name).toList());
				assertThrows(UnsupportedOperationException.class, () -> context.getParameterNames().set(0, "different"));
				assertTrue(StatementContext.with(context.getStatement(), database).build().getParameterNames().isEmpty());
				assertEquals(Arrays.asList(null, null), StatementContext.with(context.getStatement(), database)
						.parameters(1, 2).build().getParameterNames(), "Incompatible inherited names must not break manual diagnostic contexts");
				assertThrows(IllegalArgumentException.class, () -> StatementContext.with(context.getStatement(), database)
						.parameterNames(List.of("explicit")).build());
				calls.clear();
			}
		}
	}

	@Test
	void originalThreeArgumentLambdasAndBuiltInRedactorsKeepTheirContracts() {
		AtomicInteger calls = new AtomicInteger();
		Database database = Database.with(source(), DatabaseType.GENERIC).parameterRedactor((context, index, value) -> {
			calls.incrementAndGet();
			return Optional.of(context.getParameterNames().get(index) + ":" + value);
		}).build();
		StatementContext<?> context = StatementContext.with(Statement.of("legacy-redactor", "VALUES(?)"), database)
				.parameters("unchanged").parameterNames(List.of("value")).build();
		assertEquals(List.of("value:unchanged"), context.getRedactedParameters());
		assertEquals(1, calls.get());
		assertEquals(Optional.of("plain"), ParameterRedactor.none().redactParameter(context, 0, "value", "plain"));
		assertEquals(Optional.of("<redacted>"), ParameterRedactor.redactAll().redactParameter(context, 0, "value", "plain"));
	}

	@Test
	void manualAndPositionalContextsUseUnknownNamesAndBuilderSnapshotsAreImmutable() {
		List<Call> calls = new ArrayList<>();
		Database database = Database.with(source(), DatabaseType.GENERIC).parameterRedactor(named(calls)).build();
		Statement statement = Statement.of("manual", "VALUES(?, ?)");
		StatementContext<?> unknown = StatementContext.with(statement, database).parameters("a", null).build();
		assertEquals(Arrays.asList(null, null), unknown.getParameterNames());
		assertEquals(Arrays.asList("a", null), unknown.getRedactedParameters());
		assertTrue(calls.stream().allMatch(call -> call.name() == null));
		List<String> names = new ArrayList<>(Arrays.asList("password", null));
		var builder = StatementContext.with(statement, database).parameters("a", null).parameterNames(names);
		StatementContext<?> named = builder.build();
		names.set(0, "changed");
		assertEquals(Arrays.asList("password", null), named.getParameterNames());
		assertThrows(UnsupportedOperationException.class, () -> named.getParameterNames().add("extra"));
		assertEquals(Arrays.asList(null, null), builder.parameterNames(null).build().getParameterNames());
	}

	@Test
	void invalidManualNameCountsFailWithoutRenderingParameterValues() {
		Database database = Database.with(source(), DatabaseType.GENERIC).build();
		Object privateValue = new Object() {
			@Override public String toString() { throw new AssertionError("Validation must not render values"); }
		};
		for (List<String> names : List.of(List.<String>of(), List.of("one", "two"))) {
			IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
					() -> StatementContext.with(Statement.of("bad-names", "VALUES(?)"), database)
							.parameters(privateValue).parameterNames(names).build());
			assertTrue(failure.getMessage().contains("parameter count"));
		}
		assertThrows(IllegalArgumentException.class, () -> StatementContext.with(Statement.of("bad-batch-names", "VALUES(?)"), database)
				.parameters(List.of(List.of(privateValue), List.of(privateValue, privateValue))).batchParameterGroups(true)
				.parameterNames(List.of("name")).build());
	}

	@Test
	void namesAffectContextValueIdentityWithoutChangingStatementIdentityOrRendering() {
		Database database = Database.with(source(), DatabaseType.GENERIC).build();
		Statement firstStatement = Statement.named("same", "VALUES(?)", List.of("first"));
		Statement otherStatement = Statement.named("same", "VALUES(?)", List.of("other"));
		assertEquals(firstStatement, otherStatement);
		assertEquals(firstStatement.hashCode(), otherStatement.hashCode());
		assertEquals(firstStatement.toString(), otherStatement.toString());
		StatementContext<?> first = StatementContext.with(firstStatement, database).parameters(1).build();
		StatementContext<?> same = StatementContext.with(firstStatement, database).parameters(1).build();
		StatementContext<?> other = StatementContext.with(otherStatement, database).parameters(1).build();
		assertEquals(first, same);
		assertEquals(first.hashCode(), same.hashCode());
		assertNotEquals(first, other);
		assertEquals("first", new HashMap<>(Map.of(first, "first", other, "other")).get(same));
		assertEquals(List.of("override"), StatementContext.with(firstStatement, database).parameters(1)
				.parameterNames(List.of("override")).build().getParameterNames());
		assertEquals(List.of("first"), StatementContext.with(firstStatement, database).parameters(1)
				.parameterNames(null).build().getParameterNames());
	}

	@Test
	void secureOptionalAndInListValuesKeepMaskPrecedenceAndOriginalNames() {
		List<Call> calls = new ArrayList<>();
		AtomicReference<StatementContext<?>> captured = new AtomicReference<>();
		Database database = Database.with(source(), DatabaseType.GENERIC).parameterRedactor(named(calls))
				.statementLogger(log -> captured.set(log.getStatementContext())).build();
		assertEquals("secret", database.query("SELECT CAST(:password AS VARCHAR(80)) FROM (VALUES(0)) "
				+ "WHERE 1 IN (:ids) AND :nullable IS NULL")
				.bind("password", Optional.of(Parameters.secure("secret", "<explicit>")))
				.bind("ids", Parameters.secure(Parameters.inList(new int[]{1, 2}), "<ids>"))
				.bind("nullable", Optional.empty()).fetchObject(String.class).orElseThrow());
		StatementContext<?> context = captured.get();
		assertEquals(List.of("password", "ids", "ids", "nullable"), context.getParameterNames());
		assertEquals(Arrays.asList("<explicit>", "<ids>", "<ids>", null), context.getRedactedParameters());
		assertEquals(List.of(new Call(3, "nullable", null)), calls);
	}

	@Test
	void typedAndEnumBindingsRetainNamesAndExistingDisplayValueShapes() {
		TypedParameter typed = Parameters.listOf(String.class, List.of("one", "two"));
		List<Call> calls = new ArrayList<>();
		AtomicReference<StatementContext<?>> captured = new AtomicReference<>();
		List<List<String>> binderNames = new ArrayList<>();
		PreparedStatementBinder binder = PreparedStatementBinder.withCustomParameterBinders(List.of(new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType target) { return target.matchesParameterizedType(List.class, String.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index,
					Object value) throws SQLException {
				binderNames.add(context.getParameterNames());
				statement.setString(index, String.join(",", (List<String>) value));
				return BindingResult.handled();
			}
		}));
		Database database = Database.with(source(), DatabaseType.GENERIC).preparedStatementBinder(binder)
				.parameterRedactor(named(calls)).statementLogger(log -> captured.set(log.getStatementContext())).build();
		assertEquals("one,twoREADY", database.query("SELECT CAST(:typed AS VARCHAR(80)) || CAST(:status AS VARCHAR(80)) FROM (VALUES(0))")
				.bind("typed", typed).bind("status", Optional.of(Status.READY)).fetchObject(String.class).orElseThrow());
		StatementContext<?> context = captured.get();
		assertEquals(List.of("typed", "status"), context.getParameterNames());
		context.getRedactedParameters();
		assertSame(typed, calls.get(0).parameter());
		assertSame(Status.READY, calls.get(1).parameter());
		assertEquals(List.of(List.of("typed", "status")), binderNames);
	}

	@Test
	void everyQueryExecutionShapeCarriesNamedMetadataIntoItsContext() {
		List<StatementContext<?>> contexts = new ArrayList<>();
		Database database = Database.with(source(), DatabaseType.GENERIC)
				.statementLogger(log -> contexts.add(log.getStatementContext())).build();
		database.query("CREATE TABLE generated_values (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, payload INTEGER)").execute();
		contexts.clear();
		String select = "SELECT CAST(:value AS INTEGER) FROM (VALUES(0))";
		assertEquals(Optional.of(7), database.query(select).bind("value", 7).fetchObject(Integer.class));
		assertEquals(List.of(7), database.query(select).bind("value", 7).fetchList(Integer.class));
		assertEquals(List.of(7), database.query(select).bind("value", 7).fetchStream(Integer.class, values -> values.toList()));
		assertEquals(Optional.of(7), database.query(select).bind("value", 7).executeForObject(Integer.class));
		assertEquals(List.of(7), database.query(select).bind("value", 7).executeForList(Integer.class));
		String insert = "INSERT INTO generated_values(payload) VALUES(:value)";
		assertEquals(1L, database.query(insert).bind("value", 7).execute());
		assertTrue(database.query(insert).bind("value", 7).executeReturningGeneratedKey(Long.class).isPresent());
		assertTrue(database.query(insert).bind("value", 7).executeReturningGeneratedKey(Long.class, "ID").isPresent());
		assertEquals(1, database.query(insert).bind("value", 7).executeReturningGeneratedKeys(Long.class).size());
		assertEquals(1, database.query(insert).bind("value", 7).executeReturningGeneratedKeys(Long.class, "ID").size());
		assertEquals(10, contexts.size());
		assertTrue(contexts.stream().allMatch(context -> context.getParameterNames().equals(List.of("value"))));
	}

	@Test
	void batchNamesDescribeEachGroupsSlotsAndBoundedSummariesStillSkipRedactors() {
		for (Integer chunk : Arrays.asList(null, 1, 2)) {
			List<Call> calls = new ArrayList<>();
			AtomicReference<StatementContext<?>> captured = new AtomicReference<>();
			Database database = Database.with(source(), DatabaseType.GENERIC).parameterRedactor(named(calls))
					.statementLogger(log -> captured.set(log.getStatementContext())).build();
			database.query("CREATE TABLE batches (id INTEGER, password VARCHAR(80))").execute();
			assertEquals(List.of(1L, 1L, 1L), database.query("INSERT INTO batches VALUES(:id, :password)").batchChunkSize(chunk)
					.executeBatch(List.of(Map.of("id", 1, "password", "private-1"), Map.of("id", 2, "password", "private-2"),
							Map.of("id", 3, "password", "private-3"))));
			StatementContext<?> context = captured.get();
			assertEquals(3, context.getParameters().size());
			assertEquals(List.of("id", "password"), context.getParameterNames());
			assertEquals(List.of("<batch: 3 groups x 2 parameters>"), context.getRedactedParameters());
			assertFalse(context.toString().contains("private-"));
			assertTrue(calls.isEmpty());
		}
	}

	@Test
	void sameSqlDifferentExpansionLayoutsExposeOnlyCommonBatchNamesIncludingChunkedExecution() {
		for (Integer chunk : Arrays.asList(null, 1, 2)) {
			AtomicReference<StatementContext<?>> captured = new AtomicReference<>();
			Database database = Database.with(source(), DatabaseType.GENERIC)
					.statementLogger(log -> captured.set(log.getStatementContext())).build();
			database.query("CREATE TABLE expanded_batches (a INTEGER, b INTEGER, c INTEGER)").execute();
			assertEquals(List.of(1L, 1L), database.query("INSERT INTO expanded_batches VALUES(:left, :right)").batchChunkSize(chunk)
					.executeBatch(List.of(Map.of("left", Parameters.inList(new int[]{1, 2}), "right", 3),
							Map.of("left", 4, "right", Parameters.inList(new int[]{5, 6})))));
			StatementContext<?> context = captured.get();
			assertEquals(Arrays.asList("left", null, "right"), context.getParameterNames());
			assertEquals(List.of("<batch: 2 groups x 3 parameters>"), context.getRedactedParameters());
			assertEquals(List.of(1, 4), database.query("SELECT a FROM expanded_batches ORDER BY a").fetchList(Integer.class));
			assertEquals(List.of(2, 5), database.query("SELECT b FROM expanded_batches ORDER BY a").fetchList(Integer.class));
			assertEquals(List.of(3, 6), database.query("SELECT c FROM expanded_batches ORDER BY a").fetchList(Integer.class));
		}
	}

	@Test
	void acquisitionAndExecutionFailuresKeepNameMasksWithoutDuplicatingDiagnostics() {
		String secret = "private-password-91284";
		for (boolean acquisition : new boolean[]{false, true}) {
			List<Call> calls = new ArrayList<>();
			AtomicReference<StatementContext<?>> captured = new AtomicReference<>();
			DataSource source = acquisition ? (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> { throw new SQLException("Connection unavailable", "08001"); }) : source();
			Database database = Database.with(source, DatabaseType.GENERIC).parameterRedactor(named(calls))
					.statementLogger(log -> captured.set(log.getStatementContext())).build();
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query("SELECT CAST(:password AS VARCHAR(80)), "
					+ "1 / CAST(:divisor AS INTEGER) FROM (VALUES(0))").bind("password", secret).bind("divisor", 0).fetchList(Object.class));
			assertFalse(failure.getMessage().contains(secret));
			assertEquals(List.of("password", "divisor"), captured.get().getParameterNames());
			assertEquals(List.of("<named-password>", 0), captured.get().getRedactedParameters());
			if (!acquisition) {
				assertTrue(failure.getMessage().contains("<named-password>"));
				assertEquals(1, occurrences(failure.getMessage(), "parameters="));
			}
			assertEquals(acquisition ? "08001" : "22012", failure.getSqlState().orElseThrow());
			assertTrue(calls.stream().anyMatch(call -> call.name().equals("password")));
		}
	}

	@Test
	void namedRedactorFailuresRemainFailFastDirectlyAndFailClosedInDatabaseDiagnostics() {
		IllegalStateException redactorFailure = new IllegalStateException("Name policy failed");
		ParameterRedactor redactor = new ParameterRedactor() {
			@Override public Optional<Object> redactParameter(StatementContext<?> context, Integer index, Object value) { throw new AssertionError("Use named overload"); }
			@Override public Optional<Object> redactParameter(StatementContext<?> context, Integer index, String name, Object value) { throw redactorFailure; }
		};
		Database database = Database.with(source(), DatabaseType.GENERIC).parameterRedactor(redactor).build();
		StatementContext<?> context = StatementContext.with(Statement.of("fail-closed", "VALUES(?)"), database)
				.parameters("private-password-91284").parameterNames(List.of("password")).build();
		assertSame(redactorFailure, assertThrows(IllegalStateException.class, context::getRedactedParameters));
		DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query("SELECT CAST(:password AS VARCHAR(80)), "
				+ "1 / CAST(:divisor AS INTEGER) FROM (VALUES(0))").bind("password", "private-password-91284").bind("divisor", 0).fetchList(Object.class));
		assertFalse(failure.getMessage().contains("private-password-91284"));
		assertEquals("22012", failure.getSqlState().orElseThrow());
		assertTrue(Arrays.asList(failure.getSuppressed()).contains(redactorFailure));
	}

	private static int occurrences(String text, String needle) {
		int count = 0;
		for (int index = text.indexOf(needle); index >= 0; index = text.indexOf(needle, index + needle.length())) ++count;
		return count;
	}
	private static ParameterRedactor named(List<Call> calls) {
		return new ParameterRedactor() {
			@Override public Optional<Object> redactParameter(StatementContext<?> context, Integer index, Object value) { throw new AssertionError("Use named overload"); }
			@Override public Optional<Object> redactParameter(StatementContext<?> context, Integer index, String name, Object value) {
				calls.add(new Call(index, name, value));
				return Optional.ofNullable("password".equals(name) ? "<named-password>" : value);
			}
		};
	}
	private static JDBCDataSource source() {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:parameter_names_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		return source;
	}
}
