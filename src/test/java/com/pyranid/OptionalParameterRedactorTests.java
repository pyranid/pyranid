/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class OptionalParameterRedactorTests {
	private static final String NULL_OPTIONAL_MESSAGE = "ParameterRedactor must return a non-null Optional";

	@Test
	void theSingleThreeArgumentPolicyDeclaresNonNullOptionalObject() throws Exception {
		Method original = ParameterRedactor.class.getMethod("redactParameter", StatementContext.class, Integer.class, Object.class);
		assertEquals(Optional.class, original.getReturnType());
		assertTrue(original.getAnnotatedReturnType().isAnnotationPresent(org.jspecify.annotations.NonNull.class));
		ParameterizedType returnType = assertInstanceOf(ParameterizedType.class, original.getGenericReturnType());
		assertArrayEquals(new Object[]{Object.class}, returnType.getActualTypeArguments());
		assertTrue(Modifier.isAbstract(original.getModifiers()));
		assertFalse(original.isDefault());
		assertEquals(List.of(original), Arrays.stream(ParameterRedactor.class.getDeclaredMethods())
				.filter(method -> method.getName().equals("redactParameter")).toList());
		assertThrows(NoSuchMethodException.class, () -> ParameterRedactor.class.getMethod("redactParameter",
				StatementContext.class, Integer.class, String.class, Object.class));
		assertTrue(ParameterRedactor.class.isAnnotationPresent(FunctionalInterface.class));
		assertEquals(List.of(original), Arrays.stream(ParameterRedactor.class.getDeclaredMethods())
				.filter(method -> Modifier.isAbstract(method.getModifiers())).toList());
	}

	@Test
	void builtInRedactorsReturnPresentValuesOrEmptyForNull() {
		StatementContext<?> context = context(ParameterRedactor.none(), "original");
		Object value = new Object();
		ParameterRedactor none = ParameterRedactor.none();
		assertSame(value, none.redactParameter(context, 0, value).orElseThrow());
		assertTrue(none.redactParameter(context, 0, null).isEmpty());
		ParameterRedactor all = ParameterRedactor.redactAll();
		for (Object parameter : new Object[]{value, null}) {
			assertEquals(Optional.of("<redacted>"), all.redactParameter(context, 0, parameter));
		}
		assertEquals(Arrays.asList(value, null), context(ParameterRedactor.none(), value, null).getRedactedParameters());
	}

	@Test
	void directCallsAndDiagnosticsInvokeTheSameOptionalPolicyExactlyOnce() {
		AtomicInteger calls = new AtomicInteger();
		AtomicReference<StatementContext<?>> receivedContext = new AtomicReference<>();
		Object raw = new Object(), safe = new Object();
		Optional<Object> result = Optional.of(safe);
		ParameterRedactor redactor = (statementContext, index, parameter) -> {
			receivedContext.set(statementContext);
			assertEquals(0, index); assertSame(raw, parameter); calls.incrementAndGet();
			return result;
		};
		StatementContext<?> context = context(redactor, raw);
		assertSame(result, redactor.redactParameter(context, 0, raw));
		assertEquals(1, calls.get()); assertSame(context, receivedContext.get());
		assertSame(safe, context.getRedactedParameters().get(0));
		assertEquals(2, calls.get());
	}

	@Test
	void emptyResultsRenderNullInPlaceWithoutRenderingOrBindingTheOriginalValueDifferently() {
		AtomicInteger rendered = new AtomicInteger();
		Object privateValue = new Object() {
			@Override public String toString() { rendered.incrementAndGet(); throw new AssertionError("Raw parameter was rendered"); }
		};
		ParameterRedactor redactor = (statementContext, index, parameter) -> index == 1 ? Optional.of("safe") : Optional.empty();
		StatementContext<?> context = context(redactor, privateValue, privateValue, privateValue);
		List<Object> displayed = context.getRedactedParameters();
		assertEquals(Arrays.asList(null, "safe", null), displayed);
		assertEquals(3, displayed.size());
		assertThrows(UnsupportedOperationException.class, () -> displayed.set(0, "changed"));
		assertTrue(context.toString().contains("parameters=[null, safe, null]"));
		assertEquals(0, rendered.get());

		AtomicReference<StatementLog<?>> log = new AtomicReference<>();
		Database database = Database.with(source(), DatabaseType.GENERIC).parameterRedactor((statementContext, index, parameter) -> Optional.empty())
				.statementLogger(log::set).build();
		String secret = "private-display-only-284721";
		assertEquals(secret, database.query("VALUES(CAST(:value AS VARCHAR(80)))").bind("value", secret).fetchObject(String.class).orElseThrow());
		assertEquals(List.of("value"), log.get().getStatementContext().getParameterNames());
		assertEquals(Arrays.asList((Object) null), log.get().getStatementContext().getRedactedParameters());
		assertTrue(log.get().toString().contains("parameters=[null]"));
		assertFalse(log.get().toString().contains(secret));
	}

	@Test
	void secureMasksAndBatchSummariesBypassTheRedactor() {
		AtomicInteger calls = new AtomicInteger();
		ParameterRedactor redactor = new ParameterRedactor() {
			@Override public Optional<Object> redactParameter(StatementContext<?> context, Integer index, Object value) {
				calls.incrementAndGet(); throw new AssertionError("Three-argument redactor was invoked");
			}
		};
		Database database = Database.with(source(), DatabaseType.GENERIC).parameterRedactor(redactor).build();
		Statement statement = Statement.of("bypass-redactor", "VALUES(?)");
		StatementContext<?> secure = StatementContext.with(statement, database).parameters(Parameters.secure("private-value", "<explicit>"))
				.parameterNames(List.of("password")).build();
		assertEquals(List.of("<explicit>"), secure.getRedactedParameters());
		assertFalse(secure.toString().contains("private-value"));
		StatementContext<?> batch = StatementContext.with(statement, database)
				.parameters(List.of(List.of("private-first"), List.of("private-second"))).batchParameterGroups(true).build();
		assertEquals(List.of("<batch: 2 groups x 1 parameters>"), batch.getRedactedParameters());
		assertFalse(batch.toString().contains("private-"));
		assertEquals(0, calls.get());
	}

	@Test
	void runtimeFailuresFromDirectAndDecoratedPoliciesPropagateWithoutFallback() {
		for (boolean decorated : new boolean[]{false, true}) {
			IllegalStateException failure = new IllegalStateException("Redaction policy unavailable");
			ParameterRedactor policy = (context, index, parameter) -> { throw failure; };
			ParameterRedactor redactor = decorated ? (context, index, parameter) -> policy.redactParameter(context, index, parameter) : policy;
			assertSame(failure, assertThrows(IllegalStateException.class, context(redactor, "private-value")::getRedactedParameters));
		}
	}

	@Test
	void nullOptionalsFromDirectAndDecoratedPoliciesFailFastWithValueFreeDiagnostics() {
		for (boolean decorated : new boolean[]{false, true}) {
			AtomicInteger rendered = new AtomicInteger();
			Object privateValue = new Object() {
				@Override public String toString() { rendered.incrementAndGet(); throw new AssertionError("Raw parameter was rendered"); }
			};
			StatementContext<?> context = context(nullRedactor(decorated), privateValue);
			NullPointerException failure = assertThrows(NullPointerException.class, context::getRedactedParameters);
			assertEquals(NULL_OPTIONAL_MESSAGE, failure.getMessage());
			assertEquals(NULL_OPTIONAL_MESSAGE, assertThrows(NullPointerException.class, context::toString).getMessage());
			assertEquals(0, rendered.get());
		}
	}

	@Test
	void illegalNullOptionalCannotExposeRawValuesOrReplaceDriverFailureEvidence() {
		String secret = "private-null-optional-98127";
		for (boolean decorated : new boolean[]{false, true}) {
			Database database = Database.with(source(), DatabaseType.GENERIC).parameterRedactor(nullRedactor(decorated)).build();
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query("SELECT CAST(:password AS VARCHAR(80)), "
					+ "1 / CAST(:divisor AS INTEGER) FROM (VALUES(0))").bind("password", secret).bind("divisor", 0).fetchList(Object.class));
			assertEquals("22012", failure.getSqlState().orElseThrow());
			assertInstanceOf(SQLException.class, failure.getCause());
			assertTrue(failure.getMessage().contains("parameters=<unavailable>"));
			assertFalse(failure.getMessage().contains(secret));
			assertTrue(Arrays.stream(failure.getSuppressed()).anyMatch(suppressed -> suppressed instanceof NullPointerException
					&& NULL_OPTIONAL_MESSAGE.equals(suppressed.getMessage())));
		}
	}

	@Test
	void nullOptionalFromStatementLoggingRollsBackTheManagedTransaction() {
		JDBCDataSource source = source();
		Database control = Database.with(source, DatabaseType.GENERIC).build();
		control.query("CREATE TABLE work (id INTEGER, password VARCHAR(80))").execute();
		AtomicInteger logged = new AtomicInteger();
		Database database = Database.with(source, DatabaseType.GENERIC).parameterRedactor(nullRedactor(true))
				.statementLogger(log -> { logged.incrementAndGet(); log.toString(); }).build();
		NullPointerException failure = assertThrows(NullPointerException.class, () -> database.transaction(() -> {
			database.query("INSERT INTO work VALUES(:id, :password)").bind("id", 1).bind("password", "private-logger-value").execute();
		}));
		assertEquals(NULL_OPTIONAL_MESSAGE, failure.getMessage());
		assertEquals(1, logged.get());
		assertTrue(database.currentTransaction().isEmpty());
		assertEquals(0, control.query("SELECT COUNT(*) FROM work").fetchObject(Integer.class).orElseThrow());
	}

	@SuppressWarnings("DataFlowIssue")
	private static ParameterRedactor nullRedactor(boolean decorated) {
		ParameterRedactor policy = (context, index, parameter) -> null;
		return decorated ? (context, index, parameter) -> policy.redactParameter(context, index, parameter) : policy;
	}

	private static StatementContext<?> context(ParameterRedactor redactor, Object... parameters) {
		Database database = Database.with(source(), DatabaseType.GENERIC).parameterRedactor(redactor).build();
		return StatementContext.with(Statement.of("optional-redactor", "VALUES(?)"), database).parameters(parameters).build();
	}

	private static JDBCDataSource source() {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:optional_redactor_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		return source;
	}
}
