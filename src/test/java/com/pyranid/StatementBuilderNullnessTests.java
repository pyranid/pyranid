/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.AnnotatedArrayType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StatementBuilderNullnessTests {

	@Test
	void publicStatementDiagnosticsAndBuildersDeclareReferenceNullness() {
		for (Class<?> type : List.of(StatementLog.class, StatementLog.Builder.class,
				StatementContext.class, StatementContext.Builder.class)) {
			for (var method : type.getDeclaredMethods()) {
				if (!Modifier.isPublic(method.getModifiers()) || method.isSynthetic()) continue;
				if (!method.getReturnType().isPrimitive())
					assertNullness(method.getAnnotatedReturnType(), method.toString());
				for (AnnotatedType parameter : method.getAnnotatedParameterTypes())
					if (!(parameter.getType() instanceof Class<?> raw) || !raw.isPrimitive())
						assertNullness(parameter, method.toString());
			}
		}
	}

	@Test
	void nullableContextInputsAndLogFieldsRetainTheirClearingAndChainingBehavior() throws Exception {
		DataSource source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
				new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
					throw new AssertionError("Building diagnostics must not acquire a connection");
				});
		Database database = Database.with(source, DatabaseType.GENERIC).build();
		var contextBuilder = StatementContext.<String>with(Statement.of("nullness", "SELECT 1"), database);
		contextBuilder.resultSetRowType(String.class).resultSetRowType(null).parameters("first", null);
		StatementContext<?> context = contextBuilder.build();
		assertEquals(Arrays.asList("first", null), context.getParameters());
		assertTrue(context.getResultSetRowType().isEmpty());
		assertTrue(contextBuilder.parameters((Object[]) null).build().getParameters().isEmpty());
		assertTrue(contextBuilder.parameters((List<Object>) null).build().getParameters().isEmpty());
		var parameters = StatementContext.Builder.class.getMethod("parameters", Object[].class);
		var array = (AnnotatedArrayType) parameters.getAnnotatedParameterTypes()[0];
		assertNotNull(array.getAnnotation(Nullable.class));
		assertNotNull(array.getAnnotatedGenericComponentType().getAnnotation(Nullable.class));
		assertNotNull(StatementContext.Builder.class.getMethod("resultSetRowType", Class.class)
				.getAnnotatedParameterTypes()[0].getAnnotation(Nullable.class));

		var builder = StatementLog.withStatementContext(context);
		assertSame(builder, builder.preparationDuration(Duration.ofNanos(1)).preparationDuration(null));
		assertSame(builder, builder.executionDuration(Duration.ofNanos(2)).executionDuration(null));
		assertSame(builder, builder.resultSetMappingDuration(Duration.ofNanos(3)).resultSetMappingDuration(null));
		assertSame(builder, builder.batchSize(1).batchSize(null));
		assertSame(builder, builder.exception(new IllegalStateException()).exception(null));
		StatementLog<?> log = builder.build();
		assertTrue(log.getPreparationDuration().isEmpty());
		assertTrue(log.getExecutionDuration().isEmpty());
		assertTrue(log.getResultSetMappingDuration().isEmpty());
		assertTrue(log.getBatchSize().isEmpty());
		assertTrue(log.getException().isEmpty());
	}

	private static void assertNullness(AnnotatedType type, String member) {
		assertTrue(type.isAnnotationPresent(NonNull.class) || type.isAnnotationPresent(Nullable.class),
				() -> "Missing reference nullness on " + member + ": " + type);
	}
}
