/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class GenericHookTypeTests {
	public record ArrayRow(List<UUID>[] value) {}
	public record VariableRow<T extends List<UUID>>(T value) {}
	public record WildcardRow(List<? extends List<UUID>> value) {}

	@Test
	void customColumnMappersSeeDeclaredGenericArrayAndTypeVariableTargets() throws Exception {
		UUID uuid = UUID.randomUUID();
		for (boolean cached : new boolean[]{false, true}) {
			for (Class<?> rowType : List.of(ArrayRow.class, VariableRow.class)) {
				Type expected = rowType.getRecordComponents()[0].getGenericType();
				Object value = rowType == ArrayRow.class ? new List<?>[]{List.of(uuid)} : List.of(uuid);
				AtomicInteger mappings = new AtomicInteger();
				CustomColumnMapper custom = new CustomColumnMapper() {
					@Override public Boolean appliesTo(TargetType target) { return expected.equals(target.getType()); }
					@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
							Integer column, String label, InstanceProvider provider) {
						assertEquals(expected, target.getType()); mappings.incrementAndGet(); return MappingResult.of(value);
					}
				};
				ResultSetMapper mapper = ResultSetMapper.withCustomColumnMappers(List.of(custom)).planCachingEnabled(cached).build();
				for (int pass = 0; pass < 2; pass++) {
					Object row = map(mapper, rowType);
					assertSame(value, rowType.getRecordComponents()[0].getAccessor().invoke(row));
				}
				assertEquals(2, mappings.get());
			}
		}
	}

	@Test
	void customBindersSeeGenericArrayWildcardAndVariableTypesForValuesAndNulls() throws Exception {
		Type array = ArrayRow.class.getRecordComponents()[0].getGenericType();
		Type variable = VariableRow.class.getTypeParameters()[0];
		Type wildcard = ((ParameterizedType) WildcardRow.class.getRecordComponents()[0].getGenericType()).getActualTypeArguments()[0];
		for (Type expected : List.of(array, variable, wildcard)) {
			AtomicInteger nonNullCalls = new AtomicInteger(), nullCalls = new AtomicInteger();
			CustomParameterBinder custom = new CustomParameterBinder() {
				@Override public Boolean appliesTo(TargetType target) { return expected.equals(target.getType()); }
				@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws java.sql.SQLException {
					nonNullCalls.incrementAndGet(); statement.setString(index, "handled"); return BindingResult.handled();
				}
				@Override public BindingResult bindNull(StatementContext<?> context, PreparedStatement statement, Integer index, TargetType target, Integer sqlType) throws java.sql.SQLException {
					assertEquals(expected, target.getType()); nullCalls.incrementAndGet(); statement.setString(index, "handled-null"); return BindingResult.handled();
				}
			};
			PreparedStatementBinder binder = PreparedStatementBinder.withCustomParameterBinders(List.of(custom));
			List<String> bound = new ArrayList<>();
			PreparedStatement statement = statement(bound);
			for (int pass = 0; pass < 2; pass++) {
				Object value = expected == array ? new List<?>[]{List.of(UUID.randomUUID())} : List.of(UUID.randomUUID());
				binder.bindParameter(context(Object.class), statement, 1, typed(expected, value));
				binder.bindParameter(context(Object.class), statement, 1, typed(expected, null));
			}
			assertEquals(2, nonNullCalls.get()); assertEquals(2, nullCalls.get());
			assertEquals(List.of("handled", "handled-null", "handled", "handled-null"), bound);
		}
	}

	@Test
	void actualClassTargetsStillCacheCustomBinderApplicability() throws Exception {
		AtomicInteger applicability = new AtomicInteger();
		CustomParameterBinder custom = new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType target) { applicability.incrementAndGet(); return target.matchesClass(String.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws java.sql.SQLException {
				statement.setString(index, value.toString()); return BindingResult.handled();
			}
		};
		PreparedStatementBinder binder = PreparedStatementBinder.withCustomParameterBinders(List.of(custom));
		List<String> bound = new ArrayList<>();
		PreparedStatement statement = statement(bound);
		binder.bindParameter(context(Object.class), statement, 1, "first");
		binder.bindParameter(context(Object.class), statement, 1, "second");
		assertEquals(1, applicability.get()); assertEquals(List.of("first", "second"), bound);
	}

	@Test
	void cachedBinderCandidatesPreservePriorityAndTypedNullFallback() throws Exception {
		List<String> calls = new ArrayList<>();
		CustomParameterBinder first = orderedBinder("first", false, calls);
		CustomParameterBinder second = orderedBinder("second", true, calls);
		CustomParameterBinder third = orderedBinder("third", true, calls);
		DefaultPreparedStatementBinder binder = new DefaultPreparedStatementBinder(List.of(first, second, third));
		List<CustomParameterBinder> candidates = binder.customBindersFor(TargetType.of(String.class));
		assertEquals(List.of(first, second, third), candidates);
		assertThrows(UnsupportedOperationException.class, () -> candidates.add(first));
		List<String> bound = new ArrayList<>();
		PreparedStatement statement = statement(bound);
		binder.bindParameter(context(Object.class), statement, 1, "value");
		binder.bindParameter(context(Object.class), statement, 1, typed(String.class, null));
		assertEquals(List.of("first", "second", "first-null", "second-null"), calls);
		assertEquals(List.of("second", "second-null"), bound);
	}

	private static CustomParameterBinder orderedBinder(String name, boolean handled, List<String> calls) {
		return new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(String.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws java.sql.SQLException {
				calls.add(name);
				if (handled) statement.setString(index, name);
				return handled ? BindingResult.handled() : BindingResult.fallback();
			}
			@Override public BindingResult bindNull(StatementContext<?> context, PreparedStatement statement, Integer index, TargetType target, Integer sqlType) throws java.sql.SQLException {
				calls.add(name + "-null");
				if (handled) statement.setString(index, name + "-null");
				return handled ? BindingResult.handled() : BindingResult.fallback();
			}
		};
	}

	private static TypedParameter typed(Type type, Object value) {
		return new TypedParameter() {
			@Override public Type getExplicitType() { return type; }
			@Override public Optional<Object> getValue() { return Optional.ofNullable(value); }
		};
	}

	private static PreparedStatement statement(List<String> bound) {
		return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> {
					if (method.getName().equals("setString")) bound.add((String) args[1]);
					return defaultValue(method.getReturnType());
				});
	}

	private static <T> Object map(ResultSetMapper mapper, Class<T> type) throws Exception {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnLabel", "getColumnName" -> "value";
					case "getColumnType" -> Types.VARCHAR;
					case "getColumnTypeName" -> "VARCHAR";
					default -> defaultValue(method.getReturnType());
				});
		ResultSet rows = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject" -> "manual";
					default -> defaultValue(method.getReturnType());
				});
		return mapper.map(context(type), rows, type, new InstanceProvider() {}).orElseThrow();
	}

	private static <T> StatementContext<T> context(Class<T> type) {
		DataSource dataSource = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> { throw new UnsupportedOperationException(method.getName()); });
		Database database = Database.with(dataSource, DatabaseType.GENERIC).build();
		return StatementContext.<T>with(Statement.of("generic-hook", "SELECT value"), database).resultSetRowType(type).build();
	}

	private static Object defaultValue(Class<?> type) {
		if (!type.isPrimitive()) return null;
		if (type == boolean.class) return false;
		if (type == int.class) return 0;
		if (type == long.class) return 0L;
		return null;
	}
}
