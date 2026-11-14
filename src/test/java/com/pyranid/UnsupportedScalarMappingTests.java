/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.time.Duration;
import java.time.MonthDay;
import java.time.Period;
import java.time.Year;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class UnsupportedScalarMappingTests {
	@Test
	void unsupportedSingleColumnJdkValueTargetsRecommendCustomColumnMappers() {
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			for (Class<?> target : List.of(Duration.class, Period.class, MonthDay.class)) {
				for (Object source : new Object[]{"P1D", null}) {
					DatabaseException failure = assertThrows(DatabaseException.class, () -> map(mapper, source, target, 1));
					assertTrue(failure.getMessage().contains(target.getName()));
					assertTrue(failure.getMessage().contains("CustomColumnMapper"));
					assertTrue(failure.getMessage().contains("ResultSetMapper.withCustomColumnMappers"));
					assertFalse(failure.getMessage().contains("bean setters"));
				}
			}
		}
	}

	@Test
	void guidanceRetainsCustomMapperNullAndMultiColumnBehavior() throws Exception {
		AtomicInteger calls = new AtomicInteger();
		CustomColumnMapper custom = new CustomColumnMapper() {
			@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Duration.class); }
			@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
					Integer column, String label, InstanceProvider provider) {
				calls.incrementAndGet(); return MappingResult.of(Duration.ofDays(1));
			}
		};
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withCustomColumnMappers(List.of(custom)).planCachingEnabled(cached).build();
			assertEquals(Duration.ofDays(1), map(mapper, "P1D", Duration.class, 1).orElseThrow());
			int beforeNull = calls.get();
			assertTrue(map(mapper, null, Duration.class, 1).isEmpty());
			assertEquals(beforeNull, calls.get(), "NULL scalar inputs retain their existing empty-fetch behavior");
			ResultSetMapper standard = ResultSetMapper.withPlanCachingEnabled(cached).build();
			DatabaseException failure = assertThrows(DatabaseException.class, () -> map(standard, "P1D", Duration.class, 2));
			assertTrue(failure.getMessage().contains("bean setters"));
			assertEquals(Year.of(2026), map(standard, "2026", Year.class, 1).orElseThrow());
		}
	}

	private static <T> Optional<T> map(ResultSetMapper mapper, Object source, Class<T> type, int columns) throws Exception {
		DataSource dataSource = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> { throw new UnsupportedOperationException(method.getName()); });
		Database database = Database.with(dataSource, DatabaseType.GENERIC).build();
		StatementContext<T> context = StatementContext.<T>with(Statement.of("unsupported-scalar", "SELECT value"), database).resultSetRowType(type).build();
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> columns;
					case "getColumnLabel", "getColumnName" -> "value" + args[0];
					case "getColumnType" -> Types.VARCHAR;
					case "getColumnTypeName" -> "VARCHAR";
					default -> defaultValue(method.getReturnType());
				});
		ResultSet rows = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject", "getString" -> source;
					case "wasNull" -> source == null;
					default -> defaultValue(method.getReturnType());
				});
		return mapper.map(context, rows, type, new InstanceProvider() {});
	}

	private static Object defaultValue(Class<?> type) {
		if (!type.isPrimitive()) return null;
		if (type == boolean.class) return false;
		if (type == int.class) return 0;
		if (type == long.class) return 0L;
		return null;
	}
}
