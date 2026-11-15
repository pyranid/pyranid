/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.Period;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class GenericIntervalIT {
	public record Pair(SqlInterval ym, SqlInterval ds) {}
	public record NativePair(Period ym, Duration ds) {}
	public static class GenericBean<T> {
		private T amount;
		public T getAmount() { return this.amount; }
		public void setAmount(T amount) { this.amount = amount; }
	}
	public static class IntervalBean extends GenericBean<SqlInterval> {}
	private record NativeCase(String declaration, SqlInterval sample) {}

	@Test
	void targetFamilySelectsTheNativeZeroAndTypedGettersPreserveNanoseconds() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			database.query("CREATE TABLE intervals (id INT PRIMARY KEY, ym INTERVAL YEAR(9) TO MONTH, ds INTERVAL DAY(9) TO SECOND(9))").execute();
			database.transaction(() -> {
				database.query("INSERT INTO intervals VALUES (0, :ym, :ds)").bind("ym", SqlInterval.ZERO).bind("ds", SqlInterval.ZERO).execute();
				database.query("INSERT INTO intervals VALUES (1, :ym, :ds)")
						.bind("ym", SqlInterval.of(Period.ofMonths(-14))).bind("ds", SqlInterval.of(Duration.ofSeconds(-1, 1))).execute();
				database.query("INSERT INTO intervals VALUES (2, NULL, NULL)").execute();
			});
			assertEquals(List.of(new Pair(SqlInterval.ZERO, SqlInterval.ZERO), new Pair(SqlInterval.of(Period.ofMonths(-14)),
					SqlInterval.of(Duration.ofSeconds(-1, 1))), new Pair(null, null)), database.query("SELECT ym, ds FROM intervals ORDER BY id").fetchList(Pair.class));
			assertEquals(SqlInterval.ZERO, database.query("SELECT ym FROM intervals WHERE id=0").fetchObject(SqlInterval.class).orElseThrow());
			assertEquals(SqlInterval.ZERO, database.query("SELECT ds FROM intervals WHERE id=0").fetchObject(SqlInterval.class).orElseThrow());
			assertTrue(database.query("SELECT ym FROM intervals WHERE id=2").fetchObject(SqlInterval.class).isEmpty());
		}
	}

	@Test
	void supportedSubtypesAndDomainsResolveInheritedGenericPropertiesAndTypedNulls() {
		SqlInterval year = SqlInterval.of(Period.ofYears(-2));
		SqlInterval months = SqlInterval.of(Period.ofMonths(-14));
		SqlInterval day = SqlInterval.of(Duration.ofDays(-2));
		SqlInterval hour = SqlInterval.of(Duration.ofHours(-25));
		SqlInterval minute = SqlInterval.of(Duration.ofMinutes(-1502));
		SqlInterval fraction = SqlInterval.of(Duration.ofSeconds(-1, 1));
		List<NativeCase> cases = List.of(new NativeCase("INTERVAL YEAR(9)", year),
				new NativeCase("INTERVAL MONTH(9)", months), new NativeCase("INTERVAL YEAR(9) TO MONTH", months),
				new NativeCase("INTERVAL DAY(9)", day), new NativeCase("INTERVAL DAY(9) TO HOUR", hour),
				new NativeCase("INTERVAL DAY(9) TO MINUTE", minute), new NativeCase("INTERVAL DAY(9) TO SECOND(9)", fraction),
				new NativeCase("INTERVAL HOUR(9)", hour), new NativeCase("INTERVAL HOUR(9) TO MINUTE", minute),
				new NativeCase("INTERVAL HOUR(9) TO SECOND(9)", fraction), new NativeCase("INTERVAL MINUTE(9)", minute),
				new NativeCase("INTERVAL MINUTE(9) TO SECOND(9)", fraction), new NativeCase("INTERVAL SECOND(9,9)", fraction),
				new NativeCase("ym_domain", months), new NativeCase("ds_domain", fraction));
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			database.query("CREATE DOMAIN ym_domain AS INTERVAL YEAR(9) TO MONTH").execute();
			database.query("CREATE DOMAIN ds_domain AS INTERVAL DAY(9) TO SECOND(9)").execute();
			for (int index = 0; index < cases.size(); index++) {
				NativeCase test = cases.get(index);
				String table = "interval_subtype_" + index;
				database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, amount " + test.declaration() + ")").execute();
				database.query("INSERT INTO " + table + " VALUES (0, :amount)").bind("amount", test.sample()).execute();
				database.query("INSERT INTO " + table + " VALUES (1, :amount)").bind("amount", SqlInterval.ZERO).execute();
				database.query("INSERT INTO " + table + " VALUES (2, NULL)").execute();
				assertEquals(test.sample(), database.query("SELECT amount FROM " + table + " WHERE id=0")
						.fetchObject(SqlInterval.class).orElseThrow(), test.declaration());
				// HSQLDB exposes interval domains through their base family's type name. The inherited
				// erased setter must still resolve to SqlInterval, including reusable plans and SQL NULL.
				for (int pass = 0; pass < 2; pass++) {
					List<SqlInterval> actual = database.query("SELECT amount FROM " + table + " ORDER BY id")
							.fetchList(IntervalBean.class).stream().map(IntervalBean::getAmount).toList();
					assertEquals(Arrays.asList(test.sample(), SqlInterval.ZERO, null), actual, test.declaration());
				}
			}
		}
	}

	@Test
	void columnPrecisionAndLeadingYearRangeRemainExplicitDatabaseLimits() {
		Database database = database(true);
		database.query("CREATE TABLE intervals (ym INTERVAL YEAR(9) TO MONTH, ds INTERVAL DAY(2) TO SECOND(3))").execute();
		database.query("INSERT INTO intervals VALUES (:ym, :ds)").bind("ym", SqlInterval.of(Period.ofMonths(14)))
				.bind("ds", SqlInterval.of(Duration.ofNanos(1_234_567_891))).execute();
		assertEquals(SqlInterval.of(Duration.ofMillis(1234)), database.query("SELECT ds FROM intervals").fetchObject(SqlInterval.class).orElseThrow());
		// HSQLDB accepts this leading-year width but wraps the literal's month storage in the
		// server, before either JDBC getter runs. Mapping must preserve the actual stored fields.
		String wideLiteral = "VALUES INTERVAL '999999999-11' YEAR(9) TO MONTH";
		assertEquals("-73741824-01", database.query(wideLiteral).fetchObject(String.class).orElseThrow());
		assertEquals(SqlInterval.of(Period.ofMonths(-884_901_889)), database.query(wideLiteral).fetchObject(SqlInterval.class).orElseThrow());
		for (int months : new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE}) {
			SqlInterval boundary = SqlInterval.of(months, 0, Duration.ZERO);
			assertEquals(boundary, database.query("VALUES CAST(:amount AS INTERVAL YEAR(9) TO MONTH)")
					.bind("amount", boundary).fetchObject(SqlInterval.class).orElseThrow());
		}
		assertThrows(DatabaseException.class, () -> database.query("INSERT INTO intervals (ds) VALUES (:amount)")
				.bind("amount", SqlInterval.of(Period.ofDays(1))).execute());
		assertThrows(DatabaseException.class, () -> database.query("INSERT INTO intervals (ds) VALUES (:amount)")
				.bind("amount", SqlInterval.of(Period.ofMonths(1), Duration.ofHours(1))).execute());
		assertThrows(DatabaseException.class, () -> database.query("VALUES CAST('P1D' AS VARCHAR(10))").fetchObject(SqlInterval.class));
	}

	@Test
	void nativeDurationPeriodAndNullPropertiesKeepTheirExistingBehavior() {
		Database database = database(false);
		database.query("CREATE TABLE intervals (id INT, ym INTERVAL YEAR(9) TO MONTH, ds INTERVAL DAY(9) TO SECOND(9))").execute();
		database.query("INSERT INTO intervals VALUES (0, :ym, :ds)")
				.bind("ym", Period.of(1, 2, 3)).bind("ds", Duration.ofNanos(1_234_567_891)).execute();
		database.query("INSERT INTO intervals VALUES (1, NULL, NULL)").execute();
		assertEquals("1-02", database.query("SELECT CAST(ym AS VARCHAR(30)) FROM intervals WHERE id=0").fetchObject(String.class).orElseThrow());
		assertEquals(new Pair(SqlInterval.of(Period.ofMonths(14)), SqlInterval.of(Duration.ofNanos(1_234_567_891))),
				database.query("SELECT ym, ds FROM intervals WHERE id=0").fetchObject(Pair.class).orElseThrow());
		// Existing nullable Duration/Period properties must not gain a blanket planning-time rejection.
		assertEquals(new NativePair(null, null), database.query("SELECT ym, ds FROM intervals WHERE id=1").fetchObject(NativePair.class).orElseThrow());
	}

	@Test
	void unknownZeroFamilyFailsBeforeWritingAndCustomNativeBindingIsAnEscapeHatch() {
		JDBCDataSource source = source();
		Database inspection = Database.with(source, DatabaseType.GENERIC).build();
		inspection.query("CREATE TABLE intervals (amount INTERVAL DAY(9) TO SECOND(9))").execute();
		DataSource hidden = proxy(DataSource.class, source, (method, value) -> {
			if (!(value instanceof Connection connection)) return value;
			return proxy(Connection.class, connection, (connectionMethod, connectionValue) -> {
				if (!(connectionValue instanceof PreparedStatement statement)) return connectionValue;
				return proxy(PreparedStatement.class, statement, (statementMethod, statementValue) -> {
					if (statementMethod.equals("getParameterMetaData")) throw new SQLFeatureNotSupportedException("metadata unavailable", "0A000");
					return statementValue;
				});
			});
		});
		Database database = Database.with(hidden, DatabaseType.GENERIC).build();
		DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query("INSERT INTO intervals VALUES (:amount)")
				.bind("amount", SqlInterval.ZERO).execute());
		assertTrue(failure.getMessage().contains("SqlInterval"));
		assertEquals(0L, inspection.query("SELECT COUNT(*) FROM intervals").fetchObject(Long.class).orElseThrow());
		CustomParameterBinder nativeZero = new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(SqlInterval.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws java.sql.SQLException {
				assertEquals(SqlInterval.ZERO, value); statement.setObject(index, Duration.ZERO); return BindingResult.handled();
			}
		};
		Database custom = Database.with(hidden, DatabaseType.GENERIC)
				.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(nativeZero))).build();
		custom.query("INSERT INTO intervals VALUES (:amount)").bind("amount", SqlInterval.ZERO).execute();
		assertEquals(SqlInterval.ZERO, inspection.query("SELECT amount FROM intervals").fetchObject(SqlInterval.class).orElseThrow());
	}

	private static Database database(boolean cached) {
		return Database.with(source(), DatabaseType.GENERIC).resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
	private static JDBCDataSource source() {
		JDBCDataSource source = new JDBCDataSource(); source.setUrl("jdbc:hsqldb:mem:interval_" + UUID.randomUUID());
		source.setUser("SA"); source.setPassword(""); return source;
	}
	private interface ResultObserver { Object observe(String method, Object result) throws Throwable; }
	private static <T> T proxy(Class<T> type, T delegate, ResultObserver observer) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (instance, method, arguments) -> {
			try { return observer.observe(method.getName(), method.invoke(delegate, arguments)); }
			catch (InvocationTargetException failure) { throw failure.getCause(); }
		}));
	}
}
