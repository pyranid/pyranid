/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Types;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TimeRoundTripTests {
	public record Clock(Time clock) {}
	public static class ClockBean {
		private Time clock;
		public Time getClock() { return this.clock; }
		public void setClock(Time clock) { this.clock = clock; }
	}

	@Test
	void jdbcTimeMillisecondsSurviveScalarRecordBeanAndArrayRoundTrips() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			database.query("CREATE TABLE work (clock TIME(9))").execute();
			for (long epoch : new long[]{1, 123, 999, -1}) {
				Time input = new Time(epoch);
				LocalTime expected = localTime(input);
				database.query("DELETE FROM work").execute();
				database.query("INSERT INTO work VALUES (:clock)").bind("clock", input).execute();
				for (int pass = 0; pass < 3; pass++) {
					Time scalar = database.query("SELECT clock FROM work").fetchObject(Time.class).orElseThrow();
					Time record = database.query("SELECT clock FROM work").fetchObject(Clock.class).orElseThrow().clock();
					Time bean = database.query("SELECT clock FROM work").fetchObject(ClockBean.class).orElseThrow().getClock();
					for (Time actual : List.of(scalar, record, bean)) assertEquals(expected, localTime(actual));
					Time rewrite = pass == 0 ? scalar : pass == 1 ? record : bean;
					assertEquals(1L, database.query("UPDATE work SET clock=:clock WHERE clock=:clock").bind("clock", rewrite).execute());
				}
				assertEquals(expected, database.query("SELECT clock FROM work").fetchObject(LocalTime.class).orElseThrow());
				Time[] array = database.query("SELECT ARRAY[clock] FROM work").fetchObject(Time[].class).orElseThrow();
				assertEquals(expected, localTime(array[0]));
			}
		}
	}

	@Test
	void localAndOffsetTimeConversionsRetainMillisecondsAndTruncateOnlySubmilliseconds() {
		Database database = database(true);
		DefaultResultSetMapper mapper = (DefaultResultSetMapper) database.getResultSetMapper();
		StatementContext<?> context = StatementContext.with(Statement.of("legacy-time", "SELECT 1"), database).build();
		for (LocalTime time : List.of(LocalTime.parse("12:34:56.123456789"), LocalTime.parse("23:59:59.999999999"))) {
			for (Object value : List.of(time, time.atOffset(ZoneOffset.ofHoursMinutes(5, 45)))) {
				Time actual = (Time) mapper.convertResultSetValueToTargetType(context, value, TargetType.of(Time.class)).orElseThrow();
				assertEquals(time.withNano(time.getNano() / 1_000_000 * 1_000_000), localTime(actual));
				assertEquals(localTime(actual), mapper.convertResultSetValueToTargetType(context, actual, TargetType.of(LocalTime.class)).orElseThrow());
			}
		}
	}

	@Test
	void unavailableTypedGetterRetainsLegacyMillisecondsAndNativeObjectIdentity() throws Exception {
		Database database = database(true);
		Time original = new Time(-1);
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnType" -> Types.TIME;
					case "getColumnTypeName" -> "TIME";
					case "getColumnLabel", "getColumnName" -> "clock";
					case "getColumnClassName" -> Time.class.getName();
					default -> throw new AssertionError(method.getName());
				});
		ResultSet rows = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject" -> {
						if (args.length == 2) throw new SQLFeatureNotSupportedException("typed getter unavailable");
						yield original;
					}
					case "getTime" -> original;
					case "wasNull" -> false;
					default -> throw new AssertionError(method.getName());
				});
		ResultSetMapper mapper = ResultSetMapper.withDefaultConfiguration();
		assertEquals(localTime(original), map(mapper, database, rows, LocalTime.class));
		assertEquals(localTime(original), localTime(map(mapper, database, rows, Time.class)));
		assertSame(original, map(mapper, database, rows, Object.class));
	}

	private static <T> T map(ResultSetMapper mapper, Database database, ResultSet rows, Class<T> type) throws Exception {
		StatementContext<T> context = StatementContext.with(Statement.of("legacy-time-reader", "SELECT clock"), database).build();
		return mapper.map(context, rows, type, new InstanceProvider() {}).orElseThrow();
	}

	private static LocalTime localTime(Time value) {
		return value.toLocalTime().withNano((int) Math.floorMod(value.getTime(), 1000L) * 1_000_000);
	}
	private static Database database(boolean cached) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:time_roundtrip_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).timeZone(ZoneId.of("Asia/Kathmandu"))
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
