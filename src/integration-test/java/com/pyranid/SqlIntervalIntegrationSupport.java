/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class SqlIntervalIntegrationSupport {
	private SqlIntervalIntegrationSupport() {}

	static List<SqlInterval> values() {
		return List.of(SqlInterval.ZERO, SqlInterval.of(Duration.ofMinutes(90)), SqlInterval.of(Period.of(1, 2, -3), Duration.ofNanos(-1_000)),
				SqlInterval.of(0, 1, Duration.ZERO), SqlInterval.of(0, 0, fromMicros(Long.MIN_VALUE)),
				SqlInterval.of(Integer.MAX_VALUE, Integer.MIN_VALUE, fromMicros(Long.MAX_VALUE)),
				SqlInterval.of(Integer.MIN_VALUE, Integer.MAX_VALUE, Duration.ofSeconds(-1, 1_000)));
	}

	static Duration fromMicros(long micros) {
		return Duration.ofSeconds(Math.floorDiv(micros, 1_000_000), Math.floorMod(micros, 1_000_000) * 1_000);
	}

	static long micros(Duration time) {
		return BigInteger.valueOf(time.getSeconds()).multiply(BigInteger.valueOf(1_000_000))
				.add(BigInteger.valueOf(time.getNano() / 1_000)).longValueExact();
	}

	static byte[] postgresBinary(SqlInterval value) {
		return ByteBuffer.allocate(16).putLong(micros(value.getTime())).putInt(value.getDays()).putInt(value.getMonths()).array();
	}

	static List<Map<String, Object>> groups(List<SqlInterval> values) {
		List<Map<String, Object>> groups = new ArrayList<>();
		for (int index = 0; index <= values.size(); index++) {
			Map<String, Object> group = new HashMap<>();
			group.put("id", index); group.put("amount", index == values.size() ? null : values.get(index)); groups.add(group);
		}
		return groups;
	}

	static void verifyRows(Database database, String table, List<SqlInterval> values) {
		List<IntervalRow> expected = new ArrayList<>();
		for (int index = 0; index <= values.size(); index++) expected.add(new IntervalRow(index, index == values.size() ? null : values.get(index)));
		assertEquals(expected, database.query("SELECT id, amount FROM " + table + " ORDER BY id").fetchList(IntervalRow.class));
		List<IntervalRow> beans = database.query("SELECT id, amount FROM " + table + " ORDER BY id").fetchList(IntervalBean.class)
				.stream().map(bean -> new IntervalRow(bean.getId(), bean.getAmount())).toList();
		assertEquals(expected, beans);
		for (int index = 0; index < values.size(); index++)
			assertEquals(values.get(index), database.query("SELECT amount FROM " + table + " WHERE id=:id")
					.bind("id", index).fetchObject(SqlInterval.class).orElseThrow());
		assertEquals(true, database.query("SELECT amount FROM " + table + " WHERE id=:id")
				.bind("id", values.size()).fetchObject(SqlInterval.class).isEmpty());
	}

	public record IntervalRow(int id, SqlInterval amount) {}
	public static class IntervalBean {
		private int id;
		private SqlInterval amount;
		public int getId() { return this.id; }
		public void setId(int id) { this.id = id; }
		public SqlInterval getAmount() { return this.amount; }
		public void setAmount(SqlInterval amount) { this.amount = amount; }
	}
}
