/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

final class FloatingPointIntegrationSupport {
	private FloatingPointIntegrationSupport() {}
	public record DoubleValue(Double value) {}
	public record FloatValue(Float value) {}
	public record DoubleList(List<Double> value) {}
	public record FloatList(List<Float> value) {}
	public record FloatSet(java.util.Set<Float> value) {}
	public static class DoubleBean {
		private Double value;
		public Double getValue() { return value; }
		public void setValue(Double value) { this.value = value; }
	}
	public static class FloatBean {
		private Float value;
		public Float getValue() { return value; }
		public void setValue(Float value) { this.value = value; }
	}

	static void verifyStoredScalars(Database database) {
		String table = "floating_roundtrip_" + UUID.randomUUID().toString().replace("-", "");
		database.query("CREATE TABLE " + table + " (id INT PRIMARY KEY, wide DOUBLE PRECISION, narrow REAL)").execute();
		try {
			List<Double> expected = Arrays.asList(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.0, -0.0, 1.25, null);
			for (int index = 0; index < expected.size(); index++) {
				Double value = expected.get(index);
				database.query("INSERT INTO " + table + " VALUES (:id, :wide, :narrow)").bind("id", index)
						.bind("wide", value).bind("narrow", value == null ? null : value.floatValue()).execute();
			}
			for (int index = 0; index < expected.size(); index++) {
				Double value = expected.get(index);
				String sql = "SELECT wide AS value FROM " + table + " WHERE id = " + index;
				if (value == null) {
					assertTrue(database.query(sql).fetchObject(Double.class).isEmpty());
					assertTrue(database.query(sql).fetchObject(Float.class).isEmpty());
					assertNull(database.query(sql).fetchObject(DoubleValue.class).orElseThrow().value());
					assertNull(database.query(sql).fetchObject(FloatBean.class).orElseThrow().getValue());
					continue;
				}
				assertDouble(value, database.query(sql).fetchObject(Double.class).orElseThrow());
				assertDouble(value, database.query(sql).fetchObject(DoubleValue.class).orElseThrow().value());
				assertDouble(value, database.query(sql).fetchObject(DoubleBean.class).orElseThrow().getValue());
				assertFloat(value.floatValue(), database.query(sql).fetchObject(Float.class).orElseThrow());
				assertFloat(value.floatValue(), database.query(sql).fetchObject(FloatValue.class).orElseThrow().value());
				assertFloat(value.floatValue(), database.query(sql).fetchObject(FloatBean.class).orElseThrow().getValue());
				assertDouble(value, ((Number) database.query(sql).fetchObject(Object.class).orElseThrow()).doubleValue());
				String narrow = "SELECT narrow AS value FROM " + table + " WHERE id = " + index;
				assertFloat(value.floatValue(), database.query(narrow).fetchObject(Float.class).orElseThrow());
				assertDouble((double) value.floatValue(), database.query(narrow).fetchObject(DoubleValue.class).orElseThrow().value());
			}
			assertEquals(expected.size(), database.query("SELECT COUNT(*) FROM " + table).fetchObject(Integer.class).orElseThrow());
		} finally {
			database.query("DROP TABLE " + table).execute();
		}
	}

	static void verifyTypedArraysAndOverflow(Database database) {
		Double[] wide = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0, null};
		Float[] narrow = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, -0.0f, null};
		assertArrayEquals(wide, arrayQuery(database).fetchObject(Double[].class).orElseThrow());
		assertArrayEquals(narrow, arrayQuery(database).fetchObject(Float[].class).orElseThrow());
		assertEquals(Arrays.asList(wide), arrayQuery(database).fetchObject(DoubleList.class).orElseThrow().value());
		assertEquals(Arrays.asList(narrow), arrayQuery(database).fetchObject(FloatList.class).orElseThrow().value());
		assertEquals(new LinkedHashSet<>(Arrays.asList(narrow)), arrayQuery(database).fetchObject(FloatSet.class).orElseThrow().value());
		assertThrows(DatabaseException.class, () -> arrayQuery(database).fetchObject(Integer[].class));
		assertThrows(DatabaseException.class, () -> arrayQuery(database).fetchObject(float[].class));
		String scalarOverflow = "SELECT CAST('1e100' AS DOUBLE PRECISION) AS value";
		for (Class<?> type : List.of(Float.class, FloatValue.class, FloatBean.class))
			assertThrows(DatabaseException.class, () -> database.query(scalarOverflow).fetchObject(type));
		assertThrows(DatabaseException.class, () -> database.query("SELECT ARRAY[CAST('1e100' AS DOUBLE PRECISION)] AS value")
				.fetchObject(FloatList.class));
	}

	private static Query arrayQuery(Database database) {
		return database.query("SELECT ARRAY[CAST(:nan AS DOUBLE PRECISION), CAST(:positive AS DOUBLE PRECISION), "
				+ "CAST(:negative AS DOUBLE PRECISION), CAST(:zero AS DOUBLE PRECISION), CAST(NULL AS DOUBLE PRECISION)] AS value")
				.bind("nan", Double.NaN).bind("positive", Double.POSITIVE_INFINITY)
				.bind("negative", Double.NEGATIVE_INFINITY).bind("zero", -0.0);
	}

	private static void assertDouble(double expected, double actual) { assertEquals(Double.doubleToLongBits(expected), Double.doubleToLongBits(actual)); }
	private static void assertFloat(float expected, float actual) { assertEquals(Float.floatToIntBits(expected), Float.floatToIntBits(actual)); }
}
