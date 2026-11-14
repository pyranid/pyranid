/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class FloatingPointMappingTests {
	private static final String SQL = "SELECT CAST(:value AS DOUBLE) AS value FROM (VALUES (0))";
	public record DoubleValue(Double value) {}
	public record FloatValue(Float value) {}
	public record PrimitiveDoubleValue(double value) {}
	public record PrimitiveFloatValue(float value) {}
	public record FloatList(List<Float> value) {}
	public record FloatSet(Set<Float> value) {}
	public record FloatMap(Map<String, List<Float>> value) {}
	public record FloatKeys(Map<Float, Double> value) {}
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

	@Test
	void nativeSpecialValuesAndSignedZeroSurviveScalarRecordBeanAndPrimitiveMapping() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			for (double value : new double[]{Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.0, -0.0}) {
				assertDoubleBits(value, database.query(SQL).bind("value", value).fetchObject(Double.class).orElseThrow());
				assertDoubleBits(value, database.query(SQL).bind("value", value).fetchObject(double.class).orElseThrow());
				assertDoubleBits(value, database.query(SQL).bind("value", value).fetchObject(DoubleValue.class).orElseThrow().value());
				assertDoubleBits(value, database.query(SQL).bind("value", value).fetchObject(PrimitiveDoubleValue.class).orElseThrow().value());
				assertDoubleBits(value, database.query(SQL).bind("value", value).fetchObject(DoubleBean.class).orElseThrow().getValue());
				assertFloatBits((float) value, database.query(SQL).bind("value", value).fetchObject(Float.class).orElseThrow());
				assertFloatBits((float) value, database.query(SQL).bind("value", value).fetchObject(float.class).orElseThrow());
				assertFloatBits((float) value, database.query(SQL).bind("value", value).fetchObject(FloatValue.class).orElseThrow().value());
				assertFloatBits((float) value, database.query(SQL).bind("value", value).fetchObject(PrimitiveFloatValue.class).orElseThrow().value());
				assertFloatBits((float) value, database.query(SQL).bind("value", value).fetchObject(FloatBean.class).orElseThrow().getValue());
			}
		}
	}

	@Test
	void nativeFloatAndDoubleSourcesCanWidenOrNarrowTheirRepresentableSpecialValues() {
		for (Number value : List.of(Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, -0.0f,
				Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0)) {
			assertFloatBits(value.floatValue(), (Float) convert(value, TargetType.of(Float.class)));
			assertDoubleBits(value.doubleValue(), (Double) convert(value, TargetType.of(Double.class)));
			assertSame(value, convert(value, TargetType.of(Object.class)));
			assertSame(value, convert(value, TargetType.of(Number.class)));
		}
	}

	@Test
	void finiteOverflowStillFailsInScalarRowAndTypedContainerConversions() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			for (Class<?> type : List.of(Float.class, float.class, FloatValue.class, PrimitiveFloatValue.class, FloatBean.class))
				assertThrows(DatabaseException.class, () -> database.query(SQL).bind("value", Double.MAX_VALUE).fetchObject(type));
			String array = "SELECT ARRAY[CAST(:value AS DOUBLE)] AS value FROM (VALUES (0))";
			for (Class<?> type : List.of(Float[].class, float[].class, FloatList.class, FloatSet.class))
				assertThrows(DatabaseException.class, () -> database.query(array).bind("value", Double.MAX_VALUE).fetchObject(type));
		}
		for (Number finite : List.of(Double.MAX_VALUE, new BigDecimal("1E+1000"), BigInteger.TEN.pow(1000))) {
			assertThrows(DatabaseException.class, () -> convert(finite, TargetType.of(Float.class)));
			assertThrows(DatabaseException.class, () -> convert(new Object[]{finite}, component(FloatList.class)));
			assertThrows(DatabaseException.class, () -> convert(Map.of("finite", List.of(finite)), component(FloatMap.class)));
		}
		for (Number finite : List.of(new BigDecimal("1E+1000"), BigInteger.TEN.pow(1000)))
			assertThrows(DatabaseException.class, () -> convert(finite, TargetType.of(Double.class)));
		assertDoubleBits((double) Float.MAX_VALUE, (Double) convert(Float.MAX_VALUE, TargetType.of(Double.class)));
		assertFloatBits(1.25f, (Float) convert(new BigDecimal("1.25"), TargetType.of(Float.class)));
	}

	@Test
	void arraysListsSetsAndMapValuesShareSpecialValueAndSignedZeroSemantics() throws Exception {
		Object[] source = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.0, -0.0, null};
		Float[] expected = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.0f, -0.0f, null};
		assertArrayEquals(expected, (Float[]) convert(source, TargetType.of(Float[].class)));
		assertEquals(Arrays.asList(expected), convert(source, component(FloatList.class)));
		assertEquals(new LinkedHashSet<>(Arrays.asList(expected)), convert(source, component(FloatSet.class)));
		assertEquals(Map.of("specials", Arrays.asList(expected)), convert(Map.of("specials", Arrays.asList(source)), component(FloatMap.class)));
		assertDoubleBits(-0.0, ((double[]) convert(new Object[]{-0.0f}, TargetType.of(double[].class)))[0]);
		assertFloatBits(-0.0f, ((float[]) convert(new Object[]{-0.0}, TargetType.of(float[].class)))[0]);
	}

	@Test
	void convertedNanMapKeysCannotSilentlyOverwriteAnEarlierEntry() throws Exception {
		Map<Object, Object> values = new LinkedHashMap<>();
		values.put(Double.NaN, 1.0); values.put(Float.NaN, 2.0);
		DatabaseException failure = assertThrows(DatabaseException.class, () -> convert(values, component(FloatKeys.class)));
		assertTrue(failure.getMessage().contains("duplicate key after conversion"));
		assertEquals(2, values.size());
	}

	@Test
	void nonFiniteValuesRemainInvalidForIntegralTargetsAndStringVectorLiterals() {
		for (Number value : List.of(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.NaN)) {
			for (Class<?> type : List.of(Byte.class, Short.class, Integer.class, Long.class, BigInteger.class, Character.class))
				assertThrows(DatabaseException.class, () -> convert(value, TargetType.of(type)));
		}
		for (String literal : List.of("[NaN]", "[Infinity]", "[-Infinity]")) {
			assertThrows(DatabaseException.class, () -> convert(literal, TargetType.of(float[].class)));
			assertThrows(DatabaseException.class, () -> convert(literal, TargetType.of(double[].class)));
		}
	}

	@Test
	void customMappersRetainOriginalFloatingInputsAndHandledResultsWhileNativeValuesRemainNative() {
		for (boolean cached : new boolean[]{false, true}) {
			CustomColumnMapper custom = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.getRawClass() == Double.class; }
				@Override public MappingResult map(StatementContext<?> context, java.sql.ResultSet rows, Object value,
						TargetType type, Integer index, String label, InstanceProvider provider) {
					assertInstanceOf(Double.class, value);
					return Double.isNaN((Double) value) ? MappingResult.of(7.0) : MappingResult.fallback();
				}
			};
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(custom)).build());
			assertEquals(7.0, database.query(SQL).bind("value", Double.NaN).fetchObject(Double.class).orElseThrow());
			assertEquals(7.0, database.query(SQL).bind("value", Double.NaN).fetchObject(DoubleValue.class).orElseThrow().value());
			assertEquals(Double.POSITIVE_INFINITY, database.query(SQL).bind("value", Double.POSITIVE_INFINITY).fetchObject(Double.class).orElseThrow());
			assertTrue(Double.isNaN((Double) database.query(SQL).bind("value", Double.NaN).fetchObject(Object.class).orElseThrow()));
			assertTrue(Double.isNaN((Double) database.query(SQL).bind("value", Double.NaN).fetchObject(Map.class).orElseThrow().get("value")));
		}
	}

	@Test
	void nullReferenceAndPrimitiveRulesRemainUnchanged() {
		Database database = database(ResultSetMapper.withDefaultConfiguration());
		String scalar = "SELECT CAST(NULL AS DOUBLE) AS value FROM (VALUES (0))";
		assertTrue(database.query(scalar).fetchObject(Double.class).isEmpty());
		assertTrue(database.query(scalar).fetchObject(Float.class).isEmpty());
		assertNull(database.query(scalar).fetchObject(DoubleValue.class).orElseThrow().value());
		assertNull(database.query(scalar).fetchObject(FloatBean.class).orElseThrow().getValue());
		assertThrows(DatabaseException.class, () -> database.query(scalar).fetchObject(PrimitiveDoubleValue.class));
		assertThrows(DatabaseException.class, () -> database.query(scalar).fetchObject(PrimitiveFloatValue.class));
		String array = "SELECT ARRAY[CAST(NULL AS DOUBLE)] AS value FROM (VALUES (0))";
		assertArrayEquals(new Float[]{null}, database.query(array).fetchObject(Float[].class).orElseThrow());
		assertThrows(DatabaseException.class, () -> database.query(array).fetchObject(float[].class));
	}

	private static TargetType component(Class<?> record) { return TargetType.of(record.getRecordComponents()[0].getGenericType()); }
	private static Object convert(Object value, TargetType target) {
		Database database = database(ResultSetMapper.withDefaultConfiguration());
		StatementContext<?> context = StatementContext.with(Statement.of("floating", "SELECT value"), database).build();
		return ((DefaultResultSetMapper) database.getResultSetMapper()).convertResultSetValueToTargetType(context, value, target).orElseThrow();
	}
	private static Database database(ResultSetMapper mapper) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:floating_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).timeZone(ZoneId.of("UTC")).resultSetMapper(mapper).build();
	}
	private static void assertDoubleBits(double expected, double actual) { assertEquals(Double.doubleToLongBits(expected), Double.doubleToLongBits(actual)); }
	private static void assertFloatBits(float expected, float actual) { assertEquals(Float.floatToIntBits(expected), Float.floatToIntBits(actual)); }
}
