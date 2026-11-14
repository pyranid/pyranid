/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbFloatingPointIT {
	@TempDir Path directory;
	public record DoubleMap(Map<String, Double> value) {}
	public record FloatMap(Map<String, Float> value) {}
	public record FloatKeys(Map<Float, Double> value) {}

	@Test
	void writtenSpecialValuesRoundTripThroughFloatingScalarAndRowTargets() {
		for (boolean cached : new boolean[]{false, true}) FloatingPointIntegrationSupport.verifyStoredScalars(database(cached));
	}

	@Test
	void typedArraysKeepSpecialValuesAndStillRejectFiniteOverflow() {
		for (boolean cached : new boolean[]{false, true}) FloatingPointIntegrationSupport.verifyTypedArraysAndOverflow(database(cached));
	}

	@Test
	void typedMapKeysAndValuesCanRepresentNativeIeeeSpecials() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			String sql = "SELECT MAP(['nan', 'positive', 'negative', 'zero'], [CAST(:nan AS DOUBLE), "
					+ "CAST(:positive AS DOUBLE), CAST(:negative AS DOUBLE), CAST(:zero AS DOUBLE)]) AS value";
			Map<String, Double> doubles = mapQuery(database, sql).fetchObject(DoubleMap.class).orElseThrow().value();
			assertEquals(Map.of("nan", Double.NaN, "positive", Double.POSITIVE_INFINITY,
					"negative", Double.NEGATIVE_INFINITY, "zero", -0.0), doubles);
			assertEquals(Map.of("nan", Float.NaN, "positive", Float.POSITIVE_INFINITY,
					"negative", Float.NEGATIVE_INFINITY, "zero", -0.0f),
					mapQuery(database, sql).fetchObject(FloatMap.class).orElseThrow().value());
			assertEquals(Map.of(Float.NaN, Double.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Double.NaN),
					database.query("SELECT MAP([CAST(:nan AS DOUBLE), CAST(:positive AS DOUBLE)], "
							+ "[CAST(:negative AS DOUBLE), CAST(:nan AS DOUBLE)]) AS value")
						.bind("nan", Double.NaN).bind("positive", Double.POSITIVE_INFINITY).bind("negative", Double.NEGATIVE_INFINITY)
						.fetchObject(FloatKeys.class).orElseThrow().value());
			assertThrows(DatabaseException.class, () -> database.query("SELECT MAP(['finite'], [1e100::DOUBLE]) AS value").fetchObject(FloatMap.class));
		}
	}

	private static Query mapQuery(Database database, String sql) {
		return database.query(sql).bind("nan", Double.NaN).bind("positive", Double.POSITIVE_INFINITY)
				.bind("negative", Double.NEGATIVE_INFINITY).bind("zero", -0.0);
	}
	private Database database(boolean cached) {
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
				"jdbc:duckdb:" + directory.resolve(UUID.randomUUID() + ".duckdb"), null, null), DatabaseType.DUCK_DB)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
