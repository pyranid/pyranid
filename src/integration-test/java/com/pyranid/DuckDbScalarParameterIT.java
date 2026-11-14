/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbScalarParameterIT {
	@TempDir Path directory;
	public record Payload(BigDecimal amount, String letter) {}

	@Test
	void characterValuesBindAsTextInScalarQueriesPredicatesAndPrimitiveInLists() {
		Database database = database("characters");
		database.query("CREATE TABLE work (id INT PRIMARY KEY, letter VARCHAR)").execute();
		assertEquals(3, database.query("INSERT INTO work VALUES (:id, :letter)").batchChunkSize(2).executeBatch(List.of(
				Map.of("id", 1, "letter", '\''), Map.of("id", 2, "letter", Optional.of('Ω')),
				Map.of("id", 3, "letter", Parameters.secure('é')))).size());
		database.query("INSERT INTO work VALUES (4, :letter)").bind("letter", Optional.empty()).execute();
		assertEquals(List.of("'", "Ω", "é"), database.query("SELECT letter FROM work WHERE letter IS NOT NULL ORDER BY id").fetchList(String.class));
		assertEquals(List.of(1, 2), database.query("SELECT id FROM work WHERE letter IN (:letters) ORDER BY id")
				.bind("letters", Parameters.inList(new char[]{'\'', 'Ω'})).fetchList(Integer.class));
		assertEquals(1L, database.query("UPDATE work SET letter=:value WHERE letter=:old").bind("value", 'Z').bind("old", 'é').execute());
		assertEquals("Ω", database.query("SELECT :value").bind("value", 'Ω').fetchObject(String.class).orElseThrow());
		assertEquals(List.of(4), database.query("SELECT id FROM work WHERE letter IS NULL").fetchList(Integer.class));
	}

	@Test
	void negativeScaleDecimalsRetainExactValuesInWritesPredicatesAndChunkedBatches() {
		Database database = database("decimals");
		database.query("CREATE TABLE work (id INT PRIMARY KEY, amount DECIMAL(38,6) NOT NULL)").execute();
		List<BigDecimal> values = List.of(new BigDecimal("1000").stripTrailingZeros(), new BigDecimal("-42000").stripTrailingZeros(),
				new BigDecimal("1E+25"), new BigDecimal(BigInteger.ZERO, Integer.MIN_VALUE), new BigDecimal("12.345678"));
		database.query("INSERT INTO work VALUES (1, :amount)").bind("amount", values.get(0)).execute();
		assertEquals(4, database.query("INSERT INTO work VALUES (:id, :amount)").batchChunkSize(2).executeBatch(List.of(
				Map.of("id", 2, "amount", values.get(1)), Map.of("id", 3, "amount", Optional.of(values.get(2))),
				Map.of("id", 4, "amount", values.get(3)), Map.of("id", 5, "amount", Parameters.secure(values.get(4))))).size());
		List<BigDecimal> stored = database.query("SELECT amount FROM work ORDER BY id").fetchList(BigDecimal.class);
		for (int index = 0; index < values.size(); index++) {
			assertEquals(0, values.get(index).compareTo(stored.get(index)));
			assertEquals(List.of(index + 1), database.query("SELECT id FROM work WHERE amount=:value").bind("value", values.get(index)).fetchList(Integer.class));
		}
		assertEquals(0L, database.query("SELECT COUNT(*) FROM work WHERE amount IS NULL").fetchObject(Long.class).orElseThrow());
		BigDecimal replacement = new BigDecimal("12000").stripTrailingZeros();
		assertEquals(1L, database.query("UPDATE work SET amount=:value WHERE amount=:old")
				.bind("value", replacement).bind("old", values.get(0)).execute());
		assertEquals(0, replacement.compareTo(database.query("SELECT amount FROM work WHERE id=1").fetchObject(BigDecimal.class).orElseThrow()));
		assertEquals(-3, values.get(0).scale()); assertEquals(-3, replacement.scale());
		BigDecimal boundary = new BigDecimal("1E+37");
		assertEquals(boundary.setScale(0), database.query("SELECT :value").bind("value", boundary).fetchObject(BigDecimal.class).orElseThrow());
		BigDecimal tiny = new BigDecimal("1E-38");
		assertEquals(tiny, database.query("SELECT :value").bind("value", tiny).fetchObject(BigDecimal.class).orElseThrow());
	}

	@Test
	void scalarNormalizationAlsoAppliesWithinSqlArraysAndStructsWithoutMutatingInputs() {
		Database database = database("containers");
		BigDecimal negative = new BigDecimal("12000").stripTrailingZeros();
		Object[] numbers = {negative, null, BigDecimal.ZERO};
		assertArrayEquals(new BigDecimal[]{negative.setScale(0), null, BigDecimal.ZERO}, database.query("SELECT :values")
				.bind("values", Parameters.sqlArrayOf("DECIMAL(38,0)", numbers)).fetchObject(BigDecimal[].class).orElseThrow());
		assertSame(negative, numbers[0]); assertEquals(-3, negative.scale());
		Character[] letters = {'\'', 'Ω', null};
		assertArrayEquals(new String[]{"'", "Ω", null}, database.query("SELECT :values").bind("values", Parameters.sqlArrayOf("VARCHAR", letters))
				.fetchObject(String[].class).orElseThrow());
		assertArrayEquals(new Character[]{'\'', 'Ω', null}, letters);
		Payload payload = database.query("SELECT :payload").bind("payload", Parameters.sqlStructOf("STRUCT(amount DECIMAL(38,0), letter VARCHAR)",
				new Object[]{negative, 'Ω'})).fetchObject(Payload.class).orElseThrow();
		assertEquals(new Payload(negative.setScale(0), "Ω"), payload);
	}

	@Test
	void unsupportedDecimalRepresentationsFailBeforeTheyCanBecomeNullOrWriteEarlierBatchRows() {
		Database database = database("unsupported");
		database.query("CREATE TABLE work (id INT PRIMARY KEY, amount DECIMAL(38,0))").execute();
		List<BigDecimal> unsupported = List.of(new BigDecimal("1E+38"), new BigDecimal("1E+38").setScale(0),
				new BigDecimal("1E-39"), new BigDecimal(BigInteger.ZERO, Integer.MAX_VALUE),
				new BigDecimal("1.00000000000000000000000000000000000000"), new BigDecimal(BigInteger.ONE, Integer.MIN_VALUE));
		for (BigDecimal value : unsupported) {
			DatabaseException failure = assertThrows(DatabaseException.class,
					() -> database.query("INSERT INTO work VALUES (1, :value)").bind("value", Parameters.secure(value)).execute());
			assertInstanceOf(IllegalArgumentException.class, failure.getCause());
			assertTrue(failure.getCause().getMessage().contains("precision and nonnegative scale no greater than 38"));
			assertEquals(0L, database.query("SELECT COUNT(*) FROM work").fetchObject(Long.class).orElseThrow());
		}
		DatabaseException batch = assertThrows(DatabaseException.class, () -> database.transaction(() -> database.query("INSERT INTO work VALUES (:id, :amount)")
				.batchChunkSize(1).executeBatch(List.of(Map.of("id", 1, "amount", new BigDecimal("1000").stripTrailingZeros()),
						Map.of("id", 2, "amount", new BigDecimal("1E-39"))))));
		assertInstanceOf(IllegalArgumentException.class, batch.getCause());
		assertEquals(0L, database.query("SELECT COUNT(*) FROM work").fetchObject(Long.class).orElseThrow(), "Earlier executed chunks must roll back");
	}

	private Database database(String name) {
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource("jdbc:duckdb:" + directory.resolve(name + ".duckdb"), null, null),
				DatabaseType.DUCK_DB).build();
	}
}
