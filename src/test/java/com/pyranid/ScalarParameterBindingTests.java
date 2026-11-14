/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ScalarParameterBindingTests {
	@Test
	void characterParametersWorkInQueriesPredicatesBatchesAndPrimitiveInLists() {
		Database database = database(DatabaseType.GENERIC, PreparedStatementBinder.withDefaultConfiguration());
		database.query("CREATE TABLE work (id INT PRIMARY KEY, letter VARCHAR(4))").execute();
		database.query("INSERT INTO work VALUES (1, :letter)").bind("letter", '\'').execute();
		assertEquals(3, database.query("INSERT INTO work VALUES (:id, :letter)").batchChunkSize(2).executeBatch(List.of(
				Map.of("id", 2, "letter", 'Ω'), Map.of("id", 3, "letter", Optional.of('A')),
				Map.of("id", 4, "letter", Parameters.secure('é')))).size());
		database.query("INSERT INTO work VALUES (5, :letter)").bind("letter", Optional.empty()).execute();
		assertEquals(List.of("'", "Ω", "A", "é"), database.query("SELECT letter FROM work WHERE letter IS NOT NULL ORDER BY id").fetchList(String.class));
		assertEquals(List.of(1, 2), database.query("SELECT id FROM work WHERE letter IN (:letters) ORDER BY id")
				.bind("letters", Parameters.inList(new char[]{'\'', 'Ω'})).fetchList(Integer.class));
		assertEquals(1L, database.query("UPDATE work SET letter=:replacement WHERE letter=:old")
				.bind("replacement", 'Z').bind("old", 'A').execute());
		assertEquals(List.of(5), database.query("SELECT id FROM work WHERE letter IS NULL").fetchList(Integer.class));
		Character[] characters = {'\'', 'Ω', null};
		assertArrayEquals(new String[]{"'", "Ω", null}, database.query("VALUES (CAST(:letters AS VARCHAR(4) ARRAY))")
				.bind("letters", Parameters.sqlArrayOf("VARCHAR", characters)).fetchObject(String[].class).orElseThrow());
		assertArrayEquals(new Character[]{'\'', 'Ω', null}, characters);
	}

	@Test
	void customBindersReceiveOriginalCharacterAndDecimalValuesBeforeNormalization() {
		Character character = 'Ω';
		BigDecimal decimal = new BigDecimal("12000").stripTrailingZeros();
		List<Object> received = new ArrayList<>();
		PreparedStatementBinder binder = PreparedStatementBinder.withCustomParameterBinders(List.of(new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(Character.class) || type.matchesClass(BigDecimal.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws SQLException {
				received.add(value);
				statement.setString(index, value instanceof Character ? "custom-character" : "custom-decimal");
				return BindingResult.handled();
			}
		}));
		Database database = database(DatabaseType.DUCK_DB, binder);
		database.query("CREATE TABLE work (value VARCHAR(32))").execute();
		database.query("INSERT INTO work VALUES (:value)").bind("value", character).execute();
		database.query("INSERT INTO work VALUES (:value)").bind("value", Parameters.secure(Optional.of(decimal))).execute();
		assertSame(character, received.get(0)); assertSame(decimal, received.get(1));
		assertEquals(List.of("custom-character", "custom-decimal"), database.query("SELECT value FROM work ORDER BY value").fetchList(String.class));
		assertEquals(-3, decimal.scale());
	}

	@Test
	void declinedCustomBindingStillUsesTheDefaultCharacterAndDuckDecimalRepresentations() throws SQLException {
		List<Object> received = new ArrayList<>();
		PreparedStatementBinder binder = PreparedStatementBinder.withCustomParameterBinders(List.of(new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType type) { return true; }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) {
				received.add(value); return BindingResult.fallback();
			}
		}));
		Database database = database(DatabaseType.DUCK_DB, binder);
		BigDecimal decimal = new BigDecimal("-42000").stripTrailingZeros();
		assertEquals("Ω", bound(database, binder, 'Ω'));
		assertEquals(new BigDecimal("-42000"), bound(database, binder, decimal));
		assertEquals(List.of('Ω', decimal), received);
		assertEquals(-3, decimal.scale());
	}

	@Test
	void negativeDecimalScaleNormalizesExactlyOnlyForDuckDb() throws SQLException {
		PreparedStatementBinder binder = PreparedStatementBinder.withDefaultConfiguration();
		for (DatabaseType type : DatabaseType.values()) {
			Database database = database(type, binder);
			for (BigDecimal value : List.of(new BigDecimal("1000").stripTrailingZeros(), new BigDecimal("-42000").stripTrailingZeros(),
					new BigDecimal("12.3400"), BigDecimal.ZERO)) {
				Object actual = bound(database, binder, value);
				if (type == DatabaseType.DUCK_DB && value.scale() < 0) {
					assertEquals(value.setScale(0), actual); assertNotSame(value, actual);
				} else assertSame(value, actual, type.toString());
			}
		}
	}

	@Test
	void duckDecimalPreflightRejectsUnsupportedRepresentationsBeforeCoefficientExpansionOrJdbc() {
		BigDecimal largest = new BigDecimal("1E+37");
		assertEquals(largest.setScale(0), DuckDbDialect.INSTANCE.normalizeBigDecimal(largest));
		for (BigDecimal value : List.of(new BigDecimal("1E+38"), new BigDecimal(BigInteger.ONE, Integer.MIN_VALUE), new BigDecimal("1E-39"),
				new BigDecimal("1.00000000000000000000000000000000000000"), new BigDecimal(BigInteger.ZERO, Integer.MAX_VALUE))) {
			IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> DuckDbDialect.INSTANCE.normalizeBigDecimal(value));
			assertEquals("DuckDB decimal parameters require precision and nonnegative scale no greater than 38", failure.getMessage());
		}
		assertEquals(BigDecimal.ZERO, DuckDbDialect.INSTANCE.normalizeBigDecimal(new BigDecimal(BigInteger.ZERO, Integer.MIN_VALUE)));
		BigDecimal tiny = new BigDecimal("1E-38"), zero = BigDecimal.ZERO.setScale(38);
		assertSame(tiny, DuckDbDialect.INSTANCE.normalizeBigDecimal(tiny));
		assertSame(zero, DuckDbDialect.INSTANCE.normalizeBigDecimal(zero));
	}

	private static Object bound(Database database, PreparedStatementBinder binder, Object value) throws SQLException {
		AtomicReference<Object> received = new AtomicReference<>();
		PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> {
					if (method.getName().equals("setObject") || method.getName().equals("setString")) { received.set(args[1]); return null; }
					throw new AssertionError(method.getName());
				});
		StatementContext<?> context = StatementContext.with(Statement.of("scalar-normalization", "SELECT 1"), database).build();
		binder.bindParameter(context, statement, 1, value);
		return received.get();
	}

	private static Database database(DatabaseType type, PreparedStatementBinder binder) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:scalar_binding_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		return Database.with(source, type).preparedStatementBinder(binder).build();
	}
}
