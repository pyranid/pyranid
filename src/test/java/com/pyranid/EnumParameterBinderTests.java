/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EnumParameterBinderTests {
	private interface Coded { String code(); }
	private enum Plan implements Coded {
		FREE { @Override public String code() { return "free"; } },
		PRO { @Override public String code() { return "premium"; } }
	}
	private enum PlainPlan { BASIC, PLUS }

	@Test
	void implicitEnumTypeAndOriginalValueAreConsistentAcrossConstantsWrappersAndBatchChunks() {
		for (Plan first : Plan.values()) {
			List<Class<?>> selectedTypes = new ArrayList<>();
			List<Object> boundValues = new ArrayList<>();
			PreparedStatementBinder binder = PreparedStatementBinder.withCustomParameterBinders(List.of(new CustomParameterBinder() {
				@Override public Boolean appliesTo(TargetType target) {
					if (!Enum.class.isAssignableFrom(target.getRawClass())) return false;
					selectedTypes.add(target.getRawClass());
					return target.matchesClass(Plan.class);
				}
				@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index,
						Object value) throws SQLException {
					boundValues.add(value);
					statement.setString(index, ((Plan) value).code());
					return BindingResult.handled();
				}
			}));
			Database database = database(binder);
			database.query("INSERT INTO work VALUES (:id, :plan)").bind("id", 1).bind("plan", first).execute();
			List<Map<String, Object>> rows = List.of(
					Map.of("id", 2, "plan", Plan.FREE),
					Map.of("id", 3, "plan", Plan.PRO),
					Map.of("id", 4, "plan", Optional.of(Plan.PRO)),
					Map.of("id", 5, "plan", Parameters.secure(Plan.FREE)),
					Map.of("id", 6, "plan", Parameters.secure(Optional.of(Plan.PRO))));
			assertEquals(5, database.query("INSERT INTO work VALUES (:id, :plan)").batchChunkSize(2).executeBatch(rows).size());
			assertEquals(List.of(first.code(), "free", "premium", "premium", "free", "premium"),
					database.query("SELECT plan FROM work ORDER BY id").fetchList(String.class));
			assertEquals(List.of(Plan.class), selectedTypes);
			assertEquals(List.of(first, Plan.FREE, Plan.PRO, Plan.PRO, Plan.FREE, Plan.PRO), boundValues);
		}
	}

	@Test
	void explicitTargetTypesTakePrecedenceOverImplicitEnumDerivation() {
		List<Class<?>> selectedTypes = new ArrayList<>();
		PreparedStatementBinder binder = PreparedStatementBinder.withCustomParameterBinders(List.of(new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType target) {
				if (target.matchesClass(Integer.class)) return false;
				selectedTypes.add(target.getRawClass());
				return target.matchesClass(Coded.class) || target.matchesClass(Plan.PRO.getClass());
			}
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index,
					Object value) throws SQLException {
				assertSame(Plan.PRO, value);
				statement.setString(index, "explicit:" + ((Coded) value).code());
				return BindingResult.handled();
			}
		}));
		Database database = database(binder);
		for (Class<?> explicit : List.of(Coded.class, Plan.PRO.getClass())) {
			database.query("INSERT INTO work VALUES (:id, :plan)").bind("id", selectedTypes.size() + 1)
					.bind("plan", new DefaultTypedParameter(explicit, Plan.PRO)).execute();
		}
		assertEquals(List.of(Coded.class, Plan.PRO.getClass()), selectedTypes);
		assertEquals(List.of("explicit:premium", "explicit:premium"), database.query("SELECT plan FROM work ORDER BY id").fetchList(String.class));
	}

	@Test
	void defaultAndDeclinedCustomBindingKeepEnumNames() {
		for (boolean custom : new boolean[]{false, true}) {
			List<Class<?>> selectedTypes = new ArrayList<>();
			PreparedStatementBinder binder = custom
					? PreparedStatementBinder.withCustomParameterBinders(List.of(new CustomParameterBinder() {
						@Override public Boolean appliesTo(TargetType target) {
							selectedTypes.add(target.getRawClass());
							return true;
						}
						@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement,
								Integer index, Object value) { return BindingResult.fallback(); }
					})) : PreparedStatementBinder.withDefaultConfiguration();
			Database database = database(binder);
			database.query("INSERT INTO work VALUES (1, :plan)").bind("plan", Plan.PRO).execute();
			database.query("INSERT INTO work VALUES (2, :plan)").bind("plan", PlainPlan.PLUS).execute();
			assertEquals(List.of("PRO", "PLUS"), database.query("SELECT plan FROM work ORDER BY id").fetchList(String.class));
			assertEquals(custom ? List.of(Plan.class, PlainPlan.class) : List.of(), selectedTypes);
		}
	}

	@Test
	void explicitEnumNullStillUsesTheCustomNullBinder() {
		PreparedStatementBinder binder = PreparedStatementBinder.withCustomParameterBinders(List.of(new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Plan.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index,
					Object value) { throw new AssertionError("Non-null binder must not handle a null value"); }
			@Override public BindingResult bindNull(StatementContext<?> context, PreparedStatement statement, Integer index,
					TargetType target, Integer sqlType) throws SQLException {
				assertEquals(Plan.class, target.getType());
				statement.setString(index, "null-plan");
				return BindingResult.handled();
			}
		}));
		Database database = database(binder);
		database.query("INSERT INTO work VALUES (1, :plan)").bind("plan", new DefaultTypedParameter(Plan.class, null)).execute();
		assertEquals(List.of("null-plan"), database.query("SELECT plan FROM work").fetchList(String.class));
	}

	private static Database database(PreparedStatementBinder binder) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:enum_binders_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		Database database = Database.with(source, DatabaseType.GENERIC).preparedStatementBinder(binder).build();
		database.query("CREATE TABLE work (id INT PRIMARY KEY, plan VARCHAR(100))").execute();
		return database;
	}
}
