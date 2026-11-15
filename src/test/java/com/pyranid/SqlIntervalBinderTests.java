/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.util.PGobject;

import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Struct;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SqlIntervalBinderTests {
	@AfterEach void clearInterruption() { Thread.interrupted(); }

	@Test
	void postgresUsesTypedDriverObjectWithoutDescribeIncludingZero() throws SQLException {
		for (SqlInterval value : List.of(SqlInterval.ZERO, SqlInterval.of(Period.of(1, -2, 3), Duration.ofMinutes(-90)))) {
			Probe probe = new Probe(DatabaseType.POSTGRESQL);
			probe.bind(value);
			assertEquals(0, probe.describes);
			PGobject parameter = assertInstanceOf(PGobject.class, probe.values.get(0));
			assertEquals("interval", parameter.getType());
			assertEquals(SqlIntervalText.formatPostgres(value), parameter.getValue());
		}
	}

	@Test
	void duckUsesExactSignedMicrosecondTextWithoutDescribe() throws SQLException {
		Probe probe = new Probe(DatabaseType.DUCK_DB);
		probe.bind(SqlInterval.of(1, -2, SqlIntervalText.fromMicroseconds(Long.MIN_VALUE)));
		assertEquals(List.of("1 months -2 days -9223372036854 seconds -775808 microseconds"), probe.values);
		assertEquals(0, probe.describes);
		assertTrue(probe.events.contains("setString:1"));
	}

	@Test
	void knownAdapterLossesFailBeforeTheParameterSetter() {
		for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.DUCK_DB)) {
			for (SqlInterval value : List.of(SqlInterval.of(Duration.ofNanos(1)), SqlInterval.of(Duration.ofSeconds(Long.MAX_VALUE)))) {
				Probe probe = new Probe(type);
				assertThrows(DatabaseException.class, () -> probe.bind(value));
				assertTrue(probe.values.isEmpty());
				assertEquals(0, probe.describes);
			}
		}
		for (boolean positive : new boolean[]{false, true}) {
			Probe probe = new Probe(DatabaseType.POSTGRESQL);
			int field = positive ? Integer.MAX_VALUE : Integer.MIN_VALUE;
			SqlInterval value = SqlInterval.of(field, field, SqlIntervalText.fromMicroseconds(positive ? Long.MAX_VALUE : Long.MIN_VALUE));
			assertThrows(DatabaseException.class, () -> probe.bind(value));
			assertTrue(probe.values.isEmpty());
		}
	}

	@Test
	void nativeZeroChoosesOnlyThePositivelyIdentifiedTargetFamily() throws SQLException {
		for (DatabaseType type : List.of(DatabaseType.ORACLE, DatabaseType.GENERIC)) {
			for (boolean yearMonth : new boolean[]{false, true}) {
				Probe probe = new Probe(type);
				probe.types = new int[]{type == DatabaseType.ORACLE ? (yearMonth ? -103 : -104) : Types.VARCHAR};
				probe.names = new String[]{yearMonth ? "INTERVAL YEAR(9) TO MONTH" : "INTERVAL DAY(9) TO SECOND(9)"};
				probe.bind(SqlInterval.ZERO);
				if (!yearMonth && type == DatabaseType.ORACLE)
					assertEquals("+0 00:00:00.000000000", probe.values.get(0).toString());
				else assertEquals(List.of(yearMonth ? Period.ZERO : Duration.ZERO), probe.values);
				assertEquals(1, probe.describes);
				assertEquals(1, probe.typeReads);
				assertEquals(1, probe.nameReads);
			}
		}
	}

	@Test
	void missingUnknownAndConflictingZeroFamilyNeverTriesASetter() {
		for (DatabaseType type : List.of(DatabaseType.ORACLE, DatabaseType.GENERIC)) {
			for (String name : new String[]{null, "INTERVAL", "VARCHAR", "INTERVAL[]"}) {
				Probe probe = new Probe(type);
				probe.names = new String[]{name};
				DatabaseException failure = assertThrows(DatabaseException.class, () -> probe.bind(SqlInterval.ZERO));
				assertTrue(failure.getMessage().contains("family"));
				assertTrue(probe.values.isEmpty());
			}
			Probe unavailable = new Probe(type);
			unavailable.metadataFailure = new SQLException("metadata unavailable", "0A000");
			assertThrows(DatabaseException.class, () -> unavailable.bind(SqlInterval.ZERO));
			assertTrue(unavailable.values.isEmpty());
		}
		Probe conflict = new Probe(DatabaseType.ORACLE);
		conflict.types = new int[]{-103};
		conflict.names = new String[]{"INTERVALDS"};
		assertThrows(DatabaseException.class, () -> conflict.bind(SqlInterval.ZERO));
		assertTrue(conflict.values.isEmpty());
	}

	@Test
	void nonzeroNativeValuesRetainNanosecondsAndDoNotRequestNewMetadata() throws SQLException {
		for (DatabaseType type : List.of(DatabaseType.ORACLE, DatabaseType.GENERIC)) {
			Probe probe = new Probe(type);
			Duration time = Duration.ofDays(-2).plusNanos(123456789);
			probe.bind(SqlInterval.of(time));
			probe.bind(SqlInterval.of(Period.ofMonths(-14)));
			if (type == DatabaseType.ORACLE) {
				assertEquals("oracle.sql.INTERVALDS", probe.values.get(0).getClass().getName());
				assertEquals("-1 23:59:59.876543211", probe.values.get(0).toString());
			} else assertSame(time, probe.values.get(0));
			assertEquals(Period.of(-1, -2, 0), probe.values.get(1));
			assertEquals(0, probe.describes);
			for (SqlInterval value : List.of(SqlInterval.of(Period.ofDays(1)), SqlInterval.of(Period.ofMonths(1), Duration.ofHours(1)))) {
				Probe invalid = new Probe(type);
				assertThrows(DatabaseException.class, () -> invalid.bind(value));
				assertTrue(invalid.values.isEmpty());
				assertEquals(0, invalid.describes);
			}
		}
	}

	@Test
	void alreadyCachedIncompatibleNativeTargetIsRejectedWithoutAnotherDescribe() throws SQLException {
		for (DatabaseType type : List.of(DatabaseType.ORACLE, DatabaseType.GENERIC)) {
			for (boolean yearMonth : new boolean[]{false, true}) {
				Probe probe = new Probe(type);
				probe.types = new int[]{type == DatabaseType.ORACLE ? (yearMonth ? -103 : -104) : Types.VARCHAR};
				probe.names = new String[]{yearMonth ? "INTERVAL YEAR TO MONTH" : "INTERVAL DAY TO SECOND"};
				var resolver = new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(probe.context, probe.statement);
				assertTrue(resolver.determineParameterSqlType(1).isPresent());
				SqlInterval opposite = yearMonth ? SqlInterval.of(Duration.ofHours(1)) : SqlInterval.of(Period.ofMonths(14));
				assertThrows(DatabaseException.class, () -> new DefaultPreparedStatementBinder().bindParameter(
						probe.context, probe.statement, 1, opposite, resolver));
				assertEquals(1, probe.describes);
				assertEquals(1, probe.typeReads);
				assertEquals(1, probe.nameReads);
				assertTrue(probe.values.isEmpty());
				SqlInterval matching = yearMonth ? SqlInterval.of(Period.ofMonths(14)) : SqlInterval.of(Duration.ofHours(1));
				new DefaultPreparedStatementBinder().bindParameter(probe.context, probe.statement, 1, matching, resolver);
				assertEquals(1, probe.values.size());
				assertEquals(1, probe.describes);
				assertEquals(1, probe.typeReads);
				assertEquals(1, probe.nameReads);
			}
		}
	}

	@Test
	void cachedNullExpressionMetadataDoesNotVetoNonzeroNativeIntervals() throws SQLException {
		record Evidence(int code, String name) {}
		List<Evidence> expressionEvidence = List.of(new Evidence(Types.TIMESTAMP, "TIMESTAMP"),
				new Evidence(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE"),
				new Evidence(-100, "TIMESTAMP"), new Evidence(-101, "TIMESTAMP WITH TIME ZONE"),
				new Evidence(-102, "TIMESTAMP WITH LOCAL TIME ZONE"), new Evidence(Types.INTEGER, "INTEGER"),
				new Evidence(Types.INTEGER, "INTERVAL YEAR TO MONTH"), new Evidence(Types.OTHER, "INTERVAL"),
				new Evidence(Types.OTHER, null));
		Duration fraction = Duration.ofNanos(-1);
		for (DatabaseType type : List.of(DatabaseType.ORACLE, DatabaseType.GENERIC)) {
			for (Evidence evidence : expressionEvidence) {
				Probe probe = new Probe(type);
				probe.types = new int[]{evidence.code()};
				probe.names = new String[]{evidence.name()};
				var binder = new DefaultPreparedStatementBinder();
				var resolver = new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(probe.context, probe.statement);
				binder.bindParameter(probe.context, probe.statement, 1, Optional.empty(), resolver);
				assertEquals(evidence.code(), probe.values.get(0));
				binder.bindParameter(probe.context, probe.statement, 1, SqlInterval.of(fraction), resolver);
				binder.bindParameter(probe.context, probe.statement, 1, SqlInterval.of(Period.ofMonths(-14)), resolver);
				if (type == DatabaseType.ORACLE) {
					assertEquals("oracle.sql.INTERVALDS", probe.values.get(1).getClass().getName());
					assertEquals("-0 00:00:00.000000001", probe.values.get(1).toString());
				} else assertSame(fraction, probe.values.get(1));
				assertEquals(Period.of(-1, -2, 0), probe.values.get(2));
				assertEquals(1, probe.describes, "Later rows must reuse the NULL row's snapshot without another describe");
				assertEquals(1, probe.typeReads);
				assertEquals(1, probe.nameReads);
			}
		}
	}

	@Test
	void terminalZeroMetadataAndSetterFailuresRetainTheirOriginalIdentity() {
		Probe cancelled = new Probe(DatabaseType.ORACLE);
		SQLException failure = new SQLException("cancelled", "57014");
		cancelled.metadataFailure = failure;
		assertSame(failure, assertThrows(SQLException.class, () -> cancelled.bind(SqlInterval.ZERO)));
		assertTrue(cancelled.values.isEmpty());
		for (DatabaseType type : List.of(DatabaseType.DUCK_DB, DatabaseType.ORACLE, DatabaseType.GENERIC, DatabaseType.POSTGRESQL)) {
			Probe probe = new Probe(type);
			probe.setterFailure = failure;
			assertSame(failure, assertThrows(SQLException.class, () -> probe.bind(SqlInterval.of(Duration.ofSeconds(1)))));
			assertEquals(1, probe.values.size(), "Never retry a failed setter with another representation");
		}
	}

	@Test
	void plainDurationAndPeriodPassThroughIsUnchangedOnEveryDialect() throws SQLException {
		for (DatabaseType type : DatabaseType.values()) {
			Probe probe = new Probe(type);
			for (Object value : List.of(Duration.ofNanos(123456789), Period.of(1, 2, 3))) {
				probe.bind(value);
				assertSame(value, probe.values.get(probe.values.size() - 1), type.toString());
			}
			assertEquals(0, probe.describes);
		}
	}

	@Test
	void customHandledOverridesUnsupportedAndZeroTargetsWhileFallbackKeepsTheDefaultGuard() throws SQLException {
		for (DatabaseType type : List.of(DatabaseType.SQLITE, DatabaseType.ORACLE)) {
			Probe handled = new Probe(type);
			var custom = custom(true);
			new DefaultPreparedStatementBinder(List.of(custom)).bindParameter(handled.context, handled.statement, 1, SqlInterval.ZERO);
			assertEquals(List.of("custom"), handled.values);
			assertEquals(0, handled.describes);
			Probe fallback = new Probe(type);
			assertThrows(DatabaseException.class, () -> new DefaultPreparedStatementBinder(List.of(custom(false)))
					.bindParameter(fallback.context, fallback.statement, 1, SqlInterval.ZERO));
			assertTrue(fallback.values.isEmpty());
		}
	}

	@Test
	void intervalContainersFailBeforeConstructionAndWholeContainerCustomOverrideStillWins() throws SQLException {
		for (Object container : List.of(Parameters.sqlArrayOf("interval", List.of(SqlInterval.ZERO)),
				Parameters.sqlStructOf("example_interval", List.of(SqlInterval.ZERO)))) {
			Probe probe = new Probe(DatabaseType.DUCK_DB);
			DatabaseException failure = assertThrows(DatabaseException.class, () -> probe.bind(container));
			assertTrue(failure.getMessage().contains("whole-container"));
			assertEquals(0, probe.containerCreations);
			assertTrue(probe.values.isEmpty());
			Probe overridden = new Probe(DatabaseType.DUCK_DB);
			new DefaultPreparedStatementBinder(List.of(custom(true))).bindParameter(overridden.context, overridden.statement, 1, container);
			assertEquals(List.of("custom"), overridden.values);
			assertEquals(0, overridden.containerCreations);
		}
	}

	@Test
	void trustedIntervalKeepsOneProtectedSnapshotForOtherBatchParameterDemands() throws SQLException {
		Probe probe = new Probe(DatabaseType.POSTGRESQL);
		probe.autoCommit = false;
		probe.types = new int[]{Types.OTHER, Types.INTEGER, Types.TIMESTAMP_WITH_TIMEZONE};
		probe.names = new String[]{"interval", "int4", "timestamptz"};
		DefaultPreparedStatementBinder binder = new DefaultPreparedStatementBinder();
		List<List<Object>> groups = List.of(
				List.of(SqlInterval.of(Duration.ofSeconds(1)), Optional.empty(), Instant.parse("2024-01-02T03:04:05Z")),
				List.of(SqlInterval.ZERO, Optional.empty(), Instant.parse("2025-01-02T03:04:05Z")));
		var resolver = new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(probe.context, probe.statement);
		binder.registerParameterSqlTypeDemands(probe.context, resolver, groups);
		assertTrue(probe.events.isEmpty());
		resolver.prefetchRegisteredParameterSqlTypes();
		assertEquals(List.of("savepoint", "metadata", "type:2", "name:2", "type:3", "name:3", "release"), probe.events);
		for (List<Object> group : groups)
			for (int index = 0; index < group.size(); index++)
				binder.bindParameter(probe.context, probe.statement, index + 1, group.get(index), resolver);
		assertEquals(1, probe.describes);
		assertEquals(1, probe.savepoints);
		assertEquals(1, probe.releases);
		assertEquals(2, probe.typeReads);
		assertEquals(2, probe.nameReads);
		assertEquals(6, probe.values.size());
	}

	@Test
	void customBinderBarrierStillPreventsSpeculativeMetadataPrefetch() throws SQLException {
		Probe probe = new Probe(DatabaseType.POSTGRESQL);
		probe.autoCommit = false;
		DefaultPreparedStatementBinder binder = new DefaultPreparedStatementBinder(List.of(custom(false)));
		var resolver = new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(probe.context, probe.statement);
		binder.registerParameterSqlTypeDemands(probe.context, resolver, List.of(List.of(SqlInterval.ZERO, Optional.empty())));
		resolver.prefetchRegisteredParameterSqlTypes();
		assertTrue(probe.events.isEmpty());
		binder.bindParameter(probe.context, probe.statement, 1, SqlInterval.ZERO, resolver);
		assertEquals(List.of("setObject:1"), probe.events);
		binder.bindParameter(probe.context, probe.statement, 2, Optional.empty(), resolver);
		assertEquals(1, probe.describes);
		assertEquals("savepoint", probe.events.get(1), "Late protection must remain ordered after the callback/setter barrier");
	}

	private static CustomParameterBinder custom(boolean handled) {
		return new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType target) { return true; }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws SQLException {
				if (!handled) return BindingResult.fallback();
				statement.setString(index, "custom");
				return BindingResult.handled();
			}
		};
	}

	private static final class Probe {
		final StatementContext<?> context;
		final PreparedStatement statement;
		final List<String> events = new ArrayList<>();
		final List<Object> values = new ArrayList<>();
		int[] types = {Types.OTHER, Types.INTEGER};
		String[] names = {null, "int4"};
		int describes, typeReads, nameReads, savepoints, releases, containerCreations;
		boolean autoCommit = true;
		SQLException metadataFailure, setterFailure;
		Probe(DatabaseType type) {
			context = SqlIntervalAdapterTests.context(type);
			ClassLoader loader = type == DatabaseType.ORACLE ? OracleIntervalTestDriver.sharedLoader() : getClass().getClassLoader();
			Class<?>[] apis = type == DatabaseType.POSTGRESQL ? new Class<?>[]{Connection.class, PGConnection.class}
					: type == DatabaseType.ORACLE ? new Class<?>[]{Connection.class, oracleApi(loader)}
					: new Class<?>[]{Connection.class};
			Connection connection = (Connection) Proxy.newProxyInstance(loader, apis,
					(proxy, method, args) -> switch (method.getName()) {
						case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
						case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
						case "getAutoCommit" -> autoCommit;
						case "setSavepoint" -> { events.add("savepoint"); savepoints++; yield new Savepoint() {
							@Override public int getSavepointId() { return 1; }
							@Override public String getSavepointName() { return "interval_metadata"; }
						}; }
						case "releaseSavepoint" -> { events.add("release"); releases++; yield null; }
						case "createArrayOf" -> { containerCreations++; yield Proxy.newProxyInstance(getClass().getClassLoader(),
								new Class<?>[]{Array.class}, (array, call, parameters) -> null); }
						case "createStruct" -> { containerCreations++; yield Proxy.newProxyInstance(getClass().getClassLoader(),
								new Class<?>[]{Struct.class}, (struct, call, parameters) -> null); }
						default -> throw new AssertionError(method);
					});
			ParameterMetaData metadata = (ParameterMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
					new Class<?>[]{ParameterMetaData.class}, (proxy, method, args) -> {
						int index = (Integer) args[0];
						return switch (method.getName()) {
							case "getParameterType" -> { events.add("type:" + index); typeReads++; yield types[index - 1]; }
							case "getParameterTypeName" -> { events.add("name:" + index); nameReads++; yield names[index - 1]; }
							default -> throw new AssertionError(method);
						};
					});
			statement = (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{PreparedStatement.class},
					(proxy, method, args) -> {
						if (method.getName().equals("getConnection")) return connection;
						if (method.getName().equals("getParameterMetaData")) {
							events.add("metadata"); describes++;
							if (metadataFailure != null) throw metadataFailure;
							return metadata;
						}
						if (method.getName().startsWith("set")) {
							events.add(method.getName() + ":" + args[0]);
							values.add(args[1]);
							if (setterFailure != null) throw setterFailure;
							return null;
						}
						throw new AssertionError(method);
					});
		}
		void bind(Object value) throws SQLException { new DefaultPreparedStatementBinder().bindParameter(context, statement, 1, value); }
		private static Class<?> oracleApi(ClassLoader loader) {
			try { return loader.loadClass("oracle.jdbc.OracleConnection"); }
			catch (ClassNotFoundException failure) { throw new AssertionError(failure); }
		}
	}
}
