/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class OracleParameterMetadataTests {
	@AfterEach void clearInterruption() { Thread.interrupted(); }

	@Test
	void knownOracleParserFailureCachesUnavailableMetadataForAllIndices() throws SQLException {
		Probe probe = new Probe(DatabaseType.ORACLE);
		probe.failure = parserFailure("getParameterType");
		var resolver = probe.resolver();
		resolver.registerParameterIndices(List.of(1, 2, 3));
		for (int index : new int[]{1, 2, 3, 1}) assertTrue(resolver.determineParameterSqlType(index).isEmpty());
		assertEquals(1, probe.descriptions);
		assertEquals(List.of(1), probe.typeReads);
		assertTrue(probe.nameReads.isEmpty());
	}

	@Test
	void knownNameFailureRetainsTheObtainedCodeAndSkipsRemainingMetadata() throws SQLException {
		Probe probe = new Probe(DatabaseType.ORACLE);
		probe.failurePhase = "name"; probe.failure = parserFailure("getParameterTypeName"); probe.sqlType = -104;
		var resolver = probe.resolver();
		resolver.registerParameterIndices(List.of(1, 2, 3));
		assertEquals(-104, resolver.determineParameterSqlType(1).orElseThrow().getSqlType());
		assertTrue(resolver.determineParameterSqlType(2).isEmpty());
		assertEquals(-104, resolver.determineParameterSqlType(1).orElseThrow().getSqlType());
		assertEquals(1, probe.descriptions);
		assertEquals(List.of(1), probe.typeReads);
		assertEquals(List.of(1), probe.nameReads);
	}

	@Test
	void plainNullUsesTheExistingUntypedFallbackAfterKnownParserFailure() throws SQLException {
		Probe probe = new Probe(DatabaseType.ORACLE); probe.failure = parserFailure("getParameterType");
		var resolver = probe.resolver();
		var binder = new DefaultPreparedStatementBinder();
		binder.bindParameter(probe.context, probe.statement, 1, Optional.empty(), resolver);
		binder.bindParameter(probe.context, probe.statement, 2, Optional.empty(), resolver);
		assertEquals(List.of("1:" + Types.NULL, "2:" + Types.NULL), probe.nulls);
		assertEquals(1, probe.descriptions);
		assertEquals(List.of(1), probe.typeReads);
	}

	@Test
	void ambiguousZeroRetainsItsFamilyErrorWithoutTheDriverNullPointerException() {
		Probe probe = new Probe(DatabaseType.ORACLE); probe.failure = parserFailure("getParameterType");
		var resolver = probe.resolver();
		for (int attempt = 0; attempt < 2; attempt++) {
			DatabaseException failure = assertThrows(DatabaseException.class,
					() -> new DefaultPreparedStatementBinder().bindParameter(probe.context, probe.statement, 1, SqlInterval.ZERO, resolver));
			assertTrue(failure.getMessage().contains("Cannot choose the SQL interval family for SqlInterval.ZERO"));
			assertNull(failure.getCause());
		}
		assertEquals(1, probe.descriptions);
		assertEquals(List.of(1), probe.typeReads);
		assertTrue(probe.nulls.isEmpty());
	}

	@Test
	void explicitTypedNullCallbackReceivesUnknownTypeAndCanChooseNativeIntervalNull() throws SQLException {
		Probe probe = new Probe(DatabaseType.ORACLE); probe.failure = parserFailure("getParameterType");
		CustomParameterBinder callback = new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(SqlInterval.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) {
				return BindingResult.fallback();
			}
			@Override public BindingResult bindNull(StatementContext<?> context, PreparedStatement statement, Integer index,
					TargetType target, Integer sqlType) throws SQLException {
				assertEquals(Types.NULL, sqlType);
				statement.setNull(index, -104);
				return BindingResult.handled();
			}
		};
		TypedParameter intervalNull = new TypedParameter() {
			@Override public Type getExplicitType() { return SqlInterval.class; }
			@Override public Optional<Object> getValue() { return Optional.empty(); }
		};
		new DefaultPreparedStatementBinder(List.of(callback)).bindParameter(probe.context, probe.statement, 1, intervalNull, probe.resolver());
		assertEquals(List.of("1:-104"), probe.nulls);
		assertEquals(1, probe.descriptions);
	}

	@Test
	void parserFallbackCannotSilentlyBindTemporalValuesEvenAfterANullCachedItsAbsence() throws SQLException {
		Instant instant = Instant.parse("2026-01-01T05:00:00Z");
		List<Object> values = List.of(instant, instant.atOffset(ZoneOffset.ofHours(5)),
				instant.atZone(ZoneOffset.ofHours(5)), java.sql.Timestamp.from(instant), java.util.Date.from(instant),
				LocalDate.of(2026, 1, 1), LocalDateTime.of(2026, 1, 1, 10, 0), java.sql.Date.valueOf("2026-01-01"));
		for (AmbiguousTimestampBindingStrategy strategy : AmbiguousTimestampBindingStrategy.values()) {
			for (Object value : values) {
				for (boolean nullFirst : new boolean[]{false, true}) {
					Probe probe = new Probe(DatabaseType.ORACLE, strategy); probe.failure = parserFailure("getParameterType");
					var resolver = probe.resolver(); var binder = new DefaultPreparedStatementBinder();
					if (nullFirst) binder.bindParameter(probe.context, probe.statement, 1, Optional.empty(), resolver);
					DatabaseException refused = assertThrows(DatabaseException.class,
							() -> binder.bindParameter(probe.context, probe.statement, nullFirst ? 2 : 1, value, resolver));
					assertTrue(refused.getMessage().contains("Cannot determine the Oracle temporal target"));
					assertSame(probe.failure, refused.getCause());
					assertTrue(probe.bindings.isEmpty(), "Never dispatch a temporal setter using lost target evidence");
					assertEquals(1, probe.descriptions);
					// Refusal of a value must not turn the documented NULL fallback into a terminal metadata error.
					binder.bindParameter(probe.context, probe.statement, 3, Optional.empty(), resolver);
					assertEquals((nullFirst ? 2 : 1), probe.nulls.size());
				}
			}
		}
	}

	@Test
	void declaredPositionsCachedAfterParserFailureCannotBecomeSuccessfulTargetEvidence() throws SQLException {
		for (String phase : List.of("type", "name")) {
			Probe probe = new Probe(DatabaseType.ORACLE); probe.failurePhase = phase; probe.sqlType = Types.TIMESTAMP;
			probe.failure = parserFailure(phase.equals("type") ? "getParameterType" : "getParameterTypeName");
			var resolver = probe.resolver(); resolver.registerParameterIndices(List.of(1, 2, 3));
			resolver.determineNullParameterSqlType(1);
			var binder = new DefaultPreparedStatementBinder();
			for (int index : new int[]{2, 3}) {
				DatabaseException failure = assertThrows(DatabaseException.class,
						() -> binder.bindParameter(probe.context, probe.statement, index, Instant.EPOCH, resolver));
				assertSame(probe.failure, failure.getCause());
				binder.bindParameter(probe.context, probe.statement, index, Optional.empty(), resolver);
			}
			assertTrue(probe.bindings.isEmpty());
			assertEquals(1, probe.descriptions);
		}
	}

	@Test
	void positiveTimestampCodesSurviveAParserNameFailureWithoutLosingInstantSemantics() throws SQLException {
		OffsetDateTime value = OffsetDateTime.parse("2026-01-01T10:00:00+05:00");
		for (int sqlType : new int[]{Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE, -101, -102}) {
			Probe probe = new Probe(DatabaseType.ORACLE); probe.failurePhase = "name";
			probe.failure = parserFailure("getParameterTypeName"); probe.sqlType = sqlType;
			var resolver = probe.resolver(); var binder = new DefaultPreparedStatementBinder();
			binder.bindParameter(probe.context, probe.statement, 1, value, resolver);
			assertEquals(1, probe.bindings.size());
			if (sqlType == Types.TIMESTAMP) {
				assertEquals(LocalDateTime.of(2026, 1, 1, 5, 0), probe.bindings.get(0).get(1));
				assertEquals(Types.TIMESTAMP, probe.bindings.get(0).get(2));
			} else {
				assertEquals(value, probe.bindings.get(0).get(1));
				assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, probe.bindings.get(0).get(2));
			}
			assertThrows(DatabaseException.class, () -> binder.bindParameter(probe.context, probe.statement, 2, value, resolver));
			assertEquals(1, probe.bindings.size(), "A code obtained for one index says nothing about the next");
		}
	}

	@Test
	void dateCodeOnlyEvidenceIsSufficientForLocalValuesButCannotGuessAnInstantTarget() throws SQLException {
		for (Object value : List.of(LocalDate.of(2026, 1, 1), LocalDateTime.of(2026, 1, 1, 5, 0))) {
			Probe probe = new Probe(DatabaseType.ORACLE); probe.failurePhase = "name"; probe.sqlType = Types.DATE;
			probe.failure = parserFailure("getParameterTypeName");
			new DefaultPreparedStatementBinder().bindParameter(probe.context, probe.statement, 1, value, probe.resolver());
			assertEquals(value, probe.bindings.get(0).get(1));
		}
		Probe probe = new Probe(DatabaseType.ORACLE); probe.failurePhase = "name"; probe.sqlType = Types.DATE;
		probe.failure = parserFailure("getParameterTypeName");
		assertThrows(DatabaseException.class, () -> new DefaultPreparedStatementBinder()
				.bindParameter(probe.context, probe.statement, 1, Instant.EPOCH, probe.resolver()));
		assertTrue(probe.bindings.isEmpty());
	}

	@Test
	void anEarlierSuccessfulAmbiguousIndexRetainsItsStrategyWhenAnotherIndexLosesTheDescriptor() throws SQLException {
		Probe probe = new Probe(DatabaseType.ORACLE, AmbiguousTimestampBindingStrategy.TIMESTAMP_WITHOUT_TIME_ZONE);
		probe.sqlType = Types.VARCHAR; probe.failureIndex = 2; probe.failure = parserFailure("getParameterType");
		var resolver = probe.resolver(); var binder = new DefaultPreparedStatementBinder();
		resolver.determineParameterSqlType(1);
		binder.bindParameter(probe.context, probe.statement, 2, Optional.empty(), resolver);
		binder.bindParameter(probe.context, probe.statement, 1, OffsetDateTime.parse("2026-01-01T10:00:00+05:00"), resolver);
		assertEquals(LocalDateTime.of(2026, 1, 1, 5, 0), probe.bindings.get(0).get(1));
		assertEquals(Types.TIMESTAMP, probe.bindings.get(0).get(2));
	}

	@Test
	void customTemporalBindersRetainPrecedenceOverUnavailableMetadata() throws SQLException {
		Probe probe = new Probe(DatabaseType.ORACLE); probe.failure = parserFailure("getParameterType");
		var resolver = probe.resolver(); resolver.determineNullParameterSqlType(1);
		ZonedDateTime original = Instant.EPOCH.atZone(ZoneOffset.ofHours(5));
		CustomParameterBinder callback = new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(ZonedDateTime.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws SQLException {
				assertSame(original, value);
				statement.setObject(index, ((ZonedDateTime) value).toOffsetDateTime(), Types.TIMESTAMP_WITH_TIMEZONE);
				return BindingResult.handled();
			}
		};
		new DefaultPreparedStatementBinder(List.of(callback)).bindParameter(probe.context, probe.statement, 2, original, resolver);
		assertEquals(original.toOffsetDateTime(), probe.bindings.get(0).get(1));
		assertEquals(1, probe.descriptions);
	}

	@Test
	void matchingOracleStackIsNotAFallbackForOtherDialectsOrTheContextFreeResolver() {
		for (DatabaseType type : List.of(DatabaseType.GENERIC, DatabaseType.POSTGRESQL, DatabaseType.DUCK_DB)) {
			Probe probe = new Probe(type); probe.failure = parserFailure("getParameterType");
			assertRetainedFailure(probe, probe.resolver());
		}
		Probe probe = new Probe(DatabaseType.ORACLE); probe.failure = parserFailure("getParameterType");
		assertRetainedFailure(probe, new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(probe.statement));
	}

	@Test
	void applicationProxyAcquisitionAndWrongParserFailuresRemainTerminalByIdentity() {
		for (String phase : List.of("metadata", "type", "name")) {
			List<Throwable> failures = new ArrayList<>();
			failures.add(new NullPointerException("application or proxy failure"));
			IllegalStateException runtime = new IllegalStateException("unrelated runtime");
			runtime.setStackTrace(parserFailure("getParameterType").getStackTrace()); failures.add(runtime);
			NullPointerException subclass = new NullPointerException("subclass") {};
			subclass.setStackTrace(parserFailure(phase.equals("name") ? "getParameterTypeName" : "getParameterType").getStackTrace());
			failures.add(subclass);
			NullPointerException wrongFrame = parserFailure(phase.equals("name") ? "getParameterTypeName" : "getParameterType");
			StackTraceElement[] frames = wrongFrame.getStackTrace();
			frames[0] = new StackTraceElement("application.Proxy", "storeParametersMetadata", "Proxy.java", 1);
			wrongFrame.setStackTrace(frames); failures.add(wrongFrame);
			failures.add(parserFailure("getParameterCount"));
			failures.add(parserFailure(phase.equals("name") ? "getParameterType" : "getParameterTypeName"));
			if (phase.equals("metadata")) failures.add(parserFailure("getParameterType"));
			for (Throwable failure : failures) {
				Probe probe = new Probe(DatabaseType.ORACLE); probe.failurePhase = phase; probe.failure = failure;
				assertRetainedFailure(probe, probe.resolver());
			}
		}
	}

	@Test
	void knownParserShapeCannotHideCausesSuppressedFailuresOrPendingInterruption() {
		for (Throwable evidence : List.of(new InterruptedException("interrupted"), new SQLException("cancelled", "57014"),
				new AssertionError("fatal"), new IllegalStateException("unrelated"))) {
			for (boolean suppressed : new boolean[]{false, true}) {
				Probe probe = new Probe(DatabaseType.ORACLE);
				NullPointerException failure = parserFailure("getParameterType");
				if (suppressed) failure.addSuppressed(evidence); else failure.initCause(evidence);
				probe.failure = failure; assertRetainedFailure(probe, probe.resolver());
			}
		}
		Probe interrupted = new Probe(DatabaseType.ORACLE); interrupted.failure = parserFailure("getParameterType");
		Thread.currentThread().interrupt();
		assertRetainedFailure(interrupted, interrupted.resolver());
		assertTrue(Thread.currentThread().isInterrupted());
	}

	private static void assertRetainedFailure(Probe probe, DefaultPreparedStatementBinder.ParameterSqlTypeResolver resolver) {
		assertSame(probe.failure, assertThrows(Throwable.class, () -> resolver.determineParameterSqlType(1)));
		assertSame(probe.failure, assertThrows(Throwable.class, () -> resolver.determineParameterSqlType(2)));
		assertEquals(1, probe.descriptions, "A terminal failure remains cached by identity");
	}

	private static NullPointerException parserFailure(String getter) {
		NullPointerException failure = new NullPointerException("Message and source line are deliberately not matched");
		String[] methods = {"storeParametersMetadata", "query", "parseAndQuery", "checkValidIndex", getter};
		StackTraceElement[] frames = new StackTraceElement[methods.length];
		for (int index = 0; index < frames.length; index++)
			frames[index] = new StackTraceElement("oracle.jdbc.driver.OracleParameterMetaData", methods[index], "OracleParameterMetaData.java", 1);
		failure.setStackTrace(frames); return failure;
	}

	private static final class Probe {
		final StatementContext<?> context;
		final PreparedStatement statement;
		final List<Integer> typeReads = new ArrayList<>(), nameReads = new ArrayList<>();
		final List<String> nulls = new ArrayList<>();
		final List<List<Object>> bindings = new ArrayList<>();
		int descriptions, sqlType = Types.INTEGER;
		int failureIndex = -1;
		String failurePhase = "type";
		Throwable failure;
		Probe(DatabaseType type) {
			this(type, null);
		}
		Probe(DatabaseType type, AmbiguousTimestampBindingStrategy strategy) {
			DataSource unused = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> { throw new AssertionError(method); });
			context = StatementContext.with(Statement.of("oracle-metadata", "INSERT INTO work SELECT :value FROM dual"),
					Database.with(unused, type).timeZone(ZoneOffset.UTC).ambiguousTimestampBindingStrategy(strategy).build()).build();
			Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> { if (method.getName().equals("getAutoCommit")) return true; throw new AssertionError(method); });
			ParameterMetaData metadata = (ParameterMetaData) Proxy.newProxyInstance(ParameterMetaData.class.getClassLoader(), new Class<?>[]{ParameterMetaData.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getParameterType" -> { typeReads.add((Integer) args[0]); if (fails("type", (Integer) args[0])) throw failure; yield sqlType; }
						case "getParameterTypeName" -> { nameReads.add((Integer) args[0]); if (fails("name", (Integer) args[0])) throw failure; yield "INTEGER"; }
						default -> throw new AssertionError(method);
					});
			statement = (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getParameterMetaData" -> { descriptions++; if (failurePhase.equals("metadata")) throw failure; yield metadata; }
						case "getConnection" -> connection;
						case "setNull" -> { nulls.add(args[0] + ":" + args[1]); yield null; }
						case "setObject", "setTimestamp", "setDate" -> { bindings.add(List.of(args)); yield null; }
						default -> throw new AssertionError(method);
					});
		}
		DefaultPreparedStatementBinder.ParameterSqlTypeResolver resolver() {
			return new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(context, statement);
		}
		private boolean fails(String phase, int index) {
			return failurePhase.equals(phase) && (failureIndex < 0 || failureIndex == index);
		}
	}
}
