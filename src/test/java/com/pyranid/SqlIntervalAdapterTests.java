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
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SqlIntervalAdapterTests {
	@AfterEach void clearInterruption() { Thread.interrupted(); }

	@Test
	void postgresAndDuckReadersUseTextWithoutExtractingDriverObjects() throws SQLException {
		for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.DUCK_DB)) {
			Probe probe = new Probe(Types.OTHER, "interval");
			probe.text = type == DatabaseType.POSTGRESQL ? "1 mon -2 days +03:04:05.000006" : "1 month -2 days 03:04:05.000006";
			assertEquals(SqlInterval.of(1, -2, Duration.ofHours(3).plusMinutes(4).plusSeconds(5).plusNanos(6000)),
					probe.read(type).orElseThrow());
			assertEquals(List.of("getString", "wasNull"), probe.calls);
		}
	}

	@Test
	void postgresBinaryReaderRetainsBothSignedMicrosecondBoundaries() throws SQLException {
		for (long micros : new long[]{Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE}) {
			Probe probe = new Probe(Types.OTHER, "interval");
			probe.bytes = binary(0, 0, micros);
			assertEquals(SqlInterval.of(0, 0, SqlIntervalText.fromMicroseconds(micros)),
					probe.read(DatabaseType.POSTGRESQL).orElseThrow());
			assertEquals(List.of("getString", "wasNull", "getBytes", "wasNull"), probe.calls);
		}
	}

	@Test
	void postgresRejectsMalformedOrMissingBinaryDataAndBothInfinityEncodings() {
		for (byte[] bytes : new byte[][]{null, new byte[0], new byte[15], new byte[17],
				binary(Integer.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE),
				binary(Integer.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE)}) {
			Probe probe = new Probe(Types.OTHER, "interval");
			probe.bytes = bytes;
			assertThrows(DatabaseException.class, () -> probe.read(DatabaseType.POSTGRESQL));
		}
		Probe inconsistent = new Probe(Types.OTHER, "interval");
		inconsistent.bytes = binary(1, 2, 3);
		inconsistent.bytesNull = true;
		assertThrows(DatabaseException.class, () -> inconsistent.read(DatabaseType.POSTGRESQL));
	}

	@Test
	void intervalTypeEvidenceIsRequiredEvenForSqlNullBeforeAnyValueGetter() {
		for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.DUCK_DB, DatabaseType.GENERIC, DatabaseType.ORACLE)) {
			for (String name : List.of("VARCHAR", "INTEGER", "interval[]", "STRUCT(i INTERVAL)", "MAP(INTEGER, INTERVAL)", "my_interval")) {
				Probe probe = new Probe(Types.VARCHAR, name);
				probe.textNull = true;
				assertThrows(DatabaseException.class, () -> probe.read(type), type + ":" + name);
				assertTrue(probe.calls.isEmpty());
			}
		}
		Probe array = new Probe(Types.ARRAY, "interval");
		assertThrows(DatabaseException.class, () -> array.read(DatabaseType.POSTGRESQL));
	}

	@Test
	void trueNullsRemainEmptyAndNonNullOrContradictoryNullEvidenceNeverDoes() throws SQLException {
		for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.DUCK_DB)) {
			Probe nil = new Probe(Types.OTHER, "interval");
			nil.textNull = true;
			assertTrue(nil.read(type).isEmpty());
			assertEquals(List.of("getString", "wasNull"), nil.calls);
			Probe contradiction = new Probe(Types.OTHER, "interval");
			contradiction.text = "00:00:00";
			contradiction.textNull = true;
			assertThrows(DatabaseException.class, () -> contradiction.read(type));
		}
		Probe duck = new Probe(Types.OTHER, "interval");
		assertThrows(DatabaseException.class, () -> duck.read(DatabaseType.DUCK_DB));
		for (String name : List.of("INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND")) {
			Probe nil = new Probe(Types.VARCHAR, name);
			nil.nativeNull = true;
			assertTrue(nil.read(DatabaseType.GENERIC).isEmpty());
			Probe contradiction = new Probe(Types.VARCHAR, name);
			assertThrows(DatabaseException.class, () -> contradiction.read(DatabaseType.GENERIC));
		}
	}

	@Test
	void genericUsesRecognizedFamilyRatherThanTheDriversVarcharJdbcCode() throws SQLException {
		for (String name : List.of("INTERVAL YEAR TO MONTH", " interval\tYEAR ( 9 ) TO MONTH ", "INTERVAL MONTH(9)", "INTERVAL YEAR(9)")) {
			Probe probe = new Probe(Types.VARCHAR, name);
			probe.nativeValue = Period.of(1, 2, 0);
			assertEquals(SqlInterval.of(Period.ofMonths(14)), probe.read(DatabaseType.GENERIC).orElseThrow());
			assertSame(Period.class, probe.typedClass);
			assertEquals(List.of("getObject(typed)", "wasNull"), probe.calls);
		}
		for (String name : List.of("INTERVAL DAY(9) TO SECOND(9)", "INTERVAL HOUR TO SECOND(9)",
				"INTERVAL MINUTE(9) TO SECOND(9)", "INTERVAL SECOND(12,9)", "INTERVAL DAY TO HOUR", "INTERVAL DAY TO MINUTE")) {
			Probe probe = new Probe(Types.VARCHAR, name);
			Duration time = Duration.ofDays(-20).plusNanos(123456789);
			probe.nativeValue = time;
			assertEquals(SqlInterval.of(time), probe.read(DatabaseType.GENERIC).orElseThrow());
			assertSame(Duration.class, probe.typedClass);
		}
		Probe unknown = new Probe(Types.OTHER, "INTERVAL");
		assertThrows(DatabaseException.class, () -> unknown.read(DatabaseType.GENERIC));
		assertTrue(unknown.calls.isEmpty(), "Do not try unrelated getters until one succeeds");
	}

	@Test
	void oracleCodesAndNamesMustNotIdentifyConflictingFamilies() throws SQLException {
		Probe months = new Probe(-103, null);
		months.nativeValue = Period.ofMonths(-14);
		assertEquals(SqlInterval.of(Period.ofMonths(-14)), months.read(DatabaseType.ORACLE).orElseThrow());
		Probe time = new Probe(-104, "INTERVALDS");
		time.nativeValue = Duration.ofNanos(123456789);
		assertEquals(SqlInterval.of((Duration) time.nativeValue), time.read(DatabaseType.ORACLE).orElseThrow());
		for (String name : List.of("INTERVALDS", "VARCHAR", "INTERVAL DAY TO SECOND")) {
			Probe conflict = new Probe(-103, name);
			assertThrows(DatabaseException.class, () -> conflict.read(DatabaseType.ORACLE));
			assertTrue(conflict.calls.isEmpty());
		}
	}

	@Test
	void knownNonIntervalCodesCannotBeOverriddenByIntervalNamesEvenForSqlNull() {
		for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.DUCK_DB, DatabaseType.GENERIC, DatabaseType.ORACLE)) {
			String name = type == DatabaseType.GENERIC ? "INTERVAL YEAR TO MONTH" : type == DatabaseType.ORACLE ? "INTERVALYM" : "interval";
			for (int code : new int[]{Types.INTEGER, Types.NUMERIC, Types.BOOLEAN, Types.DATE, Types.TIME, Types.TIMESTAMP,
					Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY,
					Types.BLOB, Types.CLOB, Types.NCLOB, Types.REF, Types.REF_CURSOR, Types.SQLXML, Types.ROWID, Types.DATALINK,
					Types.ARRAY, Types.STRUCT}) {
				Probe probe = new Probe(code, name);
				probe.textNull = true; probe.nativeNull = true;
				assertThrows(DatabaseException.class, () -> probe.read(type), type + ":" + code);
				assertTrue(probe.calls.isEmpty(), "Conflicting metadata must reject before any getter");
			}
		}
		for (int code : new int[]{-100, -101, -102}) {
			Probe probe = new Probe(code, "INTERVALDS");
			assertThrows(DatabaseException.class, () -> probe.read(DatabaseType.ORACLE));
			assertTrue(probe.calls.isEmpty());
		}
	}

	@Test
	void opaqueTextDomainAndUnknownVendorCodesRetainPositiveIntervalNames() throws SQLException {
		for (int code : new int[]{Types.OTHER, Types.JAVA_OBJECT, Types.DISTINCT, Types.NULL, Types.CHAR, Types.VARCHAR,
				Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR, 1234567}) {
			Probe probe = new Probe(code, "INTERVAL YEAR TO MONTH");
			probe.nativeValue = Period.ofMonths(14);
			assertEquals(SqlInterval.of(Period.ofMonths(14)), probe.read(DatabaseType.GENERIC).orElseThrow());
			assertSame(Period.class, probe.typedClass);
		}
	}

	@Test
	void conflictingAvailableMetadataCannotChooseZeroFamilyBeforeAnySetter() {
		List<String> setters = new ArrayList<>();
		PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
				new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> { setters.add(method.getName()); return null; });
		for (DatabaseType type : List.of(DatabaseType.GENERIC, DatabaseType.ORACLE)) {
			for (int code : new int[]{Types.INTEGER, Types.TIMESTAMP, Types.BLOB, -100, -101, -102}) {
				if (type != DatabaseType.ORACLE && code < -99) continue;
				for (String name : List.of("INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND")) {
					assertThrows(DatabaseException.class, () -> SqlIntervalSupport.bind(statement, 1, context(type), SqlInterval.ZERO, code, name));
					assertTrue(setters.isEmpty(), "Reject conflicting existing metadata before any setter or connection lookup");
				}
			}
		}
	}

	@Test
	void invalidNativeRepresentationsAndMonthOverflowAreNotTruncated() {
		for (Object value : List.of(Period.ofYears(Integer.MAX_VALUE), Period.ofDays(1), "P1M", Duration.ZERO)) {
			Probe probe = new Probe(Types.VARCHAR, "INTERVAL YEAR TO MONTH");
			probe.nativeValue = value;
			assertThrows(DatabaseException.class, () -> probe.read(DatabaseType.GENERIC));
		}
		Probe time = new Probe(-104, "INTERVALDS");
		time.nativeValue = Period.ZERO;
		assertThrows(DatabaseException.class, () -> time.read(DatabaseType.ORACLE));
		Probe infinity = new Probe(Types.OTHER, "interval");
		infinity.text = "infinity";
		DatabaseException failure = assertThrows(DatabaseException.class, () -> infinity.read(DatabaseType.POSTGRESQL));
		assertInstanceOf(DateTimeException.class, failure.getCause());
	}

	@Test
	void jdbcAndFatalFailuresPropagateByIdentityWithoutGetterFallback() {
		for (String phase : List.of("metadata", "getString", "getBytes", "getObject(typed)", "wasNull")) {
			for (Throwable original : List.of(new SQLException("cancelled", "57014"), new AssertionError("fatal"))) {
				Probe probe = new Probe(phase.equals("getObject(typed)") ? -104 : Types.OTHER,
						phase.equals("getObject(typed)") ? "INTERVALDS" : "interval");
				probe.failurePhase = phase;
				probe.failure = original;
				assertSame(original, assertThrows(Throwable.class,
						() -> probe.read(phase.equals("getObject(typed)") ? DatabaseType.ORACLE : DatabaseType.POSTGRESQL)));
			}
		}
		Probe interrupted = new Probe(Types.OTHER, "interval");
		interrupted.textNull = true;
		Thread.currentThread().interrupt();
		assertDoesNotThrow(() -> interrupted.read(DatabaseType.POSTGRESQL));
		assertTrue(Thread.currentThread().isInterrupted());
	}

	private static byte[] binary(int months, int days, long micros) {
		return ByteBuffer.allocate(16).putLong(micros).putInt(days).putInt(months).array();
	}

	static StatementContext<?> context(DatabaseType type) {
		DataSource unused = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> { throw new AssertionError(method); });
		return StatementContext.with(Statement.of("interval-adapter", "SELECT value"), Database.with(unused, type).build()).build();
	}

	private static final class Probe {
		final int jdbcType;
		final String name;
		final List<String> calls = new ArrayList<>();
		String text;
		byte[] bytes;
		Object nativeValue;
		boolean textNull, bytesNull, nativeNull, lastNull;
		Class<?> typedClass;
		String failurePhase;
		Throwable failure;
		Probe(int jdbcType, String name) { this.jdbcType = jdbcType; this.name = name; }
		Optional<SqlInterval> read(DatabaseType type) throws SQLException {
			ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
					new Class<?>[]{ResultSetMetaData.class}, (proxy, method, args) -> {
						if ("metadata".equals(failurePhase)) throw failure;
						return switch (method.getName()) {
							case "getColumnType" -> jdbcType;
							case "getColumnTypeName" -> name;
							default -> throw new AssertionError(method);
						};
					});
			ResultSet rows = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
					(proxy, method, args) -> {
						String call = method.getName();
						if (call.equals("getObject")) {
							assertEquals(2, args.length, "Never extract a raw PGInterval first");
							call = "getObject(typed)";
							typedClass = (Class<?>) args[1];
						}
						calls.add(call);
						if (call.equals(failurePhase)) throw failure;
						return switch (call) {
							case "getString" -> { lastNull = textNull; yield text; }
							case "getBytes" -> { lastNull = bytesNull; yield bytes; }
							case "getObject(typed)" -> { lastNull = nativeNull; yield nativeValue; }
							case "wasNull" -> lastNull;
							default -> throw new AssertionError(method);
						};
					});
			return SqlIntervalSupport.read(rows, 1, context(type), metadata);
		}
	}
}
