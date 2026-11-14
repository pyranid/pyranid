/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class OracleUuidBindingTests {
	private static final UUID UUID_VALUE = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");

	@Test
	void characterMetadataSelectsCanonicalTextAndOtherTypesRetainRawBytes() throws SQLException {
		for (int sqlType : List.of(Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR,
				Types.NVARCHAR, Types.LONGNVARCHAR, Types.CLOB, Types.NCLOB)) {
			Probe probe = new Probe(sqlType);
			probe.bind(DatabaseType.ORACLE, UUID_VALUE);
			assertEquals(UUID_VALUE.toString(), probe.values.get(0), "JDBC type " + sqlType);
			assertEquals(1, probe.descriptions);
		}
		for (int sqlType : List.of(Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB,
				Types.NULL, Types.OTHER, Types.INTEGER)) {
			Probe probe = new Probe(sqlType);
			probe.bind(DatabaseType.ORACLE, UUID_VALUE);
			assertArrayEquals(bytes(UUID_VALUE), assertInstanceOf(byte[].class, probe.values.get(0)), "JDBC type " + sqlType);
		}
	}

	@Test
	void unavailableMetadataKeepsRawCompatibilityAndKnownTypeSurvivesUnavailableName() throws SQLException {
		for (Throwable unavailable : List.of(new SQLFeatureNotSupportedException("no metadata"), new AbstractMethodError("old driver"))) {
			Probe probe = new Probe(Types.VARCHAR);
			probe.metadataFailure = unavailable;
			probe.bind(DatabaseType.ORACLE, UUID_VALUE);
			assertArrayEquals(bytes(UUID_VALUE), assertInstanceOf(byte[].class, probe.values.get(0)));
		}
		Probe noMetadata = new Probe(Types.VARCHAR);
		noMetadata.noMetadata = true;
		noMetadata.bind(DatabaseType.ORACLE, UUID_VALUE);
		assertArrayEquals(bytes(UUID_VALUE), assertInstanceOf(byte[].class, noMetadata.values.get(0)));

		Probe unavailableName = new Probe(Types.VARCHAR);
		unavailableName.nameFailure = new SQLFeatureNotSupportedException("no type name");
		unavailableName.bind(DatabaseType.ORACLE, UUID_VALUE);
		assertEquals(UUID_VALUE.toString(), unavailableName.values.get(0));

		Probe explicitText = new Probe(Types.OTHER);
		explicitText.metadataFailure = new AssertionError("explicit UUID text needs no target inference");
		explicitText.bind(DatabaseType.ORACLE, UUID_VALUE.toString());
		assertEquals(UUID_VALUE.toString(), explicitText.values.get(0));
		assertEquals(0, explicitText.descriptions);
	}

	@Test
	void terminalMetadataFailuresPropagateEvenAfterTheTextTypeWasRead() {
		for (String sqlState : List.of("08006", "40001", "57014")) {
			for (boolean failName : List.of(false, true)) {
				Probe probe = new Probe(Types.VARCHAR);
				SQLException original = new SQLException("terminal metadata failure", sqlState);
				if (failName) probe.nameFailure = original;
				else probe.metadataFailure = original;
				assertSame(original, assertThrows(SQLException.class, () -> probe.bind(DatabaseType.ORACLE, UUID_VALUE)));
				assertTrue(probe.values.isEmpty());
			}
		}
	}

	@Test
	void metadataAndEachPositionAreResolvedOnceWhenTheSameUuidTargetsTextAndRaw() throws SQLException {
		Probe probe = new Probe(Types.VARCHAR, Types.VARBINARY, Types.NCHAR);
		DefaultPreparedStatementBinder binder = new DefaultPreparedStatementBinder();
		StatementContext<?> context = context(DatabaseType.ORACLE, binder);
		var resolver = new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(context, probe.statement);
		for (int row = 0; row < 3; row++) {
			for (int index = 1; index <= 3; index++)
				binder.bindParameter(context, probe.statement, index, UUID_VALUE, resolver);
		}
		assertEquals(1, probe.descriptions);
		assertEquals(3, probe.typeReads);
		assertEquals(3, probe.nameReads);
		for (int row = 0; row < 3; row++) {
			assertEquals(UUID_VALUE.toString(), probe.values.get(row * 3));
			assertArrayEquals(bytes(UUID_VALUE), assertInstanceOf(byte[].class, probe.values.get(row * 3 + 1)));
			assertEquals(UUID_VALUE.toString(), probe.values.get(row * 3 + 2));
		}
	}

	@Test
	void customBindersReceiveTheOriginalUuidAndCanChooseTextWithoutMetadata() throws SQLException {
		List<Object> received = new ArrayList<>();
		DefaultPreparedStatementBinder binder = new DefaultPreparedStatementBinder(List.of(new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(UUID.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws SQLException {
				received.add(value);
				statement.setString(index, value.toString());
				return BindingResult.handled();
			}
		}));
		Probe probe = new Probe(Types.VARBINARY);
		probe.metadataFailure = new AssertionError("a handled custom binder needs no metadata");
		binder.bindParameter(context(DatabaseType.ORACLE, binder), probe.statement, 1, Parameters.secure(Optional.of(UUID_VALUE)));
		assertSame(UUID_VALUE, received.get(0));
		assertEquals(UUID_VALUE.toString(), probe.values.get(0));
		assertEquals(0, probe.descriptions);
	}

	@Test
	void declinedCustomBindersStillUseTheOracleColumnType() throws SQLException {
		List<Object> received = new ArrayList<>();
		DefaultPreparedStatementBinder binder = new DefaultPreparedStatementBinder(List.of(new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(UUID.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) {
				received.add(value); return BindingResult.fallback();
			}
		}));
		Probe probe = new Probe(Types.VARCHAR);
		binder.bindParameter(context(DatabaseType.ORACLE, binder), probe.statement, 1, UUID_VALUE);
		assertSame(UUID_VALUE, received.get(0));
		assertEquals(UUID_VALUE.toString(), probe.values.get(0));
	}

	@Test
	void otherDialectsDoNotAddUuidMetadataProbesAndNullsKeepTheirMetadataType() throws SQLException {
		for (DatabaseType type : DatabaseType.values()) {
			if (type == DatabaseType.ORACLE) continue;
			Probe probe = new Probe(Types.VARCHAR);
			probe.metadataFailure = new AssertionError("UUID metadata is only needed for Oracle: " + type);
			probe.bind(type, UUID_VALUE);
			assertEquals(0, probe.descriptions, type.toString());
		}
		for (int sqlType : List.of(Types.VARCHAR, Types.VARBINARY)) {
			Probe probe = new Probe(sqlType);
			probe.bind(DatabaseType.ORACLE, Optional.empty());
			assertEquals(sqlType, probe.nullSqlType);
			assertTrue(probe.values.isEmpty());
		}
	}

	@Test
	void metadataDrivenBindingWorksForPredicatesInListsBatchesAndNulls() {
		DefaultPreparedStatementBinder binder = new DefaultPreparedStatementBinder();
		Database database = database(DatabaseType.ORACLE, binder);
		database.query("CREATE TABLE work (id INT PRIMARY KEY, text_id VARCHAR(36), raw_id BINARY(16))").execute();
		UUID second = UUID.fromString("11223344-5566-7788-99aa-bbccddeeff00");
		assertEquals(3, database.query("INSERT INTO work VALUES (:id, :uuid, :uuid)").batchChunkSize(1).executeBatch(List.of(
				Map.of("id", 1, "uuid", UUID_VALUE), Map.of("id", 2, "uuid", second), Map.of("id", 3, "uuid", Optional.empty()))).size());
		assertEquals(List.of(UUID_VALUE.toString(), second.toString()), database.query("SELECT text_id FROM work WHERE text_id IS NOT NULL ORDER BY id")
				.fetchList(String.class));
		assertEquals(UUID_VALUE, database.query("SELECT raw_id FROM work WHERE text_id = :uuid AND raw_id = :uuid")
				.bind("uuid", UUID_VALUE).fetchObject(UUID.class).orElseThrow());
		assertEquals(List.of(1, 2), database.query("SELECT id FROM work WHERE text_id IN (:ids) AND raw_id IN (:ids) ORDER BY id")
				.bind("ids", Parameters.inList(List.of(UUID_VALUE, second))).fetchList(Integer.class));
		assertEquals(1L, database.query("UPDATE work SET text_id = :new, raw_id = :new WHERE text_id = :old AND raw_id = :old")
				.bind("new", second).bind("old", UUID_VALUE).execute());
		assertEquals(1L, database.query("SELECT COUNT(*) FROM work WHERE text_id IS NULL AND raw_id IS NULL").fetchObject(Long.class).orElseThrow());
	}

	private static StatementContext<?> context(DatabaseType type, PreparedStatementBinder binder) {
		return StatementContext.with(Statement.of("uuid-binding", "SELECT 1"), database(type, binder)).build();
	}

	private static Database database(DatabaseType type, PreparedStatementBinder binder) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:oracle_uuid_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		return Database.with(source, type).preparedStatementBinder(binder).build();
	}

	private static byte[] bytes(UUID uuid) {
		return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array();
	}

	private static final class Probe {
		private final List<Object> values = new ArrayList<>();
		private int descriptions, typeReads, nameReads;
		private Integer nullSqlType;
		private Throwable metadataFailure, nameFailure;
		private boolean noMetadata;
		private final PreparedStatement statement;

		private Probe(int... sqlTypes) {
			ParameterMetaData metadata = proxy(ParameterMetaData.class, (method, args) -> {
				if (method.equals("getParameterType")) { typeReads++; return sqlTypes[(Integer) args[0] - 1]; }
				if (method.equals("getParameterTypeName")) {
					nameReads++;
					if (nameFailure != null) throw nameFailure;
					return "ignored";
				}
				throw new AssertionError(method);
			});
			this.statement = proxy(PreparedStatement.class, (method, args) -> {
				if (method.equals("getParameterMetaData")) {
					descriptions++;
					if (metadataFailure != null) throw metadataFailure;
					return noMetadata ? null : metadata;
				}
				if (method.equals("setObject") || method.equals("setString")) { values.add(args[1]); return null; }
				if (method.equals("setNull")) { nullSqlType = (Integer) args[1]; return null; }
				throw new AssertionError(method);
			});
		}

		private void bind(DatabaseType type, Object value) throws SQLException {
			DefaultPreparedStatementBinder binder = new DefaultPreparedStatementBinder();
			binder.bindParameter(context(type, binder), this.statement, 1, value);
		}
	}

	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> call.invoke(method.getName(), args)));
	}
	@FunctionalInterface private interface Call { Object invoke(String method, Object[] args) throws Throwable; }
}
