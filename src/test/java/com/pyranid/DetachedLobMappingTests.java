/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DetachedLobMappingTests {
	public record ObjectRow(Object value) {}
	public record NativeMap(Map<String, Object> value) {}
	public record NativeList(List<Object> value) {}
	public record NativeArray(Object[] value) {}
	public record ExplicitClob(Clob value) {}
	public record ExplicitXml(SQLXML value) {}
	public record CustomValue(Object raw) {}
	public record NativeAndCustom(@DatabaseColumn("value") Object nativeValue, @DatabaseColumn("value") CustomValue custom) {}
	public record CustomAndNative(@DatabaseColumn("value") CustomValue custom, @DatabaseColumn("value") Object nativeValue) {}

	@Test
	void optInDetachesNativeMapScalarAndObjectPropertiesWhileDefaultsKeepHandles() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (Class<?> lobType : List.of(Blob.class, Clob.class, NClob.class, SQLXML.class)) {
				for (Class<?> target : List.of(Object.class, ObjectRow.class, Map.class)) {
					Lob raw = lob(lobType, "hello");
					Object original = mappedValue(mapper(cached, false), raw.handle, raw.jdbcType, target);
					assertSame(raw.handle, original);
					raw.assertCounts(0, 0, 0);
					Lob detached = lob(lobType, "hello");
					Object result = mappedValue(mapper(cached, true), detached.handle, detached.jdbcType, target);
					assertContent(detached, result);
					detached.assertCounts(1, 1, 1);
				}
			}
		}
	}

	@Test
	void typedObjectLeavesDetachAcrossMapsCollectionsArraysAndNulls() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = mapper(cached, true);
			for (Class<?> target : List.of(NativeMap.class, NativeList.class, NativeArray.class)) {
				Lob lob = lob(Clob.class, "nested");
				Object source = target == NativeMap.class ? Map.of("lob", lob.handle) : new Object[]{lob.handle, null};
				Object row = map(mapper, source, Types.JAVA_OBJECT, target);
				Object value = row instanceof NativeMap result ? result.value().get("lob")
						: row instanceof NativeList result ? result.value().get(0) : ((NativeArray) row).value()[0];
				assertEquals("nested", value);
				lob.assertCounts(1, 1, 1);
			}
			assertNull(map(mapper, null, Types.CLOB, Object.class));
			assertNull(map(mapper, null, Types.CLOB, ObjectRow.class).value());
			assertNull(((Map<?, ?>) map(mapper, null, Types.CLOB, Map.class)).get("value"));
		}
	}

	@Test
	void limitsCoverHandlesAndJdbcIdentifiedDetachedValuesButNotOrdinaryColumns() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).detachLobValues(true)
					.maxLobBytes(2L).maxLobCharacters(2L).build();
			for (Class<?> lobType : List.of(Blob.class, Clob.class, NClob.class, SQLXML.class)) {
				Lob lob = lob(lobType, "long");
				DatabaseException failure = assertThrows(DatabaseException.class, () -> map(mapper, lob.handle, lob.jdbcType, Object.class));
				assertTrue(failure.getMessage().contains("materialization limit"));
				lob.assertCounts(1, 1, 1);
			}
			for (int jdbcType : new int[]{Types.CLOB, Types.NCLOB, Types.SQLXML})
				for (Class<?> target : List.of(Object.class, ObjectRow.class, Map.class))
					assertThrows(DatabaseException.class, () -> map(mapper, "long", jdbcType, target));
			assertThrows(DatabaseException.class, () -> map(mapper, new byte[]{1, 2, 3}, Types.BLOB, Object.class));
			assertEquals("long", map(mapper, "long", Types.VARCHAR, Object.class));
			byte[] bytes = {1, 2, 3};
			assertSame(bytes, map(mapper, bytes, Types.VARBINARY, Object.class));
			assertEquals("long", map(ResultSetMapper.withPlanCachingEnabled(cached).detachLobValues(true)
					.maxLobCharacters(1L).unboundedLobMaterialization().build(), "long", Types.CLOB, Object.class));
		}
		assertThrows(NullPointerException.class, () -> ResultSetMapper.withPlanCachingEnabled(true).detachLobValues(null));
	}

	@Test
	void copiedNativeContainersPreserveCyclesAliasesArrayClassesAndCallerData() throws Exception {
		Lob lob = lob(Clob.class, "shared");
		Object[] array = {lob.handle, lob.handle};
		Map<String, Object> source = new LinkedHashMap<>();
		source.put("first", array); source.put("second", array); source.put("self", source);
		List<Object> cycle = new ArrayList<>(); cycle.add(cycle); cycle.add(lob.handle);
		source.put("cycle", cycle);
		byte[][] primitive = {{1, 2}, {3}};
		String[][] strings = {{"a"}, {"b"}};
		source.put("primitive", primitive); source.put("strings", strings);
		Map<?, ?> result = (Map<?, ?>) map(mapper(true, true), source, Types.JAVA_OBJECT, Object.class);
		assertNotSame(source, result);
		assertSame(result, result.get("self"));
		assertSame(result.get("first"), result.get("second"));
		Object[] converted = (Object[]) result.get("first");
		assertEquals("shared", converted[0]); assertSame(converted[0], converted[1]);
		List<?> convertedCycle = (List<?>) result.get("cycle");
		assertSame(convertedCycle, convertedCycle.get(0));
		assertSame(converted[0], convertedCycle.get(1));
		assertInstanceOf(byte[][].class, result.get("primitive"));
		assertInstanceOf(String[][].class, result.get("strings"));
		assertArrayEquals(primitive[0], ((byte[][]) result.get("primitive"))[0]);
		assertSame(source, source.get("self")); assertSame(array, source.get("first"));
		assertSame(lob.handle, array[0]); assertSame(cycle, cycle.get(0));
		lob.assertCounts(1, 1, 1);
	}

	@Test
	void nativeArrayMetadataRetainsLobLimitsForAlreadyDetachedDriverElements() throws Exception {
		try (MappingResources resources = new MappingResources(2L, 2L, true)) {
			Object[] binary = {new byte[]{1, 2, 3}, null};
			resources.registerArrayType(binary, Types.BLOB);
			assertThrows(DatabaseException.class, () -> resources.detachLobs(binary, Types.OTHER, "BLOB array"));
			Object[] characters = {"long", null};
			resources.registerArrayType(characters, Types.CLOB);
			assertThrows(DatabaseException.class, () -> resources.detachLobs(characters, Types.OTHER, "CLOB array"));
			List<String> xml = List.of("<xml/>");
			resources.registerArrayType(xml, Types.SQLXML);
			assertThrows(DatabaseException.class, () -> resources.detachLobs(xml, Types.OTHER, "SQLXML array"));
			Object[] ordinary = {"long", new byte[]{1, 2, 3}};
			assertEquals("long", ((Object[]) resources.detachLobs(ordinary, Types.OTHER, "ordinary array"))[0]);
			Object[] laterIdentified = {"long"};
			resources.detachLobs(laterIdentified, Types.OTHER, "initially untyped array");
			resources.registerArrayType(laterIdentified, Types.CLOB);
			assertThrows(DatabaseException.class, () -> resources.detachLobs(laterIdentified, Types.OTHER, "later CLOB array"));
			Object[] empty = {};
			resources.registerArrayType(empty, Types.CLOB);
			assertEquals(0, ((Object[]) resources.detachLobs(empty, Types.OTHER, "empty array")).length);
		}
	}

	@Test
	void matchingTypedLobArrayValuesStillHonorKnownLobMetadataBeforeIdentityReturns() throws Exception {
		ResultSetMapper limited = ResultSetMapper.withPlanCachingEnabled(true).maxLobBytes(2L).maxLobCharacters(2L).build();
		for (int jdbcType : new int[]{Types.CLOB, Types.NCLOB}) {
			AtomicInteger freed = new AtomicInteger();
			assertThrows(DatabaseException.class, () -> map(limited, array(new String[]{"long", null}, jdbcType, freed), Types.ARRAY, String[].class));
			assertEquals(1, freed.get());
			assertArrayEquals(new String[]{"ab", null}, map(limited, array(new String[]{"ab", null}, jdbcType, new AtomicInteger()), Types.ARRAY, String[].class));
		}
		AtomicInteger freed = new AtomicInteger();
		assertThrows(DatabaseException.class, () -> map(limited, array(new byte[][]{{1, 2, 3}, null}, Types.BLOB, freed), Types.ARRAY, byte[][].class));
		assertEquals(1, freed.get());
		assertArrayEquals(new String[]{"long", null}, map(limited,
				array(new String[]{"long", null}, Types.VARCHAR, new AtomicInteger()), Types.ARRAY, String[].class));
		assertArrayEquals(new byte[]{1, 2, 3}, map(limited,
				array(new byte[][]{{1, 2, 3}, null}, Types.VARBINARY, new AtomicInteger()), Types.ARRAY, byte[][].class)[0]);
		// Typed XML retains its existing getter policy; the new native/Object opt-in is separate.
		assertArrayEquals(new String[]{"<xml/>"}, map(limited,
				array(new String[]{"<xml/>"}, Types.SQLXML, new AtomicInteger()), Types.ARRAY, String[].class));

		for (boolean handled : new boolean[]{false, true}) {
			Lob lob = lob(Clob.class, "ab");
			CustomColumnMapper custom = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(String[].class); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
						Integer column, String label, InstanceProvider provider) {
					assertSame(lob.handle, ((Object[]) raw)[0]);
					return handled ? MappingResult.of(new String[]{"manual"}) : MappingResult.fallback();
				}
			};
			ResultSetMapper customized = ResultSetMapper.withCustomColumnMappers(List.of(custom)).maxLobCharacters(2L).build();
			assertArrayEquals(new String[]{handled ? "manual" : "ab"}, map(customized,
					array(new Object[]{lob.handle}, Types.CLOB, new AtomicInteger()), Types.ARRAY, String[].class));
			lob.assertCounts(handled ? 0 : 1, handled ? 0 : 1, 0);
		}
	}

	@Test
	void customMapperInputsAndHandledOutputsRemainBorrowedAcrossFanOutOrderings() throws Exception {
		for (Class<?> target : List.of(NativeAndCustom.class, CustomAndNative.class)) {
			Lob lob = lob(Clob.class, "shared");
			CustomColumnMapper custom = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(CustomValue.class); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType type,
						Integer column, String label, InstanceProvider provider) {
					assertSame(lob.handle, raw); return MappingResult.of(new CustomValue(raw));
				}
			};
			Object row = map(ResultSetMapper.withCustomColumnMappers(List.of(custom)).detachLobValues(true).build(), lob.handle, Types.CLOB, target);
			Object nativeValue = row instanceof NativeAndCustom result ? result.nativeValue() : ((CustomAndNative) row).nativeValue();
			CustomValue handled = row instanceof NativeAndCustom result ? result.custom() : ((CustomAndNative) row).custom();
			assertEquals("shared", nativeValue); assertSame(lob.handle, handled.raw());
			lob.assertCounts(1, 1, 0);
		}
		for (boolean handled : new boolean[]{false, true}) {
			Lob lob = lob(SQLXML.class, "xml");
			Map<String, Object> opaque = Map.of("retained", lob.handle);
			CustomColumnMapper custom = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(Object.class); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType type,
						Integer column, String label, InstanceProvider provider) {
					assertSame(lob.handle, raw); return handled ? MappingResult.of(opaque) : MappingResult.fallback();
				}
			};
			Object value = map(ResultSetMapper.withCustomColumnMappers(List.of(custom)).detachLobValues(true).build(), lob.handle, Types.SQLXML, Object.class);
			if (handled) assertSame(opaque, value); else assertEquals("xml", value);
			lob.assertCounts(handled ? 0 : 1, handled ? 0 : 1, 0);
		}
	}

	@Test
	void explicitJdbcHandleTargetsRemainBorrowedEvenWithDetachmentEnabled() throws Exception {
		Lob clob = lob(Clob.class, "handle");
		assertSame(clob.handle, map(mapper(true, true), clob.handle, Types.CLOB, Clob.class));
		assertSame(clob.handle, map(mapper(true, true), clob.handle, Types.CLOB, ExplicitClob.class).value());
		clob.assertCounts(0, 0, 0);
		Lob xml = lob(SQLXML.class, "handle");
		assertSame(xml.handle, map(mapper(true, true), xml.handle, Types.SQLXML, ExplicitXml.class).value());
		xml.assertCounts(0, 0, 0);
	}

	@Test
	void optedInNestedMappingSharesSuccessfulSingleUseXmlReadsOnlyWithinTheActiveRow() throws Exception {
		Lob xml = lob(SQLXML.class, "<once/>");
		xml.singleRead = true;
		AtomicInteger callbacks = new AtomicInteger();
		ResultSetMapper[] mapper = new ResultSetMapper[1];
		mapper[0] = ResultSetMapper.withPlanCachingEnabled(true).detachLobValues(true).customColumnMappers(List.of(new CustomColumnMapper() {
			@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Object.class); }
			@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object raw, TargetType target,
					Integer column, String label, InstanceProvider provider) throws SQLException {
				assertSame(xml.handle, raw);
				if (callbacks.getAndIncrement() == 0)
					assertEquals("<once/>", DetachedLobMappingTests.map(mapper[0], raw, Types.SQLXML, Object.class));
				return MappingResult.fallback();
			}
		})).build();
		assertEquals("<once/>", map(mapper[0], xml.handle, Types.SQLXML, Object.class));
		assertEquals(2, callbacks.get());
		xml.assertCounts(1, 1, 0);
		DatabaseException nextRow = assertThrows(DatabaseException.class, () -> map(mapper[0], xml.handle, Types.SQLXML, Object.class));
		assertEquals("55000", ((SQLException) nextRow.getCause()).getSQLState(), "A later row must not reuse an earlier XML result");
		assertEquals(2, xml.characterRequests);
		xml.assertCounts(1, 1, 0);
	}

	@Test
	void sqlXmlStringFallbackRequiresAnExplicitNonterminalCapabilitySignal() throws Exception {
		Lob fallback = lob(SQLXML.class, "<xml/>");
		fallback.readerFailure = new SQLFeatureNotSupportedException("character reader unavailable", "0A000");
		assertEquals("<xml/>", map(mapper(true, true), fallback.handle, Types.SQLXML, Object.class));
		assertEquals(1, fallback.stringReads); fallback.assertCounts(0, 0, 1);
		Lob bounded = lob(SQLXML.class, "<xml/>");
		bounded.readerFailure = new SQLFeatureNotSupportedException("character reader unavailable", "0A000");
		assertThrows(DatabaseException.class, () -> map(ResultSetMapper.withPlanCachingEnabled(true).detachLobValues(true)
				.maxLobCharacters(2L).build(), bounded.handle, Types.SQLXML, Object.class));
		assertEquals(1, bounded.stringReads); bounded.assertCounts(0, 0, 1);
		for (SQLException failure : List.of(new SQLException("read failed", "HY000"), new SQLFeatureNotSupportedException("lost", "08006"))) {
			Lob terminal = lob(SQLXML.class, "<xml/>"); terminal.readerFailure = failure;
			DatabaseException thrown = assertThrows(DatabaseException.class, () -> map(mapper(true, true), terminal.handle, Types.SQLXML, Object.class));
			assertSame(failure, thrown.getCause()); assertEquals(0, terminal.stringReads); terminal.assertCounts(0, 0, 1);
		}
	}

	@Test
	void failedReadsRetainPrimaryAndCleanupFailuresAndReleaseEveryOwnedHandle() {
		Lob lob = lob(Clob.class, "failed");
		lob.readFailure = new IOException("read failed"); lob.closeFailure = new IOException("reader close failed");
		lob.freeFailure = new SQLException("free failed");
		DatabaseException thrown = assertThrows(DatabaseException.class, () -> map(mapper(true, true), lob.handle, Types.CLOB, Object.class));
		assertSame(lob.readFailure, thrown.getCause());
		assertArrayEquals(new Throwable[]{lob.closeFailure}, lob.readFailure.getSuppressed());
		assertArrayEquals(new Throwable[]{lob.freeFailure}, thrown.getSuppressed());
		lob.assertCounts(1, 1, 1);
		Lob fatal = lob(SQLXML.class, "fatal");
		AssertionError error = new AssertionError("driver error"); fatal.readerFailure = error;
		assertSame(error, assertThrows(AssertionError.class, () -> map(mapper(true, true), fatal.handle, Types.SQLXML, Object.class)));
		fatal.assertCounts(0, 0, 1);
	}

	@Test
	void convertedNativeMapAndSetCollisionsFailAndRowScopesDoNotRetainValues() throws Exception {
		for (boolean map : new boolean[]{false, true}) {
			Lob first = lob(Clob.class, "equal"), second = lob(Clob.class, "equal");
			Object source;
			if (map) { Map<Object, Object> values = new LinkedHashMap<>(); values.put(first.handle, 1); values.put(second.handle, 2); source = values; }
			else source = new LinkedHashSet<>(List.of(first.handle, second.handle));
			DatabaseException failure = assertThrows(DatabaseException.class, () -> map(mapper(true, true), source, Types.JAVA_OBJECT, Object.class));
			assertTrue(failure.getMessage().contains("duplicate"));
			first.assertCounts(1, 1, 1); second.assertCounts(1, 1, 1);
		}
		MappingResources resources = new MappingResources(20L, 20L, true);
		Map<String, Object> source = new LinkedHashMap<>();
		Lob first = lob(Clob.class, "first"); source.put("lob", first.handle);
		Map<?, ?> firstCopy = (Map<?, ?>) resources.acquire().detachLobs(source, Types.OTHER, "first");
		resources.close(); first.assertCounts(1, 1, 1);
		Lob second = lob(Clob.class, "second"); source.put("lob", second.handle);
		Map<?, ?> secondCopy = (Map<?, ?>) resources.acquire().detachLobs(source, Types.OTHER, "second");
		resources.close(); second.assertCounts(1, 1, 1);
		assertEquals("first", firstCopy.get("lob")); assertEquals("second", secondCopy.get("lob")); assertNotSame(firstCopy, secondCopy);
	}

	private static ResultSetMapper mapper(boolean cached, boolean detached) {
		return ResultSetMapper.withPlanCachingEnabled(cached).detachLobValues(detached).build();
	}
	private static Object mappedValue(ResultSetMapper mapper, Object raw, int jdbcType, Class<?> type) throws SQLException {
		Object result = map(mapper, raw, jdbcType, type);
		return result instanceof ObjectRow row ? row.value() : result instanceof Map<?, ?> map ? map.get("value") : result;
	}
	private static <T> T map(ResultSetMapper mapper, Object raw, int jdbcType, Class<T> type) throws SQLException {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnLabel", "getColumnName" -> "value";
					case "getColumnType" -> jdbcType;
					case "getColumnTypeName" -> "LOB";
					case "getColumnClassName" -> Object.class.getName();
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					default -> throw new AssertionError(method.getName());
				});
		ResultSet rows = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> {
					if (method.getName().equals("getMetaData")) return metadata;
					if (method.getName().equals("getObject")) return raw;
					throw new AssertionError(method.getName());
				});
		JDBCDataSource source = new JDBCDataSource(); source.setUrl("jdbc:hsqldb:mem:detached_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		Database database = Database.with(source, DatabaseType.GENERIC).resultSetMapper(mapper).build();
		StatementContext<T> context = StatementContext.<T>with(Statement.of("detached", "SELECT value"), database).build();
		return mapper.map(context, rows, type, new InstanceProvider() {}).orElse(null);
	}
	private static Lob lob(Class<?> type, String text) { return new Lob(type, text); }
	private static java.sql.Array array(Object values, int jdbcType, AtomicInteger freed) {
		return (java.sql.Array) Proxy.newProxyInstance(java.sql.Array.class.getClassLoader(), new Class<?>[]{java.sql.Array.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getArray" -> values;
					case "getBaseType" -> jdbcType;
					case "free" -> { freed.incrementAndGet(); yield null; }
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					case "toString" -> "tracked SQL Array";
					default -> throw new AssertionError(method.getName());
				});
	}
	private static void assertContent(Lob source, Object result) {
		if (source.type == Blob.class) assertArrayEquals(source.text.getBytes(java.nio.charset.StandardCharsets.UTF_8), (byte[]) result);
		else assertEquals(source.text, result);
	}

	private static final class Lob {
		private final Class<?> type;
		private final String text;
		private final Object handle;
		private final int jdbcType;
		private int opens, closes, frees, stringReads, characterRequests;
		private boolean singleRead;
		private Throwable readerFailure;
		private IOException readFailure, closeFailure;
		private SQLException freeFailure;
		private Lob(Class<?> type, String text) {
			this.type = type; this.text = text;
			this.jdbcType = type == Blob.class ? Types.BLOB : type == SQLXML.class ? Types.SQLXML : type == NClob.class ? Types.NCLOB : Types.CLOB;
			this.handle = Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> switch (method.getName()) {
				case "hashCode" -> System.identityHashCode(proxy);
				case "equals" -> proxy == args[0];
				case "toString" -> "tracked LOB";
				case "getBinaryStream" -> binary();
				case "getCharacterStream" -> character();
				case "getString" -> { this.stringReads++; yield this.text; }
				case "free" -> { this.frees++; if (this.freeFailure != null) throw this.freeFailure; yield null; }
				default -> throw new AssertionError(method.getName());
			});
		}
		private InputStream binary() throws Throwable {
			if (this.readerFailure != null) throw this.readerFailure;
			this.opens++;
			ByteArrayInputStream source = new ByteArrayInputStream(this.text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			return new InputStream() {
				@Override public int read() throws IOException { if (readFailure != null) throw readFailure; return source.read(); }
				@Override public int read(byte[] buffer, int offset, int length) throws IOException { if (readFailure != null) throw readFailure; return source.read(buffer, offset, length); }
				@Override public void close() throws IOException { closes++; if (closeFailure != null) throw closeFailure; source.close(); }
			};
		}
		private Reader character() throws Throwable {
			this.characterRequests++;
			if (this.readerFailure != null) throw this.readerFailure;
			if (this.singleRead && this.characterRequests > 1) throw new SQLException("SQLXML has already been read", "55000");
			this.opens++;
			StringReader source = new StringReader(this.text);
			return new Reader() {
				@Override public int read(char[] buffer, int offset, int length) throws IOException { if (readFailure != null) throw readFailure; return source.read(buffer, offset, length); }
				@Override public void close() throws IOException { closes++; if (closeFailure != null) throw closeFailure; source.close(); }
			};
		}
		private void assertCounts(int opens, int closes, int frees) {
			assertEquals(opens, this.opens, "stream opens"); assertEquals(closes, this.closes, "stream closes"); assertEquals(frees, this.frees, "LOB frees");
		}
	}
}
