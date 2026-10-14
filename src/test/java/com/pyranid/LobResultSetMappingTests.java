/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class LobResultSetMappingTests {
	public record TextRow(String value) {}
	public record BinaryRow(byte[] value) {}
	public record FanOut(@DatabaseColumn("value") String first, @DatabaseColumn("value") String second) {}
	public record BinaryFanOut(@DatabaseColumn("value") byte[] first, @DatabaseColumn("value") byte[] second) {}
	public record NestedBorrowedFanOut(@DatabaseColumn("value") List<byte[]> binary, @DatabaseColumn("value") List<Object> raw) {}
	public record BorrowedFanOut(@DatabaseColumn("value") Object raw, @DatabaseColumn("value") String text) {}
	public static class TextBean {
		private String value;
		public String getValue() { return this.value; }
		public void setValue(String value) { this.value = value; }
	}
	public static class BinaryBean {
		private byte[] value;
		public byte[] getValue() { return this.value; }
		public void setValue(byte[] value) { this.value = value; }
	}

	@Test
	void hsqlLobsHaveTheSameDetachedValuesAcrossRowShapesAndCacheSettings() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).build());
			String text = "SELECT CAST('hello' AS CLOB) AS value FROM (VALUES (0))";
			String binary = "SELECT CAST(X'0001FEFF' AS BLOB) AS value FROM (VALUES (0))";
			for (int execution = 0; execution < 2; execution++) {
				assertEquals("hello", database.query(text).fetchObject(String.class).orElseThrow());
				assertEquals("hello", database.query(text).fetchObject(TextRow.class).orElseThrow().value());
				assertEquals("hello", database.query(text).fetchObject(TextBean.class).orElseThrow().getValue());
				byte[] expected = {0, 1, (byte) 0xfe, (byte) 0xff};
				assertArrayEquals(expected, database.query(binary).fetchObject(byte[].class).orElseThrow());
				assertArrayEquals(expected, database.query(binary).fetchObject(BinaryRow.class).orElseThrow().value());
				assertArrayEquals(expected, database.query(binary).fetchObject(BinaryBean.class).orElseThrow().getValue());
				assertTrue(database.query("SELECT CAST(NULL AS CLOB) AS value FROM (VALUES (0))").fetchObject(String.class).isEmpty());
				assertNull(database.query("SELECT CAST(NULL AS BLOB) AS value FROM (VALUES (0))").fetchObject(BinaryRow.class).orElseThrow().value());
			}
		}
	}

	@Test
	void configuredLimitsApplyToScalarAndPropertyTargetsButNotOrdinaryText() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).maxLobCharacters(4L).maxLobBytes(3L).build());
			for (Class<?> target : List.of(String.class, TextRow.class, TextBean.class)) {
				DatabaseException error = assertThrows(DatabaseException.class, () -> database.query(
						"SELECT CAST('hello' AS CLOB) AS value FROM (VALUES (0))").fetchObject(target));
				assertTrue(error.getMessage().contains("4 UTF-16 code units"));
			}
			for (Class<?> target : List.of(byte[].class, BinaryRow.class, BinaryBean.class))
				assertThrows(DatabaseException.class, () -> database.query(
						"SELECT CAST(X'0001FEFF' AS BLOB) AS value FROM (VALUES (0))").fetchObject(target));
			assertEquals("hello", database.query("SELECT CAST('hello' AS VARCHAR(20)) AS value FROM (VALUES (0))")
					.fetchObject(String.class).orElseThrow());
			assertEquals("hello", database.query("SELECT CAST('hello' AS VARCHAR(20)) AS value FROM (VALUES (0))")
					.fetchObject(TextRow.class).orElseThrow().value());
		}
	}

	@Test
	void fanOutReadsAndMaterializesOnceAndFreesAfterAllBindings() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			TrackedLob text = new TrackedLob("hello");
			FanOut row = map(mapper, text.handle(), Types.CLOB, FanOut.class);
			assertEquals("hello", row.first());
			assertSame(row.first(), row.second());
			text.assertOwnership(1, 1, 1);
			TrackedLob binary = new TrackedLob(new byte[]{1, 2, 3});
			BinaryFanOut bytes = map(mapper, binary.handle(), Types.BLOB, BinaryFanOut.class);
			assertSame(bytes.first(), bytes.second());
			binary.assertOwnership(1, 1, 1);
		}
	}

	@Test
	void customHooksAndRawTargetsBorrowHandlesEvenWhenHooksReturnFallback() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			TrackedLob raw = new TrackedLob("hello");
			BorrowedFanOut row = map(ResultSetMapper.withPlanCachingEnabled(cached).build(), raw.handle(), Types.CLOB, BorrowedFanOut.class);
			assertSame(raw.handle(), row.raw());
			assertEquals("hello", row.text());
			raw.assertOwnership(1, 1, 0);
			for (boolean handled : new boolean[]{false, true}) {
				TrackedLob custom = new TrackedLob("hello");
				AtomicInteger calls = new AtomicInteger();
				ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
					@Override public Boolean appliesTo(TargetType target) { return target.getRawClass() == String.class; }
					@Override public MappingResult map(StatementContext<?> context, ResultSet resultSet, Object value,
							TargetType target, Integer column, String label, InstanceProvider instances) {
						assertSame(custom.handle(), value);
						calls.incrementAndGet();
						return handled ? MappingResult.of(null) : MappingResult.fallback();
					}
				})).build();
				assertEquals(handled ? null : "hello", map(mapper, custom.handle(), Types.CLOB, TextRow.class).value());
				assertEquals(1, calls.get());
				custom.assertOwnership(handled ? 0 : 1, handled ? 0 : 1, 0);
			}
		}
	}

	@Test
	void rawCollectionElementsBorrowLobsSharedWithTypedBindings() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			TrackedLob lob = new TrackedLob(new byte[]{1, 2});
			NestedBorrowedFanOut row = map(ResultSetMapper.withPlanCachingEnabled(cached).build(),
					new Object[]{lob.handle()}, Types.ARRAY, NestedBorrowedFanOut.class);
			assertArrayEquals(new byte[]{1, 2}, row.binary().get(0));
			assertSame(lob.handle(), row.raw().get(0));
			lob.assertOwnership(1, 1, 0);
		}
	}

	@Test
	void limitsCloseStreamsAndPreservePrimaryAndSuppressedFailures() {
		TrackedLob lob = new TrackedLob("oversized");
		lob.freeFailure = new SQLException("free failed");
		DatabaseException failure = assertThrows(DatabaseException.class, () -> map(
				ResultSetMapper.withPlanCachingEnabled(true).maxLobCharacters(4L).build(), lob.handle(), Types.NCLOB, TextRow.class));
		assertTrue(failure.getMessage().contains("4 UTF-16 code units"));
		assertSame(lob.freeFailure, failure.getSuppressed()[0]);
		lob.assertOwnership(1, 1, 1);

		TrackedLob unreadable = new TrackedLob("data");
		unreadable.readFailure = new IOException("read failed");
		unreadable.freeFailure = new SQLException("free failed");
		DatabaseException readFailure = assertThrows(DatabaseException.class, () -> map(
				ResultSetMapper.withDefaultConfiguration(), unreadable.handle(), Types.CLOB, String.class));
		assertSame(unreadable.readFailure, readFailure.getCause());
		assertSame(unreadable.freeFailure, readFailure.getSuppressed()[0]);
		unreadable.assertOwnership(1, 1, 1);
	}

	@Test
	void limitsValidateDetachedLobsAndBuilderOrderIsExplicit() throws Exception {
		ResultSetMapper limited = ResultSetMapper.withPlanCachingEnabled(false).maxLobBytes(2L).maxLobCharacters(2L).build();
		assertThrows(DatabaseException.class, () -> map(limited, "abc", Types.CLOB, String.class));
		assertThrows(DatabaseException.class, () -> map(limited, new byte[]{1, 2, 3}, Types.BLOB, BinaryRow.class));
		assertEquals("ab", map(limited, "ab", Types.NCLOB, TextRow.class).value());
		ResultSetMapper unbounded = ResultSetMapper.withPlanCachingEnabled(false).maxLobBytes(1L).maxLobCharacters(1L)
				.unboundedLobMaterialization().build();
		assertEquals("abc", map(unbounded, "abc", Types.NCLOB, String.class));
		assertArrayEquals(new byte[]{1, 2}, map(unbounded, new byte[]{1, 2}, Types.BLOB, byte[].class));
		ResultSetMapper onlyBinary = ResultSetMapper.withPlanCachingEnabled(false).unboundedLobMaterialization().maxLobBytes(1L).build();
		assertEquals("abc", map(onlyBinary, "abc", Types.CLOB, String.class));
		assertThrows(DatabaseException.class, () -> map(onlyBinary, new byte[]{1, 2}, Types.BLOB, byte[].class));
		assertThrows(IllegalArgumentException.class, () -> ResultSetMapper.withPlanCachingEnabled(true).maxLobBytes(0L));
		assertThrows(IllegalArgumentException.class, () -> ResultSetMapper.withPlanCachingEnabled(true).maxLobCharacters(-1L));
		assertThrows(NullPointerException.class, () -> ResultSetMapper.withPlanCachingEnabled(true).maxLobBytes(null));
	}

	@Test
	void defaultsHaveFiniteBoundariesAndCountUtf16CodeUnits() throws Exception {
		ResultSetMapper defaults = ResultSetMapper.withDefaultConfiguration();
		assertThrows(DatabaseException.class, () -> map(defaults, "x".repeat(8 * 1024 * 1024 + 1), Types.CLOB, String.class));
		assertThrows(DatabaseException.class, () -> map(defaults, new byte[16 * 1024 * 1024 + 1], Types.BLOB, byte[].class));
		ResultSetMapper twoUnits = ResultSetMapper.withPlanCachingEnabled(true).maxLobCharacters(2L).build();
		assertEquals("\uD83D\uDE00", map(twoUnits, new TrackedLob("\uD83D\uDE00").handle(), Types.NCLOB, String.class));
		assertThrows(DatabaseException.class, () -> map(twoUnits, new TrackedLob("\uD83D\uDE00x").handle(), Types.NCLOB, String.class));
	}

	@Test
	void opaqueBorrowingAndUncheckedCleanupFailuresPreserveOwnership() throws Exception {
		TrackedLob borrowed = new TrackedLob("value");
		try (MappingResources resources = new MappingResources(10L, 10L)) {
			Object array = Proxy.newProxyInstance(java.sql.Array.class.getClassLoader(), new Class<?>[]{java.sql.Array.class},
					(proxy, method, arguments) -> { throw new AssertionError("Borrowing must not inspect a driver array"); });
			resources.borrow(array);
			assertEquals("value", resources.materialize(borrowed.handle(), String.class, Types.CLOB, "value"));
		}
		borrowed.assertOwnership(1, 1, 0);

		MappingResources resources = new MappingResources(10L, 10L);
		TrackedLob first = new TrackedLob("one");
		TrackedLob second = new TrackedLob("two");
		AssertionError failure = new AssertionError("driver free failed");
		first.uncheckedFreeFailure = failure;
		resources.materialize(first.handle(), String.class, Types.CLOB, "one");
		resources.materialize(second.handle(), String.class, Types.CLOB, "two");
		assertSame(failure, assertThrows(AssertionError.class, resources::close));
		first.assertOwnership(1, 1, 1);
		second.assertOwnership(1, 1, 1);
		resources.close();
		first.assertOwnership(1, 1, 1);
	}

	@Test
	void rawScalarHandlesAreBorrowedAndRuntimeLobsIgnoreMisleadingMetadata() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).maxLobCharacters(4L).build();
			for (Class<?> target : List.of(Clob.class, NClob.class)) {
				TrackedLob lob = new TrackedLob("hello");
				assertSame(lob.handle(), map(mapper, lob.handle(), Types.CLOB, target));
				lob.assertOwnership(0, 0, 0);
			}
			TrackedLob binary = new TrackedLob(new byte[]{1, 2});
			assertSame(binary.handle(), map(mapper, binary.handle(), Types.BLOB, Blob.class));
			binary.assertOwnership(0, 0, 0);
			for (int jdbcType : new int[]{Types.VARCHAR, Types.OTHER, Types.JAVA_OBJECT}) {
				TrackedLob lob = new TrackedLob("hello");
				assertThrows(DatabaseException.class, () -> map(mapper, lob.handle(), jdbcType, TextRow.class));
				lob.assertOwnership(1, 1, 1);
			}
		}
	}

	@Test
	void customMapperCanReenterTheSameMapperWithoutSharingRowOwnership() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			TrackedLob outer = new TrackedLob("outer");
			TrackedLob inner = new TrackedLob(new byte[]{1, 2});
			ResultSetMapper[] mapper = new ResultSetMapper[1];
			mapper[0] = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.getRawClass() == String.class; }
				@Override public MappingResult map(StatementContext<?> context, ResultSet resultSet, Object value,
						TargetType target, Integer column, String label, InstanceProvider provider) throws SQLException {
					assertArrayEquals(new byte[]{1, 2}, LobResultSetMappingTests.map(mapper[0], inner.handle(), Types.BLOB, byte[].class));
					inner.assertOwnership(1, 1, 1);
					outer.assertOwnership(0, 0, 0);
					return MappingResult.fallback();
				}
			})).build();
			assertEquals("outer", map(mapper[0], outer.handle(), Types.CLOB, TextRow.class).value());
			outer.assertOwnership(1, 1, 0);
			inner.assertOwnership(1, 1, 1);
		}
	}

	@Test
	void reentrantMappingRespectsBorrowedAndOwnedAliasesInActiveRows() throws Exception {
		TrackedLob shared = new TrackedLob("shared");
		ResultSetMapper[] mapper = new ResultSetMapper[1];
		mapper[0] = ResultSetMapper.withCustomColumnMappers(List.of(new CustomColumnMapper() {
			@Override public Boolean appliesTo(TargetType target) { return target.getRawClass() == TextRow.class; }
			@Override public MappingResult map(StatementContext<?> context, ResultSet resultSet, Object value,
					TargetType target, Integer column, String label, InstanceProvider provider) throws SQLException {
				String detached = LobResultSetMappingTests.map(mapper[0], value, Types.CLOB, String.class);
				shared.assertOwnership(1, 1, 0);
				return MappingResult.of(new TextRow(detached));
			}
		})).build();
		assertEquals("shared", map(mapper[0], shared.handle(), Types.CLOB, TextRow.class).value());
		shared.assertOwnership(1, 1, 0);

		MappingResources pool = new MappingResources(20L, 20L);
		MappingResources outer = pool.acquire();
		TrackedLob owned = new TrackedLob("owned");
		outer.materialize(owned.handle(), String.class, Types.CLOB, "outer");
		MappingResources child = pool.acquire();
		child.materialize(owned.handle(), String.class, Types.CLOB, "child");
		child.close();
		owned.assertOwnership(2, 2, 0);
		outer.close();
		owned.assertOwnership(2, 2, 1);

		outer = pool.acquire();
		TrackedLob retained = new TrackedLob("retained");
		outer.materialize(retained.handle(), String.class, Types.CLOB, "outer");
		child = pool.acquire();
		child.borrow(retained.handle());
		child.close();
		outer.close();
		retained.assertOwnership(1, 1, 0);
	}

	@Test
	void reusedScopesResetAfterFailureAndKeepReentrantRowsIndependent() throws Exception {
		MappingResources pool = new MappingResources(10L, 10L);
		MappingResources outer = pool.acquire();
		TrackedLob borrowed = new TrackedLob("outer");
		outer.borrow(borrowed.handle());
		assertEquals("outer", outer.materialize(borrowed.handle(), String.class, Types.CLOB, "outer"));
		MappingResources inner = pool.acquire();
		assertNotSame(outer, inner);
		TrackedLob failing = new TrackedLob("inner");
		failing.uncheckedFreeFailure = new AssertionError("free");
		inner.materialize(failing.handle(), String.class, Types.CLOB, "inner");
		assertSame(failing.uncheckedFreeFailure, assertThrows(AssertionError.class, inner::close));
		assertSame(inner, pool.acquire());
		TrackedLob next = new TrackedLob("next");
		inner.materialize(next.handle(), String.class, Types.CLOB, "next");
		inner.close();
		outer.close();
		borrowed.assertOwnership(1, 1, 0);
		failing.assertOwnership(1, 1, 1);
		next.assertOwnership(1, 1, 1);
		assertSame(outer, pool.acquire());
		// Borrowed/materialized state must not carry across rows, even for the same handle.
		outer.materialize(borrowed.handle(), String.class, Types.CLOB, "again");
		outer.close();
		borrowed.assertOwnership(2, 2, 1);
	}

	private static Database database(ResultSetMapper mapper) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:lob_mapping");
		source.setUser("sa");
		return Database.withDataSource(source).databaseType(DatabaseType.GENERIC).resultSetMapper(mapper).build();
	}

	private static <T> T map(ResultSetMapper mapper, Object raw, int jdbcType, Class<T> target) throws SQLException {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class}, (proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnType" -> jdbcType;
					case "getColumnTypeName" -> "LOB";
					case "getColumnLabel", "getColumnName" -> "value";
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					default -> throw new AssertionError("Unexpected metadata call: " + method.getName());
				});
		AtomicInteger reads = new AtomicInteger();
		ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject" -> { reads.incrementAndGet(); yield raw; }
					case "wasNull" -> raw == null;
					default -> throw new AssertionError("Unexpected/unbounded JDBC read: " + method.getName());
				});
		StatementContext<T> context = StatementContext.<T>with(Statement.of("lob", "SELECT value"), database(mapper))
				.resultSetRowType(target).build();
		T value = mapper.map(context, resultSet, target, new InstanceProvider() {}).orElse(null);
		assertEquals(1, reads.get(), "Each source column should be fetched once");
		return value;
	}

	private static final class TrackedLob {
		private final Object handle;
		private int opens;
		private int closes;
		private int frees;
		private IOException readFailure;
		private SQLException freeFailure;
		private Error uncheckedFreeFailure;
		TrackedLob(Object content) {
			boolean binary = content instanceof byte[];
			this.handle = Proxy.newProxyInstance(Blob.class.getClassLoader(), new Class<?>[]{binary ? Blob.class : NClob.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getBinaryStream" -> {
							this.opens++;
							yield new ByteArrayInputStream((byte[]) content) {
								@Override public void close() { closes++; }
							};
						}
						case "getCharacterStream" -> {
							this.opens++;
							yield new StringReader((String) content) {
								@Override public int read(char[] chars, int offset, int length) throws IOException {
									if (readFailure != null) throw readFailure;
									return super.read(chars, offset, length);
								}
								@Override public void close() { closes++; super.close(); }
							};
						}
						case "free" -> { this.frees++; if (this.uncheckedFreeFailure != null) throw this.uncheckedFreeFailure; if (this.freeFailure != null) throw this.freeFailure; yield null; }
						case "toString" -> "TrackedLob";
						default -> throw new AssertionError("Unexpected LOB operation: " + method.getName());
					});
		}
		Object handle() { return this.handle; }
		void assertOwnership(int opens, int closes, int frees) {
			assertEquals(opens, this.opens, "streams opened");
			assertEquals(closes, this.closes, "streams closed");
			assertEquals(frees, this.frees, "LOB handles freed");
		}
	}
}
