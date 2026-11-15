/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MappingResourceReuseTests {
	public record BinaryRow(byte[] body) {}
	public record NestedRow(byte[] body, Integer trigger) {}

	@Test
	void completedScopesReuseAnEmptyRootWithoutReusingThePreviousRowsValues() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			DefaultResultSetMapper mapper = mapper(cached);
			Lob lob = new Lob(null, null);
			assertArrayEquals(new byte[]{1, 2}, map(mapper, BinaryRow.class, row(lob)).body());
			WeakReference<MappingResources> holder = holder(mapper);
			MappingResources root = holder.get();
			assertNotNull(root);
			assertEmpty(root);
			lob.bytes = new byte[]{3, 4};
			assertArrayEquals(new byte[]{3, 4}, map(mapper, BinaryRow.class, row(lob)).body());
			assertSame(holder, holder(mapper));
			assertSame(root, holder.get());
			assertEquals(2, lob.reads.get());
			assertEquals(2, lob.frees.get());
			assertEmpty(root);
			Reference.reachabilityFence(root);
		}
	}

	@Test
	void readAndCleanupFailuresStillLeaveTheReusedScopeEmpty() throws Exception {
		for (boolean readFails : new boolean[]{false, true}) {
			for (Throwable cleanup : new Throwable[]{new SQLException("free failed"), new AssertionError("free failed")}) {
				DefaultResultSetMapper mapper = mapper(true);
				map(mapper, BinaryRow.class, row(new Lob(null, null)));
				MappingResources root = holder(mapper).get();
				assertNotNull(root);
				SQLException read = readFails ? new SQLException("read failed") : null;
				Lob failed = new Lob(read, cleanup);
				Throwable thrown = assertThrows(Throwable.class, () -> map(mapper, BinaryRow.class, row(failed)));
				if (readFails) {
					assertSame(read, thrown.getCause());
					assertArrayEquals(new Throwable[]{cleanup}, thrown.getSuppressed());
				} else assertSame(cleanup, thrown);
				assertEquals(1, failed.frees.get());
				assertEmpty(root);
				Lob recovered = new Lob(null, null);
				assertArrayEquals(new byte[]{1, 2}, map(mapper, BinaryRow.class, row(recovered)).body());
				assertSame(root, holder(mapper).get());
				assertEquals(1, recovered.reads.get());
				assertEquals(1, recovered.frees.get());
				assertEmpty(root);
				Reference.reachabilityFence(root);
			}
		}
	}

	@Test
	void nestedCleanupDoesNotDiscardTheActiveRootAndChildFailuresDoNotContaminateLaterRows() throws Exception {
		for (boolean failChildCleanup : new boolean[]{false, true}) {
			Lob outer = new Lob(null, null);
			AssertionError childFailure = failChildCleanup ? new AssertionError("nested free failed") : null;
			Lob inner = new Lob(null, childFailure);
			AtomicReference<DefaultResultSetMapper> owner = new AtomicReference<>();
			AtomicReference<MappingResources> activeRoot = new AtomicReference<>();
			DefaultResultSetMapper mapper = (DefaultResultSetMapper) ResultSetMapper.withCustomColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(Integer.class); }
				@Override public MappingResult map(StatementContext<?> context, ResultSet result, Object value, TargetType target,
						Integer index, String label, InstanceProvider provider) throws SQLException {
					try {
						MappingResources root = holder(owner.get()).get();
						assertNotNull(root); activeRoot.set(root);
						assertTrue((boolean) field(root, "inUse"));
						if (childFailure == null) assertArrayEquals(new byte[]{1, 2}, MappingResourceReuseTests.map(owner.get(), BinaryRow.class, row(inner)).body());
						else assertSame(childFailure, assertThrows(AssertionError.class,
								() -> MappingResourceReuseTests.map(owner.get(), BinaryRow.class, row(inner))));
						assertSame(root, holder(owner.get()).get());
						assertTrue((boolean) field(root, "inUse"));
						assertEquals(0, outer.frees.get(), "Nested completion must not release the outer row's handle");
						assertEquals(1, inner.frees.get());
						assertEmpty((MappingResources) field(root, "nested"));
						return MappingResult.of(7);
					} catch (ReflectiveOperationException failure) { throw new SQLException("Cannot inspect test scope", failure); }
				}
			})).build();
			owner.set(mapper);
			assertEquals(7, map(mapper, NestedRow.class, row(outer, 7)).trigger());
			assertEquals(1, outer.frees.get());
			MappingResources root = activeRoot.get();
			assertEmpty(root);
			assertEmpty((MappingResources) field(root, "nested"));
			Lob next = new Lob(null, null);
			map(mapper, BinaryRow.class, row(next));
			assertSame(root, holder(mapper).get());
			assertEquals(1, next.reads.get());
			assertEquals(1, next.frees.get());
			assertEmpty(root);
		}
	}

	@Test
	void clearedWeakHolderCreatesAnotherRootAndScalarCursorMetadataRefreshes() throws Exception {
		DefaultResultSetMapper mapper = mapper(true);
		AtomicInteger firstTypes = new AtomicInteger(), secondTypes = new AtomicInteger(), thirdTypes = new AtomicInteger();
		assertEquals("first", map(mapper, String.class, scalar("first", Types.VARCHAR, firstTypes)));
		WeakReference<MappingResources> previous = holder(mapper);
		MappingResources root = previous.get();
		assertNotNull(root);
		assertEmpty(root);
		previous.clear(); // Deterministically model collection of the idle weak referent.
		assertEquals("42", map(mapper, String.class, scalar(42, Types.INTEGER, secondTypes)));
		assertNotSame(previous, holder(mapper));
		assertNotSame(root, holder(mapper).get());
		MappingResources replacement = holder(mapper).get();
		assertNotNull(replacement);
		assertEmpty(replacement);
		AtomicInteger clobReads = new AtomicInteger(), clobFrees = new AtomicInteger();
		Clob clob = (Clob) Proxy.newProxyInstance(Clob.class.getClassLoader(), new Class<?>[]{Clob.class},
				(proxy, method, arguments) -> switch (method.getName()) {
					case "getCharacterStream" -> { clobReads.incrementAndGet(); yield new StringReader("third"); }
					case "free" -> { clobFrees.incrementAndGet(); yield null; }
					default -> throw new AssertionError("CLOB must be read and freed through materialization: " + method.getName());
				});
		assertEquals("third", map(mapper, String.class, scalar(clob, Types.CLOB, thirdTypes)));
		assertEquals(1, clobReads.get());
		assertEquals(1, clobFrees.get());
		assertEquals("fourth", map(mapper, String.class, scalar("fourth", Types.VARCHAR, new AtomicInteger())));
		assertTrue(firstTypes.get() > 0);
		assertTrue(secondTypes.get() > 0);
		assertTrue(thirdTypes.get() > 0, "Reused root must refresh scalar JDBC type for another cursor identity");
		assertSame(replacement, holder(mapper).get());
		assertEmpty(replacement);
		Reference.reachabilityFence(root);
		Reference.reachabilityFence(replacement);
	}

	private static DefaultResultSetMapper mapper(boolean cached) {
		return (DefaultResultSetMapper) ResultSetMapper.withPlanCachingEnabled(cached).build();
	}

	private static <T> T map(DefaultResultSetMapper mapper, Class<T> type, ResultSet rows) throws SQLException {
		Database database = Database.with(new JDBCDataSource(), DatabaseType.GENERIC).resultSetMapper(mapper).build();
		StatementContext<T> context = StatementContext.<T>with(Statement.of("scope-reuse", "SELECT values"), database).resultSetRowType(type).build();
		try (rows) { return mapper.map(context, rows, type, new InstanceProvider() {}).orElseThrow(); }
	}

	@SuppressWarnings("unchecked")
	private static WeakReference<MappingResources> holder(DefaultResultSetMapper mapper) throws ReflectiveOperationException {
		ThreadLocal<?> local = (ThreadLocal<?>) field(mapper, "mappingResources");
		Object value = local.get();
		assertNotNull(value);
		assertSame(WeakReference.class, value.getClass(), "A live worker may hold only the plain JDK weak-reference value");
		return (WeakReference<MappingResources>) value;
	}

	private static Object field(Object value, String name) throws ReflectiveOperationException {
		Field field = value.getClass().getDeclaredField(name);
		field.setAccessible(true);
		return field.get(value);
	}

	private static void assertEmpty(MappingResources resources) throws ReflectiveOperationException {
		assertNotNull(resources);
		for (String name : List.of("owned", "borrowed", "materialized", "detachedContainers", "arrayTypes", "sqlArrayExtractions", "containerTypeNames"))
			assertNull(field(resources, name), "Completed scope retained row state: " + name);
		assertEquals(false, field(resources, "inUse"));
		assertEquals(false, field(resources, "opaqueBorrow"));
		Object cursor = field(resources, "scalarResultSet");
		if (cursor != null) assertSame(WeakReference.class, cursor.getClass());
	}

	private static ResultSet row(Lob lob, Object... trigger) {
		return rows(trigger.length == 0 ? new String[]{"body"} : new String[]{"body", "trigger"},
				trigger.length == 0 ? new int[]{Types.BLOB} : new int[]{Types.BLOB, Types.INTEGER},
				trigger.length == 0 ? new Object[]{lob.handle} : new Object[]{lob.handle, trigger[0]});
	}

	private static ResultSet scalar(Object value, int type, AtomicInteger typeReads) {
		return rows(new String[]{"value"}, new int[]{type}, new Object[]{value}, typeReads);
	}

	private static ResultSet rows(String[] labels, int[] types, Object[] values) {
		return rows(labels, types, values, new AtomicInteger());
	}

	private static ResultSet rows(String[] labels, int[] types, Object[] values, AtomicInteger typeReads) {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class}, (proxy, method, arguments) -> switch (method.getName()) {
					case "getColumnCount" -> labels.length;
					case "getColumnLabel", "getColumnName" -> labels[(int) arguments[0] - 1];
					case "getColumnType" -> { typeReads.incrementAndGet(); yield types[(int) arguments[0] - 1]; }
					case "getColumnTypeName" -> types[(int) arguments[0] - 1] == Types.BLOB ? "BLOB" : "VALUE";
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == arguments[0];
					default -> throw new AssertionError(method.getName());
				});
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, arguments) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject", "getBlob", "getClob" -> values[(int) arguments[0] - 1];
					case "getString" -> {
						Object value = values[(int) arguments[0] - 1];
						assertFalse(value instanceof Clob, "A CLOB must not bypass resource ownership through getString");
						yield value.toString();
					}
					case "wasNull" -> false;
					case "close" -> null;
					default -> throw new AssertionError(method.getName());
				});
	}

	private static final class Lob {
		byte[] bytes = {1, 2};
		final AtomicInteger reads = new AtomicInteger(), frees = new AtomicInteger();
		final Blob handle;
		Lob(Throwable readFailure, Throwable freeFailure) {
			handle = (Blob) Proxy.newProxyInstance(Blob.class.getClassLoader(), new Class<?>[]{Blob.class},
					(proxy, method, arguments) -> switch (method.getName()) {
						case "getBinaryStream" -> {
							reads.incrementAndGet();
							if (readFailure != null) throw readFailure;
							yield new ByteArrayInputStream(bytes);
						}
						case "length" -> (long) bytes.length;
						case "free" -> {
							frees.incrementAndGet();
							if (freeFailure != null) throw freeFailure;
							yield null;
						}
						default -> throw new AssertionError(method.getName());
					});
		}
	}
}
