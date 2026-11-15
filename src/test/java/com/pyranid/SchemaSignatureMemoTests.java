/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SchemaSignatureMemoTests {
	@Test
	void cursorMemoUsesIdentityAndSkipsFreshMetadataOnHits() throws Exception {
		DefaultResultSetMapper mapper = mapper();
		StatementContext<?> context = context(DatabaseType.GENERIC);
		ResultSet first = cursor(), second = cursor();
		AtomicInteger firstReads = new AtomicInteger(), secondReads = new AtomicInteger();
		String firstSignature = mapper.schemaSignatureFor(context, first, metadata("first", firstReads, null));
		assertEquals(firstSignature, mapper.schemaSignatureFor(context, first, metadata("must_not_be_read", new AtomicInteger(),
				() -> fail("A memoized cursor must not inspect newly supplied metadata"))));
		String secondSignature = mapper.schemaSignatureFor(context, second, metadata("second", secondReads, null));
		assertNotEquals(firstSignature, secondSignature);
		assertEquals(1, firstReads.get());
		assertEquals(1, secondReads.get());
		assertEquals(2, memo(mapper).size());
		assertWeakKeys(mapper);
		assertLookupCleared(mapper);
	}

	@Test
	void oneCursorKeepsIndependentDialectSignaturesAndOnlyWeakStoredKeys() throws Exception {
		DefaultResultSetMapper mapper = mapper();
		ResultSet cursor = cursor();
		AtomicInteger reads = new AtomicInteger();
		ResultSetMetaData metadata = metadata("id", reads, null);
		for (DatabaseType type : List.of(DatabaseType.GENERIC, DatabaseType.POSTGRESQL)) {
			StatementContext<?> context = context(type);
			String signature = mapper.schemaSignatureFor(context, cursor, metadata);
			assertEquals(signature, mapper.schemaSignatureFor(context, cursor, metadata));
		}
		assertEquals(2, reads.get(), "Each dialect builds once, even when the cursor identity is shared");
		assertEquals(1, memo(mapper).size());
		assertEquals(2, ((Map<?, ?>) memo(mapper).values().iterator().next()).size());
		assertWeakKeys(mapper);
		assertLookupCleared(mapper);
	}

	@Test
	void executionMemoIncludesMapperIdentityAndDoesNotChangeContextValueSemantics() throws Exception {
		DefaultResultSetMapper root = mapper();
		DefaultResultSetMapper turkish = (DefaultResultSetMapper) ResultSetMapper.withNormalizationLocale(Locale.forLanguageTag("tr")).build();
		StatementContext<?> context = context(DatabaseType.GENERIC), equalContext = context(DatabaseType.GENERIC);
		int hash = context.hashCode();
		String rendering = context.toString();
		ResultSet cursor = cursor();
		ResultSetMetaData metadata = metadata("\u0130", new AtomicInteger(), null);
		String rootSignature = root.schemaSignatureFor(context, cursor, metadata);
		String turkishSignature = turkish.schemaSignatureFor(context, cursor, metadata);
		assertNotEquals(rootSignature, turkishSignature);
		for (int pass = 0; pass < 3; pass++) {
			assertEquals(rootSignature, root.schemaSignatureFor(context, cursor, metadata));
			assertEquals(turkishSignature, turkish.schemaSignatureFor(context, cursor, metadata));
		}
		assertEquals(equalContext, context);
		assertEquals(hash, context.hashCode());
		assertEquals(rendering, context.toString());
	}

	@Test
	void executionMemoContainsOnlyWeakIdentitiesAndRechecksEachOne() throws Exception {
		DefaultResultSetMapper mapper = mapper();
		StatementContext<?> context = context(DatabaseType.GENERIC);
		ResultSet cursor = cursor();
		ResultSetMetaData metadata = metadata("id", new AtomicInteger(), null);
		String expected = mapper.schemaSignatureFor(context, cursor, metadata);
		for (String identity : List.of("mapper", "resultSet", "dialect")) {
			Object memo = context.schemaSignatureMemo();
			int references = 0, strings = 0;
			for (Field field : memo.getClass().getDeclaredFields()) {
				field.setAccessible(true);
				Object value = field.get(memo);
				if (value instanceof Reference<?>) references++;
				else { assertInstanceOf(String.class, value); strings++; }
			}
			assertEquals(3, references);
			assertEquals(1, strings);
			Field field = memo.getClass().getDeclaredField(identity);
			field.setAccessible(true);
			((Reference<?>) field.get(memo)).clear();
			assertEquals(expected, mapper.schemaSignatureFor(context, cursor, metadata));
			assertNotSame(memo, context.schemaSignatureMemo(), "A cleared weak identity cannot authorize a fast hit: " + identity);
			assertLookupCleared(mapper);
		}
	}

	@Test
	void aWarmExecutionDoesNotWaitForTheSharedSchemaCacheLock() throws Exception {
		DefaultResultSetMapper mapper = mapper();
		StatementContext<?> context = context(DatabaseType.GENERIC);
		ResultSet cursor = cursor();
		ResultSetMetaData metadata = metadata("id", new AtomicInteger(), null);
		String expected = mapper.schemaSignatureFor(context, cursor, metadata);
		ExecutorService executor = Executors.newFixedThreadPool(2);
		CountDownLatch held = new CountDownLatch(1), release = new CountDownLatch(1);
		try {
			Future<?> owner = executor.submit(() -> {
				mapper.getSchemaSignatureByResultSetMetaDataLock().lock();
				try { held.countDown(); assertTrue(release.await(10, TimeUnit.SECONDS)); }
				finally { mapper.getSchemaSignatureByResultSetMetaDataLock().unlock(); }
				return null;
			});
			assertTrue(held.await(10, TimeUnit.SECONDS));
			Future<String> hit = executor.submit(() -> mapper.schemaSignatureFor(context, cursor, metadata));
			assertEquals(expected, hit.get(2, TimeUnit.SECONDS));
			release.countDown();
			owner.get(10, TimeUnit.SECONDS);
		} finally {
			release.countDown(); executor.shutdownNow();
			assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
		}
	}

	@Test
	void reusableLookupIsClearedBeforeMetadataCallbacksIncludingFailures() throws Exception {
		for (Throwable failure : new Throwable[]{new SQLException("metadata unavailable"), new AssertionError("metadata failed")}) {
			DefaultResultSetMapper mapper = mapper();
			StatementContext<?> context = context(DatabaseType.GENERIC);
			ResultSet cursor = cursor();
			ResultSetMetaData broken = metadata("broken", new AtomicInteger(), () -> {
				assertLookupCleared(mapper);
				assertFalse(mapper.getSchemaSignatureByResultSetMetaDataLock().isHeldByCurrentThread());
				throw failure;
			});
			assertSame(failure, assertThrows(Throwable.class, () -> mapper.schemaSignatureFor(context, cursor, broken)));
			assertLookupCleared(mapper);
			assertTrue(memo(mapper).isEmpty());
			String signature = mapper.schemaSignatureFor(context, cursor, metadata("recovered", new AtomicInteger(), null));
			assertTrue(signature.contains("recovered"));
			assertLookupCleared(mapper);
		}
	}

	@Test
	void metadataMayReenterTheSameMapperWithoutOverwritingAnOuterLookup() throws Exception {
		DefaultResultSetMapper mapper = mapper();
		StatementContext<?> context = context(DatabaseType.GENERIC);
		ResultSet outer = cursor(), inner = cursor();
		AtomicInteger innerReads = new AtomicInteger(), outerReads = new AtomicInteger();
		ResultSetMetaData innerMetadata = metadata("inner", innerReads, null);
		ResultSetMetaData outerMetadata = metadata("outer", outerReads, () -> {
			assertLookupCleared(mapper);
			assertFalse(mapper.getSchemaSignatureByResultSetMetaDataLock().isHeldByCurrentThread());
			assertTrue(mapper.schemaSignatureFor(context, inner, innerMetadata).contains("inner"));
			assertLookupCleared(mapper);
		});
		String signature = mapper.schemaSignatureFor(context, outer, outerMetadata);
		assertTrue(signature.contains("outer"));
		assertEquals(signature, mapper.schemaSignatureFor(context, outer, outerMetadata));
		assertTrue(mapper.schemaSignatureFor(context, inner, innerMetadata).contains("inner"));
		assertEquals(1, outerReads.get());
		assertEquals(1, innerReads.get());
		assertEquals(2, memo(mapper).size());
		assertLookupCleared(mapper);
	}

	@Test
	void anExpiredCursorKeyIsDrainedAndCanBeReplacedWithoutAStaleIdentityMatch() throws Exception {
		DefaultResultSetMapper mapper = mapper();
		StatementContext<?> context = context(DatabaseType.GENERIC);
		ResultSet cursor = cursor();
		ResultSetMetaData metadata = metadata("id", new AtomicInteger(), null);
		String expected = mapper.schemaSignatureFor(context, cursor, metadata);
		Reference<?> expired = (Reference<?>) memo(mapper).keySet().iterator().next();
		expired.clear();
		assertTrue(expired.enqueue());
		clearExecutionIdentity(context, "resultSet");
		assertEquals(expected, mapper.schemaSignatureFor(context, cursor, metadata));
		assertEquals(1, memo(mapper).size());
		Reference<?> replacement = (Reference<?>) memo(mapper).keySet().iterator().next();
		assertNotSame(expired, replacement);
		assertSame(cursor, replacement.get());
		assertWeakKeys(mapper);
		assertLookupCleared(mapper);
	}

	@Test
	void expiredDialectKeysArePrunedOnTheNextMiss() throws Exception {
		DefaultResultSetMapper mapper = mapper();
		StatementContext<?> context = context(DatabaseType.GENERIC);
		ResultSet cursor = cursor();
		ResultSetMetaData metadata = metadata("id", new AtomicInteger(), null);
		String expected = mapper.schemaSignatureFor(context, cursor, metadata);
		Map<?, ?> dialects = (Map<?, ?>) memo(mapper).values().iterator().next();
		Reference<?> expired = (Reference<?>) dialects.keySet().iterator().next();
		expired.clear();
		clearExecutionIdentity(context, "dialect");
		assertEquals(expected, mapper.schemaSignatureFor(context, cursor, metadata));
		assertEquals(1, dialects.size());
		Reference<?> replacement = (Reference<?>) dialects.keySet().iterator().next();
		assertNotSame(expired, replacement);
		assertSame(context.getDatabaseDialect(), replacement.get());
		assertLookupCleared(mapper);
	}

	@Test
	void warmExecutionHitsStillDrainOtherExpiredCursorEntries() throws Exception {
		DefaultResultSetMapper mapper = mapper();
		StatementContext<?> oldContext = context(DatabaseType.GENERIC), warmContext = context(DatabaseType.GENERIC);
		ResultSet expiredCursor = cursor(), warmCursor = cursor();
		ResultSetMetaData metadata = metadata("id", new AtomicInteger(), null);
		mapper.schemaSignatureFor(oldContext, expiredCursor, metadata);
		String expected = mapper.schemaSignatureFor(warmContext, warmCursor, metadata);
		Reference<?> expired = memo(mapper).keySet().stream().map(key -> (Reference<?>) key)
				.filter(reference -> reference.get() == expiredCursor).findFirst().orElseThrow();
		expired.clear();
		assertTrue(expired.enqueue());
		clearExecutionIdentity(oldContext, "resultSet");
		assertEquals(expected, mapper.schemaSignatureFor(warmContext, warmCursor, metadata));
		assertEquals(1, memo(mapper).size());
		assertSame(warmCursor, ((Reference<?>) memo(mapper).keySet().iterator().next()).get());
		assertLookupCleared(mapper);
	}

	@Test
	void concurrentCursorHitsAndMissesCannotShareSearchReferents() throws Exception {
		DefaultResultSetMapper mapper = mapper();
		StatementContext<?> context = context(DatabaseType.GENERIC);
		ExecutorService executor = Executors.newFixedThreadPool(6);
		CountDownLatch start = new CountDownLatch(1);
		List<ResultSet> retainedCursors = new ArrayList<>();
		List<Future<?>> futures = new ArrayList<>();
		try {
			for (int index = 0; index < 6; index++) {
				String label = "column_" + index;
				ResultSet cursor = cursor();
				retainedCursors.add(cursor);
				AtomicInteger reads = new AtomicInteger();
				ResultSetMetaData metadata = metadata(label, reads, null);
				futures.add(executor.submit(() -> {
					assertTrue(start.await(10, TimeUnit.SECONDS));
					String expected = mapper.schemaSignatureFor(context, cursor, metadata);
					assertTrue(expected.contains(label));
					for (int row = 0; row < 500; row++)
						assertEquals(expected, mapper.schemaSignatureFor(context, cursor, metadata));
					assertEquals(1, reads.get());
					return null;
				}));
			}
			start.countDown();
			for (Future<?> future : futures) future.get(20, TimeUnit.SECONDS);
			assertEquals(retainedCursors.size(), memo(mapper).size());
			assertWeakKeys(mapper);
			assertLookupCleared(mapper);
		} finally {
			start.countDown();
			executor.shutdownNow();
			assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
			Reference.reachabilityFence(retainedCursors);
		}
	}

	private static DefaultResultSetMapper mapper() {
		return (DefaultResultSetMapper) ResultSetMapper.withPlanCachingEnabled(true).build();
	}

	private static StatementContext<?> context(DatabaseType type) {
		return StatementContext.with(Statement.of("schema-memo", "SELECT id"), Database.with(new JDBCDataSource(), type).build()).build();
	}

	private static ResultSet cursor() {
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, arguments) -> { throw new AssertionError("Cursor method must not run during identity memoization: " + method.getName()); });
	}

	@FunctionalInterface
	private interface MetadataCallback { void run() throws Throwable; }

	private static ResultSetMetaData metadata(String label, AtomicInteger labelReads, MetadataCallback callback) {
		return (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, arguments) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnLabel" -> {
						labelReads.incrementAndGet();
						if (callback != null) callback.run();
						yield label;
					}
					case "getColumnType" -> Types.INTEGER;
					case "getColumnTypeName" -> "INTEGER";
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == arguments[0];
					default -> throw new AssertionError(method.getName());
				});
	}

	private static Map<?, ?> memo(DefaultResultSetMapper mapper) throws Exception {
		Field field = DefaultResultSetMapper.class.getDeclaredField("schemaSignaturesByResultSet");
		field.setAccessible(true);
		return (Map<?, ?>) field.get(mapper);
	}

	private static void clearExecutionIdentity(StatementContext<?> context, String name) throws Exception {
		Object memo = context.schemaSignatureMemo();
		Field field = memo.getClass().getDeclaredField(name);
		field.setAccessible(true);
		((Reference<?>) field.get(memo)).clear();
	}

	private static void assertLookupCleared(DefaultResultSetMapper mapper) throws Exception {
		Field field = DefaultResultSetMapper.class.getDeclaredField("schemaSignatureLookup");
		field.setAccessible(true);
		Object lookup = field.get(mapper);
		Field referent = lookup.getClass().getDeclaredField("referent");
		referent.setAccessible(true);
		assertNull(referent.get(lookup), "The reusable search key must never retain a cursor or dialect after unlocking");
		assertEquals(0, lookup.hashCode());
	}

	private static void assertWeakKeys(DefaultResultSetMapper mapper) throws Exception {
		for (Map.Entry<?, ?> entry : memo(mapper).entrySet()) {
			assertInstanceOf(Reference.class, entry.getKey());
			for (Object dialect : ((Map<?, ?>) entry.getValue()).keySet()) assertInstanceOf(Reference.class, dialect);
		}
	}
}
