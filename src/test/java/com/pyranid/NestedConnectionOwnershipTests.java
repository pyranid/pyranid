/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class NestedConnectionOwnershipTests {
	@Test
	void aKnownReusedConnectionCannotCommitOrDisposeOfTheOwnersWork() throws Exception {
		for (boolean commitOuter : new boolean[]{false, true})
			for (boolean captureInner : new boolean[]{false, true})
				try (SingletonFixture fixture = new SingletonFixture()) {
					TransactionOutcome<Void> outer = fixture.database.transactionWithOutcome(() -> {
						fixture.insert(1);
						int stateCalls = fixture.stateCalls.get();
						if (captureInner) {
							TransactionOutcome<Void> inner = fixture.database.transactionWithOutcome(() -> { fixture.insert(2); });
							assertEquals(TransactionResult.ROLLED_BACK, inner.getResult());
							assertTrue(inner.getAttemptedValue().isEmpty());
							assertOwnershipFailure(inner.getFailure().orElseThrow());
						} else {
							assertOwnershipFailure(assertThrows(DatabaseException.class,
									() -> fixture.database.transaction(() -> { fixture.insert(2); })));
						}
						assertEquals(stateCalls, fixture.stateCalls.get(), "Reject before reading or changing another owner's JDBC state");
						assertEquals(0, fixture.closes.get(), "The rejected inner lease must never be returned to the source");
						assertEquals(0, fixture.aborts.get());
						assertEquals(0, fixture.commits.get());
						assertEquals(0, fixture.rollbacks.get());
						assertFalse(fixture.database.currentTransaction().orElseThrow().isRollbackOnly());
						fixture.insert(3);
						if (!commitOuter) fixture.database.currentTransaction().orElseThrow().setRollbackOnly(true);
					});
					assertEquals(commitOuter ? TransactionResult.COMMITTED : TransactionResult.ROLLED_BACK, outer.getResult());
					assertTrue(outer.getFailure().isEmpty());
					assertEquals(commitOuter ? List.of(1, 3) : List.of(), fixture.ids());
					assertEquals(2, fixture.checkouts.get());
					assertEquals(1, fixture.closes.get());
					assertEquals(0, fixture.aborts.get());
					assertEquals(commitOuter ? 1 : 0, fixture.commits.get());
					assertEquals(commitOuter ? 0 : 1, fixture.rollbacks.get());
					assertEquals(1, fixture.acquisitionFailures.get());
					assertEquals(List.of(MetricsCollector.PhysicalTransactionBeginFailurePhase.ACQUIRE_CONNECTION), fixture.beginFailures);
				}
	}

	@Test
	void rejectionIsFencedEvenIfTheInnerCallbackHandlesItsFirstFailure() throws Exception {
		try (SingletonFixture fixture = new SingletonFixture()) {
			fixture.database.transaction(() -> {
				fixture.insert(1);
				TransactionOutcome<String> inner = fixture.database.transactionWithOutcome(() -> {
					DatabaseException first = assertThrows(DatabaseException.class, () -> fixture.insert(2));
					assertSame(first, assertThrows(DatabaseException.class, () -> fixture.insert(4)));
					return Optional.of("handled");
				});
				assertEquals(TransactionResult.ROLLED_BACK, inner.getResult());
				assertEquals(Optional.of("handled"), inner.getAttemptedValue());
				assertOwnershipFailure(inner.getFailure().orElseThrow());
				assertEquals(2, fixture.checkouts.get(), "A failed begin cannot reacquire the rejected connection");
				assertEquals(0, fixture.closes.get());
				fixture.insert(3);
			});
			assertEquals(List.of(1, 3), fixture.ids());
			assertEquals(1, fixture.acquisitionFailures.get());
		}
	}

	@Test
	void identityCheckIncludesEveryAmbientOwnerAcrossDifferentDatabaseSources() throws Exception {
		try (SingletonFixture fixture = new SingletonFixture()) {
			JDBCDataSource otherSource = nativeSource();
			Database other = Database.with(otherSource, DatabaseType.GENERIC).build();
			fixture.database.transaction(() -> {
				fixture.insert(1);
				other.transaction(() -> {
					assertEquals(7, other.query("VALUES (7)").fetchObject(Integer.class).orElseThrow());
					TransactionOutcome<Void> rejected = fixture.database.transactionWithOutcome(() -> { fixture.insert(2); });
					assertOwnershipFailure(rejected.getFailure().orElseThrow());
					assertEquals(TransactionResult.ROLLED_BACK, rejected.getResult());
					assertFalse(other.currentTransaction().orElseThrow().isRollbackOnly());
				});
				assertEquals(0, fixture.closes.get());
				fixture.insert(3);
			});
			assertEquals(List.of(1, 3), fixture.ids());
		}
	}

	@Test
	void aLazyOuterBoundaryCanAcquireTheHandleAfterAnIndependentInnerLeaseHasFinished() throws Exception {
		try (SingletonFixture fixture = new SingletonFixture()) {
			TransactionOutcome<Void> outer = fixture.database.transactionWithOutcome(() -> {
				assertEquals(0, fixture.checkouts.get());
				fixture.database.transaction(() -> { fixture.insert(2); });
				assertEquals(1, fixture.closes.get());
				fixture.insert(3);
				fixture.database.currentTransaction().orElseThrow().setRollbackOnly(true);
			});
			assertEquals(TransactionResult.ROLLED_BACK, outer.getResult());
			assertTrue(outer.getFailure().isEmpty());
			assertEquals(List.of(2), fixture.ids(), "Non-overlapping independent physical leases remain valid");
			assertEquals(0, fixture.acquisitionFailures.get());
			assertEquals(2, fixture.closes.get());
		}
	}

	@Test
	void legitimateDistinctConnectionsStillCommitIndependentInnerWork() {
		JDBCDataSource source = nativeSource();
		Database database = Database.with(source, DatabaseType.GENERIC).build();
		database.query("CREATE TABLE outer_work (id INTEGER PRIMARY KEY)").execute();
		database.query("CREATE TABLE inner_work (id INTEGER PRIMARY KEY)").execute();
		TransactionOutcome<Void> outer = database.transactionWithOutcome(() -> {
			database.query("INSERT INTO outer_work VALUES (1)").execute();
			TransactionOutcome<Void> inner = database.transactionWithOutcome(() -> {
				database.query("INSERT INTO inner_work VALUES (2)").execute();
			});
			assertEquals(TransactionResult.COMMITTED, inner.getResult());
			assertTrue(inner.getFailure().isEmpty());
			database.currentTransaction().orElseThrow().setRollbackOnly(true);
		});
		assertEquals(TransactionResult.ROLLED_BACK, outer.getResult());
		assertTrue(outer.getFailure().isEmpty());
		assertEquals(List.of(), database.query("SELECT id FROM outer_work").fetchList(Integer.class));
		assertEquals(List.of(2), database.query("SELECT id FROM inner_work").fetchList(Integer.class));
	}

	private static void assertOwnershipFailure(Throwable failure) {
		assertInstanceOf(DatabaseException.class, failure);
		assertTrue(failure.getMessage().contains("already owned by an enclosing transaction"), failure::getMessage);
	}

	private static JDBCDataSource nativeSource() {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:connection_ownership_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		return source;
	}

	private static final class SingletonFixture implements AutoCloseable {
		private final Connection physical;
		private final Database inspection;
		private final Database database;
		private final AtomicInteger checkouts = new AtomicInteger(), closes = new AtomicInteger(), aborts = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger(), stateCalls = new AtomicInteger();
		private final AtomicInteger acquisitionFailures = new AtomicInteger();
		private final List<MetricsCollector.PhysicalTransactionBeginFailurePhase> beginFailures = new ArrayList<>();

		private SingletonFixture() throws Exception {
			JDBCDataSource nativeSource = nativeSource();
			this.inspection = Database.with(nativeSource, DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work (id INTEGER PRIMARY KEY)").execute();
			this.physical = nativeSource.getConnection();
			Connection reused = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> {
						switch (method.getName()) {
							case "close" -> { this.closes.incrementAndGet(); return null; }
							case "abort" -> { this.aborts.incrementAndGet(); throw new AssertionError("An enclosing connection cannot be aborted"); }
							case "equals", "hashCode", "unwrap", "isWrapperFor" -> throw new AssertionError("Lease detection must use exact identity only");
							case "commit" -> this.commits.incrementAndGet();
							case "rollback" -> this.rollbacks.incrementAndGet();
							case "getAutoCommit", "setAutoCommit", "getTransactionIsolation", "setTransactionIsolation", "isReadOnly", "setReadOnly" -> this.stateCalls.incrementAndGet();
						}
						try { return method.invoke(this.physical, args); }
						catch (InvocationTargetException failure) { throw failure.getCause(); }
					});
			DataSource singleton = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> {
						if (method.getName().equals("getConnection")) { this.checkouts.incrementAndGet(); return reused; }
						throw new AssertionError(method.getName());
					});
			this.database = Database.with(singleton, DatabaseType.GENERIC).metricsCollector(new MetricsCollector() {
				@Override public void didFailToAcquireTransactionConnection(Transaction transaction, DatabaseType type,
						java.time.Duration duration, Throwable failure) { acquisitionFailures.incrementAndGet(); }
				@Override public void didFailToBeginPhysicalTransaction(Transaction transaction, TransactionIsolation isolation,
						PhysicalTransactionBeginFailurePhase phase, DatabaseType type, Throwable failure) { beginFailures.add(phase); }
			}).build();
		}

		private void insert(int id) { this.database.query("INSERT INTO work VALUES (:id)").bind("id", id).execute(); }
		private List<Integer> ids() { return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class); }
		@Override public void close() throws Exception { this.physical.close(); }
	}
}
