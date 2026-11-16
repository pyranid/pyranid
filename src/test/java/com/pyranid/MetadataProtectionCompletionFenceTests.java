/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.TransactionState;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MetadataProtectionCompletionFenceTests {
	@AfterEach void clearInterruption() { Thread.interrupted(); }

	@Test
	void uncertaintyRefusesCompletionEvenWhenTheDriverReportsAUsableOrIdleTransaction() {
		for (TransactionState state : List.of(TransactionState.OPEN, TransactionState.IDLE)) {
			Fixture fixture = new Fixture(); fixture.state.set(state);
			SQLException original = new SQLException("uncertain internal metadata cleanup", "HY000");
			TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(() -> {
				fixture.insert(1);
				Transaction transaction = fixture.transaction();
				fixture.database.useRawConnection(raw -> {
					Connection actual = transaction.getExistingConnection().orElseThrow();
					actual.rollback(); // Model control failure after surrounding work has already been discarded.
					Transaction.recordMetadataProtectionFailure(actual, original);
					return Optional.empty();
				});
				assertFalse(transaction.isRollbackOnly(), "An internal refusal is distinct from application rollback-only");
				fixture.insert(2);
				return Optional.of("attempted receipt");
			});
			assertRefusal(outcome, original);
			assertEquals(Optional.of("attempted receipt"), outcome.getAttemptedValue());
			assertEquals(0, fixture.commits.get()); assertEquals(2, fixture.rollbacks.get());
			assertEquals(List.of(0), fixture.ids());
		}
	}

	@Test
	void rollbackOnlyRemainsIntentionalButCannotHideOrClearAnInternalRefusal() {
		Fixture intentional = new Fixture();
		TransactionOutcome<Void> clean = intentional.database.transactionWithOutcome(() -> {
			intentional.insert(1); intentional.transaction().setRollbackOnly(true);
		});
		assertEquals(TransactionResult.ROLLED_BACK, clean.getResult()); assertTrue(clean.getFailure().isEmpty());
		for (boolean clearRollbackOnly : new boolean[]{false, true}) {
			Fixture fixture = new Fixture(); SQLException original = new SQLException("uncertain savepoint", "HY000");
			TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
				fixture.insert(1); Transaction transaction = fixture.transaction();
				record(transaction, original, true); transaction.setRollbackOnly(true);
				if (clearRollbackOnly) transaction.setRollbackOnly(false);
			});
			assertRefusal(outcome, original); assertEquals(0, fixture.commits.get()); assertEquals(List.of(0), fixture.ids());
		}
	}

	@Test
	void successfulApplicationSavepointRecoveryDoesNotEraseUncertainControlFailure() {
		Fixture fixture = new Fixture(); SQLException original = new SQLException("uncertain rollback or release", "HY000");
		IllegalStateException body = new IllegalStateException("recover application scope");
		TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(1); Transaction transaction = fixture.transaction();
			assertSame(body, assertThrows(IllegalStateException.class, () -> transaction.withSavepoint(() -> {
				fixture.insert(2); record(transaction, original, true); throw body;
			})));
			fixture.insert(3);
		});
		assertRefusal(outcome, original); assertEquals(List.of(0), fixture.ids()); assertEquals(0, fixture.commits.get());
	}

	@Test
	void aProvenRejectionUsesOrdinaryEvidenceAndAllowsAcknowledgedSavepointRecovery() {
		for (String sqlState : List.of("21000", "U0001", "57014", "40001")) {
			Fixture fixture = new Fixture(); SQLException original = new SQLException("rejected aborted operation", sqlState);
			IllegalStateException body = new IllegalStateException("scope failure");
			TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
				fixture.insert(1); Transaction transaction = fixture.transaction();
				assertSame(body, assertThrows(IllegalStateException.class, () -> transaction.withSavepoint(() -> {
					fixture.insert(2); record(transaction, original, false); throw body;
				})));
				fixture.insert(3);
			});
			assertEquals(TransactionResult.COMMITTED, outcome.getResult()); assertTrue(outcome.getFailure().isEmpty());
			assertEquals(List.of(0, 1, 3), fixture.ids()); assertEquals(1, fixture.commits.get());
		}
	}

	@Test
	void anUnrecoveredRejectionRetainsItsOriginalClassificationForPrecommitRefusal() {
		Fixture fixture = new Fixture(); SQLException original = new SQLException("custom business state", "U0001");
		TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(1); record(fixture.transaction(), original, false); fixture.state.set(TransactionState.FAILED);
		});
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		DatabaseException failure = assertInstanceOf(DatabaseException.class, outcome.getFailure().orElseThrow());
		assertEquals(Optional.of("U0001"), failure.getSqlState()); assertTrue(contains(failure, original));
		assertEquals(0, fixture.commits.get()); assertEquals(List.of(0), fixture.ids());
	}

	@Test
	void aNestedIndependentBoundaryCanFenceOnlyTheMatchingOuterPhysicalOwner() {
		Fixture fixture = new Fixture(); SQLException original = new SQLException("outer metadata cleanup", "HY000");
		TransactionOutcome<Void> outer = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(1); Transaction owner = fixture.transaction();
			fixture.database.useRawConnection(raw -> {
				Connection actual = owner.getExistingConnection().orElseThrow();
				TransactionOutcome<Void> inner = fixture.database.transactionWithOutcome(() -> {
					Transaction nested = fixture.transaction(); nested.getConnection();
					Transaction.recordMetadataProtectionFailure(actual, original);
					assertFalse(nested.isRollbackOnly());
				});
				assertEquals(TransactionResult.COMMITTED, inner.getResult()); assertTrue(inner.getFailure().isEmpty());
				return Optional.empty();
			});
			fixture.insert(3);
		});
		assertRefusal(outer, original); assertEquals(1, fixture.commits.get()); assertEquals(List.of(0), fixture.ids());
	}

	@Test
	void foreignAndUnlockedConnectionHandlesCannotFenceAnAmbientOwner() throws Exception {
		Fixture fixture = new Fixture(); SQLException original = new SQLException("foreign operation", "HY000");
		try (Connection foreign = fixture.source.getConnection()) {
			TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
				fixture.insert(1); Transaction transaction = fixture.transaction();
				Connection actual = transaction.getExistingConnection().orElseThrow();
				Transaction.recordMetadataProtectionFailure(actual, original); // No owner JDBC lock is held here.
				transaction.getConnectionLock().lock();
				try { Transaction.recordMetadataProtectionFailure(foreign, original); }
				finally { transaction.getConnectionLock().unlock(); }
			});
			assertEquals(TransactionResult.COMMITTED, outcome.getResult()); assertTrue(outcome.getFailure().isEmpty());
			assertEquals(List.of(0, 1), fixture.ids());
		}
	}

	@Test
	void theFirstUncertainFailureIsRetainedAndCompletedTransactionsReleaseIt() throws Exception {
		Fixture fixture = new Fixture(); SQLException first = new SQLException("first uncertainty", "HY000");
		SQLException later = new SQLException("later uncertainty", "08006"); AtomicReference<Transaction> completed = new AtomicReference<>();
		TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(1); Transaction transaction = fixture.transaction(); completed.set(transaction);
			record(transaction, first, true); record(transaction, later, true);
		});
		assertRefusal(outcome, first); assertFalse(contains(outcome.getFailure().orElseThrow(), later));
		Field field = Transaction.class.getDeclaredField("metadataProtectionFailure"); field.setAccessible(true);
		assertNull(field.get(completed.get()), "A retained completed Transaction must not retain JDBC failure graphs");
	}

	@Test
	void fatalAndInterruptedFailureEvidenceKeepsItsIdentityAndPublicationBoundary() {
		Fixture fatal = new Fixture(); AssertionError original = new AssertionError("fatal internal control failure");
		assertSame(original, assertThrows(AssertionError.class, () -> fatal.database.transactionWithOutcome(() -> {
			fatal.insert(1); record(fatal.transaction(), original, true);
		})));
		assertEquals(0, fatal.commits.get()); assertEquals(List.of(0), fatal.ids());
		Fixture interrupted = new Fixture(); InterruptedException cause = new InterruptedException("cancelled metadata cleanup");
		SQLException failure = new SQLException("cancelled cleanup", "57014", cause);
		TransactionOutcome<Void> outcome = interrupted.database.transactionWithOutcome(() -> {
			interrupted.insert(1); record(interrupted.transaction(), failure, true);
			assertFalse(Thread.currentThread().isInterrupted());
		});
		assertRefusal(outcome, failure); assertTrue(contains(outcome.getFailure().orElseThrow(), cause));
		assertTrue(Thread.interrupted(), "Cancellation is published at the unenclosed completion boundary");
		Fixture enclosed = new Fixture();
		enclosed.database.transaction(() -> {
			enclosed.insert(1);
			TransactionOutcome<Void> inner = enclosed.database.transactionWithOutcome(() -> record(enclosed.transaction(), failure, true));
			assertRefusal(inner, failure);
			assertFalse(Thread.currentThread().isInterrupted(), "A handled inner outcome must not interrupt surviving outer work");
			enclosed.insert(3);
		});
		assertFalse(Thread.currentThread().isInterrupted()); assertEquals(List.of(0, 1, 3), enclosed.ids());
	}

	@Test
	void admittedMetadataCleanupCanFenceCompletionWhileItsOwnerWaitsForTheJdbcLease() throws Exception {
		Fixture fixture = new Fixture(); SQLException original = new SQLException("late uncertain metadata release", "HY000");
		CountDownLatch workerEntered = new CountDownLatch(1), finishWorker = new CountDownLatch(1);
		AtomicReference<Transaction> transaction = new AtomicReference<>(); AtomicReference<TransactionOutcome<Void>> outcome = new AtomicReference<>();
		AtomicReference<Throwable> ownerFailure = new AtomicReference<>(), workerFailure = new AtomicReference<>();
		AtomicReference<Thread> workerThread = new AtomicReference<>();
		Thread owner = new Thread(() -> {
			try { outcome.set(fixture.database.transactionWithOutcome(() -> {
				fixture.insert(1); Transaction tx = fixture.transaction(); transaction.set(tx);
				Thread worker = new Thread(() -> {
					try { fixture.database.participate(tx, () -> fixture.database.useRawConnection(raw -> {
						Connection actual = tx.getExistingConnection().orElseThrow(); workerEntered.countDown(); await(finishWorker);
						Transaction.recordMetadataProtectionFailure(actual, original); return Optional.empty();
					})); } catch (Throwable failure) { workerFailure.set(failure); }
				});
				worker.setDaemon(true); workerThread.set(worker); worker.start(); await(workerEntered);
			})); } catch (Throwable failure) { ownerFailure.set(failure); }
		});
		owner.setDaemon(true); owner.start(); await(workerEntered);
		try {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (!transaction.get().getConnectionLock().hasQueuedThread(owner) && System.nanoTime() < deadline) Thread.sleep(1);
			assertTrue(transaction.get().getConnectionLock().hasQueuedThread(owner), "Completion must wait for the admitted JDBC lease");
		} finally { finishWorker.countDown(); owner.join(5_000); workerThread.get().join(5_000); }
		assertFalse(owner.isAlive()); assertFalse(workerThread.get().isAlive()); assertNull(ownerFailure.get()); assertNull(workerFailure.get());
		assertRefusal(outcome.get(), original); assertEquals(0, fixture.commits.get()); assertEquals(List.of(0), fixture.ids());
	}

	private static void record(Transaction transaction, Throwable failure, boolean uncertain) {
		transaction.getConnectionLock().lock();
		try {
			Connection actual = transaction.getConnection();
			if (uncertain) Transaction.recordMetadataProtectionFailure(actual, failure);
			else Transaction.recordMetadataProtectionRejection(actual, failure);
		} finally { transaction.getConnectionLock().unlock(); }
	}

	private static void assertRefusal(TransactionOutcome<?> outcome, Throwable original) {
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		DatabaseException failure = assertInstanceOf(DatabaseException.class, outcome.getFailure().orElseThrow());
		assertTrue(failure.getMessage().contains("uncertain internal parameter-metadata protection"), failure.getMessage());
		assertTrue(contains(failure, original));
	}
	private static boolean contains(Throwable failure, Throwable expected) {
		return ThrowableGraph.inspect(failure, node -> node == expected ? 1 : 0, 0) != 0;
	}
	private static void await(CountDownLatch latch) throws InterruptedException {
		assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for transaction fence barrier");
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); } catch (InvocationTargetException failure) { throw failure.getTargetException(); }
	}

	private static final class Fixture {
		final JDBCDataSource source = new JDBCDataSource();
		final Database database, inspection;
		final AtomicReference<TransactionState> state = new AtomicReference<>(TransactionState.OPEN);
		final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger();
		Fixture() {
			source.setUrl("jdbc:hsqldb:mem:metadata_completion_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
			inspection = Database.with(source, DatabaseType.GENERIC).build();
			inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute(); inspection.query("INSERT INTO work VALUES (0)").execute();
			DataSource wrapped = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> {
						Object value = invoke(source, method, args);
						if (!(value instanceof Connection actual)) return value;
						return Proxy.newProxyInstance(BaseConnection.class.getClassLoader(), new Class<?>[]{BaseConnection.class},
								(handle, call, parameters) -> switch (call.getName()) {
									case "getTransactionState" -> state.get();
									case "unwrap" -> ((Class<?>) parameters[0]).isInstance(handle) ? handle : invoke(actual, call, parameters);
									case "isWrapperFor" -> ((Class<?>) parameters[0]).isInstance(handle) || actual.isWrapperFor((Class<?>) parameters[0]);
									case "commit" -> { commits.incrementAndGet(); yield invoke(actual, call, parameters); }
									case "rollback" -> { if (parameters == null || parameters.length == 0) rollbacks.incrementAndGet(); yield invoke(actual, call, parameters); }
									default -> invoke(actual, call, parameters);
								});
					});
			database = Database.with(wrapped, DatabaseType.POSTGRESQL).build();
		}
		void insert(int id) { database.query("INSERT INTO work VALUES (" + id + ")").execute(); }
		Transaction transaction() { return database.currentTransaction().orElseThrow(); }
		List<Integer> ids() { return inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class); }
	}
}
