/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Savepoint;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionCompletionGuardTests {
	@Test
	public void closureReturnAndFailureClosePublicAdmissionBeforeJdbcCompletion() throws Exception {
		for (String mode : List.of("commit", "throw", "rollbackOnly")) {
			CountDownLatch completionEntered = new CountDownLatch(1), allowCompletion = new CountDownLatch(1);
			Fixture fixture = fixture((connection, method, args) -> {
				if (method.getName().equals(mode.equals("commit") ? "commit" : "rollback")) {
					completionEntered.countDown(); await(allowCompletion);
				}
				return invoke(connection, method, args);
			});
			AtomicReference<Transaction> transaction = new AtomicReference<>();
			AtomicReference<Savepoint> savepoint = new AtomicReference<>();
			AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
			AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
			AtomicBoolean hookCalled = new AtomicBoolean();
			Consumer<TransactionResult> hook = result -> hookCalled.set(true);
			IllegalStateException expected = new IllegalStateException("body failed");
			Thread owner = new Thread(() -> {
				try {
					fixture.database.transaction(TransactionOptions.withReadOnly(null).completionObserver(completion::set).build(), () -> {
						Transaction tx = fixture.database.currentTransaction().orElseThrow(); transaction.set(tx);
						savepoint.set(tx.createSavepoint()); tx.addPostTransactionOperation(hook);
						fixture.database.query("INSERT INTO events VALUES (1)").execute();
						if (mode.equals("throw")) throw expected;
						if (mode.equals("rollbackOnly")) tx.setRollbackOnly(true);
					});
				} catch (Throwable failure) { ownerFailure.set(failure); }
			});
			owner.setDaemon(true); owner.start();
			try {
				await(completionEntered);
				Transaction tx = transaction.get();
				assertThrows(IllegalStateException.class, () -> tx.setRollbackOnly(!mode.equals("rollbackOnly")));
				assertThrows(IllegalStateException.class, () -> tx.addPostTransactionOperation(result -> fail("late hook")));
				assertThrows(IllegalStateException.class, () -> tx.removePostTransactionOperation(hook));
				assertThrows(IllegalStateException.class, tx::createSavepoint);
				assertThrows(IllegalStateException.class, () -> tx.rollback(savepoint.get()));
				assertThrows(IllegalStateException.class, () -> tx.releaseSavepoint(savepoint.get()));
				assertThrows(IllegalStateException.class, () -> tx.withSavepoint(() -> fail("late scope")));
				assertThrows(IllegalStateException.class, tx::getConnection);
				assertThrows(IllegalStateException.class, () -> fixture.database.participate(tx, () -> fail("late participant")));
				assertEquals(mode.equals("rollbackOnly"), tx.isRollbackOnly(), "Rejected calls must not mutate transaction state");
			} finally { allowCompletion.countDown(); owner.join(5_000); }
			assertFalse(owner.isAlive()); assertTrue(hookCalled.get());
			if (mode.equals("throw")) assertSame(expected, ownerFailure.get()); else assertNull(ownerFailure.get());
			assertEquals(mode.equals("commit") ? TransactionResult.COMMITTED : TransactionResult.ROLLED_BACK, completion.get().getResult());
			assertEquals(mode.equals("commit") ? 1L : 0L,
					fixture.setup.query("SELECT COUNT(*) FROM events").fetchObject(Long.class).orElseThrow());
		}
	}

	@Test
	public void metadataMutationsDoNotWaitForWorkerHoldingJdbcConnection() {
		Fixture fixture = fixture(TransactionCompletionGuardTests::invoke);
		AtomicBoolean hookCalled = new AtomicBoolean();
		fixture.database.transaction(() -> {
			Transaction tx = fixture.database.currentTransaction().orElseThrow();
			CountDownLatch workerEntered = new CountDownLatch(1), allowWorker = new CountDownLatch(1);
			AtomicReference<Throwable> workerFailure = new AtomicReference<>();
			Thread worker = new Thread(() -> {
				try { fixture.database.participate(tx, () -> fixture.database.useRawConnection(connection -> {
					workerEntered.countDown(); await(allowWorker); return Optional.empty();
				})); } catch (Throwable failure) { workerFailure.set(failure); }
			});
			worker.setDaemon(true); worker.start();
			try {
				await(workerEntered);
				tx.setRollbackOnly(true);
				Consumer<TransactionResult> removedHook = result -> fail("removed hook");
				tx.addPostTransactionOperation(removedHook); assertTrue(tx.removePostTransactionOperation(removedHook));
				tx.addPostTransactionOperation(result -> hookCalled.set(true));
			} finally { allowWorker.countDown(); worker.join(5_000); }
			assertFalse(worker.isAlive()); assertNull(workerFailure.get()); assertTrue(tx.isRollbackOnly());
		});
		assertTrue(hookCalled.get());
	}

	@Test
	public void queuedPublicJdbcAccessRechecksPhaseAfterAcquiringConnectionLock() throws Exception {
		java.util.concurrent.atomic.AtomicInteger savepointCalls = new java.util.concurrent.atomic.AtomicInteger();
		Fixture fixture = fixture((connection, method, args) -> {
			if (method.getName().equals("setSavepoint")) savepointCalls.incrementAndGet();
			return invoke(connection, method, args);
		});
		CountDownLatch bodyEntered = new CountDownLatch(1), returnFromBody = new CountDownLatch(1);
		AtomicReference<Transaction> transaction = new AtomicReference<>();
		AtomicReference<Throwable> ownerFailure = new AtomicReference<>(), waiterFailure = new AtomicReference<>();
		Thread owner = new Thread(() -> {
			try { fixture.database.transaction(() -> {
				Transaction tx = fixture.database.currentTransaction().orElseThrow(); tx.getConnection(); transaction.set(tx);
				bodyEntered.countDown(); await(returnFromBody);
			}); } catch (Throwable failure) { ownerFailure.set(failure); }
		});
		owner.setDaemon(true); owner.start(); await(bodyEntered);
		Transaction tx = transaction.get();
		Thread waiter = new Thread(() -> {
			try { tx.createSavepoint(); } catch (Throwable failure) { waiterFailure.set(failure); }
		});
		waiter.setDaemon(true);
		tx.getConnectionLock().lock();
		try {
			waiter.start(); awaitQueued(tx, waiter);
			returnFromBody.countDown(); awaitQueued(tx, owner);
			assertThrows(IllegalStateException.class, () -> tx.setRollbackOnly(true),
					"Completion admission closes before waiting for the JDBC lock");
		} finally { returnFromBody.countDown(); tx.getConnectionLock().unlock(); }
		owner.join(5_000); waiter.join(5_000);
		assertFalse(owner.isAlive()); assertFalse(waiter.isAlive()); assertNull(ownerFailure.get());
		assertInstanceOf(IllegalStateException.class, waiterFailure.get()); assertEquals(0, savepointCalls.get());
	}

	@Test
	public void hookEqualityRunsWithoutHoldingLifecycleLock() {
		Fixture fixture = fixture(TransactionCompletionGuardTests::invoke);
		fixture.database.transaction(() -> {
			Transaction transaction = fixture.database.currentTransaction().orElseThrow();
			Consumer<TransactionResult> stored = result -> fail("removed hook");
			transaction.addPostTransactionOperation(stored);
			Consumer<TransactionResult> matching = new Consumer<>() {
				@Override public void accept(TransactionResult result) {}
				@Override public boolean equals(Object other) {
					AtomicReference<Throwable> failure = new AtomicReference<>();
					Thread worker = new Thread(() -> {
						try { transaction.setRollbackOnly(true); } catch (Throwable throwable) { failure.set(throwable); }
					});
					worker.setDaemon(true); worker.start();
					try { worker.join(5_000); } catch (InterruptedException interrupted) {
						Thread.currentThread().interrupt(); throw new AssertionError(interrupted);
					}
					assertFalse(worker.isAlive(), "Consumer.equals must not hold the lifecycle lock"); assertNull(failure.get());
					return other == stored;
				}
			};
			assertTrue(transaction.removePostTransactionOperation(matching)); assertTrue(transaction.isRollbackOnly());
		});
	}

	private static void awaitQueued(Transaction transaction, Thread thread) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!transaction.getConnectionLock().hasQueuedThread(thread) && System.nanoTime() < deadline) Thread.sleep(1);
		assertTrue(transaction.getConnectionLock().hasQueuedThread(thread), "Thread did not queue for the connection lock");
	}

	private static void await(CountDownLatch latch) throws InterruptedException {
		assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for transaction boundary");
	}
	private static Fixture fixture(ConnectionInvocation invocation) {
		JDBCDataSource source = new JDBCDataSource(); source.setUrl("jdbc:hsqldb:mem:completion_guard_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		Database setup = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
		setup.query("CREATE TABLE events (id INT)").execute();
		DataSource wrapped = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> {
					Object value = invoke(source, method, args);
					if (value instanceof Connection connection)
						return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
								(connectionProxy, connectionMethod, connectionArgs) -> invocation.invoke(connection, connectionMethod, connectionArgs));
					return value;
				});
		return new Fixture(Database.withDataSource(wrapped).databaseType(DatabaseType.GENERIC).build(), setup);
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); } catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
	private record Fixture(Database database, Database setup) {}
	@FunctionalInterface private interface ConnectionInvocation { Object invoke(Connection connection, Method method, Object[] args) throws Throwable; }
}
