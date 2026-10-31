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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Savepoint;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class SavepointCleanupCompletionTests {
	@Test
	void terminalReleaseRollbackWhileCompletionWaitsPreventsCommit() throws Exception {
		for (boolean typed : new boolean[]{true, false}) {
			SQLException rollback = new SQLTransactionRollbackException("server rolled back", "40001");
			SQLException failure = typed ? optional(rollback) : rollback;
			Fixture fixture = new Fixture("releaseSavepoint", failure, true);

			fixture.run();

			fixture.assertRolledBack();
			assertTrue(contains(fixture.body, failure));
		}
	}

	@Test
	void releaseCancellationWhileCompletionWaitsMarksRollbackOnlyWithoutPublishingInterrupt() throws Exception {
		for (boolean typed : new boolean[]{true, false}) {
			SQLException cancellation = new SQLException("cancelled release", "57014", new InterruptedException("cancelled"));
			Fixture fixture = new Fixture("releaseSavepoint", typed ? optional(cancellation) : cancellation, false);

			fixture.run();

			fixture.assertRolledBack();
			assertTrue(contains(fixture.body, cancellation));
			assertFalse(fixture.workerInterrupted.get(), "Handled cleanup cancellation retains its publication boundary");
			assertFalse(Thread.currentThread().isInterrupted());
		}
	}

	@Test
	void failedSavepointRollbackWhileCompletionWaitsPreventsCommit() throws Exception {
		SQLException failure = new SQLTransactionRollbackException("server rolled back", "40001");
		Fixture fixture = new Fixture("rollback", failure, true);

		fixture.run();

		fixture.assertRolledBack();
		assertTrue(contains(fixture.body, failure));
		assertEquals(0, fixture.releases.get(), "Completion has closed admission before the later release attempt");
	}

	@Test
	void benignUnsupportedReleaseWhileCompletionWaitsKeepsOuterWork() throws Exception {
		Fixture fixture = new Fixture("releaseSavepoint", new SQLFeatureNotSupportedException("optional release", "0A000"), false);

		fixture.run();

		assertEquals(List.of(0), fixture.ids());
		assertFalse(fixture.rollbackOnlyInside.get());
		assertEquals(1, fixture.commits.get());
		assertEquals(0, fixture.fullRollbacks.get());
		assertEquals(TransactionResult.COMMITTED, fixture.completion.get().getResult());
		assertArrayEquals(new Throwable[0], fixture.body.getSuppressed());
		fixture.assertClosed();
	}

	private static SQLException optional(SQLException terminal) {
		SQLException failure = new SQLFeatureNotSupportedException("optional release", "0A000");
		failure.setNextException(terminal);
		return failure;
	}

	private static boolean contains(Throwable failure, Throwable expected) {
		return ThrowableGraph.inspect(failure, node -> node == expected ? 1 : 0, 0) != 0;
	}

	private static void await(CountDownLatch latch) throws InterruptedException {
		assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for completion race barrier");
	}

	private static final class Fixture {
		private final Database database, inspection;
		private final String faultOperation;
		private final SQLException cleanupFailure;
		private final boolean physicalRollback;
		private final IllegalArgumentException body = new IllegalArgumentException("handled savepoint body");
		private final CountDownLatch cleanupEntered = new CountDownLatch(1), finishCleanup = new CountDownLatch(1);
		private final AtomicReference<Transaction> transaction = new AtomicReference<>();
		private final AtomicReference<Savepoint> manualSavepoint = new AtomicReference<>();
		private final AtomicReference<Throwable> workerFailure = new AtomicReference<>(), controllerFailure = new AtomicReference<>();
		private final AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
		private final AtomicReference<Thread> worker = new AtomicReference<>();
		private final AtomicInteger commits = new AtomicInteger(), fullRollbacks = new AtomicInteger(), releases = new AtomicInteger();
		private final AtomicInteger connections = new AtomicInteger(), closes = new AtomicInteger(), statementCloses = new AtomicInteger();
		private final AtomicBoolean rollbackOnlyInside = new AtomicBoolean(), workerInterrupted = new AtomicBoolean();

		private Fixture(String faultOperation, SQLException cleanupFailure, boolean physicalRollback) {
			this.faultOperation = faultOperation;
			this.cleanupFailure = cleanupFailure;
			this.physicalRollback = physicalRollback;
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:savepoint_completion_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			this.inspection = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object value = invoke(source, method, args);
				if (!(value instanceof Connection connection)) return value;
				this.connections.incrementAndGet();
				return proxy(Connection.class, (operation, parameters) -> {
					String name = operation.getName();
					boolean savepointRollback = name.equals("rollback") && parameters != null && parameters.length == 1;
					if (name.equals("releaseSavepoint")) this.releases.incrementAndGet();
					if (name.equals(this.faultOperation) && (name.equals("releaseSavepoint") || savepointRollback)) {
						this.cleanupEntered.countDown();
						await(this.finishCleanup);
						if (this.physicalRollback) connection.rollback();
						throw this.cleanupFailure;
					}
					if (name.equals("commit")) this.commits.incrementAndGet();
					if (name.equals("rollback") && !savepointRollback) this.fullRollbacks.incrementAndGet();
					if (name.equals("close")) this.closes.incrementAndGet();
					Object result = invoke(connection, operation, parameters);
					if (!(result instanceof PreparedStatement statement)) return result;
					return proxy(PreparedStatement.class, (statementMethod, statementArgs) -> {
						if (statementMethod.getName().equals("close")) this.statementCloses.incrementAndGet();
						return invoke(statement, statementMethod, statementArgs);
					});
				});
			});
			this.database = Database.withDataSource(wrapped).databaseType(DatabaseType.GENERIC).build();
		}

		private void run() throws Exception {
			Thread controller = new Thread(() -> {
				try {
					await(this.cleanupEntered);
					long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
					boolean admissionClosed = false;
					while (System.nanoTime() < deadline) {
						try { this.database.participate(this.transaction.get(), () -> {}); }
						catch (IllegalStateException expected) { admissionClosed = true; break; }
						Thread.onSpinWait();
					}
					assertTrue(admissionClosed, "Owner must close admission while the participant still holds its lease");
					// The internal cleanup fix must not reopen late public mutation of the transaction.
					assertThrows(IllegalStateException.class, () -> this.transaction.get().setRollbackOnly(true));
					assertThrows(IllegalStateException.class, () -> this.transaction.get().rollback(this.manualSavepoint.get()));
					assertFalse(this.transaction.get().isRollbackOnly());
				} catch (Throwable failure) { this.controllerFailure.set(failure); }
				finally { this.finishCleanup.countDown(); }
			}, "savepoint-completion-controller");
			controller.setDaemon(true);
			controller.start();
			TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(this.completion::set).build();
			try {
				this.database.transaction(options, () -> {
					Transaction current = this.database.currentTransaction().orElseThrow();
					this.transaction.set(current);
					this.database.query("INSERT INTO work VALUES (0)").execute();
					this.manualSavepoint.set(current.createSavepoint());
					Thread participant = new Thread(() -> {
						try {
							this.database.participate(current, () -> this.database.useRawConnection(connection -> {
								// A real public raw lease remains held until the scope has cleared its ownership.
								try {
									current.withSavepoint(() -> { this.database.query("INSERT INTO work VALUES (1)").execute(); throw this.body; });
								} catch (IllegalArgumentException failure) {
									assertSame(this.body, failure);
									this.rollbackOnlyInside.set(current.isRollbackOnly());
								}
								return Optional.empty();
							}));
						} catch (Throwable failure) { this.workerFailure.set(failure); }
						finally { this.workerInterrupted.set(Thread.currentThread().isInterrupted()); }
					}, "savepoint-completion-participant");
					participant.setDaemon(true);
					this.worker.set(participant);
					participant.start();
					await(this.cleanupEntered);
				});
			} finally {
				this.finishCleanup.countDown();
				controller.join(5_000);
				if (this.worker.get() != null) this.worker.get().join(5_000);
			}
			assertFalse(controller.isAlive(), "Controller leaked");
			assertNotNull(this.worker.get());
			assertFalse(this.worker.get().isAlive(), "Participant leaked");
			assertNull(this.controllerFailure.get());
			assertNull(this.workerFailure.get());
		}

		private List<Integer> ids() { return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class); }

		private void assertRolledBack() {
			assertEquals(List.of(), ids());
			assertTrue(this.rollbackOnlyInside.get());
			assertEquals(0, this.commits.get());
			assertEquals(1, this.fullRollbacks.get());
			assertEquals(TransactionResult.ROLLED_BACK, this.completion.get().getResult());
			assertClosed();
		}

		private void assertClosed() {
			assertEquals(1, this.connections.get());
			assertEquals(1, this.closes.get());
			assertEquals(2, this.statementCloses.get());
			assertTrue(this.database.currentTransaction().isEmpty());
		}
	}

	@FunctionalInterface
	private interface Operation { Object apply(Method method, Object[] args) throws Throwable; }

	private static <T> T proxy(Class<T> type, Operation operation) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> operation.apply(method, args)));
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
