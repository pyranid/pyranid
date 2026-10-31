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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SavepointConcurrencyTests {
	@Test
	public void preexistingParticipantCannotLoseWritesToAnotherThreadsSavepoint() {
		Database database = database(dataSource());

		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			CountDownLatch participantEntered = new CountDownLatch(1);
			CountDownLatch attemptInsert = new CountDownLatch(1);
			CountDownLatch insertRejected = new CountDownLatch(1);
			CountDownLatch scopeFinished = new CountDownLatch(1);
			Worker worker = startWorker(() -> database.participate(transaction, () -> {
				participantEntered.countDown();
				await(attemptInsert);
				try {
					DatabaseException failure = Assertions.assertThrows(DatabaseException.class,
							() -> database.query("INSERT INTO events VALUES (2)").execute());
					Assertions.assertTrue(failure.getMessage().contains("active savepoint scope"));
				} finally {
					insertRejected.countDown();
				}
				await(scopeFinished);
				database.query("INSERT INTO events VALUES (2)").execute();
			}));

			Throwable bodyFailure = null;
			try {
				await(participantEntered);
				IllegalArgumentException expected = new IllegalArgumentException("recoverable scope failure");
				Assertions.assertSame(expected, Assertions.assertThrows(IllegalArgumentException.class,
						() -> transaction.withSavepoint(() -> {
							database.query("INSERT INTO events VALUES (1)").execute();
							attemptInsert.countDown();
							await(insertRejected);
							throw expected;
						})));
			} catch (Throwable throwable) {
				bodyFailure = throwable;
				throw throwable;
			} finally {
				attemptInsert.countDown();
				scopeFinished.countDown();
				finishIfStarted(worker, bodyFailure);
			}

			Assertions.assertFalse(transaction.isRollbackOnly());
			database.query("INSERT INTO events VALUES (3)").execute();
		});

		Assertions.assertEquals(List.of(2, 3), ids(database));
	}

	@Test
	public void scopedAndManualSavepointsAndEveryConnectionPathRejectCompetingThreads() {
		Database database = database(dataSource());

		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			Savepoint manualSavepoint = transaction.createSavepoint();
			transaction.withSavepoint(() -> {
				transaction.withSavepoint(() -> database.query("INSERT INTO events VALUES (1)").execute());

				// Finishing the nested scope must leave the outer scope's ownership intact.
				finish(startWorker(() -> database.participate(transaction, () -> {
					Assertions.assertThrows(IllegalStateException.class, () -> transaction.withSavepoint(() -> {
						Assertions.fail("Competing scoped savepoint callback must not run");
					}));
					Assertions.assertThrows(IllegalStateException.class, transaction::createSavepoint);
					Assertions.assertThrows(IllegalStateException.class, () -> transaction.releaseSavepoint(manualSavepoint));
					Assertions.assertThrows(IllegalStateException.class, () -> transaction.rollback(manualSavepoint));
					Assertions.assertThrows(DatabaseException.class, () -> database.useRawConnection(connection -> {
						Assertions.fail("Competing raw connection callback must not run");
						return Optional.empty();
					}));
					Assertions.assertThrows(DatabaseException.class, () -> database.query("SELECT id FROM events")
							.fetchStream(Integer.class, stream -> stream.toList()));
					Assertions.assertFalse(transaction.isRollbackOnly());
				})));
			});

			// Once the outer scope ends, another participant can acquire a scoped savepoint normally.
			finish(startWorker(() -> database.participate(transaction, () ->
					transaction.withSavepoint(() -> database.query("INSERT INTO events VALUES (2)").execute()))));
			transaction.releaseSavepoint(manualSavepoint);
		});

		Assertions.assertEquals(List.of(1, 2), ids(database));
	}

	@Test
	public void failedSavepointCreationReleasesOwnership() {
		AtomicBoolean failNextSavepoint = new AtomicBoolean(true);
		DataSource source = dataSource();
		DataSource failingSource = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
				new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> {
					Object result = invoke(method, source, arguments);
					if (!(result instanceof Connection connection))
						return result;
					return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
							(connectionProxy, connectionMethod, connectionArguments) -> {
								if (connectionMethod.getName().equals("setSavepoint") && failNextSavepoint.getAndSet(false))
									throw new SQLException("savepoint creation failed");
								return invoke(connectionMethod, connection, connectionArguments);
							});
				});
		Database database = database(failingSource);

		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			Assertions.assertThrows(DatabaseException.class, () -> transaction.withSavepoint(() -> {
				Assertions.fail("Callback must not run when savepoint creation fails");
			}));
			Assertions.assertFalse(transaction.isRollbackOnly());
			finish(startWorker(() -> database.participate(transaction, () ->
					transaction.withSavepoint(() -> database.query("INSERT INTO events VALUES (1)").execute()))));
		});

		Assertions.assertEquals(List.of(1), ids(database));
	}

	@Test
	public void escapedParticipationConflictRetainsRollbackOnlyContract() {
		Database database = database(dataSource());

		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			database.query("INSERT INTO events VALUES (1)").execute();
			transaction.withSavepoint(() -> finish(startWorker(() -> {
				Assertions.assertThrows(DatabaseException.class, () -> database.participate(transaction, () ->
						database.query("INSERT INTO events VALUES (2)").execute()));
			})));
			Assertions.assertTrue(transaction.isRollbackOnly());
		});

		Assertions.assertEquals(List.of(), ids(database));
	}

	@Test
	public void interruptedScopeEntryDoesNotRetainOwnership() {
		Database database = database(dataSource());

		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			transaction.getConnectionLock().lock();
			Worker worker = startWorker(() -> {
				DatabaseException failure = Assertions.assertThrows(DatabaseException.class,
						() -> transaction.withSavepoint(() -> Assertions.fail("Interrupted scope must not start")));
				Assertions.assertInstanceOf(InterruptedException.class, failure.getCause());
				Assertions.assertTrue(Thread.currentThread().isInterrupted());
			});

			try {
				long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
				while (!transaction.getConnectionLock().hasQueuedThread(worker.thread()) && System.nanoTime() < deadline)
					Thread.onSpinWait();
				Assertions.assertTrue(transaction.getConnectionLock().hasQueuedThread(worker.thread()));
				worker.thread().interrupt();
				finish(worker);
			} finally {
				transaction.getConnectionLock().unlock();
			}

			transaction.withSavepoint(() -> database.query("INSERT INTO events VALUES (1)").execute());
		});

		Assertions.assertEquals(List.of(1), ids(database));
	}

	@Test
	public void completionDuringAnotherThreadsScopeRollsBackAndReleasesTheConnection() throws InterruptedException {
		JDBCDataSource source = dataSource();
		Database setupDatabase = database(source);
		List<String> events = Collections.synchronizedList(new ArrayList<>());
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = Database.withDataSource(completionEventRecordingDataSource(source, events, false))
				.metricsCollector(metricsCollector)
				.build();
		List<TransactionResult> hookResults = Collections.synchronizedList(new ArrayList<>());
		AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
		AtomicReference<Transaction> completedTransaction = new AtomicReference<>();
		TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completion::set).build();
		CountDownLatch scopeEntered = new CountDownLatch(1);
		CountDownLatch completionFinished = new CountDownLatch(1);
		AtomicReference<Worker> worker = new AtomicReference<>();

		Throwable bodyFailure = null;
		try {
			IllegalStateException conflict = Assertions.assertThrows(IllegalStateException.class, () ->
					database.transaction(options, () -> {
						Transaction transaction = database.currentTransaction().orElseThrow();
						completedTransaction.set(transaction);
						transaction.addPostTransactionOperation(hookResults::add);
						database.query("INSERT INTO events VALUES (1)").execute();
						worker.set(startWorker(() -> database.participate(transaction, () -> {
							IllegalStateException late = Assertions.assertThrows(IllegalStateException.class,
									() -> transaction.withSavepoint(() -> {
										scopeEntered.countDown();
										// Hold the scope across the owner's completion so commit() meets the ownership guard.
										await(completionFinished);
									}));
							Assertions.assertTrue(late.getMessage().contains("already completed its closure"));
						})));
						await(scopeEntered);
					}));

			Assertions.assertTrue(conflict.getMessage().contains("active savepoint scope"));
			Assertions.assertEquals(0, conflict.getSuppressed().length);
			Assertions.assertSame(conflict, completion.get().getFailure().orElseThrow());
			Assertions.assertEquals(TransactionResult.ROLLED_BACK, completion.get().getResult());
			Assertions.assertEquals(List.of(TransactionResult.ROLLED_BACK), hookResults);
			Assertions.assertEquals(0, completedTransaction.get().retrySafety().outcomes());
			Assertions.assertFalse(events.contains("commit"));
			Assertions.assertFalse(events.contains("abort"));
			Assertions.assertTrue(events.contains("rollback"), "Rollback must be recorded; an absent event orders vacuously");
			Assertions.assertTrue(events.indexOf("rollback") < events.indexOf("restoreAutoCommit"));
			Assertions.assertTrue(events.indexOf("restoreAutoCommit") < events.lastIndexOf("close"));
			Assertions.assertEquals(List.of(), ids(setupDatabase));

			MetricsCollector.Snapshot snapshot = metricsCollector.snapshot().orElseThrow();
			Assertions.assertEquals(0L, snapshot.physicalTransactionsCommitFailed());
			Assertions.assertEquals(1L, snapshot.physicalTransactionsRolledBack());
			Assertions.assertEquals(1L, snapshot.transactionClosuresRolledBack());
			Assertions.assertEquals(0L, snapshot.transactionClosuresFailed());
		} catch (Throwable throwable) {
			bodyFailure = throwable;
			throw throwable;
		} finally {
			completionFinished.countDown();
			finishIfStarted(worker.get(), bodyFailure);
		}
	}

	@Test
	public void completionConflictWithFailedRollbackRemainsInDoubtAndDiscardsTheConnection() throws InterruptedException {
		JDBCDataSource source = dataSource();
		Database setupDatabase = database(source);
		List<String> events = Collections.synchronizedList(new ArrayList<>());
		Database database = Database.withDataSource(completionEventRecordingDataSource(source, events, true)).build();
		List<TransactionResult> hookResults = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch scopeEntered = new CountDownLatch(1);
		CountDownLatch completionFinished = new CountDownLatch(1);
		AtomicReference<Transaction> completedTransaction = new AtomicReference<>();
		AtomicReference<Worker> worker = new AtomicReference<>();

		Throwable bodyFailure = null;
		try {
			IllegalStateException conflict = Assertions.assertThrows(IllegalStateException.class, () ->
					database.transaction(() -> {
						Transaction transaction = database.currentTransaction().orElseThrow();
						completedTransaction.set(transaction);
						transaction.addPostTransactionOperation(hookResults::add);
						database.query("INSERT INTO events VALUES (1)").execute();
						worker.set(startWorker(() -> database.participate(transaction, () ->
								Assertions.assertThrows(IllegalStateException.class, () -> transaction.withSavepoint(() -> {
									scopeEntered.countDown();
									await(completionFinished);
								})))));
						await(scopeEntered);
					}));

			// Only the guard's own ordering is provable. An unconfirmed rollback still leaves the outcome unknown.
			Assertions.assertTrue(conflict.getMessage().contains("active savepoint scope"));
			Assertions.assertTrue(List.of(conflict.getSuppressed()).stream().anyMatch(suppressed ->
					"Unable to roll back transaction".equals(suppressed.getMessage())));
			Assertions.assertEquals(List.of(TransactionResult.IN_DOUBT), hookResults);
			Assertions.assertEquals(TransactionRetrySafety.IN_DOUBT, completedTransaction.get().retrySafety().outcomes());
			Assertions.assertFalse(events.contains("commit"));
			Assertions.assertTrue(events.contains("abort"), "Abort must be recorded; an absent event orders vacuously");
			Assertions.assertTrue(events.indexOf("abort") < events.lastIndexOf("close"));
			Assertions.assertEquals(List.of(), ids(setupDatabase));
		} catch (Throwable throwable) {
			bodyFailure = throwable;
			throw throwable;
		} finally {
			completionFinished.countDown();
			finishIfStarted(worker.get(), bodyFailure);
		}
	}

	@Test
	public void completionConflictDoesNotFenceAnEnclosingRetry() {
		Database database = database(dataSource());
		AtomicInteger attempts = new AtomicInteger();
		RetryPolicy retryPolicy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> true);

		TransactionRetryResult<Void> retryResult = database.transactionWithRetry(retryPolicy, () -> {
			if (attempts.incrementAndGet() > 1) {
				database.query("INSERT INTO events VALUES (2)").execute();
				return;
			}

			CountDownLatch scopeEntered = new CountDownLatch(1);
			CountDownLatch completionFinished = new CountDownLatch(1);
			AtomicReference<Worker> worker = new AtomicReference<>();

			Throwable bodyFailure = null;
			try {
				Assertions.assertThrows(IllegalStateException.class, () -> database.transaction(() -> {
					Transaction inner = database.currentTransaction().orElseThrow();
					database.query("INSERT INTO events VALUES (1)").execute();
					worker.set(startWorker(() -> database.participate(inner, () ->
							Assertions.assertThrows(IllegalStateException.class, () -> inner.withSavepoint(() -> {
								scopeEntered.countDown();
								await(completionFinished);
							})))));
					await(scopeEntered);
				}));
			} catch (Throwable throwable) {
				bodyFailure = throwable;
				throw throwable;
			} finally {
				completionFinished.countDown();
				finishIfStarted(worker.get(), bodyFailure);
			}

			// The inner conflict committed nothing, so it must not fence this attempt against replay.
			throw new DatabaseException("retryable", new SQLException("serialization failure", "40001"));
		});

		Assertions.assertEquals(2, attempts.get());
		Assertions.assertEquals(2, retryResult.getAttemptCount());
		Assertions.assertEquals(List.of(2), ids(database));
	}

	private static JDBCDataSource dataSource() {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:savepoint_concurrency_" + UUID.randomUUID());
		source.setUser("sa");
		source.setPassword("");
		return source;
	}

	private static Database database(DataSource source) {
		Database database = Database.withDataSource(source).build();
		database.query("CREATE TABLE events (id INT)").execute();
		return database;
	}

	private static List<Integer> ids(Database database) {
		return database.query("SELECT id FROM events ORDER BY id").fetchList(Integer.class);
	}

	private static void await(CountDownLatch latch) throws InterruptedException {
		Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for participant");
	}

	private static Worker startWorker(TransactionalOperation operation) {
		AtomicReference<Throwable> failure = new AtomicReference<>();
		Thread thread = new Thread(() -> {
			try {
				operation.perform();
			} catch (Throwable throwable) {
				failure.set(throwable);
			}
		});
		thread.setDaemon(true);
		thread.start();
		return new Worker(thread, failure);
	}

	// Called from a finally: a failure before the worker is published leaves nothing to join, and a participant failure
	// is suppressed onto the body failure already in flight rather than replacing the more informative one.
	private static void finishIfStarted(Worker worker,
																			@Nullable Throwable bodyFailure) throws InterruptedException {
		if (worker == null)
			return;

		try {
			finish(worker);
		} catch (AssertionError participantFailure) {
			if (bodyFailure == null)
				throw participantFailure;

			bodyFailure.addSuppressed(participantFailure);
		}
	}

	private static void finish(Worker worker) throws InterruptedException {
		worker.thread().join(5_000);
		if (worker.thread().isAlive()) {
			worker.thread().interrupt();
			Assertions.fail("Participant did not finish; savepoint scope must not hold the connection lock across its callback");
		}
		if (worker.failure().get() != null)
			throw new AssertionError("Participant failed", worker.failure().get());
	}

	private static Object invoke(Method method, Object target, Object[] arguments) throws Throwable {
		try {
			return method.invoke(target, arguments);
		} catch (InvocationTargetException exception) {
			throw exception.getCause();
		}
	}

	private static DataSource completionEventRecordingDataSource(DataSource source,
																															 List<String> events,
																															 boolean failRollback) {
		return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
				new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> {
					Object result = invoke(method, source, arguments);
					if (!(result instanceof Connection connection))
						return result;
					return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
							(connectionProxy, connectionMethod, connectionArguments) -> {
								String name = connectionMethod.getName();
								boolean noArguments = connectionMethod.getParameterCount() == 0;

								if (noArguments && name.equals("commit")) {
									events.add("commit");
								} else if (noArguments && name.equals("rollback")) {
									events.add("rollback");
									if (failRollback)
										throw new SQLException("rollback was not confirmed", "08006");
								} else if (name.equals("abort")) {
									events.add("abort");
								} else if (noArguments && name.equals("close")) {
									events.add("close");
								} else if (name.equals("setAutoCommit") && Boolean.TRUE.equals(connectionArguments[0])) {
									events.add("restoreAutoCommit");
								}

								return invoke(connectionMethod, connection, connectionArguments);
							});
				});
	}

	private record Worker(Thread thread, AtomicReference<Throwable> failure) {}
}
