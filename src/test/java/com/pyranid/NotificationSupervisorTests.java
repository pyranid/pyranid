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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Production lifecycle tests; clock, session and sleeper seams do not create JDBC connections. */
@Timeout(10)
public class NotificationSupervisorTests {
	private static final Database DATABASE = Database.withDataSource((DataSource) Proxy.newProxyInstance(
			NotificationSupervisorTests.class.getClassLoader(), new Class<?>[]{DataSource.class},
			(proxy, method, args) -> { throw new AssertionError("Unexpected data source call: " + method); }))
			.databaseType(DatabaseType.POSTGRESQL).build();
	private static final DatabaseException FAILURE = new DatabaseException("listener failed");

	@Test
	public void constructionIsLazyAndRequiresCompleteValidConfiguration() {
		Assertions.assertThrows(NullPointerException.class, () -> NotificationSupervisor.withDatabase(null));
		Assertions.assertThrows(IllegalStateException.class, () -> NotificationSupervisor.withDatabase(DATABASE).build());
		Assertions.assertThrows(IllegalStateException.class,
				() -> NotificationSupervisor.withDatabase(DATABASE).channels(Set.of("job_ready")).build());
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder().channels(Set.of()));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder().channels(Set.of(" ")));
		Assertions.assertThrows(NullPointerException.class, () -> builder().channels(null));
		Assertions.assertThrows(NullPointerException.class, () -> builder().reconcile(null));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> builder().retry(f -> true, RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ZERO));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> builder().retry(f -> true, RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofSeconds(-1)));
		Assertions.assertThrows(NullPointerException.class,
				() -> builder().retry(null, RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofSeconds(1)));
		NotificationSupervisor supervisor = builder().build();
		Assertions.assertEquals(NotificationSupervisor.State.NEW, supervisor.snapshot().getState());
		Assertions.assertEquals(0L, supervisor.snapshot().getAttemptNumber());
		Assertions.assertFalse(supervisor.snapshot().isEverReady());
		Assertions.assertTrue(supervisor.snapshot().getLastFailure().isEmpty());
		Assertions.assertThrows(NullPointerException.class, () -> supervisor.start(null));
	}

	@Test
	public void publicDatabaseOpeningFailureTerminatesReadinessWithoutReconciliation() throws Exception {
		AtomicInteger checkouts = new AtomicInteger();
		SQLException failure = new SQLException("checkout failed", "08001");
		DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
				new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
					if (method.getName().equals("getConnection")) {
						checkouts.incrementAndGet();
						throw failure;
					}
					throw new AssertionError(method);
				});
		AtomicInteger reconciliations = new AtomicInteger();
		NotificationSupervisor supervisor = NotificationSupervisor.withDatabase(
				Database.withDataSource(source).databaseType(DatabaseType.POSTGRESQL).build())
				.channels(Set.of("job_ready")).reconcile(reconciliations::incrementAndGet).build();
		Worker worker = start(supervisor);
		Throwable terminal = result(supervisor).getFailure().orElseThrow();
		Assertions.assertInstanceOf(DatabaseException.class, terminal);
		Assertions.assertSame(failure, terminal.getCause());
		Assertions.assertSame(terminal, readinessFailure(supervisor));
		Assertions.assertEquals(1, checkouts.get());
		Assertions.assertEquals(0, reconciliations.get());
		worker.join();
	}

	@Test
	public void inlineAndRejectedExecutorsFailBeforeAnySessionWork() throws Exception {
		for (boolean inline : new boolean[]{true, false}) {
			AtomicInteger attempts = new AtomicInteger();
			NotificationSupervisor supervisor = build(builder(), operation -> attempts.incrementAndGet());
			RejectedExecutionException rejection = new RejectedExecutionException("executor rejected");
			Executor executor = inline ? Runnable::run : command -> { throw rejection; };
			RejectedExecutionException thrown = Assertions.assertThrows(RejectedExecutionException.class,
					() -> supervisor.start(executor));
			if (!inline)
				Assertions.assertSame(rejection, thrown);
			Assertions.assertSame(thrown, result(supervisor).getFailure().orElseThrow());
			Assertions.assertSame(thrown, readinessFailure(supervisor));
			Assertions.assertEquals(0, attempts.get());
			Assertions.assertThrows(IllegalStateException.class, () -> supervisor.start(executor));
		}
	}

	@Test
	public void executorErrorsAreReportedAndRethrown() throws Exception {
		NotificationSupervisor supervisor = build(builder(), operation -> Assertions.fail());
		AssertionError failure = new AssertionError("executor failure");
		Assertions.assertSame(failure, Assertions.assertThrows(AssertionError.class,
				() -> supervisor.start(command -> { throw failure; })));
		Assertions.assertSame(failure, result(supervisor).getFailure().orElseThrow());
		Assertions.assertSame(failure, readinessFailure(supervisor));
	}

	@Test
	public void stopBeforeStartAndQueuedStopPreventAllSessionWork() throws Exception {
		for (boolean submitFirst : new boolean[]{true, false}) {
			AtomicReference<Runnable> queued = new AtomicReference<>();
			NotificationSupervisor supervisor = build(builder(), operation -> Assertions.fail("opened after stop"));
			if (submitFirst)
				supervisor.start(queued::set);
			supervisor.requestStop();
			supervisor.requestStop();
			Assertions.assertTrue(result(supervisor).isStopRequested());
			Assertions.assertTrue(result(supervisor).getFailure().isEmpty());
			Assertions.assertInstanceOf(CancellationException.class, readinessFailure(supervisor));
			Assertions.assertThrows(IllegalStateException.class, () -> supervisor.start(queued::set));
			if (submitFirst)
				queued.get().run();
			Assertions.assertEquals(0L, supervisor.snapshot().getAttemptNumber());
		}
	}

	@Test
	public void concurrentStopDuringSubmissionCannotLoseCancellation() throws Exception {
		CountDownLatch submitting = new CountDownLatch(1);
		CountDownLatch accepted = new CountDownLatch(1);
		AtomicReference<Runnable> queued = new AtomicReference<>();
		AtomicReference<Throwable> submissionFailure = new AtomicReference<>();
		NotificationSupervisor supervisor = build(builder(), operation -> Assertions.fail("opened after queued stop"));
		Thread submitter = new Thread(() -> {
			try {
				supervisor.start(command -> {
					submitting.countDown();
					awaitUninterruptibly(accepted);
					queued.set(command);
				});
			} catch (Throwable failure) {
				submissionFailure.set(failure);
			}
		});
		submitter.start();
		try {
			await(submitting);
			supervisor.requestStop();
			Assertions.assertTrue(result(supervisor).isStopRequested());
			Assertions.assertInstanceOf(CancellationException.class, readinessFailure(supervisor));
		} finally {
			accepted.countDown();
			submitter.join(5_000);
		}
		Assertions.assertFalse(submitter.isAlive());
		Assertions.assertNull(submissionFailure.get());
		queued.get().run();
		Assertions.assertEquals(0L, supervisor.snapshot().getAttemptNumber());
	}

	@Test
	public void laterQueuedExecutionOnSubmittingThreadIsAllowed() throws Exception {
		AtomicReference<Runnable> queued = new AtomicReference<>();
		AtomicInteger reconciliations = new AtomicInteger();
		NotificationSupervisor supervisor = build(builder().reconcile(reconciliations::incrementAndGet),
				operation -> operation.perform(wait -> { throw new InterruptedException(); }));
		supervisor.start(queued::set);
		try {
			queued.get().run();
			Assertions.assertTrue(Thread.currentThread().isInterrupted());
			Assertions.assertTrue(result(supervisor).getFailure().isEmpty());
			Assertions.assertEquals(1, reconciliations.get());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void alreadyInterruptedWorkerDoesNotOpenSession() throws Exception {
		NotificationSupervisor supervisor = build(builder(), operation -> Assertions.fail("unexpected session"));
		AtomicBoolean interruptedOnExit = new AtomicBoolean();
		Thread thread = new Thread(() -> {
			supervisor.start(command -> {
				Thread worker = new Thread(() -> {
					Thread.currentThread().interrupt();
					command.run();
					interruptedOnExit.set(Thread.currentThread().isInterrupted());
				});
				worker.start();
			});
		});
		thread.start();
		thread.join();
		Assertions.assertTrue(result(supervisor).getFailure().isEmpty());
		Assertions.assertInstanceOf(CancellationException.class, readinessFailure(supervisor));
		// The termination stage may complete just before the executor wrapper records its flag.
		for (int i = 0; i < 100 && !interruptedOnExit.get(); i++)
			Thread.sleep(1);
		Assertions.assertTrue(interruptedOnExit.get());
	}

	@Test
	public void reconciliationPrecedesReadinessAndEmptyReceivesDoNotReconcile() throws Exception {
		AtomicInteger reconciliations = new AtomicInteger();
		AtomicInteger receives = new AtomicInteger();
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		NotificationSupervisor supervisor = build(builder().reconcile(() -> {
			if (reconciliations.incrementAndGet() == 1)
				Assertions.assertFalse(reference.get().firstReady().toCompletableFuture().isDone());
		}), operation -> operation.perform(wait -> {
			Assertions.assertTrue(reference.get().firstReady().toCompletableFuture().isDone());
			Assertions.assertEquals(NotificationSupervisor.State.ACTIVE, reference.get().snapshot().getState());
			return switch (receives.incrementAndGet()) {
				case 1 -> List.of();
				case 2 -> List.of(Notification.of("job_ready", ""));
				default -> throw new InterruptedException();
			};
		}));
		reference.set(supervisor);
		Worker worker = start(supervisor);
		Assertions.assertTrue(result(supervisor).getFailure().isEmpty());
		Assertions.assertEquals(2, reconciliations.get());
		Assertions.assertEquals(3, receives.get());
		Assertions.assertTrue(supervisor.snapshot().isEverReady());
		worker.join();
	}

	@Test
	public void stopDuringInitialReconciliationPreventsLateReadiness() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		NotificationSupervisor supervisor = build(builder().reconcile(() -> {
			entered.countDown();
			awaitUninterruptibly(release);
		}), operation -> operation.perform(wait -> Assertions.fail("receive after startup cancellation")));
		Worker worker = start(supervisor);
		await(entered);
		supervisor.requestStop();
		Assertions.assertInstanceOf(CancellationException.class, readinessFailure(supervisor));
		Assertions.assertFalse(supervisor.termination().toCompletableFuture().isDone());
		release.countDown();
		Assertions.assertTrue(result(supervisor).getFailure().isEmpty());
		Assertions.assertFalse(supervisor.snapshot().isEverReady());
		worker.join();
		Assertions.assertTrue(worker.interrupted.get());
	}

	@Test
	public void nonemptyLateInterruptedBatchPerformsFinalTransactionButEmptyBatchDoesNot() throws Exception {
		for (boolean nonempty : new boolean[]{true, false}) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:supervisor_final_batch_" + nonempty);
			Database workDatabase = Database.withDataSource(source).build();
			AtomicInteger reconciliations = new AtomicInteger();
			NotificationSupervisor supervisor = build(builder().reconcile(() -> {
				Assertions.assertFalse(Thread.currentThread().isInterrupted());
				workDatabase.transaction(() -> workDatabase.query("VALUES(1)").fetchObject(Integer.class));
				reconciliations.incrementAndGet();
			}), operation -> operation.perform(wait -> {
				Thread.currentThread().interrupt();
				return nonempty ? List.of(Notification.of("job_ready", "")) : List.of();
			}));
			Worker worker = start(supervisor);
			Assertions.assertTrue(result(supervisor).getFailure().isEmpty());
			worker.join();
			Assertions.assertTrue(worker.interrupted.get());
			Assertions.assertEquals(nonempty ? 2 : 1, reconciliations.get());
		}
	}

	@Test
	public void stopRacingWithDecodedBatchStillReconcilesAndRetainsFailure() throws Exception {
		for (boolean failFinal : new boolean[]{false, true}) {
			CountDownLatch receiving = new CountDownLatch(1);
			AtomicInteger reconciliations = new AtomicInteger();
			NotificationSupervisor supervisor = build(builder().reconcile(() -> {
				Assertions.assertFalse(Thread.currentThread().isInterrupted());
				if (reconciliations.incrementAndGet() == 2 && failFinal)
					throw FAILURE;
			}), operation -> operation.perform(wait -> {
				receiving.countDown();
				try {
					new CountDownLatch(1).await();
				} catch (InterruptedException interrupted) {
					Thread.currentThread().interrupt();
				}
				return List.of(Notification.of("job_ready", ""));
			}));
			Worker worker = start(supervisor);
			await(receiving);
			supervisor.requestStop();
			NotificationSupervisor.Result result = result(supervisor);
			Assertions.assertEquals(failFinal, result.getFailure().isPresent());
			if (failFinal)
				Assertions.assertSame(FAILURE, result.getFailure().orElseThrow());
			Assertions.assertTrue(result.isStopRequested());
			Assertions.assertEquals(2, reconciliations.get());
			worker.join();
		}
	}

	@Test
	public void retryableStartupFailureLeavesReadinessPendingUntilSuccessfulAttempt() throws Exception {
		AtomicInteger attempts = new AtomicInteger();
		AtomicInteger sleeps = new AtomicInteger();
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		NotificationSupervisor supervisor = builder().retry(failure -> {
			Assertions.assertEquals(NotificationSupervisor.FailurePhase.OPEN, failure.getPhase());
			Assertions.assertEquals(1, failure.getConsecutiveFailures());
			Assertions.assertFalse(failure.isFirstReadyReached());
			Assertions.assertFalse(reference.get().firstReady().toCompletableFuture().isDone());
			return true;
		}, RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)).build(operation -> {
			if (attempts.incrementAndGet() == 1)
				throw FAILURE;
			operation.perform(wait -> { throw new InterruptedException(); });
		}, System::nanoTime, delay -> sleeps.incrementAndGet());
		reference.set(supervisor);
		Worker worker = start(supervisor);
		Assertions.assertTrue(result(supervisor).getFailure().isEmpty());
		supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
		Assertions.assertEquals(2, attempts.get());
		Assertions.assertEquals(1, sleeps.get());
		Assertions.assertSame(FAILURE, supervisor.snapshot().getLastFailure().orElseThrow());
		worker.join();
	}

	@Test
	public void firstReadinessRemainsSuccessfulWhileCurrentAttemptBacksOff() throws Exception {
		CountDownLatch sleeping = new CountDownLatch(1);
		NotificationSupervisor supervisor = builder().retry(f -> true,
				RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)).build(
				operation -> operation.perform(wait -> { throw FAILURE; }), System::nanoTime,
				delay -> { sleeping.countDown(); new CountDownLatch(1).await(); });
		Worker worker = start(supervisor);
		await(sleeping);
		supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
		Assertions.assertTrue(supervisor.snapshot().isEverReady());
		Assertions.assertEquals(NotificationSupervisor.State.BACKING_OFF, supervisor.snapshot().getState());
		supervisor.requestStop();
		Assertions.assertSame(FAILURE, result(supervisor).getFailure().orElseThrow());
		supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
		worker.join();
		Assertions.assertTrue(worker.interrupted.get());
	}

	@Test
	public void allDatabaseFailurePhasesReachExplicitRetryClassification() throws Exception {
		for (NotificationSupervisor.FailurePhase phase : NotificationSupervisor.FailurePhase.values()) {
			AtomicReference<NotificationSupervisor.RetryFailure> observed = new AtomicReference<>();
			NotificationSupervisor.Builder builder = builder().retry(f -> { observed.set(f); return false; },
					RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1));
			if (phase == NotificationSupervisor.FailurePhase.RECONCILE)
				builder.reconcile(() -> { throw FAILURE; });
			NotificationSupervisor supervisor = build(builder, operation -> {
				if (phase == NotificationSupervisor.FailurePhase.OPEN)
					throw FAILURE;
				try {
					operation.perform(wait -> {
						if (phase == NotificationSupervisor.FailurePhase.RECEIVE)
							throw FAILURE;
						throw new InterruptedException();
					});
				} catch (InterruptedException interrupted) {
					if (phase == NotificationSupervisor.FailurePhase.CLEANUP)
						throw FAILURE;
					throw interrupted;
				}
			});
			Worker worker = start(supervisor);
			Assertions.assertSame(FAILURE, result(supervisor).getFailure().orElseThrow());
			Assertions.assertEquals(phase, observed.get().getPhase());
			Assertions.assertSame(FAILURE, observed.get().getException());
			worker.join();
		}
	}

	@Test
	public void primaryReceiveFailureKeepsItsPhaseAndSuppressedCleanupFailure() throws Exception {
		DatabaseException primary = new DatabaseException("receive");
		DatabaseException cleanup = new DatabaseException("cleanup");
		AtomicReference<NotificationSupervisor.FailurePhase> phase = new AtomicReference<>();
		NotificationSupervisor supervisor = build(builder().retry(f -> { phase.set(f.getPhase()); return false; },
				RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)), operation -> {
			try {
				operation.perform(wait -> { throw primary; });
			} catch (DatabaseException failure) {
				failure.addSuppressed(cleanup);
				throw failure;
			}
		});
		Worker worker = start(supervisor);
		Assertions.assertSame(primary, result(supervisor).getFailure().orElseThrow());
		Assertions.assertEquals(NotificationSupervisor.FailurePhase.RECEIVE, phase.get());
		Assertions.assertArrayEquals(new Throwable[]{cleanup}, primary.getSuppressed());
		worker.join();
	}

	@Test
	public void wrappedUnexpectedApplicationExceptionsAndUnsupportedOperationsNeverRetry() throws Exception {
		for (boolean unsupported : new boolean[]{false, true}) {
			RuntimeException original = unsupported ? new UnsupportedOperationException("disabled")
					: new IllegalArgumentException("application error");
			AtomicInteger retries = new AtomicInteger();
			NotificationSupervisor supervisor = build(builder().reconcile(() -> { throw original; })
					.retry(f -> { retries.incrementAndGet(); return true; },
							RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)), operation -> {
				if (unsupported)
					throw original;
				try {
					operation.perform(wait -> Assertions.fail());
				} catch (RuntimeException failure) {
					throw new DatabaseException("Notification session operation failed", failure);
				}
			});
			Worker worker = start(supervisor);
			Throwable terminal = result(supervisor).getFailure().orElseThrow();
			Assertions.assertSame(original, unsupported ? terminal : terminal.getCause());
			Assertions.assertEquals(0, retries.get());
			worker.join();
		}
	}

	@Test
	public void unsupportedReceiveWrappedBySessionStillDoesNotRetry() throws Exception {
		UnsupportedOperationException unsupported = new UnsupportedOperationException("driver capability unavailable");
		NotificationSupervisor supervisor = build(builder().retry(f -> Assertions.fail("retried unsupported receive"),
				RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)), operation -> {
			try {
				operation.perform(wait -> { throw unsupported; });
			} catch (RuntimeException failure) {
				throw new DatabaseException("Notification session operation failed", failure);
			}
		});
		Worker worker = start(supervisor);
		Assertions.assertSame(unsupported, result(supervisor).getFailure().orElseThrow().getCause());
		worker.join();
	}

	@Test
	public void errorsRemainExactAndAreNotRetried() throws Exception {
		AssertionError error = new AssertionError("fatal receive");
		NotificationSupervisor supervisor = build(builder().retry(f -> Assertions.fail("retried Error"),
				RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)),
				operation -> operation.perform(wait -> { throw error; }));
		Worker worker = start(supervisor);
		Assertions.assertSame(error, result(supervisor).getFailure().orElseThrow());
		worker.join();
		Assertions.assertSame(error, worker.uncaught.get());
	}

	@Test
	public void invalidBackoffAndPolicyFailureRetainTheTriggeringDatabaseFailure() throws Exception {
		for (int variant = 0; variant < 4; variant++) {
			int selected = variant;
			IllegalStateException policyFailure = new IllegalStateException("bad policy");
			NotificationSupervisor supervisor = build(builder().retry(f -> {
				if (selected == 0)
					throw policyFailure;
				return true;
			}, (number, failure) -> {
				if (selected == 1)
					throw policyFailure;
				return selected == 2 ? null : Duration.ofSeconds(-1);
			}, Duration.ofMinutes(1)), operation -> { throw FAILURE; });
			Worker worker = start(supervisor);
			Throwable terminal = result(supervisor).getFailure().orElseThrow();
			Assertions.assertSame(FAILURE, terminal.getSuppressed()[0]);
			if (selected < 2)
				Assertions.assertSame(policyFailure, terminal);
			worker.join();
		}
	}

	@Test
	public void stabilityExcludesAcquisitionAndCleanupAndResetsOnlyAfterEstablishedSession() throws Exception {
		AtomicLong clock = new AtomicLong();
		AtomicInteger attempts = new AtomicInteger();
		List<Integer> failures = new ArrayList<>();
		List<Duration> sleeps = new ArrayList<>();
		NotificationSupervisor supervisor = builder().retry(f -> {
			failures.add(f.getConsecutiveFailures());
			return attempts.get() < 3;
		}, RetryPolicy.Backoff.exponential(Duration.ofNanos(1), Duration.ofNanos(100)), Duration.ofNanos(100))
				.build(operation -> {
					int attempt = attempts.incrementAndGet();
					clock.addAndGet(1_000); // Acquisition must not establish stability.
					try {
						operation.perform(wait -> {
							clock.addAndGet(attempt == 3 ? 100 : 10);
							throw FAILURE;
						});
					} finally {
						clock.addAndGet(1_000); // Nor should slow cleanup hide a flapping session.
					}
				}, clock::get, sleeps::add);
		Worker worker = start(supervisor);
		Assertions.assertSame(FAILURE, result(supervisor).getFailure().orElseThrow());
		Assertions.assertEquals(List.of(1, 2, 1), failures);
		Assertions.assertEquals(List.of(Duration.ofNanos(1), Duration.ofNanos(2)), sleeps);
		worker.join();
	}

	@Test
	public void extremeDurationsAndMonotonicClockWrapDoNotOverflow() throws Exception {
		AtomicLong clock = new AtomicLong(Long.MAX_VALUE - 50);
		AtomicInteger attempts = new AtomicInteger();
		List<Integer> counts = new ArrayList<>();
		NotificationSupervisor supervisor = builder().retry(f -> {
			counts.add(f.getConsecutiveFailures());
			return attempts.get() < 2;
		}, RetryPolicy.Backoff.fixed(Duration.ofSeconds(Long.MAX_VALUE)), Duration.ofSeconds(Long.MAX_VALUE))
				.build(operation -> {
					attempts.incrementAndGet();
					operation.perform(wait -> { clock.addAndGet(100); throw FAILURE; });
				}, clock::get, delay -> Assertions.assertEquals(Duration.ofSeconds(Long.MAX_VALUE), delay));
		Worker worker = start(supervisor);
		Assertions.assertSame(FAILURE, result(supervisor).getFailure().orElseThrow());
		Assertions.assertEquals(List.of(1, 2), counts);
		worker.join();
	}

	@Test
	public void interruptedBackoffAndConcurrentFailureKeepOriginalFailureAndInterrupt() throws Exception {
		for (boolean duringBackoff : new boolean[]{false, true}) {
			AtomicInteger policies = new AtomicInteger();
			NotificationSupervisor supervisor = builder().retry(f -> { policies.incrementAndGet(); return true; },
					RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)).build(operation -> {
				if (!duringBackoff)
					Thread.currentThread().interrupt();
				throw FAILURE;
			}, System::nanoTime, delay -> { throw new InterruptedException(); });
			Worker worker = start(supervisor);
			Assertions.assertSame(FAILURE, result(supervisor).getFailure().orElseThrow());
			Assertions.assertSame(FAILURE, readinessFailure(supervisor));
			Assertions.assertEquals(duringBackoff ? 1 : 0, policies.get());
			worker.join();
			Assertions.assertTrue(worker.interrupted.get());
		}
	}

	@Test
	public void completionViewsCannotMutateLifecycleAndCallbacksCanInspectState() throws Exception {
		NotificationSupervisor supervisor = build(builder(), operation -> Assertions.fail());
		CompletableFuture<Void> forgedReady = supervisor.firstReady().toCompletableFuture();
		forgedReady.complete(null);
		CompletableFuture<NotificationSupervisor.Result> cancelledTermination = supervisor.termination().toCompletableFuture();
		cancelledTermination.cancel(true);
		Assertions.assertFalse(supervisor.firstReady().toCompletableFuture().isDone());
		Assertions.assertFalse(supervisor.termination().toCompletableFuture().isDone());
		AtomicReference<NotificationSupervisor.State> observed = new AtomicReference<>();
		supervisor.termination().thenRun(() -> {
			observed.set(supervisor.snapshot().getState());
			supervisor.requestStop();
		});
		supervisor.requestStop();
		Assertions.assertInstanceOf(CancellationException.class, readinessFailure(supervisor));
		Assertions.assertTrue(result(supervisor).isStopRequested());
		Assertions.assertEquals(NotificationSupervisor.State.TERMINATED, observed.get());
	}

	@Test
	public void terminationWaitsForCleanupAndIndependentPollingCanStillProgress() throws Exception {
		CountDownLatch receiving = new CountDownLatch(1);
		CountDownLatch cleanupEntered = new CountDownLatch(1);
		CountDownLatch releaseCleanup = new CountDownLatch(1);
		NotificationSupervisor supervisor = build(builder(), operation -> {
			try {
				operation.perform(wait -> { receiving.countDown(); new CountDownLatch(1).await(); return List.of(); });
			} finally {
				cleanupEntered.countDown();
				awaitUninterruptibly(releaseCleanup);
			}
		});
		Worker worker = start(supervisor);
		await(receiving);
		var poller = Executors.newSingleThreadScheduledExecutor();
		try {
			CountDownLatch polled = new CountDownLatch(1);
			poller.execute(polled::countDown);
			await(polled);
			supervisor.requestStop();
			await(cleanupEntered);
			Assertions.assertEquals(NotificationSupervisor.State.STOPPING, supervisor.snapshot().getState());
			Assertions.assertFalse(supervisor.termination().toCompletableFuture().isDone());
		} finally {
			releaseCleanup.countDown();
			poller.shutdownNow();
		}
		Assertions.assertTrue(result(supervisor).getFailure().isEmpty());
		worker.join();
	}

	@Test
	public void completedSupervisorNeverInterruptsAReusedExecutorThread() throws Exception {
		var executor = Executors.newSingleThreadExecutor();
		CountDownLatch receiving = new CountDownLatch(1);
		NotificationSupervisor supervisor = build(builder(), operation -> operation.perform(wait -> {
			receiving.countDown();
			new CountDownLatch(1).await();
			return List.of();
		}));
		try {
			supervisor.start(executor);
			await(receiving);
			supervisor.requestStop();
			result(supervisor);
			CountDownLatch nextTask = new CountDownLatch(1);
			CountDownLatch release = new CountDownLatch(1);
			var next = executor.submit(() -> {
				nextTask.countDown();
				return release.await(5, TimeUnit.SECONDS) && !Thread.currentThread().isInterrupted();
			});
			await(nextTask);
			for (int i = 0; i < 1_000; i++)
				supervisor.requestStop();
			release.countDown();
			Assertions.assertTrue(next.get(5, TimeUnit.SECONDS));
		} finally {
			executor.shutdownNow();
		}
	}

	private static NotificationSupervisor.Builder builder() {
		return NotificationSupervisor.withDatabase(DATABASE).channels(Set.of("job_ready")).reconcile(() -> {});
	}

	private static NotificationSupervisor build(NotificationSupervisor.Builder builder,
			NotificationSupervisor.SessionAttempt attempt) {
		return builder.build(attempt, System::nanoTime, delay -> {});
	}

	private static NotificationSupervisor.Result result(NotificationSupervisor supervisor) throws Exception {
		return supervisor.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
	}

	private static Throwable readinessFailure(NotificationSupervisor supervisor) {
		return Assertions.assertThrows(ExecutionException.class,
				() -> supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS)).getCause();
	}

	private static Worker start(NotificationSupervisor supervisor) {
		Worker worker = new Worker();
		supervisor.start(command -> {
			worker.thread = new Thread(() -> {
				try {
					command.run();
				} finally {
					worker.interrupted.set(Thread.currentThread().isInterrupted());
				}
			}, "notification-supervisor-test");
			worker.thread.setDaemon(true);
			worker.thread.setUncaughtExceptionHandler((thread, failure) -> worker.uncaught.set(failure));
			worker.thread.start();
		});
		return worker;
	}

	private static void await(CountDownLatch latch) throws InterruptedException {
		Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for test phase");
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				await(latch);
				break;
			} catch (InterruptedException signal) {
				interrupted = true;
			}
		}
		if (interrupted)
			Thread.currentThread().interrupt();
	}

	private static final class Worker {
		private Thread thread;
		private final AtomicBoolean interrupted = new AtomicBoolean();
		private final AtomicReference<Throwable> uncaught = new AtomicReference<>();

		private void join() throws InterruptedException {
			this.thread.join(5_000);
			Assertions.assertFalse(this.thread.isAlive(), "worker did not terminate");
		}
	}
}
