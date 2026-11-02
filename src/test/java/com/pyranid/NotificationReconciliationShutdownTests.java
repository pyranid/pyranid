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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

/** Preserves accepted reconciliation outcomes across listener shutdown and callback cancellation. */
@Timeout(15)
class NotificationReconciliationShutdownTests {
	private static final Database DATABASE = Database.with((DataSource) Proxy.newProxyInstance(
			NotificationReconciliationShutdownTests.class.getClassLoader(), new Class<?>[]{DataSource.class},
			(proxy, method, args) -> { throw new AssertionError("Unexpected data source call: " + method); }), DatabaseType.POSTGRESQL).build();

	@Test
	void failureFromFinalAcceptedPassSurvivesListenerCleanupWithAndWithoutPolling() throws Exception {
		for (boolean polling : new boolean[]{false, true}) {
			CountDownLatch entered = new CountDownLatch(1);
			CountDownLatch finish = new CountDownLatch(1);
			CountDownLatch cleaned = new CountDownLatch(1);
			CountDownLatch opened = new CountDownLatch(1);
			DatabaseException failure = new DatabaseException("Final accepted reconciliation failed");
			NotificationSupervisor.Builder builder = builder().reconcile(() -> {
				entered.countDown();
				awaitUninterruptibly(finish);
				throw failure;
			});
			if (polling)
				builder.pollEvery(Duration.ofDays(1));
			NotificationSupervisor supervisor = build(builder, operation -> {
				opened.countDown();
				try {
					operation.perform(wait -> fail("Failed initial reconciliation must not enter receive"));
				} finally {
					cleaned.countDown();
				}
			});
			try {
				supervisor.start();
				await(entered);
				await(opened);
				supervisor.requestShutdown();
				await(cleaned);
				assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
				finish.countDown();
				assertSame(failure, result(supervisor).getFailure().orElse(null),
						"Final accepted reconciliation failure was lost; polling=" + polling);
				assertSame(failure, supervisor.snapshot().getLastReconciliationFailure().orElseThrow());
			} finally {
				finish.countDown();
				stop(supervisor);
			}
		}
	}

	@Test
	void failureBeforeListenerCleanupSealsTheWorkerIsRetained() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch finish = new CountDownLatch(1);
		CountDownLatch cleanup = new CountDownLatch(1);
		CountDownLatch opened = new CountDownLatch(1);
		CountDownLatch finishCleanup = new CountDownLatch(1);
		CountDownLatch reported = new CountDownLatch(1);
		CountDownLatch finishReport = new CountDownLatch(1);
		ConcurrentHashMap<NotificationSupervisor.ThreadRole, Thread> workers = new ConcurrentHashMap<>();
		DatabaseException failure = new DatabaseException("Reconciliation failed before worker sealing");
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofDays(1)).threadFactory(role -> task -> {
			Thread worker = new Thread(task, "reconciliation-shutdown-" + role);
			worker.setDaemon(true);
			workers.put(role, worker);
			return worker;
		}).reconcile(() -> {
			entered.countDown();
			awaitUninterruptibly(finish);
			throw failure;
		}).onReconciliationFailure(observed -> {
			assertSame(failure, observed);
			reported.countDown();
			awaitUninterruptibly(finishReport);
		}), operation -> {
			opened.countDown();
			try {
				operation.perform(wait -> fail("Unexpected receive"));
			} finally {
				cleanup.countDown();
				awaitUninterruptibly(finishCleanup);
			}
		});
		try {
			supervisor.start();
			await(entered);
			await(opened);
			supervisor.requestShutdown();
			await(cleanup);
			finish.countDown();
			await(reported);
			finishCleanup.countDown();
			join(workers.get(NotificationSupervisor.ThreadRole.LISTENER));
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
			finishReport.countDown();
			assertSame(failure, result(supervisor).getFailure().orElse(null),
					"Failure observed before worker sealing was lost");
		} finally {
			finish.countDown();
			finishCleanup.countDown();
			finishReport.countDown();
			stop(supervisor);
			for (Thread worker : workers.values())
				join(worker);
		}
	}

	@Test
	void listenerFailureAndFailedDrainingPollAreBothRetained() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch finish = new CountDownLatch(1);
		ConcurrentHashMap<NotificationSupervisor.ThreadRole, Thread> workers = new ConcurrentHashMap<>();
		DatabaseException listenerFailure = new DatabaseException("Listener failed while poll was active");
		DatabaseException reconciliationFailure = new DatabaseException("Draining poll failed");
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofDays(1)).threadFactory(role -> task -> {
			Thread worker = new Thread(task, "failed-drain-" + role);
			worker.setDaemon(true);
			workers.put(role, worker);
			return worker;
		}).reconcile(() -> {
			entered.countDown();
			awaitUninterruptibly(finish);
			throw reconciliationFailure;
		}), operation -> {
			await(entered);
			throw listenerFailure;
		});
		try {
			supervisor.start();
			await(entered);
			join(workers.get(NotificationSupervisor.ThreadRole.LISTENER));
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
			finish.countDown();
			Throwable failure = result(supervisor).getFailure().orElseThrow();
			assertSame(listenerFailure, failure);
			assertContains(failure, reconciliationFailure);
		} finally {
			finish.countDown();
			stop(supervisor);
			for (Thread worker : workers.values())
				join(worker);
		}
	}

	@Test
	void recoveredPollingFailureDoesNotTurnALaterShutdownIntoFailure() throws Exception {
		AtomicInteger calls = new AtomicInteger();
		AtomicInteger reported = new AtomicInteger();
		CountDownLatch receiving = new CountDownLatch(1);
		DatabaseException failure = new DatabaseException("Recoverable first pass");
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofMillis(10))
				.onReconciliationFailure(observed -> {
			assertSame(failure, observed);
			reported.incrementAndGet();
		}).reconcile(() -> {
			if (calls.incrementAndGet() == 1)
				throw failure;
		}), operation -> operation.perform(wait -> {
			receiving.countDown();
			new CountDownLatch(1).await();
			return List.of();
		}));
		try {
			supervisor.start();
			await(receiving);
			assertTrue(supervisor.snapshot().getLastSuccessfulReconciliation().isPresent());
			assertTrue(supervisor.snapshot().getLastReconciliationFailure().isEmpty());
			supervisor.requestShutdown();
			assertTrue(result(supervisor).getFailure().isEmpty());
			assertEquals(1, reported.get());
			assertTrue(calls.get() >= 2);
		} finally {
			stop(supervisor);
		}
	}

	@Test
	void drainingFailureObserverFailureKeepsItsOriginalDatabaseFailure() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch finish = new CountDownLatch(1);
		CountDownLatch cleaned = new CountDownLatch(1);
		DatabaseException failure = new DatabaseException("Final accepted reconciliation failed");
		IllegalStateException observerFailure = new IllegalStateException("Failure reporter failed");
		NotificationSupervisor supervisor = build(builder().reconcile(() -> {
			entered.countDown();
			awaitUninterruptibly(finish);
			throw failure;
		}).onReconciliationFailure(observed -> {
			assertSame(failure, observed);
			throw observerFailure;
		}), operation -> {
			try {
				operation.perform(wait -> fail("Unexpected receive"));
			} finally {
				cleaned.countDown();
			}
		});
		try {
			supervisor.start();
			await(entered);
			supervisor.requestShutdown();
			await(cleaned);
			finish.countDown();
			Throwable terminal = result(supervisor).getFailure().orElseThrow();
			assertSame(observerFailure, terminal);
			assertContains(terminal, failure);
		} finally {
			finish.countDown();
			stop(supervisor);
		}
	}


	@Test
	void interruptedPassDoesNotCompleteItsRequestOrRecordSuccess() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch finish = new CountDownLatch(1);
		AtomicReference<Throwable> terminalFailure = new AtomicReference<>();
		AtomicReference<Throwable> requestFailure = new AtomicReference<>();
		AtomicBoolean completed = new AtomicBoolean();
		AtomicBoolean interruptedAtExit = new AtomicBoolean();
		NotificationReconciler reconciler = new NotificationReconciler(() -> {
			entered.countDown();
			awaitUninterruptibly(finish);
			Thread.currentThread().interrupt();
		}, null, System::nanoTime, failure -> false, failure -> fail(failure), terminalFailure::set);
		Thread caller = new Thread(() -> {
			try {
				reconciler.reconcileAndAwait();
				completed.set(true);
			} catch (Throwable failure) {
				requestFailure.set(failure);
			}
		}, "interrupted-reconciliation-caller");
		Thread worker = new Thread(() -> {
			reconciler.run();
			interruptedAtExit.set(Thread.currentThread().isInterrupted());
		}, "interrupted-reconciliation-worker");
		caller.setDaemon(true);
		worker.setDaemon(true);
		try {
			caller.start();
			worker.start();
			await(entered);
			finish.countDown();
			join(worker);
			join(caller);
			assertInstanceOf(InterruptedException.class, terminalFailure.get());
			assertTrue(interruptedAtExit.get());
			assertFalse(completed.get(), "Interrupted work was published as a successful request");
			assertNotNull(requestFailure.get());
			assertContains(requestFailure.get(), terminalFailure.get());
			assertNull(reconciler.snapshot().lastSuccessTime());
			assertNull(reconciler.snapshot().lastSuccessNanos());
			assertSame(terminalFailure.get(), reconciler.snapshot().lastFailure());
		} finally {
			finish.countDown();
			reconciler.stop();
			caller.interrupt();
			join(worker);
			join(caller);
		}
	}

	@Test
	void interruptedInitialPassCannotPublishReadinessBeforeFatalStatePublication() throws Exception {
		CountDownLatch fatalPublication = new CountDownLatch(1);
		CountDownLatch allowFatalPublication = new CountDownLatch(1);
		CountDownLatch listenerAdvanced = new CountDownLatch(1);
		AtomicInteger receives = new AtomicInteger();
		AtomicBoolean publicationDelayed = new AtomicBoolean();
		NotificationSupervisor supervisor = build(builder().threadFactory(role -> task -> {
			Thread worker = new Thread(task, "interrupt-readiness-" + role);
			worker.setDaemon(true);
			return worker;
		}).reconcile(() -> Thread.currentThread().interrupt()), operation -> {
			try {
				operation.perform(wait -> {
					receives.incrementAndGet();
					listenerAdvanced.countDown();
					new CountDownLatch(1).await();
					return List.of();
				});
			} finally {
				listenerAdvanced.countDown();
			}
		});
		// Delay only fatal-state publication to model the reconciliation worker losing its timeslice
		// after notifying the listener. This exposes the previous success-publication race deterministically.
		ReentrantLock publicationGate = new ReentrantLock() {
			@Override public void lock() {
				if (Thread.currentThread().getName().equals("interrupt-readiness-RECONCILIATION")
						&& publicationDelayed.compareAndSet(false, true)) {
					fatalPublication.countDown();
					awaitUninterruptibly(allowFatalPublication);
				}
				super.lock();
			}
		};
		var lockField = NotificationSupervisor.class.getDeclaredField("lifecycleLock");
		lockField.setAccessible(true);
		lockField.set(supervisor, publicationGate);
		try {
			supervisor.start();
			await(fatalPublication);
			await(listenerAdvanced);
			CompletableFuture<Void> readiness = supervisor.firstReady().toCompletableFuture();
			assertFalse(readiness.isDone() && !readiness.isCompletedExceptionally(),
					"Cancelled initial reconciliation published successful readiness");
			assertEquals(0, receives.get());
			assertTrue(supervisor.snapshot().getLastSuccessfulReconciliation().isEmpty());
			allowFatalPublication.countDown();
			assertTrue(result(supervisor).getFailure().isPresent());
			assertTrue(readiness.isCompletedExceptionally());
		} finally {
			allowFatalPublication.countDown();
			stop(supervisor);
		}
	}

	@Test
	void interruptedPeriodicPassPreservesThePreviousSuccessTime() throws Exception {
		CountDownLatch receiving = new CountDownLatch(1);
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		AtomicReference<Instant> previousSuccess = new AtomicReference<>();
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofMillis(10)).reconcile(() -> {
			if (receiving.getCount() == 0) {
				previousSuccess.set(reference.get().snapshot().getLastSuccessfulReconciliation().orElseThrow());
				Thread.currentThread().interrupt();
			}
		}), operation -> operation.perform(wait -> {
			receiving.countDown();
			new CountDownLatch(1).await();
			return List.of();
		}));
		reference.set(supervisor);
		try {
			supervisor.start();
			await(receiving);
			Throwable failure = result(supervisor).getFailure().orElseThrow();
			assertEquals(1, ThrowableGraph.inspect(failure,
					node -> node instanceof InterruptedException ? 1 : 0, 0));
			assertNotNull(previousSuccess.get());
			assertEquals(previousSuccess.get(), supervisor.snapshot().getLastSuccessfulReconciliation().orElseThrow());
			assertTrue(supervisor.firstReady().toCompletableFuture().isDone());
			assertFalse(supervisor.firstReady().toCompletableFuture().isCompletedExceptionally());
		} finally {
			stop(supervisor);
		}
	}

	private static NotificationSupervisor.Builder builder() {
		return NotificationSupervisor.withDatabase(DATABASE).channels(Set.of("job_ready")).reconcile(() -> {});
	}

	private static NotificationSupervisor build(NotificationSupervisor.Builder builder,
			NotificationSupervisor.SessionAttempt attempt) {
		return builder.build(attempt, System::nanoTime, delay -> {});
	}

	private static NotificationSupervisor.ShutdownResult result(NotificationSupervisor supervisor) throws Exception {
		return supervisor.awaitShutdownAsync().toCompletableFuture().get(5, TimeUnit.SECONDS);
	}

	private static void stop(NotificationSupervisor supervisor) throws Exception {
		supervisor.requestShutdown();
		result(supervisor);
	}

	private static void await(CountDownLatch latch) throws InterruptedException {
		assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for test phase");
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		try {
			while (true) {
				try {
					await(latch);
					return;
				} catch (InterruptedException signal) {
					interrupted = true;
				}
			}
		} finally {
			if (interrupted)
				Thread.currentThread().interrupt();
		}
	}

	private static void join(Thread worker) throws InterruptedException {
		worker.join(5_000);
		assertFalse(worker.isAlive(), "Worker did not exit: " + worker.getName());
	}

	private static void assertContains(Throwable failure, Throwable expected) {
		assertEquals(1, ThrowableGraph.inspect(failure, node -> node == expected ? 1 : 0, 0),
				"Shutdown lost an observed failure");
	}
}
