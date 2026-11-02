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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Failure-path shutdown and diagnostic callbacks must leave lifecycle control available. */
@Timeout(10)
class NotificationSupervisorShutdownSafetyTests {
	private static final Database DATABASE = Database.with((DataSource) Proxy.newProxyInstance(
			NotificationSupervisorShutdownSafetyTests.class.getClassLoader(), new Class<?>[]{DataSource.class},
			(proxy, method, args) -> { throw new AssertionError("Unexpected data source call: " + method); }), DatabaseType.POSTGRESQL).build();

	@Test
	void rejectedListenerInterruptStillCancelsPollingAndIsRetainedAfterCleanup() throws Exception {
		for (Throwable rejection : List.of(new SecurityException("interrupt denied"), new AssertionError("interrupt failed"))) {
			CountDownLatch receiving = new CountDownLatch(1);
			CountDownLatch releaseReceive = new CountDownLatch(1);
			CountDownLatch repeatedPolls = new CountDownLatch(2);
			AtomicBoolean shutdownRequested = new AtomicBoolean();
			NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofMillis(10))
					.threadFactory(role -> task -> rejectingThread(role, task, rejection, null))
					.reconcile(() -> {
						if (shutdownRequested.get()) repeatedPolls.countDown();
					}), operation -> operation.perform(wait -> {
				receiving.countDown();
				releaseReceive.await();
				return List.of();
			}));
			try {
				supervisor.start();
				await(receiving);
				assertDoesNotThrow(supervisor::requestShutdown);
				shutdownRequested.set(true);
				// One already-admitted pass is permitted; repeated new polls are not.
				assertFalse(repeatedPolls.await(150, TimeUnit.MILLISECONDS));
				supervisor.requestShutdown();
				assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
				releaseReceive.countDown();
				NotificationSupervisor.ShutdownResult result = result(supervisor);
				assertTrue(result.isShutdownRequested());
				assertSame(rejection, result.getFailure().orElseThrow());
			} finally {
				releaseReceive.countDown();
				supervisor.requestShutdown();
				result(supervisor);
			}
		}
	}

	@Test
	void rejectedInterruptBeforeReadinessStillPublishesCancellationAndWaitsForWork() throws Exception {
		SecurityException rejection = new SecurityException("interrupt denied");
		CountDownLatch reconciling = new CountDownLatch(1);
		CountDownLatch releaseReconciliation = new CountDownLatch(1);
		NotificationSupervisor supervisor = build(builder()
				.threadFactory(role -> task -> rejectingThread(role, task, rejection, null))
				.reconcile(() -> { reconciling.countDown(); awaitUninterruptibly(releaseReconciliation); }),
				operation -> operation.perform(wait -> { throw new AssertionError("Shutdown reached receive"); }));
		try {
			supervisor.start();
			await(reconciling);
			assertDoesNotThrow(supervisor::requestShutdown);
			assertInstanceOf(CancellationException.class, assertThrows(ExecutionException.class,
					() -> supervisor.firstReady().toCompletableFuture().get(2, TimeUnit.SECONDS)).getCause());
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
			releaseReconciliation.countDown();
			assertSame(rejection, result(supervisor).getFailure().orElseThrow());
		} finally {
			releaseReconciliation.countDown();
			supervisor.requestShutdown();
			result(supervisor);
		}
	}

	@Test
	void automaticFailureRetainsRejectedInterruptAndWaitsForListenerCleanup() throws Exception {
		AssertionError fatal = new AssertionError("fatal reconciliation");
		SecurityException rejection = new SecurityException("interrupt denied");
		CountDownLatch receiving = new CountDownLatch(1);
		CountDownLatch releaseReceive = new CountDownLatch(1);
		CountDownLatch rejected = new CountDownLatch(1);
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofMillis(10))
				.threadFactory(role -> task -> rejectingThread(role, task, rejection, rejected))
				.reconcile(() -> { if (receiving.getCount() == 0) throw fatal; }),
				operation -> operation.perform(wait -> {
					receiving.countDown(); releaseReceive.await(); return List.of();
				}));
		try {
			supervisor.start();
			await(rejected);
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
			releaseReceive.countDown();
			NotificationSupervisor.ShutdownResult result = result(supervisor);
			assertSame(fatal, result.getFailure().orElseThrow());
			assertArrayEquals(new Throwable[]{rejection}, fatal.getSuppressed());
			assertFalse(result.isShutdownRequested());
		} finally {
			releaseReceive.countDown();
			supervisor.requestShutdown();
			result(supervisor);
		}
	}

	@Test
	void legacyExecutorShutdownAlsoRetainsRejectedInterruption() throws Exception {
		SecurityException rejection = new SecurityException("interrupt denied");
		CountDownLatch receiving = new CountDownLatch(1);
		CountDownLatch releaseReceive = new CountDownLatch(1);
		AtomicReference<Thread> worker = new AtomicReference<>();
		NotificationSupervisor supervisor = build(builder(), operation -> operation.perform(wait -> {
			receiving.countDown(); releaseReceive.await(); return List.of();
		}));
		try {
			supervisor.start(task -> {
				Thread thread = rejectingThread(NotificationSupervisor.ThreadRole.LISTENER, task, rejection, null);
				worker.set(thread); thread.start();
			});
			await(receiving);
			assertDoesNotThrow(supervisor::requestShutdown);
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
			releaseReceive.countDown();
			assertSame(rejection, result(supervisor).getFailure().orElseThrow());
		} finally {
			releaseReceive.countDown(); supervisor.requestShutdown(); result(supervisor);
			worker.get().join(2_000); assertFalse(worker.get().isAlive());
		}
	}

	@Test
	void legacyFailureKeepsItsIdentityWhenShutdownInterruptionAlsoFails() throws Exception {
		DatabaseException primary = new DatabaseException("listener unavailable");
		SecurityException rejection = new SecurityException("interrupt denied");
		CountDownLatch backingOff = new CountDownLatch(1);
		CountDownLatch releaseBackoff = new CountDownLatch(1);
		AtomicReference<Thread> worker = new AtomicReference<>();
		NotificationSupervisor supervisor = builder().retry(failure -> true,
				RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1))
				.build(operation -> { throw primary; }, System::nanoTime, delay -> {
					backingOff.countDown(); releaseBackoff.await();
				});
		try {
			supervisor.start(task -> {
				Thread thread = rejectingThread(NotificationSupervisor.ThreadRole.LISTENER, task, rejection, null);
				worker.set(thread); thread.start();
			});
			await(backingOff);
			assertDoesNotThrow(supervisor::requestShutdown);
			releaseBackoff.countDown();
			assertSame(primary, result(supervisor).getFailure().orElseThrow());
			assertArrayEquals(new Throwable[]{rejection}, primary.getSuppressed());
		} finally {
			releaseBackoff.countDown(); supervisor.requestShutdown(); result(supervisor);
			worker.get().join(2_000); assertFalse(worker.get().isAlive());
		}
	}

	@Test
	void failureAccessorsCanReadStatusAndRequestShutdownFromAnotherThread() throws Exception {
		CountDownLatch receiving = new CountDownLatch(1);
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		ConcurrentLinkedQueue<Thread> readers = new ConcurrentLinkedQueue<>();
		AtomicBoolean blockedReader = new AtomicBoolean();
		AtomicInteger inspections = new AtomicInteger();
		AssertionError fatal = new AssertionError("fatal reconciliation");
		DatabaseException cleanup = new DatabaseException("cleanup failed") {
			@Override public Throwable getCause() {
				inspections.incrementAndGet();
				CountDownLatch read = new CountDownLatch(1);
				Thread reader = new Thread(() -> {
					reference.get().snapshot();
					reference.get().requestShutdown();
					read.countDown();
				});
				reader.setDaemon(true); readers.add(reader); reader.start();
				if (!awaitUninterruptibly(read, 1, TimeUnit.SECONDS)) blockedReader.set(true);
				return null;
			}
		};
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofMillis(10))
				.threadFactory(role -> task -> daemonThread(task))
				.reconcile(() -> { if (receiving.getCount() == 0) throw fatal; }), operation -> {
			try {
				operation.perform(wait -> { receiving.countDown(); new CountDownLatch(1).await(); return List.of(); });
			} finally { throw cleanup; }
		});
		reference.set(supervisor);
		try {
			supervisor.start();
			assertSame(fatal, result(supervisor).getFailure().orElseThrow());
			assertArrayEquals(new Throwable[]{cleanup}, fatal.getSuppressed());
			assertTrue(inspections.get() >= 2, "Did not exercise both normalization and aggregation");
			assertFalse(blockedReader.get(), "Failure inspection blocked lifecycle control on another thread");
		} finally {
			supervisor.requestShutdown(); result(supervisor);
			for (Thread reader : readers) { reader.join(2_000); assertFalse(reader.isAlive()); }
		}
	}

	@Test
	void shutdownResultWaitsForDiagnosticAggregationWithoutHoldingTheLifecycleLock() throws Exception {
		CountDownLatch receiving = new CountDownLatch(1);
		CountDownLatch inspecting = new CountDownLatch(1);
		CountDownLatch releaseInspection = new CountDownLatch(1);
		AtomicInteger inspections = new AtomicInteger();
		AssertionError fatal = new AssertionError("fatal reconciliation");
		DatabaseException cleanup = new DatabaseException("cleanup failed") {
			@Override public Throwable getCause() {
				if (inspections.incrementAndGet() == 2) {
					inspecting.countDown(); awaitUninterruptibly(releaseInspection);
				}
				return null;
			}
		};
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofMillis(10))
				.threadFactory(role -> task -> daemonThread(task))
				.reconcile(() -> { if (receiving.getCount() == 0) throw fatal; }), operation -> {
			try {
				operation.perform(wait -> { receiving.countDown(); new CountDownLatch(1).await(); return List.of(); });
			} finally { throw cleanup; }
		});
		Thread controller = null;
		try {
			supervisor.start(); await(inspecting);
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
			CountDownLatch controlled = new CountDownLatch(1);
			controller = new Thread(() -> { supervisor.snapshot(); supervisor.requestShutdown(); controlled.countDown(); });
			controller.setDaemon(true); controller.start();
			await(controlled);
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
			releaseInspection.countDown();
			NotificationSupervisor.ShutdownResult result = result(supervisor);
			assertSame(fatal, result.getFailure().orElseThrow());
			assertArrayEquals(new Throwable[]{cleanup}, fatal.getSuppressed());
			assertTrue(result.isShutdownRequested());
		} finally {
			releaseInspection.countDown(); supervisor.requestShutdown(); result(supervisor);
			if (controller != null) { controller.join(2_000); assertFalse(controller.isAlive()); }
		}
	}

	private static NotificationSupervisor.Builder builder() {
		return NotificationSupervisor.withDatabase(DATABASE).channels(Set.of("changed")).reconcile(() -> {});
	}

	private static NotificationSupervisor build(NotificationSupervisor.Builder builder,
			NotificationSupervisor.SessionAttempt attempt) {
		return builder.build(attempt, System::nanoTime, delay -> {});
	}

	private static Thread rejectingThread(NotificationSupervisor.ThreadRole role, Runnable task,
			Throwable rejection, CountDownLatch rejected) {
		Thread thread = role == NotificationSupervisor.ThreadRole.LISTENER ? new Thread(task) {
			@Override public void interrupt() {
				if (rejected != null) rejected.countDown();
				if (rejection instanceof Error error) throw error;
				throw (RuntimeException) rejection;
			}
		} : new Thread(task);
		thread.setDaemon(true); thread.setUncaughtExceptionHandler((worker, failure) -> {});
		return thread;
	}

	private static Thread daemonThread(Runnable task) {
		Thread thread = new Thread(task); thread.setDaemon(true);
		thread.setUncaughtExceptionHandler((worker, failure) -> {}); return thread;
	}

	private static NotificationSupervisor.ShutdownResult result(NotificationSupervisor supervisor) throws Exception {
		return supervisor.awaitShutdownAsync().toCompletableFuture().get(5, TimeUnit.SECONDS);
	}

	private static void await(CountDownLatch latch) throws InterruptedException {
		assertTrue(latch.await(3, TimeUnit.SECONDS), "Timed out waiting for test phase");
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		assertTrue(awaitUninterruptibly(latch, 3, TimeUnit.SECONDS), "Timed out waiting for test phase");
	}

	private static boolean awaitUninterruptibly(CountDownLatch latch, long timeout, TimeUnit unit) {
		long deadline = System.nanoTime() + unit.toNanos(timeout);
		boolean interrupted = Thread.interrupted();
		try {
			while (true) {
				try { return latch.await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
				catch (InterruptedException ignored) { interrupted = true; }
			}
		} finally { if (interrupted) Thread.currentThread().interrupt(); }
	}
}
