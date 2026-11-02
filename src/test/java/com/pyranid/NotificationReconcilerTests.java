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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/** Deterministic barriers exercise the internal generation, retry and draining contracts. */
@Timeout(15)
class NotificationReconcilerTests {
	@Test
	void noPollingDoesNotRunBeforeAListenerRequest() throws Exception {
		AtomicInteger calls = new AtomicInteger();
		try (Harness harness = new Harness(calls::incrementAndGet, null)) {
			harness.start();
			until(() -> harness.worker.getState() == Thread.State.WAITING);
			assertEquals(0, calls.get());
			harness.request().get(5, TimeUnit.SECONDS);
			assertEquals(1, calls.get());
			assertNotNull(harness.reconciler.snapshot().lastSuccessTime());
			assertNotNull(harness.reconciler.snapshot().lastSuccessNanos());
		}
	}

	@Test
	void pollingStartsWithoutAListenerAndRecoversAfterDatabaseFailure() throws Exception {
		AtomicInteger calls = new AtomicInteger();
		CountDownLatch successes = new CountDownLatch(2);
		DatabaseException expected = new DatabaseException("poll failed");
		try (Harness harness = new Harness(() -> {
			if (calls.incrementAndGet() == 1)
				throw expected;
			successes.countDown();
		}, Duration.ofMillis(10))) {
			harness.start();
			await(successes);
			until(() -> harness.reconciler.snapshot().lastSuccessTime() != null);
			assertSame(expected, harness.reported.get());
			assertNull(harness.fatal.get());
			assertNull(harness.reconciler.snapshot().lastFailure());
			assertTrue(calls.get() >= 3);
		}
	}

	@Test
	void requestsDuringAnOlderPassRequireOneFreshFollowUpAndNeverOverlap() throws Exception {
		CountDownLatch firstStarted = new CountDownLatch(1);
		CountDownLatch finishFirst = new CountDownLatch(1);
		CountDownLatch secondStarted = new CountDownLatch(1);
		CountDownLatch finishSecond = new CountDownLatch(1);
		CountDownLatch thirdStarted = new CountDownLatch(1);
		CountDownLatch finishThird = new CountDownLatch(1);
		AtomicInteger calls = new AtomicInteger();
		AtomicInteger active = new AtomicInteger();
		AtomicInteger maximumActive = new AtomicInteger();
		try (Harness harness = new Harness(() -> {
			maximumActive.accumulateAndGet(active.incrementAndGet(), Math::max);
			try {
				switch (calls.incrementAndGet()) {
					case 1 -> { firstStarted.countDown(); awaitUninterruptibly(finishFirst); }
					case 2 -> { secondStarted.countDown(); awaitUninterruptibly(finishSecond); }
					case 3 -> { thirdStarted.countDown(); awaitUninterruptibly(finishThird); }
					default -> fail("unexpected extra reconciliation");
				}
			} finally {
				active.decrementAndGet();
			}
		}, Duration.ofDays(1))) {
			try {
				harness.start();
				await(firstStarted);
				CompletableFuture<Void> firstRequest = harness.request();
				CompletableFuture<Void> secondRequest = harness.request();
				until(() -> harness.callers.size() == 2
						&& harness.callers.stream().allMatch(t -> t.getState() == Thread.State.WAITING));
				assertTrue(harness.reconciler.snapshot().pending());
				finishFirst.countDown();
				await(secondStarted);
				assertFalse(firstRequest.isDone());
				assertFalse(secondRequest.isDone());
				CompletableFuture<Void> thirdRequest = harness.request();
				until(() -> harness.reconciler.snapshot().pending());
				finishSecond.countDown();
				firstRequest.get(5, TimeUnit.SECONDS);
				secondRequest.get(5, TimeUnit.SECONDS);
				await(thirdStarted);
				assertFalse(thirdRequest.isDone());
				finishThird.countDown();
				thirdRequest.get(5, TimeUnit.SECONDS);
				assertEquals(3, calls.get());
				assertEquals(1, maximumActive.get());
			} finally {
				finishFirst.countDown();
				finishSecond.countDown();
				finishThird.countDown();
			}
		}
	}

	@Test
	void failedFreshPassDoesNotReleaseReadinessUntilALaterPollSucceeds() throws Exception {
		AtomicInteger calls = new AtomicInteger();
		CountDownLatch retryStarted = new CountDownLatch(1);
		CountDownLatch finishRetry = new CountDownLatch(1);
		DatabaseException failure = new DatabaseException("initial snapshot failed");
		try (Harness harness = new Harness(() -> {
			if (calls.incrementAndGet() == 1)
				throw failure;
			retryStarted.countDown();
			awaitUninterruptibly(finishRetry);
		}, Duration.ofMillis(10))) {
			try {
				CompletableFuture<Void> request = harness.request();
				until(() -> harness.reconciler.snapshot().pending());
				harness.start();
				await(retryStarted);
				assertFalse(request.isDone());
				assertSame(failure, harness.reported.get());
				finishRetry.countDown();
				request.get(5, TimeUnit.SECONDS);
				assertNull(harness.fatal.get());
			} finally {
				finishRetry.countDown();
			}
		}
	}

	@Test
	void noPollingReturnsFailureToListenerAndAllowsItsNextAttempt() throws Exception {
		AtomicInteger calls = new AtomicInteger();
		DatabaseException failure = new DatabaseException("snapshot failed");
		try (Harness harness = new Harness(() -> {
			if (calls.incrementAndGet() == 1)
				throw failure;
		}, null)) {
			harness.start();
			assertSame(failure, requestFailure(harness.request()));
			harness.request().get(5, TimeUnit.SECONDS);
			assertEquals(2, calls.get());
			assertNull(harness.fatal.get());
			assertNull(harness.reconciler.snapshot().lastFailure());
		}
	}

	@Test
	void stopSealsNewRequestsAndDrainsAlreadyAcceptedRequests() throws Exception {
		AtomicInteger calls = new AtomicInteger();
		try (Harness harness = new Harness(calls::incrementAndGet, null)) {
			CompletableFuture<Void> request = harness.request();
			until(() -> harness.reconciler.snapshot().pending());
			harness.reconciler.stop();
			assertThrows(CancellationException.class, harness.reconciler::reconcileAndAwait);
			harness.start();
			request.get(5, TimeUnit.SECONDS);
			harness.worker.join(5_000);
			assertFalse(harness.worker.isAlive());
			assertEquals(1, calls.get());
			assertNull(harness.fatal.get());
		}
	}

	@Test
	void stoppingPollingReleasesAWaiterWhoseFailedPassWasAwaitingTheTimer() throws Exception {
		CountDownLatch reported = new CountDownLatch(1);
		DatabaseException failure = new DatabaseException("snapshot failed");
		try (Harness harness = new Harness(() -> { throw failure; }, Duration.ofDays(1),
				ignored -> reported.countDown())) {
			CompletableFuture<Void> request = harness.request();
			until(() -> harness.reconciler.snapshot().pending());
			harness.start();
			await(reported);
			assertFalse(request.isDone());
			harness.reconciler.stopPolling();
			assertSame(failure, requestFailure(request));
			assertNull(harness.fatal.get());
		}
	}

	@Test
	void programmingFailureStopsTheWorkerAndFailsItsWaiter() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch failPass = new CountDownLatch(1);
		IllegalArgumentException failure = new IllegalArgumentException("bad cache mapping");
		try (Harness harness = new Harness(() -> {
			entered.countDown();
			awaitUninterruptibly(failPass);
			throw failure;
		}, Duration.ofDays(1))) {
			try {
				harness.start();
				await(entered);
				CompletableFuture<Void> request = harness.request();
				until(() -> harness.reconciler.snapshot().pending());
				failPass.countDown();
				assertSame(failure, requestFailure(request));
				harness.worker.join(5_000);
				assertSame(failure, harness.fatal.get());
				assertSame(failure, harness.reconciler.snapshot().lastFailure());
				assertFalse(harness.reconciler.snapshot().pending());
				assertFalse(harness.worker.isAlive());
				assertSame(failure, assertThrows(IllegalArgumentException.class,
						harness.reconciler::reconcileAndAwait));
			} finally {
				failPass.countDown();
			}
		}
	}

	@Test
	void failureObserverFailureIsTerminalAndRetainsTheDatabaseFailure() throws Exception {
		DatabaseException databaseFailure = new DatabaseException("snapshot failed");
		IllegalStateException observerFailure = new IllegalStateException("observer failed");
		try (Harness harness = new Harness(() -> { throw databaseFailure; }, Duration.ofDays(1),
				ignored -> { throw observerFailure; })) {
			harness.start();
			harness.worker.join(5_000);
			assertSame(observerFailure, harness.fatal.get());
			assertArrayEquals(new Throwable[]{databaseFailure}, observerFailure.getSuppressed());
			assertFalse(harness.worker.isAlive());
		}
	}

	@Test
	void interruptingAWaiterDoesNotCancelItsAlreadyAcceptedRefresh() throws Exception {
		CountDownLatch started = new CountDownLatch(1);
		CountDownLatch finish = new CountDownLatch(1);
		AtomicInteger calls = new AtomicInteger();
		try (Harness harness = new Harness(() -> {
			calls.incrementAndGet();
			started.countDown();
			awaitUninterruptibly(finish);
		}, null)) {
			try {
				harness.start();
				CompletableFuture<Void> request = harness.request();
				await(started);
				harness.callers.get(0).interrupt();
				assertInstanceOf(InterruptedException.class, requestFailure(request));
				harness.reconciler.stop();
				assertTrue(harness.worker.isAlive());
				finish.countDown();
				harness.worker.join(5_000);
				assertFalse(harness.worker.isAlive());
				assertEquals(1, calls.get());
				assertNotNull(harness.reconciler.snapshot().lastSuccessTime());
				assertNull(harness.fatal.get());
			} finally {
				finish.countDown();
			}
		}
	}

	@Test
	void reconcilerCanRequestItsOwnStopWithoutDeadlock() throws Exception {
		AtomicReference<NotificationReconciler> reference = new AtomicReference<>();
		try (Harness harness = new Harness(() -> reference.get().stop(), Duration.ofDays(1))) {
			reference.set(harness.reconciler);
			harness.start();
			harness.worker.join(5_000);
			assertFalse(harness.worker.isAlive());
			assertNull(harness.fatal.get());
		}
	}

	private static Throwable requestFailure(CompletableFuture<Void> request) throws Exception {
		return assertThrows(ExecutionException.class, () -> request.get(5, TimeUnit.SECONDS)).getCause();
	}

	private static void await(CountDownLatch latch) throws InterruptedException {
		assertTrue(latch.await(5, TimeUnit.SECONDS), "barrier did not open");
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		try {
			while (true) {
				try {
					await(latch);
					return;
				} catch (InterruptedException ignored) {
					interrupted = true;
				}
			}
		} finally {
			if (interrupted)
				Thread.currentThread().interrupt();
		}
	}

	private static void until(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() - deadline < 0)
			Thread.sleep(1);
		assertTrue(condition.getAsBoolean(), "condition did not become true");
	}

	private static final class Harness implements AutoCloseable {
		private final AtomicReference<Throwable> reported = new AtomicReference<>();
		private final AtomicReference<Throwable> fatal = new AtomicReference<>();
		private final List<Thread> callers = new ArrayList<>();
		private final NotificationReconciler reconciler;
		private final Thread worker;

		private Harness(Runnable work, Duration pollInterval) {
			this(work, pollInterval, ignored -> {});
		}

		private Harness(Runnable work, Duration pollInterval, Consumer<Throwable> observer) {
			this.reconciler = new NotificationReconciler(work, pollInterval, System::nanoTime,
					failure -> failure instanceof DatabaseException, failure -> {
						this.reported.set(failure);
						observer.accept(failure);
					}, this.fatal::set);
			this.worker = new Thread(this.reconciler, "test-reconciler");
			this.worker.setDaemon(true);
		}

		private void start() {
			this.worker.start();
		}

		private CompletableFuture<Void> request() {
			CompletableFuture<Void> result = new CompletableFuture<>();
			Thread caller = new Thread(() -> {
				try {
					this.reconciler.reconcileAndAwait();
					result.complete(null);
				} catch (Throwable failure) {
					result.completeExceptionally(failure);
				}
			}, "test-reconciliation-request");
			caller.setDaemon(true);
			this.callers.add(caller);
			caller.start();
			return result;
		}

		@Override
		public void close() throws InterruptedException {
			this.reconciler.stop();
			this.worker.join(5_000);
			assertFalse(this.worker.isAlive(), "reconciliation worker leaked");
			for (Thread caller : this.callers) {
				caller.join(5_000);
				assertFalse(caller.isAlive(), "reconciliation waiter leaked");
			}
		}
	}
}
