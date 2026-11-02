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
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** Protects decoded hints and failure evidence when cancellation races reconciliation admission or recovery. */
@Timeout(15)
class NotificationAdmissionCancellationTests {
	private static final Database DATABASE = Database.with((DataSource) Proxy.newProxyInstance(
			NotificationAdmissionCancellationTests.class.getClassLoader(), new Class<?>[]{DataSource.class},
			(proxy, method, args) -> { throw new AssertionError("Unexpected data source call: " + method); }), DatabaseType.POSTGRESQL).build();

	@Test
	void decodedBatchIsAcceptedWhenShutdownWinsAfterTheStopCheck() throws Exception {
		for (boolean polling : new boolean[]{false, true})
			runDecodedBatchRace(polling, false);
	}

	@Test
	void failedFinalDecodedBatchIsDrainedAndRetained() throws Exception {
		for (boolean polling : new boolean[]{false, true})
			runDecodedBatchRace(polling, true);
	}

	private static void runDecodedBatchRace(boolean polling, boolean failFinalPass) throws Exception {
		AtomicInteger passes = new AtomicInteger();
		AtomicBoolean finalPass = new AtomicBoolean();
		CountDownLatch decoded = new CountDownLatch(1);
		CountDownLatch deliver = new CountDownLatch(1);
		DatabaseException failure = new DatabaseException("Final decoded batch reconciliation failed");
		List<Notification> hints = new AbstractList<>() {
			@Override public Notification get(int index) { return Notification.of("changed", ""); }
			@Override public int size() { return 1; }
			@Override public boolean isEmpty() {
				// Pause after consume has sampled stop/interrupt, as an ordinary scheduling delay can.
				decoded.countDown();
				awaitUninterruptibly(deliver);
				return false;
			}
		};
		NotificationSupervisor.Builder builder = builder().reconcile(() -> {
			passes.incrementAndGet();
			if (finalPass.get() && failFinalPass)
				throw failure;
		});
		if (polling)
			builder.pollEvery(Duration.ofDays(1));
		NotificationSupervisor supervisor = build(builder, operation -> operation.perform(wait -> hints));
		try {
			supervisor.start();
			await(decoded);
			int initialPasses = passes.get();
			finalPass.set(true);
			supervisor.requestShutdown();
			deliver.countDown();
			NotificationSupervisor.ShutdownResult result = result(supervisor);
			assertEquals(initialPasses + 1, passes.get(), "Decoded batch was discarded; polling=" + polling);
			if (failFinalPass)
				assertContains(result.getFailure().orElse(null), failure);
			else
				assertTrue(result.getFailure().isEmpty());
			assertTrue(result.isShutdownRequested());
		} finally {
			deliver.countDown();
			stop(supervisor);
		}
	}

	@Test
	void alreadyInterruptedWaiterStillAdmitsWorkAndCanCancelItsWait() throws Exception {
		AtomicInteger passes = new AtomicInteger();
		AtomicReference<Throwable> terminal = new AtomicReference<>();
		AtomicReference<Throwable> waitFailure = new AtomicReference<>();
		NotificationReconciler reconciler = new NotificationReconciler(passes::incrementAndGet, null,
				System::nanoTime, failure -> false, failure -> fail(failure), terminal::set);
		Thread waiter = daemonThread(() -> {
			Thread.currentThread().interrupt();
			try { reconciler.reconcileAndAwait(); }
			catch (Throwable failure) { waitFailure.set(failure); }
		});
		Thread worker = daemonThread(reconciler);
		try {
			waiter.start();
			join(waiter);
			assertInstanceOf(InterruptedException.class, waitFailure.get());
			assertTrue(reconciler.snapshot().pending(), "Cancellation discarded the request before admission");
			reconciler.stop();
			worker.start();
			join(worker);
			assertEquals(1, passes.get());
			assertNull(terminal.get());
			assertNotNull(reconciler.snapshot().lastSuccessTime());
		} finally {
			reconciler.stop();
			if (worker.getState() == Thread.State.NEW) worker.start();
			join(worker);
			join(waiter);
		}
	}

	@Test
	void interruptionWhileWaitingForAdmissionCannotDiscardTheRequest() throws Exception {
		AtomicInteger passes = new AtomicInteger();
		AtomicReference<Throwable> waitFailure = new AtomicReference<>();
		NotificationReconciler reconciler = new NotificationReconciler(passes::incrementAndGet, null,
				System::nanoTime, failure -> false, failure -> fail(failure), failure -> fail(failure));
		var field = NotificationReconciler.class.getDeclaredField("lock");
		field.setAccessible(true);
		ReentrantLock admission = (ReentrantLock) field.get(reconciler);
		Thread waiter = daemonThread(() -> {
			try { reconciler.reconcileAndAwait(); }
			catch (Throwable failure) { waitFailure.set(failure); }
		});
		Thread worker = daemonThread(reconciler);
		try {
			admission.lock();
			try {
				waiter.start();
				until(() -> admission.hasQueuedThread(waiter));
				waiter.interrupt();
			} finally {
				admission.unlock();
			}
			join(waiter);
			assertInstanceOf(InterruptedException.class, waitFailure.get());
			assertTrue(reconciler.snapshot().pending(), "A raced interrupt discarded the admitted request");
			reconciler.stop();
			worker.start();
			join(worker);
			assertEquals(1, passes.get());
		} finally {
			reconciler.stop();
			if (worker.getState() == Thread.State.NEW) worker.start();
			join(worker);
			join(waiter);
		}
	}

	@Test
	void restoringInterruptionInTheFailureObserverRetainsTheUnrecoveredFailure() throws Exception {
		for (boolean polling : new boolean[]{false, true}) {
			DatabaseException failure = new DatabaseException("Authoritative state refresh failed");
			CountDownLatch observed = new CountDownLatch(1);
			NotificationSupervisor.Builder builder = builder().reconcile(() -> { throw failure; })
					.onReconciliationFailure(observedFailure -> {
						assertSame(failure, observedFailure);
						observed.countDown();
						Thread.currentThread().interrupt();
					});
			if (polling)
				builder.pollEvery(Duration.ofDays(1));
			NotificationSupervisor supervisor = build(builder,
					operation -> operation.perform(wait -> fail("Failed reconciliation must not become ready")));
			try {
				supervisor.start();
				await(observed);
				Throwable terminal = result(supervisor).getFailure().orElseThrow();
				assertContains(terminal, failure);
				assertContainsType(terminal, InterruptedException.class);
				assertTrue(supervisor.snapshot().getLastSuccessfulReconciliation().isEmpty());
			} finally {
				stop(supervisor);
			}
		}
	}

	@Test
	void observerCatchingAndRestoringInterruptionRetainsTheDatabaseFailure() throws Exception {
		DatabaseException failure = new DatabaseException("Authoritative state refresh failed");
		CountDownLatch observing = new CountDownLatch(1);
		CountDownLatch releaseObserver = new CountDownLatch(1);
		AtomicReference<Thread> reconciliationWorker = new AtomicReference<>();
		AtomicBoolean interrupted = new AtomicBoolean();
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofDays(1))
				.threadFactory(role -> task -> {
					Thread thread = daemonThread(task);
					if (role == NotificationSupervisor.ThreadRole.RECONCILIATION)
						reconciliationWorker.set(thread);
					return thread;
				}).reconcile(() -> { throw failure; }).onReconciliationFailure(observed -> {
			assertSame(failure, observed);
			observing.countDown();
			try { releaseObserver.await(3, TimeUnit.SECONDS); }
			catch (InterruptedException cancellation) {
				interrupted.set(true);
				Thread.currentThread().interrupt();
			}
		}), operation -> operation.perform(wait -> fail("Failed reconciliation must not become ready")));
		try {
			supervisor.start();
			await(observing);
			reconciliationWorker.get().interrupt();
			Throwable terminal = result(supervisor).getFailure().orElseThrow();
			assertTrue(interrupted.get());
			assertContains(terminal, failure);
			assertContainsType(terminal, InterruptedException.class);
		} finally {
			releaseObserver.countDown();
			stop(supervisor);
		}
	}

	@Test
	void interruptedRecoveryPassRetainsThePreviousUnrecoveredFailure() throws Exception {
		DatabaseException failure = new DatabaseException("Initial recovery failure");
		AtomicInteger passes = new AtomicInteger();
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofMillis(5)).reconcile(() -> {
			if (passes.incrementAndGet() == 1)
				throw failure;
			Thread.currentThread().interrupt();
		}), operation -> operation.perform(wait -> fail("An interrupted recovery must not become ready")));
		try {
			supervisor.start();
			Throwable terminal = result(supervisor).getFailure().orElseThrow();
			assertContains(terminal, failure);
			assertContainsType(terminal, InterruptedException.class);
			assertEquals(2, passes.get());
			assertTrue(supervisor.snapshot().getLastSuccessfulReconciliation().isEmpty());
		} finally {
			stop(supervisor);
		}
	}

	@Test
	void aRecoveredFailureIsNotResurrectedByLaterCancellation() throws Exception {
		DatabaseException failure = new DatabaseException("Recovered initial failure");
		AtomicInteger passes = new AtomicInteger();
		AtomicBoolean cancel = new AtomicBoolean();
		CountDownLatch receiving = new CountDownLatch(1);
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofMillis(5)).reconcile(() -> {
			if (passes.incrementAndGet() == 1)
				throw failure;
			if (cancel.get())
				Thread.currentThread().interrupt();
		}), operation -> operation.perform(wait -> {
			receiving.countDown();
			new CountDownLatch(1).await();
			return List.of();
		}));
		try {
			supervisor.start();
			await(receiving);
			assertTrue(supervisor.snapshot().getLastReconciliationFailure().isEmpty());
			cancel.set(true);
			Throwable terminal = result(supervisor).getFailure().orElseThrow();
			assertContainsType(terminal, InterruptedException.class);
			assertFalse(contains(terminal, failure), "A recovered failure was incorrectly resurrected");
			assertTrue(supervisor.snapshot().getLastSuccessfulReconciliation().isPresent());
		} finally {
			stop(supervisor);
		}
	}

	private static NotificationSupervisor.Builder builder() {
		return NotificationSupervisor.withDatabase(DATABASE).channels(Set.of("changed")).reconcile(() -> {});
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

	private static void assertContains(Throwable root, Throwable expected) {
		assertTrue(contains(root, expected), "Failure evidence was lost: " + expected);
	}

	private static boolean contains(Throwable root, Throwable expected) {
		return inspect(root, current -> current == expected);
	}

	private static void assertContainsType(Throwable root, Class<? extends Throwable> type) {
		assertTrue(inspect(root, type::isInstance), "Missing terminal evidence of " + type.getSimpleName());
	}

	private static boolean inspect(Throwable root, java.util.function.Predicate<Throwable> evidence) {
		if (root == null) return false;
		Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		ArrayDeque<Throwable> pending = new ArrayDeque<>();
		pending.add(root);
		while (!pending.isEmpty()) {
			Throwable current = pending.removeFirst();
			if (!visited.add(current)) continue;
			if (evidence.test(current)) return true;
			if (current.getCause() != null) pending.add(current.getCause());
			Collections.addAll(pending, current.getSuppressed());
		}
		return false;
	}

	private static Thread daemonThread(Runnable task) {
		Thread thread = new Thread(task);
		thread.setDaemon(true);
		return thread;
	}

	private static void await(CountDownLatch latch) throws InterruptedException {
		assertTrue(latch.await(3, TimeUnit.SECONDS), "Timed out waiting for test phase");
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
		boolean interrupted = Thread.interrupted();
		try {
			while (true) {
				try {
					assertTrue(latch.await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS),
							"Timed out waiting for test phase");
					return;
				} catch (InterruptedException cancellation) {
					interrupted = true;
				}
			}
		} finally {
			if (interrupted) Thread.currentThread().interrupt();
		}
	}

	private static void until(BooleanSupplier condition) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
		while (!condition.getAsBoolean()) {
			assertTrue(System.nanoTime() - deadline < 0, "Timed out waiting for test condition");
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
		}
	}

	private static void join(Thread thread) throws InterruptedException {
		thread.join(5_000);
		assertFalse(thread.isAlive(), "Test worker leaked");
	}
}
