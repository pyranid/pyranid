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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/** Keeps readiness and freshness publication consistent with terminal failure and cancellation. */
@Timeout(15)
class NotificationReadinessFreshnessTests {
	private static final Database DATABASE = Database.with((DataSource) Proxy.newProxyInstance(
			NotificationReadinessFreshnessTests.class.getClassLoader(), new Class<?>[]{DataSource.class},
			(proxy, method, args) -> { throw new AssertionError("Unexpected data source call: " + method); }), DatabaseType.POSTGRESQL).build();

	@Test
	void recordedTerminalFailureCannotBeOverwrittenByFirstReadinessWhenInterruptIsRejected() throws Exception {
		CountDownLatch beforeReadiness = new CountDownLatch(1);
		CountDownLatch fatalRecorded = new CountDownLatch(1);
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		AtomicBoolean firstInterrupt = new AtomicBoolean();
		AtomicBoolean fatalWhileUnready = new AtomicBoolean();
		AtomicReference<NotificationSupervisor.State> failureState = new AtomicReference<>();
		AtomicInteger receives = new AtomicInteger();
		AssertionError fatal = new AssertionError("Periodic reconciliation failed before readiness publication");
		SecurityException rejection = new SecurityException("Listener interruption denied");
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofMillis(1))
				.threadFactory(role -> task -> {
			Thread thread = role == NotificationSupervisor.ThreadRole.LISTENER
					? new Thread(task, "failure-readiness-listener") {
				@Override public void interrupt() {
					if (firstInterrupt.compareAndSet(false, true)) {
						NotificationSupervisor current = reference.get();
						fatalWhileUnready.set(!current.firstReady().toCompletableFuture().isDone()
								&& !current.snapshot().isEverReady());
						failureState.set(current.snapshot().getState());
						fatalRecorded.countDown();
					}
					throw rejection;
				}
			} : new Thread(task, "failure-readiness-reconciler");
			thread.setDaemon(true);
			thread.setUncaughtExceptionHandler((worker, failure) -> {});
			return thread;
		}).reconcile(() -> {
			if (beforeReadiness.getCount() == 0)
				throw fatal;
		}), operation -> operation.perform(wait -> {
			receives.incrementAndGet();
			throw new AssertionError("A terminal startup failure must not reach notification receive");
		}));
		reference.set(supervisor);

		// Model a scheduling pause after the preceding stop check and before the atomic readiness check.
		ReentrantLock readinessGate = new ReentrantLock() {
			@Override public void lock() {
				if (Thread.currentThread().getName().equals("failure-readiness-listener")
						&& StackWalker.getInstance().walk(frames -> frames.skip(1).findFirst().orElseThrow()
								.getMethodName()).equals("consume")) {
					beforeReadiness.countDown();
					awaitUninterruptibly(fatalRecorded);
				}
				super.lock();
			}
		};
		var lockField = NotificationSupervisor.class.getDeclaredField("lifecycleLock");
		lockField.setAccessible(true);
		lockField.set(supervisor, readinessGate);
		try {
			supervisor.start();
			assertSame(fatal, result(supervisor).getFailure().orElseThrow());
			assertTrue(fatalWhileUnready.get(), "Fatal failure did not precede readiness publication");
			assertEquals(NotificationSupervisor.State.STOPPING, failureState.get());
			assertTrue(supervisor.firstReady().toCompletableFuture().isCompletedExceptionally());
			assertFalse(supervisor.snapshot().isEverReady());
			assertEquals(0, receives.get());
		} finally {
			supervisor.requestShutdown();
			result(supervisor);
		}
	}

	@Test
	void interruptedInitialLegacyPassCannotCreateASuccessTimestamp() throws Exception {
		AtomicBoolean interruptedAtExit = new AtomicBoolean();
		NotificationSupervisor supervisor = build(builder().reconcile(() -> Thread.currentThread().interrupt()),
				operation -> operation.perform(wait -> fail("Cancelled initial reconciliation reached receive")));
		Thread worker = startLegacy(supervisor, interruptedAtExit);
		try {
			assertTrue(result(supervisor).getFailure().isEmpty());
			join(worker);
			assertTrue(interruptedAtExit.get());
			assertTrue(supervisor.firstReady().toCompletableFuture().isCompletedExceptionally());
			assertTrue(supervisor.snapshot().getLastSuccessfulReconciliation().isEmpty());
			assertFalse(supervisor.snapshot().isEverReady());
		} finally {
			supervisor.requestShutdown();
			result(supervisor);
			join(worker);
		}
	}

	@Test
	void interruptedLegacyFollowUpKeepsThePreviousSuccessfulTimestamp() throws Exception {
		AtomicInteger passes = new AtomicInteger();
		AtomicReference<Instant> firstSuccess = new AtomicReference<>();
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		NotificationSupervisor supervisor = build(builder().reconcile(() -> {
			if (passes.incrementAndGet() == 2)
				Thread.currentThread().interrupt();
		}), operation -> operation.perform(wait -> {
			Instant success = reference.get().snapshot().getLastSuccessfulReconciliation().orElseThrow();
			firstSuccess.set(success);
			// Ensure that an erroneous timestamp update would be observably newer than the initial one.
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
			while (!Instant.now().isAfter(success)) {
				assertTrue(System.nanoTime() - deadline < 0, "Wall clock did not advance");
				Thread.yield();
			}
			return List.of(Notification.of("changed", ""));
		}));
		reference.set(supervisor);
		Thread worker = startLegacy(supervisor, new AtomicBoolean());
		try {
			assertTrue(result(supervisor).getFailure().isEmpty());
			assertEquals(2, passes.get());
			assertTrue(supervisor.firstReady().toCompletableFuture().isDone());
			assertFalse(supervisor.firstReady().toCompletableFuture().isCompletedExceptionally());
			assertEquals(firstSuccess.get(), supervisor.snapshot().getLastSuccessfulReconciliation().orElseThrow());
			assertTrue(supervisor.snapshot().getLastReconciliationFailure().isEmpty());
		} finally {
			supervisor.requestShutdown();
			result(supervisor);
			join(worker);
		}
	}

	@Test
	void interruptedLegacyRetryDoesNotClearTheUnrecoveredFailure() throws Exception {
		AtomicInteger passes = new AtomicInteger();
		DatabaseException failure = new DatabaseException("Initial reconciliation failed");
		NotificationSupervisor supervisor = build(builder().reconcile(() -> {
			if (passes.incrementAndGet() == 1)
				throw failure;
			Thread.currentThread().interrupt();
		}).retry(ignored -> true, RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)),
				operation -> operation.perform(wait -> fail("Interrupted retry reached receive")));
		Thread worker = startLegacy(supervisor, new AtomicBoolean());
		try {
			Throwable terminal = result(supervisor).getFailure().orElseThrow();
			assertTrue(contains(terminal, current -> current == failure));
			assertTrue(contains(terminal, current -> current instanceof InterruptedException));
			assertEquals(2, passes.get());
			assertTrue(supervisor.firstReady().toCompletableFuture().isCompletedExceptionally());
			assertTrue(supervisor.snapshot().getLastSuccessfulReconciliation().isEmpty());
			assertSame(failure, supervisor.snapshot().getLastReconciliationFailure().orElseThrow());
		} finally {
			supervisor.requestShutdown();
			result(supervisor);
			join(worker);
		}
	}

	@Test
	void successfulLegacyRetryClearsEarlierFailureBeforeReceiveCancellation() throws Exception {
		AtomicInteger passes = new AtomicInteger();
		DatabaseException failure = new DatabaseException("Recoverable initial reconciliation failure");
		NotificationSupervisor supervisor = build(builder().reconcile(() -> {
			if (passes.incrementAndGet() == 1)
				throw failure;
		}).retry(ignored -> true, RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)),
				operation -> operation.perform(wait -> { throw new InterruptedException(); }));
		AtomicBoolean interruptedAtExit = new AtomicBoolean();
		Thread worker = startLegacy(supervisor, interruptedAtExit);
		try {
			assertTrue(result(supervisor).getFailure().isEmpty());
			join(worker);
			assertTrue(interruptedAtExit.get());
			assertEquals(2, passes.get());
			assertTrue(supervisor.firstReady().toCompletableFuture().isDone());
			assertFalse(supervisor.firstReady().toCompletableFuture().isCompletedExceptionally());
			assertTrue(supervisor.snapshot().getLastSuccessfulReconciliation().isPresent());
			assertTrue(supervisor.snapshot().getLastReconciliationFailure().isEmpty());
		} finally {
			supervisor.requestShutdown();
			result(supervisor);
			join(worker);
		}
	}

	private static boolean contains(Throwable root, Predicate<Throwable> match) {
		Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		ArrayDeque<Throwable> pending = new ArrayDeque<>();
		pending.add(root);
		while (!pending.isEmpty()) {
			Throwable current = pending.removeLast();
			if (!visited.add(current))
				continue;
			if (match.test(current))
				return true;
			if (current.getCause() != null)
				pending.add(current.getCause());
			Collections.addAll(pending, current.getSuppressed());
		}
		return false;
	}

	private static NotificationSupervisor.Builder builder() {
		return NotificationSupervisor.withDatabase(DATABASE).channels(Set.of("changed")).reconcile(() -> {});
	}

	private static NotificationSupervisor build(NotificationSupervisor.Builder builder,
			NotificationSupervisor.SessionAttempt attempt) {
		return builder.build(attempt, System::nanoTime, delay -> {});
	}

	private static Thread startLegacy(NotificationSupervisor supervisor, AtomicBoolean interruptedAtExit) {
		AtomicReference<Thread> worker = new AtomicReference<>();
		supervisor.start(task -> {
			Thread thread = new Thread(() -> {
				task.run();
				interruptedAtExit.set(Thread.currentThread().isInterrupted());
			}, "legacy-freshness-worker");
			thread.setDaemon(true);
			worker.set(thread);
			thread.start();
		});
		return worker.get();
	}

	private static NotificationSupervisor.ShutdownResult result(NotificationSupervisor supervisor) throws Exception {
		return supervisor.awaitShutdownAsync().toCompletableFuture().get(5, TimeUnit.SECONDS);
	}

	private static void join(Thread worker) throws InterruptedException {
		worker.join(5_000);
		assertFalse(worker.isAlive(), "Notification worker leaked");
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		try {
			while (true) {
				try {
					assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for test phase");
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
}
