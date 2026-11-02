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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

/** Retry guards must retain diagnostic failures before a later readable scan can forget them. */
@Timeout(20)
class NotificationRetryInspectionTests {
	private static final Database DATABASE = Database.with((DataSource) Proxy.newProxyInstance(
			NotificationRetryInspectionTests.class.getClassLoader(), new Class<?>[]{DataSource.class},
			(proxy, method, args) -> { throw new AssertionError("Unexpected data source call: " + method); }), DatabaseType.POSTGRESQL).build();

	@AfterEach
	void clearCallingThreadInterruption() {
		Thread.interrupted();
	}

	@Test
	void managedCauseInspectionFailuresCannotAuthorizeRecovery() throws Exception {
		assertCauseInspectionStops(false);
	}

	@Test
	void legacyCauseInspectionFailuresSkipRetryPolicyAndBackoff() throws Exception {
		assertCauseInspectionStops(true);
	}

	private static void assertCauseInspectionStops(boolean legacy) throws Exception {
		for (int failingCall : new int[]{1, 2}) {
			for (Diagnostic diagnostic : diagnostics()) {
				AtomicInteger inspections = new AtomicInteger();
				DatabaseException original = new DatabaseException("Reconciliation failed") {
					@Override public Throwable getCause() {
						if (inspections.incrementAndGet() == failingCall)
							throwUnchecked(diagnostic.failure());
						return null;
					}
				};
				Outcome outcome = run(original, legacy, false);
				assertStoppedOnInspection(outcome, original, diagnostic);
				assertTrue(inspections.get() >= failingCall);
			}
		}
	}

	@Test
	void managedJdbcNextInspectionFailuresCannotAuthorizeRecovery() throws Exception {
		assertJdbcNextInspectionStops(false);
	}

	@Test
	void legacyJdbcNextInspectionFailuresSkipRetryPolicyAndBackoff() throws Exception {
		assertJdbcNextInspectionStops(true);
	}

	private static void assertJdbcNextInspectionStops(boolean legacy) throws Exception {
		for (int failingCall : new int[]{1, 2}) {
			for (Diagnostic diagnostic : diagnostics()) {
				AtomicBoolean armed = new AtomicBoolean();
				AtomicInteger inspections = new AtomicInteger();
				SQLException jdbcFailure = new SQLException("Read failed", "08006") {
					@Override public SQLException getNextException() {
						if (armed.get() && inspections.incrementAndGet() == failingCall)
							throwUnchecked(diagnostic.failure());
						return null;
					}
				};
				DatabaseException original = new DatabaseException("Reconciliation failed", jdbcFailure);
				armed.set(true);
				Outcome outcome = run(original, legacy, false);
				assertStoppedOnInspection(outcome, original, diagnostic);
				assertTrue(inspections.get() >= failingCall);
			}
		}
	}

	@Test
	void permanentlyUnreadableDiagnosticsAreRetainedOnceAcrossClassifiers() throws Exception {
		for (boolean legacy : new boolean[]{false, true}) {
			IllegalStateException diagnostic = new IllegalStateException("Diagnostic unavailable");
			DatabaseException original = new DatabaseException("Reconciliation failed") {
				@Override public Throwable getCause() { throw diagnostic; }
			};
			assertStoppedOnInspection(run(original, legacy, false), original, new Diagnostic(diagnostic, false));
			assertArrayEquals(new Throwable[]{diagnostic}, original.getSuppressed());
		}
	}

	@Test
	void pendingInterruptionSurvivesAccessorsThatConsumeTheFlag() throws Exception {
		for (boolean legacy : new boolean[]{false, true}) {
			DatabaseException original = new DatabaseException("Interrupted reconciliation") {
				@Override public Throwable getCause() {
					Thread.interrupted();
					return null;
				}
			};
			Outcome outcome = run(original, legacy, true);
			assertSame(original, outcome.result().getFailure().orElseThrow());
			assertEquals(1, outcome.passes());
			assertEquals(0, outcome.policyCalls());
			assertEquals(0, outcome.backoffCalls());
			assertEquals(0, outcome.recoveryReports());
			assertTrue(outcome.operationWorkerInterrupted());
		}
	}

	@Test
	void interruptionDeliveredDuringInspectionSurvivesTheNextAccessor() throws Exception {
		for (boolean legacy : new boolean[]{false, true}) {
			for (String consumptionPoint : List.of("nextScan", "childCause", "jdbcNext")) {
				CountDownLatch inspecting = new CountDownLatch(1);
				CountDownLatch releaseInspection = new CountDownLatch(1);
				AtomicReference<Thread> operationWorker = new AtomicReference<>();
				AtomicInteger inspections = new AtomicInteger();
				Runnable firstInspection = () -> {
					operationWorker.set(Thread.currentThread());
					inspecting.countDown();
					try { assertTrue(releaseInspection.await(5, TimeUnit.SECONDS), "Inspection was not interrupted"); }
					catch (InterruptedException cancelled) { Thread.currentThread().interrupt(); }
				};
				DatabaseException original;
				if (consumptionPoint.equals("jdbcNext")) {
					AtomicBoolean armed = new AtomicBoolean();
					SQLException jdbcFailure = new SQLException("Interrupted JDBC failure", "08006") {
						@Override public Throwable getCause() {
							if (armed.get() && inspections.incrementAndGet() == 1) firstInspection.run();
							return null;
						}
						@Override public SQLException getNextException() {
							if (armed.get()) Thread.interrupted();
							return null;
						}
					};
					original = new DatabaseException("Interrupted while inspecting reconciliation failure", jdbcFailure);
					armed.set(true);
				} else {
					RuntimeException child = new RuntimeException("Consumes interruption in a child diagnostic") {
						@Override public Throwable getCause() { Thread.interrupted(); return null; }
					};
					original = new DatabaseException("Interrupted while inspecting reconciliation failure") {
						@Override public Throwable getCause() {
							if (inspections.incrementAndGet() == 1) firstInspection.run();
							else if (consumptionPoint.equals("nextScan")) Thread.interrupted();
							return consumptionPoint.equals("childCause") ? child : null;
						}
					};
				}
				try {
					Outcome outcome = run(original, legacy, false, () -> {
						try { assertTrue(inspecting.await(5, TimeUnit.SECONDS), "Inspection did not begin"); }
						catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
						operationWorker.get().interrupt();
					});
					assertSame(original, outcome.result().getFailure().orElse(null), consumptionPoint);
					assertEquals(1, outcome.passes(), consumptionPoint);
					assertEquals(0, outcome.policyCalls(), consumptionPoint);
					assertEquals(0, outcome.backoffCalls(), consumptionPoint);
					assertEquals(0, outcome.recoveryReports(), consumptionPoint);
					assertTrue(outcome.operationWorkerInterrupted(), consumptionPoint);
				} finally {
					releaseInspection.countDown();
				}
			}
		}
	}

	@Test
	void terminalClassificationIsVisibleBeforeTheFailedWaiterCanRetry() throws Exception {
		CountDownLatch opened = new CountDownLatch(1);
		CountDownLatch notifyingTerminalFailure = new CountDownLatch(1);
		CountDownLatch releaseTerminalNotification = new CountDownLatch(1);
		AtomicInteger inspections = new AtomicInteger();
		AtomicInteger opens = new AtomicInteger();
		AtomicInteger policyCalls = new AtomicInteger();
		AtomicInteger backoffCalls = new AtomicInteger();
		AtomicReference<Thread> listener = new AtomicReference<>();
		ConcurrentLinkedQueue<Thread> workers = new ConcurrentLinkedQueue<>();
		IllegalStateException diagnostic = new IllegalStateException("One-shot diagnostic failure");
		DatabaseException original = new DatabaseException("Terminal reconciliation failure") {
			@Override public Throwable getCause() {
				if (inspections.incrementAndGet() == 1) throw diagnostic;
				return null;
			}
		};
		NotificationSupervisor supervisor = NotificationSupervisor.withDatabase(DATABASE).channels(Set.of("changed"))
				.pollEvery(Duration.ofDays(1)).reconcile(() -> {
					awaitUninterruptibly(opened);
					throw original;
				}).retry(failure -> { policyCalls.incrementAndGet(); return true; },
						(attempt, failure) -> { backoffCalls.incrementAndGet(); return Duration.ZERO; }, Duration.ofMinutes(1))
				.threadFactory(role -> task -> {
					Thread thread = new Thread(task, "retry-handoff-" + role);
					thread.setDaemon(true);
					thread.setUncaughtExceptionHandler((worker, failure) -> {});
					workers.add(thread);
					if (role == NotificationSupervisor.ThreadRole.LISTENER) listener.set(thread);
					return thread;
				}).build(operation -> {
					if (opens.incrementAndGet() > 1) throw new InterruptedException();
					opened.countDown();
					operation.perform(wait -> fail("A terminal initial failure must not receive notifications"));
				}, System::nanoTime, delay -> {});
		ReentrantLock notificationGate = new ReentrantLock() {
			@Override public void lock() {
				if (Thread.currentThread().getName().equals("retry-handoff-RECONCILIATION")
						&& StackWalker.getInstance().walk(frames -> frames.skip(1).findFirst().orElseThrow()
								.getMethodName()).equals("managedFailure")) {
					notifyingTerminalFailure.countDown();
					awaitUninterruptibly(releaseTerminalNotification);
				}
				super.lock();
			}
		};
		var lockField = NotificationSupervisor.class.getDeclaredField("lifecycleLock");
		lockField.setAccessible(true);
		lockField.set(supervisor, notificationGate);
		try {
			supervisor.start();
			assertTrue(notifyingTerminalFailure.await(5, TimeUnit.SECONDS));
			listener.get().join(5_000);
			assertFalse(listener.get().isAlive(), "Listener waited for a terminal notification before stopping");
			assertEquals(0, policyCalls.get(), "A terminal classification reached the listener retry policy");
			assertEquals(0, backoffCalls.get());
			assertEquals(1, opens.get());
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
			releaseTerminalNotification.countDown();
			assertSame(original, supervisor.awaitShutdownAsync().toCompletableFuture()
					.get(5, TimeUnit.SECONDS).getFailure().orElseThrow());
			assertArrayEquals(new Throwable[]{diagnostic}, original.getSuppressed());
		} finally {
			releaseTerminalNotification.countDown();
			supervisor.requestShutdown();
			supervisor.awaitShutdownAsync().toCompletableFuture().get(5, TimeUnit.SECONDS);
			join(workers);
		}
	}

	@Test
	void ordinaryManagedDatabaseFailureStillRecovers() throws Exception {
		Outcome outcome = run(new DatabaseException("Recoverable reconciliation failure"), false, false);
		assertTrue(outcome.result().getFailure().isEmpty());
		assertTrue(outcome.passes() >= 2);
		assertEquals(1, outcome.recoveryReports());
		assertEquals(0, outcome.policyCalls(), "Polling must not reconnect a healthy listener");
		assertEquals(0, outcome.backoffCalls());
		assertTrue(outcome.supervisor().snapshot().getLastReconciliationFailure().isEmpty());
		assertTrue(outcome.supervisor().snapshot().getLastSuccessfulReconciliation().isPresent());
	}

	@Test
	void ordinaryLegacyDatabaseFailureStillUsesExplicitRetry() throws Exception {
		Outcome outcome = run(new DatabaseException("Recoverable reconciliation failure"), true, false);
		assertTrue(outcome.result().getFailure().isEmpty());
		assertEquals(2, outcome.passes());
		assertEquals(1, outcome.policyCalls());
		assertEquals(1, outcome.backoffCalls());
		assertEquals(0, outcome.recoveryReports());
		assertTrue(outcome.supervisor().snapshot().getLastReconciliationFailure().isEmpty());
		assertTrue(outcome.supervisor().snapshot().getLastSuccessfulReconciliation().isPresent());
	}

	@Test
	void genericInterruptRestorationKeepsItsNonRetainingContract() {
		IllegalStateException diagnostic = new IllegalStateException("Unavailable cause", new InterruptedException());
		DatabaseException original = new DatabaseException("Operation failed") {
			@Override public Throwable getCause() { throw diagnostic; }
		};
		Database.restoreInterruptIfNeeded(original);
		assertFalse(Thread.currentThread().isInterrupted());
		assertArrayEquals(new Throwable[0], original.getSuppressed());
	}

	private static void assertStoppedOnInspection(Outcome outcome, DatabaseException original,
			Diagnostic diagnostic) {
		assertSame(original, outcome.result().getFailure().orElseThrow());
		assertEquals(1, outcome.passes(), "Inspection failure must not admit another reconciliation");
		assertEquals(0, outcome.policyCalls());
		assertEquals(0, outcome.backoffCalls());
		assertEquals(0, outcome.recoveryReports());
		assertArrayEquals(new Throwable[]{diagnostic.failure()}, original.getSuppressed());
		assertTrue(outcome.supervisor().firstReady().toCompletableFuture().isCompletedExceptionally());
		if (diagnostic.interruption())
			assertTrue(outcome.operationWorkerInterrupted(), "Diagnostic cancellation lost its interrupt status");
	}

	private static List<Diagnostic> diagnostics() {
		return List.of(new Diagnostic(new AssertionError("Fatal diagnostic failure"), false),
				new Diagnostic(new IllegalStateException("Diagnostic cancellation", new InterruptedException("Cancel")), true),
				new Diagnostic(new IllegalStateException("Unreadable diagnostic"), false));
	}

	private static Outcome run(DatabaseException original, boolean legacy, boolean interruptBeforeFailure)
			throws Exception {
		return run(original, legacy, interruptBeforeFailure, () -> {});
	}

	private static Outcome run(DatabaseException original, boolean legacy, boolean interruptBeforeFailure,
			Runnable afterStart) throws Exception {
		AtomicInteger passes = new AtomicInteger();
		AtomicInteger policyCalls = new AtomicInteger();
		AtomicInteger backoffCalls = new AtomicInteger();
		AtomicInteger recoveryReports = new AtomicInteger();
		AtomicBoolean operationWorkerInterrupted = new AtomicBoolean();
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		ConcurrentLinkedQueue<Thread> workers = new ConcurrentLinkedQueue<>();
		NotificationSupervisor.Builder builder = NotificationSupervisor.withDatabase(DATABASE)
				.channels(Set.of("changed")).reconcile(() -> {
					if (passes.incrementAndGet() == 1) {
						if (interruptBeforeFailure) Thread.currentThread().interrupt();
						throw original;
					}
				}).retry(failure -> {
					policyCalls.incrementAndGet();
					return true;
				}, (attempt, failure) -> {
					backoffCalls.incrementAndGet();
					return Duration.ZERO;
				}, Duration.ofMinutes(1));
		if (!legacy) {
			builder.pollEvery(Duration.ofMillis(10)).onReconciliationFailure(failure -> recoveryReports.incrementAndGet())
					.threadFactory(role -> task -> worker(task, role == NotificationSupervisor.ThreadRole.RECONCILIATION,
							operationWorkerInterrupted, workers));
		}
		NotificationSupervisor supervisor = builder.build(operation -> operation.perform(wait -> {
			// Shut down only after the successful callback has returned and readiness has been published.
			reference.get().requestShutdown();
			return List.of();
		}), System::nanoTime, delay -> {});
		reference.set(supervisor);
		try {
			if (legacy)
				supervisor.start(task -> worker(task, true, operationWorkerInterrupted, workers).start());
			else
				supervisor.start();
			afterStart.run();
			NotificationSupervisor.ShutdownResult result = supervisor.awaitShutdownAsync().toCompletableFuture()
					.get(5, TimeUnit.SECONDS);
			join(workers);
			return new Outcome(supervisor, result, passes.get(), policyCalls.get(), backoffCalls.get(),
					recoveryReports.get(), operationWorkerInterrupted.get());
		} finally {
			supervisor.requestShutdown();
			supervisor.awaitShutdownAsync().toCompletableFuture().get(5, TimeUnit.SECONDS);
			join(workers);
		}
	}

	private static Thread worker(Runnable task, boolean operationWorker, AtomicBoolean interrupted,
			ConcurrentLinkedQueue<Thread> workers) {
		Thread thread = new Thread(() -> {
			try { task.run(); }
			finally {
				if (operationWorker) interrupted.set(Thread.currentThread().isInterrupted());
			}
		}, "notification-retry-inspection");
		thread.setDaemon(true);
		thread.setUncaughtExceptionHandler((worker, failure) -> {});
		workers.add(thread);
		return thread;
	}

	private static void join(ConcurrentLinkedQueue<Thread> workers) throws InterruptedException {
		for (Thread worker : workers) {
			worker.join(5_000);
			assertFalse(worker.isAlive(), "Notification worker leaked");
		}
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		try {
			while (true) {
				try { assertTrue(latch.await(5, TimeUnit.SECONDS), "Test phase did not complete"); return; }
				catch (InterruptedException cancellation) { interrupted = true; }
			}
		} finally {
			if (interrupted) Thread.currentThread().interrupt();
		}
	}

	private static void throwUnchecked(Throwable failure) {
		if (failure instanceof Error error) throw error;
		throw (RuntimeException) failure;
	}

	private record Diagnostic(Throwable failure, boolean interruption) {}
	private record Outcome(NotificationSupervisor supervisor, NotificationSupervisor.ShutdownResult result,
			int passes, int policyCalls, int backoffCalls, int recoveryReports, boolean operationWorkerInterrupted) {}
}
