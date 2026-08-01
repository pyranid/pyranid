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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.logging.Logger;

/**
 * Compiles and deterministically exercises the caller-owned notification-supervisor pattern documented for 4.6.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@ThreadSafe
public class NotificationSupervisorTests {
	@Test
	public void publicOneAndTwoDatabaseWiringCompilesAgainstJava17Api() {
		CountingFailingDataSource applicationDataSource =
				new CountingFailingDataSource(new SQLException("application checkout should not occur"));
		CountingFailingDataSource listenerDataSource =
				new CountingFailingDataSource(new SQLException("listener checkout should not occur"));
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database oneInstanceDatabase = Database.withDataSource(applicationDataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.metricsCollector(metricsCollector)
				.build();
		Database applicationDatabase = Database.withDataSource(applicationDataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.metricsCollector(metricsCollector)
				.build();
		Database listenerDatabase = Database.withDataSource(listenerDataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.metricsCollector(metricsCollector)
				.build();

		SessionAttempt oneInstanceAttempt = NotificationSupervisor.forDatabase(oneInstanceDatabase);
		SessionAttempt twoInstanceAttempt = NotificationSupervisor.forDatabase(listenerDatabase);
		SingleChannelSessionMethod singleChannelForm = Database::withNotificationSession;
		MultipleChannelSessionMethod multipleChannelForm = Database::withNotificationSession;

		Assertions.assertNotNull(applicationDatabase);
		Assertions.assertNotNull(oneInstanceAttempt);
		Assertions.assertNotNull(twoInstanceAttempt);
		Assertions.assertNotNull(singleChannelForm);
		Assertions.assertNotNull(multipleChannelForm);
		Assertions.assertEquals(0, applicationDataSource.checkouts());
		Assertions.assertEquals(0, listenerDataSource.checkouts());
	}

	@Test
	public void publicDatabaseAdapterOpeningFailureFailsStartupWithoutCallbackOrRetry() throws Exception {
		SQLException acquisitionFailure = new SQLException("listener acquisition failed");
		CountingFailingDataSource listenerDataSource = new CountingFailingDataSource(acquisitionFailure);
		Database listenerDatabase = Database.withDataSource(listenerDataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.build();
		AtomicInteger reconciliations = new AtomicInteger();
		AtomicInteger sleeps = new AtomicInteger();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				NotificationSupervisor.forDatabase(listenerDatabase),
				reconciliations::incrementAndGet,
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> {
					sleeps.incrementAndGet();
					return true;
				},
				failureNumber -> {});

		DatabaseException thrown = Assertions.assertThrows(
				DatabaseException.class, () -> supervisor.run(startup, failure -> true));
		ExecutionException startupFailure = Assertions.assertThrows(
				ExecutionException.class, () -> startup.await(0, TimeUnit.NANOSECONDS));

		Assertions.assertSame(acquisitionFailure, thrown.getCause());
		Assertions.assertSame(thrown, startupFailure.getCause());
		Assertions.assertEquals(1, listenerDataSource.checkouts());
		Assertions.assertEquals(0, reconciliations.get());
		Assertions.assertEquals(0, sleeps.get());
	}

	@Test
	public void alreadyInterruptedWorkerCancelsWithoutOpeningSession() {
		AtomicInteger attempts = new AtomicInteger();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> attempts.incrementAndGet(),
				() -> {},
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> true,
				failureNumber -> {});

		Thread.currentThread().interrupt();

		try {
			supervisor.run(startup, failure -> true);

			Assertions.assertEquals(0, attempts.get());
			Assertions.assertInstanceOf(CancellationException.class, startup.failure());
			Assertions.assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void initialReconciliationFailureFailsStartupWithoutRetry() {
		DatabaseException reconciliationFailure = new DatabaseException("initial reconciliation failed");
		AtomicInteger attempts = new AtomicInteger();
		AtomicInteger sleeps = new AtomicInteger();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> {
					attempts.incrementAndGet();
					operation.perform(maxWait -> List.of());
				},
				() -> {
					throw reconciliationFailure;
				},
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> {
					sleeps.incrementAndGet();
					return true;
				},
				failureNumber -> {});

		DatabaseException thrown = Assertions.assertThrows(
				DatabaseException.class, () -> supervisor.run(startup, failure -> true));

		Assertions.assertSame(reconciliationFailure, thrown);
		Assertions.assertSame(reconciliationFailure, startup.failure());
		Assertions.assertEquals(1, attempts.get());
		Assertions.assertEquals(0, sleeps.get());
	}

	@Test
	public void readinessFollowsInitialReconciliationAndCleanInterruptionDoesNotRetry() {
		StartupSignal startup = new StartupSignal();
		AtomicInteger reconciliations = new AtomicInteger();
		AtomicInteger attempts = new AtomicInteger();
		AtomicBoolean cleanupComplete = new AtomicBoolean();
		NotificationSupervisor supervisor = supervisor(
				operation -> {
					attempts.incrementAndGet();

					try {
						operation.perform(maxWait -> {
							throw new InterruptedException("stop");
						});
					} finally {
						cleanupComplete.set(true);
					}
				},
				() -> {
					Assertions.assertFalse(startup.isReady());
					reconciliations.incrementAndGet();
				},
				() -> 7L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> true,
				failureNumber -> {});

		try {
			supervisor.run(startup, failure -> true);

			Assertions.assertTrue(startup.isReady());
			Assertions.assertEquals(1, reconciliations.get());
			Assertions.assertEquals(1, attempts.get());
			Assertions.assertTrue(cleanupComplete.get());
			Assertions.assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	@Timeout(5)
	public void startupCancellationAfterAwaitTimeoutPreventsLateReadiness() throws Exception {
		CountDownLatch attemptStarted = new CountDownLatch(1);
		CountDownLatch allowCallback = new CountDownLatch(1);
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> {
					attemptStarted.countDown();

					if (!allowCallback.await(2, TimeUnit.SECONDS))
						throw new AssertionError("Timed out waiting to release callback entry");

					operation.perform(maxWait -> {
						throw new InterruptedException("stop after late readiness attempt");
					});
				},
				() -> {},
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> true,
				failureNumber -> {});
		FutureTask<Void> worker = supervisorWorker(supervisor, startup, failure -> true, null);
		Thread workerThread = startWorker(worker, "notification-supervisor-startup-cancellation-test");

		try {
			Assertions.assertTrue(attemptStarted.await(2, TimeUnit.SECONDS));
			Assertions.assertThrows(TimeoutException.class,
					() -> startup.await(0, TimeUnit.NANOSECONDS));

			startup.cancelled();
			allowCallback.countDown();
			worker.get(2, TimeUnit.SECONDS);

			ExecutionException cancellation = Assertions.assertThrows(
					ExecutionException.class, () -> startup.await(0, TimeUnit.NANOSECONDS));
			Assertions.assertInstanceOf(CancellationException.class, cancellation.getCause());
			Assertions.assertFalse(startup.isReady());
		} finally {
			allowCallback.countDown();
			workerThread.interrupt();
			workerThread.join(2_000L);
		}
	}

	@Test
	@Timeout(5)
	public void laterTerminalWorkerFailureCannotReplaceReadiness() throws Exception {
		IllegalStateException terminalFailure = new IllegalStateException("terminal worker failure");
		CountDownLatch receiveEntered = new CountDownLatch(1);
		CountDownLatch allowFailure = new CountDownLatch(1);
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> operation.perform(maxWait -> {
					receiveEntered.countDown();

					if (!allowFailure.await(2, TimeUnit.SECONDS))
						throw new AssertionError("Timed out waiting to release terminal failure");

					throw terminalFailure;
				}),
				() -> {},
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> true,
				failureNumber -> {});
		FutureTask<Void> worker = supervisorWorker(supervisor, startup, failure -> true, null);
		Thread workerThread = startWorker(worker, "notification-supervisor-ready-race-test");

		try {
			startup.await(2, TimeUnit.SECONDS);
			Assertions.assertTrue(receiveEntered.await(2, TimeUnit.SECONDS));
			allowFailure.countDown();

			ExecutionException workerFailure = Assertions.assertThrows(
					ExecutionException.class, () -> worker.get(2, TimeUnit.SECONDS));
			Assertions.assertSame(terminalFailure, workerFailure.getCause());
			startup.await(0, TimeUnit.NANOSECONDS);
			Assertions.assertTrue(startup.isReady());
			Assertions.assertNull(startup.failure());
		} finally {
			allowFailure.countDown();
			workerThread.interrupt();
			workerThread.join(2_000L);
		}
	}

	@Test
	public void flappingReadySessionsUseIncreasingFailureNumbers() {
		AtomicInteger attempts = new AtomicInteger();
		AtomicInteger reconciliations = new AtomicInteger();
		java.util.ArrayList<Integer> failureNumbers = new java.util.ArrayList<>();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> {
					int attempt = attempts.incrementAndGet();
					operation.perform(maxWait -> {
						if (attempt <= 3)
							throw new DatabaseException("flap " + attempt);

						throw new InterruptedException("stop");
					});
				},
				reconciliations::incrementAndGet,
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> true,
				failureNumbers::add);

		try {
			supervisor.run(startup, failure -> true);

			Assertions.assertEquals(List.of(1, 2, 3), failureNumbers);
			Assertions.assertEquals(4, attempts.get());
			Assertions.assertEquals(4, reconciliations.get());
			Assertions.assertTrue(startup.isReady());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void stableSessionResetsNextFailureNumber() {
		AtomicLong clock = new AtomicLong();
		AtomicInteger attempts = new AtomicInteger();
		java.util.ArrayList<Integer> failureNumbers = new java.util.ArrayList<>();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = new NotificationSupervisor(
				operation -> {
					int attempt = attempts.incrementAndGet();
					clock.set(attempt * 10L);
					operation.perform(maxWait -> {
						if (attempt == 3)
							clock.set(200L);

						if (attempt <= 3)
							throw new DatabaseException("attempt " + attempt);

						throw new InterruptedException("stop");
					});
				},
				() -> {},
				clock::get,
				100L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> true,
				failureNumbers::add);

		try {
			supervisor.run(startup, failure -> true);
			Assertions.assertEquals(List.of(1, 2, 1), failureNumbers);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void acquisitionAndRegistrationTimeDoNotCountTowardStabilityWindow() {
		AtomicLong clock = new AtomicLong();
		AtomicInteger attempts = new AtomicInteger();
		java.util.ArrayList<Integer> failureNumbers = new java.util.ArrayList<>();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = new NotificationSupervisor(
				operation -> {
					int attempt = attempts.incrementAndGet();
					clock.addAndGet(1_000L); // Simulated acquisition and registration latency.
					operation.perform(maxWait -> {
						if (attempt <= 2) {
							clock.incrementAndGet();
							throw new DatabaseException("brief ready session " + attempt);
						}

						throw new InterruptedException("stop");
					});
				},
				() -> {},
				clock::get,
				100L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> true,
				failureNumbers::add);

		try {
			supervisor.run(startup, failure -> true);
			Assertions.assertEquals(List.of(1, 2), failureNumbers);
			Assertions.assertEquals(3, attempts.get());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void steadyStateReconciliationFailureExitsSessionAndReachesSupervisor() {
		DatabaseException reconciliationFailure = new DatabaseException("steady-state reconciliation failed");
		AtomicInteger reconciliations = new AtomicInteger();
		AtomicInteger retryClassifications = new AtomicInteger();
		AtomicInteger sleeps = new AtomicInteger();
		AtomicBoolean sessionExited = new AtomicBoolean();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> {
					try {
						operation.perform(maxWait -> List.of(Notification.of("job_ready", "")));
					} finally {
						sessionExited.set(true);
					}
				},
				() -> {
					if (reconciliations.incrementAndGet() == 2)
						throw reconciliationFailure;
				},
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> {
					sleeps.incrementAndGet();
					return true;
				},
				failureNumber -> {});

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> supervisor.run(startup, failure -> {
					retryClassifications.incrementAndGet();
					return false;
				}));

		Assertions.assertSame(reconciliationFailure, thrown);
		Assertions.assertTrue(sessionExited.get());
		Assertions.assertTrue(startup.isReady());
		Assertions.assertEquals(2, reconciliations.get());
		Assertions.assertEquals(1, retryClassifications.get());
		Assertions.assertEquals(0, sleeps.get());
	}

	@Test
	public void lateInterruptWithEmptyHintsSkipsSteadyStateReconciliation() {
		AtomicInteger reconciliations = new AtomicInteger();
		AtomicInteger attempts = new AtomicInteger();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> {
					attempts.incrementAndGet();
					operation.perform(maxWait -> {
						Thread.currentThread().interrupt();
						return List.of();
					});
				},
				reconciliations::incrementAndGet,
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> true,
				failureNumber -> {});

		try {
			supervisor.run(startup, failure -> true);
			Assertions.assertEquals(1, reconciliations.get());
			Assertions.assertEquals(1, attempts.get());
			Assertions.assertTrue(startup.isReady());
			Assertions.assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void lateInterruptWithNonemptyHintsPerformsFinalReconciliation() {
		AtomicInteger reconciliations = new AtomicInteger();
		AtomicInteger receiveCalls = new AtomicInteger();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> operation.perform(maxWait -> {
					if (receiveCalls.incrementAndGet() == 1) {
						Thread.currentThread().interrupt();
						return List.of(Notification.of("job_ready", ""));
					}

					if (Thread.interrupted())
						throw new InterruptedException("stop after final reconciliation");

					throw new AssertionError("Expected the restored interrupt before another receive");
				}),
				reconciliations::incrementAndGet,
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> true,
				failureNumber -> {});

		try {
			supervisor.run(startup, failure -> true);
			Assertions.assertEquals(2, reconciliations.get());
			Assertions.assertEquals(2, receiveCalls.get());
			Assertions.assertTrue(startup.isReady());
			Assertions.assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void lateInterruptWithNonemptyHintsPreservesReconciliationFailure() {
		DatabaseException reconciliationFailure = new DatabaseException("final reconciliation failed");
		AtomicInteger reconciliations = new AtomicInteger();
		AtomicInteger retryClassifications = new AtomicInteger();
		AtomicInteger sleeps = new AtomicInteger();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> operation.perform(maxWait -> {
					Thread.currentThread().interrupt();
					return List.of(Notification.of("job_ready", ""));
				}),
				() -> {
					if (reconciliations.incrementAndGet() == 2)
						throw reconciliationFailure;
				},
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> {
					sleeps.incrementAndGet();
					return true;
				},
				failureNumber -> {});

		try {
			DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
					() -> supervisor.run(startup, failure -> {
						retryClassifications.incrementAndGet();
						return true;
					}));

			Assertions.assertSame(reconciliationFailure, thrown);
			Assertions.assertEquals(2, reconciliations.get());
			Assertions.assertEquals(0, retryClassifications.get());
			Assertions.assertEquals(0, sleeps.get());
			Assertions.assertTrue(startup.isReady());
			Assertions.assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	@Timeout(5)
	public void interruptedBackoffPreservesObservedFailureInWorkerFutureAndStopsRetry() throws Exception {
		DatabaseException listenerFailure = new DatabaseException("listener failed");
		AtomicInteger attempts = new AtomicInteger();
		AtomicBoolean workerInterrupted = new AtomicBoolean();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> {
					attempts.incrementAndGet();
					operation.perform(maxWait -> {
						throw listenerFailure;
					});
				},
				() -> {},
				() -> 0L,
				(failureNumber, failure) -> Duration.ofSeconds(1),
				delay -> {
					Thread.currentThread().interrupt();
					return false;
				},
				failureNumber -> {});
		FutureTask<Void> worker = supervisorWorker(
				supervisor, startup, failure -> true, workerInterrupted);
		Thread workerThread = startWorker(worker, "notification-supervisor-backoff-interruption-test");

		try {
			ExecutionException thrown = Assertions.assertThrows(
					ExecutionException.class, () -> worker.get(2, TimeUnit.SECONDS));

			Assertions.assertSame(listenerFailure, thrown.getCause());
			Assertions.assertEquals(1, attempts.get());
			Assertions.assertTrue(workerInterrupted.get());
			Assertions.assertTrue(startup.isReady());
		} finally {
			workerThread.interrupt();
			workerThread.join(2_000L);
		}
	}

	@Test
	@Timeout(5)
	public void failureObservedWhileInterruptedIsVisibleInWorkerFutureAndNotRetried() throws Exception {
		DatabaseException listenerFailure = new DatabaseException("transport or cleanup failed during stop");
		AtomicInteger attempts = new AtomicInteger();
		AtomicInteger retryClassifications = new AtomicInteger();
		AtomicInteger sleeps = new AtomicInteger();
		AtomicBoolean workerInterrupted = new AtomicBoolean();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> {
					attempts.incrementAndGet();
					operation.perform(maxWait -> {
						Thread.currentThread().interrupt();
						throw listenerFailure;
					});
				},
				() -> {},
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> {
					sleeps.incrementAndGet();
					return true;
				},
				failureNumber -> {});
		FutureTask<Void> worker = supervisorWorker(
				supervisor, startup, failure -> {
					retryClassifications.incrementAndGet();
					return true;
				}, workerInterrupted);
		Thread workerThread = startWorker(worker, "notification-supervisor-failure-interruption-test");

		try {
			ExecutionException thrown = Assertions.assertThrows(
					ExecutionException.class, () -> worker.get(2, TimeUnit.SECONDS));

			Assertions.assertSame(listenerFailure, thrown.getCause());
			Assertions.assertEquals(1, attempts.get());
			Assertions.assertEquals(0, retryClassifications.get());
			Assertions.assertEquals(0, sleeps.get());
			Assertions.assertTrue(workerInterrupted.get());
			Assertions.assertTrue(startup.isReady());
		} finally {
			workerThread.interrupt();
			workerThread.join(2_000L);
		}
	}

	@Test
	public void rejectedPostReadinessRetryTerminatesWithoutSleeping() {
		DatabaseException listenerFailure = new DatabaseException("permanent listener failure");
		AtomicInteger sleeps = new AtomicInteger();
		StartupSignal startup = new StartupSignal();
		NotificationSupervisor supervisor = supervisor(
				operation -> operation.perform(maxWait -> {
					throw listenerFailure;
				}),
				() -> {},
				() -> 0L,
				(failureNumber, failure) -> Duration.ZERO,
				delay -> {
					sleeps.incrementAndGet();
					return true;
				},
				failureNumber -> {});

		DatabaseException thrown = Assertions.assertThrows(
				DatabaseException.class, () -> supervisor.run(startup, failure -> false));

		Assertions.assertSame(listenerFailure, thrown);
		Assertions.assertEquals(0, sleeps.get());
		Assertions.assertTrue(startup.isReady());
	}

	private static NotificationSupervisor supervisor(SessionAttempt sessionAttempt,
																	 Reconciler reconciler,
																	 LongSupplier nanoTimeSupplier,
																	 RetryPolicy.Backoff backoff,
																	 Sleeper sleeper,
																	 IntConsumer failureNumberObserver) {
		return new NotificationSupervisor(
				sessionAttempt,
				reconciler,
				nanoTimeSupplier,
				Duration.ofMinutes(1).toNanos(),
				backoff,
				sleeper,
				failureNumberObserver);
	}

	private static FutureTask<Void> supervisorWorker(NotificationSupervisor supervisor,
			StartupSignal startup,
			Predicate<? super DatabaseException> retryableAfterReadiness,
			AtomicBoolean interruptedOnExit) {
		Objects.requireNonNull(supervisor);
		Objects.requireNonNull(startup);
		Objects.requireNonNull(retryableAfterReadiness);

		return new FutureTask<>(() -> {
			try {
				supervisor.run(startup, retryableAfterReadiness);
				return null;
			} finally {
				if (interruptedOnExit != null)
					interruptedOnExit.set(Thread.currentThread().isInterrupted());
			}
		});
	}

	private static Thread startWorker(FutureTask<Void> worker, String name) {
		Objects.requireNonNull(worker);
		Objects.requireNonNull(name);
		Thread thread = new Thread(worker, name);
		thread.setDaemon(true);
		thread.start();
		return thread;
	}

	@FunctionalInterface
	private interface SessionAttempt {
		void open(SessionOperation operation) throws InterruptedException;
	}

	@FunctionalInterface
	private interface SingleChannelSessionMethod {
		void open(Database database, String channel, NotificationSessionOperation operation)
				throws InterruptedException;
	}

	@FunctionalInterface
	private interface MultipleChannelSessionMethod {
		void open(Database database, Set<String> channels, NotificationSessionOperation operation)
				throws InterruptedException;
	}

	@FunctionalInterface
	private interface SessionOperation {
		void perform(HintSession session) throws InterruptedException;
	}

	@FunctionalInterface
	private interface HintSession {
		List<Notification> await(Duration maxWait) throws InterruptedException;
	}

	@FunctionalInterface
	private interface Reconciler {
		void reconcile();
	}

	@FunctionalInterface
	private interface Sleeper {
		boolean sleep(Duration delay);
	}

	private static final class NotificationSupervisor {
		private static final Set<String> JOB_CHANNELS = Set.of("job_ready");
		private static final Duration RECEIVE_INTERVAL = Duration.ofSeconds(30);

		private final SessionAttempt sessionAttempt;
		private final Reconciler reconciler;
		private final LongSupplier nanoTimeSupplier;
		private final long stabilityWindowNanos;
		private final RetryPolicy.Backoff backoff;
		private final Sleeper sleeper;
		private final IntConsumer failureNumberObserver;

		private NotificationSupervisor(SessionAttempt sessionAttempt,
																	 Reconciler reconciler,
														 LongSupplier nanoTimeSupplier,
														 long stabilityWindowNanos,
														 RetryPolicy.Backoff backoff,
														 Sleeper sleeper,
																	 IntConsumer failureNumberObserver) {
			this.sessionAttempt = Objects.requireNonNull(sessionAttempt);
			this.reconciler = Objects.requireNonNull(reconciler);
			this.nanoTimeSupplier = Objects.requireNonNull(nanoTimeSupplier);
			this.stabilityWindowNanos = stabilityWindowNanos;
			this.backoff = Objects.requireNonNull(backoff);
			this.sleeper = Objects.requireNonNull(sleeper);
			this.failureNumberObserver = Objects.requireNonNull(failureNumberObserver);
		}

		private static SessionAttempt forDatabase(Database listenerDatabase) {
			Objects.requireNonNull(listenerDatabase);
			return operation -> listenerDatabase.withNotificationSession(
					JOB_CHANNELS,
					session -> operation.perform(session::awaitNotifications));
		}

		private void run(StartupSignal startup,
										 Predicate<? super DatabaseException> retryableAfterReadiness) {
			Objects.requireNonNull(startup);
			Objects.requireNonNull(retryableAfterReadiness);
			AtomicBoolean everReady = new AtomicBoolean();
			AtomicReference<Long> attemptReadySinceNanos = new AtomicReference<>();
			int consecutiveFailures = 0;

			for (;;) {
				if (Thread.currentThread().isInterrupted()) {
					startup.cancelled();
					return;
				}

				attemptReadySinceNanos.set(null);

				try {
					this.sessionAttempt.open(session -> {
						throwIfInterrupted();
						this.reconciler.reconcile();
						throwIfInterrupted();
						attemptReadySinceNanos.set(this.nanoTimeSupplier.getAsLong());
						everReady.set(true);
						startup.ready();

						for (;;) {
							List<Notification> hints = session.await(RECEIVE_INTERVAL);
							boolean stopRequested = Thread.interrupted();

							if (stopRequested && hints.isEmpty())
								throw new InterruptedException();

							try {
								this.reconciler.reconcile();
							} finally {
								if (stopRequested)
									Thread.currentThread().interrupt();
							}
						}
					});

					if (attemptReadySinceNanos.get() == null) {
						if (Thread.currentThread().isInterrupted()) {
							startup.cancelled();
							return;
						}

						throw new IllegalStateException("Notification listener stopped before readiness");
					}

					return;
				} catch (InterruptedException interruptedException) {
					Thread.currentThread().interrupt();
					startup.cancelled();
					return;
				} catch (DatabaseException listenerOrReconciliationFailure) {
					Long readySinceNanos = attemptReadySinceNanos.getAndSet(null);

					if (Thread.currentThread().isInterrupted()) {
						startup.failed(listenerOrReconciliationFailure);
						throw listenerOrReconciliationFailure;
					}

					if (!everReady.get()) {
						startup.failed(listenerOrReconciliationFailure);
						throw listenerOrReconciliationFailure;
					}

					if (!retryableAfterReadiness.test(listenerOrReconciliationFailure))
						throw listenerOrReconciliationFailure;

					if (readySinceNanos != null
							&& this.nanoTimeSupplier.getAsLong() - readySinceNanos >= this.stabilityWindowNanos)
						consecutiveFailures = 0;

					if (consecutiveFailures < Integer.MAX_VALUE)
						++consecutiveFailures;

					this.failureNumberObserver.accept(consecutiveFailures);
					Duration delay = Objects.requireNonNull(
							this.backoff.delayAfterFailedAttempt(
									consecutiveFailures, listenerOrReconciliationFailure));

					if (delay.isNegative())
						throw new IllegalArgumentException("Supervisor backoff must not be negative");

					if (!this.sleeper.sleep(delay))
						throw listenerOrReconciliationFailure;
				} catch (RuntimeException | Error terminalFailure) {
					startup.failed(terminalFailure);
					throw terminalFailure;
				}
			}
		}

		private static void throwIfInterrupted() throws InterruptedException {
			if (Thread.interrupted())
				throw new InterruptedException();
		}
	}

	private static final class StartupSignal {
		private static final Object READY = new Object();

		private final AtomicReference<Object> outcome = new AtomicReference<>();
		private final CountDownLatch completed = new CountDownLatch(1);

		private void ready() {
			complete(READY);
		}

		private void failed(Throwable failure) {
			complete(Objects.requireNonNull(failure));
		}

		private void cancelled() {
			failed(new CancellationException("Notification listener stopped before readiness"));
		}

		private boolean isReady() {
			return this.outcome.get() == READY;
		}

		private Throwable failure() {
			Object value = this.outcome.get();
			return value instanceof Throwable throwable ? throwable : null;
		}

		private void await(long timeout, TimeUnit unit)
				throws InterruptedException, TimeoutException, ExecutionException {
			Objects.requireNonNull(unit);

			if (!this.completed.await(timeout, unit))
				throw new TimeoutException("Timed out waiting for notification readiness");

			Object result = this.outcome.get();

			if (result instanceof Throwable failure)
				throw new ExecutionException(failure);
		}

		private void complete(Object value) {
			if (this.outcome.compareAndSet(null, value))
				this.completed.countDown();
		}
	}

	private static final class CountingFailingDataSource implements DataSource {
		private final SQLException failure;
		private final AtomicInteger checkouts = new AtomicInteger();

		private CountingFailingDataSource(SQLException failure) {
			this.failure = Objects.requireNonNull(failure);
		}

		@Override
		public Connection getConnection() throws SQLException {
			this.checkouts.incrementAndGet();
			throw this.failure;
		}

		@Override
		public Connection getConnection(String username, String password) throws SQLException {
			return getConnection();
		}

		private int checkouts() {
			return this.checkouts.get();
		}

		@Override
		public PrintWriter getLogWriter() {
			return null;
		}

		@Override
		public void setLogWriter(PrintWriter out) {}

		@Override
		public void setLoginTimeout(int seconds) {}

		@Override
		public int getLoginTimeout() {
			return 0;
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			throw new SQLFeatureNotSupportedException();
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			Objects.requireNonNull(iface);

			if (iface.isInstance(this))
				return iface.cast(this);

			throw new SQLException("Not a wrapper for " + iface.getName());
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) {
			return Objects.requireNonNull(iface).isInstance(this);
		}
	}
}
