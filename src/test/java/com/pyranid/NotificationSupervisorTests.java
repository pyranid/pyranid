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

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Compiles and deterministically exercises the caller-owned notification-supervisor pattern documented for 4.6.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@ThreadSafe
public class NotificationSupervisorTests {
	@Test
	public void publicDatabaseAdapterCompilesAgainstJava17Api() {
		Function<Database, SessionAttempt> adapter = NotificationSupervisor::forDatabase;
		Assertions.assertNotNull(adapter);
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
		NotificationSupervisor supervisor = supervisor(
				operation -> {
					attempts.incrementAndGet();
					operation.perform(maxWait -> {
						throw new InterruptedException("stop");
					});
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
			Assertions.assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
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
	public void interruptedBackoffPreservesObservedFailureAndStopsRetry() {
		DatabaseException listenerFailure = new DatabaseException("listener failed");
		AtomicInteger attempts = new AtomicInteger();
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

		try {
			DatabaseException thrown = Assertions.assertThrows(
					DatabaseException.class, () -> supervisor.run(startup, failure -> true));

			Assertions.assertSame(listenerFailure, thrown);
			Assertions.assertEquals(1, attempts.get());
			Assertions.assertTrue(Thread.currentThread().isInterrupted());
			Assertions.assertTrue(startup.isReady());
		} finally {
			Thread.interrupted();
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
																									 DelayPolicy delayPolicy,
																									 Sleeper sleeper,
																									 IntConsumer failureNumberObserver) {
		return new NotificationSupervisor(
				sessionAttempt,
				reconciler,
				nanoTimeSupplier,
				Duration.ofMinutes(1).toNanos(),
				delayPolicy,
				sleeper,
				failureNumberObserver);
	}

	@FunctionalInterface
	private interface SessionAttempt {
		void open(SessionOperation operation) throws InterruptedException;
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
	private interface DelayPolicy {
		Duration delay(int failureNumber, DatabaseException failure);
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
		private final DelayPolicy delayPolicy;
		private final Sleeper sleeper;
		private final IntConsumer failureNumberObserver;

		private NotificationSupervisor(SessionAttempt sessionAttempt,
																	 Reconciler reconciler,
																	 LongSupplier nanoTimeSupplier,
																	 long stabilityWindowNanos,
																	 DelayPolicy delayPolicy,
																	 Sleeper sleeper,
																	 IntConsumer failureNumberObserver) {
			this.sessionAttempt = Objects.requireNonNull(sessionAttempt);
			this.reconciler = Objects.requireNonNull(reconciler);
			this.nanoTimeSupplier = Objects.requireNonNull(nanoTimeSupplier);
			this.stabilityWindowNanos = stabilityWindowNanos;
			this.delayPolicy = Objects.requireNonNull(delayPolicy);
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
							this.delayPolicy.delay(consecutiveFailures, listenerOrReconciliationFailure));

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

		@SuppressWarnings("unused")
		private void await(long timeout, TimeUnit unit) throws InterruptedException {
			if (!this.completed.await(timeout, unit))
				throw new AssertionError("Timed out waiting for notification readiness");
		}

		private void complete(Object value) {
			if (this.outcome.compareAndSet(null, value))
				this.completed.countDown();
		}
	}
}
