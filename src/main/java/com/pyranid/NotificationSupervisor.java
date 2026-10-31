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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * An explicitly started, single-use supervisor for notification-driven durable reconciliation.
 * <p>
 * The supervisor registers its channels, reconciles authoritative state, then reconciles once per nonempty
 * notification batch. A selected retry opens a new session and repeats initial reconciliation. Notifications
 * remain lossy hints: applications needing progress without a notification must poll durable state on an
 * independent worker. Reconciliation must be bounded, repeatable, and safe alongside such polling.
 * <p>
 * This class creates no threads or executors and never closes the caller's executor or data source. The supplied
 * executor must dispatch asynchronously. A listener needs a session-affine source and holds one connection;
 * ordinary reconciliation queries acquire their own connections. See {@link Database#withNotificationSession}.
 * <p>
 * Completion-stage views cannot be used to cancel or complete this supervisor. Use {@link #requestStop()} to
 * stop it. Stage continuations may run on the completing thread: keep them short or use asynchronous continuations.
 * Neither a receive timeout nor an {@link State#ACTIVE} snapshot proves that the physical connection is healthy.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.8.0
 */
@ThreadSafe
public final class NotificationSupervisor {
	private static final Duration RECEIVE_INTERVAL = Duration.ofSeconds(30);
	private final ReentrantLock lifecycleLock = new ReentrantLock();
	private final SessionAttempt sessionAttempt;
	private final Runnable reconciler;
	private final @Nullable Predicate<? super RetryFailure> retryCondition;
	private final RetryPolicy.@Nullable Backoff backoff;
	private final long stabilityWindowNanos;
	private final LongSupplier nanoTime;
	private final Sleeper sleeper;
	private final CompletableFuture<Void> ready = new CompletableFuture<>();
	private final CompletableFuture<Result> terminated = new CompletableFuture<>();
	private final CompletionStage<Void> readyView = this.ready.minimalCompletionStage();
	private final CompletionStage<Result> terminatedView = this.terminated.minimalCompletionStage();
	// All mutable lifecycle fields are guarded by lifecycleLock.
	private State state = State.NEW;
	private @Nullable Thread worker;
	private boolean stopRequested;
	private boolean readinessDecided;
	private boolean everReady;
	private long attemptNumber;
	private @Nullable Throwable lastFailure;

	private NotificationSupervisor(Builder builder, SessionAttempt sessionAttempt,
			LongSupplier nanoTime, Sleeper sleeper) {
		this.sessionAttempt = requireNonNull(sessionAttempt);
		this.reconciler = requireNonNull(builder.reconciler);
		this.retryCondition = builder.retryCondition;
		this.backoff = builder.backoff;
		this.stabilityWindowNanos = saturatedNanos(builder.stabilityWindow);
		this.nanoTime = requireNonNull(nanoTime);
		this.sleeper = requireNonNull(sleeper);
	}

	/**
	 * Creates a builder without opening a connection or starting work.
	 *
	 * @param listenerDatabase database whose source preserves listener-session affinity
	 * @return a builder
	 */
	@NonNull
	public static Builder withDatabase(@NonNull Database listenerDatabase) {
		return new Builder(requireNonNull(listenerDatabase));
	}

	/**
	 * Submits this supervisor exactly once to an application-owned asynchronous executor.
	 * <p>
	 * Inline execution, including a caller-runs rejection policy, is rejected before any JDBC or reconciliation work.
	 * A later queued invocation on the submitting thread is allowed. The executor must eventually run an accepted
	 * task; silently discarded tasks cannot be detected. Calling {@link #requestStop()} still terminates a queued task.
	 *
	 * @param executor asynchronous executor with capacity for a long-lived listener
	 * @throws IllegalStateException if this supervisor has already been started or stopped
	 * @throws RejectedExecutionException if the executor rejects submission or executes inline
	 */
	public void start(@NonNull Executor executor) {
		requireNonNull(executor);
		this.lifecycleLock.lock();
		try {
			if (this.state != State.NEW)
				throw new IllegalStateException("Notification supervisor has already been started or stopped");
			this.state = State.STARTING;
		} finally {
			this.lifecycleLock.unlock();
		}

		Thread submittingThread = Thread.currentThread();
		AtomicBoolean submitting = new AtomicBoolean(true);
		AtomicReference<RejectedExecutionException> inlineFailure = new AtomicReference<>();
		try {
			executor.execute(() -> {
				if (Thread.currentThread() == submittingThread && submitting.get()) {
					RejectedExecutionException failure = new RejectedExecutionException(
							"Notification supervisor requires an asynchronous executor");
					inlineFailure.set(failure);
					finish(failure);
					return;
				}
				runWorker();
			});
			if (inlineFailure.get() != null)
				throw inlineFailure.get();
		} catch (RuntimeException | Error failure) {
			finish(failure);
			throw failure;
		} finally {
			submitting.set(false);
		}
	}

	/**
	 * Requests cooperative stop without waiting for listener I/O or cleanup. Repeated requests are harmless.
	 * <p>
	 * A stop before worker entry prevents JDBC work. A running worker is interrupted. A decoded nonempty batch still
	 * receives one final bounded reconciliation, with an already-observed interrupt temporarily cleared and restored.
	 * Arbitrary JDBC I/O cannot be forcibly bounded: wait on {@link #termination()} with an application deadline.
	 */
	public void requestStop() {
		CancellationException cancellation = null;
		Result result = null;
		this.lifecycleLock.lock();
		try {
			if (this.state == State.TERMINATED || this.stopRequested)
				return;
			this.stopRequested = true;
			if (!this.readinessDecided) {
				this.readinessDecided = true;
				cancellation = cancelledBeforeReadiness();
			}
			if (this.worker == null) {
				this.state = State.TERMINATED;
				result = new Result(null, true);
			} else {
				this.state = State.STOPPING;
				// Keep interrupt delivery inside the unregister handshake: the thread must not be reused first.
				this.worker.interrupt();
			}
		} finally {
			this.lifecycleLock.unlock();
		}
		if (cancellation != null)
			this.ready.completeExceptionally(cancellation);
		if (result != null)
			this.terminated.complete(result);
	}

	/**
	 * Gets a read-only signal for the first successful registration and initial reconciliation.
	 * <p>
	 * Retryable startup failures leave this pending. Terminal failure or stop before readiness completes it
	 * exceptionally. Later outages do not undo successful readiness; inspect {@link #snapshot()} and
	 * {@link #termination()} separately. Timing out while waiting does not request stop.
	 *
	 * @return first-readiness stage
	 */
	@NonNull
	public CompletionStage<Void> firstReady() {
		return this.readyView;
	}

	/**
	 * Gets a read-only stage that completes normally with the terminal result, including any terminal failure.
	 * <p>
	 * Completion means session cleanup has finished and no more supervisor JDBC or reconciliation work can occur.
	 * It does not mean that a caller's synchronous stage continuation has returned or that the executor has terminated.
	 *
	 * @return terminal-result stage
	 */
	@NonNull
	public CompletionStage<Result> termination() {
		return this.terminatedView;
	}

	/**
	 * Gets an immutable snapshot of observed lifecycle state, not a physical-connection health check.
	 *
	 * @return current lifecycle snapshot
	 */
	@NonNull
	public Snapshot snapshot() {
		this.lifecycleLock.lock();
		try {
			return new Snapshot(this.state, this.attemptNumber, this.everReady, this.lastFailure);
		} finally {
			this.lifecycleLock.unlock();
		}
	}

	private void runWorker() {
		this.lifecycleLock.lock();
		try {
			if (this.state == State.TERMINATED || this.stopRequested || this.worker != null)
				return;
			this.worker = Thread.currentThread();
		} finally {
			this.lifecycleLock.unlock();
		}
		Throwable terminalFailure = null;
		try {
			terminalFailure = runAttempts();
		} catch (RuntimeException | Error failure) {
			terminalFailure = failure;
		} finally {
			finish(terminalFailure);
		}
		if (terminalFailure instanceof Error error)
			throw error;
	}

	@Nullable
	private Throwable runAttempts() {
		int consecutiveFailures = 0;
		DatabaseException previousFailure = null;
		while (true) {
			this.lifecycleLock.lock();
			try {
				if (this.stopRequested || Thread.currentThread().isInterrupted())
					return previousFailure;
				this.state = State.STARTING;
				if (this.attemptNumber < Long.MAX_VALUE)
					++this.attemptNumber;
			} finally {
				this.lifecycleLock.unlock();
			}
			Attempt attempt = new Attempt();
			try {
				this.sessionAttempt.open(session -> consume(session, attempt));
				throw new IllegalStateException("Notification session returned unexpectedly");
			} catch (InterruptedException interrupted) {
				Thread.currentThread().interrupt();
				return null;
			} catch (DatabaseException failure) {
				noteFailure(attempt, failure);
				// The session has finished cleanup. A driver can consume interruption and retain it only
				// in a receive or cleanup failure, so recover that evidence before admitting another attempt.
				Database.restoreInterruptIfNeeded(failure);
				if (stopping() || Thread.currentThread().isInterrupted()
						|| attempt.unexpectedCallbackFailure || this.retryCondition == null
						|| containsTerminalFailure(failure))
					return failure;
				if (attempt.readySince != null
						&& attempt.failedAt - attempt.readySince >= this.stabilityWindowNanos)
					consecutiveFailures = 0;
				if (consecutiveFailures < Integer.MAX_VALUE)
					++consecutiveFailures;
				boolean previouslyReady;
				this.lifecycleLock.lock();
				try {
					previouslyReady = this.everReady;
				} finally {
					this.lifecycleLock.unlock();
				}
				Duration delay;
				try {
					if (!this.retryCondition.test(new RetryFailure(
							failure, attempt.phase, consecutiveFailures, previouslyReady)))
						return failure;
					if (stopping() || Thread.currentThread().isInterrupted())
						return failure;
					delay = requireNonNull(requireNonNull(this.backoff)
							.delayAfterFailedAttempt(consecutiveFailures, failure));
					if (delay.isNegative())
						throw new IllegalArgumentException("Supervisor backoff must not be negative");
				} catch (RuntimeException | Error policyFailure) {
					if (policyFailure != failure)
						policyFailure.addSuppressed(failure);
					throw policyFailure;
				}
				if (stopping() || Thread.currentThread().isInterrupted())
					return failure;
				try {
					this.sleeper.sleep(delay);
				} catch (InterruptedException interrupted) {
					Thread.currentThread().interrupt();
					return failure;
				}
				previousFailure = failure;
			}
		}
	}

	private static boolean containsTerminalFailure(Throwable failure) {
		// Fatal errors and unreadable graphs remain terminal even inside connection cleanup failures.
		if (ThrowableGraph.inspect(failure, current -> current instanceof Error ? 1 : 0, 1) != 0)
			return true;
		// Unsupported abort/close operations do not imply that notification receive is unavailable.
		// Prune only those explicit cleanup branches; wrapped or suppressed receive failures still count.
		return ThrowableGraph.inspect(failure, current -> current instanceof NotificationReceiveUnsupportedException
				|| current instanceof UnsupportedOperationException || current instanceof SQLFeatureNotSupportedException ? 1 : 0,
				1, current -> !(current instanceof NotificationConnectionCleanupException)) != 0;
	}

	private void consume(HintSession session, Attempt attempt) throws InterruptedException {
		try {
			throwIfStopping();
			reconcile(attempt);
			throwIfStopping();
			boolean publishReadiness;
			this.lifecycleLock.lock();
			try {
				if (this.stopRequested || Thread.currentThread().isInterrupted())
					throw new InterruptedException();
				attempt.readySince = this.nanoTime.getAsLong();
				this.state = State.ACTIVE;
				this.everReady = true;
				publishReadiness = !this.readinessDecided;
				this.readinessDecided = true;
			} finally {
				this.lifecycleLock.unlock();
			}
			if (publishReadiness)
				this.ready.complete(null);
			while (true) {
				throwIfStopping();
				attempt.phase = FailurePhase.RECEIVE;
				List<Notification> hints = session.await(RECEIVE_INTERVAL);
				boolean interrupted = Thread.interrupted();
				boolean stop = stopping();
				if (stop)
					interrupted |= Thread.interrupted();
				try {
					if (!hints.isEmpty())
						reconcile(attempt);
				} finally {
					if (interrupted)
						Thread.currentThread().interrupt();
				}
				if (stop || interrupted)
					throw new InterruptedException();
			}
		} catch (RuntimeException | Error failure) {
			// The core session can wrap callback runtime exceptions in DatabaseException. Do not turn
			// an unsupported capability or programming error into a retryable database failure.
			attempt.unexpectedCallbackFailure |= !(failure instanceof DatabaseException);
			noteFailure(attempt, failure);
			throw failure;
		} finally {
			if (!attempt.failureObserved)
				attempt.phase = FailurePhase.CLEANUP;
		}
	}

	private void reconcile(Attempt attempt) {
		attempt.phase = FailurePhase.RECONCILE;
		try {
			this.reconciler.run();
		} catch (RuntimeException | Error failure) {
			attempt.unexpectedCallbackFailure = !(failure instanceof DatabaseException);
			throw failure;
		}
	}

	private void noteFailure(Attempt attempt, Throwable failure) {
		if (!attempt.failureObserved) {
			attempt.failureObserved = true;
			attempt.failedAt = this.nanoTime.getAsLong();
		}
		this.lifecycleLock.lock();
		try {
			this.lastFailure = failure;
			if (!this.stopRequested)
				this.state = State.BACKING_OFF;
		} finally {
			this.lifecycleLock.unlock();
		}
	}

	private boolean stopping() {
		this.lifecycleLock.lock();
		try {
			return this.stopRequested;
		} finally {
			this.lifecycleLock.unlock();
		}
	}

	private void throwIfStopping() throws InterruptedException {
		if (Thread.interrupted() || stopping())
			throw new InterruptedException();
	}

	private void finish(@Nullable Throwable failure) {
		Throwable readinessFailure = null;
		Result result;
		this.lifecycleLock.lock();
		try {
			if (this.state == State.TERMINATED)
				return;
			this.worker = null;
			this.state = State.TERMINATED;
			if (failure != null)
				this.lastFailure = failure;
			if (!this.readinessDecided) {
				this.readinessDecided = true;
				readinessFailure = failure == null ? cancelledBeforeReadiness() : failure;
			}
			result = new Result(failure, this.stopRequested);
		} finally {
			this.lifecycleLock.unlock();
		}
		if (readinessFailure != null)
			this.ready.completeExceptionally(readinessFailure);
		this.terminated.complete(result);
	}

	private static CancellationException cancelledBeforeReadiness() {
		return new CancellationException("Notification supervisor stopped before readiness");
	}

	private static long saturatedNanos(Duration duration) {
		try {
			return duration.toNanos();
		} catch (ArithmeticException overflow) {
			return Long.MAX_VALUE;
		}
	}

	private static final class Attempt {
		private FailurePhase phase = FailurePhase.OPEN;
		private @Nullable Long readySince;
		private boolean failureObserved;
		private long failedAt;
		private boolean unexpectedCallbackFailure;
	}

	@FunctionalInterface
	interface SessionAttempt {
		void open(SessionOperation operation) throws InterruptedException;
	}

	@FunctionalInterface
	interface SessionOperation {
		void perform(HintSession session) throws InterruptedException;
	}

	@FunctionalInterface
	interface HintSession {
		List<Notification> await(Duration maxWait) throws InterruptedException;
	}

	@FunctionalInterface
	interface Sleeper {
		void sleep(Duration delay) throws InterruptedException;
	}

	/** Builder for a single-use supervisor. No work starts until {@link NotificationSupervisor#start}. */
	@NotThreadSafe
	public static final class Builder {
		private final Database database;
		private @Nullable Set<String> channels;
		private @Nullable Runnable reconciler;
		private @Nullable Predicate<? super RetryFailure> retryCondition;
		private RetryPolicy.@Nullable Backoff backoff;
		private Duration stabilityWindow = Duration.ofMinutes(1);

		private Builder(Database database) {
			this.database = database;
		}

		/**
		 * Sets the nonempty, fixed channel set, copying it immediately.
		 * @param channels channels to register
		 * @return this builder
		 */
		@NonNull
		public Builder channels(@NonNull Set<@NonNull String> channels) {
			requireNonNull(channels);
			if (channels.isEmpty())
				throw new IllegalArgumentException("channels must not be empty");
			for (String channel : channels)
				Notification.validateChannel(channel);
			this.channels = Set.copyOf(channels);
			return this;
		}

		/**
		 * Sets bounded, repeatable reconciliation, called after registration and each nonempty batch.
		 * @param reconciler application operation that reads or claims authoritative durable state
		 * @return this builder
		 */
		@NonNull
		public Builder reconcile(@NonNull Runnable reconciler) {
			this.reconciler = requireNonNull(reconciler);
			return this;
		}

		/**
		 * Enables explicit retry classification and backoff, including during startup.
		 * <p>
		 * A retryable startup failure leaves first readiness pending. Reconciliation failures are offered to the policy
		 * only when they are {@link DatabaseException}s. Unsupported capabilities, other application exceptions, errors,
		 * and failures in the policy terminate. Stop or interruption prevents retries and preserves observed failures.
		 * @param condition decides whether a database failure is retryable
		 * @param backoff chooses a nonnegative delay after each selected failure
		 * @param stabilityWindow positive established-session duration needed to reset consecutive failure escalation
		 * @return this builder
		 */
		@NonNull
		public Builder retry(@NonNull Predicate<? super RetryFailure> condition,
				RetryPolicy.@NonNull Backoff backoff, @NonNull Duration stabilityWindow) {
			requireNonNull(stabilityWindow);
			if (stabilityWindow.isZero() || stabilityWindow.isNegative())
				throw new IllegalArgumentException("stabilityWindow must be positive");
			this.retryCondition = requireNonNull(condition);
			this.backoff = requireNonNull(backoff);
			this.stabilityWindow = stabilityWindow;
			return this;
		}

		/**
		 * Builds a supervisor without acquiring resources. Channels and reconciliation are required.
		 * @return a new, unstarted supervisor
		 * @throws IllegalStateException if channels or reconciliation have not been configured
		 */
		@NonNull
		public NotificationSupervisor build() {
			validate();
			Set<String> fixedChannels = requireNonNull(this.channels);
			return build(operation -> this.database.withNotificationSession(fixedChannels,
					session -> operation.perform(session::awaitNotifications)),
					System::nanoTime, delay -> TimeUnit.NANOSECONDS.sleep(saturatedNanos(delay)));
		}

		NotificationSupervisor build(SessionAttempt sessionAttempt, LongSupplier nanoTime, Sleeper sleeper) {
			validate();
			return new NotificationSupervisor(this, sessionAttempt, nanoTime, sleeper);
		}

		private void validate() {
			if (this.channels == null || this.reconciler == null)
				throw new IllegalStateException("Notification supervisor requires channels and reconciliation");
		}
	}

	/** Locally observed lifecycle state; no state certifies physical-connection health. */
	public enum State {
		/** Built, with no work submitted. */ NEW,
		/** Queued or establishing a new session and performing initial reconciliation. */ STARTING,
		/** The current session completed registration and initial reconciliation. */ ACTIVE,
		/** A failed attempt is being cleaned up, classified, or delayed before retry. */ BACKING_OFF,
		/** Stop requested while a worker still owns lifecycle work. */ STOPPING,
		/** Cleanup complete; no future supervisor JDBC or reconciliation work. */ TERMINATED
	}

	/** Origin of the primary database failure; a suppressed cleanup failure does not replace it. */
	public enum FailurePhase {
		/** Connection acquisition and listener setup. */ OPEN,
		/** Receiving notification hints. */ RECEIVE,
		/** Application reconciliation. */ RECONCILE,
		/** Session cleanup without an earlier primary failure. */ CLEANUP
	}

	/** Immutable input to an application retry condition. */
	@ThreadSafe
	public static final class RetryFailure {
		private final DatabaseException exception;
		private final FailurePhase phase;
		private final int consecutiveFailures;
		private final boolean firstReadyReached;

		private RetryFailure(DatabaseException exception, FailurePhase phase,
				int consecutiveFailures, boolean firstReadyReached) {
			this.exception = exception;
			this.phase = phase;
			this.consecutiveFailures = consecutiveFailures;
			this.firstReadyReached = firstReadyReached;
		}

		/** @return primary database failure */
		@NonNull public DatabaseException getException() { return this.exception; }
		/** @return phase where the primary failure occurred */
		@NonNull public FailurePhase getPhase() { return this.phase; }
		/** @return one-based failure number, reset only after a stable session */
		@NonNull public Integer getConsecutiveFailures() { return this.consecutiveFailures; }
		/** @return whether any attempt has successfully registered and reconciled */
		@NonNull public Boolean isFirstReadyReached() { return this.firstReadyReached; }
	}

	/** Immutable lifecycle snapshot. The last failure remains available after a successful reconnect. */
	@ThreadSafe
	public static final class Snapshot {
		private final State state;
		private final long attemptNumber;
		private final boolean everReady;
		private final @Nullable Throwable lastFailure;

		private Snapshot(State state, long attemptNumber, boolean everReady, @Nullable Throwable lastFailure) {
			this.state = state;
			this.attemptNumber = attemptNumber;
			this.everReady = everReady;
			this.lastFailure = lastFailure;
		}

		/** @return observed lifecycle state */
		@NonNull public State getState() { return this.state; }
		/** @return number of session attempts started, initially zero */
		@NonNull public Long getAttemptNumber() { return this.attemptNumber; }
		/** @return whether first readiness was reached */
		@NonNull public Boolean isEverReady() { return this.everReady; }
		/** @return most recently observed failure, if any */
		@NonNull public Optional<Throwable> getLastFailure() { return Optional.ofNullable(this.lastFailure); }
	}

	/** Terminal lifecycle result. A stop-correlated failure is retained rather than treated as success. */
	@ThreadSafe
	public static final class Result {
		private final @Nullable Throwable failure;
		private final boolean stopRequested;

		private Result(@Nullable Throwable failure, boolean stopRequested) {
			this.failure = failure;
			this.stopRequested = stopRequested;
		}

		/** @return terminal failure, empty for clean cooperative stop */
		@NonNull public Optional<Throwable> getFailure() { return Optional.ofNullable(this.failure); }
		/** @return whether stop was requested before termination, without attributing a failure to that request */
		@NonNull public Boolean isStopRequested() { return this.stopRequested; }
	}
}
