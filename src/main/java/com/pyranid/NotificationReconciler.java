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

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/** Serial reconciliation and polling on one managed worker, independent of listener I/O. */
final class NotificationReconciler implements Runnable {
	private final ReentrantLock lock = new ReentrantLock();
	private final Condition changed = this.lock.newCondition();
	private final Runnable reconciler;
	private final long pollNanos;
	private final LongSupplier nanoTime;
	private final Predicate<Throwable> recoverable;
	private final Consumer<Throwable> recoverableFailure;
	private final Consumer<Throwable> terminalFailure;
	// All mutable state is guarded by lock. No callback runs while holding lock.
	private boolean polling;
	private boolean stopping;
	private boolean started;
	private boolean exited;
	private boolean running;
	private boolean firstPoll = true;
	private long requested;
	private long processed;
	private long completed;
	private long failedThrough;
	private long lastPassEnd;
	private @Nullable Long lastSuccessNanos;
	private @Nullable Instant lastSuccessTime;
	private @Nullable Throwable lastFailure;
	private @Nullable Throwable requestFailure;
	private @Nullable Throwable fatalFailure;

	NotificationReconciler(Runnable reconciler, @Nullable Duration pollInterval,
			LongSupplier nanoTime, Predicate<Throwable> recoverable,
			Consumer<Throwable> recoverableFailure, Consumer<Throwable> terminalFailure) {
		this.reconciler = requireNonNull(reconciler);
		this.nanoTime = requireNonNull(nanoTime);
		this.recoverable = requireNonNull(recoverable);
		this.recoverableFailure = requireNonNull(recoverableFailure);
		this.terminalFailure = requireNonNull(terminalFailure);
		this.polling = pollInterval != null;
		if (pollInterval != null && (pollInterval.isZero() || pollInterval.isNegative()))
			throw new IllegalArgumentException("Poll interval must be positive");
		this.pollNanos = pollInterval == null ? 0 : saturatedNanos(pollInterval);
	}

	/**
	 * Accepts a request even when interruption races admission, then waits interruptibly for a fresh pass.
	 * Cancelling the wait cannot discard an already decoded hint; accepted requests drain during shutdown.
	 */
	void reconcileAndAwait() throws InterruptedException {
		this.lock.lock();
		try {
			if (this.stopping || this.exited)
				throw failureOrCancellation();
			if (this.requested == Long.MAX_VALUE)
				throw new IllegalStateException("Too many outstanding reconciliation generations");
			long ticket = ++this.requested;
			this.changed.signalAll();
			while (true) {
				if (this.completed >= ticket)
					return;
				if (this.fatalFailure != null)
					throwUnchecked(this.fatalFailure);
				if (this.failedThrough >= ticket)
					throwUnchecked(requireNonNull(this.requestFailure));
				if (this.exited)
					throw failureOrCancellation();
				this.changed.await();
			}
		} finally {
			this.lock.unlock();
		}
	}

	/** Cancels periodic work while keeping the worker available for final listener reconciliation. */
	void stopPolling() {
		this.lock.lock();
		try {
			this.polling = false;
			// A failed pass may have left a listener waiting for the next periodic attempt. Stopping
			// polling must release that waiter, even when no new pass or callback will run.
			if (!this.running && this.lastFailure != null && this.processed > this.completed) {
				this.failedThrough = this.processed;
				this.requestFailure = this.lastFailure;
			}
			this.changed.signalAll();
		} finally {
			this.lock.unlock();
		}
	}

	/** Seals new requests and drains requests accepted before shutdown. Does not interrupt application work. */
	void stop() {
		this.lock.lock();
		try {
			this.stopping = true;
			this.polling = false;
			if (!this.running && this.lastFailure != null && this.processed > this.completed) {
				this.failedThrough = this.processed;
				this.requestFailure = this.lastFailure;
			}
			this.changed.signalAll();
		} finally {
			this.lock.unlock();
		}
	}

	Snapshot snapshot() {
		this.lock.lock();
		try {
			boolean pending = !this.exited && (this.requested > this.processed
					|| (!this.running && this.requested > this.completed && this.requested > this.failedThrough));
			return new Snapshot(this.lastSuccessNanos, this.lastSuccessTime, this.lastFailure,
					this.running, pending);
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public void run() {
		Throwable failure = null;
		try {
			this.lock.lock();
			try {
				if (this.started)
					throw new IllegalStateException("Reconciliation worker has already started");
				this.started = true;
			} finally {
				this.lock.unlock();
			}
			runPasses();
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			Throwable previousFailure;
			this.lock.lock();
			try {
				previousFailure = this.lastFailure;
			} finally {
				this.lock.unlock();
			}
			try {
				// Cancellation ends recovery; it must not erase the database failure that still
				// needs recovery. Inspect foreign exception accessors outside the worker lock.
				failure = retainUnrecoveredFailure(interrupted, previousFailure);
			} finally {
				if (!Thread.currentThread().isInterrupted())
					Thread.currentThread().interrupt();
			}
		} catch (RuntimeException | Error terminal) {
			failure = terminal;
		} finally {
			this.lock.lock();
			try {
				// Once draining finishes, an unrecovered pass failure can no longer be retried. The
				// listener may already have stopped waiting, so report it independently of that waiter.
				if (failure == null)
					failure = this.lastFailure;
				this.exited = true;
				this.running = false;
				this.fatalFailure = failure;
				if (failure != null)
					this.lastFailure = failure;
				this.changed.signalAll();
			} finally {
				this.lock.unlock();
			}
		}
		if (failure != null)
			this.terminalFailure.accept(failure);
		if (failure instanceof Error error)
			throw error;
	}

	static Throwable retainUnrecoveredFailure(Throwable cancellation,
			@Nullable Throwable previousFailure) {
		if (previousFailure == null || previousFailure == cancellation
				|| ThrowableGraph.inspect(cancellation, current -> current == previousFailure ? 1 : 0, 0) != 0)
			return cancellation;
		if (ThrowableGraph.inspect(previousFailure, current -> current == cancellation ? 1 : 0, 0) != 0)
			return previousFailure;
		cancellation.addSuppressed(previousFailure);
		return cancellation;
	}

	private void runPasses() throws InterruptedException {
		while (true) {
			long covers;
			this.lock.lockInterruptibly();
			try {
				while (true) {
					boolean requestedPass = this.requested > this.processed;
					if (this.stopping && !requestedPass)
						return;
					long wait = this.polling && !this.firstPoll
							? remainingPollNanos() : 0;
					if (requestedPass || (this.polling && (this.firstPoll || wait <= 0))) {
						// Capture BEFORE user work so requests arriving during this pass force a fresh pass.
						covers = this.requested;
						this.processed = covers;
						this.firstPoll = false;
						this.running = true;
						break;
					}
					if (this.polling)
						this.changed.awaitNanos(wait);
					else
						this.changed.await();
				}
			} finally {
				this.lock.unlock();
			}

			Throwable failure = null;
			boolean mayRecover = false;
			try {
				this.reconciler.run();
				// A callback can restore cancellation and return. Do not acknowledge that pass or
				// publish fresh readiness before the next interruptible wait notices the cancellation.
				if (Thread.currentThread().isInterrupted())
					throw new InterruptedException();
			} catch (RuntimeException | Error observed) {
				failure = observed;
				try {
					mayRecover = this.recoverable.test(observed);
				} catch (RuntimeException | Error classificationFailure) {
					if (classificationFailure != observed)
						classificationFailure.addSuppressed(observed);
					failure = classificationFailure;
				}
			}

			long finishedAt = this.nanoTime.getAsLong();
			Instant finishedTime = failure == null ? Instant.now() : null;
			this.lock.lock();
			try {
				this.running = false;
				this.lastPassEnd = finishedAt;
				this.lastFailure = failure;
				if (failure == null) {
					this.completed = covers;
					this.lastSuccessNanos = finishedAt;
					this.lastSuccessTime = finishedTime;
				} else if (!mayRecover) {
					this.fatalFailure = failure;
				} else if (!this.polling || this.stopping) {
					this.failedThrough = covers;
					this.requestFailure = failure;
				}
				this.changed.signalAll();
			} finally {
				this.lock.unlock();
			}
			if (failure != null) {
				if (!mayRecover)
					throwUnchecked(failure);
				try {
					this.recoverableFailure.accept(failure);
				} catch (RuntimeException | Error observerFailure) {
					if (observerFailure != failure)
						observerFailure.addSuppressed(failure);
					throw observerFailure;
				}
			}
		}
	}

	private long remainingPollNanos() {
		long elapsed = this.nanoTime.getAsLong() - this.lastPassEnd;
		return elapsed < 0 ? this.pollNanos : this.pollNanos - elapsed;
	}

	private RuntimeException failureOrCancellation() {
		if (this.fatalFailure != null)
			throwUnchecked(this.fatalFailure);
		return new CancellationException("Reconciliation worker has stopped");
	}

	private static void throwUnchecked(Throwable failure) {
		if (failure instanceof RuntimeException runtime)
			throw runtime;
		if (failure instanceof Error error)
			throw error;
		throw new IllegalStateException("Reconciliation worker interrupted", failure);
	}

	private static long saturatedNanos(Duration duration) {
		try {
			return duration.toNanos();
		} catch (ArithmeticException overflow) {
			return Long.MAX_VALUE;
		}
	}

	record Snapshot(@Nullable Long lastSuccessNanos, @Nullable Instant lastSuccessTime,
			@Nullable Throwable lastFailure, boolean running, boolean pending) {}
}
