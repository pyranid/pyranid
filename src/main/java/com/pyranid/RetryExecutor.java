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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/** Retry admission and failure preservation, with deterministic clock and sleep seams for tests. */
final class RetryExecutor {
	static final RetryExecutor DEFAULT = new RetryExecutor(System::nanoTime, RetryExecutor::sleepBackoff);

	private final LongSupplier clock;
	private final Sleeper sleeper;

	RetryExecutor(LongSupplier clock, Sleeper sleeper) {
		this.clock = requireNonNull(clock);
		this.sleeper = requireNonNull(sleeper);
	}

	<T> TransactionRetryResult<T> execute(RetryPolicy retryPolicy, Supplier<Optional<T>> operation) {
		requireNonNull(retryPolicy);
		requireNonNull(operation);
		Duration budget = retryPolicy.getMaxElapsedTime().orElse(null);
		long startedAt = budget == null ? 0 : this.clock.getAsLong();
		List<DatabaseException> priorFailures = new ArrayList<>();

		for (int attempt = 1; attempt <= retryPolicy.getMaxAttempts(); ++attempt) {
			if (attempt > 1 && (Thread.currentThread().isInterrupted()
					|| (budget != null && elapsedSince(startedAt).compareTo(budget) >= 0))) {
				DatabaseException lastFailure = priorFailures.get(priorFailures.size() - 1);
				suppressPriorFailures(lastFailure, priorFailures);
				throw lastFailure;
			}

			TransactionRetrySafety attemptSafety = new TransactionRetrySafety();
			try (var ignored = attemptSafety.activate()) {
				return TransactionRetryResult.of(operation.get(), priorFailures);
			} catch (DatabaseException failure) {
				// Outcome fences precede any user policy invocation. An enclosing rollback must not erase evidence
				// that an independent inner transaction committed or has an unknown outcome.
				TransactionRetrySafety.applyTo(failure, attemptSafety.outcomes());
				if (Thread.currentThread().isInterrupted() || failure.isTransactionOutcomeRetryUnsafe()
						|| attempt == retryPolicy.getMaxAttempts()
						|| (budget != null && elapsedSince(startedAt).compareTo(budget) >= 0)) {
					suppressPriorFailures(failure, priorFailures);
					throw failure;
				}

				Boolean retryable;
				try {
					retryable = retryPolicy.getCondition().shouldRetry(failure);
				} catch (RuntimeException | Error conditionFailure) {
					suppressRetryFailures(conditionFailure, failure, priorFailures);
					throw conditionFailure;
				}
				if (Thread.currentThread().isInterrupted()) {
					suppressPriorFailures(failure, priorFailures);
					throw failure;
				}
				if (retryable == null) {
					NullPointerException invalidCondition = new NullPointerException("RetryPolicy.Condition returned null");
					suppressRetryFailures(invalidCondition, failure, priorFailures);
					throw invalidCondition;
				}
				if (!retryable) {
					suppressPriorFailures(failure, priorFailures);
					throw failure;
				}
				if (budget != null && elapsedSince(startedAt).compareTo(budget) >= 0) {
					suppressPriorFailures(failure, priorFailures);
					throw failure;
				}

				priorFailures.add(failure);
				Duration delay;
				try {
					delay = retryPolicy.getBackoff().delayAfterFailedAttempt(attempt, failure);
				} catch (RuntimeException | Error backoffFailure) {
					suppressPriorFailures(backoffFailure, priorFailures);
					throw backoffFailure;
				}
				if (Thread.currentThread().isInterrupted()) {
					suppressPriorFailures(failure, priorFailures);
					throw failure;
				}
				if (delay == null) {
					NullPointerException invalidBackoff = new NullPointerException("RetryPolicy.Backoff returned null");
					suppressPriorFailures(invalidBackoff, priorFailures);
					throw invalidBackoff;
				}
				if (delay.isNegative()) {
					IllegalArgumentException invalidBackoff = new IllegalArgumentException("RetryPolicy.Backoff returned a negative delay");
					suppressPriorFailures(invalidBackoff, priorFailures);
					throw invalidBackoff;
				}

				if (budget != null) {
					Duration elapsed = elapsedSince(startedAt);
					if (elapsed.compareTo(budget) >= 0 || delay.compareTo(budget.minus(elapsed)) >= 0) {
						suppressPriorFailures(failure, priorFailures);
						throw failure;
					}
				}

				try {
					if (!delay.isZero())
						this.sleeper.sleep(delay);
				} catch (InterruptedException interruptedException) {
					Thread.currentThread().interrupt();
					suppressPriorFailures(failure, priorFailures);
					failure.addSuppressed(interruptedException);
					throw failure;
				}
			}
		}
		throw new AssertionError("unreachable");
	}

	private Duration elapsedSince(long startedAt) {
		// Subtraction handles nanoTime's signed wraparound for intervals shorter than 2^63 nanoseconds.
		return Duration.ofNanos(Math.max(0L, this.clock.getAsLong() - startedAt));
	}

	@FunctionalInterface
	interface Sleeper {
		void sleep(Duration delay) throws InterruptedException;
	}

	private static void sleepBackoff(@NonNull Duration delay) throws InterruptedException {
		requireNonNull(delay);

		if (delay.isZero())
			return;

		long millis;
		int nanos;

		try {
			millis = delay.toMillis();
			Duration remainder = delay.minusMillis(millis);
			nanos = (int) Math.min(999_999L, Math.max(0L, remainder.toNanos()));
		} catch (ArithmeticException e) {
			millis = Long.MAX_VALUE;
			nanos = 999_999;
		}

		Thread.sleep(millis, nanos);
	}

	private void suppressRetryFailures(@NonNull Throwable failure,
																		 @NonNull DatabaseException currentFailure,
																		 @NonNull List<@NonNull DatabaseException> priorFailures) {
		requireNonNull(failure);
		requireNonNull(currentFailure);
		requireNonNull(priorFailures);

		suppressPriorFailures(failure, priorFailures);
		suppressIfDifferent(failure, currentFailure);
	}

	private void suppressPriorFailures(@NonNull Throwable failure,
																		 @NonNull List<@NonNull DatabaseException> priorFailures) {
		requireNonNull(failure);
		requireNonNull(priorFailures);

		for (DatabaseException priorFailure : priorFailures)
			suppressIfDifferent(failure, priorFailure);
	}

	private void suppressIfDifferent(@NonNull Throwable failure,
																	 @NonNull Throwable suppressed) {
		requireNonNull(failure);
		requireNonNull(suppressed);

		if (failure != suppressed)
			failure.addSuppressed(suppressed);
	}
}
