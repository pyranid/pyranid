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

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Defines when and how {@link Database#transactionWithRetry(RetryPolicy, TransactionalOperation)} retries a transaction.
 * <p>
 * A retry policy requires all retry-critical decisions to be explicit: total attempts, retry condition, and backoff.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.4.0
 */
@ThreadSafe
public final class RetryPolicy {
	@NonNull
	private final Integer maxAttempts;
	@NonNull
	private final Condition condition;
	@NonNull
	private final Backoff backoff;

	private RetryPolicy(@NonNull Integer maxAttempts,
											@NonNull Condition condition,
											@NonNull Backoff backoff) {
		this.maxAttempts = maxAttempts;
		this.condition = condition;
		this.backoff = backoff;
	}

	/**
	 * Creates a retry policy.
	 * <p>
	 * {@code maxAttempts} is the total number of attempts, including the initial attempt. For example, {@code 3} means one
	 * initial attempt plus up to two retries.
	 *
	 * @param maxAttempts total attempts, including the initial attempt
	 * @param backoff     determines how long to wait before the next retry
	 * @param condition   determines whether a failed attempt is retryable
	 * @return a retry policy
	 */
	@NonNull
	public static RetryPolicy ofMaxAttempts(@NonNull Integer maxAttempts,
			@NonNull Backoff backoff,
			@NonNull Condition condition) {
		requireNonNull(maxAttempts);
		requireNonNull(backoff);
		requireNonNull(condition);

		if (maxAttempts < 1)
			throw new IllegalArgumentException("maxAttempts must be at least 1");

		return new RetryPolicy(maxAttempts, condition, backoff);
	}

	/**
	 * Gets the total number of attempts, including the initial attempt.
	 *
	 * @return total attempts
	 */
	@NonNull
	public Integer getMaxAttempts() {
		return this.maxAttempts;
	}

	/**
	 * Gets the retry condition.
	 *
	 * @return retry condition
	 */
	@NonNull
	public Condition getCondition() {
		return this.condition;
	}

	/**
	 * Gets the retry backoff.
	 *
	 * @return retry backoff
	 */
	@NonNull
	public Backoff getBackoff() {
		return this.backoff;
	}

	@Override
	@NonNull
	public String toString() {
		return format("%s{maxAttempts=%s, condition=%s, backoff=%s}",
				getClass().getSimpleName(), getMaxAttempts(), getCondition(), getBackoff());
	}

	/**
	 * Determines whether a failed transaction attempt should be retried.
	 * <p>
	 * Implementations should be thread-safe if the {@link RetryPolicy} is shared across threads.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 4.4.0
	 */
	@FunctionalInterface
	public interface Condition {
		/**
		 * Determines whether a failed transaction attempt should be retried.
		 *
		 * @param failure the database failure that caused the transaction attempt to fail
		 * @return {@code true} to retry, {@code false} to stop retrying
		 */
		@NonNull
		Boolean shouldRetry(@NonNull DatabaseException failure);

		/**
		 * Creates a condition that retries serialization failures and deadlocks.
		 *
		 * @return a serialization-failure-or-deadlock retry condition
		 */
		@NonNull
		static Condition serializationFailureOrDeadlock() {
			return failure -> {
				requireNonNull(failure);
				return failure.isSerializationFailure() || failure.isDeadlock();
			};
		}

		/**
		 * Creates a condition that retries serialization failures.
		 *
		 * @return a serialization-failure retry condition
		 */
		@NonNull
		static Condition serializationFailure() {
			return failure -> {
				requireNonNull(failure);
				return failure.isSerializationFailure();
			};
		}

		/**
		 * Creates a condition that retries deadlocks.
		 *
		 * @return a deadlock retry condition
		 */
		@NonNull
		static Condition deadlock() {
			return failure -> {
				requireNonNull(failure);
				return failure.isDeadlock();
			};
		}

		/**
		 * Creates a condition that retries timeouts and cancellations recognized by Pyranid.
		 * <p>
		 * Timeouts and cancellations are not always safe to retry: the database may have completed part or all of the work
		 * before the timeout surfaced to the caller. Use this condition only when the retried transaction body is safe to run
		 * more than once for the application workflow.
		 *
		 * @return a timeout retry condition
		 */
		@NonNull
		static Condition timeout() {
			return failure -> {
				requireNonNull(failure);
				return failure.isTimeout();
			};
		}
	}

	/**
	 * Determines how long to wait before retrying after a failed transaction attempt.
	 * <p>
	 * Implementations should be thread-safe if the {@link RetryPolicy} is shared across threads.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 4.4.0
	 */
	@FunctionalInterface
	public interface Backoff {
		/**
		 * Determines how long to wait after a failed attempt before the next retry.
		 *
		 * @param failedAttemptNumber one-based number of the attempt that just failed
		 * @param failure             the database failure that caused the transaction attempt to fail
		 * @return delay before the next retry
		 */
		@NonNull
		Duration delayAfterFailedAttempt(@NonNull Integer failedAttemptNumber,
																			@NonNull DatabaseException failure);

		/**
		 * Creates a fixed backoff.
		 *
		 * @param delay delay before every retry
		 * @return a fixed backoff
		 */
		@NonNull
		static Backoff fixed(@NonNull Duration delay) {
			requireNonNull(delay);
			validateDelay(delay, "delay");

			return (failedAttemptNumber, failure) -> {
				requireNonNull(failedAttemptNumber);
				requireNonNull(failure);
				validateFailedAttemptNumber(failedAttemptNumber);
				return delay;
			};
		}

		/**
		 * Creates an exponential backoff.
		 * <p>
		 * Failed attempt 1 returns {@code initialDelay}, failed attempt 2 returns {@code initialDelay * 2}, failed attempt 3
		 * returns {@code initialDelay * 4}, and so on until the delay reaches {@code maxDelay}. Arithmetic overflow saturates
		 * at {@code maxDelay}.
		 *
		 * @param initialDelay initial delay
		 * @param maxDelay     maximum delay
		 * @return an exponential backoff
		 */
		@NonNull
		static Backoff exponential(@NonNull Duration initialDelay,
															 @NonNull Duration maxDelay) {
			requireNonNull(initialDelay);
			requireNonNull(maxDelay);
			validateDelay(initialDelay, "initialDelay");
			validateDelay(maxDelay, "maxDelay");

			if (maxDelay.compareTo(initialDelay) < 0)
				throw new IllegalArgumentException("maxDelay must be greater than or equal to initialDelay");

			return (failedAttemptNumber, failure) -> {
				requireNonNull(failedAttemptNumber);
				requireNonNull(failure);
				validateFailedAttemptNumber(failedAttemptNumber);

				if (initialDelay.isZero())
					return Duration.ZERO;

				Duration delay = initialDelay;

				for (int attempt = 1; attempt < failedAttemptNumber && delay.compareTo(maxDelay) < 0; ++attempt) {
					try {
						delay = delay.multipliedBy(2);
					} catch (ArithmeticException e) {
						return maxDelay;
					}

					if (delay.compareTo(maxDelay) > 0)
						return maxDelay;
				}

				return delay;
			};
		}

		private static void validateDelay(@NonNull Duration delay,
																			@NonNull String name) {
			requireNonNull(delay);
			requireNonNull(name);

			if (delay.isNegative())
				throw new IllegalArgumentException(format("%s must not be negative", name));
		}

		private static void validateFailedAttemptNumber(@NonNull Integer failedAttemptNumber) {
			requireNonNull(failedAttemptNumber);

			if (failedAttemptNumber < 1)
				throw new IllegalArgumentException("failedAttemptNumber must be at least 1");
		}
	}
}
