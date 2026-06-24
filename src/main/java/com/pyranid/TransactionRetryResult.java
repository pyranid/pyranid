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

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Result of a successful {@link Database#transactionWithRetry(RetryPolicy, ReturningTransactionalOperation)} call.
 * <p>
 * Retry methods return this type instead of a bare {@link Optional} so callers can inspect failures that were recovered
 * before the transaction eventually succeeded.
 * <p>
 * If retry attempts are exhausted, Pyranid throws the final {@link DatabaseException} instead of returning this type. Prior
 * failed attempts are attached to the thrown exception as suppressed exceptions.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.4.0
 */
@ThreadSafe
public final class TransactionRetryResult<T> {
	@Nullable
	private final T value;
	@NonNull
	private final List<@NonNull DatabaseException> failures;

	private TransactionRetryResult(@Nullable T value,
																 @NonNull List<@NonNull DatabaseException> failures) {
		requireNonNull(failures);

		this.value = value;
		this.failures = List.copyOf(failures);
	}

	@NonNull
	static <T> TransactionRetryResult<T> of(@NonNull Optional<T> value,
																					@NonNull List<@NonNull DatabaseException> failures) {
		requireNonNull(value);
		requireNonNull(failures);

		return new TransactionRetryResult<>(value.orElse(null), failures);
	}

	/**
	 * Gets the value returned by the successful transaction attempt.
	 *
	 * @return successful transaction value, or empty if no value was returned
	 */
	@NonNull
	public Optional<T> getValue() {
		return Optional.ofNullable(this.value);
	}

	/**
	 * Gets the failed attempts that were retried before the transaction eventually succeeded.
	 * <p>
	 * Failures are returned in occurrence order. The returned list is immutable.
	 *
	 * @return failed attempts that were retried before success
	 */
	@NonNull
	public List<@NonNull DatabaseException> getFailures() {
		return this.failures;
	}

	/**
	 * Gets the number of transaction attempts, including the successful final attempt.
	 *
	 * @return attempt count
	 */
	@NonNull
	public Integer getAttemptCount() {
		return getFailures().size() + 1;
	}

	/**
	 * Indicates whether at least one failed attempt was retried before success.
	 *
	 * @return {@code true} if the transaction succeeded after retrying, {@code false} otherwise
	 */
	@NonNull
	public Boolean wasRetried() {
		return !getFailures().isEmpty();
	}
}
