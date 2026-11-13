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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The completed database outcome, attempted callback value and invocation failure of one transaction boundary.
 * <p>
 * A callback value does not establish commit, and a failure does not establish rollback: cleanup or a
 * post-transaction hook can fail after a successful commit. Independent nested transactions and external side
 * effects are outside this boundary's result. This result is process-local, not a durable business receipt or
 * permission to replay an operation.
 * <p>
 * The callback value and original failure are retained by reference. Neither is copied or guaranteed immutable
 * or thread-safe. A final {@link Error} escapes the invocation instead of being retained in an outcome.
 *
 * @param <T> the callback value type
 * @since 4.9.0
 */
public final class TransactionOutcome<T> {
	@NonNull
	private final TransactionCompletion completion;
	@NonNull
	private final Optional<T> attemptedValue;

	TransactionOutcome(@NonNull TransactionCompletion completion, @NonNull Optional<T> attemptedValue) {
		this.completion = requireNonNull(completion);
		this.attemptedValue = requireNonNull(attemptedValue);
		if (completion.getFailure().filter(failure -> !(failure instanceof RuntimeException)).isPresent())
			throw new IllegalArgumentException("A transaction outcome can retain only a RuntimeException");
	}

	/**
	 * Gets the final completion, after connection cleanup and post-transaction hooks. A configured completion
	 * observer receives this same instance. Its identifiers correlate process-local boundaries, not durable operations.
	 *
	 * @return the completion for this transaction boundary
	 */
	@NonNull
	public TransactionCompletion getCompletion() {
		return this.completion;
	}

	/**
	 * Gets this boundary's database result, independently of the callback value and invocation failure.
	 * Boundaries without physical JDBC work retain their logical commit or rollback result.
	 *
	 * @return the database result
	 */
	@NonNull
	public TransactionResult getResult() {
		return this.completion.getResult();
	}

	/**
	 * Gets the value returned normally by the callback, even if completion subsequently rolled back, became
	 * uncertain or failed after commit. A callback that throws has no attempted value. Void callbacks, an empty
	 * optional and an accidentally null optional all produce an empty value.
	 *
	 * @return the attempted callback value, if any
	 */
	@NonNull
	public Optional<T> getAttemptedValue() {
		return this.attemptedValue;
	}

	/**
	 * Gets the final invocation failure the traditional transaction API would throw, retaining its identity,
	 * wrapping and suppressed failures. A committed transaction can have a failure, while normal rollback-only
	 * completion has none. Unrecognized failures, including programming errors, should normally be rethrown.
	 *
	 * @return the invocation failure, or empty for successful invocation completion
	 */
	@NonNull
	public Optional<RuntimeException> getFailure() {
		return this.completion.getFailure().map(RuntimeException.class::cast);
	}
}
