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

import javax.annotation.concurrent.Immutable;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Immutable observation of one completed transaction boundary, after cleanup and post-transaction hooks.
 * <p>
 * The database result and the invocation failure describe different facts: a committed transaction can still fail
 * during cleanup or a post-transaction hook. A normal rollback-only return has no failure. Nested independent
 * transactions have separate completions even when the same failure escapes both boundaries.
 * <p>
 * The failure is the original throwable reference, not an immutable copy of that throwable.
 *
 * @since 4.8.0
 */
@Immutable
public final class TransactionCompletion {
	@NonNull
	private final Long transactionId;
	@Nullable
	private final Long enclosingTransactionId;
	@NonNull
	private final TransactionResult result;
	@Nullable
	private final Throwable failure;

	TransactionCompletion(@NonNull Long transactionId, @Nullable Long enclosingTransactionId,
			@NonNull TransactionResult result, @Nullable Throwable failure) {
		this.transactionId = requireNonNull(transactionId);
		this.enclosingTransactionId = enclosingTransactionId;
		this.result = requireNonNull(result);
		this.failure = failure;
	}

	/**
	 * Gets this boundary's identifier. IDs correlate transactions within the process, not durable business operations.
	 *
	 * @return transaction identifier
	 */
	@NonNull
	public Long getTransactionId() {
		return this.transactionId;
	}

	/**
	 * Gets the identifier of the ambient transaction when this boundary started, including a different database's
	 * transaction when an independent nested transaction was started explicitly.
	 *
	 * @return enclosing transaction identifier, if any
	 */
	@NonNull
	public Optional<Long> getEnclosingTransactionId() {
		return Optional.ofNullable(this.enclosingTransactionId);
	}

	/**
	 * Gets this boundary's database outcome. Scopes without physical JDBC work retain their logical commit or rollback
	 * result. This result does not describe the outcome of independently completed nested transactions.
	 *
	 * @return transaction result
	 */
	@NonNull
	public TransactionResult getResult() {
		return this.result;
	}

	/**
	 * Gets the exact throwable propagated by this invocation, including suppressed cleanup and hook failures.
	 * The throwable may subsequently gain suppressed failures as it crosses enclosing transaction boundaries.
	 *
	 * @return invocation failure, or empty for a normal return
	 */
	@NonNull
	public Optional<Throwable> getFailure() {
		return Optional.ofNullable(this.failure);
	}
}
