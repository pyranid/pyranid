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

import javax.annotation.concurrent.NotThreadSafe;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Thrown when a post-transaction operation fails.
 * <p>
 * The {@link #getTransactionResult()} value describes how the transaction completed before the post-transaction operation
 * failed. For example, {@link TransactionResult#COMMITTED} means the transaction commit completed successfully, even though
 * a post-transaction operation failed afterwards.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
@NotThreadSafe
public class PostTransactionOperationException extends RuntimeException {
	@NonNull
	private final TransactionResult transactionResult;

	/**
	 * Creates a {@code PostTransactionOperationException} for a failed post-transaction operation.
	 *
	 * @param transactionResult the transaction result supplied to the failed post-transaction operation
	 * @param cause             the post-transaction operation failure
	 */
	public PostTransactionOperationException(@NonNull TransactionResult transactionResult,
																					 @NonNull Throwable cause) {
		super(format("Post-transaction operation failed after transaction result %s", requireNonNull(transactionResult)),
				requireNonNull(cause));

		this.transactionResult = transactionResult;
	}

	/**
	 * @return the transaction result supplied to the failed post-transaction operation
	 */
	@NonNull
	public TransactionResult getTransactionResult() {
		return this.transactionResult;
	}
}
