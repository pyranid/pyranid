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

import java.util.concurrent.atomic.AtomicInteger;

/** Retry evidence is scoped to an attempt, shared by participants, and never retains failures or threads. */
final class TransactionRetrySafety {
	static final int COMMITTED = 1;
	static final int IN_DOUBT = 2;
	private static final ThreadLocal<TransactionRetrySafety> CURRENT = new ThreadLocal<>();
	private final AtomicInteger outcomes = new AtomicInteger();

	static TransactionRetrySafety currentOrNew() {
		TransactionRetrySafety current = CURRENT.get();
		return current == null ? new TransactionRetrySafety() : current;
	}

	void record(int additional) {
		if (additional != 0)
			this.outcomes.getAndUpdate(existing -> existing | additional);
	}

	int outcomes() {
		return this.outcomes.get();
	}

	Scope activate() {
		TransactionRetrySafety previous = CURRENT.get();
		CURRENT.set(this);
		return new Scope(this, previous, false);
	}

	static Scope isolate() {
		return isolate(true);
	}

	// Preserve already-pending interruption across diagnostics by default, including inside a transaction.
	// Failure-path dialect detection alone opts out under an ambient transaction to retain existing handling of
	// cancellation caught inside the closure. Cancellation consumed there remains a known limitation; this opt-out
	// is not a general rule for transaction diagnostics.
	static Scope isolate(boolean shouldPreserveInterrupt) {
		TransactionRetrySafety previous = CURRENT.get();
		boolean interrupted = shouldPreserveInterrupt && Thread.currentThread().isInterrupted();
		if (previous == null && !interrupted) return null;
		// Diagnostics must not consume cancellation the caller still needs, nor contribute independent transaction
		// outcomes. Preserve the no-allocation fast path when neither kind of state needs protection.
		if (previous != null) CURRENT.remove();
		return new Scope(null, previous, interrupted);
	}

	static final class Scope implements AutoCloseable {
		private final TransactionRetrySafety active;
		private final TransactionRetrySafety previous;
		private final boolean interrupted;

		private Scope(TransactionRetrySafety active, TransactionRetrySafety previous, boolean interrupted) {
			this.active = active;
			this.previous = previous;
			this.interrupted = interrupted;
		}

		@Override
		public void close() {
			if (this.previous == null) {
				CURRENT.remove();
			} else {
				// An explicitly nested retry attempt or participation binding must not hide unsafe work from its caller.
				// Diagnostic isolation has no active state to merge.
				if (this.active != null) this.previous.record(this.active.outcomes());
				CURRENT.set(this.previous);
			}
			if (this.interrupted) Thread.currentThread().interrupt();
		}
	}

	static void applyTo(DatabaseException failure, int outcomes) {
		if ((outcomes & COMMITTED) != 0)
			failure.markTransactionOutcomeCommitted();
		if ((outcomes & IN_DOUBT) != 0)
			failure.markTransactionOutcomeIndeterminate();
	}

	static int outcomesOf(Throwable failure) {
		return ThrowableGraph.inspect(failure, current -> {
			int outcomes = 0;
			if (current instanceof DatabaseException databaseException) {
				if (databaseException.isTransactionOutcomeCommitted()) outcomes |= COMMITTED;
				if (databaseException.isTransactionOutcomeIndeterminate()) outcomes |= IN_DOUBT;
			} else if (current instanceof PostTransactionOperationException postTransactionFailure) {
				if (postTransactionFailure.getTransactionResult() == TransactionResult.COMMITTED) outcomes |= COMMITTED;
				if (postTransactionFailure.getTransactionResult() == TransactionResult.IN_DOUBT) outcomes |= IN_DOUBT;
			}
			return outcomes;
		}, IN_DOUBT);
	}
}
