/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

import static java.util.Objects.requireNonNull;

/** Rejects terminal evidence before an optional JDBC operation can fall back. */
final class JdbcFallbacks {
	private static final int TERMINAL = 1;
	private static final int INTERRUPTED = 2;
	private static final int COMMITTED = 4;
	private static final int IN_DOUBT = 8;

	private JdbcFallbacks() {}

	/** Inspects failures without publishing interruption or changing their identity. */
	static boolean isTerminal(Throwable failure) {
		return hasTerminalEvidence(failure, false);
	}

	/** Execution may fall back only on a direct capability signal with no conflicting failure evidence. */
	static boolean isExecutionFallbackSafe(Throwable failure) {
		requireNonNull(failure);
		if (!(failure instanceof SQLFeatureNotSupportedException
				|| failure instanceof UnsupportedOperationException || failure instanceof AbstractMethodError))
			return false;
		return !hasTerminalEvidence(failure, true);
	}

	private static boolean hasTerminalEvidence(Throwable failure, boolean execution) {
		requireNonNull(failure);
		int evidence = Thread.currentThread().isInterrupted() ? (execution ? INTERRUPTED : TERMINAL)
				: ThrowableGraph.inspectRetainingFailures(failure, current -> {
			if (current instanceof InterruptedException)
				return execution ? INTERRUPTED : TERMINAL;
			// A root AbstractMethodError is the legacy JDBC capability signal. Errors carried by that
			// signal, or by another failure, are still terminal.
			if (current instanceof Error && (current != failure || !(current instanceof AbstractMethodError))
					|| current instanceof java.io.InterruptedIOException
					|| current instanceof java.nio.channels.ClosedByInterruptException)
				return TERMINAL;
			if (execution && current instanceof java.io.IOException)
				return TERMINAL;
			if (current instanceof DatabaseException databaseException) {
				if (!execution) {
					if (databaseException.isTransactionOutcomeCommitted() || databaseException.isTransactionOutcomeIndeterminate())
						return TERMINAL;
				} else {
					int outcomes = 0;
					if (databaseException.isTransactionOutcomeCommitted()) outcomes |= COMMITTED;
					if (databaseException.isTransactionOutcomeIndeterminate()) outcomes |= IN_DOUBT;
					if (outcomes != 0) return outcomes;
				}
			}
			if (current instanceof PostTransactionOperationException postTransactionFailure) {
				TransactionResult result = postTransactionFailure.getTransactionResult();
				if (result != TransactionResult.ROLLED_BACK) {
					if (!execution) return TERMINAL;
					return result == TransactionResult.COMMITTED ? COMMITTED : IN_DOUBT;
				}
			}
			if (!(current instanceof SQLException sqlFailure))
				return 0;
			// A batch execution failure contradicts an unavailable-method signal even when its state is
			// absent or capability-shaped. Do not reread update-count accessors to authorize replay.
			if (execution && sqlFailure instanceof BatchUpdateException)
				return TERMINAL;
			if (sqlFailure instanceof SQLTransientException || sqlFailure instanceof SQLRecoverableException
					|| sqlFailure instanceof SQLNonTransientConnectionException)
				return TERMINAL;
			String sqlState = sqlFailure.getSQLState();
			// A typed capability signal cannot hide a server error elsewhere in its graph. Read SQLState once
			// in this scan so a changing or throwing accessor cannot erase evidence between separate guards.
			if (execution && sqlState != null && !sqlState.isEmpty()
					&& !sqlState.startsWith("0A") && !"HYC00".equals(sqlState))
				return TERMINAL;
			return sqlState != null && (sqlState.startsWith("40") || sqlState.startsWith("08")
					|| "57014".equals(sqlState) || "HY008".equals(sqlState)
					|| "HYT00".equals(sqlState) || "HYT01".equals(sqlState)) ? TERMINAL : 0;
		}, TERMINAL, current -> true, execution ? INTERRUPTED : 0);
		if (execution) retainExecutionEvidence(failure, evidence);
		return evidence != 0;
	}

	private static void retainExecutionEvidence(Throwable failure, int evidence) {
		int retained = evidence & (INTERRUPTED | COMMITTED | IN_DOUBT);
		if (retained == 0) return;
		// A later accessor may hide an outcome or consume a flag this scan already observed. Preserve
		// both the attempt fence and stable throwable evidence, without publishing interruption here.
		int outcomes = transactionOutcomes(retained);
		if (outcomes != 0) TransactionRetrySafety.currentOrNew().record(outcomes);
		for (Throwable suppressed : failure.getSuppressed())
			if (suppressed instanceof ExecutionFailureEvidence previous) retained &= ~previous.evidence;
		if (retained != 0) failure.addSuppressed(new ExecutionFailureEvidence(retained));
	}

	private static int transactionOutcomes(int evidence) {
		return ((evidence & COMMITTED) != 0 ? TransactionRetrySafety.COMMITTED : 0)
				| ((evidence & IN_DOUBT) != 0 ? TransactionRetrySafety.IN_DOUBT : 0);
	}

	private static final class ExecutionFailureEvidence extends DatabaseException {
		private static final long serialVersionUID = 1L;
		private final int evidence;

		private ExecutionFailureEvidence(int evidence) {
			super("Retained JDBC execution failure evidence", (evidence & INTERRUPTED) != 0
					? new InterruptedException("Interruption observed while inspecting a JDBC execution failure") : null);
			this.evidence = evidence;
			TransactionRetrySafety.applyTo(this, transactionOutcomes(evidence));
		}
	}
}
