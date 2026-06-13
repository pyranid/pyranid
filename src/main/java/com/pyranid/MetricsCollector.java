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
import java.time.Duration;
import java.util.Optional;

/**
 * Contract for collecting operational metrics from Pyranid.
 * <p>
 * The default collector is {@link #disabledInstance()}, which performs no work. {@link #inMemoryInstance()} returns a
 * fresh counter-only collector useful for tests and ad-hoc inspection through {@link #snapshot()}.
 * <p>
 * Implementations must be thread-safe, non-blocking, and failure-tolerant. Pyranid catches and discards collector
 * exceptions so metrics collection cannot affect database behavior.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
@ThreadSafe
public interface MetricsCollector {
	default void willAcquireStatementConnection(@NonNull StatementContext<?> ctx) {
		// No-op by default
	}

	default void didAcquireStatementConnection(@NonNull StatementContext<?> ctx,
																						 @NonNull Duration acquisitionDuration) {
		// No-op by default
	}

	default void didFailToAcquireStatementConnection(@NonNull StatementContext<?> ctx,
																									 @NonNull DatabaseType databaseType,
																									 @NonNull Duration acquisitionDuration,
																									 @NonNull Throwable throwable) {
		// No-op by default
	}

	default void didReleaseStatementConnection(@NonNull StatementContext<?> ctx,
																						 @NonNull Duration heldDuration) {
		// No-op by default
	}

	default void didFailToReleaseStatementConnection(@NonNull StatementContext<?> ctx,
																									 @NonNull Duration heldDuration,
																									 @NonNull Throwable throwable) {
		// No-op by default
	}

	default void willAcquireTransactionConnection(@NonNull Transaction transaction,
																							 @NonNull DatabaseType databaseType) {
		// No-op by default
	}

	default void didAcquireTransactionConnection(@NonNull Transaction transaction,
																							@NonNull DatabaseType databaseType,
																							@NonNull Duration acquisitionDuration) {
		// No-op by default
	}

	default void didFailToAcquireTransactionConnection(@NonNull Transaction transaction,
																											@NonNull DatabaseType databaseType,
																											@NonNull Duration acquisitionDuration,
																											@NonNull Throwable throwable) {
		// No-op by default
	}

	default void didReleaseTransactionConnection(@NonNull Transaction transaction,
																							@NonNull DatabaseType databaseType,
																							@NonNull Duration heldDuration) {
		// No-op by default
	}

	default void didFailToReleaseTransactionConnection(@NonNull Transaction transaction,
																											@NonNull DatabaseType databaseType,
																											@NonNull Duration heldDuration,
																											@NonNull Throwable throwable) {
		// No-op by default
	}

	default void didEnterTransactionClosure(@NonNull Transaction transaction,
																					@NonNull TransactionIsolation isolation,
																					@NonNull DatabaseType databaseType) {
		// No-op by default
	}

	default void didExitTransactionClosure(@NonNull Transaction transaction,
																				 @NonNull TransactionClosureOutcome outcome,
																				 @NonNull DatabaseType databaseType,
																				 @NonNull Duration logicalDuration,
																				 @Nullable Throwable thrown) {
		// No-op by default
	}

	default void didBeginPhysicalTransaction(@NonNull Transaction transaction,
																					 @NonNull TransactionIsolation isolation,
																					 @NonNull DatabaseType databaseType) {
		// No-op by default
	}

	default void didFailToBeginPhysicalTransaction(@NonNull Transaction transaction,
																								@NonNull TransactionIsolation isolation,
																								@NonNull PhysicalTransactionBeginFailurePhase phase,
																								@NonNull DatabaseType databaseType,
																								@NonNull Throwable throwable) {
		// No-op by default
	}

	default void didCommitPhysicalTransaction(@NonNull Transaction transaction,
																						@NonNull DatabaseType databaseType,
																						@NonNull Duration physicalDuration) {
		// No-op by default
	}

	default void didFailToCommitPhysicalTransaction(@NonNull Transaction transaction,
																								 @NonNull DatabaseType databaseType,
																								 @NonNull Duration physicalDuration,
																								 @NonNull Throwable throwable) {
		// No-op by default
	}

	default void didRollbackPhysicalTransaction(@NonNull Transaction transaction,
																						 @NonNull DatabaseType databaseType,
																						 @NonNull Duration physicalDuration) {
		// No-op by default
	}

	default void didFailToRollbackPhysicalTransaction(@NonNull Transaction transaction,
																										@NonNull DatabaseType databaseType,
																										@NonNull Duration physicalDuration,
																										@NonNull Throwable throwable) {
		// No-op by default
	}

	default void didRunPostTransactionOperation(@NonNull Transaction transaction,
																						 @NonNull TransactionResult result,
																						 @NonNull DatabaseType databaseType,
																						 @NonNull Duration duration,
																						 @Nullable Throwable throwable) {
		// No-op by default
	}

	default void didCreateSavepoint(@NonNull Transaction transaction,
																	@NonNull DatabaseType databaseType) {
		// No-op by default
	}

	default void didRollbackToSavepoint(@NonNull Transaction transaction,
																			@NonNull DatabaseType databaseType) {
		// No-op by default
	}

	default void didReleaseSavepoint(@NonNull Transaction transaction,
																	 @NonNull DatabaseType databaseType) {
		// No-op by default
	}

	default void willExecuteStatement(@NonNull StatementContext<?> ctx) {
		// No-op by default
	}

	default void didExecuteStatement(@NonNull StatementContext<?> ctx,
																	 @NonNull StatementLog<?> statementLog,
																	 @NonNull StatementResult result) {
		// No-op by default
	}

	default void didFailToExecuteStatement(@NonNull StatementContext<?> ctx,
																				 @NonNull StatementLog<?> statementLog,
																				 @NonNull DatabaseType databaseType,
																				 @NonNull Throwable throwable) {
		// No-op by default
	}

	default void willOpenStream(@NonNull StatementContext<?> ctx) {
		// No-op by default
	}

	default void didOpenStream(@NonNull StatementContext<?> ctx,
														 @NonNull Duration openDuration) {
		// No-op by default
	}

	default void didFailToOpenStream(@NonNull StatementContext<?> ctx,
																	 @NonNull DatabaseType databaseType,
																	 @NonNull Duration openDuration,
																	 @NonNull Throwable throwable) {
		// No-op by default
	}

	default void didCloseStream(@NonNull StatementContext<?> ctx,
															@NonNull StreamTerminalOutcome outcome,
															@NonNull Long rowsConsumed,
															@NonNull Duration streamDuration,
															@Nullable Throwable throwable) {
		// No-op by default
	}

	/**
	 * Returns a counter snapshot if this collector supports in-process inspection.
	 * <p>
	 * Implementations may read counters independently without cross-counter atomicity. Callers that need invariant-consistent
	 * snapshots should drain in-flight work before reading.
	 *
	 * @return a metrics snapshot, if supported
	 */
	@NonNull
	default Optional<Snapshot> snapshot() {
		return Optional.empty();
	}

	/**
	 * Best-effort reset for test and ad-hoc use.
	 * <p>
	 * Concurrent updates may race with reset operations. Production rolling-window semantics should be implemented outside
	 * this interface.
	 */
	default void reset() {
		// No-op by default
	}

	@NonNull
	static MetricsCollector disabledInstance() {
		return DisabledMetricsCollector.defaultInstance();
	}

	@NonNull
	static MetricsCollector inMemoryInstance() {
		return InMemoryMetricsCollector.defaultInstance();
	}

	enum TransactionClosureOutcome {
		COMMITTED,
		ROLLED_BACK,
		NO_PHYSICAL_TX,
		FAILED
	}

	enum PhysicalTransactionBeginFailurePhase {
		ACQUIRE_CONNECTION,
		READ_INITIAL_AUTOCOMMIT,
		READ_INITIAL_ISOLATION,
		SET_AUTOCOMMIT_FALSE,
		SET_ISOLATION
	}

	enum StreamTerminalOutcome {
		COMPLETED_NORMALLY,
		EARLY_CLOSE,
		CALLBACK_FAILURE,
		ITERATION_FAILURE,
		OPEN_FAILURE
	}

	@ThreadSafe
	record Snapshot(@NonNull Long connectionsAcquiredStatementScope,
									@NonNull Long connectionsAcquiredTransactionScope,
									@NonNull Long connectionsFailedStatementScope,
									@NonNull Long connectionsFailedTransactionScope,
									@NonNull Long connectionReleaseFailuresStatementScope,
									@NonNull Long connectionReleaseFailuresTransactionScope,
									@NonNull Long transactionClosuresEntered,
									@NonNull Long transactionClosuresExited,
									@NonNull Long transactionClosuresCommitted,
									@NonNull Long transactionClosuresRolledBack,
									@NonNull Long transactionClosuresNoPhysical,
									@NonNull Long transactionClosuresFailed,
									@NonNull Long physicalTransactionsBegun,
									@NonNull Long physicalTransactionsBeginFailed,
									@NonNull Long physicalTransactionsCommitted,
									@NonNull Long physicalTransactionsCommitFailed,
									@NonNull Long physicalTransactionsRolledBack,
									@NonNull Long physicalTransactionsRollbackFailed,
									@NonNull Long savepointsCreated,
									@NonNull Long savepointsRolledBack,
									@NonNull Long savepointsReleased,
									@NonNull Long statementsExecuted,
									@NonNull Long statementsFailed,
									@NonNull Long streamsOpened,
									@NonNull Long streamsOpenFailures,
									@NonNull Long streamsClosedNormally,
									@NonNull Long streamsEarlyClosed,
									@NonNull Long streamsCallbackFailed,
									@NonNull Long streamsIterationFailed,
									@NonNull Long postTransactionOperationsRun,
									@NonNull Long postTransactionOperationsFailed) {
	}
}
