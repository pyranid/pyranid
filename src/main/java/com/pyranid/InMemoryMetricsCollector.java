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
import java.util.concurrent.atomic.LongAdder;

/**
 * Counter-only {@link MetricsCollector} implementation used for in-process snapshots.
 * <p>
 * Instances are independent and stateful. {@link #reset()} uses {@link LongAdder#reset()}, so reset semantics are
 * best-effort when concurrent updates are in progress.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
@ThreadSafe
final class InMemoryMetricsCollector implements MetricsCollector {
	private final LongAdder connectionsAcquiredStatementScope;
	private final LongAdder connectionsAcquiredTransactionScope;
	private final LongAdder connectionsFailedStatementScope;
	private final LongAdder connectionsFailedTransactionScope;
	private final LongAdder connectionReleaseFailuresStatementScope;
	private final LongAdder connectionReleaseFailuresTransactionScope;
	private final LongAdder transactionClosuresEntered;
	private final LongAdder transactionClosuresExited;
	private final LongAdder transactionClosuresCommitted;
	private final LongAdder transactionClosuresRolledBack;
	private final LongAdder transactionClosuresNoPhysical;
	private final LongAdder transactionClosuresFailed;
	private final LongAdder physicalTransactionsBegun;
	private final LongAdder physicalTransactionsBeginFailed;
	private final LongAdder physicalTransactionsCommitted;
	private final LongAdder physicalTransactionsCommitFailed;
	private final LongAdder physicalTransactionsRolledBack;
	private final LongAdder physicalTransactionsRollbackFailed;
	private final LongAdder savepointsCreated;
	private final LongAdder savepointsRolledBack;
	private final LongAdder savepointsReleased;
	private final LongAdder statementsExecuted;
	private final LongAdder statementsFailed;
	private final LongAdder streamsOpened;
	private final LongAdder streamsOpenFailures;
	private final LongAdder streamsClosedNormally;
	private final LongAdder streamsEarlyClosed;
	private final LongAdder streamsCallbackFailed;
	private final LongAdder streamsIterationFailed;
	private final LongAdder postTransactionOperationsRun;
	private final LongAdder postTransactionOperationsFailed;

	@NonNull
	static InMemoryMetricsCollector defaultInstance() {
		return new InMemoryMetricsCollector();
	}

	private InMemoryMetricsCollector() {
		this.connectionsAcquiredStatementScope = new LongAdder();
		this.connectionsAcquiredTransactionScope = new LongAdder();
		this.connectionsFailedStatementScope = new LongAdder();
		this.connectionsFailedTransactionScope = new LongAdder();
		this.connectionReleaseFailuresStatementScope = new LongAdder();
		this.connectionReleaseFailuresTransactionScope = new LongAdder();
		this.transactionClosuresEntered = new LongAdder();
		this.transactionClosuresExited = new LongAdder();
		this.transactionClosuresCommitted = new LongAdder();
		this.transactionClosuresRolledBack = new LongAdder();
		this.transactionClosuresNoPhysical = new LongAdder();
		this.transactionClosuresFailed = new LongAdder();
		this.physicalTransactionsBegun = new LongAdder();
		this.physicalTransactionsBeginFailed = new LongAdder();
		this.physicalTransactionsCommitted = new LongAdder();
		this.physicalTransactionsCommitFailed = new LongAdder();
		this.physicalTransactionsRolledBack = new LongAdder();
		this.physicalTransactionsRollbackFailed = new LongAdder();
		this.savepointsCreated = new LongAdder();
		this.savepointsRolledBack = new LongAdder();
		this.savepointsReleased = new LongAdder();
		this.statementsExecuted = new LongAdder();
		this.statementsFailed = new LongAdder();
		this.streamsOpened = new LongAdder();
		this.streamsOpenFailures = new LongAdder();
		this.streamsClosedNormally = new LongAdder();
		this.streamsEarlyClosed = new LongAdder();
		this.streamsCallbackFailed = new LongAdder();
		this.streamsIterationFailed = new LongAdder();
		this.postTransactionOperationsRun = new LongAdder();
		this.postTransactionOperationsFailed = new LongAdder();
	}

	@Override
	public void didAcquireStatementConnection(@NonNull StatementContext<?> ctx,
																						@NonNull Duration acquisitionDuration) {
		this.connectionsAcquiredStatementScope.increment();
	}

	@Override
	public void didFailToAcquireStatementConnection(@NonNull StatementContext<?> ctx,
																									@NonNull Duration acquisitionDuration,
																									@NonNull Throwable throwable) {
		this.connectionsFailedStatementScope.increment();
	}

	@Override
	public void didAcquireTransactionConnection(@NonNull Transaction transaction,
																						 @NonNull DatabaseType databaseType,
																						 @NonNull Duration acquisitionDuration) {
		this.connectionsAcquiredTransactionScope.increment();
	}

	@Override
	public void didFailToAcquireTransactionConnection(@NonNull Transaction transaction,
																									 @NonNull DatabaseType databaseType,
																									 @NonNull Duration acquisitionDuration,
																									 @NonNull Throwable throwable) {
		this.connectionsFailedTransactionScope.increment();
	}

	@Override
	public void didFailToReleaseStatementConnection(@NonNull StatementContext<?> ctx,
																									@NonNull Duration heldDuration,
																									@NonNull Throwable throwable) {
		this.connectionReleaseFailuresStatementScope.increment();
	}

	@Override
	public void didFailToReleaseTransactionConnection(@NonNull Transaction transaction,
																									 @NonNull DatabaseType databaseType,
																									 @NonNull Duration heldDuration,
																									 @NonNull Throwable throwable) {
		this.connectionReleaseFailuresTransactionScope.increment();
	}

	@Override
	public void didEnterTransactionClosure(@NonNull Transaction transaction,
																				 @NonNull TransactionIsolation isolation,
																				 @NonNull DatabaseType databaseType) {
		this.transactionClosuresEntered.increment();
	}

	@Override
	public void didExitTransactionClosure(@NonNull Transaction transaction,
																				@NonNull TransactionClosureOutcome outcome,
																				@NonNull DatabaseType databaseType,
																				@NonNull Duration logicalDuration,
																				@Nullable Throwable thrown) {
		this.transactionClosuresExited.increment();

		switch (outcome) {
			case COMMITTED -> this.transactionClosuresCommitted.increment();
			case ROLLED_BACK -> this.transactionClosuresRolledBack.increment();
			case NO_PHYSICAL_TX -> this.transactionClosuresNoPhysical.increment();
			case FAILED -> this.transactionClosuresFailed.increment();
		}
	}

	@Override
	public void didBeginPhysicalTransaction(@NonNull Transaction transaction,
																					@NonNull TransactionIsolation isolation,
																					@NonNull DatabaseType databaseType) {
		this.physicalTransactionsBegun.increment();
	}

	@Override
	public void didFailToBeginPhysicalTransaction(@NonNull Transaction transaction,
																							 @NonNull TransactionIsolation isolation,
																							 @NonNull PhysicalTransactionBeginFailurePhase phase,
																							 @NonNull DatabaseType databaseType,
																							 @NonNull Throwable throwable) {
		this.physicalTransactionsBeginFailed.increment();
	}

	@Override
	public void didCommitPhysicalTransaction(@NonNull Transaction transaction,
																					 @NonNull DatabaseType databaseType,
																					 @NonNull Duration physicalDuration) {
		this.physicalTransactionsCommitted.increment();
	}

	@Override
	public void didFailToCommitPhysicalTransaction(@NonNull Transaction transaction,
																								@NonNull DatabaseType databaseType,
																								@NonNull Duration physicalDuration,
																								@NonNull Throwable throwable) {
		this.physicalTransactionsCommitFailed.increment();
	}

	@Override
	public void didRollbackPhysicalTransaction(@NonNull Transaction transaction,
																						@NonNull DatabaseType databaseType,
																						@NonNull Duration physicalDuration) {
		this.physicalTransactionsRolledBack.increment();
	}

	@Override
	public void didFailToRollbackPhysicalTransaction(@NonNull Transaction transaction,
																									 @NonNull DatabaseType databaseType,
																									 @NonNull Duration physicalDuration,
																									 @NonNull Throwable throwable) {
		this.physicalTransactionsRollbackFailed.increment();
	}

	@Override
	public void didCreateSavepoint(@NonNull Transaction transaction,
																 @NonNull DatabaseType databaseType) {
		this.savepointsCreated.increment();
	}

	@Override
	public void didRollbackToSavepoint(@NonNull Transaction transaction,
																		 @NonNull DatabaseType databaseType) {
		this.savepointsRolledBack.increment();
	}

	@Override
	public void didReleaseSavepoint(@NonNull Transaction transaction,
																	@NonNull DatabaseType databaseType) {
		this.savepointsReleased.increment();
	}

	@Override
	public void didExecuteStatement(@NonNull StatementContext<?> ctx,
																	@NonNull StatementLog<?> statementLog,
																	@NonNull StatementResult result) {
		this.statementsExecuted.increment();
	}

	@Override
	public void didFailToExecuteStatement(@NonNull StatementContext<?> ctx,
																				@NonNull StatementLog<?> statementLog,
																				@NonNull Throwable throwable) {
		this.statementsFailed.increment();
	}

	@Override
	public void didOpenStream(@NonNull StatementContext<?> ctx,
														@NonNull Duration openDuration) {
		this.streamsOpened.increment();
	}

	@Override
	public void didFailToOpenStream(@NonNull StatementContext<?> ctx,
																	@NonNull Duration openDuration,
																	@NonNull Throwable throwable) {
		this.streamsOpenFailures.increment();
	}

	@Override
	public void didCloseStream(@NonNull StatementContext<?> ctx,
														 @NonNull StreamTerminalOutcome outcome,
														 @NonNull Long rowsConsumed,
														 @NonNull Duration streamDuration,
														 @Nullable Throwable throwable) {
		switch (outcome) {
			case COMPLETED_NORMALLY -> this.streamsClosedNormally.increment();
			case EARLY_CLOSE -> this.streamsEarlyClosed.increment();
			case CALLBACK_FAILURE -> this.streamsCallbackFailed.increment();
			case ITERATION_FAILURE -> this.streamsIterationFailed.increment();
			case OPEN_FAILURE -> {
				// didFailToOpenStream owns this counter; didCloseStream normally is not emitted for open failures.
			}
		}
	}

	@Override
	public void didRunPostTransactionOperation(@NonNull Transaction transaction,
																						 @NonNull TransactionResult result,
																						 @NonNull DatabaseType databaseType,
																						 @NonNull Duration duration,
																						 @Nullable Throwable throwable) {
		this.postTransactionOperationsRun.increment();

		if (throwable != null)
			this.postTransactionOperationsFailed.increment();
	}

	@Override
	@NonNull
	public Optional<Snapshot> snapshot() {
		return Optional.of(new Snapshot(
				this.connectionsAcquiredStatementScope.sum(),
				this.connectionsAcquiredTransactionScope.sum(),
				this.connectionsFailedStatementScope.sum(),
				this.connectionsFailedTransactionScope.sum(),
				this.connectionReleaseFailuresStatementScope.sum(),
				this.connectionReleaseFailuresTransactionScope.sum(),
				this.transactionClosuresEntered.sum(),
				this.transactionClosuresExited.sum(),
				this.transactionClosuresCommitted.sum(),
				this.transactionClosuresRolledBack.sum(),
				this.transactionClosuresNoPhysical.sum(),
				this.transactionClosuresFailed.sum(),
				this.physicalTransactionsBegun.sum(),
				this.physicalTransactionsBeginFailed.sum(),
				this.physicalTransactionsCommitted.sum(),
				this.physicalTransactionsCommitFailed.sum(),
				this.physicalTransactionsRolledBack.sum(),
				this.physicalTransactionsRollbackFailed.sum(),
				this.savepointsCreated.sum(),
				this.savepointsRolledBack.sum(),
				this.savepointsReleased.sum(),
				this.statementsExecuted.sum(),
				this.statementsFailed.sum(),
				this.streamsOpened.sum(),
				this.streamsOpenFailures.sum(),
				this.streamsClosedNormally.sum(),
				this.streamsEarlyClosed.sum(),
				this.streamsCallbackFailed.sum(),
				this.streamsIterationFailed.sum(),
				this.postTransactionOperationsRun.sum(),
				this.postTransactionOperationsFailed.sum()));
	}

	@Override
	public void reset() {
		this.connectionsAcquiredStatementScope.reset();
		this.connectionsAcquiredTransactionScope.reset();
		this.connectionsFailedStatementScope.reset();
		this.connectionsFailedTransactionScope.reset();
		this.connectionReleaseFailuresStatementScope.reset();
		this.connectionReleaseFailuresTransactionScope.reset();
		this.transactionClosuresEntered.reset();
		this.transactionClosuresExited.reset();
		this.transactionClosuresCommitted.reset();
		this.transactionClosuresRolledBack.reset();
		this.transactionClosuresNoPhysical.reset();
		this.transactionClosuresFailed.reset();
		this.physicalTransactionsBegun.reset();
		this.physicalTransactionsBeginFailed.reset();
		this.physicalTransactionsCommitted.reset();
		this.physicalTransactionsCommitFailed.reset();
		this.physicalTransactionsRolledBack.reset();
		this.physicalTransactionsRollbackFailed.reset();
		this.savepointsCreated.reset();
		this.savepointsRolledBack.reset();
		this.savepointsReleased.reset();
		this.statementsExecuted.reset();
		this.statementsFailed.reset();
		this.streamsOpened.reset();
		this.streamsOpenFailures.reset();
		this.streamsClosedNormally.reset();
		this.streamsEarlyClosed.reset();
		this.streamsCallbackFailed.reset();
		this.streamsIterationFailed.reset();
		this.postTransactionOperationsRun.reset();
		this.postTransactionOperationsFailed.reset();
	}
}
