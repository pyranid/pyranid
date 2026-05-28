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

/**
 * Package-local dispatcher that applies the metrics disabled fast path and failure-containment contract.
 * <p>
 * Every dispatch method checks whether metrics are enabled before doing work and silently drops collector failures so
 * observability cannot affect JDBC behavior.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
@ThreadSafe
final class MetricsCollectorDispatcher {
	@NonNull
	private final MetricsCollector metricsCollector;
	private final boolean enabled;

	MetricsCollectorDispatcher(@Nullable MetricsCollector metricsCollector) {
		this.metricsCollector = metricsCollector == null ? MetricsCollector.disabledInstance() : metricsCollector;
		this.enabled = !(this.metricsCollector instanceof DisabledMetricsCollector);
	}

	@NonNull
	MetricsCollector getMetricsCollector() {
		return this.metricsCollector;
	}

	boolean isEnabled() {
		return this.enabled;
	}

	void willAcquireStatementConnection(@NonNull StatementContext<?> ctx) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.willAcquireStatementConnection(ctx);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didAcquireStatementConnection(@NonNull StatementContext<?> ctx,
																		 @NonNull Duration acquisitionDuration) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didAcquireStatementConnection(ctx, acquisitionDuration);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didFailToAcquireStatementConnection(@NonNull StatementContext<?> ctx,
																					 @NonNull Duration acquisitionDuration,
																					 @NonNull Throwable throwable) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didFailToAcquireStatementConnection(ctx, acquisitionDuration, throwable);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didReleaseStatementConnection(@NonNull StatementContext<?> ctx,
																		 @NonNull Duration heldDuration) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didReleaseStatementConnection(ctx, heldDuration);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didFailToReleaseStatementConnection(@NonNull StatementContext<?> ctx,
																					 @NonNull Duration heldDuration,
																					 @NonNull Throwable throwable) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didFailToReleaseStatementConnection(ctx, heldDuration, throwable);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void willAcquireTransactionConnection(@NonNull Transaction transaction,
																				@NonNull DatabaseType databaseType) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.willAcquireTransactionConnection(transaction, databaseType);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didAcquireTransactionConnection(@NonNull Transaction transaction,
																			 @NonNull DatabaseType databaseType,
																			 @NonNull Duration acquisitionDuration) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didAcquireTransactionConnection(transaction, databaseType, acquisitionDuration);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didFailToAcquireTransactionConnection(@NonNull Transaction transaction,
																					 @NonNull DatabaseType databaseType,
																					 @NonNull Duration acquisitionDuration,
																					 @NonNull Throwable throwable) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didFailToAcquireTransactionConnection(transaction, databaseType, acquisitionDuration, throwable);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didReleaseTransactionConnection(@NonNull Transaction transaction,
																			 @NonNull DatabaseType databaseType,
																			 @NonNull Duration heldDuration) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didReleaseTransactionConnection(transaction, databaseType, heldDuration);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didFailToReleaseTransactionConnection(@NonNull Transaction transaction,
																					 @NonNull DatabaseType databaseType,
																					 @NonNull Duration heldDuration,
																					 @NonNull Throwable throwable) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didFailToReleaseTransactionConnection(transaction, databaseType, heldDuration, throwable);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didEnterTransactionClosure(@NonNull Transaction transaction,
																	@NonNull TransactionIsolation isolation,
																	@NonNull DatabaseType databaseType) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didEnterTransactionClosure(transaction, isolation, databaseType);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didExitTransactionClosure(@NonNull Transaction transaction,
																 MetricsCollector.TransactionClosureOutcome outcome,
																 @NonNull DatabaseType databaseType,
																 @NonNull Duration logicalDuration,
																 @Nullable Throwable thrown) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didExitTransactionClosure(transaction, outcome, databaseType, logicalDuration, thrown);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didBeginPhysicalTransaction(@NonNull Transaction transaction,
																	 @NonNull TransactionIsolation isolation,
																	 @NonNull DatabaseType databaseType) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didBeginPhysicalTransaction(transaction, isolation, databaseType);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didFailToBeginPhysicalTransaction(@NonNull Transaction transaction,
																				 @NonNull TransactionIsolation isolation,
																				 MetricsCollector.PhysicalTransactionBeginFailurePhase phase,
																				 @NonNull DatabaseType databaseType,
																				 @NonNull Throwable throwable) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didFailToBeginPhysicalTransaction(transaction, isolation, phase, databaseType, throwable);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didCommitPhysicalTransaction(@NonNull Transaction transaction,
																		@NonNull DatabaseType databaseType,
																		@NonNull Duration physicalDuration) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didCommitPhysicalTransaction(transaction, databaseType, physicalDuration);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didFailToCommitPhysicalTransaction(@NonNull Transaction transaction,
																					@NonNull DatabaseType databaseType,
																					@NonNull Duration physicalDuration,
																					@NonNull Throwable throwable) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didFailToCommitPhysicalTransaction(transaction, databaseType, physicalDuration, throwable);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didRollbackPhysicalTransaction(@NonNull Transaction transaction,
																		 @NonNull DatabaseType databaseType,
																		 @NonNull Duration physicalDuration) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didRollbackPhysicalTransaction(transaction, databaseType, physicalDuration);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didFailToRollbackPhysicalTransaction(@NonNull Transaction transaction,
																						@NonNull DatabaseType databaseType,
																						@NonNull Duration physicalDuration,
																						@NonNull Throwable throwable) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didFailToRollbackPhysicalTransaction(transaction, databaseType, physicalDuration, throwable);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didRunPostTransactionOperation(@NonNull Transaction transaction,
																			@NonNull TransactionResult result,
																			@NonNull DatabaseType databaseType,
																			@NonNull Duration duration,
																			@Nullable Throwable throwable) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didRunPostTransactionOperation(transaction, result, databaseType, duration, throwable);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didCreateSavepoint(@NonNull Transaction transaction,
													@NonNull DatabaseType databaseType) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didCreateSavepoint(transaction, databaseType);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didRollbackToSavepoint(@NonNull Transaction transaction,
															@NonNull DatabaseType databaseType) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didRollbackToSavepoint(transaction, databaseType);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didReleaseSavepoint(@NonNull Transaction transaction,
													 @NonNull DatabaseType databaseType) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didReleaseSavepoint(transaction, databaseType);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void willExecuteStatement(@NonNull StatementContext<?> ctx) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.willExecuteStatement(ctx);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didExecuteStatement(@NonNull StatementContext<?> ctx,
													 @NonNull StatementLog<?> statementLog,
													 @NonNull StatementResult result) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didExecuteStatement(ctx, statementLog, result);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didFailToExecuteStatement(@NonNull StatementContext<?> ctx,
																 @NonNull StatementLog<?> statementLog,
																 @NonNull Throwable throwable) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didFailToExecuteStatement(ctx, statementLog, throwable);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void willOpenStream(@NonNull StatementContext<?> ctx) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.willOpenStream(ctx);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didOpenStream(@NonNull StatementContext<?> ctx,
										 @NonNull Duration openDuration) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didOpenStream(ctx, openDuration);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didFailToOpenStream(@NonNull StatementContext<?> ctx,
													 @NonNull Duration openDuration,
													 @NonNull Throwable throwable) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didFailToOpenStream(ctx, openDuration, throwable);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	void didCloseStream(@NonNull StatementContext<?> ctx,
											MetricsCollector.StreamTerminalOutcome outcome,
											@NonNull Long rowsConsumed,
											@NonNull Duration streamDuration,
											@Nullable Throwable throwable) {
		if (!isEnabled())
			return;

		try {
			this.metricsCollector.didCloseStream(ctx, outcome, rowsConsumed, streamDuration, throwable);
		} catch (Throwable t) {
			ignoreMetricsFailure(t);
		}
	}

	private void ignoreMetricsFailure(@NonNull Throwable ignored) {
		// Metrics collection is observational and must not affect database behavior.
	}
}
