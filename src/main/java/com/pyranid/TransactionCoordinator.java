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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;

/** Coordinates transaction boundaries while retaining a thread-wide ambient stack. */
final class TransactionCoordinator {
	static final ThreadLocal<Deque<Transaction>> TRANSACTION_STACK_HOLDER = new ThreadLocal<>();

	private final DataSource dataSource;
	private final MetricsCollectorDispatcher metrics;
	private final Supplier<DatabaseType> cachedDatabaseType;
	private final Function<Connection, DatabaseType> databaseTypeResolver;
	private final Logger logger = Logger.getLogger(TransactionCoordinator.class.getName());

	TransactionCoordinator(DataSource dataSource, MetricsCollectorDispatcher metrics,
			Supplier<DatabaseType> cachedDatabaseType, Function<Connection, DatabaseType> databaseTypeResolver) {
		this.dataSource = requireNonNull(dataSource);
		this.metrics = requireNonNull(metrics);
		this.cachedDatabaseType = requireNonNull(cachedDatabaseType);
		this.databaseTypeResolver = requireNonNull(databaseTypeResolver);
	}

	@NonNull
	Optional<Transaction> currentTransaction() {
		@Nullable Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();
		Transaction transaction = transactionStack == null || transactionStack.isEmpty() ? null : transactionStack.peek();

		if (transaction == null || !isTransactionOwnedByThisDatabase(transaction))
			return Optional.empty();

		return Optional.of(transaction);
	}

	@NonNull
	Optional<Transaction> currentTransactionForDatabaseOperation() {
		@Nullable Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();
		Transaction transaction = transactionStack == null || transactionStack.isEmpty() ? null : transactionStack.peek();

		if (transaction == null)
			return Optional.empty();

		if (!isTransactionOwnedByThisDatabase(transaction))
			throw wrongDatabaseTransactionException(transaction);

		return Optional.of(transaction);
	}

	static boolean hasAmbientTransaction() {
		Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();
		return transactionStack != null && !transactionStack.isEmpty();
	}

	@NonNull
	private Deque<Transaction> transactionStackForPush() {
		Deque<Transaction> transactionStack = TRANSACTION_STACK_HOLDER.get();

		if (transactionStack == null) {
			transactionStack = new ArrayDeque<>();
			TRANSACTION_STACK_HOLDER.set(transactionStack);
		}

		return transactionStack;
	}

	private boolean isTransactionOwnedByThisDatabase(@NonNull Transaction transaction) {
		requireNonNull(transaction);
		return transaction.isOwnedBy(this.dataSource);
	}

	@NonNull
	private DatabaseException wrongDatabaseTransactionException(@NonNull Transaction transaction) {
		requireNonNull(transaction);
		return new DatabaseException(format("Transaction %s belongs to a different %s than this %s. "
						+ "Use the %s instance that created the transaction, or explicitly participate only with a transaction "
						+ "created from the same %s.",
				transaction.id(), DataSource.class.getSimpleName(), Database.class.getSimpleName(),
				Database.class.getSimpleName(), DataSource.class.getSimpleName()));
	}

	@NonNull
	<T> Optional<T> run(@NonNull TransactionOptions transactionOptions,
																		 @NonNull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transactionOptions);
		requireNonNull(transactionalOperation);

		Transaction transaction = new Transaction(dataSource, transactionOptions, this.metrics,
				this.cachedDatabaseType.get(), this.databaseTypeResolver);
		try (var ignored = transaction.retrySafety().activate()) {
			return run(transaction, transactionOptions, transactionalOperation);
		}
	}

	private <T> Optional<T> run(Transaction transaction, TransactionOptions transactionOptions,
			ReturningTransactionalOperation<T> transactionalOperation) {
		Deque<Transaction> transactionStack = transactionStackForPush();
		Long enclosingTransactionId = transactionStack.isEmpty() ? null : transactionStack.peek().id();
		transactionStack.push(transaction);
		boolean committed = false;
		boolean commitFailed = false;
		boolean rollbackFailed = false;
		boolean rollbackAttempted = false;
		Throwable thrown = null;
		long transactionStartTime = nanoTime();
		this.metrics.didEnterTransactionClosure(transaction, transactionOptions.getIsolation(), transaction.getDatabaseType());

		try {
			Optional<T> returnValue;
			try {
				returnValue = transactionalOperation.perform();
			} finally {
				// Close public admission before waiting for the JDBC lock on either the success or failure path.
				transaction.beginCompletion();
			}

			// Safeguard in case user code accidentally returns null instead of Optional.empty()
			if (returnValue == null)
				returnValue = Optional.empty();

			transaction.getConnectionLock().lock();

			try {
				// A failed physical begin is terminal for this transaction even if application code caught its first
				// manifestation. Never commit, or report success from a rollback-only path, through a partially initialized
				// connection.
				transaction.throwPhysicalTransactionBeginFailureIfPresent();

				if (transaction.isRollbackOnly()) {
					rollbackAttempted = true;
					transaction.rollback();
				} else {
					try {
						transaction.commit();
					} catch (RuntimeException | Error e) {
						// The savepoint-ownership guard refuses the commit before issuing it, so nothing committed and the rollback
						// below proves the outcome. Every other commit failure leaves the outcome unknown.
						commitFailed = !transaction.didSkipCommitForSavepointScopeConflict(e);
						throw e;
					}
					committed = true;
				}

				transaction.markCompleted();
			} finally {
				transaction.getConnectionLock().unlock();
			}

			return returnValue;
		} catch (RuntimeException e) {
			thrown = e;
			if (rollbackAttempted) {
				rollbackFailed = true;
				markTransactionCompleted(transaction);
			} else {
				rollbackFailed = rollbackTransactionAfterFailure(transaction, e);
			}

			Database.restoreInterruptIfNeeded(e);
			throw e;
		} catch (Error e) {
			thrown = e;
			if (rollbackAttempted) {
				rollbackFailed = true;
				markTransactionCompleted(transaction);
			} else {
				rollbackFailed = rollbackTransactionAfterFailure(transaction, e);
			}

			Database.restoreInterruptIfNeeded(e);
			throw e;
		} catch (Throwable t) {
			RuntimeException wrapped = new RuntimeException(t);
			thrown = wrapped;
			if (rollbackAttempted) {
				rollbackFailed = true;
				markTransactionCompleted(transaction);
			} else {
				rollbackFailed = rollbackTransactionAfterFailure(transaction, wrapped);
			}

			Database.restoreInterruptIfNeeded(t);
			throw wrapped;
		} finally {
			transactionStack.pop();

			// Ensure txn stack is fully cleaned up
			if (transactionStack.isEmpty())
				TRANSACTION_STACK_HOLDER.remove();

			Throwable cleanupFailure = null;
			boolean hadPhysicalTransaction = false;
			boolean physicalTransactionBeganSuccessfully = false;
			boolean completionFailed = commitFailed || rollbackFailed;
			boolean commitSerializationFailureRolledBack = commitFailed && !rollbackFailed
					&& transaction.didCommitFailWithSerializationFailure();
			boolean transactionOutcomeIndeterminate =
					(commitFailed && !commitSerializationFailureRolledBack)
							|| (rollbackFailed && transaction.couldApplicationWorkHaveExecuted());
			boolean discardConnection = completionFailed || transaction.didPhysicalTransactionBeginFail();
			TransactionResult result = transactionResult(committed, transactionOutcomeIndeterminate);

			try {
				transaction.getConnectionLock().lock();

				try {
					hadPhysicalTransaction = transaction.hasConnection();
					physicalTransactionBeganSuccessfully = transaction.didPhysicalTransactionBeginSuccessfully();

					if (!transaction.isCompleted())
						transaction.markCompleted();

					cleanupFailure = cleanupCompletedTransactionConnection(transaction, cleanupFailure, discardConnection);
				} finally {
					transaction.getConnectionLock().unlock();
				}
			} finally {
				if (transactionOutcomeIndeterminate && thrown instanceof DatabaseException)
					((DatabaseException) thrown).markTransactionOutcomeIndeterminate();

				// Execute any user-supplied post-execution hooks
				for (Consumer<TransactionResult> postTransactionOperation : transaction.getPostTransactionOperations()) {
					long postTransactionStartTime = nanoTime();
					Throwable postTransactionThrowable = null;
					TransactionResult transactionResult = result;
					try {
						postTransactionOperation.accept(transactionResult);
					} catch (Throwable cleanupException) {
						postTransactionThrowable = cleanupException;
						PostTransactionOperationException postTransactionOperationException =
								new PostTransactionOperationException(transactionResult, cleanupException);

						if (cleanupFailure == null)
							cleanupFailure = postTransactionOperationException;
						else
							cleanupFailure.addSuppressed(postTransactionOperationException);
					} finally {
						this.metrics.didRunPostTransactionOperation(transaction, transactionResult, transaction.getDatabaseType(),
								Duration.ofNanos(nanoTime() - postTransactionStartTime), postTransactionThrowable);
					}
				}
			}

			Throwable exitThrown = thrown;

			if (cleanupFailure != null) {
				if (thrown != null) {
					appendSuppressed(thrown, cleanupFailure);
				} else {
					exitThrown = cleanupFailure instanceof RuntimeException || cleanupFailure instanceof Error
							? cleanupFailure : new RuntimeException(cleanupFailure);
					if (committed && exitThrown instanceof DatabaseException databaseException)
						databaseException.markTransactionOutcomeCommitted();
				}
			}

			int unsafeOutcomes = TransactionRetrySafety.outcomesOf(exitThrown);
			if (transactionOutcomeIndeterminate) unsafeOutcomes |= TransactionRetrySafety.IN_DOUBT;
			if (committed && exitThrown != null) unsafeOutcomes |= TransactionRetrySafety.COMMITTED;
			transaction.retrySafety().record(unsafeOutcomes);
			if (exitThrown instanceof DatabaseException databaseException)
				TransactionRetrySafety.applyTo(databaseException, transaction.retrySafety().outcomes());

			this.metrics.didExitTransactionClosure(transaction,
					transactionClosureOutcome(committed, hadPhysicalTransaction, physicalTransactionBeganSuccessfully,
							transactionOutcomeIndeterminate),
					transaction.getDatabaseType(), Duration.ofNanos(nanoTime() - transactionStartTime), exitThrown);

			Consumer<TransactionCompletion> observer = transactionOptions.getCompletionObserver().orElse(null);
			if (observer != null)
				observeCompletion(observer, new TransactionCompletion(transaction.id(), enclosingTransactionId, result, exitThrown));

			// Cleanup and post-transaction hooks can introduce interruption after the operation's catch block.
			// Restore from the final failure after diagnostics, which may themselves consume the thread flag.
			if (exitThrown != null)
				Database.restoreInterruptIfNeeded(exitThrown);

			if (thrown == null && exitThrown != null) {
				if (exitThrown instanceof Error error)
					throw error;
				throw (RuntimeException) exitThrown;
			}
		}
	}

	private void observeCompletion(Consumer<TransactionCompletion> observer, TransactionCompletion completion) {
		try (var ignored = TransactionRetrySafety.isolate()) {
			observer.accept(completion);
		} catch (Throwable ignored) {
			try (var ignoredScope = TransactionRetrySafety.isolate()) {
				// Avoid arbitrary throwable messages, stack traces, or user-defined toString() in diagnostics.
				this.logger.log(Level.FINE, "Transaction completion observer failure ignored");
			} catch (Throwable loggingFailure) {
				// Broken application logging handlers must not affect completion or retry decisions.
			}
		}
	}

	private boolean rollbackTransactionAfterFailure(@NonNull Transaction transaction,
																								 @NonNull Throwable primary) {
		requireNonNull(transaction);
		requireNonNull(primary);

		boolean rollbackFailed = false;
		transaction.getConnectionLock().lock();

		try {
			try {
				if (transaction.isPhysicalRollbackPermitted()) {
					try {
						transaction.rollback();
					} catch (Throwable rollbackException) {
						rollbackFailed = true;
						if (primary != rollbackException)
							primary.addSuppressed(rollbackException);
					}
				}
			} finally {
				transaction.markCompleted();
			}
		} finally {
			transaction.getConnectionLock().unlock();
		}

		return rollbackFailed;
	}

	private void markTransactionCompleted(@NonNull Transaction transaction) {
		requireNonNull(transaction);
		transaction.getConnectionLock().lock();

		try {
			transaction.markCompleted();
		} finally {
			transaction.getConnectionLock().unlock();
		}
	}

	@Nullable
	private Throwable cleanupCompletedTransactionConnection(@NonNull Transaction transaction,
																		 @Nullable Throwable cleanupFailure,
																		 boolean discardConnection) {
		requireNonNull(transaction);
		boolean abortRequired = discardConnection;

		// A begin, commit, or rollback failure makes the connection unsafe to restore and reuse, even when the logical outcome
		// is known. When the outcome is unknown, restoring auto-commit to true is especially dangerous because it can commit
		// work that Pyranid meant to roll back.
		if (!abortRequired) {
			try {
				transaction.restoreTransactionIsolationIfNeeded();
			} catch (Throwable cleanupException) {
				cleanupFailure = appendSuppressed(cleanupFailure, cleanupException);
				abortRequired = true;
			}

			if (!abortRequired) {
				try {
					transaction.restoreReadOnlyIfNeeded();
				} catch (Throwable cleanupException) {
					cleanupFailure = appendSuppressed(cleanupFailure, cleanupException);
					abortRequired = true;
				}
			}

			if (!abortRequired && transaction.getInitialAutoCommit().isPresent() && transaction.getInitialAutoCommit().get()) {
				try {
					// Autocommit was true initially, so restoring to true now that transaction has completed
					transaction.setAutoCommit(true);
				} catch (Throwable cleanupException) {
					cleanupFailure = appendSuppressed(cleanupFailure, cleanupException);
					abortRequired = true;
				}
			}
		}

		Connection connection = transaction.getExistingConnection().orElse(null);

		if (connection != null) {
			Duration heldDuration = transaction.getConnectionAcquiredAtNanos()
					.map(acquiredAtNanos -> Duration.ofNanos(nanoTime() - acquiredAtNanos))
					.orElse(Duration.ZERO);

			if (abortRequired) {
				try {
					abortConnection(connection);
				} catch (Throwable cleanupException) {
					cleanupFailure = appendSuppressed(cleanupFailure, cleanupException);
				}
			}

			try {
				Database.closeConnection(connection);
				this.metrics.didReleaseTransactionConnection(transaction, transaction.getDatabaseType(), heldDuration);
			} catch (Throwable cleanupException) {
				this.metrics.didFailToReleaseTransactionConnection(transaction, transaction.getDatabaseType(), heldDuration, cleanupException);
				cleanupFailure = appendSuppressed(cleanupFailure, cleanupException);
			} finally {
				// A completed Transaction must never retain a discarded or closed connection handle, including when close fails.
				transaction.clearConnection();
			}
		}

		return cleanupFailure;
	}

	@NonNull
	private static Throwable appendSuppressed(@Nullable Throwable existing,
																						@NonNull Throwable additional) {
		requireNonNull(additional);

		if (existing == null)
			return additional;

		if (existing != additional)
			existing.addSuppressed(additional);

		return existing;
	}

	private void abortConnection(@NonNull Connection connection) {
		requireNonNull(connection);

		try {
			connection.abort(Runnable::run);
		} catch (SQLException e) {
			throw DatabaseException.wrapSafely("Unable to abort database connection", e,
					DatabaseDialect.forExceptionCause(e));
		}
	}

	@NonNull
	private static TransactionResult transactionResult(@NonNull Boolean committed,
																				 @NonNull Boolean transactionOutcomeIndeterminate) {
		requireNonNull(committed);
		requireNonNull(transactionOutcomeIndeterminate);

		if (committed)
			return TransactionResult.COMMITTED;

		return transactionOutcomeIndeterminate ? TransactionResult.IN_DOUBT : TransactionResult.ROLLED_BACK;
	}

	private static MetricsCollector.TransactionClosureOutcome transactionClosureOutcome(@NonNull Boolean committed,
																							 @NonNull Boolean hadPhysicalTransaction,
																							 @NonNull Boolean physicalTransactionBeganSuccessfully,
																							 @NonNull Boolean transactionOutcomeIndeterminate) {
		requireNonNull(committed);
		requireNonNull(hadPhysicalTransaction);
		requireNonNull(physicalTransactionBeganSuccessfully);
		requireNonNull(transactionOutcomeIndeterminate);

		if (!hadPhysicalTransaction)
			return MetricsCollector.TransactionClosureOutcome.NO_PHYSICAL_TX;

		if (!physicalTransactionBeganSuccessfully)
			return MetricsCollector.TransactionClosureOutcome.FAILED;

		if (committed)
			return MetricsCollector.TransactionClosureOutcome.COMMITTED;

		return transactionOutcomeIndeterminate
				? MetricsCollector.TransactionClosureOutcome.FAILED
				: MetricsCollector.TransactionClosureOutcome.ROLLED_BACK;
	}

	@NonNull
	<T> Optional<T> participate(@NonNull Transaction transaction,
																		 @NonNull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transaction);
		requireNonNull(transactionalOperation);

		if (!isTransactionOwnedByThisDatabase(transaction))
			throw wrongDatabaseTransactionException(transaction);

		transaction.assertCanParticipate();

		try (var ignored = transaction.retrySafety().activate()) {
			return participateInContext(transaction, transactionalOperation);
		}
	}

	private <T> Optional<T> participateInContext(Transaction transaction,
			ReturningTransactionalOperation<T> transactionalOperation) {
		Deque<Transaction> transactionStack = transactionStackForPush();
		transactionStack.push(transaction);

		try {
			Optional<T> returnValue = transactionalOperation.perform();
			return returnValue == null ? Optional.empty() : returnValue;
		} catch (RuntimeException e) {
			setRollbackOnlyAfterParticipationFailure(transaction, e);
			Database.restoreInterruptIfNeeded(e);
			throw e;
		} catch (Error e) {
			setRollbackOnlyAfterParticipationFailure(transaction, e);
			Database.restoreInterruptIfNeeded(e);
			throw e;
		} catch (Throwable t) {
			RuntimeException wrapped = new RuntimeException(t);
			setRollbackOnlyAfterParticipationFailure(transaction, wrapped);
			Database.restoreInterruptIfNeeded(t);
			throw wrapped;
		} finally {
			try {
				transactionStack.pop();
			} finally {
				if (transactionStack.isEmpty())
					TRANSACTION_STACK_HOLDER.remove();
			}
		}
	}

	private void setRollbackOnlyAfterParticipationFailure(@NonNull Transaction transaction,
																												@NonNull Throwable primary) {
		requireNonNull(transaction);
		requireNonNull(primary);

		transaction.retrySafety().record(TransactionRetrySafety.outcomesOf(primary));

		try {
			transaction.setRollbackOnly(true);
		} catch (IllegalStateException e) {
			primary.addSuppressed(e);
		}
	}

}
