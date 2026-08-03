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
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

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
	/**
	 * Called immediately before Pyranid attempts to acquire a statement-scoped {@link java.sql.Connection}.
	 * <p>
	 * Statement-scoped connections are used for standalone statement execution and stream opening outside a physical
	 * {@link Transaction}.
	 *
	 * @param ctx statement context for the operation that needs a connection
	 */
	default void willAcquireStatementConnection(@NonNull StatementContext<?> ctx) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully acquires a statement-scoped {@link java.sql.Connection}.
	 *
	 * @param ctx                 statement context for the operation that acquired a connection
	 * @param acquisitionDuration elapsed time spent acquiring the connection
	 */
	default void didAcquireStatementConnection(@NonNull StatementContext<?> ctx,
																							 @NonNull Duration acquisitionDuration) {
		// No-op by default
	}

	/**
	 * Called after Pyranid fails to acquire a statement-scoped {@link java.sql.Connection}.
	 *
	 * @param ctx                 statement context for the operation that needed a connection
	 * @param databaseType        database type known at the time of failure
	 * @param acquisitionDuration elapsed time spent attempting to acquire the connection
	 * @param throwable           failure that prevented connection acquisition
	 */
	default void didFailToAcquireStatementConnection(@NonNull StatementContext<?> ctx,
																										 @NonNull DatabaseType databaseType,
																										 @NonNull Duration acquisitionDuration,
																										 @NonNull Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully releases a statement-scoped {@link java.sql.Connection}.
	 *
	 * @param ctx          statement context for the operation that used the connection
	 * @param heldDuration elapsed time between successful acquisition and release
	 */
	default void didReleaseStatementConnection(@NonNull StatementContext<?> ctx,
																							 @NonNull Duration heldDuration) {
		// No-op by default
	}

	/**
	 * Called after Pyranid fails to release a statement-scoped {@link java.sql.Connection}.
	 *
	 * @param ctx          statement context for the operation that used the connection
	 * @param heldDuration elapsed time between successful acquisition and the failed release attempt
	 * @param throwable    failure that prevented connection release
	 */
	default void didFailToReleaseStatementConnection(@NonNull StatementContext<?> ctx,
																										 @NonNull Duration heldDuration,
																										 @NonNull Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called immediately before Pyranid attempts to acquire the JDBC connection backing a physical transaction.
	 * <p>
	 * Pyranid transactions acquire their JDBC connection lazily, so this callback is emitted only when transaction work
	 * first needs database access.
	 *
	 * @param transaction  transaction that needs a physical connection
	 * @param databaseType database type known at the time of acquisition
	 */
	default void willAcquireTransactionConnection(@NonNull Transaction transaction,
																								 @NonNull DatabaseType databaseType) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully acquires the JDBC connection backing a physical transaction.
	 *
	 * @param transaction         transaction that acquired the connection
	 * @param databaseType        database type known at the time of acquisition
	 * @param acquisitionDuration elapsed time spent acquiring the connection
	 */
	default void didAcquireTransactionConnection(@NonNull Transaction transaction,
																								@NonNull DatabaseType databaseType,
																								@NonNull Duration acquisitionDuration) {
		// No-op by default
	}

	/**
	 * Called after Pyranid fails to acquire the JDBC connection backing a physical transaction.
	 *
	 * @param transaction         transaction that needed a connection
	 * @param databaseType        database type known at the time of failure
	 * @param acquisitionDuration elapsed time spent attempting to acquire the connection
	 * @param throwable           failure that prevented connection acquisition
	 */
	default void didFailToAcquireTransactionConnection(@NonNull Transaction transaction,
																												@NonNull DatabaseType databaseType,
																												@NonNull Duration acquisitionDuration,
																												@NonNull Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully releases the JDBC connection backing a physical transaction.
	 *
	 * @param transaction  transaction that owned the connection
	 * @param databaseType database type known at the time of release
	 * @param heldDuration elapsed time between successful acquisition and release
	 */
	default void didReleaseTransactionConnection(@NonNull Transaction transaction,
																								@NonNull DatabaseType databaseType,
																								@NonNull Duration heldDuration) {
		// No-op by default
	}

	/**
	 * Called after Pyranid fails to release the JDBC connection backing a physical transaction.
	 *
	 * @param transaction  transaction that owned the connection
	 * @param databaseType database type known at the time of failure
	 * @param heldDuration elapsed time between successful acquisition and the failed release attempt
	 * @param throwable    failure that prevented connection release
	 */
	default void didFailToReleaseTransactionConnection(@NonNull Transaction transaction,
																												@NonNull DatabaseType databaseType,
																												@NonNull Duration heldDuration,
																												@NonNull Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called when Pyranid enters a closure-based transaction.
	 * <p>
	 * This is a logical transaction event and may occur even if user code never performs database work and no physical JDBC
	 * transaction is begun.
	 *
	 * @param transaction  transaction passed to the closure through Pyranid APIs
	 * @param isolation    requested transaction isolation
	 * @param databaseType database type known at transaction entry
	 */
	default void didEnterTransactionClosure(@NonNull Transaction transaction,
																						@NonNull TransactionIsolation isolation,
																						@NonNull DatabaseType databaseType) {
		// No-op by default
	}

	/**
	 * Called when Pyranid exits a closure-based transaction.
	 *
	 * @param transaction     transaction that is exiting
	 * @param outcome         logical transaction outcome
	 * @param databaseType    database type known at transaction exit
	 * @param logicalDuration elapsed time from transaction entry to exit, including user code, physical transaction work,
	 *                        cleanup, and post-transaction operations
	 * @param thrown          exception or error that caused the transaction to exit abnormally, or {@code null} when the
	 *                        transaction completed without a reported failure
	 */
	default void didExitTransactionClosure(@NonNull Transaction transaction,
																					 @NonNull TransactionClosureOutcome outcome,
																					 @NonNull DatabaseType databaseType,
																				 @NonNull Duration logicalDuration,
																				 @Nullable Throwable thrown) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully begins a physical JDBC transaction.
	 *
	 * @param transaction  transaction whose physical JDBC transaction began
	 * @param isolation    requested transaction isolation
	 * @param databaseType database type known at transaction begin
	 */
	default void didBeginPhysicalTransaction(@NonNull Transaction transaction,
																						 @NonNull TransactionIsolation isolation,
																						 @NonNull DatabaseType databaseType) {
		// No-op by default
	}

	/**
	 * Called after Pyranid fails while beginning a physical JDBC transaction.
	 *
	 * @param transaction  transaction whose physical JDBC transaction failed to begin
	 * @param isolation    requested transaction isolation
	 * @param phase        begin phase that failed
	 * @param databaseType database type known at the time of failure
	 * @param throwable    failure that prevented the physical transaction from beginning
	 */
	default void didFailToBeginPhysicalTransaction(@NonNull Transaction transaction,
																									@NonNull TransactionIsolation isolation,
																									@NonNull PhysicalTransactionBeginFailurePhase phase,
																								@NonNull DatabaseType databaseType,
																								@NonNull Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully commits a physical JDBC transaction.
	 *
	 * @param transaction      transaction that committed
	 * @param databaseType     database type known at commit time
	 * @param physicalDuration elapsed time between physical transaction begin and commit
	 */
	default void didCommitPhysicalTransaction(@NonNull Transaction transaction,
																							@NonNull DatabaseType databaseType,
																							@NonNull Duration physicalDuration) {
		// No-op by default
	}

	/**
	 * Called after Pyranid fails to commit a physical JDBC transaction.
	 *
	 * @param transaction      transaction whose commit failed
	 * @param databaseType     database type known at commit time
	 * @param physicalDuration elapsed time between physical transaction begin and the failed commit attempt
	 * @param throwable        failure that prevented commit from completing normally
	 */
	default void didFailToCommitPhysicalTransaction(@NonNull Transaction transaction,
																									 @NonNull DatabaseType databaseType,
																									 @NonNull Duration physicalDuration,
																								 @NonNull Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully rolls back a physical JDBC transaction.
	 *
	 * @param transaction      transaction that rolled back
	 * @param databaseType     database type known at rollback time
	 * @param physicalDuration elapsed time between physical transaction begin and rollback
	 */
	default void didRollbackPhysicalTransaction(@NonNull Transaction transaction,
																							 @NonNull DatabaseType databaseType,
																							 @NonNull Duration physicalDuration) {
		// No-op by default
	}

	/**
	 * Called after Pyranid fails to roll back a physical JDBC transaction.
	 *
	 * @param transaction      transaction whose rollback failed
	 * @param databaseType     database type known at rollback time
	 * @param physicalDuration elapsed time between physical transaction begin and the failed rollback attempt
	 * @param throwable        failure that prevented rollback from completing normally
	 */
	default void didFailToRollbackPhysicalTransaction(@NonNull Transaction transaction,
																											@NonNull DatabaseType databaseType,
																											@NonNull Duration physicalDuration,
																										@NonNull Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called after a post-transaction operation runs.
	 *
	 * @param transaction  transaction whose post-transaction operation ran
	 * @param result       transaction result visible to the post-transaction operation
	 * @param databaseType database type known at post-transaction execution time
	 * @param duration     elapsed time spent running the post-transaction operation
	 * @param throwable    failure thrown by the post-transaction operation, or {@code null} when it completed normally
	 */
	default void didRunPostTransactionOperation(@NonNull Transaction transaction,
																							 @NonNull TransactionResult result,
																							 @NonNull DatabaseType databaseType,
																						 @NonNull Duration duration,
																						 @Nullable Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully creates a transaction savepoint.
	 *
	 * @param transaction  transaction that created the savepoint
	 * @param databaseType database type known at savepoint creation time
	 */
	default void didCreateSavepoint(@NonNull Transaction transaction,
																		@NonNull DatabaseType databaseType) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully rolls back to a transaction savepoint.
	 *
	 * @param transaction  transaction that rolled back to the savepoint
	 * @param databaseType database type known at savepoint rollback time
	 */
	default void didRollbackToSavepoint(@NonNull Transaction transaction,
																				@NonNull DatabaseType databaseType) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully releases a transaction savepoint.
	 *
	 * @param transaction  transaction that released the savepoint
	 * @param databaseType database type known at savepoint release time
	 */
	default void didReleaseSavepoint(@NonNull Transaction transaction,
																		 @NonNull DatabaseType databaseType) {
		// No-op by default
	}

	/**
	 * Called immediately before Pyranid executes a statement.
	 *
	 * @param ctx statement context for the statement about to execute
	 */
	default void willExecuteStatement(@NonNull StatementContext<?> ctx) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully executes a statement.
	 *
	 * @param ctx          statement context for the executed statement
	 * @param statementLog diagnostic statement log for the execution
	 * @param result       statement execution result
	 */
	default void didExecuteStatement(@NonNull StatementContext<?> ctx,
																		 @NonNull StatementLog<?> statementLog,
																		 @NonNull StatementResult result) {
		// No-op by default
	}

	/**
	 * Called after Pyranid fails to execute a statement.
	 *
	 * @param ctx          statement context for the failed statement
	 * @param statementLog diagnostic statement log for the failed execution
	 * @param databaseType database type known at statement failure time
	 * @param throwable    failure thrown while executing the statement
	 */
	default void didFailToExecuteStatement(@NonNull StatementContext<?> ctx,
																					 @NonNull StatementLog<?> statementLog,
																					 @NonNull DatabaseType databaseType,
																				 @NonNull Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called immediately before Pyranid opens a streaming statement.
	 *
	 * @param ctx statement context for the stream about to open
	 */
	default void willOpenStream(@NonNull StatementContext<?> ctx) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully opens a streaming statement.
	 *
	 * @param ctx          statement context for the opened stream
	 * @param openDuration elapsed time spent opening the stream
	 */
	default void didOpenStream(@NonNull StatementContext<?> ctx,
															 @NonNull Duration openDuration) {
		// No-op by default
	}

	/**
	 * Called after Pyranid fails to open a streaming statement.
	 *
	 * @param ctx          statement context for the stream that failed to open
	 * @param databaseType database type known at stream-open failure time
	 * @param openDuration elapsed time spent attempting to open the stream
	 * @param throwable    failure that prevented the stream from opening
	 */
	default void didFailToOpenStream(@NonNull StatementContext<?> ctx,
																		 @NonNull DatabaseType databaseType,
																		 @NonNull Duration openDuration,
																		 @NonNull Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called after an opened stream reaches a terminal state.
	 * <p>
	 * Pyranid does not emit this callback for streams that fail before they open; those failures are reported through
	 * {@link #didFailToOpenStream(StatementContext, DatabaseType, Duration, Throwable)}.
	 *
	 * @param ctx            statement context for the stream
	 * @param outcome        terminal stream outcome
	 * @param rowsConsumed   number of rows consumed from the stream
	 * @param streamDuration elapsed time between stream-open start and terminal close handling
	 * @param throwable      failure associated with the terminal outcome, or {@code null} when no failure is available
	 */
	default void didCloseStream(@NonNull StatementContext<?> ctx,
															@NonNull StreamTerminalOutcome outcome,
															@NonNull Long rowsConsumed,
															@NonNull Duration streamDuration,
															@Nullable Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called immediately before Pyranid attempts to open a notification session.
	 * <p>
	 * Argument validation, ambient-transaction rejection, database-type resolution, and backend-specific channel
	 * validation occur before this callback. Exactly one of
	 * {@link #didOpenNotificationSession(DatabaseType, UUID, Duration)} or
	 * {@link #didFailToOpenNotificationSession(DatabaseType, UUID, Duration, Throwable)} follows it.
	 *
	 * @param databaseType         database type resolved for the notification session
	 * @param notificationSessionId identifier for this notification-session invocation
	 * @since 4.6.0
	 */
	default void willOpenNotificationSession(@NonNull DatabaseType databaseType,
																					@NonNull UUID notificationSessionId) {
		// No-op by default
	}

	/**
	 * Called after Pyranid successfully opens a notification session and registers every requested channel.
	 * <p>
	 * This callback occurs before the application operation is dispatched. It does not indicate that application
	 * reconciliation or readiness work has completed.
	 *
	 * @param databaseType          database type resolved for the notification session
	 * @param notificationSessionId identifier for this notification-session invocation
	 * @param openDuration          elapsed time from the corresponding
	 *                              {@link #willOpenNotificationSession(DatabaseType, UUID)} callback through successful
	 *                              channel registration
	 * @since 4.6.0
	 */
	default void didOpenNotificationSession(@NonNull DatabaseType databaseType,
																				 @NonNull UUID notificationSessionId,
																				 @NonNull Duration openDuration) {
		// No-op by default
	}

	/**
	 * Called after Pyranid fails to open a notification session.
	 * <p>
	 * This is the terminal lifecycle callback for an invocation that never reached
	 * {@link #didOpenNotificationSession(DatabaseType, UUID, Duration)}. It is not followed by
	 * {@link #didCloseNotificationSession(DatabaseType, UUID, NotificationSessionOutcome, Duration, Throwable)}.
	 * Pyranid emits it after selecting the opening failure and before cleaning up any acquired candidate connection.
	 *
	 * @param databaseType          database type resolved for the notification session
	 * @param notificationSessionId identifier for this notification-session invocation
	 * @param openDuration          elapsed time from the corresponding
	 *                              {@link #willOpenNotificationSession(DatabaseType, UUID)} callback through the opening
	 *                              failure
	 * @param throwable             failure that prevented the notification session from opening
	 * @since 4.6.0
	 */
	default void didFailToOpenNotificationSession(@NonNull DatabaseType databaseType,
																					 @NonNull UUID notificationSessionId,
																					 @NonNull Duration openDuration,
																					 @NonNull Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called when Pyranid delivers a nonempty notification batch to application code.
	 * <p>
	 * Empty receive results and notifications discarded during setup or cleanup are not reported.
	 *
	 * @param databaseType          database type resolved for the notification session
	 * @param notificationSessionId identifier for the notification session that delivered the batch
	 * @param notificationCount     number of notifications in the delivered batch
	 * @since 4.6.0
	 */
	default void didDeliverNotificationBatch(@NonNull DatabaseType databaseType,
																					@NonNull UUID notificationSessionId,
																					@NonNull Long notificationCount) {
		// No-op by default
	}

	/**
	 * Called when a receive operation detects terminal uncertainty or loss of an opened notification connection.
	 * <p>
	 * This callback is emitted at most once for a notification session. It does not report setup failures, failures
	 * from ordinary database operations performed by application code, or cleanup-only failures.
	 *
	 * @param databaseType          database type resolved for the notification session
	 * @param notificationSessionId identifier for the notification session whose connection was lost
	 * @param throwable             terminal connection failure
	 * @since 4.6.0
	 */
	default void didLoseNotificationConnection(@NonNull DatabaseType databaseType,
																						@NonNull UUID notificationSessionId,
																						@NonNull Throwable throwable) {
		// No-op by default
	}

	/**
	 * Called after an opened notification session reaches a terminal state and cleanup completes.
	 * <p>
	 * Every invocation that emits {@link #didOpenNotificationSession(DatabaseType, UUID, Duration)} emits this callback
	 * exactly once. Invocations that fail before opening terminate through
	 * {@link #didFailToOpenNotificationSession(DatabaseType, UUID, Duration, Throwable)} instead.
	 *
	 * @param databaseType          database type resolved for the notification session
	 * @param notificationSessionId identifier for the notification session
	 * @param outcome               terminal notification-session outcome
	 * @param sessionDuration       elapsed time from the corresponding
	 *                              {@link #willOpenNotificationSession(DatabaseType, UUID)} callback through completed
	 *                              cleanup
	 * @param throwable             selected failure for {@link NotificationSessionOutcome#FAILED}, or {@code null} for
	 *                              {@link NotificationSessionOutcome#CALLBACK_RETURNED} and
	 *                              {@link NotificationSessionOutcome#INTERRUPTED}
	 * @since 4.6.0
	 */
	default void didCloseNotificationSession(@NonNull DatabaseType databaseType,
																					 @NonNull UUID notificationSessionId,
																					 @NonNull NotificationSessionOutcome outcome,
																					 @NonNull Duration sessionDuration,
																					 @Nullable Throwable throwable) {
		// No-op by default
	}

	/**
	 * Returns a notification counter snapshot if this collector supports in-process inspection.
	 * <p>
	 * Implementations may read counters independently without cross-counter atomicity. Callers that need
	 * invariant-consistent snapshots should drain in-flight work before reading.
	 *
	 * @return a notification metrics snapshot, if supported
	 * @since 4.6.0
	 */
	@NonNull
	default Optional<NotificationSnapshot> notificationSnapshot() {
		return Optional.empty();
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

	/**
	 * Returns the shared no-op metrics collector.
	 *
	 * @return no-op metrics collector used when metrics collection is disabled
	 */
	@NonNull
	static MetricsCollector disabledInstance() {
		return DisabledMetricsCollector.defaultInstance();
	}

	/**
	 * Creates a fresh in-memory counter collector.
	 * <p>
	 * This collector is intended for tests and lightweight local inspection through {@link #snapshot()} and
	 * {@link #notificationSnapshot()}.
	 *
	 * @return new in-memory metrics collector
	 */
	@NonNull
	static MetricsCollector inMemoryInstance() {
		return InMemoryMetricsCollector.defaultInstance();
	}

	/**
	 * Terminal outcome for an opened notification session.
	 *
	 * @since 4.6.0
	 */
	enum NotificationSessionOutcome {
		/**
		 * The application callback returned and notification-session cleanup completed normally.
		 */
		CALLBACK_RETURNED,

		/**
		 * Cooperative interruption ended the notification session and cleanup completed normally.
		 */
		INTERRUPTED,

		/**
		 * Setup after opening, notification receive, application callback, or cleanup failed.
		 */
		FAILED
	}

	/**
	 * Counter snapshot for notification metrics.
	 * <p>
	 * Implementations may read counters independently without cross-counter atomicity. Callers that need
	 * invariant-consistent snapshots should drain in-flight work before reading.
	 * <p>
	 * Instances are immutable value objects created through {@link #of(Long, Long, Long, Long, Long, Long, Long)}.
	 * Future releases may add counters and accessors while retaining this factory with defined defaults for any
	 * newly added values.
	 *
	 * @since 4.6.0
	 */
	@ThreadSafe
	final class NotificationSnapshot {
		@NonNull
		private final Long sessionsStarted;
		@NonNull
		private final Long sessionsOpened;
		@NonNull
		private final Long sessionsCallbackReturned;
		@NonNull
		private final Long sessionsInterrupted;
		@NonNull
		private final Long sessionsFailed;
		@NonNull
		private final Long batchesDelivered;
		@NonNull
		private final Long notificationsDelivered;

		private NotificationSnapshot(@NonNull Long sessionsStarted,
														 @NonNull Long sessionsOpened,
														 @NonNull Long sessionsCallbackReturned,
														 @NonNull Long sessionsInterrupted,
														 @NonNull Long sessionsFailed,
														 @NonNull Long batchesDelivered,
														 @NonNull Long notificationsDelivered) {
			this.sessionsStarted = requireNonNull(sessionsStarted);
			this.sessionsOpened = requireNonNull(sessionsOpened);
			this.sessionsCallbackReturned = requireNonNull(sessionsCallbackReturned);
			this.sessionsInterrupted = requireNonNull(sessionsInterrupted);
			this.sessionsFailed = requireNonNull(sessionsFailed);
			this.batchesDelivered = requireNonNull(batchesDelivered);
			this.notificationsDelivered = requireNonNull(notificationsDelivered);
		}

		/**
		 * Creates a notification metrics snapshot.
		 *
		 * @param sessionsStarted          valid, type-resolved notification-session invocations that entered measured setup
		 * @param sessionsOpened           notification sessions that completed channel registration
		 * @param sessionsCallbackReturned opened notification sessions whose callbacks returned and cleanup completed normally
		 * @param sessionsInterrupted      opened notification sessions that ended through clean cooperative interruption
		 * @param sessionsFailed           notification-session invocations that failed before opening or after opening
		 * @param batchesDelivered         nonempty notification batches delivered to application code
		 * @param notificationsDelivered   notifications contained in delivered batches
		 * @return notification metrics snapshot
		 * @throws NullPointerException if any counter is null
		 * @since 4.6.0
		 */
		@NonNull
		public static NotificationSnapshot of(@NonNull Long sessionsStarted,
																							 @NonNull Long sessionsOpened,
																							 @NonNull Long sessionsCallbackReturned,
																							 @NonNull Long sessionsInterrupted,
																							 @NonNull Long sessionsFailed,
																							 @NonNull Long batchesDelivered,
																							 @NonNull Long notificationsDelivered) {
			return new NotificationSnapshot(sessionsStarted, sessionsOpened, sessionsCallbackReturned, sessionsInterrupted,
					sessionsFailed, batchesDelivered, notificationsDelivered);
		}

		/**
		 * @return valid, type-resolved notification-session invocations that entered measured setup
		 * @since 4.6.0
		 */
		@NonNull
		public Long sessionsStarted() {
			return this.sessionsStarted;
		}

		/**
		 * @return notification sessions that completed channel registration
		 * @since 4.6.0
		 */
		@NonNull
		public Long sessionsOpened() {
			return this.sessionsOpened;
		}

		/**
		 * @return opened notification sessions whose callbacks returned and cleanup completed normally
		 * @since 4.6.0
		 */
		@NonNull
		public Long sessionsCallbackReturned() {
			return this.sessionsCallbackReturned;
		}

		/**
		 * @return opened notification sessions that ended through clean cooperative interruption
		 * @since 4.6.0
		 */
		@NonNull
		public Long sessionsInterrupted() {
			return this.sessionsInterrupted;
		}

		/**
		 * @return notification-session invocations that failed before opening or after opening
		 * @since 4.6.0
		 */
		@NonNull
		public Long sessionsFailed() {
			return this.sessionsFailed;
		}

		/**
		 * @return nonempty notification batches delivered to application code
		 * @since 4.6.0
		 */
		@NonNull
		public Long batchesDelivered() {
			return this.batchesDelivered;
		}

		/**
		 * @return notifications contained in delivered batches
		 * @since 4.6.0
		 */
		@NonNull
		public Long notificationsDelivered() {
			return this.notificationsDelivered;
		}

		@Override
		public boolean equals(@Nullable Object object) {
			if (this == object)
				return true;

			if (!(object instanceof NotificationSnapshot notificationSnapshot))
				return false;

			return sessionsStarted().equals(notificationSnapshot.sessionsStarted())
					&& sessionsOpened().equals(notificationSnapshot.sessionsOpened())
					&& sessionsCallbackReturned().equals(notificationSnapshot.sessionsCallbackReturned())
					&& sessionsInterrupted().equals(notificationSnapshot.sessionsInterrupted())
					&& sessionsFailed().equals(notificationSnapshot.sessionsFailed())
					&& batchesDelivered().equals(notificationSnapshot.batchesDelivered())
					&& notificationsDelivered().equals(notificationSnapshot.notificationsDelivered());
		}

		@Override
		public int hashCode() {
			return Objects.hash(sessionsStarted(), sessionsOpened(), sessionsCallbackReturned(), sessionsInterrupted(),
					sessionsFailed(), batchesDelivered(), notificationsDelivered());
		}

		@Override
		@NonNull
		public String toString() {
			return "NotificationSnapshot[" +
					"sessionsStarted=" + sessionsStarted() +
					", sessionsOpened=" + sessionsOpened() +
					", sessionsCallbackReturned=" + sessionsCallbackReturned() +
					", sessionsInterrupted=" + sessionsInterrupted() +
					", sessionsFailed=" + sessionsFailed() +
					", batchesDelivered=" + batchesDelivered() +
					", notificationsDelivered=" + notificationsDelivered() +
					']';
		}
	}

	/**
	 * Logical outcome for a closure-based transaction.
	 */
	enum TransactionClosureOutcome {
		/**
		 * The transaction used a physical JDBC transaction and commit completed normally.
		 */
		COMMITTED,

		/**
		 * The transaction used a physical JDBC transaction and was rolled back, including a recognized commit-time
		 * serialization failure whose follow-up rollback completed normally.
		 */
		ROLLED_BACK,

		/**
		 * The transaction closure exited without ever acquiring a JDBC connection.
		 */
		NO_PHYSICAL_TX,

		/**
		 * The transaction failed before Pyranid could report a normal commit or rollback outcome.
		 */
		FAILED
	}

	/**
	 * Physical transaction begin phase that failed.
	 */
	enum PhysicalTransactionBeginFailurePhase {
		/**
		 * Connection acquisition from the configured {@link javax.sql.DataSource} failed.
		 */
		ACQUIRE_CONNECTION,

		/**
		 * Reading the connection's initial autocommit setting failed.
		 */
		READ_INITIAL_AUTOCOMMIT,

		/**
		 * Reading the connection's initial transaction isolation failed.
		 */
		READ_INITIAL_ISOLATION,

		/**
		 * Reading the connection's initial read-only setting failed.
		 */
		READ_INITIAL_READ_ONLY,

		/**
		 * Disabling autocommit for the transaction failed.
		 */
		SET_AUTOCOMMIT_FALSE,

		/**
		 * Applying the requested transaction isolation failed.
		 */
		SET_ISOLATION,

		/**
		 * Applying the requested read-only setting failed.
		 */
		SET_READ_ONLY
	}

	/**
	 * Terminal outcome for an opened stream.
	 */
	enum StreamTerminalOutcome {
		/**
		 * Stream iteration reached the end of the result set and cleanup completed without an iteration or callback failure.
		 */
		COMPLETED_NORMALLY,

		/**
		 * The stream was closed before all rows were consumed.
		 */
		EARLY_CLOSE,

		/**
		 * The caller-provided stream callback failed.
		 */
		CALLBACK_FAILURE,

		/**
		 * Result-set iteration failed while the stream was open.
		 */
		ITERATION_FAILURE,

		/**
		 * The stream failed before it opened.
		 * <p>
		 * Pyranid normally reports this through {@link #didFailToOpenStream(StatementContext, DatabaseType, Duration,
		 * Throwable)} instead of {@link #didCloseStream(StatementContext, StreamTerminalOutcome, Long, Duration, Throwable)}.
		 */
		OPEN_FAILURE
	}

	/**
	 * Counter snapshot for collectors that support in-process inspection.
	 * <p>
	 * Implementations may read counters independently without cross-counter atomicity. Callers that need invariant-consistent
	 * snapshots should drain in-flight work before reading.
	 *
	 * @param connectionsAcquiredStatementScope          statement-scoped connection acquisitions that completed normally
	 * @param connectionsAcquiredTransactionScope        transaction-scoped connection acquisitions that completed normally
	 * @param connectionsFailedStatementScope            statement-scoped connection acquisitions that failed
	 * @param connectionsFailedTransactionScope          transaction-scoped connection acquisitions that failed
	 * @param connectionReleaseFailuresStatementScope    statement-scoped connection releases that failed
	 * @param connectionReleaseFailuresTransactionScope  transaction-scoped connection releases that failed
	 * @param transactionClosuresEntered                 closure-based transactions entered
	 * @param transactionClosuresExited                  closure-based transactions exited
	 * @param transactionClosuresCommitted               closure-based transactions that committed
	 * @param transactionClosuresRolledBack              closure-based transactions that rolled back
	 * @param transactionClosuresNoPhysical              closure-based transactions that exited without a physical JDBC transaction
	 * @param transactionClosuresFailed                  closure-based transactions that failed before a normal commit or rollback outcome
	 * @param physicalTransactionsBegun                  physical JDBC transactions begun
	 * @param physicalTransactionsBeginFailed            physical JDBC transactions that failed while beginning
	 * @param physicalTransactionsCommitted              physical JDBC transactions committed
	 * @param physicalTransactionsCommitFailed           physical JDBC transactions whose commit failed
	 * @param physicalTransactionsRolledBack             physical JDBC transactions rolled back
	 * @param physicalTransactionsRollbackFailed         physical JDBC transactions whose rollback failed
	 * @param savepointsCreated                          transaction savepoints created
	 * @param savepointsRolledBack                       transaction savepoints rolled back to
	 * @param savepointsReleased                         transaction savepoints released
	 * @param statementsExecuted                         statements that executed successfully
	 * @param statementsFailed                           statements that failed during execution
	 * @param streamsOpened                              streams that opened successfully
	 * @param streamsOpenFailures                        streams that failed before opening
	 * @param streamsClosedNormally                      opened streams that reached the end of the result set
	 * @param streamsEarlyClosed                         opened streams that closed before all rows were consumed
	 * @param streamsCallbackFailed                      opened streams whose caller-provided callback failed
	 * @param streamsIterationFailed                     opened streams whose result-set iteration failed
	 * @param postTransactionOperationsRun               post-transaction operations that ran
	 * @param postTransactionOperationsFailed            post-transaction operations that failed
	 */
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
