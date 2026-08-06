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

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;

/**
 * A callback-scoped session for receiving transient database notifications.
 * <p>
 * Instances are supplied only to {@link NotificationSessionOperation} by
 * {@link Database#withNotificationSession(String, NotificationSessionOperation)} or
 * {@link Database#withNotificationSession(java.util.Set, NotificationSessionOperation)}. A session is confined to
 * the callback thread, expires when that callback exits, and never reconnects. It exposes no lifecycle snapshot:
 * an apparently quiet session is not proof that its physical connection remains healthy.
 * <p>
 * Notification delivery is lossy and non-durable. Treat each returned batch as a hint to reconcile authoritative
 * state rather than as an event count or work queue.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@NotThreadSafe
public final class NotificationSession {
	private static final long MIN_RECEIVE_SLICE_NANOS = Duration.ofMillis(1).toNanos();
	private static final long MAX_RECEIVE_SLICE_NANOS = Duration.ofMillis(250).toNanos();

	@NonNull
	private final Database database;
	@NonNull
	private final NotificationTransport transport;
	@NonNull
	private final DatabaseType databaseType;
	@NonNull
	private final Thread ownerThread;
	@NonNull
	private final LongSupplier nanoTimeSupplier;
	@Nullable
	private final UUID notificationSessionId;
	@NonNull
	private State state;
	@Nullable
	private Throwable terminalFailure;
	private boolean receiving;
	private boolean connectionLossReported;

	NotificationSession(@NonNull Database database,
											@NonNull NotificationTransport transport,
											@NonNull DatabaseType databaseType,
											@Nullable UUID notificationSessionId) {
		this(database, transport, databaseType, notificationSessionId, System::nanoTime);
	}

	NotificationSession(@NonNull Database database,
											@NonNull NotificationTransport transport,
											@NonNull DatabaseType databaseType,
											@Nullable UUID notificationSessionId,
											@NonNull LongSupplier nanoTimeSupplier) {
		this.database = requireNonNull(database);
		this.transport = requireNonNull(transport);
		this.databaseType = requireNonNull(databaseType);
		this.ownerThread = Thread.currentThread();
		this.nanoTimeSupplier = requireNonNull(nanoTimeSupplier);
		this.notificationSessionId = notificationSessionId;
		this.state = State.ACTIVE;
	}

	/**
	 * Waits for a nonempty batch of notifications, until the best-effort elapsed-time budget expires.
	 * <p>
	 * Pyranid divides positive waits into driver calls of at most 250 milliseconds so interruption can normally be
	 * observed between calls. The budget is not a hard completion deadline: a JDBC driver call already in progress
	 * may overrun it. A zero duration has exactly the polling semantics of {@link #drainNotifications()}.
	 * <p>
	 * A nonempty batch wins over an interrupt that races after the driver returns; the batch is returned and the
	 * interrupt flag remains set for application code or the next receive to observe.
	 * If final reconciliation uses interrupt-sensitive work such as Pyranid transaction entry, clear and remember
	 * that flag with {@link Thread#interrupted()}, perform only bounded reconciliation, and restore the flag in a
	 * {@code finally} block.
	 *
	 * @param maxWait maximum best-effort elapsed time to wait, which must not be negative
	 * @return an immutable notification batch, empty only when the budget expires without an observed notification
	 * @throws NullPointerException if {@code maxWait} is null
	 * @throws IllegalArgumentException if {@code maxWait} is negative
	 * @throws IllegalStateException if the session is expired, failed, used from another thread, used reentrantly, or
	 *                               used while any Pyranid transaction is active on this thread
	 * @throws InterruptedException if interruption is observed before protocol work or after an empty receive
	 * @throws DatabaseException if notification transport fails
	 * @since 4.6.0
	 */
	@NonNull
	public List<@NonNull Notification> awaitNotifications(@NonNull Duration maxWait)
			throws InterruptedException {
		enterReceive();

		try {
			requireNonNull(maxWait);

			if (maxWait.isNegative())
				throw new IllegalArgumentException("maxWait must not be negative");

			rejectAmbientTransaction();

			if (maxWait.isZero())
				return drainActiveTransport();

			if (Thread.interrupted())
				throw new InterruptedException();

			long waitNanos = durationToNanosSaturated(maxWait);
			long startTime = this.nanoTimeSupplier.getAsLong();
			boolean firstSlice = true;

			for (;;) {
				long elapsedNanos = this.nanoTimeSupplier.getAsLong() - startTime;
				long remainingNanos = waitNanos - Math.max(0L, elapsedNanos);

				if (remainingNanos <= 0L
						&& !(firstSlice && waitNanos < MIN_RECEIVE_SLICE_NANOS)) {
					if (Thread.interrupted())
						throw new InterruptedException();

					return List.of();
				}

				long waitSliceNanos = Math.min(
						Math.max(remainingNanos, MIN_RECEIVE_SLICE_NANOS),
						MAX_RECEIVE_SLICE_NANOS);
				Duration waitSlice = Duration.ofNanos(waitSliceNanos);
				List<Notification> notifications = receiveFromTransport(waitSlice);
				firstSlice = false;

				if (!notifications.isEmpty())
					return delivered(notifications);

				if (Thread.interrupted())
					throw new InterruptedException();
			}
		} finally {
			exitReceive();
		}
	}

	/**
	 * Polls the existing listener connection once using the adapter's driver-specific non-waiting mode.
	 * <p>
	 * The method performs no acquisition, registration, sleep, reconnect, or reconciliation. A nonempty batch wins
	 * over an interrupt that races after the driver returns; the batch is returned and the interrupt flag remains set.
	 * If final reconciliation uses interrupt-sensitive work such as Pyranid transaction entry, clear and remember that
	 * flag with {@link Thread#interrupted()}, perform only bounded reconciliation, and restore the flag in a
	 * {@code finally} block.
	 *
	 * @return an immutable notification batch, possibly empty
	 * @throws IllegalStateException if the session is expired, failed, used from another thread, used reentrantly, or
	 *                               used while any Pyranid transaction is active on this thread
	 * @throws InterruptedException if interruption is observed before protocol work or after an empty receive
	 * @throws DatabaseException if notification transport fails
	 * @since 4.6.0
	 */
	@NonNull
	public List<@NonNull Notification> drainNotifications()
			throws InterruptedException {
		enterReceive();

		try {
			rejectAmbientTransaction();
			return drainActiveTransport();
		} finally {
			exitReceive();
		}
	}

	private void enterReceive() {
		if (Thread.currentThread() != this.ownerThread)
			throw new IllegalStateException("Notification session may only be used from its callback thread");

		if (this.state == State.EXPIRED)
			throw new IllegalStateException("Notification session has expired");

		if (this.state == State.FAILED)
			throw new IllegalStateException("Notification session has failed");

		if (this.receiving)
			throw new IllegalStateException("Notification session receive methods are not reentrant");

		this.receiving = true;
	}

	private void exitReceive() {
		this.receiving = false;
	}

	private void rejectAmbientTransaction() {
		if (Database.hasAmbientTransaction())
			throw new IllegalStateException("Notification receive is not permitted inside a Pyranid transaction");
	}

	@NonNull
	private List<@NonNull Notification> drainActiveTransport()
			throws InterruptedException {
		if (Thread.interrupted())
			throw new InterruptedException();

		List<Notification> notifications = drainTransport();

		if (!notifications.isEmpty())
			return delivered(notifications);

		if (Thread.interrupted())
			throw new InterruptedException();

		return List.of();
	}

	@NonNull
	private List<@NonNull Notification> receiveFromTransport(@NonNull Duration waitSlice) {
		requireNonNull(waitSlice);

		try {
			return immutableNotifications(this.transport.receive(waitSlice));
		} catch (SQLException | RuntimeException exception) {
			throw fail(exception);
		} catch (Error error) {
			throw failIfConnectionUncertain(error);
		}
	}

	@NonNull
	private List<@NonNull Notification> drainTransport() {
		try {
			return immutableNotifications(this.transport.drain());
		} catch (SQLException | RuntimeException exception) {
			throw fail(exception);
		} catch (Error error) {
			throw failIfConnectionUncertain(error);
		}
	}

	@NonNull
	private List<@NonNull Notification> immutableNotifications(@NonNull List<@NonNull Notification> notifications) {
		return List.copyOf(requireNonNull(notifications));
	}

	@NonNull
	private List<@NonNull Notification> delivered(@NonNull List<@NonNull Notification> notifications) {
		requireNonNull(notifications);

		if (this.notificationSessionId != null)
			this.database.getMetricsCollectorDispatcher().didDeliverNotificationBatch(
					this.databaseType, this.notificationSessionId, (long) notifications.size());

		return notifications;
	}

	@NonNull
	private DatabaseException fail(@NonNull Throwable cause) {
		requireNonNull(cause);

		DatabaseException databaseException = cause instanceof DatabaseException
				? (DatabaseException) cause
				: new DatabaseException(
						"Unable to receive database notifications", cause, this.databaseType.dialect());

		latchFailure(databaseException);
		return databaseException;
	}

	@NonNull
	private Error failIfConnectionUncertain(@NonNull Error error) {
		requireNonNull(error);

		if (this.transport.isConnectionUncertain())
			latchFailure(error);

		return error;
	}

	private void latchFailure(@NonNull Throwable failure) {
		requireNonNull(failure);

		this.state = State.FAILED;
		this.terminalFailure = failure;

		if (!this.connectionLossReported && this.notificationSessionId != null) {
			this.connectionLossReported = true;
			this.database.getMetricsCollectorDispatcher().didLoseNotificationConnection(
					this.databaseType, this.notificationSessionId, failure);
		}
	}

	@NonNull
	private static Long durationToNanosSaturated(@NonNull Duration duration) {
		requireNonNull(duration);

		try {
			return duration.toNanos();
		} catch (ArithmeticException ignored) {
			return Long.MAX_VALUE;
		}
	}

	void expire() {
		this.state = State.EXPIRED;
	}

	@Nullable
	Throwable terminalFailure() {
		return this.terminalFailure;
	}

	boolean isConnectionUncertain() {
		return this.terminalFailure != null || this.transport.isConnectionUncertain();
	}

	private enum State {
		ACTIVE,
		FAILED,
		EXPIRED
	}
}
