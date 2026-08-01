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

import org.hsqldb.jdbc.JDBCDataSource;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.concurrent.ThreadSafe;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@ThreadSafe
public class NotificationSessionTests {
	@NonNull
	private static final AtomicInteger DATABASE_ID = new AtomicInteger();

	@AfterEach
	public void clearInterruptStatus() {
		Thread.interrupted();
	}

	@Test
	public void constructorAndDurationValidation() throws InterruptedException {
		Database database = database();
		FakeNotificationTransport transport = new FakeNotificationTransport();

		Assertions.assertThrows(NullPointerException.class, () ->
				new NotificationSession(null, transport, DatabaseType.POSTGRESQL, null));
		Assertions.assertThrows(NullPointerException.class, () ->
				new NotificationSession(database, null, DatabaseType.POSTGRESQL, null));
		Assertions.assertThrows(NullPointerException.class, () ->
				new NotificationSession(database, transport, null, null));

		NotificationSession session = session(database, transport);

		Assertions.assertThrows(NullPointerException.class, () -> session.awaitNotifications(null));
		Assertions.assertThrows(IllegalArgumentException.class, () ->
				session.awaitNotifications(Duration.ofNanos(-1L)));
		Assertions.assertEquals(0, transport.receiveCalls.get());
		Assertions.assertEquals(0, transport.drainCalls.get());

		Assertions.assertEquals(List.of(), session.drainNotifications(),
				"Validation failures must not poison an active session");
	}

	@Test
	public void zeroWaitUsesDrainAndReturnsAnImmutableBatch() throws InterruptedException {
		FakeNotificationTransport transport = new FakeNotificationTransport();
		Notification expected = Notification.of("car_changed", null);
		List<Notification> transportBatch = new ArrayList<>(List.of(expected));
		transport.drainOperation = () -> transportBatch;
		transport.receiveOperation = waitSlice -> {
			throw new AssertionError("A zero wait must not use timed receive");
		};
		NotificationSession session = session(database(), transport);

		List<Notification> actual = session.awaitNotifications(Duration.ZERO);

		Assertions.assertEquals(List.of(expected), actual);
		Assertions.assertNull(actual.get(0).getPayload());
		Assertions.assertNotSame(transportBatch, actual);
		Assertions.assertEquals(1, transport.drainCalls.get());
		Assertions.assertEquals(0, transport.receiveCalls.get());
		Assertions.assertThrows(UnsupportedOperationException.class,
				() -> actual.add(Notification.of("other", "")));

		transportBatch.clear();
		Assertions.assertEquals(List.of(expected), actual,
				"Mutating a transport-owned list must not mutate the delivered batch");
	}

	@Test
	public void positiveWaitSlicesAreOneTo250MillisecondsAndStopAtFirstBatch() throws InterruptedException {
		Notification expected = Notification.of("car_changed", "42");
		List<Notification> transportBatch = new ArrayList<>(List.of(expected));
		FakeNotificationTransport upperBoundTransport = new FakeNotificationTransport();
		upperBoundTransport.receiveOperation = waitSlice ->
				upperBoundTransport.receiveCalls.get() < 3 ? List.of() : transportBatch;
		NotificationSession upperBoundSession = session(database(), upperBoundTransport);

		List<Notification> actual = upperBoundSession.awaitNotifications(Duration.ofSeconds(10L));

		Assertions.assertEquals(List.of(expected), actual);
		Assertions.assertEquals(3, upperBoundTransport.receiveCalls.get());
		Assertions.assertEquals(3, upperBoundTransport.receiveSlices.size());
		Assertions.assertTrue(upperBoundTransport.receiveSlices.stream().allMatch(waitSlice ->
				waitSlice.compareTo(Duration.ofMillis(1L)) >= 0
						&& waitSlice.compareTo(Duration.ofMillis(250L)) <= 0));
		Assertions.assertThrows(UnsupportedOperationException.class,
				() -> actual.add(Notification.of("other", "")));

		FakeNotificationTransport minimumTransport = new FakeNotificationTransport();
		minimumTransport.receiveOperation = waitSlice -> List.of(expected);
		NotificationSession minimumSession = session(database(), minimumTransport);

		Assertions.assertEquals(List.of(expected),
				minimumSession.awaitNotifications(Duration.ofNanos(1L)));
		Assertions.assertEquals(List.of(Duration.ofMillis(1L)), minimumTransport.receiveSlices,
				"A positive sub-millisecond budget must perform one one-millisecond transport receive");
	}

	@Test
	public void timedWaitRecomputesBudgetAndHandlesSaturationAndNanoTimeWrap()
			throws InterruptedException {
		AtomicLong clock = new AtomicLong();
		FakeNotificationTransport transport = new FakeNotificationTransport();
		transport.receiveOperation = waitSlice -> {
			clock.addAndGet(waitSlice.toNanos());
			return List.of();
		};
		NotificationSession session = new NotificationSession(
				database(), transport, DatabaseType.POSTGRESQL, null, clock::get);

		Assertions.assertEquals(
				List.of(), session.awaitNotifications(Duration.ofMillis(550)));
		Assertions.assertEquals(
				List.of(
						Duration.ofMillis(250),
						Duration.ofMillis(250),
						Duration.ofMillis(50)),
				transport.receiveSlices);

		FakeNotificationTransport saturatedTransport = new FakeNotificationTransport();
		Notification expected = Notification.of("car_changed", "saturated");
		saturatedTransport.receiveOperation = waitSlice -> List.of(expected);
		NotificationSession saturatedSession = new NotificationSession(
				database(), saturatedTransport, DatabaseType.POSTGRESQL, null, () -> 0L);

		Assertions.assertEquals(
				List.of(expected),
				saturatedSession.awaitNotifications(Duration.ofSeconds(Long.MAX_VALUE)));
		Assertions.assertEquals(
				List.of(Duration.ofMillis(250)), saturatedTransport.receiveSlices);

		AtomicLong wrappingClock = new AtomicLong(Long.MAX_VALUE - 10L);
		FakeNotificationTransport wrappingTransport = new FakeNotificationTransport();
		wrappingTransport.receiveOperation = waitSlice -> {
			wrappingClock.addAndGet(30L);
			return List.of();
		};
		NotificationSession wrappingSession = new NotificationSession(
				database(), wrappingTransport, DatabaseType.POSTGRESQL, null, wrappingClock::get);

		Assertions.assertEquals(
				List.of(), wrappingSession.awaitNotifications(Duration.ofNanos(25L)));
		Assertions.assertEquals(
				List.of(Duration.ofMillis(1)), wrappingTransport.receiveSlices);
	}

	@Test
	public void interruptionBeforeIoAndAfterEmptyResultsClearsTheFlag() throws InterruptedException {
		FakeNotificationTransport transport = new FakeNotificationTransport();
		NotificationSession session = session(database(), transport);

		Thread.currentThread().interrupt();
		Assertions.assertThrows(InterruptedException.class, () ->
				session.awaitNotifications(Duration.ofSeconds(1L)));
		Assertions.assertFalse(Thread.currentThread().isInterrupted());
		Assertions.assertEquals(0, transport.receiveCalls.get());

		Notification expected = Notification.of("car_changed", "42");
		transport.receiveOperation = waitSlice -> {
			if (transport.receiveCalls.get() == 1) {
				Thread.currentThread().interrupt();
				return List.of();
			}

			return List.of(expected);
		};

		Assertions.assertThrows(InterruptedException.class, () ->
				session.awaitNotifications(Duration.ofSeconds(1L)));
		Assertions.assertFalse(Thread.currentThread().isInterrupted());
		Assertions.assertEquals(1, transport.receiveCalls.get(),
				"An interrupt observed after an empty slice must prevent another transport receive");
		Assertions.assertEquals(List.of(expected),
				session.awaitNotifications(Duration.ofSeconds(1L)),
				"Observed interruption must not poison the session");

		FakeNotificationTransport drainTransport = new FakeNotificationTransport();
		drainTransport.drainOperation = () -> {
			Thread.currentThread().interrupt();
			return List.of();
		};
		NotificationSession drainSession = session(database(), drainTransport);

		Assertions.assertThrows(InterruptedException.class, drainSession::drainNotifications);
		Assertions.assertFalse(Thread.currentThread().isInterrupted());
		Assertions.assertEquals(1, drainTransport.drainCalls.get());
	}

	@Test
	public void nonemptyBatchWinsALateInterruptAndLeavesTheFlagSet() throws InterruptedException {
		FakeNotificationTransport transport = new FakeNotificationTransport();
		Notification expected = Notification.of("car_changed", "42");
		transport.receiveOperation = waitSlice -> {
			Thread.currentThread().interrupt();
			return List.of(expected);
		};
		NotificationSession session = session(database(), transport);

		Assertions.assertEquals(List.of(expected),
				session.awaitNotifications(Duration.ofSeconds(1L)));
		Assertions.assertTrue(Thread.currentThread().isInterrupted());
		Assertions.assertEquals(1, transport.receiveCalls.get());

		Assertions.assertThrows(InterruptedException.class, () ->
				session.awaitNotifications(Duration.ofSeconds(1L)));
		Assertions.assertFalse(Thread.currentThread().isInterrupted());
		Assertions.assertEquals(1, transport.receiveCalls.get(),
				"The next receive must consume the pending flag before transport I/O");
	}

	@Test
	public void ownerThreadAndExpiredStateAreEnforcedBeforeTransportAccess() throws InterruptedException {
		FakeNotificationTransport transport = new FakeNotificationTransport();
		NotificationSession session = session(database(), transport);
		AtomicReference<Throwable> wrongThreadFailure = new AtomicReference<>();
		Thread foreignThread = new Thread(() -> {
			try {
				session.drainNotifications();
			} catch (Throwable throwable) {
				wrongThreadFailure.set(throwable);
			}
		}, "notification-session-foreign-thread");

		foreignThread.start();
		foreignThread.join(TimeUnit.SECONDS.toMillis(5L));

		Assertions.assertFalse(foreignThread.isAlive());
		Assertions.assertInstanceOf(IllegalStateException.class, wrongThreadFailure.get());
		Assertions.assertEquals(0, transport.drainCalls.get());

		session.expire();

		Assertions.assertThrows(IllegalStateException.class, session::drainNotifications);
		Assertions.assertThrows(IllegalStateException.class, () ->
				session.awaitNotifications(Duration.ZERO));
		Assertions.assertEquals(0, transport.drainCalls.get());
		Assertions.assertEquals(0, transport.receiveCalls.get());
	}

	@Test
	public void receiveMethodsAreNotReentrant() throws InterruptedException {
		FakeNotificationTransport transport = new FakeNotificationTransport();
		AtomicReference<NotificationSession> sessionHolder = new AtomicReference<>();
		AtomicReference<IllegalStateException> reentrantFailure = new AtomicReference<>();
		transport.drainOperation = () -> {
			reentrantFailure.set(Assertions.assertThrows(IllegalStateException.class,
					sessionHolder.get()::drainNotifications));
			return List.of();
		};
		NotificationSession session = session(database(), transport);
		sessionHolder.set(session);

		Assertions.assertEquals(List.of(), session.drainNotifications());
		Assertions.assertNotNull(reentrantFailure.get());
		Assertions.assertEquals(1, transport.drainCalls.get(),
				"A rejected reentrant receive must not reach the transport");

		transport.drainOperation = List::of;
		Assertions.assertEquals(List.of(), session.drainNotifications(),
				"The outer receive must clear its reentrancy guard");
		Assertions.assertEquals(2, transport.drainCalls.get());
	}

	@Test
	public void anyAmbientTransactionIsRejectedBeforeTransportAccess() throws InterruptedException {
		Database owningDatabase = database();
		Database foreignDatabase = database();
		FakeNotificationTransport transport = new FakeNotificationTransport();
		NotificationSession session = session(owningDatabase, transport);

		Assertions.assertThrows(IllegalStateException.class, () ->
				owningDatabase.transaction(() -> {
					session.drainNotifications();
				}));
		Assertions.assertThrows(IllegalStateException.class, () ->
				foreignDatabase.transaction(() -> {
					session.drainNotifications();
				}));
		Assertions.assertEquals(0, transport.drainCalls.get());
		Assertions.assertEquals(0, transport.receiveCalls.get());
		Assertions.assertEquals(List.of(), session.drainNotifications(),
				"Ambient-transaction rejection must not poison the session");
	}

	@Test
	public void transportDatabaseExceptionIsLatchedExactlyForOuterAccess() {
		FakeNotificationTransport transport = new FakeNotificationTransport();
		DatabaseException expected = new DatabaseException("transport failed");
		transport.receiveOperation = waitSlice -> {
			Thread.currentThread().interrupt();
			throw expected;
		};
		NotificationSession session = session(database(), transport);

		Assertions.assertNull(session.terminalFailure());
		Assertions.assertFalse(session.isConnectionUncertain());
		transport.connectionUncertain = true;
		Assertions.assertTrue(session.isConnectionUncertain(),
				"Active-session uncertainty must delegate to the transport");
		transport.connectionUncertain = false;

		DatabaseException actual = Assertions.assertThrows(DatabaseException.class, () ->
				session.awaitNotifications(Duration.ofSeconds(1L)));

		Assertions.assertSame(expected, actual);
		Assertions.assertSame(expected, session.terminalFailure());
		Assertions.assertTrue(session.isConnectionUncertain());
		Assertions.assertTrue(Thread.currentThread().isInterrupted(),
				"A transport failure must win without clearing a concurrent interrupt");
		Thread.interrupted();

		Assertions.assertThrows(IllegalStateException.class, () ->
				session.awaitNotifications(Duration.ZERO));
		Assertions.assertThrows(IllegalStateException.class, session::drainNotifications);
		Assertions.assertEquals(1, transport.receiveCalls.get());
		Assertions.assertEquals(0, transport.drainCalls.get());

		session.expire();

		Assertions.assertSame(expected, session.terminalFailure());
		Assertions.assertTrue(session.isConnectionUncertain(),
				"Expiry must not erase the uncertainty needed by outer cleanup");
	}

	@Test
	public void uncertainTransportErrorIsLatchedExactlyAndReportedOnce() {
		RecordingMetricsCollector metricsCollector = new RecordingMetricsCollector();
		Database database = database(metricsCollector);
		FakeNotificationTransport transport = new FakeNotificationTransport();
		UUID notificationSessionId = UUID.randomUUID();
		AssertionError expected = new AssertionError("transport linkage failed");
		transport.receiveOperation = waitSlice -> {
			transport.connectionUncertain = true;
			throw expected;
		};
		NotificationSession session = new NotificationSession(
				database, transport, DatabaseType.POSTGRESQL, notificationSessionId);

		AssertionError actual = Assertions.assertThrows(AssertionError.class, () ->
				session.awaitNotifications(Duration.ofSeconds(1L)));

		Assertions.assertSame(expected, actual);
		Assertions.assertSame(expected, session.terminalFailure(),
				"The raw Error must remain visible to outer session outcome selection");
		Assertions.assertTrue(session.isConnectionUncertain());
		Assertions.assertEquals(1, metricsCollector.connectionLossCalls.get());
		Assertions.assertSame(expected, metricsCollector.connectionLossFailure.get());
		Assertions.assertEquals(DatabaseType.POSTGRESQL, metricsCollector.lossDatabaseType.get());
		Assertions.assertEquals(notificationSessionId, metricsCollector.lossSessionId.get());

		Assertions.assertThrows(IllegalStateException.class, session::drainNotifications);
		Assertions.assertThrows(IllegalStateException.class, () ->
				session.awaitNotifications(Duration.ZERO));
		Assertions.assertEquals(1, transport.receiveCalls.get());
		Assertions.assertEquals(0, transport.drainCalls.get());
		Assertions.assertEquals(1, metricsCollector.connectionLossCalls.get());

		session.expire();

		Assertions.assertSame(expected, session.terminalFailure());
		Assertions.assertTrue(session.isConnectionUncertain(),
				"Expiry must retain a swallowed raw Error for outer cleanup and outcome selection");
	}

	@Test
	public void uncertainDrainErrorAlsoTransitionsTheSessionToFailed() {
		FakeNotificationTransport transport = new FakeNotificationTransport();
		AssertionError expected = new AssertionError("drain transport failed");
		transport.drainOperation = () -> {
			transport.connectionUncertain = true;
			throw expected;
		};
		NotificationSession session = session(database(), transport);

		AssertionError actual = Assertions.assertThrows(
				AssertionError.class, session::drainNotifications);

		Assertions.assertSame(expected, actual);
		Assertions.assertSame(expected, session.terminalFailure());
		Assertions.assertTrue(session.isConnectionUncertain());
		Assertions.assertThrows(IllegalStateException.class, () ->
				session.awaitNotifications(Duration.ofSeconds(1L)));
		Assertions.assertEquals(1, transport.drainCalls.get());
		Assertions.assertEquals(0, transport.receiveCalls.get());
	}

	@Test
	public void unrelatedBatchCopyErrorDoesNotPoisonACertainTransport() throws InterruptedException {
		RecordingMetricsCollector metricsCollector = new RecordingMetricsCollector();
		FakeNotificationTransport transport = new FakeNotificationTransport();
		AssertionError expected = new AssertionError("application-side batch copy failed");
		List<Notification> uncopyableBatch = new java.util.AbstractList<>() {
			@Override
			public Notification get(int index) {
				throw expected;
			}

			@Override
			public int size() {
				return 1;
			}
		};
		transport.receiveOperation = waitSlice -> uncopyableBatch;
		NotificationSession session = new NotificationSession(
				database(metricsCollector), transport, DatabaseType.POSTGRESQL, UUID.randomUUID());

		AssertionError actual = Assertions.assertThrows(AssertionError.class, () ->
				session.awaitNotifications(Duration.ofSeconds(1L)));

		Assertions.assertSame(expected, actual);
		Assertions.assertNull(session.terminalFailure());
		Assertions.assertFalse(session.isConnectionUncertain());
		Assertions.assertEquals(0, metricsCollector.connectionLossCalls.get());

		transport.receiveOperation = waitSlice -> List.of(Notification.of("car_changed", "42"));
		Assertions.assertEquals(List.of(Notification.of("car_changed", "42")),
				session.awaitNotifications(Duration.ofSeconds(1L)),
				"An Error outside an uncertain transport must not transition the session to FAILED");
	}

	@Test
	public void nonemptyDeliveryAndConnectionLossEmitMetrics() throws InterruptedException {
		RecordingMetricsCollector metricsCollector = new RecordingMetricsCollector();
		Database database = database(metricsCollector);
		FakeNotificationTransport transport = new FakeNotificationTransport();
		UUID notificationSessionId = UUID.randomUUID();
		Notification first = Notification.of("car_changed", "42");
		Notification second = Notification.of("car_changed", "43");
		SQLException receiveFailure = new SQLException("connection lost", "08006");
		transport.drainOperation = () -> {
			return switch (transport.drainCalls.get()) {
				case 1 -> List.of();
				case 2 -> List.of(first, second);
				default -> throw receiveFailure;
			};
		};
		NotificationSession session = new NotificationSession(
				database, transport, DatabaseType.POSTGRESQL, notificationSessionId);

		Assertions.assertEquals(List.of(), session.drainNotifications());
		Assertions.assertEquals(List.of(first, second), session.drainNotifications());
		Assertions.assertEquals(1, metricsCollector.deliveryCalls.get());
		Assertions.assertEquals(2L, metricsCollector.notificationsDelivered.get());
		Assertions.assertEquals(DatabaseType.POSTGRESQL, metricsCollector.deliveryDatabaseType.get());
		Assertions.assertEquals(notificationSessionId, metricsCollector.deliverySessionId.get());

		DatabaseException terminalFailure = Assertions.assertThrows(
				DatabaseException.class, session::drainNotifications);

		Assertions.assertSame(receiveFailure, terminalFailure.getCause());
		Assertions.assertSame(terminalFailure, session.terminalFailure());
		Assertions.assertEquals(1, metricsCollector.connectionLossCalls.get());
		Assertions.assertSame(terminalFailure, metricsCollector.connectionLossFailure.get());
		Assertions.assertEquals(DatabaseType.POSTGRESQL, metricsCollector.lossDatabaseType.get());
		Assertions.assertEquals(notificationSessionId, metricsCollector.lossSessionId.get());

		Assertions.assertThrows(IllegalStateException.class, session::drainNotifications);
		Assertions.assertEquals(1, metricsCollector.deliveryCalls.get());
		Assertions.assertEquals(1, metricsCollector.connectionLossCalls.get());
	}

	@NonNull
	private static NotificationSession session(@NonNull Database database,
			@NonNull FakeNotificationTransport transport) {
		return new NotificationSession(
				requireNonNull(database), requireNonNull(transport), DatabaseType.POSTGRESQL, null);
	}

	@NonNull
	private static Database database() {
		return database(MetricsCollector.disabledInstance());
	}

	@NonNull
	private static Database database(@NonNull MetricsCollector metricsCollector) {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl("jdbc:hsqldb:mem:notification_session_" + DATABASE_ID.incrementAndGet());
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return Database.withDataSource(dataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.metricsCollector(requireNonNull(metricsCollector))
				.build();
	}

	@FunctionalInterface
	private interface ReceiveOperation {
		@NonNull
		List<@NonNull Notification> receive(@NonNull Duration waitSlice) throws SQLException;
	}

	@FunctionalInterface
	private interface DrainOperation {
		@NonNull
		List<@NonNull Notification> drain() throws SQLException;
	}

	private static final class FakeNotificationTransport implements NotificationTransport {
		@NonNull
		private final AtomicInteger receiveCalls;
		@NonNull
		private final AtomicInteger drainCalls;
		@NonNull
		private final List<Duration> receiveSlices;
		@NonNull
		private ReceiveOperation receiveOperation;
		@NonNull
		private DrainOperation drainOperation;
		private boolean connectionUncertain;

		private FakeNotificationTransport() {
			this.receiveCalls = new AtomicInteger();
			this.drainCalls = new AtomicInteger();
			this.receiveSlices = new ArrayList<>();
			this.receiveOperation = waitSlice -> List.of();
			this.drainOperation = List::of;
		}

		@Override
		public void listen(@NonNull Set<@NonNull String> channels) {
			// Not used by NotificationSession.
		}

		@Override
		public void unlistenAll() {
			// Not used by NotificationSession.
		}

		@NonNull
		@Override
		public List<@NonNull Notification> receive(@NonNull Duration waitSlice) throws SQLException {
			this.receiveCalls.incrementAndGet();
			this.receiveSlices.add(requireNonNull(waitSlice));
			return this.receiveOperation.receive(waitSlice);
		}

		@NonNull
		@Override
		public List<@NonNull Notification> drain() throws SQLException {
			this.drainCalls.incrementAndGet();
			return this.drainOperation.drain();
		}

		@Override
		public boolean isConnectionUncertain() {
			return this.connectionUncertain;
		}
	}

	@ThreadSafe
	private static final class RecordingMetricsCollector implements MetricsCollector {
		@NonNull
		private final AtomicInteger deliveryCalls;
		@NonNull
		private final AtomicLong notificationsDelivered;
		@NonNull
		private final AtomicReference<DatabaseType> deliveryDatabaseType;
		@NonNull
		private final AtomicReference<UUID> deliverySessionId;
		@NonNull
		private final AtomicInteger connectionLossCalls;
		@NonNull
		private final AtomicReference<DatabaseType> lossDatabaseType;
		@NonNull
		private final AtomicReference<UUID> lossSessionId;
		@NonNull
		private final AtomicReference<Throwable> connectionLossFailure;

		private RecordingMetricsCollector() {
			this.deliveryCalls = new AtomicInteger();
			this.notificationsDelivered = new AtomicLong();
			this.deliveryDatabaseType = new AtomicReference<>();
			this.deliverySessionId = new AtomicReference<>();
			this.connectionLossCalls = new AtomicInteger();
			this.lossDatabaseType = new AtomicReference<>();
			this.lossSessionId = new AtomicReference<>();
			this.connectionLossFailure = new AtomicReference<>();
		}

		@Override
		public void didDeliverNotificationBatch(@NonNull DatabaseType databaseType,
				@NonNull UUID notificationSessionId,
				@NonNull Long notificationCount) {
			this.deliveryCalls.incrementAndGet();
			this.notificationsDelivered.addAndGet(notificationCount);
			this.deliveryDatabaseType.set(databaseType);
			this.deliverySessionId.set(notificationSessionId);
		}

		@Override
		public void didLoseNotificationConnection(@NonNull DatabaseType databaseType,
				@NonNull UUID notificationSessionId,
				@NonNull Throwable throwable) {
			this.connectionLossCalls.incrementAndGet();
			this.lossDatabaseType.set(databaseType);
			this.lossSessionId.set(notificationSessionId);
			this.connectionLossFailure.set(throwable);
		}
	}
}
