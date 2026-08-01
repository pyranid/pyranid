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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@ThreadSafe
public class DatabaseNotificationTests {
	@Test
	public void unsupportedGenericOperationsDoNotCheckoutAndSessionFailureIsMeasured() {
		ConnectionHarness harness = new ConnectionHarness(true);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = Database.withDataSource(harness.dataSource())
				.databaseType(DatabaseType.GENERIC)
				.metricsCollector(metricsCollector)
				.build();
		AtomicBoolean callbackInvoked = new AtomicBoolean();

		Assertions.assertFalse(database.isNotificationListeningSupported());
		Assertions.assertThrows(UnsupportedOperationException.class,
				() -> database.sendNotification("car_changed", "42"));
		Assertions.assertThrows(UnsupportedOperationException.class,
				() -> database.withNotificationSession("car_changed", session -> callbackInvoked.set(true)));

		Assertions.assertFalse(callbackInvoked.get());
		Assertions.assertEquals(0, harness.dataSource().checkouts());
		Assertions.assertEquals(List.of(), harness.events());

		MetricsCollector.NotificationSnapshot snapshot = notificationSnapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.sessionsStarted());
		Assertions.assertEquals(0L, snapshot.sessionsOpened());
		Assertions.assertEquals(1L, snapshot.sessionsFailed());
	}

	@Test
	public void supportedSessionUsesOneCheckoutAndHonorsSetupAndHealthyCleanupOrder()
			throws InterruptedException {
		ConnectionHarness harness = new ConnectionHarness(true);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);

		database.withNotificationSession(Set.of("car_changed", "truck_changed"),
				session -> harness.events().add("callback"));

		List<String> events = harness.events();
		int callbackIndex = events.indexOf("callback");

		Assertions.assertEquals(1, harness.dataSource().checkouts());
		Assertions.assertEquals("checkout", events.get(0));
		Assertions.assertEquals("getAutoCommit:true", events.get(1));
		Assertions.assertEquals("UNLISTEN *", events.get(2));
		Assertions.assertEquals("drain:1", events.get(3));
		Assertions.assertEquals(2L, events.subList(4, callbackIndex).stream()
				.filter(event -> event.startsWith("LISTEN ")).count());
		Assertions.assertEquals("UNLISTEN *", events.get(callbackIndex + 1));
		Assertions.assertEquals("drain:2", events.get(callbackIndex + 2));
		Assertions.assertEquals("close", events.get(callbackIndex + 3));
		Assertions.assertEquals(callbackIndex + 4, events.size());
		Assertions.assertFalse(events.contains("abort"));

		MetricsCollector.NotificationSnapshot snapshot = notificationSnapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.sessionsStarted());
		Assertions.assertEquals(1L, snapshot.sessionsOpened());
		Assertions.assertEquals(1L, snapshot.sessionsCallbackReturned());
		Assertions.assertEquals(0L, snapshot.sessionsFailed());
	}

	@Test
	public void autocommitFalseIsRolledBackBeforeEnableAndRestoredBeforeClose()
			throws InterruptedException {
		ConnectionHarness harness = new ConnectionHarness(false);
		Database database = postgresDatabase(harness, null);

		database.withNotificationSession("car_changed", session -> harness.events().add("callback"));

		Assertions.assertEquals(List.of(
				"checkout",
				"getAutoCommit:false",
				"rollback",
				"setAutoCommit:true",
				"UNLISTEN *",
				"drain:1",
				"LISTEN \"car_changed\"",
				"callback",
				"UNLISTEN *",
				"drain:2",
				"setAutoCommit:false",
				"close"), harness.events());
	}

	@Test
	public void dispatchGateInterruptionSkipsCallbackAndClosesInterrupted() {
		ConnectionHarness harness = new ConnectionHarness(true).interruptAfterListen(1);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);
		AtomicBoolean callbackInvoked = new AtomicBoolean();

		try {
			Assertions.assertThrows(InterruptedException.class,
					() -> database.withNotificationSession("car_changed", session -> callbackInvoked.set(true)));

			Assertions.assertFalse(callbackInvoked.get());
			Assertions.assertFalse(Thread.currentThread().isInterrupted());
			Assertions.assertEquals(List.of(
					"checkout",
					"getAutoCommit:true",
					"UNLISTEN *",
					"drain:1",
					"LISTEN \"car_changed\"",
					"UNLISTEN *",
					"drain:2",
					"close"), harness.events());

			MetricsCollector.NotificationSnapshot snapshot = notificationSnapshot(metricsCollector);
			Assertions.assertEquals(1L, snapshot.sessionsOpened());
			Assertions.assertEquals(1L, snapshot.sessionsInterrupted());
			Assertions.assertEquals(0L, snapshot.sessionsFailed());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void callbackFailureTranslationPreservesSpecifiedThrowableIdentity() {
		IllegalStateException runtimeFailure = new IllegalStateException("runtime callback failure");
		DatabaseException wrappedRuntime = invokeWithCallbackFailure(runtimeFailure);
		Assertions.assertSame(runtimeFailure, wrappedRuntime.getCause());

		UnsupportedOperationException unsupportedFailure =
				new UnsupportedOperationException("callback unsupported operation");
		DatabaseException wrappedUnsupported = invokeWithCallbackFailure(unsupportedFailure);
		Assertions.assertSame(unsupportedFailure, wrappedUnsupported.getCause());

		ConnectionHarness databaseExceptionHarness = new ConnectionHarness(true);
		Database databaseExceptionDatabase = postgresDatabase(databaseExceptionHarness, null);
		DatabaseException databaseFailure = new DatabaseException("database callback failure");
		DatabaseException observedDatabaseFailure = Assertions.assertThrows(DatabaseException.class,
				() -> databaseExceptionDatabase.withNotificationSession("car_changed", session -> {
					throw databaseFailure;
				}));
		Assertions.assertSame(databaseFailure, observedDatabaseFailure);

		ConnectionHarness errorHarness = new ConnectionHarness(true);
		Database errorDatabase = postgresDatabase(errorHarness, null);
		AssertionError errorFailure = new AssertionError("callback error");
		AssertionError observedErrorFailure = Assertions.assertThrows(AssertionError.class,
				() -> errorDatabase.withNotificationSession("car_changed", session -> {
					throw errorFailure;
				}));
		Assertions.assertSame(errorFailure, observedErrorFailure);
	}

	@Test
	public void directThrowableCallbackFailureIsWrappedAfterExpirationAndCleanup()
			throws InterruptedException {
		ConnectionHarness harness = new ConnectionHarness(true);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);
		DirectThrowable callbackFailure = new DirectThrowable("direct callback failure");
		AtomicReference<NotificationSession> callbackSession = new AtomicReference<>();

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.withNotificationSession("car_changed", session -> {
					callbackSession.set(session);
					throwUnchecked(callbackFailure);
				}));

		Assertions.assertEquals("Notification session operation failed", thrown.getMessage());
		Assertions.assertSame(callbackFailure, thrown.getCause());
		Assertions.assertThrows(IllegalStateException.class,
				() -> requireNonNull(callbackSession.get()).drainNotifications());
		Assertions.assertEquals("UNLISTEN *", harness.events().get(harness.events().size() - 3));
		Assertions.assertEquals("drain:2", harness.events().get(harness.events().size() - 2));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));

		MetricsCollector.NotificationSnapshot snapshot = notificationSnapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.sessionsOpened());
		Assertions.assertEquals(1L, snapshot.sessionsFailed());
		Assertions.assertEquals(0L, snapshot.sessionsCallbackReturned());
	}

	@Test
	public void callbackInterruptedExceptionRetainsIdentityAndCleansUp()
			throws InterruptedException {
		ConnectionHarness harness = new ConnectionHarness(true);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);
		InterruptedException callbackFailure = new InterruptedException("callback interrupted");

		InterruptedException thrown = Assertions.assertThrows(InterruptedException.class,
				() -> database.withNotificationSession("car_changed", session -> {
					throw callbackFailure;
				}));

		Assertions.assertSame(callbackFailure, thrown);
		Assertions.assertFalse(Thread.currentThread().isInterrupted());
		Assertions.assertEquals("UNLISTEN *", harness.events().get(harness.events().size() - 3));
		Assertions.assertEquals("drain:2", harness.events().get(harness.events().size() - 2));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));

		MetricsCollector.NotificationSnapshot snapshot = notificationSnapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.sessionsInterrupted());
		Assertions.assertEquals(0L, snapshot.sessionsFailed());
	}

	@Test
	public void callbackInterruptedExceptionAndStatusAreClearedForHealthyCleanup()
			throws InterruptedException {
		ConnectionHarness harness = new ConnectionHarness(true);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);
		InterruptedException callbackFailure = new InterruptedException("callback interrupted with status");

		try {
			InterruptedException thrown = Assertions.assertThrows(InterruptedException.class,
					() -> database.withNotificationSession("car_changed", session -> {
						Thread.currentThread().interrupt();
						throw callbackFailure;
					}));

			Assertions.assertSame(callbackFailure, thrown);
			Assertions.assertFalse(Thread.currentThread().isInterrupted());
			Assertions.assertEquals(1L, notificationSnapshot(metricsCollector).sessionsInterrupted());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void callbackCanCatchInterruptedReceiveAndReturnNormally()
			throws InterruptedException {
		ConnectionHarness harness = new ConnectionHarness(true);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);
		AtomicBoolean interruptionCaught = new AtomicBoolean();

		try {
			database.withNotificationSession("car_changed", session -> {
				Thread.currentThread().interrupt();

				try {
					session.drainNotifications();
				} catch (InterruptedException interruptedException) {
					interruptionCaught.set(true);
				}

				Assertions.assertFalse(Thread.currentThread().isInterrupted());
				Assertions.assertEquals(List.of(), session.drainNotifications());
			});

			Assertions.assertTrue(interruptionCaught.get());
			Assertions.assertFalse(Thread.currentThread().isInterrupted());
			Assertions.assertEquals(1L, notificationSnapshot(metricsCollector).sessionsCallbackReturned());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void callbackReturnWithInterruptStatusProducesCleanInterruption()
			throws InterruptedException {
		ConnectionHarness harness = new ConnectionHarness(true);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);

		try {
			Assertions.assertThrows(InterruptedException.class,
					() -> database.withNotificationSession("car_changed",
							session -> Thread.currentThread().interrupt()));

			Assertions.assertFalse(Thread.currentThread().isInterrupted());
			Assertions.assertEquals("UNLISTEN *", harness.events().get(harness.events().size() - 3));
			Assertions.assertEquals("drain:2", harness.events().get(harness.events().size() - 2));
			Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));
			Assertions.assertEquals(1L, notificationSnapshot(metricsCollector).sessionsInterrupted());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void preInterruptedEntryClearsStatusWithoutOpeningSession() {
		ConnectionHarness harness = new ConnectionHarness(true);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);

		try {
			Thread.currentThread().interrupt();

			Assertions.assertThrows(InterruptedException.class,
					() -> database.withNotificationSession("car_changed", session -> {}));

			Assertions.assertFalse(Thread.currentThread().isInterrupted());
			Assertions.assertEquals(0, harness.dataSource().checkouts());
			Assertions.assertTrue(harness.events().isEmpty());
			MetricsCollector.NotificationSnapshot snapshot = notificationSnapshot(metricsCollector);
			Assertions.assertEquals(0L, snapshot.sessionsStarted());
			Assertions.assertEquals(0L, snapshot.sessionsOpened());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void cleanupFailureDisplacesConcreteCallbackInterruption()
			throws InterruptedException {
		SQLException cleanupFailure = new SQLException("cleanup unlisten failed");
		ConnectionHarness harness = new ConnectionHarness(true).failUnlistenOn(2, cleanupFailure);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);
		InterruptedException callbackFailure = new InterruptedException("callback interrupted with status");
		DatabaseException thrown;
		boolean interruptReasserted;

		try {
			thrown = Assertions.assertThrows(DatabaseException.class,
					() -> database.withNotificationSession("car_changed", session -> {
						Thread.currentThread().interrupt();
						throw callbackFailure;
					}));
			interruptReasserted = Thread.currentThread().isInterrupted();
		} finally {
			Thread.interrupted();
		}

		Assertions.assertSame(cleanupFailure, thrown.getCause());
		Assertions.assertArrayEquals(new Throwable[]{callbackFailure}, thrown.getSuppressed());
		Assertions.assertTrue(interruptReasserted);
		Assertions.assertEquals("abort", harness.events().get(harness.events().size() - 2));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));

		MetricsCollector.NotificationSnapshot snapshot = notificationSnapshot(metricsCollector);
		Assertions.assertEquals(0L, snapshot.sessionsInterrupted());
		Assertions.assertEquals(1L, snapshot.sessionsFailed());
	}

	@Test
	public void interruptedConnectionLossRetainsIdentityAndReportsFailedBeforeAbort()
			throws InterruptedException {
		SQLException receiveFailure = new SQLException("receive failed", "08006");
		ConnectionHarness harness = new ConnectionHarness(true)
				.failDrainOn(2, receiveFailure)
				.interruptWithDrainFailure();
		RecordingNotificationMetricsCollector metricsCollector = new RecordingNotificationMetricsCollector();
		Database database = postgresDatabase(harness, metricsCollector);
		AtomicReference<DatabaseException> discoveredFailure = new AtomicReference<>();
		DatabaseException thrown;
		boolean interruptReasserted;

		try {
			thrown = Assertions.assertThrows(DatabaseException.class,
					() -> database.withNotificationSession("car_changed", session -> {
						try {
							session.drainNotifications();
						} catch (DatabaseException failure) {
							discoveredFailure.set(failure);
							Assertions.assertThrows(IllegalStateException.class, session::drainNotifications);
							throw failure;
						}
					}));
			interruptReasserted = Thread.currentThread().isInterrupted();
		} finally {
			Thread.interrupted();
		}

		Assertions.assertSame(discoveredFailure.get(), thrown);
		Assertions.assertSame(receiveFailure, thrown.getCause());
		Assertions.assertSame(thrown, metricsCollector.connectionLossFailure.get());
		Assertions.assertSame(thrown, metricsCollector.closeFailure.get());
		Assertions.assertEquals(MetricsCollector.NotificationSessionOutcome.FAILED,
				metricsCollector.closeOutcome.get());
		Assertions.assertEquals(1, metricsCollector.connectionLossCount);
		Assertions.assertTrue(interruptReasserted);
		Assertions.assertEquals("abort", harness.events().get(harness.events().size() - 2));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));
		Assertions.assertEquals(1L, harness.events().stream()
				.filter(event -> "UNLISTEN *".equals(event)).count());
	}

	@Test
	public void callbackCannotSwallowReceiveErrorFromUncertainTransport()
			throws InterruptedException {
		AssertionError receiveFailure = new AssertionError("driver receive error");
		ConnectionHarness harness = new ConnectionHarness(true).failDrainOn(2, receiveFailure);
		RecordingNotificationMetricsCollector metricsCollector = new RecordingNotificationMetricsCollector();
		Database database = postgresDatabase(harness, metricsCollector);
		AtomicReference<AssertionError> discoveredFailure = new AtomicReference<>();

		AssertionError thrown = Assertions.assertThrows(AssertionError.class,
				() -> database.withNotificationSession("car_changed", session -> {
					try {
						session.drainNotifications();
					} catch (AssertionError failure) {
						discoveredFailure.set(failure);
						Assertions.assertThrows(IllegalStateException.class, session::drainNotifications);
					}
				}));

		Assertions.assertSame(receiveFailure, discoveredFailure.get());
		Assertions.assertSame(receiveFailure, thrown);
		Assertions.assertSame(receiveFailure, metricsCollector.connectionLossFailure.get());
		Assertions.assertSame(receiveFailure, metricsCollector.closeFailure.get());
		Assertions.assertEquals(MetricsCollector.NotificationSessionOutcome.FAILED,
				metricsCollector.closeOutcome.get());
		Assertions.assertEquals(1, metricsCollector.connectionLossCount);
		Assertions.assertEquals("abort", harness.events().get(harness.events().size() - 2));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));
	}

	@Test
	public void sendNotificationAppliesDatabaseDefaultPreparedStatementSettings() {
		ConnectionHarness harness = new ConnectionHarness(true);
		Database database = Database.withDataSource(harness.dataSource())
				.databaseType(DatabaseType.POSTGRESQL)
				.queryTimeout(Duration.ofMillis(1_500))
				.fetchSize(27)
				.maxRows(9)
				.build();

		database.sendNotification("car_changed", "42");

		Assertions.assertEquals(List.of(
				"checkout",
				"prepareStatement:SELECT pg_notify(?, ?)",
				"setQueryTimeout:2",
				"setFetchSize:27",
				"setMaxRows:9",
				"setObject:1:car_changed",
				"setObject:2:42",
				"executePreparedStatement",
				"closePreparedStatement",
				"close"), harness.events());
	}

	@Test
	public void sendNotificationWithoutDatabaseDefaultsDoesNotMutatePreparedStatementSettings() {
		ConnectionHarness harness = new ConnectionHarness(true);
		Database database = postgresDatabase(harness, null);

		database.sendNotification("car_changed", "42");

		Assertions.assertEquals(List.of(
				"checkout",
				"prepareStatement:SELECT pg_notify(?, ?)",
				"setObject:1:car_changed",
				"setObject:2:42",
				"executePreparedStatement",
				"closePreparedStatement",
				"close"), harness.events());
	}

	@Test
	public void callbackCaughtReceiveFailureStillEscapesOuterScopeAndAborts() {
		SQLException receiveFailure = new SQLException("receive failed", "08006");
		ConnectionHarness harness = new ConnectionHarness(true).failDrainOn(2, receiveFailure);
		Database database = postgresDatabase(harness, null);
		AtomicReference<DatabaseException> callbackFailure = new AtomicReference<>();

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.withNotificationSession("car_changed", session -> {
					try {
						session.drainNotifications();
					} catch (DatabaseException exception) {
						callbackFailure.set(exception);
					}
				}));

		Assertions.assertSame(callbackFailure.get(), thrown);
		Assertions.assertSame(receiveFailure, thrown.getCause());
		Assertions.assertEquals("abort", harness.events().get(harness.events().size() - 2));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));
		Assertions.assertEquals(1L, harness.events().stream()
				.filter(event -> "UNLISTEN *".equals(event)).count());
	}

	@Test
	public void partialRegistrationFailureUsesHealthyUnlistenAndDrainCleanup() {
		SQLException registrationFailure = new SQLException("registration failed", "42000");
		ConnectionHarness harness = new ConnectionHarness(true).failListenOn(2, registrationFailure);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);
		AtomicBoolean callbackInvoked = new AtomicBoolean();

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.withNotificationSession(Set.of("car_changed", "truck_changed"),
						session -> callbackInvoked.set(true)));

		Assertions.assertSame(registrationFailure, thrown.getCause());
		Assertions.assertFalse(callbackInvoked.get());
		Assertions.assertEquals(2L, harness.events().stream()
				.filter(event -> "UNLISTEN *".equals(event)).count());
		Assertions.assertEquals(2L, harness.events().stream()
				.filter(event -> event.startsWith("drain:")).count());
		Assertions.assertEquals(2L, harness.events().stream()
				.filter(event -> event.startsWith("LISTEN ")).count());
		Assertions.assertFalse(harness.events().contains("abort"));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));

		MetricsCollector.NotificationSnapshot snapshot = notificationSnapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.sessionsStarted());
		Assertions.assertEquals(0L, snapshot.sessionsOpened());
		Assertions.assertEquals(1L, snapshot.sessionsFailed());
	}

	@Test
	public void nonSqlRegistrationFailuresStillUseHealthyPartialRegistrationCleanup() {
		IllegalStateException runtimeFailure = new IllegalStateException("registration runtime failure");
		ConnectionHarness runtimeHarness = new ConnectionHarness(true).failListenOn(2, runtimeFailure);
		Database runtimeDatabase = postgresDatabase(runtimeHarness, null);

		DatabaseException runtimeThrown = Assertions.assertThrows(DatabaseException.class, () ->
				runtimeDatabase.withNotificationSession(
						Set.of("car_changed", "truck_changed"), session -> {}));

		Assertions.assertSame(runtimeFailure, runtimeThrown.getCause());
		assertHealthyPartialRegistrationCleanup(runtimeHarness);

		AssertionError errorFailure = new AssertionError("registration error");
		ConnectionHarness errorHarness = new ConnectionHarness(true).failListenOn(2, errorFailure);
		Database errorDatabase = postgresDatabase(errorHarness, null);

		AssertionError errorThrown = Assertions.assertThrows(AssertionError.class, () ->
				errorDatabase.withNotificationSession(
						Set.of("car_changed", "truck_changed"), session -> {}));

		Assertions.assertSame(errorFailure, errorThrown);
		assertHealthyPartialRegistrationCleanup(errorHarness);
	}

	@Test
	public void ordinaryInitialDrainFailureIsDatabaseFailureRatherThanUnsupportedCapability() {
		SQLException drainFailure = new SQLException("receive protocol failure", "XX000");
		ConnectionHarness harness = new ConnectionHarness(true).failDrainOn(1, drainFailure);
		Database database = postgresDatabase(harness, null);

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class, () ->
				database.withNotificationSession("car_changed", session -> {}));

		Assertions.assertSame(drainFailure, thrown.getCause());
		Assertions.assertEquals("abort", harness.events().get(harness.events().size() - 2));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));
	}

	@Test
	public void timeoutGuardCapabilityFailureIsUnsupportedUnlessConnectionDiagnosticFails() {
		SQLFeatureNotSupportedException capabilityFailure =
				new SQLFeatureNotSupportedException("network timeout unsupported");
		ConnectionHarness unsupportedHarness =
				new ConnectionHarness(true).failNetworkTimeoutInspection(capabilityFailure);
		Database unsupportedDatabase = postgresDatabase(unsupportedHarness, null);

		UnsupportedOperationException unsupportedThrown = Assertions.assertThrows(
				UnsupportedOperationException.class, () ->
						unsupportedDatabase.withNotificationSession("car_changed", session -> {}));

		Assertions.assertInstanceOf(
				NotificationReceiveUnsupportedException.class, unsupportedThrown.getCause());
		Assertions.assertSame(capabilityFailure, unsupportedThrown.getCause().getCause());

		SQLException plainCapabilityFailure =
				new SQLException("proxy cannot inspect network timeout", "HY000");
		ConnectionHarness plainUnsupportedHarness =
				new ConnectionHarness(true).failNetworkTimeoutInspection(plainCapabilityFailure);
		Database plainUnsupportedDatabase = postgresDatabase(plainUnsupportedHarness, null);

		UnsupportedOperationException plainUnsupportedThrown = Assertions.assertThrows(
				UnsupportedOperationException.class, () ->
						plainUnsupportedDatabase.withNotificationSession("car_changed", session -> {}));

		Assertions.assertInstanceOf(
				NotificationReceiveUnsupportedException.class, plainUnsupportedThrown.getCause());
		Assertions.assertSame(plainCapabilityFailure, plainUnsupportedThrown.getCause().getCause());
		Assertions.assertEquals(
				"abort", plainUnsupportedHarness.events().get(plainUnsupportedHarness.events().size() - 2));
		Assertions.assertEquals(
				"close", plainUnsupportedHarness.events().get(plainUnsupportedHarness.events().size() - 1));

		SQLFeatureNotSupportedException secondCapabilityFailure =
				new SQLFeatureNotSupportedException("network timeout unsupported");
		SQLException connectionDiagnosticFailure =
				new SQLException("connection lost while checking health", "08006");
		ConnectionHarness failedConnectionHarness = new ConnectionHarness(true)
				.failNetworkTimeoutInspection(secondCapabilityFailure)
				.failClosedCheck(connectionDiagnosticFailure);
		Database failedConnectionDatabase = postgresDatabase(failedConnectionHarness, null);

		DatabaseException databaseThrown = Assertions.assertThrows(DatabaseException.class, () ->
				failedConnectionDatabase.withNotificationSession("car_changed", session -> {}));

		Assertions.assertInstanceOf(
				NotificationReceiveUnsupportedException.class, databaseThrown.getCause());
		Assertions.assertArrayEquals(
				new Throwable[]{connectionDiagnosticFailure}, databaseThrown.getCause().getSuppressed());
		Assertions.assertEquals(
				"abort", failedConnectionHarness.events().get(failedConnectionHarness.events().size() - 2));
		Assertions.assertEquals(
				"close", failedConnectionHarness.events().get(failedConnectionHarness.events().size() - 1));
	}

	@Test
	public void preSanitationFailureAbortsAndClosesCandidate() {
		SQLException configurationFailure = new SQLException("autocommit inspection failed");
		ConnectionHarness harness = new ConnectionHarness(true).failGetAutoCommit(configurationFailure);
		Database database = postgresDatabase(harness, null);
		AtomicBoolean callbackInvoked = new AtomicBoolean();

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.withNotificationSession("car_changed", session -> callbackInvoked.set(true)));

		Assertions.assertSame(configurationFailure, thrown.getCause());
		Assertions.assertFalse(callbackInvoked.get());
		Assertions.assertEquals(List.of(
				"checkout",
				"getAutoCommit:true",
				"abort",
				"close"), harness.events());
	}

	@Test
	public void exactStoredTransportFailureIsNotSelfSuppressed() {
		DatabaseException transportFailure = new DatabaseException("exact receive failure");
		ConnectionHarness harness = new ConnectionHarness(true).failDrainOn(2, transportFailure);
		Database database = postgresDatabase(harness, null);

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.withNotificationSession("car_changed", session -> {
					try {
						session.drainNotifications();
					} catch (DatabaseException exception) {
						throw exception;
					}
				}));

		Assertions.assertSame(transportFailure, thrown);
		Assertions.assertEquals(0, thrown.getSuppressed().length);
		Assertions.assertEquals("abort", harness.events().get(harness.events().size() - 2));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));
	}

	@Test
	public void cleanupFailureDisplacesDispatchInterruption() {
		SQLException cleanupFailure = new SQLException("cleanup unlisten failed");
		ConnectionHarness harness = new ConnectionHarness(true)
				.interruptAfterListen(1)
				.failUnlistenOn(2, cleanupFailure);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);
		AtomicBoolean callbackInvoked = new AtomicBoolean();
		DatabaseException thrown;
		boolean interruptReasserted;

		try {
			thrown = Assertions.assertThrows(DatabaseException.class,
					() -> database.withNotificationSession("car_changed", session -> callbackInvoked.set(true)));
			interruptReasserted = Thread.currentThread().isInterrupted();
		} finally {
			Thread.interrupted();
		}

		Assertions.assertSame(cleanupFailure, thrown.getCause());
		Assertions.assertTrue(interruptReasserted);
		Assertions.assertFalse(callbackInvoked.get());
		Assertions.assertEquals("abort", harness.events().get(harness.events().size() - 2));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));

		MetricsCollector.NotificationSnapshot snapshot = notificationSnapshot(metricsCollector);
		Assertions.assertEquals(1L, snapshot.sessionsOpened());
		Assertions.assertEquals(0L, snapshot.sessionsInterrupted());
		Assertions.assertEquals(1L, snapshot.sessionsFailed());
	}

	@Test
	public void cleanupDrainFailureAbortsInsteadOfReturningConnection() {
		SQLException cleanupFailure = new SQLException("cleanup drain failed", "XX000");
		ConnectionHarness harness = new ConnectionHarness(true).failDrainOn(2, cleanupFailure);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.withNotificationSession("car_changed", session -> {}));

		Assertions.assertSame(cleanupFailure, thrown.getCause());
		Assertions.assertEquals(List.of("drain:2", "abort", "close"),
				harness.events().subList(harness.events().size() - 3, harness.events().size()));
		Assertions.assertEquals(1L, notificationSnapshot(metricsCollector).sessionsFailed());
	}

	@Test
	public void healthyCloseFailureDoesNotReuseConnectionHandle() throws InterruptedException {
		SQLException closeFailure = new SQLException("close failed");
		ConnectionHarness harness = new ConnectionHarness(true).failClose(closeFailure);
		RecordingNotificationMetricsCollector metricsCollector = new RecordingNotificationMetricsCollector();
		Database database = postgresDatabase(harness, metricsCollector);

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.withNotificationSession("car_changed", session -> {}));

		Assertions.assertSame(closeFailure, thrown.getCause());
		Assertions.assertSame(thrown, metricsCollector.closeFailure.get());
		Assertions.assertEquals(MetricsCollector.NotificationSessionOutcome.FAILED,
				metricsCollector.closeOutcome.get());
		Assertions.assertEquals(1L, harness.events().stream()
				.filter(event -> "close".equals(event)).count());
		Assertions.assertFalse(harness.events().contains("abort"));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));
	}

	@Test
	public void abortFailureDoesNotPreventFinalCloseAttempt() {
		SQLException cleanupFailure = new SQLException("cleanup unlisten failed");
		SQLException abortFailure = new SQLException("abort failed");
		ConnectionHarness harness = new ConnectionHarness(true)
				.failUnlistenOn(2, cleanupFailure)
				.failAbort(abortFailure);
		Database database = postgresDatabase(harness, null);

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.withNotificationSession("car_changed", session -> {}));

		Assertions.assertSame(cleanupFailure, thrown.getCause());
		Assertions.assertEquals(1, thrown.getSuppressed().length);
		Assertions.assertInstanceOf(DatabaseException.class, thrown.getSuppressed()[0]);
		Assertions.assertSame(abortFailure, thrown.getSuppressed()[0].getCause());
		Assertions.assertEquals(List.of("UNLISTEN *", "abort", "close"),
				harness.events().subList(harness.events().size() - 3, harness.events().size()));
	}

	@Test
	public void autocommitRestorationFailureAbortsAndClosesConnection() {
		SQLException restorationFailure = new SQLException("autocommit restoration failed");
		ConnectionHarness harness = new ConnectionHarness(false)
				.failSetAutoCommitOn(2, restorationFailure);
		MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();
		Database database = postgresDatabase(harness, metricsCollector);

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.withNotificationSession("car_changed", session -> {}));

		Assertions.assertSame(restorationFailure, thrown.getCause());
		Assertions.assertEquals(List.of("setAutoCommit:false", "abort", "close"),
				harness.events().subList(harness.events().size() - 3, harness.events().size()));
		Assertions.assertEquals(1L, notificationSnapshot(metricsCollector).sessionsFailed());
	}

	@Test
	public void uncertainAutocommitFalseSessionSkipsRestorationAndAborts() {
		SQLException receiveFailure = new SQLException("receive failed", "08006");
		ConnectionHarness harness = new ConnectionHarness(false).failDrainOn(2, receiveFailure);
		Database database = postgresDatabase(harness, null);

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.withNotificationSession("car_changed", NotificationSession::drainNotifications));

		Assertions.assertSame(receiveFailure, thrown.getCause());
		Assertions.assertTrue(harness.events().contains("setAutoCommit:true"));
		Assertions.assertFalse(harness.events().contains("setAutoCommit:false"),
				"An uncertain connection must not be mutated before it is discarded");
		Assertions.assertEquals(List.of("abort", "close"),
				harness.events().subList(harness.events().size() - 2, harness.events().size()));
	}

	@NonNull
	private static Database postgresDatabase(@NonNull ConnectionHarness harness,
			MetricsCollector metricsCollector) {
		Database.Builder builder = Database.withDataSource(harness.dataSource())
				.databaseType(DatabaseType.POSTGRESQL);

		if (metricsCollector != null)
			builder.metricsCollector(metricsCollector);

		return builder.build();
	}

	@NonNull
	private static DatabaseException invokeWithCallbackFailure(@NonNull RuntimeException callbackFailure) {
		ConnectionHarness harness = new ConnectionHarness(true);
		Database database = postgresDatabase(harness, null);

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class,
				() -> database.withNotificationSession("car_changed", session -> {
					throw callbackFailure;
				}));

		Assertions.assertEquals("Notification session operation failed", thrown.getMessage());
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));
		return thrown;
	}

	private static MetricsCollector.NotificationSnapshot notificationSnapshot(
			@NonNull MetricsCollector metricsCollector) {
		return metricsCollector.notificationSnapshot().orElseThrow();
	}

	private static void assertHealthyPartialRegistrationCleanup(@NonNull ConnectionHarness harness) {
		Assertions.assertEquals(2L, harness.events().stream()
				.filter(event -> "UNLISTEN *".equals(event)).count());
		Assertions.assertEquals(2L, harness.events().stream()
				.filter(event -> event.startsWith("drain:")).count());
		Assertions.assertFalse(harness.events().contains("abort"));
		Assertions.assertEquals("close", harness.events().get(harness.events().size() - 1));
	}

	@SuppressWarnings("unchecked")
	private static <T extends Throwable> void throwUnchecked(@NonNull Throwable throwable) throws T {
		throw (T) requireNonNull(throwable);
	}

	private static final class DirectThrowable extends Throwable {
		private DirectThrowable(@NonNull String message) {
			super(requireNonNull(message));
		}
	}

	private static final class RecordingNotificationMetricsCollector implements MetricsCollector {
		private int connectionLossCount;
		@NonNull
		private final AtomicReference<Throwable> connectionLossFailure = new AtomicReference<>();
		@NonNull
		private final AtomicReference<NotificationSessionOutcome> closeOutcome = new AtomicReference<>();
		@NonNull
		private final AtomicReference<Throwable> closeFailure = new AtomicReference<>();

		@Override
		public void didLoseNotificationConnection(@NonNull DatabaseType databaseType,
				@NonNull UUID notificationSessionId,
				@NonNull Throwable throwable) {
			this.connectionLossCount++;
			this.connectionLossFailure.set(throwable);
		}

		@Override
		public void didCloseNotificationSession(@NonNull DatabaseType databaseType,
				@NonNull UUID notificationSessionId,
				@NonNull NotificationSessionOutcome outcome,
				@NonNull Duration sessionDuration,
				@Nullable Throwable throwable) {
			this.closeOutcome.set(outcome);
			this.closeFailure.set(throwable);
		}
	}

	private static final class ConnectionHarness {
		@NonNull
		private final List<String> events;
		@NonNull
		private final Connection connection;
		@NonNull
		private final RecordingDataSource dataSource;
		private boolean autoCommit;
		private boolean closed;
		private int unlistenCalls;
		private int listenCalls;
		private int drainCalls;
		private int interruptAfterListenCall;
		private int failUnlistenCall;
		private int failListenCall;
		private int failDrainCall;
		private int setAutoCommitCalls;
		private int failSetAutoCommitCall;
		private Throwable getAutoCommitFailure;
		private Throwable setAutoCommitFailure;
		private Throwable unlistenFailure;
		private Throwable listenFailure;
		private Throwable drainFailure;
		private Throwable abortFailure;
		private Throwable closeFailure;
		private Throwable networkTimeoutInspectionFailure;
		private Throwable closedCheckFailure;
		private boolean interruptWithDrainFailure;

		private ConnectionHarness(boolean autoCommit) {
			this.events = new ArrayList<>();
			this.autoCommit = autoCommit;
			this.interruptAfterListenCall = -1;
			this.failUnlistenCall = -1;
			this.failListenCall = -1;
			this.failDrainCall = -1;
			this.failSetAutoCommitCall = -1;
			this.connection = (Connection) Proxy.newProxyInstance(
					DatabaseNotificationTests.class.getClassLoader(),
					new Class<?>[]{Connection.class, PGConnection.class},
					this::invokeConnection);
			this.dataSource = new RecordingDataSource(this.connection, this.events);
		}

		@NonNull
		private ConnectionHarness interruptAfterListen(int call) {
			this.interruptAfterListenCall = call;
			return this;
		}

		@NonNull
		private ConnectionHarness failGetAutoCommit(@NonNull Throwable failure) {
			this.getAutoCommitFailure = requireNonNull(failure);
			return this;
		}

		@NonNull
		private ConnectionHarness failSetAutoCommitOn(int call,
				@NonNull Throwable failure) {
			this.failSetAutoCommitCall = call;
			this.setAutoCommitFailure = requireNonNull(failure);
			return this;
		}

		@NonNull
		private ConnectionHarness failUnlistenOn(int call,
				@NonNull Throwable failure) {
			this.failUnlistenCall = call;
			this.unlistenFailure = requireNonNull(failure);
			return this;
		}

		@NonNull
		private ConnectionHarness failListenOn(int call,
				@NonNull Throwable failure) {
			this.failListenCall = call;
			this.listenFailure = requireNonNull(failure);
			return this;
		}

		@NonNull
		private ConnectionHarness failDrainOn(int call,
				@NonNull Throwable failure) {
			this.failDrainCall = call;
			this.drainFailure = requireNonNull(failure);
			return this;
		}

		@NonNull
		private ConnectionHarness interruptWithDrainFailure() {
			this.interruptWithDrainFailure = true;
			return this;
		}

		@NonNull
		private ConnectionHarness failAbort(@NonNull Throwable failure) {
			this.abortFailure = requireNonNull(failure);
			return this;
		}

		@NonNull
		private ConnectionHarness failClose(@NonNull Throwable failure) {
			this.closeFailure = requireNonNull(failure);
			return this;
		}

		@NonNull
		private ConnectionHarness failNetworkTimeoutInspection(@NonNull Throwable failure) {
			this.networkTimeoutInspectionFailure = requireNonNull(failure);
			return this;
		}

		@NonNull
		private ConnectionHarness failClosedCheck(@NonNull Throwable failure) {
			this.closedCheckFailure = requireNonNull(failure);
			return this;
		}

		@NonNull
		private List<String> events() {
			return this.events;
		}

		@NonNull
		private RecordingDataSource dataSource() {
			return this.dataSource;
		}

		private Object invokeConnection(Object proxy,
				@NonNull Method method,
				Object[] args) throws Throwable {
			if (method.getDeclaringClass() == Object.class)
				return objectMethod(proxy, method, args, "notificationConnection");

			return switch (method.getName()) {
				case "getAutoCommit" -> getAutoCommit();
				case "setAutoCommit" -> {
					boolean value = (Boolean) args[0];
					this.setAutoCommitCalls++;
					this.events.add("setAutoCommit:" + value);

					if (this.setAutoCommitCalls == this.failSetAutoCommitCall)
						throw requireNonNull(this.setAutoCommitFailure);

					this.autoCommit = value;
					yield null;
				}
				case "rollback" -> {
					this.events.add("rollback");
					yield null;
				}
				case "createStatement" -> statementProxy();
				case "prepareStatement" -> preparedStatementProxy((String) args[0]);
				case "getNotifications" -> notifications(args);
				case "getNetworkTimeout" -> {
					if (this.networkTimeoutInspectionFailure != null)
						throw this.networkTimeoutInspectionFailure;

					yield 0;
				}
				case "setNetworkTimeout" -> null;
				case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
				case "unwrap" -> unwrap(proxy, (Class<?>) args[0]);
				case "isClosed" -> {
					if (this.closedCheckFailure != null)
						throw this.closedCheckFailure;

					yield this.closed;
				}
				case "abort" -> {
					this.events.add("abort");

					if (this.abortFailure != null)
						throw this.abortFailure;

					this.closed = true;
					yield null;
				}
				case "close" -> {
					this.events.add("close");
					this.closed = true;

					if (this.closeFailure != null)
						throw this.closeFailure;

					yield null;
				}
				default -> defaultValue(method.getReturnType());
			};
		}

		@NonNull
		private PreparedStatement preparedStatementProxy(@NonNull String sql) {
			this.events.add("prepareStatement:" + requireNonNull(sql));
			return (PreparedStatement) Proxy.newProxyInstance(
					DatabaseNotificationTests.class.getClassLoader(),
					new Class<?>[]{PreparedStatement.class},
					this::invokePreparedStatement);
		}

		private Object invokePreparedStatement(Object proxy,
				@NonNull Method method,
				Object[] args) {
			if (method.getDeclaringClass() == Object.class)
				return objectMethod(proxy, method, args, "notificationPreparedStatement");

			return switch (method.getName()) {
				case "setQueryTimeout" -> {
					this.events.add("setQueryTimeout:" + args[0]);
					yield null;
				}
				case "setFetchSize" -> {
					this.events.add("setFetchSize:" + args[0]);
					yield null;
				}
				case "setMaxRows" -> {
					this.events.add("setMaxRows:" + args[0]);
					yield null;
				}
				case "setObject" -> {
					this.events.add("setObject:" + args[0] + ":" + args[1]);
					yield null;
				}
				case "execute" -> {
					this.events.add("executePreparedStatement");
					yield false;
				}
				case "getUpdateCount" -> -1;
				case "close" -> {
					this.events.add("closePreparedStatement");
					yield null;
				}
				case "getConnection" -> this.connection;
				default -> defaultValue(method.getReturnType());
			};
		}

		private boolean getAutoCommit() throws Throwable {
			this.events.add("getAutoCommit:" + this.autoCommit);

			if (this.getAutoCommitFailure != null)
				throw this.getAutoCommitFailure;

			return this.autoCommit;
		}

		@NonNull
		private Statement statementProxy() {
			return (Statement) Proxy.newProxyInstance(
					DatabaseNotificationTests.class.getClassLoader(),
					new Class<?>[]{Statement.class},
					this::invokeStatement);
		}

		private Object invokeStatement(Object proxy,
				@NonNull Method method,
				Object[] args) throws Throwable {
			if (method.getDeclaringClass() == Object.class)
				return objectMethod(proxy, method, args, "notificationStatement");

			if ("execute".equals(method.getName()) && args != null && args.length > 0
					&& args[0] instanceof String sql) {
				this.events.add(sql);

				if ("UNLISTEN *".equals(sql)) {
					this.unlistenCalls++;

					if (this.unlistenCalls == this.failUnlistenCall)
						throw requireNonNull(this.unlistenFailure);
				} else if (sql.startsWith("LISTEN ")) {
					this.listenCalls++;

					if (this.listenCalls == this.failListenCall)
						throw requireNonNull(this.listenFailure);

					if (this.listenCalls == this.interruptAfterListenCall)
						Thread.currentThread().interrupt();
				}

				return false;
			}

			return defaultValue(method.getReturnType());
		}

		@NonNull
		private PGNotification[] notifications(Object[] args) throws Throwable {
			if (args != null && args.length > 0) {
				this.events.add("receive:" + args[0]);
				return new PGNotification[0];
			}

			this.drainCalls++;
			this.events.add("drain:" + this.drainCalls);

			if (this.drainCalls == this.failDrainCall)
				if (this.interruptWithDrainFailure)
					Thread.currentThread().interrupt();

			if (this.drainCalls == this.failDrainCall)
				throw requireNonNull(this.drainFailure);

			return new PGNotification[0];
		}
	}

	private static final class RecordingDataSource implements DataSource {
		@NonNull
		private final Connection connection;
		@NonNull
		private final List<String> events;
		private int checkouts;

		private RecordingDataSource(@NonNull Connection connection,
				@NonNull List<String> events) {
			this.connection = requireNonNull(connection);
			this.events = requireNonNull(events);
		}

		@Override
		public Connection getConnection() {
			this.checkouts++;
			this.events.add("checkout");
			return this.connection;
		}

		@Override
		public Connection getConnection(String username,
				String password) {
			return getConnection();
		}

		private int checkouts() {
			return this.checkouts;
		}

		@Override
		public PrintWriter getLogWriter() {
			return null;
		}

		@Override
		public void setLogWriter(PrintWriter out) {
			// No-op for the fake data source.
		}

		@Override
		public void setLoginTimeout(int seconds) {
			// No-op for the fake data source.
		}

		@Override
		public int getLoginTimeout() {
			return 0;
		}

		@Override
		public Logger getParentLogger() {
			return Logger.getLogger(RecordingDataSource.class.getName());
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			if (iface.isInstance(this))
				return iface.cast(this);

			throw new SQLException("No wrapper for " + iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) {
			return iface.isInstance(this);
		}
	}

	private static Object unwrap(@NonNull Object object,
			@NonNull Class<?> type) throws SQLException {
		if (type.isInstance(object))
			return type.cast(object);

		throw new SQLException("No wrapper for " + type);
	}

	private static Object objectMethod(@NonNull Object proxy,
			@NonNull Method method,
			Object[] args,
			@NonNull String description) {
		return switch (method.getName()) {
			case "toString" -> description;
			case "hashCode" -> System.identityHashCode(proxy);
			case "equals" -> proxy == args[0];
			default -> throw new UnsupportedOperationException(method.getName());
		};
	}

	private static Object defaultValue(@NonNull Class<?> returnType) {
		if (!returnType.isPrimitive() || returnType == void.class)
			return null;

		if (returnType == boolean.class)
			return false;

		if (returnType == byte.class)
			return (byte) 0;

		if (returnType == short.class)
			return (short) 0;

		if (returnType == int.class)
			return 0;

		if (returnType == long.class)
			return 0L;

		if (returnType == float.class)
			return 0.0F;

		if (returnType == double.class)
			return 0.0D;

		if (returnType == char.class)
			return '\0';

		throw new IllegalArgumentException("Unsupported primitive type " + returnType);
	}
}
