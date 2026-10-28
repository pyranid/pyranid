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

import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises retry fences through the production PostgreSQL adapter and notification-session lifecycle. */
class NotificationSupervisorFatalFailureTests {
	@Test
	void unsupportedAbortDuringTransientFailureStillAllowsReconnect() throws Exception {
		for (Throwable unsupported : List.of(new SQLFeatureNotSupportedException("abort unavailable"),
				new UnsupportedOperationException("abort unavailable"),
				new DatabaseException("abort", new SQLFeatureNotSupportedException("abort unavailable")))) {
			DatabaseException primary = new DatabaseException("receive", new SQLException("connection lost", "08006"));
			Fixture fixture = new Fixture(primary);
			fixture.abortFailure = unsupported;
			assertReconnects(fixture, primary);
			assertSame(unsupported, primary.getSuppressed()[0].getCause());
		}
	}

	@Test
	void unsupportedCloseDuringTransientFailureStillAllowsReconnect() throws Exception {
		DatabaseException primary = new DatabaseException("receive", new SQLException("connection lost", "08006"));
		Fixture fixture = new Fixture(primary);
		fixture.firstCloseFailure = new SQLFeatureNotSupportedException("close unavailable");
		assertReconnects(fixture, primary);
		assertSame(fixture.firstCloseFailure, primary.getSuppressed()[0].getCause());
	}

	@Test
	void wrappedErrorInConnectionCleanupRemainsTerminal() throws Exception {
		DatabaseException primary = new DatabaseException("receive", new SQLException("connection lost", "08006"));
		Fixture fixture = new Fixture(primary);
		fixture.abortFailure = new DatabaseException("abort", new AssertionError("fatal abort failure"));
		Throwable terminal = fixture.run(fixture.builder().build()).getFailure().orElseThrow();
		assertSame(primary, terminal);
		assertSame(fixture.abortFailure, primary.getSuppressed()[0].getCause());
		fixture.assertFatalAttempt();
	}

	@Test
	void sharedUnsupportedReceiveAndCleanupFailureRemainsTerminal() throws Exception {
		SQLFeatureNotSupportedException unsupported = new SQLFeatureNotSupportedException("unavailable");
		Fixture fixture = new Fixture(unsupported);
		fixture.abortFailure = unsupported;
		Throwable terminal = fixture.run(fixture.builder().build()).getFailure().orElseThrow();
		assertSame(unsupported, terminal.getCause().getCause());
		assertSame(unsupported, terminal.getSuppressed()[0].getCause());
		fixture.assertFatalAttempt();
	}

	@Test
	void plainUnsupportedSqlStateInJdbcNextExceptionPreventsRegistrationRetry() throws Exception {
		for (String sqlState : List.of("0A000", "42601", "42883")) {
			SQLException primary = new SQLException("registration failed", "HY000");
			SQLException detail = new SQLException("LISTEN is disabled", sqlState);
			primary.setNextException(detail);
			detail.setNextException(primary);
			Fixture fixture = new Fixture(primary);
			fixture.failOnRegistration = true;
			Throwable terminal = fixture.run(fixture.builder().build()).getFailure().orElseThrow();
			assertInstanceOf(UnsupportedOperationException.class, terminal);
			assertSame(primary, terminal.getCause());
			assertEquals(1, fixture.checkouts.get());
			assertEquals(1, fixture.closes.get());
			assertEquals(0, fixture.policyCalls.get());
			assertEquals(0, fixture.backoffCalls.get());
		}
	}

	@Test
	void statementCloseConnectionFailureCannotAuthorizeRetryOfUnsupportedRegistration() throws Exception {
		SQLException primary = new SQLException("LISTEN unavailable", "0A000");
		Fixture fixture = new Fixture(primary);
		fixture.failOnRegistration = true;
		fixture.registrationCloseFailure = new SQLException("statement close connection lost", "08006");
		Throwable terminal = fixture.run(fixture.builder().build()).getFailure().orElseThrow();
		assertInstanceOf(UnsupportedOperationException.class, terminal);
		assertSame(primary, terminal.getCause());
		assertArrayEquals(new Throwable[]{fixture.registrationCloseFailure}, primary.getSuppressed());
		fixture.assertFatalAttempt();
	}

	private static void assertReconnects(Fixture fixture, Throwable primary) throws Exception {
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		AtomicInteger reconciliations = new AtomicInteger();
		NotificationSupervisor supervisor = fixture.builder().reconcile(() -> {
			if (reconciliations.incrementAndGet() == 2)
				reference.get().requestStop();
		}).build();
		reference.set(supervisor);
		NotificationSupervisor.Result result = fixture.run(supervisor);
		assertTrue(result.getFailure().isEmpty());
		assertTrue(result.isStopRequested());
		assertEquals(2, fixture.checkouts.get());
		assertEquals(2, fixture.closes.get());
		assertEquals(1, fixture.aborts.get());
		assertEquals(1, fixture.policyCalls.get());
		assertEquals(1, fixture.backoffCalls.get());
		assertEquals(2, reconciliations.get());
		assertSame(primary, supervisor.snapshot().getLastFailure().orElseThrow());
	}

	@Test
	void unsupportedTimedReceiveDoesNotReachRetryPolicyOrBackoff() throws Exception {
		for (Throwable unsupported : List.of(new SQLFeatureNotSupportedException("timed receive unavailable"),
				new UnsupportedOperationException("timed receive unavailable"), new AbstractMethodError("timed receive unavailable"))) {
			Fixture fixture = new Fixture(unsupported);
			NotificationSupervisor supervisor = fixture.builder().build();
			Throwable terminal = fixture.run(supervisor).getFailure().orElseThrow();
			assertSame(fixture.observedFailure.get(), terminal);
			assertInstanceOf(NotificationReceiveUnsupportedException.class, terminal.getCause());
			assertSame(unsupported, terminal.getCause().getCause());
			fixture.assertFatalAttempt();
			supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
		}
	}

	@Test
	void cleanupErrorSuppressedUnderReceiveFailurePreventsRetry() throws Exception {
		DatabaseException primary = new DatabaseException("receive", new SQLException("connection lost", "08006"));
		AssertionError cleanup = new AssertionError("abort failed");
		Fixture fixture = new Fixture(primary);
		fixture.abortFailure = cleanup;
		Throwable terminal = fixture.run(fixture.builder().build()).getFailure().orElseThrow();
		assertSame(primary, terminal);
		assertSame(primary, fixture.observedFailure.get());
		assertArrayEquals(new Throwable[]{cleanup}, primary.getSuppressed());
		fixture.assertFatalAttempt();
	}

	@Test
	void nestedFatalCausesAndSuppressedCyclesKeepThePrimaryFailureUnchanged() throws Exception {
		for (Throwable fatal : List.of(new AssertionError("fatal"), new UnsupportedOperationException("unsupported"),
				new SQLFeatureNotSupportedException("unsupported"), new NotificationReceiveUnsupportedException("unsupported"))) {
			for (boolean suppressed : new boolean[]{false, true}) {
				RuntimeException nested = new RuntimeException("wrapper", fatal);
				DatabaseException primary = suppressed ? new DatabaseException("receive") : new DatabaseException("receive", nested);
				if (suppressed)
					primary.addSuppressed(nested);
				nested.addSuppressed(primary); // A legal exception-graph cycle must not hang classification.
				Throwable[] originalSuppressed = primary.getSuppressed();
				Fixture fixture = new Fixture(primary);
				assertSame(primary, fixture.run(fixture.builder().build()).getFailure().orElseThrow());
				assertArrayEquals(originalSuppressed, primary.getSuppressed());
				assertArrayEquals(new Throwable[]{primary}, nested.getSuppressed());
				assertSame(fatal, nested.getCause());
				fixture.assertFatalAttempt();
			}
		}
	}

	@Test
	void failedGraphInspectionStopsRetryWithoutReplacingThePrimaryFailure() throws Exception {
		for (Throwable inspectionFailure : List.of(new IllegalStateException("cause inspection"), new AssertionError("cause inspection"))) {
			RuntimeException diagnostic = new RuntimeException("diagnostic") {
				@Override public Throwable getCause() {
					if (inspectionFailure instanceof RuntimeException runtime)
						throw runtime;
					throw (Error) inspectionFailure;
				}
			};
			DatabaseException primary = new DatabaseException("receive");
			primary.addSuppressed(diagnostic);
			Fixture fixture = new Fixture(primary);
			assertSame(primary, fixture.run(fixture.builder().build()).getFailure().orElseThrow());
			assertArrayEquals(new Throwable[]{diagnostic}, primary.getSuppressed());
			fixture.assertFatalAttempt();
		}
	}

	@Test
	void deeplySuppressedFatalFailureDoesNotRequireRecursiveTraversal() throws Exception {
		DatabaseException primary = new DatabaseException("receive");
		Throwable tail = primary;
		for (int depth = 0; depth < 10_000; depth++) {
			RuntimeException next = new RuntimeException("wrapper");
			tail.addSuppressed(next);
			tail = next;
		}
		tail.addSuppressed(new AssertionError("fatal"));
		Fixture fixture = new Fixture(primary);
		assertSame(primary, fixture.run(fixture.builder().build()).getFailure().orElseThrow());
		fixture.assertFatalAttempt();
	}

	@Test
	void jdbcNextExceptionCapabilityAndFatalCausePreventRetry() throws Exception {
		for (SQLException detail : List.of(new SQLFeatureNotSupportedException("timed receive unavailable"),
				new SQLException("fatal detail", new AssertionError("fatal receive")))) {
			SQLException primary = new SQLException("receive failed", "08006");
			primary.setNextException(detail);
			detail.setNextException(primary);
			Fixture fixture = new Fixture(primary);
			Throwable terminal = fixture.run(fixture.builder().build()).getFailure().orElseThrow();
			assertSame(primary, terminal.getCause());
			assertSame(terminal, fixture.observedFailure.get());
			assertSame(detail, primary.getNextException());
			assertSame(primary, detail.getNextException());
			fixture.assertFatalAttempt();
		}
	}

	@Test
	void throwingJdbcNextExceptionAccessorPreventsRetryWithoutReplacingFailure() throws Exception {
		SQLException primary = new SQLException("receive failed", "08006") {
			@Override public SQLException getNextException() { throw new AssertionError("next exception unavailable"); }
		};
		Fixture fixture = new Fixture(primary);
		Throwable terminal = fixture.run(fixture.builder().build()).getFailure().orElseThrow();
		assertSame(primary, terminal.getCause());
		assertSame(terminal, fixture.observedFailure.get());
		fixture.assertFatalAttempt();
	}

	@Test
	void ordinaryFailureWithCycleStillReconnectsAndReconciles() throws Exception {
		SQLException sqlFailure = new SQLException("connection lost", "08006");
		SQLException detail = new SQLException("connection details");
		sqlFailure.setNextException(detail);
		detail.setNextException(sqlFailure);
		DatabaseException primary = new DatabaseException("receive", sqlFailure);
		RuntimeException diagnostic = new RuntimeException("diagnostic");
		primary.addSuppressed(diagnostic);
		diagnostic.addSuppressed(primary);
		Fixture fixture = new Fixture(primary);
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		AtomicInteger reconciliations = new AtomicInteger();
		NotificationSupervisor supervisor = fixture.builder().reconcile(() -> {
			if (reconciliations.incrementAndGet() == 2)
				reference.get().requestStop();
		}).build();
		reference.set(supervisor);
		NotificationSupervisor.Result result = fixture.run(supervisor);
		assertTrue(result.getFailure().isEmpty());
		assertTrue(result.isStopRequested());
		assertTrue(fixture.interruptedOnExit.get());
		assertEquals(2, fixture.checkouts.get());
		assertEquals(2, fixture.closes.get());
		assertEquals(1, fixture.aborts.get());
		assertEquals(1, fixture.policyCalls.get());
		assertEquals(1, fixture.backoffCalls.get());
		assertEquals(2, reconciliations.get());
		assertSame(primary, supervisor.snapshot().getLastFailure().orElseThrow());
		assertArrayEquals(new Throwable[]{diagnostic}, primary.getSuppressed());
	}

	@Test
	void stopFromRetryPolicySkipsBackoffAndPreservesFailureAndInterrupt() throws Exception {
		DatabaseException primary = new DatabaseException("receive");
		Fixture fixture = new Fixture(primary);
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		NotificationSupervisor supervisor = fixture.builder().retry(failure -> {
			fixture.policyCalls.incrementAndGet();
			assertSame(primary, failure.getException());
			reference.get().requestStop();
			return true;
		}, (attempt, failure) -> {
			fixture.backoffCalls.incrementAndGet();
			return Duration.ZERO;
		}, Duration.ofSeconds(1)).build();
		reference.set(supervisor);
		NotificationSupervisor.Result result = fixture.run(supervisor);
		assertSame(primary, result.getFailure().orElseThrow());
		assertTrue(result.isStopRequested());
		assertTrue(fixture.interruptedOnExit.get());
		assertEquals(1, fixture.checkouts.get());
		assertEquals(1, fixture.policyCalls.get());
		assertEquals(0, fixture.backoffCalls.get());
	}

	@Test
	void interruptedReceiveFailureStillSkipsPolicyAndBackoff() throws Exception {
		DatabaseException primary = new DatabaseException("receive");
		Fixture fixture = new Fixture(primary);
		fixture.interruptOnReceive = true;
		NotificationSupervisor.Result result = fixture.run(fixture.builder().build());
		assertSame(primary, result.getFailure().orElseThrow());
		assertFalse(result.isStopRequested());
		assertTrue(fixture.interruptedOnExit.get());
		fixture.assertFatalAttempt();
	}

	private static final class Fixture {
		private final Throwable receiveFailure;
		private final AtomicInteger checkouts = new AtomicInteger();
		private final AtomicInteger closes = new AtomicInteger();
		private final AtomicInteger aborts = new AtomicInteger();
		private final AtomicInteger policyCalls = new AtomicInteger();
		private final AtomicInteger backoffCalls = new AtomicInteger();
		private final AtomicReference<Throwable> observedFailure = new AtomicReference<>();
		private final AtomicBoolean interruptedOnExit = new AtomicBoolean();
		private Throwable abortFailure;
		private Throwable firstCloseFailure;
		private boolean failOnRegistration;
		private Throwable registrationCloseFailure;
		private boolean interruptOnReceive;

		private Fixture(Throwable receiveFailure) {
			this.receiveFailure = receiveFailure;
		}

		private NotificationSupervisor.Builder builder() {
			DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
					new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
						if (!method.getName().equals("getConnection"))
							throw new AssertionError("Unexpected data source method: " + method.getName());
						this.checkouts.incrementAndGet();
						return connection();
					});
			Database database = Database.withDataSource(source).databaseType(DatabaseType.POSTGRESQL)
					.metricsCollector(new MetricsCollector() {
						@Override public void didLoseNotificationConnection(DatabaseType type, java.util.UUID id, Throwable failure) {
							observedFailure.set(failure);
						}
					}).build();
			return NotificationSupervisor.withDatabase(database).channels(Set.of("fatal_failure_test")).reconcile(() -> {})
					.retry(failure -> this.policyCalls.incrementAndGet() < 2, (attempt, failure) -> {
						this.backoffCalls.incrementAndGet();
						return Duration.ZERO;
					}, Duration.ofSeconds(1));
		}

		private Connection connection() {
			return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
					new Class<?>[]{Connection.class, PGConnection.class}, (proxy, method, args) -> switch (method.getName()) {
						case "getAutoCommit" -> true;
						case "getNetworkTimeout" -> 0;
						case "setNetworkTimeout" -> null;
						case "isClosed" -> false;
						case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
						case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
						case "createStatement" -> {
							AtomicBoolean registration = new AtomicBoolean();
							yield Proxy.newProxyInstance(getClass().getClassLoader(),
									new Class<?>[]{Statement.class}, (statement, operation, parameters) -> switch (operation.getName()) {
										case "execute" -> {
											registration.set(((String) parameters[0]).startsWith("LISTEN "));
											if (this.failOnRegistration && registration.get())
												throw this.receiveFailure;
											yield false;
										}
										case "close" -> {
											if (registration.get() && this.registrationCloseFailure != null)
												throw this.registrationCloseFailure;
											yield null;
										}
										default -> throw new AssertionError("Unexpected statement method: " + operation.getName());
									});
						}
						case "getNotifications" -> {
							if (args != null && args.length > 0) {
								if (this.interruptOnReceive)
									Thread.currentThread().interrupt();
								throw this.receiveFailure;
							}
							yield new PGNotification[0];
						}
						case "abort" -> {
							this.aborts.incrementAndGet();
							if (this.abortFailure != null)
								throw this.abortFailure;
							yield null;
						}
						case "close" -> {
							if (this.closes.incrementAndGet() == 1 && this.firstCloseFailure != null)
								throw this.firstCloseFailure;
							yield null;
						}
						default -> throw new AssertionError("Unexpected connection method: " + method.getName());
					});
		}

		private NotificationSupervisor.Result run(NotificationSupervisor supervisor) throws Exception {
			AtomicReference<Thread> worker = new AtomicReference<>();
			AtomicReference<Throwable> uncaught = new AtomicReference<>();
			supervisor.start(command -> {
				Thread thread = new Thread(() -> {
					try {
						command.run();
					} finally {
						this.interruptedOnExit.set(Thread.currentThread().isInterrupted());
					}
				}, "notification-fatal-failure-test");
				thread.setDaemon(true);
				thread.setUncaughtExceptionHandler((ignored, failure) -> uncaught.set(failure));
				worker.set(thread);
				thread.start();
			});
			try {
				NotificationSupervisor.Result result = supervisor.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
				worker.get().join(5_000);
				assertFalse(worker.get().isAlive(), "worker did not terminate");
				assertNull(uncaught.get());
				return result;
			} finally {
				supervisor.requestStop();
				worker.get().join(5_000);
			}
		}

		private void assertFatalAttempt() {
			assertEquals(1, this.checkouts.get());
			assertEquals(1, this.aborts.get());
			assertEquals(1, this.closes.get());
			assertEquals(0, this.policyCalls.get());
			assertEquals(0, this.backoffCalls.get());
		}
	}
}
