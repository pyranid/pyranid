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

/** Uses the production PostgreSQL adapter and session cleanup, including the worker's final interrupt state. */
class NotificationCancellationFailureTests {
	@Test
	void receiveCancellationInEveryFailureEdgePreventsReconnect() throws Exception {
		for (String edge : List.of("cause", "next", "suppressed", "cycle", "unreadableCause")) {
			SQLException original = cancellation(edge);
			Fixture fixture = new Fixture(original);
			NotificationSupervisor supervisor = fixture.builder().build();
			Throwable terminal = fixture.run(supervisor).getFailure().orElseThrow();

			assertTrue(contains(terminal, original), edge);
			assertSame(fixture.observedFailure.get(), terminal, edge);
			assertSame(terminal, supervisor.snapshot().getLastFailure().orElseThrow(), edge);
			fixture.assertCancelled(1, 1);
		}
	}

	@Test
	void connectionCleanupCancellationPreventsReconnectAndPreservesReceiveFailure() throws Exception {
		for (String operation : List.of("abort", "close")) {
			for (String edge : List.of("cause", "next", "suppressed", "cycle")) {
				DatabaseException primary = new DatabaseException("receive failed", new SQLException("lost", "08006"));
				SQLException cleanup = cancellation(edge);
				Fixture fixture = new Fixture(primary);
				if (operation.equals("abort")) fixture.abortFailure = cleanup;
				else fixture.closeFailure = cleanup;
				Throwable terminal = fixture.run(fixture.builder().build()).getFailure().orElseThrow();

				assertSame(primary, terminal, operation + " " + edge);
				assertTrue(contains(terminal, cleanup), operation + " " + edge);
				fixture.assertCancelled(1, 1);
			}
		}
	}

	@Test
	void setupCancellationPreventsAnotherCheckoutAfterCleanup() throws Exception {
		for (String phase : List.of("checkout", "registration", "initialDrain")) {
			for (String edge : List.of("cause", "next", "suppressed")) {
				SQLException original = cancellation(edge);
				Fixture fixture = new Fixture(original);
				fixture.setupPhase = phase;
				NotificationSupervisor supervisor = fixture.builder().build();
				Throwable terminal = fixture.run(supervisor).getFailure().orElseThrow();

				assertTrue(contains(terminal, original), phase + " " + edge);
				assertTrue(supervisor.firstReady().toCompletableFuture().isCompletedExceptionally());
				assertEquals(0, fixture.reconciliations.get());
				fixture.assertCancelled(phase.equals("checkout") ? 0 : 1, phase.equals("initialDrain") ? 1 : 0);
			}
		}
	}

	@Test
	void wrappedReconciliationCancellationStopsAfterHealthySessionCleanup() throws Exception {
		DatabaseException original = new DatabaseException("cancelled reconciliation", cancellation("cause"));
		Fixture fixture = new Fixture(new SQLException("unused receive", "08006"));
		NotificationSupervisor supervisor = fixture.builder().reconcile(() -> { throw original; }).build();
		Throwable terminal = fixture.run(supervisor).getFailure().orElseThrow();

		assertSame(original, terminal);
		fixture.assertCancelled(1, 0);
	}

	@Test
	void alreadyPendingInterruptionStillSkipsPolicyAndBackoff() throws Exception {
		DatabaseException original = new DatabaseException("receive failed");
		Fixture fixture = new Fixture(original);
		fixture.interruptOnReceive = true;

		assertSame(original, fixture.run(fixture.builder().build()).getFailure().orElseThrow());
		fixture.assertCancelled(1, 1);
	}

	@Test
	void checkedReceiveInterruptionKeepsItsCleanTerminationContract() throws Exception {
		Fixture fixture = new Fixture(new SQLException("unused receive", "08006"));
		fixture.interruptOnReceive = true;
		fixture.emptyReceive = true;
		NotificationSupervisor.Result result = fixture.run(fixture.builder().build());

		assertTrue(result.getFailure().isEmpty());
		assertFalse(result.isStopRequested());
		fixture.assertCancelled(1, 0);
	}

	@Test
	void ordinaryConnectionFailuresAndPlainCancellationSqlStateRemainPolicyControlled() throws Exception {
		for (String state : List.of("08006", "57014")) {
			DatabaseException primary = new DatabaseException("receive failed", new SQLException("JDBC failure", state));
			Fixture fixture = new Fixture(primary);
			assertSame(primary, fixture.run(fixture.builder().build()).getFailure().orElseThrow());
			fixture.assertRetried();
		}
	}

	@Test
	void unsupportedConnectionCleanupWithoutInterruptionStillAllowsReconnect() throws Exception {
		for (String operation : List.of("abort", "close")) {
			DatabaseException primary = new DatabaseException("receive failed", new SQLException("lost", "08006"));
			Fixture fixture = new Fixture(primary);
			SQLFeatureNotSupportedException cleanup = new SQLFeatureNotSupportedException("cleanup unavailable", "0A000");
			if (operation.equals("abort")) fixture.abortFailure = cleanup;
			else fixture.closeFailure = cleanup;
			assertSame(primary, fixture.run(fixture.builder().build()).getFailure().orElseThrow());
			assertTrue(contains(primary, cleanup));
			fixture.assertRetried();
		}
	}

	private static boolean contains(Throwable failure, Throwable expected) {
		return ThrowableGraph.inspect(failure, node -> node == expected ? 1 : 0, 0) != 0;
	}

	private static SQLException cancellation(String edge) {
		InterruptedException interruption = new InterruptedException("cancelled listener");
		SQLException cancelled = new SQLException("JDBC operation cancelled", "57014", interruption);
		if (edge.equals("cause")) return cancelled;
		SQLException head = edge.equals("unreadableCause") ? new SQLException("driver wrapper", "HY000") {
			@Override public Throwable getCause() { throw new IllegalStateException("unreadable cause"); }
		} : new SQLException("driver wrapper", "HY000");
		if (edge.equals("next") || edge.equals("cycle")) head.setNextException(cancelled);
		else head.addSuppressed(cancelled);
		if (edge.equals("cycle")) cancelled.setNextException(head);
		return head;
	}

	private static final class Fixture {
		private final Throwable receiveFailure;
		private final AtomicInteger checkouts = new AtomicInteger(), closes = new AtomicInteger(), aborts = new AtomicInteger();
		private final AtomicInteger policyCalls = new AtomicInteger(), backoffCalls = new AtomicInteger();
		private final AtomicInteger reconciliations = new AtomicInteger();
		private final AtomicReference<Throwable> observedFailure = new AtomicReference<>();
		private final AtomicBoolean interruptedOnExit = new AtomicBoolean(), interruptedDuringCleanup = new AtomicBoolean();
		private Throwable abortFailure, closeFailure;
		private String setupPhase;
		private boolean interruptOnReceive, emptyReceive;

		private Fixture(Throwable receiveFailure) { this.receiveFailure = receiveFailure; }

		private NotificationSupervisor.Builder builder() {
			DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> {
						if (!method.getName().equals("getConnection")) throw new AssertionError("Unexpected source method: " + method.getName());
						this.checkouts.incrementAndGet();
						if ("checkout".equals(this.setupPhase)) throw this.receiveFailure;
						return connection();
					});
			Database database = Database.withDataSource(source).databaseType(DatabaseType.POSTGRESQL)
					.metricsCollector(new MetricsCollector() {
						@Override public void didLoseNotificationConnection(DatabaseType type, java.util.UUID id, Throwable failure) {
							observedFailure.set(failure);
						}
					}).build();
			return NotificationSupervisor.withDatabase(database).channels(Set.of("cancellation_failure_test"))
					.reconcile(this.reconciliations::incrementAndGet)
					.retry(failure -> this.policyCalls.incrementAndGet() < 2, (attempt, failure) -> {
						this.backoffCalls.incrementAndGet();
						return Duration.ZERO;
					}, Duration.ofSeconds(1));
		}

		private Connection connection() {
			AtomicInteger drains = new AtomicInteger();
			return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class, PGConnection.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getAutoCommit" -> true;
						case "getNetworkTimeout" -> 0;
						case "setNetworkTimeout" -> null;
						case "isClosed" -> false;
						case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
						case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
						case "createStatement" -> Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Statement.class},
								(statement, operation, parameters) -> switch (operation.getName()) {
									case "execute" -> {
										if ("registration".equals(this.setupPhase) && ((String) parameters[0]).startsWith("LISTEN "))
											throw this.receiveFailure;
										yield false;
									}
									case "close" -> null;
									default -> throw new AssertionError("Unexpected statement method: " + operation.getName());
								});
						case "getNotifications" -> {
							if (args != null && args.length > 0) {
								if (this.interruptOnReceive) Thread.currentThread().interrupt();
								if (this.emptyReceive) yield new PGNotification[0];
								throw this.receiveFailure;
							}
							if (drains.incrementAndGet() == 1 && "initialDrain".equals(this.setupPhase)) throw this.receiveFailure;
							yield new PGNotification[0];
						}
						case "abort" -> {
							this.interruptedDuringCleanup.compareAndSet(false, Thread.currentThread().isInterrupted());
							this.aborts.incrementAndGet();
							if (this.abortFailure != null) throw this.abortFailure;
							yield null;
						}
						case "close" -> {
							this.interruptedDuringCleanup.compareAndSet(false, Thread.currentThread().isInterrupted());
							this.closes.incrementAndGet();
							if (this.closeFailure != null) throw this.closeFailure;
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
					try { command.run(); }
					finally { this.interruptedOnExit.set(Thread.currentThread().isInterrupted()); }
				}, "notification-cancellation-test");
				thread.setDaemon(true);
				thread.setUncaughtExceptionHandler((ignored, failure) -> uncaught.set(failure));
				worker.set(thread);
				thread.start();
			});
			try {
				NotificationSupervisor.Result result = supervisor.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
				worker.get().join(5_000);
				assertFalse(worker.get().isAlive(), "Worker must exit after session cleanup");
				assertNull(uncaught.get());
				return result;
			} finally {
				supervisor.requestStop();
				worker.get().join(5_000);
				assertFalse(worker.get().isAlive(), "Worker leaked from test");
			}
		}

		private void assertCancelled(int expectedCloses, int expectedAborts) {
			assertEquals(1, this.checkouts.get());
			assertEquals(expectedCloses, this.closes.get());
			assertEquals(expectedAborts, this.aborts.get());
			assertEquals(0, this.policyCalls.get());
			assertEquals(0, this.backoffCalls.get());
			assertTrue(this.interruptedOnExit.get());
			assertFalse(this.interruptedDuringCleanup.get(), "Interruption must be restored after cleanup");
		}

		private void assertRetried() {
			assertEquals(2, this.checkouts.get());
			assertEquals(2, this.closes.get());
			assertEquals(2, this.aborts.get());
			assertEquals(2, this.policyCalls.get());
			assertEquals(1, this.backoffCalls.get());
			assertFalse(this.interruptedOnExit.get());
			assertFalse(this.interruptedDuringCleanup.get());
		}
	}
}
