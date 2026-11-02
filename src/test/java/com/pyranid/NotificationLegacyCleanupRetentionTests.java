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
import org.junit.jupiter.api.Timeout;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises cancellation promoted to a cleanup failure through the production notification-session runner. */
@Timeout(15)
class NotificationLegacyCleanupRetentionTests {
	@Test
	void cancelledRetryAndListenerCloseFailureRetainTheUnrecoveredReconciliationFailure() throws Exception {
		Fixture fixture = new Fixture(Mode.INTERRUPTED_RETRY, new SQLException("Listener close failed", "HY000"));
		NotificationSupervisor.ShutdownResult result = fixture.run();

		assertContains(result.getFailure().orElseThrow(), fixture.reconciliationFailure);
		assertContains(result.getFailure().orElseThrow(), fixture.closeFailure);
		assertInterrupted(result.getFailure().orElseThrow());
		assertSame(fixture.reconciliationFailure,
				fixture.supervisor.snapshot().getLastReconciliationFailure().orElseThrow());
		assertTrue(fixture.supervisor.snapshot().getLastSuccessfulReconciliation().isEmpty());
		assertTrue(fixture.supervisor.firstReady().toCompletableFuture().isCompletedExceptionally());
		fixture.assertStopped(2, 1);
	}

	@Test
	void closeErrorDuringCancelledRetryAlsoRetainsTheUnrecoveredFailure() throws Exception {
		AssertionError closeError = new AssertionError("Listener close failed fatally");
		Fixture fixture = new Fixture(Mode.INTERRUPTED_RETRY, closeError);
		Throwable terminal = fixture.run().getFailure().orElseThrow();

		assertContains(terminal, fixture.reconciliationFailure);
		assertContains(terminal, closeError);
		assertInterrupted(terminal);
		assertAcyclic(terminal, identitySet(), identitySet());
		fixture.assertStopped(2, 1);
	}

	@Test
	void successfulRetryClearsTheOldFailureBeforeReceiveCancellationAndCloseFailure() throws Exception {
		Fixture fixture = new Fixture(Mode.SUCCESSFUL_RETRY, new SQLException("Listener close failed", "HY000"));
		NotificationSupervisor.ShutdownResult result = fixture.run();

		Throwable terminal = result.getFailure().orElseThrow();
		assertContains(terminal, fixture.closeFailure);
		assertInterrupted(terminal);
		assertFalse(contains(terminal, fixture.reconciliationFailure), "Recovered failure was resurrected");
		assertTrue(fixture.supervisor.snapshot().getLastReconciliationFailure().isEmpty());
		assertTrue(fixture.supervisor.snapshot().getLastSuccessfulReconciliation().isPresent());
		assertFalse(fixture.supervisor.firstReady().toCompletableFuture().isCompletedExceptionally());
		fixture.assertStopped(2, 1);
	}

	@Test
	void cleanupFailureAlreadyRetainingTheOriginalDoesNotIntroduceADuplicateOrCycle() throws Exception {
		DatabaseException original = new DatabaseException("Authoritative state refresh failed");
		SQLException cleanup = new SQLException("Listener close failed", "HY000", original);
		Fixture fixture = new Fixture(Mode.INTERRUPTED_RETRY, original, cleanup);
		Throwable terminal = fixture.run().getFailure().orElseThrow();

		assertContains(terminal, original);
		assertContains(terminal, cleanup);
		assertInterrupted(terminal);
		assertEquals(1, referencesTo(terminal, original), "Original failure was retained more than once");
		assertAcyclic(terminal, identitySet(), identitySet());
		fixture.assertStopped(2, 1);
	}

	@Test
	void reusingTheSameCloseFailureAcrossAttemptsPreservesBothFailuresWithoutCycles() throws Exception {
		Fixture fixture = new Fixture(Mode.INTERRUPTED_RETRY, new SQLException("Shared close failure", "HY000"));
		fixture.failFirstClose = true;
		Throwable terminal = fixture.run().getFailure().orElseThrow();

		assertContains(terminal, fixture.reconciliationFailure);
		assertContains(terminal, fixture.closeFailure);
		assertInterrupted(terminal);
		assertEquals(1, referencesTo(terminal, fixture.reconciliationFailure));
		assertAcyclic(terminal, identitySet(), identitySet());
		fixture.assertStopped(2, 1);
	}

	@Test
	void initialReceiveCancellationAndCloseFailureDoNotInventAReconciliationFailure() throws Exception {
		Fixture fixture = new Fixture(Mode.SUCCESSFUL_INITIAL, new SQLException("Listener close failed", "HY000"));
		Throwable terminal = fixture.run().getFailure().orElseThrow();

		assertContains(terminal, fixture.closeFailure);
		assertInterrupted(terminal);
		assertFalse(contains(terminal, fixture.reconciliationFailure));
		assertTrue(fixture.supervisor.snapshot().getLastReconciliationFailure().isEmpty());
		assertTrue(fixture.supervisor.snapshot().getLastSuccessfulReconciliation().isPresent());
		fixture.assertStopped(1, 0);
	}

	@Test
	void cleanupDiagnosticsCannotConsumeTheFinalInterruptWhileRetainingTheOriginalFailure() throws Exception {
		SQLException cleanup = new SQLException("Listener close failed", "HY000") {
			@Override public Throwable getCause() {
				Thread.interrupted();
				return super.getCause();
			}
		};
		Fixture fixture = new Fixture(Mode.INTERRUPTED_RETRY, cleanup);
		Throwable terminal = fixture.run().getFailure().orElseThrow();

		assertContains(terminal, fixture.reconciliationFailure);
		assertContains(terminal, cleanup);
		assertInterrupted(terminal);
		fixture.assertStopped(2, 1);
	}

	private enum Mode { INTERRUPTED_RETRY, SUCCESSFUL_RETRY, SUCCESSFUL_INITIAL }

	private static final class Fixture {
		private final DatabaseException reconciliationFailure;
		private final Throwable closeFailure;
		private final AtomicInteger checkouts = new AtomicInteger();
		private final AtomicInteger passes = new AtomicInteger();
		private final AtomicInteger closes = new AtomicInteger();
		private final AtomicInteger policyCalls = new AtomicInteger();
		private final AtomicBoolean interruptedOnExit = new AtomicBoolean();
		private final NotificationSupervisor supervisor;
		private boolean failFirstClose;

		private Fixture(Mode mode, Throwable closeFailure) {
			this(mode, new DatabaseException("Authoritative state refresh failed"), closeFailure);
		}

		private Fixture(Mode mode, DatabaseException reconciliationFailure, Throwable closeFailure) {
			this.reconciliationFailure = reconciliationFailure;
			this.closeFailure = closeFailure;
			DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
					new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
				if (!method.getName().equals("getConnection")) throw new AssertionError(method);
				return connection(mode, this.checkouts.incrementAndGet());
			});
			Database database = Database.with(source, DatabaseType.POSTGRESQL).build();
			this.supervisor = NotificationSupervisor.withDatabase(database).channels(Set.of("changed"))
					.reconcile(() -> {
						int pass = this.passes.incrementAndGet();
						if (mode != Mode.SUCCESSFUL_INITIAL && pass == 1) throw this.reconciliationFailure;
						if (mode == Mode.INTERRUPTED_RETRY) Thread.currentThread().interrupt();
					}).retry(failure -> {
						this.policyCalls.incrementAndGet();
						return this.checkouts.get() < 3;
					}, RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)).build();
		}

		private Connection connection(Mode mode, int checkout) {
			return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
					new Class<?>[]{Connection.class, PGConnection.class}, (proxy, method, args) -> switch (method.getName()) {
				case "getAutoCommit" -> true;
				case "getNetworkTimeout" -> 0;
				case "setNetworkTimeout", "abort" -> null;
				case "isClosed" -> false;
				case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
				case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
				case "getNotifications" -> {
					// The parameterless read is registration sanitation; the timed read is actual receive.
					if (args != null && args.length > 0 && mode != Mode.INTERRUPTED_RETRY)
						Thread.currentThread().interrupt();
					yield new PGNotification[0];
				}
				case "createStatement" -> Proxy.newProxyInstance(getClass().getClassLoader(),
						new Class<?>[]{Statement.class}, (statement, operation, values) -> switch (operation.getName()) {
						case "execute" -> false;
						case "close" -> null;
						default -> throw new AssertionError(operation);
					});
				case "close" -> {
					this.closes.incrementAndGet();
					if (checkout == 2 || mode == Mode.SUCCESSFUL_INITIAL || this.failFirstClose) throw this.closeFailure;
					yield null;
				}
				default -> throw new AssertionError(method);
			});
		}

		private NotificationSupervisor.ShutdownResult run() throws Exception {
			AtomicReference<Thread> worker = new AtomicReference<>();
			try {
				this.supervisor.start(task -> {
					Thread thread = new Thread(() -> {
						try { task.run(); }
						finally { this.interruptedOnExit.set(Thread.currentThread().isInterrupted()); }
					}, "legacy-cleanup-retention-worker");
					thread.setDaemon(true);
					thread.setUncaughtExceptionHandler((threadWithFailure, failure) -> {});
					worker.set(thread);
					thread.start();
				});
				NotificationSupervisor.ShutdownResult result = this.supervisor.awaitShutdownAsync()
						.toCompletableFuture().get(5, TimeUnit.SECONDS);
				join(worker.get());
				return result;
			} finally {
				this.supervisor.requestShutdown();
				this.supervisor.awaitShutdownAsync().toCompletableFuture().get(5, TimeUnit.SECONDS);
				if (worker.get() != null) join(worker.get());
			}
		}

		private void assertStopped(int attempts, int retryDecisions) {
			assertEquals(attempts, this.checkouts.get());
			assertEquals(attempts, this.passes.get());
			assertEquals(attempts, this.closes.get());
			assertEquals(retryDecisions, this.policyCalls.get(), "Cancellation admitted another retry");
			assertTrue(this.interruptedOnExit.get(), "Cancellation flag was lost before worker exit");
		}
	}

	private static void assertInterrupted(Throwable failure) {
		assertNotEquals(0, ThrowableGraph.inspect(failure, node -> node instanceof InterruptedException ? 1 : 0, 0));
	}

	private static void assertContains(Throwable failure, Throwable expected) {
		assertTrue(contains(failure, expected), "Expected failure identity was absent from the terminal graph");
	}

	private static boolean contains(Throwable failure, Throwable expected) {
		return ThrowableGraph.inspect(failure, node -> node == expected ? 1 : 0, 0) != 0;
	}

	private static Set<Throwable> identitySet() {
		return Collections.newSetFromMap(new IdentityHashMap<>());
	}

	private static List<Throwable> edges(Throwable failure) {
		List<Throwable> edges = new ArrayList<>();
		Throwable cause = failure.getCause();
		if (cause != null) edges.add(cause);
		Collections.addAll(edges, failure.getSuppressed());
		if (failure instanceof SQLException sql) {
			SQLException next = sql.getNextException();
			if (next != null) edges.add(next);
		}
		return edges;
	}

	private static int referencesTo(Throwable root, Throwable target) {
		int count = root == target ? 1 : 0;
		Set<Throwable> seen = identitySet();
		ArrayDeque<Throwable> pending = new ArrayDeque<>();
		pending.add(root);
		while (!pending.isEmpty()) {
			Throwable next = pending.removeFirst();
			if (!seen.add(next)) continue;
			for (Throwable edge : edges(next)) {
				if (edge == target) ++count;
				pending.add(edge);
			}
		}
		return count;
	}

	private static void assertAcyclic(Throwable failure, Set<Throwable> active, Set<Throwable> complete) {
		assertFalse(active.contains(failure), "Failure aggregation introduced a cycle");
		if (complete.contains(failure)) return;
		active.add(failure);
		for (Throwable edge : edges(failure)) assertAcyclic(edge, active, complete);
		active.remove(failure);
		complete.add(failure);
	}

	private static void join(Thread worker) throws InterruptedException {
		worker.join(5_000);
		assertFalse(worker.isAlive(), "Notification worker leaked");
	}
}
