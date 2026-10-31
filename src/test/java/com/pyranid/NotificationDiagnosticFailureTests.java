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

/** Exercises diagnostic classification through the production adapter, session cleanup and supervisor. */
class NotificationDiagnosticFailureTests {
	@Test
	void unreadableSqlStateCannotEraseCancellationAtAnyGuardPhase() throws Exception {
		for (Phase phase : Phase.values()) {
			for (boolean oneShot : new boolean[]{false, true}) {
				RuntimeException diagnostic = new IllegalStateException("unreadable SQLState");
				SQLException original = unreadableState(diagnostic, oneShot);
				Fixture fixture = new Fixture(phase, original);
				Throwable terminal = fixture.run();

				assertTrue(contains(terminal, original), phase + " oneShot=" + oneShot);
				assertTrue(contains(terminal, diagnostic), phase + " oneShot=" + oneShot);
				if (phase == Phase.RESTORE) assertSame(fixture.receiveFailure, terminal.getCause());
				fixture.assertSingleFailure(true);
			}
		}
	}

	@Test
	void timeoutRestorationDiagnosticErrorCannotReplaceTheReceivePrimary() throws Exception {
		AssertionError diagnostic = new AssertionError("SQLState inspection failed");
		SQLException original = unreadableState(diagnostic, false);
		Fixture fixture = new Fixture(Phase.RESTORE, original);

		Throwable terminal = fixture.run();

		assertInstanceOf(DatabaseException.class, terminal);
		assertSame(fixture.receiveFailure, terminal.getCause());
		assertTrue(contains(terminal, original));
		assertTrue(contains(terminal, diagnostic));
		fixture.assertSingleFailure(true);
	}

	@Test
	void unreadableConnectionHealthRetainsBothOperationAndDiagnosticFailures() throws Exception {
		for (Phase phase : Phase.values()) {
			for (String kind : List.of("runtime", "error", "sql")) {
				Throwable diagnostic = switch (kind) {
					case "runtime" -> new IllegalStateException("health inspection cancelled", new InterruptedException("cancelled"));
					case "error" -> new AssertionError("health inspection failed");
					default -> new SQLException("health inspection cancelled", "HY000", new InterruptedException("cancelled"));
				};
				SQLException original = new SQLException("guard failure", "HY000");
				Fixture fixture = new Fixture(phase, original);
				fixture.closedCheckFailure = diagnostic;

				Throwable terminal = fixture.run();

				assertTrue(contains(terminal, original), phase + " " + kind);
				assertTrue(contains(terminal, diagnostic), phase + " " + kind);
				if (phase == Phase.RESTORE) assertSame(fixture.receiveFailure, terminal.getCause());
				fixture.assertSingleFailure(!kind.equals("error"));
			}
		}
	}

	@Test
	void aSharedReceiveAndRestorationFailureNeverSelfSuppresses() throws Exception {
		SQLException shared = new SQLException("cancelled receive and restore", "57014", new InterruptedException("cancelled"));
		Fixture fixture = new Fixture(Phase.RESTORE, shared);
		fixture.receiveFailure = shared;

		Throwable terminal = fixture.run();

		assertSame(shared, terminal.getCause());
		assertArrayEquals(new Throwable[0], shared.getSuppressed());
		fixture.assertSingleFailure(true);
	}

	@Test
	void selfThrowingAndCyclicDiagnosticsKeepTheOriginalFailure() throws Exception {
		for (boolean self : new boolean[]{false, true}) {
			AtomicReference<SQLException> reference = new AtomicReference<>();
			RuntimeException diagnostic = new IllegalStateException("cyclic diagnostic");
			SQLException original = new SQLException("cancelled guard", "57014", new InterruptedException("cancelled")) {
				@Override public String getSQLState() {
					if (self) throw unchecked(reference.get());
					throw diagnostic;
				}
			};
			reference.set(original);
			if (!self) diagnostic.addSuppressed(original);
			Fixture fixture = new Fixture(Phase.READ, original);

			Throwable terminal = fixture.run();

			assertTrue(contains(terminal, original));
			if (self) assertArrayEquals(new Throwable[0], original.getSuppressed());
			else assertTrue(contains(terminal, diagnostic));
			fixture.assertSingleFailure(true);
		}
	}

	@Test
	void readableCapabilityReportsRetainTheirExistingTerminalContract() throws Exception {
		for (Phase phase : Phase.values()) {
			SQLException original = new SQLException("cancelled optional guard", "57014", new InterruptedException("cancelled"));
			Fixture fixture = new Fixture(phase, original);

			Throwable terminal = fixture.run();

			assertTrue(contains(terminal, original));
			if (phase == Phase.UNWRAP) assertInstanceOf(UnsupportedOperationException.class, terminal);
			else assertInstanceOf(DatabaseException.class, terminal);
			if (phase == Phase.RESTORE) assertSame(fixture.receiveFailure, terminal.getCause());
			// Setup unwrapping is already terminal as an unsupported capability; its flag contract is unchanged.
			fixture.assertSingleFailure(phase != Phase.UNWRAP);
		}
	}

	@Test
	void ordinaryConnectionFailuresStillReachTheConfiguredRetryPolicy() throws Exception {
		for (Phase phase : Phase.values()) {
			SQLException original = new SQLException("connection lost", "08006");
			Fixture fixture = new Fixture(phase, original);

			Throwable terminal = fixture.run();

			assertTrue(contains(terminal, original));
			if (phase == Phase.RESTORE) assertSame(fixture.receiveFailure, terminal.getCause());
			fixture.assertRetried();
		}
	}

	@Test
	void unsupportedConnectionDisposalDoesNotDisableAnOrdinaryReconnect() throws Exception {
		Fixture fixture = new Fixture(Phase.RESTORE, new SQLException("connection lost", "08006"));
		SQLFeatureNotSupportedException cleanup = new SQLFeatureNotSupportedException("abort unavailable", "0A000");
		fixture.abortFailure = cleanup;

		Throwable terminal = fixture.run();

		assertTrue(contains(terminal, cleanup));
		assertSame(fixture.receiveFailure, terminal.getCause());
		fixture.assertRetried();
	}

	private static SQLException unreadableState(Throwable diagnostic, boolean oneShot) {
		AtomicInteger reads = new AtomicInteger();
		return new SQLException("cancelled guard", "57014", new InterruptedException("cancelled")) {
			@Override public String getSQLState() {
				if (!oneShot || reads.incrementAndGet() == 1) throw unchecked(diagnostic);
				return super.getSQLState();
			}
		};
	}

	private static RuntimeException unchecked(Throwable failure) {
		NotificationDiagnosticFailureTests.<RuntimeException>raise(failure);
		throw new AssertionError("unreachable");
	}

	@SuppressWarnings("unchecked")
	private static <T extends Throwable> void raise(Throwable failure) throws T { throw (T) failure; }

	private static boolean contains(Throwable failure, Throwable expected) {
		return ThrowableGraph.inspect(failure, node -> node == expected ? 1 : 0, 0) != 0;
	}

	private enum Phase { UNWRAP, READ, NEUTRALIZE, RESTORE }

	private static final class Fixture {
		private final Phase phase;
		private final SQLException guardFailure;
		private Throwable receiveFailure = new SQLException("primary receive failure", "08006");
		private Throwable closedCheckFailure, abortFailure;
		private final AtomicInteger checkouts = new AtomicInteger(), closes = new AtomicInteger(), aborts = new AtomicInteger();
		private final AtomicInteger policyCalls = new AtomicInteger(), backoffCalls = new AtomicInteger();
		private final AtomicInteger statementsCreated = new AtomicInteger(), statementsClosed = new AtomicInteger();
		private final AtomicBoolean interruptedOnExit = new AtomicBoolean(), interruptedDuringCleanup = new AtomicBoolean();

		private Fixture(Phase phase, SQLException guardFailure) {
			this.phase = phase;
			this.guardFailure = guardFailure;
		}

		private NotificationSupervisor.Builder builder() {
			DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> {
						if (!method.getName().equals("getConnection")) throw new AssertionError("Unexpected source method: " + method.getName());
						this.checkouts.incrementAndGet();
						return connection();
					});
			Database database = Database.withDataSource(source).databaseType(DatabaseType.POSTGRESQL).build();
			return NotificationSupervisor.withDatabase(database).channels(Set.of("diagnostic_failure_test")).reconcile(() -> {})
					.retry(failure -> this.policyCalls.incrementAndGet() < 2, (attempt, failure) -> {
						this.backoffCalls.incrementAndGet();
						return Duration.ZERO;
					}, Duration.ofSeconds(1));
		}

		private Connection connection() {
			AtomicInteger timeoutReads = new AtomicInteger();
			AtomicBoolean timedGuard = new AtomicBoolean(), timedReceive = new AtomicBoolean();
			return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class, PGConnection.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getAutoCommit" -> true;
						case "getNetworkTimeout" -> {
							timedGuard.set(timeoutReads.incrementAndGet() > 1);
							if (timedGuard.get() && this.phase == Phase.READ) throw this.guardFailure;
							yield this.phase == Phase.UNWRAP || this.phase == Phase.READ ? 0 : 731;
						}
						case "setNetworkTimeout" -> {
							if (timedGuard.get() && this.phase == Phase.NEUTRALIZE && (int) args[1] == 0) throw this.guardFailure;
							if (timedReceive.get() && this.phase == Phase.RESTORE && (int) args[1] > 0) throw this.guardFailure;
							yield null;
						}
						case "isClosed" -> {
							if (this.closedCheckFailure != null) throw this.closedCheckFailure;
							yield false;
						}
						case "isWrapperFor" -> {
							if (this.phase == Phase.UNWRAP) throw this.guardFailure;
							yield ((Class<?>) args[0]).isInstance(proxy);
						}
						case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
						case "createStatement" -> {
							this.statementsCreated.incrementAndGet();
							yield Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Statement.class},
									(statement, operation, parameters) -> switch (operation.getName()) {
										case "execute" -> false;
										case "close" -> { this.statementsClosed.incrementAndGet(); yield null; }
										default -> throw new AssertionError("Unexpected statement method: " + operation.getName());
									});
						}
						case "getNotifications" -> {
							if (args != null && args.length > 0) {
								timedReceive.set(true);
								throw this.receiveFailure;
							}
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
							yield null;
						}
						default -> throw new AssertionError("Unexpected connection method: " + method.getName());
					});
		}

		private Throwable run() throws Exception {
			NotificationSupervisor supervisor = builder().build();
			AtomicReference<Thread> worker = new AtomicReference<>();
			AtomicReference<Throwable> uncaught = new AtomicReference<>();
			supervisor.start(command -> {
				Thread thread = new Thread(() -> {
					try { command.run(); }
					finally { this.interruptedOnExit.set(Thread.currentThread().isInterrupted()); }
				}, "notification-diagnostic-test");
				thread.setDaemon(true);
				thread.setUncaughtExceptionHandler((ignored, failure) -> uncaught.set(failure));
				worker.set(thread);
				thread.start();
			});
			try {
				NotificationSupervisor.Result result = supervisor.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
				worker.get().join(5_000);
				assertFalse(worker.get().isAlive(), "Worker must exit after session cleanup");
				Throwable failure = result.getFailure().orElseThrow();
				if (failure instanceof Error) assertSame(failure, uncaught.get());
				else assertNull(uncaught.get());
				assertSame(failure, supervisor.snapshot().getLastFailure().orElseThrow());
				return failure;
			} finally {
				supervisor.requestStop();
				worker.get().join(5_000);
				assertFalse(worker.get().isAlive(), "Worker leaked from test");
			}
		}

		private void assertSingleFailure(boolean interrupted) {
			assertEquals(1, this.checkouts.get());
			assertEquals(1, this.closes.get());
			assertEquals(1, this.aborts.get());
			assertEquals(0, this.policyCalls.get());
			assertEquals(0, this.backoffCalls.get());
			assertEquals(interrupted, this.interruptedOnExit.get());
			assertFalse(this.interruptedDuringCleanup.get(), "Interruption must be restored after disposal");
			assertEquals(this.statementsCreated.get(), this.statementsClosed.get());
			assertEquals(this.phase == Phase.UNWRAP ? 0 : 2, this.statementsClosed.get());
		}

		private void assertRetried() {
			assertEquals(2, this.checkouts.get());
			assertEquals(2, this.closes.get());
			assertEquals(2, this.aborts.get());
			assertEquals(2, this.policyCalls.get());
			assertEquals(1, this.backoffCalls.get());
			assertFalse(this.interruptedOnExit.get());
			assertFalse(this.interruptedDuringCleanup.get());
			assertEquals(this.statementsCreated.get(), this.statementsClosed.get());
		}
	}
}
