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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.postgresql.PGConnection;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/** Real-server release checks for listener outages, independent polling, and ordered resource shutdown. */
@Testcontainers
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(60)
class NotificationSupervisorReleaseIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = postgres();

	private static PostgreSQLContainer postgres() {
		PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse(
				System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17"))
				.asCompatibleSubstituteFor("postgres"))
				.withDatabaseName("pyranid_release").withUsername("pyranid").withPassword("pyranid");
		// Docker can reassign an automatically published port on restart. Keep the same endpoint for existing data sources.
		try (ServerSocket availablePort = new ServerSocket(0)) {
			postgres.setPortBindings(List.of(availablePort.getLocalPort() + ":5432"));
		} catch (IOException failure) {
			throw new IllegalStateException("Cannot reserve a PostgreSQL test port", failure);
		}
		return postgres;
	}

	@Test
	void repeatedDisconnectsRecoverDurableChangesDuringBackoffAndReleaseEveryConnection() throws Exception {
		try (Fixture fixture = new Fixture()) {
			AtomicInteger expectedDuringBackoff = new AtomicInteger();
			AtomicInteger observedDuringBackoff = new AtomicInteger();
			AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
			AtomicReference<CountDownLatch> allowReconnect = new AtomicReference<>();
			NotificationSupervisor supervisor = fixture.builder().reconcile(() -> {
				int version = fixture.reconcile();
				NotificationSupervisor current = reference.get();
				if (current != null && current.snapshot().getState() == NotificationSupervisor.State.BACKING_OFF
						&& version >= expectedDuringBackoff.get())
					observedDuringBackoff.accumulateAndGet(version, Math::max);
			}).retry(failure -> { fixture.retries.incrementAndGet(); return retryable(failure); },
					(attempt, failure) -> {
						CountDownLatch gate = allowReconnect.get();
						if (gate != null) awaitUninterruptibly(gate);
						return Duration.ZERO;
					}, Duration.ofMinutes(1)).build();
			reference.set(supervisor);
			fixture.supervisor = supervisor;
			supervisor.start();
			supervisor.firstReady().toCompletableFuture().get(10, TimeUnit.SECONDS);
			int version = 0;
			for (int cycle = 0; cycle < 3; cycle++) {
				int oldPid = fixture.listenerSource.backendPid.get();
				int retryCount = fixture.retries.get();
				CountDownLatch gate = new CountDownLatch(1);
				allowReconnect.set(gate);
				try {
					assertTrue(fixture.admin.query("SELECT pg_terminate_backend(:pid)")
							.bind("pid", oldPid).fetchObject(Boolean.class).orElseThrow());
					until(() -> fixture.retries.get() > retryCount, "Listener did not detect backend termination");
					int pollingVersion = ++version;
					expectedDuringBackoff.set(pollingVersion);
					fixture.update(pollingVersion, false);
					until(() -> observedDuringBackoff.get() >= pollingVersion,
							"Independent polling did not advance durable state during reconnect backoff");
				} finally {
					gate.countDown();
					allowReconnect.set(null);
				}
				until(() -> fixture.listenerSource.backendPid.get() != oldPid
						&& supervisor.snapshot().getState() == NotificationSupervisor.State.ACTIVE,
						"Listener did not register a replacement session");
				for (int update = 0; update < 3; update++) {
					int expected = ++version;
					fixture.update(expected, update % 2 == 0);
					until(() -> fixture.observed.get() >= expected, "Committed change was not reconciled");
				}
			}
			assertEquals(12, fixture.observed.get());
			assertTrue(fixture.retries.get() >= 3);
			fixture.shutdownCleanly();
			assertTrue(fixture.listenerSource.checkouts.get() >= 4);
			fixture.assertReleased();
		}
	}

	@Test
	void databaseRestartRecoversListenerAndPollingWithoutAnotherNotification() throws Exception {
		try (Fixture fixture = new Fixture()) {
			fixture.supervisor = fixture.builder().reconcile(fixture::reconcile)
					.retry(NotificationSupervisorReleaseIT::retryable,
							RetryPolicy.Backoff.fixed(Duration.ofMillis(100)), Duration.ofMinutes(1)).build();
			fixture.supervisor.start();
			fixture.supervisor.firstReady().toCompletableFuture().get(10, TimeUnit.SECONDS);
			fixture.update(1, false);
			until(() -> fixture.observed.get() == 1, "Initial durable state was not reconciled");
			int failures = fixture.reconciliationFailures.get();
			int openingFailures = fixture.listenerSource.openFailures.get();
			boolean stopped = false;
			try {
				DockerClientFactory.instance().client().stopContainerCmd(POSTGRES.getContainerId()).withTimeout(0).exec();
				stopped = true;
				until(() -> fixture.reconciliationFailures.get() > failures,
						"Polling did not observe the real database outage");
				until(() -> fixture.listenerSource.openFailures.get() > openingFailures,
						"Listener did not attempt to reconnect during the outage");
			} finally {
				if (stopped)
					DockerClientFactory.instance().client().startContainerCmd(POSTGRES.getContainerId()).exec();
			}
			until(() -> {
				try { return fixture.admin.query("SELECT 1").fetchObject(Integer.class).orElseThrow() == 1; }
				catch (DatabaseException unavailable) { return false; }
			}, "PostgreSQL did not become queryable after restart");
			fixture.update(2, false);
			until(() -> fixture.observed.get() == 2, "Polling did not recover after server restart");
			until(() -> fixture.supervisor.snapshot().getState() == NotificationSupervisor.State.ACTIVE,
					"Listener did not become ready after server restart");
			assertTrue(fixture.supervisor.snapshot().getAttemptNumber() > 1);
			until(() -> fixture.supervisor.snapshot().getLastReconciliationFailure().isEmpty(),
					"Successful recovery did not clear the polling error");
			fixture.shutdownCleanly();
			fixture.assertReleased();
		}
	}

	@Test
	void shutdownDrainsARealBlockedReconciliationBeforeClosingApplicationResources() throws Exception {
		try (Fixture fixture = new Fixture(); Connection lockOwner = source("release-lock-owner").getConnection()) {
			long key = Integer.toUnsignedLong(fixture.table.hashCode());
			try (var statement = lockOwner.createStatement()) {
				statement.execute("SELECT pg_advisory_lock(" + key + ")");
			}
			AtomicBoolean gate = new AtomicBoolean();
			AtomicBoolean reconciliationInterrupted = new AtomicBoolean();
			fixture.supervisor = fixture.builder().reconcile(() -> fixture.reconcile(() -> {
				if (gate.get()) {
					fixture.work.transaction(() -> fixture.work
							.query("SELECT 1 FROM pg_advisory_xact_lock(:key)").bind("key", key)
							.queryTimeout(Duration.ofSeconds(10)).fetchObject(Integer.class).orElseThrow());
				}
				reconciliationInterrupted.compareAndSet(false, Thread.currentThread().isInterrupted());
			})).build();
			fixture.supervisor.start();
			fixture.supervisor.firstReady().toCompletableFuture().get(10, TimeUnit.SECONDS);
			gate.set(true);
			try {
				until(() -> fixture.admin.query("SELECT count(*) FROM pg_stat_activity WHERE application_name = :name "
						+ "AND wait_event_type = 'Lock'").bind("name", fixture.workSource.applicationName)
						.fetchObject(Long.class).orElseThrow() > 0, "Reconciliation never entered the server-side lock wait");
				fixture.supervisor.requestShutdown();
				assertFalse(fixture.supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
				assertFalse(fixture.listenerSource.ownerClosed.get());
				assertFalse(fixture.workSource.ownerClosed.get());
				assertTrue(fixture.workSource.active.get() > 0);
			} finally {
				try (var statement = lockOwner.createStatement()) {
					statement.execute("SELECT pg_advisory_unlock(" + key + ")");
				}
			}
			fixture.shutdownCleanly();
			assertFalse(reconciliationInterrupted.get(), "Shutdown interrupted application reconciliation work");
			fixture.assertReleased();
		}
	}

	@Test
	void shutdownWaitsForRealListenerConnectionCleanupToReturn() throws Exception {
		try (Fixture fixture = new Fixture()) {
			CountDownLatch closing = new CountDownLatch(1);
			CountDownLatch finishClose = new CountDownLatch(1);
			fixture.listenerSource.afterPhysicalClose.set(() -> {
				closing.countDown(); awaitUninterruptibly(finishClose);
			});
			fixture.supervisor = fixture.builder().reconcile(fixture::reconcile).build();
			fixture.supervisor.start();
			fixture.supervisor.firstReady().toCompletableFuture().get(10, TimeUnit.SECONDS);
			try {
				fixture.supervisor.requestShutdown();
				assertTrue(closing.await(10, TimeUnit.SECONDS), "Listener connection was not closed");
				assertFalse(fixture.supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
				assertFalse(fixture.listenerSource.ownerClosed.get());
				assertFalse(fixture.workSource.ownerClosed.get());
			} finally {
				finishClose.countDown();
			}
			fixture.shutdownCleanly();
			fixture.assertReleased();
		}
	}

	private static boolean retryable(NotificationSupervisor.RetryFailure failure) {
		return failure.getException().getSqlState()
				.map(state -> state.startsWith("08") || state.equals("57P01") || state.equals("57P02") || state.equals("57P03"))
				.orElse(false);
	}

	private static PGSimpleDataSource source(String applicationName) {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setURL(POSTGRES.getJdbcUrl());
		source.setUser(POSTGRES.getUsername());
		source.setPassword(POSTGRES.getPassword());
		source.setApplicationName(applicationName);
		source.setConnectTimeout(1);
		source.setSocketTimeout(2);
		return source;
	}

	private static void until(BooleanSupplier condition, String failure) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
		while (!condition.getAsBoolean()) {
			assertTrue(System.nanoTime() - deadline < 0, failure);
			Thread.sleep(10);
		}
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		try {
			while (true) {
					try { assertTrue(latch.await(20, TimeUnit.SECONDS)); return; }
				catch (InterruptedException cancellation) { interrupted = true; }
			}
		} finally {
			if (interrupted) Thread.currentThread().interrupt();
		}
	}

	private static final class Fixture implements AutoCloseable {
		private final String suffix = UUID.randomUUID().toString().replace("-", "");
		private final String table = "release_state_" + this.suffix;
		private final String channel = "release_hint_" + this.suffix;
		private final Database admin = Database.with(source("release-admin-" + this.suffix), DatabaseType.POSTGRESQL).build();
		private final TrackingSource listenerSource = new TrackingSource("release-listener-" + this.suffix);
		private final TrackingSource workSource = new TrackingSource("release-work-" + this.suffix);
		private final Database work = Database.with(this.workSource, DatabaseType.POSTGRESQL).build();
		private final AtomicInteger activeCallbacks = new AtomicInteger();
		private final AtomicInteger maximumCallbacks = new AtomicInteger();
		private final AtomicInteger observed = new AtomicInteger(-1);
		private final AtomicInteger reconciliationFailures = new AtomicInteger();
		private final AtomicInteger retries = new AtomicInteger();
		private final ConcurrentLinkedQueue<Thread> workers = new ConcurrentLinkedQueue<>();
		private NotificationSupervisor supervisor;

		private Fixture() {
			this.admin.query("CREATE TABLE " + this.table + " (version INTEGER NOT NULL)").execute();
			this.admin.query("INSERT INTO " + this.table + " VALUES (0)").execute();
		}

		private NotificationSupervisor.Builder builder() {
			return NotificationSupervisor.withDatabase(Database.with(this.listenerSource, DatabaseType.POSTGRESQL).build()).channels(Set.of(this.channel))
					.pollEvery(Duration.ofMillis(30)).onReconciliationFailure(failure -> this.reconciliationFailures.incrementAndGet())
					.threadFactory(role -> task -> {
						Thread thread = new Thread(task, "release-" + role + "-" + this.suffix);
						thread.setDaemon(true); this.workers.add(thread); return thread;
					});
		}

		private int reconcile() {
			return reconcile(() -> {});
		}

		private int reconcile(Runnable beforeQuery) {
			int callbacks = this.activeCallbacks.incrementAndGet();
			this.maximumCallbacks.accumulateAndGet(callbacks, Math::max);
			try {
				beforeQuery.run();
				int version = this.work.query("SELECT version FROM " + this.table).queryTimeout(Duration.ofSeconds(2))
						.fetchObject(Integer.class).orElseThrow();
				this.observed.set(version);
				return version;
			} finally {
				this.activeCallbacks.decrementAndGet();
			}
		}

		private void update(int version, boolean notify) {
			this.admin.transaction(() -> {
				this.admin.query("UPDATE " + this.table + " SET version = :version").bind("version", version).execute();
				if (notify) this.admin.sendNotification(this.channel);
			});
		}

		private void shutdownCleanly() throws Exception {
			this.supervisor.requestShutdown();
			assertTrue(this.supervisor.awaitShutdownAsync().toCompletableFuture()
					.get(15, TimeUnit.SECONDS).getFailure().isEmpty(), "Supervisor did not shut down cleanly");
			for (Thread worker : this.workers) {
				worker.join(5_000); assertFalse(worker.isAlive(), "Managed worker leaked: " + worker.getName());
			}
		}

		private void assertReleased() throws InterruptedException {
			assertEquals(1, this.maximumCallbacks.get(), "Reconciliation callbacks overlapped");
			assertEquals(0, this.activeCallbacks.get());
			this.listenerSource.assertReleased(); this.workSource.assertReleased();
			assertEquals(0, this.listenerSource.accessAfterOwnerClose.get());
			assertEquals(0, this.workSource.accessAfterOwnerClose.get());
			until(() -> this.admin.query("SELECT count(*) FROM pg_stat_activity WHERE application_name IN (:listener, :work)")
					.bind("listener", this.listenerSource.applicationName).bind("work", this.workSource.applicationName)
					.fetchObject(Long.class).orElseThrow() == 0, "PostgreSQL sessions remained after supervisor shutdown");
		}

		@Override public void close() throws Exception {
			try {
				if (this.supervisor != null) {
					this.supervisor.requestShutdown();
					this.supervisor.awaitShutdownAsync().toCompletableFuture().get(15, TimeUnit.SECONDS);
					for (Thread worker : this.workers) worker.join(5_000);
				}
			} finally {
				try {
					this.listenerSource.close();
				} finally {
					try {
						this.workSource.close();
					} finally {
						this.admin.query("DROP TABLE IF EXISTS " + this.table).execute();
					}
				}
			}
		}
	}

	private static final class TrackingSource implements DataSource, AutoCloseable {
		private final String applicationName;
		private final AtomicInteger checkouts = new AtomicInteger();
		private final AtomicInteger closes = new AtomicInteger();
		private final AtomicInteger active = new AtomicInteger();
		private final AtomicInteger openFailures = new AtomicInteger();
		private final AtomicInteger backendPid = new AtomicInteger();
		private final AtomicInteger accessAfterOwnerClose = new AtomicInteger();
		private final AtomicBoolean ownerClosed = new AtomicBoolean();
		private final AtomicReference<Runnable> afterPhysicalClose = new AtomicReference<>();

		private TrackingSource(String applicationName) { this.applicationName = applicationName; }

		@Override public Connection getConnection() throws SQLException {
			if (this.ownerClosed.get()) {
				this.accessAfterOwnerClose.incrementAndGet(); throw new SQLException("Application source already closed");
			}
			Connection connection;
			try { connection = source(this.applicationName).getConnection(); }
			catch (SQLException failure) { this.openFailures.incrementAndGet(); throw failure; }
			this.checkouts.incrementAndGet(); this.active.incrementAndGet();
			this.backendPid.set(connection.unwrap(PGConnection.class).getBackendPID());
			AtomicBoolean closed = new AtomicBoolean();
			return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, arguments) -> {
						if (method.getName().equals("close")) {
							if (closed.compareAndSet(false, true)) {
								try {
									connection.close();
									Runnable hook = this.afterPhysicalClose.get();
									if (hook != null) hook.run();
								} finally { this.closes.incrementAndGet(); this.active.decrementAndGet(); }
							}
							return null;
						}
						try { return method.invoke(connection, arguments); }
						catch (InvocationTargetException failure) { throw failure.getCause(); }
					});
		}

		private void assertReleased() { assertEquals(this.checkouts.get(), this.closes.get()); assertEquals(0, this.active.get()); }
		@Override public void close() { assertReleased(); this.ownerClosed.set(true); }
		@Override public Connection getConnection(String user, String password) throws SQLException { return getConnection(); }
		@Override public PrintWriter getLogWriter() { return null; }
		@Override public void setLogWriter(PrintWriter out) {}
		@Override public void setLoginTimeout(int seconds) {}
		@Override public int getLoginTimeout() { return 1; }
		@Override public Logger getParentLogger() throws SQLFeatureNotSupportedException { return Logger.getLogger("pyranid.release"); }
		@Override public <T> T unwrap(Class<T> type) throws SQLException { if (type.isInstance(this)) return type.cast(this); throw new SQLException("Not a wrapper"); }
		@Override public boolean isWrapperFor(Class<?> type) { return type.isInstance(this); }
	}
}
