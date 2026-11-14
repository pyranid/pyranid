/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies the notification guide's idle-session configuration and explicit reconnect policy. */
@Testcontainers
class PostgreSqlIdleSessionTimeoutIT {
	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_idle_listener").withUsername("pyranid").withPassword("pyranid")
			.withCommand("postgres", "-c", "idle_session_timeout=2s");

	@Test
	@Timeout(value = 30, unit = TimeUnit.SECONDS)
	void documentedPolicyReconnectsAfterIdleTimeoutDespiteIndependentPolling() throws Exception {
		PGSimpleDataSource unprotectedSource = source(null);
		PGSimpleDataSource protectedSource = source("-c idle_session_timeout=0");
		AtomicInteger checkouts = new AtomicInteger();
		PGSimpleDataSource listenerSource = new PGSimpleDataSource() {
			@Override public Connection getConnection() throws SQLException {
				// The first listener inherits the server timeout; the replacement disables it.
				return (checkouts.incrementAndGet() == 1 ? unprotectedSource : protectedSource).getConnection();
			}
		};
		Database queries = database(protectedSource);
		AtomicInteger refreshes = new AtomicInteger();
		AtomicInteger refreshesAtFailure = new AtomicInteger();
		AtomicReference<NotificationSupervisor.RetryFailure> idleFailure = new AtomicReference<>();
		AtomicReference<NotificationSupervisor> supervisorReference = new AtomicReference<>();
		CountDownLatch recovered = new CountDownLatch(1);
		Predicate<NotificationSupervisor.RetryFailure> policy = documentedPolicy();
		NotificationSupervisor supervisor = NotificationSupervisor.withDatabase(database(listenerSource))
				.channels(Set.of(channel())).pollEvery(Duration.ofMillis(100)).reconcile(() -> {
			assertEquals(1, queries.query("SELECT 1").queryTimeout(Duration.ofSeconds(5)).fetchObject(Integer.class).orElseThrow());
			refreshes.incrementAndGet();
			if (checkouts.get() == 2 && supervisorReference.get().snapshot().getState() == NotificationSupervisor.State.ACTIVE)
				recovered.countDown();
		}).retry(failure -> {
			idleFailure.set(failure);
			refreshesAtFailure.set(refreshes.get());
			return policy.test(failure);
		}, RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)).build();
		supervisorReference.set(supervisor);
		try {
			supervisor.start();
			supervisor.firstReady().toCompletableFuture().get(10, TimeUnit.SECONDS);
			assertTrue(recovered.await(10, TimeUnit.SECONDS), "Idle-session termination did not reconnect and reconcile");
			assertNotNull(idleFailure.get());
			assertEquals("57P05", idleFailure.get().getException().getSqlState().orElseThrow());
			assertEquals(NotificationSupervisor.FailurePhase.RECEIVE, idleFailure.get().getPhase());
			assertTrue(refreshesAtFailure.get() > 1, "Separate query polls should run before the listener expires");
			assertTrue(refreshes.get() > refreshesAtFailure.get(), "Replacement registration must reconcile again");
			assertEquals(2L, supervisor.snapshot().getAttemptNumber());
			assertEquals(2, checkouts.get());
		} finally {
			supervisor.requestShutdown();
			assertTrue(supervisor.awaitShutdownAsync().toCompletableFuture().get(10, TimeUnit.SECONDS).getFailure().isEmpty());
		}

		// Adding 57P05 does not turn the recipe into a general retry-on-database-error policy.
		assertOpenFailureIsNotRetried("57014");
		assertOpenFailureIsNotRetried("45000");
		assertReconciliationFailureIsNotRetried(protectedSource);
	}

	@Test
	@Timeout(value = 30, unit = TimeUnit.SECONDS)
	void listenerStartupOptionsOverrideServerIdleTimeoutAndPreserveAQuietSession() throws Exception {
		assertEquals("2s", database(source(null)).query("SHOW idle_session_timeout").fetchObject(String.class).orElseThrow());
		AtomicInteger checkouts = new AtomicInteger();
		AtomicReference<Connection> physicalConnection = new AtomicReference<>();
		PGSimpleDataSource delegate = source("-c idle_session_timeout=0");
		PGSimpleDataSource listenerSource = new PGSimpleDataSource() {
			@Override public Connection getConnection() throws SQLException {
				checkouts.incrementAndGet();
				Connection connection = delegate.getConnection();
				physicalConnection.set(connection);
				return connection;
			}
		};
		database(listenerSource).withNotificationSession(channel(), session -> {
			try (Statement statement = physicalConnection.get().createStatement();
				 ResultSet rows = statement.executeQuery("SHOW idle_session_timeout")) {
				assertTrue(rows.next());
				assertEquals("0", rows.getString(1));
			} catch (SQLException failure) {
				throw new DatabaseException(failure);
			}
			// This exceeds the server's default. Receive waits themselves send no keepalive SQL.
			long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
			long remaining;
			while ((remaining = deadline - System.nanoTime()) > 0)
				assertTrue(session.awaitNotifications(Duration.ofNanos(remaining)).isEmpty());
			try (Statement statement = physicalConnection.get().createStatement(); ResultSet rows = statement.executeQuery("SELECT 1")) {
				assertTrue(rows.next());
				assertEquals(1, rows.getInt(1));
			} catch (SQLException failure) {
				throw new DatabaseException(failure);
			}
		});
		assertEquals(1, checkouts.get(), "A direct notification session must preserve the same listener connection");
	}

	private static void assertOpenFailureIsNotRetried(String sqlState) throws Exception {
		AtomicInteger checkouts = new AtomicInteger();
		PGSimpleDataSource unavailableSource = new PGSimpleDataSource() {
			@Override public Connection getConnection() throws SQLException {
				checkouts.incrementAndGet();
				throw new SQLException("non-disconnect failure", sqlState);
			}
		};
		NotificationSupervisor supervisor = NotificationSupervisor.withDatabase(database(unavailableSource))
				.channels(Set.of(channel())).reconcile(() -> fail("Failed acquisition must not reconcile"))
				.retry(documentedPolicy(), RetryPolicy.Backoff.fixed(Duration.ofMillis(100)), Duration.ofMinutes(1)).build();
		try {
			supervisor.start();
			assertThrows(ExecutionException.class, () -> supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS));
			DatabaseException failure = assertInstanceOf(DatabaseException.class,
					supervisor.awaitShutdownAsync().toCompletableFuture().get(5, TimeUnit.SECONDS).getFailure().orElseThrow());
			assertEquals(sqlState, failure.getSqlState().orElseThrow());
			assertEquals(1, checkouts.get());
		} finally {
			supervisor.requestShutdown();
			supervisor.awaitShutdownAsync().toCompletableFuture().get(5, TimeUnit.SECONDS);
		}
	}

	private static void assertReconciliationFailureIsNotRetried(PGSimpleDataSource source) throws Exception {
		AtomicInteger refreshes = new AtomicInteger();
		AtomicReference<NotificationSupervisor.FailurePhase> failedPhase = new AtomicReference<>();
		NotificationSupervisor supervisor = NotificationSupervisor.withDatabase(database(source))
				.channels(Set.of(channel())).reconcile(() -> {
			refreshes.incrementAndGet();
			throw new DatabaseException(new SQLException("reconciliation failure", "57P05"));
		}).retry(failure -> {
			failedPhase.set(failure.getPhase());
			return documentedPolicy().test(failure);
		}, RetryPolicy.Backoff.fixed(Duration.ofMillis(100)), Duration.ofMinutes(1)).build();
		try {
			supervisor.start();
			assertThrows(ExecutionException.class, () -> supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS));
			assertTrue(supervisor.awaitShutdownAsync().toCompletableFuture().get(5, TimeUnit.SECONDS).getFailure().isPresent());
			assertEquals(NotificationSupervisor.FailurePhase.RECONCILE, failedPhase.get());
			assertEquals(1, refreshes.get());
		} finally {
			supervisor.requestShutdown();
			supervisor.awaitShutdownAsync().toCompletableFuture().get(5, TimeUnit.SECONDS);
		}
	}

	private static Predicate<NotificationSupervisor.RetryFailure> documentedPolicy() {
		return failure -> (failure.getPhase() == NotificationSupervisor.FailurePhase.OPEN
				|| failure.getPhase() == NotificationSupervisor.FailurePhase.RECEIVE)
				&& failure.getException().getSqlState()
				.map(state -> state.startsWith("08")
						|| Set.of("57P01", "57P02", "57P03", "57P05").contains(state))
				.orElse(false);
	}

	private static PGSimpleDataSource source(String options) {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		if (options != null) source.setOptions(options);
		return source;
	}

	private static Database database(PGSimpleDataSource source) {
		return Database.with(source, DatabaseType.POSTGRESQL).build();
	}

	private static String channel() {
		return "idle_listener_" + UUID.randomUUID().toString().replace("-", "");
	}
}
