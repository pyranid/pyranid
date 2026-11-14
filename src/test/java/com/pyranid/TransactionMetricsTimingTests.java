/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TransactionMetricsTimingTests {
	@Test
	void successfulCommitMeasuresCompletionAttemptSeparatelyFromUserWork() {
		verifyTiming("commit", false);
	}

	@Test
	void failedCommitMeasuresCompletionAttemptSeparatelyFromUserWork() {
		verifyTiming("commit", true);
	}

	@Test
	void successfulRollbackMeasuresCompletionAttemptSeparatelyFromUserWork() {
		verifyTiming("rollback", false);
	}

	@Test
	void failedRollbackMeasuresCompletionAttemptSeparatelyFromUserWork() {
		verifyTiming("rollback", true);
	}

	private static void verifyTiming(String operation, boolean fail) {
		Fixture fixture = new Fixture(operation, fail);
		if (fail) assertThrows(DatabaseException.class, fixture::run);
		else assertDoesNotThrow(fixture::run);

		String eventName = operation + (fail ? "Failure" : "Success");
		List<Event> matches = fixture.collector.events.stream().filter(event -> event.name.equals(eventName)).toList();
		assertEquals(1, matches.size(), "Expected one callback for the selected completion attempt");
		Event event = matches.get(0);
		assertNotNull(fixture.jdbcDuration);
		assertNotNull(fixture.bodyDuration);
		assertNotNull(fixture.collector.logicalDuration);
		assertTrue(fixture.bodyDuration.compareTo(Duration.ZERO) > 0, "The transaction performed measured user work");
		assertTrue(event.duration.compareTo(fixture.jdbcDuration) >= 0,
				"Completion timing must contain the raw JDBC call's interval");
		Duration afterBodyUntilCallback = Duration.ofNanos(event.observedAt - fixture.bodyFinishedAt);
		assertTrue(event.duration.compareTo(afterBodyUntilCallback) <= 0,
				"Completion timing must fit entirely after user work and before its metric callback");
		// These intervals are disjoint inside the logical closure. An elapsed-since-begin
		// physical timer would count transaction work twice rather than describe completion latency.
		assertTrue(fixture.collector.logicalDuration.compareTo(fixture.bodyDuration.plus(event.duration)) >= 0,
				"Logical timing must contain both user work and the separate completion attempt");
		assertSame(fixture.transaction, event.transaction);
		assertSame(fixture.transaction, fixture.collector.logicalTransaction);
		if (fail) assertNotNull(event.failure);
		else assertNull(event.failure);
	}

	private record Event(String name, Transaction transaction, Duration duration, Throwable failure, long observedAt) {}

	private static final class Collector implements MetricsCollector {
		private final List<Event> events = new ArrayList<>();
		private Duration logicalDuration;
		private Transaction logicalTransaction;

		@Override public void didCommitPhysicalTransaction(Transaction transaction, DatabaseType type, Duration duration) {
			this.events.add(new Event("commitSuccess", transaction, duration, null, System.nanoTime()));
		}
		@Override public void didFailToCommitPhysicalTransaction(Transaction transaction, DatabaseType type, Duration duration, Throwable failure) {
			this.events.add(new Event("commitFailure", transaction, duration, failure, System.nanoTime()));
		}
		@Override public void didRollbackPhysicalTransaction(Transaction transaction, DatabaseType type, Duration duration) {
			this.events.add(new Event("rollbackSuccess", transaction, duration, null, System.nanoTime()));
		}
		@Override public void didFailToRollbackPhysicalTransaction(Transaction transaction, DatabaseType type, Duration duration, Throwable failure) {
			this.events.add(new Event("rollbackFailure", transaction, duration, failure, System.nanoTime()));
		}
		@Override public void didExitTransactionClosure(Transaction transaction, TransactionClosureOutcome outcome,
				DatabaseType type, Duration duration, Throwable failure) {
			this.logicalTransaction = transaction;
			this.logicalDuration = duration;
		}
	}

	private static final class Fixture {
		private final String operation;
		private final boolean fail;
		private final Collector collector = new Collector();
		private final Database database;
		private Duration jdbcDuration;
		private Duration bodyDuration;
		private long bodyFinishedAt;
		private Transaction transaction;

		private Fixture(String operation, boolean fail) {
			this.operation = operation;
			this.fail = fail;
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:transaction_metrics_timing_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			DataSource wrapped = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
					new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
						Object result = invoke(source, method, args);
						return result instanceof Connection connection ? wrap(connection) : result;
					});
			this.database = Database.with(wrapped, DatabaseType.GENERIC).metricsCollector(this.collector).build();
		}

		private Connection wrap(Connection connection) {
			return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> {
						if (!method.getName().equals(this.operation) || method.getParameterCount() != 0)
							return invoke(connection, method, args);
						long startedAt = System.nanoTime();
						try {
							if (this.fail) throw new SQLException("Completion attempt failed", "HY000");
							return invoke(connection, method, args);
						} finally {
							this.jdbcDuration = Duration.ofNanos(System.nanoTime() - startedAt);
						}
					});
		}

		private void run() {
			this.database.transaction(() -> {
				this.transaction = this.database.currentTransaction().orElseThrow();
				// Start the physical transaction before measuring additional application/database work.
				assertEquals(0, this.database.query("VALUES (0)").fetchObject(Integer.class).orElseThrow());
				long startedAt = System.nanoTime();
				for (int value = 1; value <= 8; value++)
					assertEquals(value, this.database.query("VALUES (CAST(:value AS INTEGER))").bind("value", value).fetchObject(Integer.class).orElseThrow());
				this.bodyFinishedAt = System.nanoTime();
				this.bodyDuration = Duration.ofNanos(this.bodyFinishedAt - startedAt);
				if (this.operation.equals("rollback")) this.transaction.setRollbackOnly(true);
			});
		}
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try {
			return method.invoke(target, args);
		} catch (InvocationTargetException failure) {
			throw failure.getCause();
		}
	}
}
