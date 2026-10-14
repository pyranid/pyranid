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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Failure logs and metrics must include time spent in the JDBC/mapper call that threw.
 */
public class StatementFailureTimingTests {
	@Test
	public void failedPreparationIsMeasuredBeforeStatementLogging() {
		for (String failureMethod : List.of("prepareStatement", "setObject", "customize")) {
			for (boolean stream : new boolean[]{false, true}) {
				Harness harness = new Harness(failureMethod);
				Query query = harness.database.query("SELECT :value").bind("value", 1)
						.customize((context, statement) -> harness.failIfRequested("customize"));
				long elapsed = expectFailure(() -> {
					if (stream)
						query.fetchStream(Integer.class, values -> values.toList());
					else
						query.fetchList(Integer.class);
				});
				StatementLog<?> log = harness.assertFailedLog(elapsed);
				assertIncludesDelay(log.getPreparationDuration(), harness.failureNanos);
				Assertions.assertTrue(log.getExecutionDuration().isEmpty());
				Assertions.assertTrue(log.getResultSetMappingDuration().isEmpty());
			}
		}
	}

	@Test
	public void failedQueryAndStreamExecutionRetainElapsedTime() {
		for (Consumer<Query> operation : List.<Consumer<Query>>of(
				query -> query.fetchObject(Integer.class),
				query -> query.fetchList(Integer.class),
				query -> query.fetchStream(Integer.class, values -> values.toList()))) {
			Harness harness = new Harness("executeQuery");
			long elapsed = expectFailure(() -> operation.accept(harness.database.query("SELECT 1")));
			StatementLog<?> log = harness.assertFailedLog(elapsed);
			assertIncludesDelay(log.getExecutionDuration(), harness.failureNanos);
			Assertions.assertTrue(log.getPreparationDuration().isPresent());
			Assertions.assertTrue(log.getResultSetMappingDuration().isEmpty());
		}
	}

	@Test
	public void failedUpdatesAndGeneratedKeysRetainExecutionTime() {
		Harness update = new Harness("executeLargeUpdate");
		long updateElapsed = expectFailure(() -> update.database.query("UPDATE example SET value = 1").execute());
		assertIncludesDelay(update.assertFailedLog(updateElapsed).getExecutionDuration(), update.failureNanos);

		for (String failureMethod : List.of("executeLargeUpdate", "getGeneratedKeys")) {
			for (Consumer<Query> operation : List.<Consumer<Query>>of(
					query -> query.executeReturningGeneratedKey(Integer.class),
					query -> query.executeReturningGeneratedKeys(Integer.class))) {
				Harness harness = new Harness(failureMethod);
				long elapsed = expectFailure(() -> operation.accept(harness.database.query("INSERT INTO example VALUES (1)")));
				StatementLog<?> log = harness.assertFailedLog(elapsed);
				assertIncludesDelay(log.getExecutionDuration(), harness.failureNanos);
				Assertions.assertTrue(log.getResultSetMappingDuration().isEmpty());
			}
		}
	}

	@Test
	public void failedBatchesAndNotificationsRetainExecutionTime() {
		for (Integer chunkSize : new Integer[]{null, 1}) {
			Harness harness = new Harness("executeLargeBatch");
			long elapsed = expectFailure(() -> harness.database.query("INSERT INTO example VALUES (:value)")
					.batchChunkSize(chunkSize).executeBatch(List.of(Map.of("value", 1), Map.of("value", 2))));
			assertIncludesDelay(harness.assertFailedLog(elapsed).getExecutionDuration(), harness.failureNanos);
		}

		Harness notification = new Harness("execute", DatabaseType.POSTGRESQL);
		long elapsed = expectFailure(() -> notification.database.sendNotification("timing_test", "payload"));
		assertIncludesDelay(notification.assertFailedLog(elapsed).getExecutionDuration(), notification.failureNanos);
	}

	@Test
	public void failedMappingAndCursorAdvanceKeepBothExecutionAndMappingTime() {
		for (String failureMethod : List.of("map", "next")) {
			for (Consumer<Query> operation : List.<Consumer<Query>>of(
					query -> query.fetchObject(Integer.class),
					query -> query.fetchList(Integer.class),
					query -> query.executeReturningGeneratedKey(Integer.class),
					query -> query.executeReturningGeneratedKeys(Integer.class),
					query -> query.fetchStream(Integer.class, values -> values.toList()))) {
				Harness harness = new Harness(failureMethod);
				long elapsed = expectFailure(() -> operation.accept(harness.database.query("SELECT 1")));
				StatementLog<?> log = harness.assertFailedLog(elapsed);
				assertIncludesDelay(log.getExecutionDuration(), harness.executionNanos);
				assertIncludesDelay(log.getResultSetMappingDuration(), harness.failureNanos);
			}
		}
	}

	@Test
	public void executionErrorsAlsoRetainElapsedTime() {
		for (boolean stream : new boolean[]{false, true}) {
			Harness harness = new Harness("executeQuery");
			harness.throwError = true;
			long start = System.nanoTime();
			Assertions.assertThrows(AssertionError.class, () -> {
				if (stream)
					harness.database.query("SELECT 1").fetchStream(Integer.class, values -> values.toList());
				else
					harness.database.query("SELECT 1").fetchList(Integer.class);
			});
			StatementLog<?> log = harness.assertFailedLog(System.nanoTime() - start);
			assertIncludesDelay(log.getExecutionDuration(), harness.failureNanos);
		}
	}

	@Test
	public void successfulStreamsMeasureCursorReadsWithoutIncludingConsumerIdleTime() {
		Harness harness = new Harness("none");
		long[] idleNanos = new long[1];
		long start = System.nanoTime();
		List<Integer> values = harness.database.query("SELECT 1").fetchStream(Integer.class, stream -> {
			idleNanos[0] = pause();
			return stream.toList();
		});
		long elapsed = System.nanoTime() - start;
		Assertions.assertEquals(List.of(1), values);
		Assertions.assertEquals(1, harness.logs.size());
		Assertions.assertEquals(1, harness.successLogs.size());
		Assertions.assertTrue(harness.failureLogs.isEmpty());
		StatementLog<?> log = harness.logs.get(0);
		Assertions.assertSame(log, harness.successLogs.get(0));
		assertIncludesDelay(log.getResultSetMappingDuration(), harness.cursorNanos);
		Assertions.assertTrue(log.getTotalDuration().toNanos() + idleNanos[0] <= elapsed,
				"The statement total must exclude time spent in the stream consumer between JDBC calls");
		assertDurationSum(log);
		Assertions.assertTrue(log.getElapsedDuration().orElseThrow().toNanos() >= log.getTotalDuration().toNanos() + idleNanos[0]);
	}

	@Test
	public void elapsedDurationEndsAtEarlyCloseAndIncludesConnectionCleanup() {
		for (boolean explicitClose : new boolean[]{false, true}) {
			Harness harness = new Harness("none");
			long[] afterCloseIdle = new long[1];
			long started = System.nanoTime();
			harness.database.query("SELECT 1").fetchStream(Integer.class, stream -> {
				if (explicitClose) stream.close();
				else Assertions.assertEquals(List.of(1), stream.toList());
				Assertions.assertEquals(1, harness.logs.size(), "Resource closure emits its statement log immediately");
				Assertions.assertTrue(harness.connectionClosed);
				StatementLog<?> log = harness.logs.get(0);
				assertIncludesDelay(log.getElapsedDuration(), harness.cleanupNanos);
				long frozenElapsed = log.getElapsedDuration().orElseThrow().toNanos();
				afterCloseIdle[0] = pause();
				Assertions.assertEquals(frozenElapsed, log.getElapsedDuration().orElseThrow().toNanos());
				return null;
			});
			long actual = System.nanoTime() - started;
			Assertions.assertTrue(harness.logs.get(0).getElapsedDuration().orElseThrow().toNanos() + afterCloseIdle[0] <= actual);
			Assertions.assertEquals(1, harness.logs.size());
		}
	}

	@Test
	public void manuallyBuiltLogsLeaveElapsedAbsentAndKeepTotalAsPhaseSum() {
		Harness harness = new Harness("none");
		StatementContext<Void> context = StatementContext.with(Statement.of("timing", "SELECT 1"), harness.database).build();
		StatementLog<?> manual = StatementLog.withStatementContext(context).executionDuration(Duration.ofSeconds(2)).build();
		Assertions.assertTrue(manual.getElapsedDuration().isEmpty());
		Assertions.assertEquals(Duration.ofSeconds(2), manual.getTotalDuration());
		StatementLog<?> elapsed = StatementLog.withStatementContext(context).executionDuration(Duration.ofSeconds(2))
				.elapsedDuration(Duration.ofSeconds(5)).build();
		Assertions.assertEquals(Duration.ofSeconds(2), elapsed.getTotalDuration());
		Assertions.assertEquals(Duration.ofSeconds(5), elapsed.getElapsedDuration().orElseThrow());
		Assertions.assertNotEquals(manual, elapsed);
		Assertions.assertTrue(elapsed.toString().contains("elapsedDuration=PT5S"));
		Assertions.assertThrows(IllegalArgumentException.class, () -> StatementLog.withStatementContext(context).elapsedDuration(Duration.ofNanos(-1)));
	}

	private long expectFailure(Runnable operation) {
		long start = System.nanoTime();
		Assertions.assertThrows(DatabaseException.class, operation::run);
		return System.nanoTime() - start;
	}

	private static void assertIncludesDelay(Optional<Duration> duration, long delayNanos) {
		Assertions.assertTrue(delayNanos > 0L, "The intended delayed path must have executed");
		Assertions.assertTrue(duration.isPresent(), "Elapsed duration must be retained");
		Assertions.assertTrue(duration.orElseThrow().toNanos() >= delayNanos,
				() -> "Recorded " + duration.orElseThrow() + " omitted part of the measured delay " + Duration.ofNanos(delayNanos));
	}

	private static void assertDurationSum(StatementLog<?> log) {
		Duration sum = log.getConnectionAcquisitionDuration().orElse(Duration.ZERO)
				.plus(log.getPreparationDuration().orElse(Duration.ZERO))
				.plus(log.getExecutionDuration().orElse(Duration.ZERO))
				.plus(log.getResultSetMappingDuration().orElse(Duration.ZERO));
		Assertions.assertEquals(sum, log.getTotalDuration());
	}

	private static long pause() {
		long start = System.nanoTime();
		try {
			Thread.sleep(20);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		}
		return System.nanoTime() - start;
	}

	private static final class Harness {
		private final String failureMethod;
		private final List<StatementLog<?>> logs = new ArrayList<>();
		private final List<StatementLog<?>> failureLogs = new ArrayList<>();
		private final List<StatementLog<?>> successLogs = new ArrayList<>();
		private final Database database;
		private Connection connection;
		private long failureNanos;
		private long executionNanos;
		private long cursorNanos;
		private long cleanupNanos;
		private boolean connectionClosed;
		private int nextCalls;
		private boolean throwError;

		private Harness(String failureMethod) {
			this(failureMethod, DatabaseType.GENERIC);
		}

		private Harness(String failureMethod, DatabaseType databaseType) {
			this.failureMethod = failureMethod;
			ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
					(proxy, method, args) -> {
						if (method.getName().equals("next")) {
							if (++this.nextCalls > 1)
								failIfRequested("next");
							this.cursorNanos += pause();
							return this.nextCalls == 1;
						}
						return defaultValue(method.getReturnType());
					});
			PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
					new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
						failIfRequested(method.getName());
						return switch (method.getName()) {
							case "executeQuery", "getGeneratedKeys" -> {
								this.executionNanos += pause();
								yield resultSet;
							}
							case "executeLargeUpdate" -> 1L;
							case "executeUpdate" -> 1;
							case "getUpdateCount" -> -1;
							case "getConnection" -> this.connection;
							default -> defaultValue(method.getReturnType());
						};
					});
			this.connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> {
						failIfRequested(method.getName());
						if (method.getName().equals("close")) {
							this.cleanupNanos += pause();
							this.connectionClosed = true;
						}
						return method.getName().equals("prepareStatement") ? statement : defaultValue(method.getReturnType());
					});
			DataSource dataSource = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> method.getName().equals("getConnection") ? this.connection : defaultValue(method.getReturnType()));
			this.database = Database.withDataSource(dataSource)
					.databaseType(databaseType)
					.statementLogger(this.logs::add)
					.metricsCollector(new MetricsCollector() {
						@Override
						public void didFailToExecuteStatement(StatementContext<?> context, StatementLog<?> log,
								DatabaseType type, Throwable failure) {
							failureLogs.add(log);
						}
						@Override
						public void didExecuteStatement(StatementContext<?> context, StatementLog<?> log, StatementResult result) {
							successLogs.add(log);
						}
					})
					.resultSetMapper(new ResultSetMapper() {
						@Override
						public <T> Optional<T> map(StatementContext<T> context, ResultSet rows, Class<T> type,
								InstanceProvider instanceProvider) throws SQLException {
							failIfRequested("map");
							return Optional.of(type.cast(1));
						}
					})
					.build();
		}

		private void failIfRequested(String method) throws SQLException {
			if (!this.failureMethod.equals(method))
				return;
			this.failureNanos = pause();
			if (this.throwError)
				throw new AssertionError("Delayed JDBC error");
			throw new SQLTimeoutException("Delayed " + method + " failure", "57014");
		}

		private StatementLog<?> assertFailedLog(long observedNanos) {
			Assertions.assertEquals(1, this.logs.size());
			Assertions.assertEquals(1, this.failureLogs.size());
			Assertions.assertTrue(this.successLogs.isEmpty());
			StatementLog<?> log = this.logs.get(0);
			Assertions.assertSame(log, this.failureLogs.get(0));
			Assertions.assertTrue(log.getException().isPresent());
			Assertions.assertTrue(log.getTotalDuration().toNanos() >= this.failureNanos);
			Assertions.assertTrue(log.getTotalDuration().toNanos() <= observedNanos,
					"Statement phases must not overlap or exceed the observed operation duration");
			assertDurationSum(log);
			Assertions.assertTrue(log.getElapsedDuration().orElseThrow().toNanos() >= log.getTotalDuration().toNanos());
			return log;
		}

		private static Object defaultValue(Class<?> type) {
			if (type == boolean.class) return false;
			if (type == int.class) return 0;
			if (type == long.class) return 0L;
			return null;
		}
	}
}
