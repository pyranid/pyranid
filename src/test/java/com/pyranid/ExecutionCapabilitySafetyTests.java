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
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionCapabilitySafetyTests {

	@Test
	void ordinarySqlFailuresCannotAuthorizeReplayOrPoisonCapabilityDetection() {
		List<Supplier<SQLException>> failures = new ArrayList<>();
		for (String message : List.of("unsupported customer value", "not supported table name", "not implemented column name")) {
			failures.add(() -> new SQLException(message, "23505"));
			failures.add(() -> new BatchUpdateException(message, "23505", 0, new int[0]));
		}
		failures.add(() -> new SQLException("optional operation", "0A000"));
		failures.add(() -> new SQLException("optional operation", "HYC00"));
		failures.add(() -> new SQLException("optional operation", "HY000", new UnsupportedOperationException("unsupported")));

		for (String operation : List.of("update", "batch")) {
			for (boolean transactional : new boolean[]{false, true}) {
				for (Supplier<SQLException> failureFactory : failures)
					assertFailureIsNotReplayed(operation, transactional, failureFactory.get());
			}
		}
	}

	@Test
	void explicitCapabilitySignalsCannotOverrideContradictorySqlEvidence() {
		List<Supplier<Throwable>> failures = new ArrayList<>();
		for (String state : List.of("23505", "42000", "HY000"))
			failures.add(() -> new SQLFeatureNotSupportedException("unsupported", state));
		failures.add(() -> {
			SQLException failure = new SQLFeatureNotSupportedException("unsupported", "0A000");
			failure.setNextException(new SQLException("server execution failed", "23505"));
			return failure;
		});
		failures.add(() -> {
			SQLException failure = new SQLFeatureNotSupportedException("unsupported", "0A000");
			failure.addSuppressed(new SQLException("server execution failed", "42000"));
			return failure;
		});
		failures.add(() -> new SQLFeatureNotSupportedException("unsupported", null,
				new SQLException("server execution failed", "HY000")));
		failures.add(() -> new UnsupportedOperationException("unsupported",
				new SQLException("server execution failed", "23505")));
		failures.add(() -> new SQLFeatureNotSupportedException("unsupported", "0A000",
				new java.io.IOException("execution transport failed")));

		for (String operation : List.of("update", "batch")) {
			for (boolean transactional : new boolean[]{false, true}) {
				for (Supplier<Throwable> failureFactory : failures)
					assertFailureIsNotReplayed(operation, transactional, failureFactory.get());
			}
		}
	}

	@Test
	void batchExecutionFailuresCannotHideBehindExplicitCapabilitySignals() {
		for (String operation : List.of("update", "batch")) {
			for (boolean transactional : new boolean[]{false, true}) {
				for (String state : new String[]{null, "", "0A000", "HYC00"}) {
					for (String kind : List.of("sql", "runtime", "legacy")) {
						for (String edge : List.of("cause", "next", "suppressed")) {
							Throwable failure = switch (kind) {
								case "runtime" -> new UnsupportedOperationException("large operation unavailable");
								case "legacy" -> new AbstractMethodError("large operation unavailable");
								default -> new SQLFeatureNotSupportedException("large operation unavailable", "0A000");
							};
							BatchUpdateException batch = new BatchUpdateException("batch partially executed", state, 0,
									new int[]{1, java.sql.Statement.SUCCESS_NO_INFO});
							if (edge.equals("suppressed")) failure.addSuppressed(batch);
							else if (edge.equals("cause")) failure.initCause(batch);
							else if (failure instanceof SQLException sqlFailure) sqlFailure.setNextException(batch);
							else {
								SQLException detail = new SQLFeatureNotSupportedException("optional method detail", "0A000");
								detail.setNextException(batch);
								failure.initCause(detail);
							}

							assertFailureIsNotReplayed(operation, transactional, failure);
						}
					}
				}
				// Batch failure is contradictory execution evidence even without readable successful counts.
				for (long[] counts : List.of(new long[0], new long[]{1L << 33})) {
					SQLException failure = new SQLFeatureNotSupportedException("large operation unavailable", "0A000");
					failure.setNextException(new BatchUpdateException("batch execution failed", "0A000", 0, counts, null));
					assertFailureIsNotReplayed(operation, transactional, failure);
				}
			}
		}
	}

	@Test
	void unreadableTypedCapabilityEvidencePreventsFallbackAndRetainsDiagnostics() {
		for (String operation : List.of("update", "batch")) {
			for (boolean transactional : new boolean[]{false, true}) {
				for (String accessor : List.of("state", "cause", "next")) {
					RuntimeException diagnostic = new RuntimeException("unreadable " + accessor);
					SQLException failure = new SQLFeatureNotSupportedException("optional method unavailable", "0A000") {
						@Override public String getSQLState() {
							if (accessor.equals("state")) throw diagnostic;
							return super.getSQLState();
						}
						@Override public Throwable getCause() {
							if (accessor.equals("cause")) throw diagnostic;
							return super.getCause();
						}
						@Override public SQLException getNextException() {
							if (accessor.equals("next")) throw diagnostic;
							return super.getNextException();
						}
					};

					assertFailureIsNotReplayed(operation, transactional, failure);
					assertTrue(List.of(failure.getSuppressed()).contains(diagnostic));
				}
			}
		}
	}

	@Test
	void typedCapabilityFallbackDoesNotReadTheExceptionMessage() {
		for (String operation : List.of("update", "batch")) {
			for (boolean transactional : new boolean[]{false, true}) {
				AtomicInteger messageReads = new AtomicInteger();
				SQLException failure = new SQLFeatureNotSupportedException("optional method unavailable", "0A000") {
					@Override public String getMessage() {
						messageReads.incrementAndGet();
						throw new AssertionError("Capability fallback must not read exception messages");
					}
					@Override public String toString() { return "SQLFeatureNotSupportedException[unreadable message]"; }
				};
				Fixture fixture = new Fixture(operation, failure, false, 1, null);

				assertEquals(fixture.expectedCounts(), fixture.execute(transactional, 1));
				assertEquals(fixture.expectedCounts(), fixture.execute(transactional, 10));
				List<Integer> expected = new ArrayList<>(fixture.expected(1));
				expected.addAll(fixture.expected(10));
				assertEquals(expected, fixture.ids());
				assertEquals(0, messageReads.get());
				assertEquals(1, fixture.largeCalls.get());
				assertEquals(2, fixture.fallbackCalls.get());
				assertEquals(transactional ? 2 : 0, fixture.commits.get());
				assertEquals(0, fixture.rollbacks.get());
			}
		}
	}

	@Test
	void interruptionPublishedByCapabilityInspectionPreventsFallback() {
		for (String operation : List.of("update", "batch")) {
			SQLException failure = new SQLFeatureNotSupportedException("optional operation", "0A000") {
				@Override public String getSQLState() {
					Thread.currentThread().interrupt();
					return super.getSQLState();
				}
			};
			Fixture fixture = new Fixture(operation, failure, false, 1, null);
			try {
				DatabaseException thrown = assertThrows(DatabaseException.class,
						() -> fixture.execute(true, 1));

				assertSame(failure, thrown.getCause());
				assertTrue(Thread.interrupted());
				assertEquals(1, fixture.largeCalls.get());
				assertEquals(0, fixture.fallbackCalls.get());
				assertEquals(1, fixture.rollbacks.get());
				assertEquals(0, fixture.commits.get());
				assertEquals(List.of(), fixture.ids());
			} finally {
				Thread.interrupted();
			}
		}
	}

	@Test
	void anEmptyBatchAfterAnOrdinaryFailureMustNotTurnTheFailureIntoSuccess() {
		for (boolean transactional : new boolean[]{false, true}) {
			SQLException failure = new BatchUpdateException("duplicate key in unsupported_table", "23505", 0, new int[0]);
			Fixture fixture = new Fixture("batch", failure, false, 1, null);
			fixture.emptyFallbackBatch = true;

			DatabaseException thrown = assertThrows(DatabaseException.class,
					() -> fixture.execute(transactional, 1));

			assertSame(failure, thrown.getCause());
			assertEquals(List.of(), fixture.ids());
			assertEquals(1, fixture.largeCalls.get());
			assertEquals(0, fixture.fallbackCalls.get());
			assertEquals(transactional ? 1 : 0, fixture.rollbacks.get());
			assertEquals(0, fixture.commits.get());
		}
	}

	@Test
	void supportedLargeMethodsContinueToExecuteNormally() {
		for (String operation : List.of("update", "batch")) {
			for (boolean transactional : new boolean[]{false, true}) {
				Fixture fixture = new Fixture(operation, null, false, 0, null);

				assertEquals(fixture.expectedCounts(), fixture.execute(transactional, 1));
				assertEquals(fixture.expectedCounts(), fixture.execute(transactional, 10));

				List<Integer> expected = new ArrayList<>(fixture.expected(1));
				expected.addAll(fixture.expected(10));
				assertEquals(expected, fixture.ids());
				assertEquals(2, fixture.largeCalls.get());
				assertEquals(0, fixture.fallbackCalls.get());
				assertEquals(transactional ? 2 : 0, fixture.commits.get());
				assertEquals(0, fixture.rollbacks.get());
			}
		}
	}

	@Test
	void explicitCapabilitySignalsStillFallbackAndCacheTheSupportedMethod() {
		for (String operation : List.of("update", "batch")) {
			for (boolean transactional : new boolean[]{false, true}) {
				for (Throwable failure : List.of(new SQLFeatureNotSupportedException("optional operation", "0A000"),
						new UnsupportedOperationException("optional operation"), new AbstractMethodError("optional operation"))) {
					Fixture fixture = new Fixture(operation, failure, false, 1, null);

					assertEquals(fixture.expectedCounts(), fixture.execute(transactional, 1));
					assertEquals(fixture.expectedCounts(), fixture.execute(transactional, 10));

					List<Integer> expected = new ArrayList<>(fixture.expected(1));
					expected.addAll(fixture.expected(10));
					assertEquals(expected, fixture.ids());
					assertEquals(1, fixture.largeCalls.get());
					assertEquals(2, fixture.fallbackCalls.get());
					assertEquals(transactional ? 2 : 0, fixture.commits.get());
					assertEquals(0, fixture.rollbacks.get());
				}
			}
		}
	}

	@Test
	void concurrentInitialFailuresEachPropagateWithoutFallback() throws Exception {
		for (String operation : List.of("update", "batch")) {
			for (boolean transactional : new boolean[]{false, true}) {
				SQLException failure = new SQLException("unsupported value in failing execution", "23505");
				Fixture fixture = new Fixture(operation, failure, true, 2, new CountDownLatch(2));
				ExecutorService workers = Executors.newFixedThreadPool(2);
				try {
					List<Future<DatabaseException>> results = new ArrayList<>();
					for (int id : new int[]{1, 10}) {
						results.add(workers.submit(() -> assertThrows(DatabaseException.class,
								() -> fixture.execute(transactional, id))));
					}
					for (Future<DatabaseException> result : results)
						assertSame(failure, result.get(10, TimeUnit.SECONDS).getCause());
				} finally {
					workers.shutdownNow();
					assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
				}

				List<Integer> expected = new ArrayList<>();
				if (!transactional) {
					expected.addAll(fixture.expected(1));
					expected.addAll(fixture.expected(10));
				}
				assertEquals(expected, fixture.ids());
				assertEquals(2, fixture.largeCalls.get());
				assertEquals(0, fixture.fallbackCalls.get());
				assertEquals(transactional ? 2 : 0, fixture.rollbacks.get());
				assertEquals(0, fixture.commits.get());
				// Neither concurrent ordinary failure may downgrade the shared capability cache.
				assertEquals(fixture.expectedCounts(), fixture.execute(transactional, 20));
				assertEquals(3, fixture.largeCalls.get());
				assertEquals(0, fixture.fallbackCalls.get());
			}
		}
	}

	private static void assertFailureIsNotReplayed(String operation, boolean transactional, Throwable failure) {
		// The driver has executed before it reports the failure: another call can repeat durable work.
		Fixture fixture = new Fixture(operation, failure, true, 1, null);

		Class<? extends Throwable> expectedType = failure instanceof Error ? Error.class : DatabaseException.class;
		Throwable thrown = assertThrows(expectedType,
				() -> fixture.execute(transactional, 1));

		if (failure instanceof Error) assertSame(failure, thrown);
		else assertSame(failure, thrown.getCause());
		assertEquals(1, fixture.largeCalls.get());
		assertEquals(0, fixture.fallbackCalls.get());
		assertEquals(transactional ? List.of() : fixture.expected(1), fixture.ids());
		assertEquals(transactional ? 1 : 0, fixture.rollbacks.get());
		assertEquals(0, fixture.commits.get());

		// A failed execution leaves the capability unknown; the next operation must try the large method.
		assertEquals(fixture.expectedCounts(), fixture.execute(transactional, 10));
		assertEquals(2, fixture.largeCalls.get());
		assertEquals(0, fixture.fallbackCalls.get());
		List<Integer> expected = new ArrayList<>(transactional ? List.of() : fixture.expected(1));
		expected.addAll(fixture.expected(10));
		assertEquals(expected, fixture.ids());
	}

	private static final class Fixture {
		private final String operation;
		private final Database database;
		private final Database inspection;
		private final AtomicInteger largeCalls = new AtomicInteger();
		private final AtomicInteger fallbackCalls = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger();
		private final AtomicInteger rollbacks = new AtomicInteger();
		private boolean emptyFallbackBatch;

		private Fixture(String operation, Throwable failure, boolean executeBeforeFailure, int failureCount,
				CountDownLatch concurrentCalls) {
			this.operation = operation;
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:execution_capability_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			this.inspection = Database.with(source, DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
			AtomicInteger remainingFailures = new AtomicInteger(failureCount);
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(source, method, args);
				if (!(result instanceof Connection connection)) return result;
				Connection[] handle = new Connection[1];
				handle[0] = proxy(Connection.class, (call, values) -> {
					if (call.getName().equals("commit")) this.commits.incrementAndGet();
					if (call.getName().equals("rollback")) this.rollbacks.incrementAndGet();
					Object value = invoke(connection, call, values);
					if (!(value instanceof PreparedStatement statement)) return value;
					return proxy(PreparedStatement.class, (jdbc, parameters) -> {
						String name = jdbc.getName();
						if (name.equals("getConnection")) return handle[0];
						if (name.equals("executeLargeUpdate") || name.equals("executeLargeBatch")) {
							this.largeCalls.incrementAndGet();
							if (remainingFailures.getAndDecrement() > 0) {
								if (concurrentCalls != null) {
									concurrentCalls.countDown();
									if (!concurrentCalls.await(5, TimeUnit.SECONDS))
										throw new AssertionError("Both initial executions must reach the driver");
								}
								if (executeBeforeFailure) invoke(statement, jdbc, parameters);
								throw failure;
							}
						}
						if (name.equals("executeUpdate") || name.equals("executeBatch")) {
							this.fallbackCalls.incrementAndGet();
							if (name.equals("executeBatch") && this.emptyFallbackBatch) return new int[0];
						}
						return invoke(statement, jdbc, parameters);
					});
				});
				return handle[0];
			});
			this.database = Database.with(wrapped, DatabaseType.GENERIC).build();
		}

		private List<Long> execute(boolean transactional, int id) {
			if (transactional)
				return this.database.transaction(() -> Optional.of(execute(id))).orElseThrow();
			return execute(id);
		}

		private List<Long> execute(int id) {
			if (this.operation.equals("update"))
				return List.of(this.database.query("INSERT INTO work VALUES (:id)").bind("id", id).execute());
			return this.database.query("INSERT INTO work VALUES (:id)")
					.executeBatch(List.of(Map.of("id", id), Map.of("id", id + 1)));
		}

		private List<Long> expectedCounts() { return this.operation.equals("update") ? List.of(1L) : List.of(1L, 1L); }
		private List<Integer> expected(int id) { return this.operation.equals("update") ? List.of(id) : List.of(id, id + 1); }
		private List<Integer> ids() { return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class); }
	}

	@FunctionalInterface private interface Call { Object run(Method method, Object[] args) throws Throwable; }

	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> call.run(method, args)));
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
