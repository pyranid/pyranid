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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTransactionRollbackException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MetricsDiscoveryFailureTests {
	@AfterEach
	void clearInterruptFlag() { Thread.interrupted(); }

	@Test
	void metricsDiscoveryRollbackCannotReportSuccessOrCommitOnlyLaterWrites() {
		for (boolean typed : new boolean[]{false, true}) {
			SQLException failure = typed ? new SQLFeatureNotSupportedException("optional metadata", "0A000")
					: new SQLTransactionRollbackException("metadata rolled back transaction", "40001");
			if (typed) failure.setNextException(new SQLTransactionRollbackException("rolled back", "40001"));
			Fixture fixture = new Fixture(failure, true, true);

			DatabaseException thrown = assertThrows(DatabaseException.class,
					() -> fixture.database.transaction(fixture::work));

			assertTrue(contains(thrown, failure));
			assertEquals(List.of(), fixture.ids());
			fixture.assertResources(1, 1, 0, 1);
			assertEquals(0, fixture.completedBodies.get());
		}
	}

	@Test
	void metricsDiscoveryCancellationCannotAdmitAnotherAttempt() {
		for (String edge : List.of("cause", "next", "suppressed", "cycle", "unreadable")) {
			InterruptedException interruption = new InterruptedException("cancelled metadata");
			SQLException failure = edge.equals("unreadable")
					? new SQLFeatureNotSupportedException("optional metadata", "0A000") {
					@Override public String getSQLState() { throw new IllegalStateException("unreadable state"); }
				} : new SQLFeatureNotSupportedException("optional metadata", "0A000");
			if (edge.equals("cause")) failure.initCause(interruption);
			else if (edge.equals("next") || edge.equals("cycle")) {
				SQLException detail = new SQLException("cancelled", "57014", interruption);
				failure.setNextException(detail);
				if (edge.equals("cycle")) detail.setNextException(failure);
			} else failure.addSuppressed(interruption);
			Fixture fixture = new Fixture(failure, false, true);
			AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), ignored -> {
				policyCalls.incrementAndGet(); return true;
			});

			DatabaseException thrown = assertThrows(DatabaseException.class,
					() -> fixture.database.transactionWithRetry(policy, () -> {
						attempts.incrementAndGet(); fixture.work();
					}));

			assertTrue(Thread.interrupted(), edge);
			assertTrue(contains(thrown, failure));
			assertEquals(1, attempts.get());
			assertEquals(0, policyCalls.get());
			assertEquals(List.of(), fixture.ids());
			fixture.assertResources(1, 1, 0, 1);
		}
	}

	@Test
	void metricsDiscoveryCannotHideFatalOrCommittedEvidence() {
		for (boolean rawError : new boolean[]{false, true}) {
			Throwable failure;
			if (rawError) failure = new AssertionError("fatal metadata failure");
			else {
				DatabaseException committed = new DatabaseException("independent transaction committed");
				committed.markTransactionOutcomeCommitted();
				failure = new SQLFeatureNotSupportedException("optional metadata", "0A000", committed);
			}
			Fixture fixture = new Fixture(failure, false, true);
			AtomicInteger policyCalls = new AtomicInteger();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), ignored -> {
				policyCalls.incrementAndGet(); return true;
			});
			Class<? extends Throwable> expected = rawError ? Error.class : DatabaseException.class;

			Throwable thrown = assertThrows(expected,
					() -> fixture.database.transactionWithRetry(policy, fixture::work));

			if (rawError) assertSame(failure, thrown);
			else assertTrue(contains(thrown, failure));
			assertEquals(0, policyCalls.get());
			assertEquals(List.of(), fixture.ids());
			fixture.assertResources(1, 1, 0, 1);
		}
	}

	@Test
	void metricsDiscoverySerializationFailureRetriesAllApplicationWork() {
		SQLException failure = new SQLTransactionRollbackException("metadata rolled back transaction", "40001",
				new SQLFeatureNotSupportedException("optional metadata", "0A000"));
		Fixture fixture = new Fixture(failure, true, true);
		AtomicInteger attempts = new AtomicInteger();
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO),
				DatabaseException::isSerializationFailure);

		TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(policy, () -> {
			attempts.incrementAndGet(); fixture.work();
		});

		assertEquals(2, attempts.get());
		assertEquals(2, result.getAttemptCount());
		assertTrue(contains(result.getFailures().get(0), failure));
		assertEquals(List.of(1, 2, 3), fixture.ids());
		assertEquals(1, fixture.completedBodies.get());
		fixture.assertResources(3, 2, 1, 1);
	}

	@Test
	void benignMetadataFailuresRemainOptionalForMetrics() {
		for (Throwable failure : List.of(new SQLFeatureNotSupportedException("optional metadata", "0A000"),
				new UnsupportedOperationException("optional metadata"), new AbstractMethodError("optional metadata"))) {
			Fixture fixture = new Fixture(failure, false, true);

			fixture.database.transaction(fixture::work);

			assertEquals(List.of(1, 2, 3), fixture.ids());
			assertEquals(1, fixture.completedBodies.get());
			assertTrue(fixture.metadataCalls.get() >= 2);
			assertFalse(Thread.currentThread().isInterrupted());
			fixture.assertResources(2, 1, 1, 0);
		}
	}

	@Test
	void disabledMetricsDoNotProbeMetadataAfterExecution() {
		Fixture fixture = new Fixture(new SQLTransactionRollbackException("unused failure", "40001"), true, false);

		fixture.database.transaction(fixture::work);

		assertEquals(List.of(1, 2, 3), fixture.ids());
		assertEquals(1, fixture.metadataCalls.get());
		fixture.assertResources(2, 1, 1, 0);
	}

	private static boolean contains(Throwable failure, Throwable original) {
		return ThrowableGraph.inspect(failure, node -> node == original ? 1 : 0, 0) != 0;
	}

	private static final class Fixture {
		private final Database database, inspection;
		private final AtomicInteger metadataCalls = new AtomicInteger(), completedBodies = new AtomicInteger();
		private final AtomicInteger statementsClosed = new AtomicInteger(), connectionsClosed = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger();

		private Fixture(Throwable failure, boolean physicalRollback, boolean metricsEnabled) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:metrics_discovery_" + UUID.randomUUID());
			source.setUser("sa"); source.setPassword("");
			this.inspection = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(source, method, args);
				if (!(result instanceof Connection connection)) return result;
				return proxy(Connection.class, (call, values) -> {
					String name = call.getName();
					if (name.equals("commit")) this.commits.incrementAndGet();
					if (name.equals("rollback")) this.rollbacks.incrementAndGet();
					if (name.equals("close")) this.connectionsClosed.incrementAndGet();
					Object value = invoke(connection, call, values);
					if (value instanceof PreparedStatement statement) return proxy(PreparedStatement.class, (jdbc, parameters) -> {
						if (jdbc.getName().equals("close")) this.statementsClosed.incrementAndGet();
						return invoke(statement, jdbc, parameters);
					});
					if (value instanceof DatabaseMetaData metadata) return proxy(DatabaseMetaData.class, (getter, parameters) -> {
						if (getter.getName().equals("getDatabaseProductName")) {
							int attempt = this.metadataCalls.incrementAndGet();
							// Leave the type unknown at transaction begin so metrics must discover it after a write.
							if (attempt == 1) throw new SQLFeatureNotSupportedException("initial metadata unavailable", "0A000");
							if (attempt == 2) {
								if (physicalRollback) connection.rollback();
								throw failure;
							}
						}
						return invoke(metadata, getter, parameters);
					});
					return value;
				});
			});
			this.database = Database.withDataSource(wrapped).metricsCollector(metricsEnabled
					? new MetricsCollector() {} : MetricsCollector.disabledInstance()).build();
		}

		private void work() {
			this.database.useRawConnection(connection -> {
				try (java.sql.Statement statement = connection.createStatement()) { statement.executeUpdate("INSERT INTO work VALUES (1)"); }
				return Optional.empty();
			});
			this.database.query("INSERT INTO work VALUES (2)").execute();
			this.database.query("INSERT INTO work VALUES (3)").execute();
			this.completedBodies.incrementAndGet();
		}

		private List<Integer> ids() { return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class); }
		private void assertResources(int statements, int connections, int commits, int rollbacks) {
			assertEquals(statements, this.statementsClosed.get());
			assertEquals(connections, this.connectionsClosed.get());
			assertEquals(commits, this.commits.get());
			assertEquals(rollbacks, this.rollbacks.get());
			assertTrue(this.database.currentTransaction().isEmpty());
		}
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
