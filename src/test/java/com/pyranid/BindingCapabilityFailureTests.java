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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BindingCapabilityFailureTests {
	@AfterEach
	void clearInterruptFlag() { Thread.interrupted(); }

	@Test
	void legacyCapabilityFailureCannotHideRollbackAndCommitOnlyTheLaterWrite() {
		for (Site site : Site.LEGACY) {
			AbstractMethodError failure = new AbstractMethodError("optional JDBC method");
			failure.initCause(new SQLTransactionRollbackException("server rolled back", "40001"));
			Fixture fixture = new Fixture(site, failure, true, false);

			Error thrown = assertThrows(Error.class, () -> fixture.database.transaction(fixture::work), site.name());

			assertSame(failure, thrown);
			fixture.assertAborted(1);
			assertFalse(Thread.currentThread().isInterrupted());
		}
	}

	@Test
	void legacyCapabilityCancellationAndPendingInterruptionCannotFallbackOrRetry() {
		for (Site site : Site.LEGACY) {
			for (String edge : List.of("cause", "suppressed", "cycle", "pending")) {
				AbstractMethodError failure = new AbstractMethodError("optional JDBC method");
				InterruptedException interruption = new InterruptedException("cancelled binding");
				if (edge.equals("cause")) failure.initCause(interruption);
				else if (edge.equals("suppressed")) failure.addSuppressed(interruption);
				else if (edge.equals("cycle")) {
					SQLException detail = new SQLException("cancelled", "57014", interruption);
					failure.initCause(detail);
					detail.setNextException(new SQLException("cyclic detail", failure));
				}
				Fixture fixture = new Fixture(site, failure, false, edge.equals("pending"));
				AtomicInteger policies = new AtomicInteger();

				Error thrown = assertThrows(Error.class, () -> fixture.database.transactionWithRetry(
						retry(policies), fixture::work), site + " " + edge);

				assertTrue(Thread.interrupted(), site + " " + edge);
				assertSame(failure, thrown);
				assertEquals(0, policies.get());
				fixture.assertAborted(1);
			}
		}
	}

	@Test
	void nestedFatalOrOutcomeEvidenceIsNotABenignLegacyCapabilityFailure() {
		for (Site site : Site.LEGACY) {
			for (boolean committed : new boolean[]{false, true}) {
				AbstractMethodError failure = new AbstractMethodError("optional JDBC method");
				if (committed) {
					DatabaseException completed = new DatabaseException("independent transaction committed");
					completed.markTransactionOutcomeCommitted();
					failure.addSuppressed(completed);
				} else failure.addSuppressed(new AssertionError("fatal driver failure"));
				Fixture fixture = new Fixture(site, failure, false, false);

				assertSame(failure, assertThrows(Error.class, () -> fixture.database.transaction(fixture::work)));

				fixture.assertAborted(1);
			}
		}
	}

	@Test
	void bareLegacyCapabilitySignalsStillFallbackSuccessfully() {
		for (Site site : Site.LEGACY) {
			Fixture fixture = new Fixture(site, new AbstractMethodError("optional JDBC method"), false, false);

			fixture.database.transaction(fixture::work);

			assertEquals(List.of(1, 2), fixture.ids());
			assertEquals(1, fixture.faults.get());
			assertEquals(1, fixture.executions.get());
			assertEquals(1, fixture.commits.get());
			assertEquals(0, fixture.rollbacks.get());
			fixture.assertClosed(1);
			assertFalse(Thread.currentThread().isInterrupted());
		}
	}

	@Test
	void postgresWrapperDiscoveryCannotHideRollback() {
		for (boolean legacy : new boolean[]{false, true}) {
			Throwable failure;
			if (legacy) {
				failure = new AbstractMethodError("wrapper API unavailable");
				failure.initCause(new SQLTransactionRollbackException("server rolled back", "40001"));
			} else failure = new SQLTransactionRollbackException("wrapper discovery rolled back", "40001");
			Fixture fixture = new Fixture(Site.POSTGRES, failure, true, false);
			Class<? extends Throwable> expected = legacy ? Error.class : DatabaseException.class;

			Throwable thrown = assertThrows(expected, () -> fixture.database.transaction(fixture::work));

			assertTrue(contains(thrown, failure));
			fixture.assertAborted(1);
		}
	}

	@Test
	void postgresWrapperDiscoveryCancellationCannotAdmitAnotherAttempt() {
		for (String kind : List.of("sql", "runtime", "legacy", "pending")) {
			InterruptedException interruption = new InterruptedException("cancelled wrapper discovery");
			Throwable failure = switch (kind) {
				case "sql" -> new SQLException("cancelled", "57014", interruption);
				case "runtime" -> new UnsupportedOperationException("unavailable", interruption);
				default -> new AbstractMethodError("wrapper API unavailable");
			};
			if (kind.equals("legacy")) failure.addSuppressed(interruption);
			Fixture fixture = new Fixture(Site.POSTGRES, failure, false, kind.equals("pending"));
			AtomicInteger policies = new AtomicInteger();
			Class<? extends Throwable> expected = failure instanceof Error ? Error.class : DatabaseException.class;

			Throwable thrown = assertThrows(expected,
					() -> fixture.database.transactionWithRetry(retry(policies), fixture::work));

			assertTrue(Thread.interrupted(), kind);
			assertTrue(contains(thrown, failure));
			assertEquals(0, policies.get());
			fixture.assertAborted(1);
		}
	}

	@Test
	void postgresWrapperSerializationFailureRetriesTheWholeTransaction() {
		SQLException failure = new SQLTransactionRollbackException("wrapper discovery rolled back", "40001");
		Fixture fixture = new Fixture(Site.POSTGRES, failure, true, false);
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO),
				DatabaseException::isSerializationFailure);

		TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(policy, fixture::work);

		assertEquals(2, result.getAttemptCount());
		assertTrue(contains(result.getFailures().get(0), failure));
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(2, fixture.attempts.get());
		assertEquals(1, fixture.executions.get());
		assertEquals(1, fixture.commits.get());
		assertEquals(1, fixture.rollbacks.get());
		fixture.assertClosed(2);
	}

	@Test
	void benignWrapperDiscoveryFailuresStillFindTheDriverAndBindItsValue() {
		for (Throwable failure : new Throwable[]{new SQLFeatureNotSupportedException("wrapper API unavailable", "0A000"),
				new UnsupportedOperationException("wrapper API unavailable"), new AbstractMethodError("wrapper API unavailable"), null}) {
			Fixture fixture = new Fixture(Site.POSTGRES, failure, false, false);

			fixture.database.transaction(fixture::work);

			assertEquals(List.of(1, 2), fixture.ids());
			assertEquals("{}", fixture.inspection.query("SELECT payload FROM work WHERE id = 2").fetchObject(String.class).orElseThrow());
			assertEquals(1, fixture.executions.get());
			assertEquals(1, fixture.commits.get());
			assertEquals(0, fixture.rollbacks.get());
			fixture.assertClosed(1);
			assertFalse(Thread.currentThread().isInterrupted());
		}
	}

	private static RetryPolicy retry(AtomicInteger policies) {
		return RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			policies.incrementAndGet(); return true;
		});
	}
	private static boolean contains(Throwable root, Throwable target) {
		return ThrowableGraph.inspect(root, node -> node == target ? 1 : 0, 0) != 0;
	}

	private enum Site {
		TYPED, UNTYPED, METADATA, PARAMETER_TYPE, PARAMETER_NAME, PLAIN_NULL, POSTGRES;
		static final List<Site> LEGACY = List.of(TYPED, UNTYPED, METADATA, PARAMETER_TYPE, PARAMETER_NAME, PLAIN_NULL);
	}

	private static final class Fixture {
		private final Site site;
		private final Database database, inspection;
		private final AtomicInteger faults = new AtomicInteger(), attempts = new AtomicInteger(), executions = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger();
		private final AtomicInteger statementCloses = new AtomicInteger(), rawCloses = new AtomicInteger(), connectionCloses = new AtomicInteger();

		private Fixture(Site site, Throwable failure, boolean physicalRollback, boolean pendingInterrupt) {
			this.site = site;
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:binding_capability_" + UUID.randomUUID());
			source.setUser("sa"); source.setPassword("");
			this.inspection = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work(id INT PRIMARY KEY, value_integer INTEGER, value_timestamp TIMESTAMP, payload VARCHAR(100))").execute();
			AtomicBoolean armed = new AtomicBoolean(true);
			// A second visible loader exercises discovery fallback even when both supported drivers are on the IT classpath.
			ClassLoader driverLoader = new ClassLoader(BindingCapabilityFailureTests.class.getClassLoader()) {};
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(source, method, args);
				if (!(result instanceof Connection connection)) return result;
				Connection[] handle = new Connection[1];
				Call fault = (ignored, parameters) -> {
					if (failure != null && armed.compareAndSet(true, false)) {
						this.faults.incrementAndGet();
						if (physicalRollback) connection.rollback();
						if (pendingInterrupt) Thread.currentThread().interrupt();
						throw failure;
					}
					return null;
				};
				handle[0] = proxy(Connection.class, driverLoader, (operation, parameters) -> {
					String name = operation.getName();
					if (site == Site.POSTGRES && name.equals("isWrapperFor")) {
						fault.run(operation, parameters);
						return ((Class<?>) parameters[0]).getName().equals("org.postgresql.PGConnection");
					}
					if (name.equals("commit")) this.commits.incrementAndGet();
					if (name.equals("rollback")) this.rollbacks.incrementAndGet();
					if (name.equals("close")) this.connectionCloses.incrementAndGet();
					Object value = invoke(connection, operation, parameters);
					if (!(value instanceof PreparedStatement statement)) {
						if (value instanceof Statement raw) return proxy(Statement.class, (call, values) -> {
							if (call.getName().equals("close")) this.rawCloses.incrementAndGet();
							return invoke(raw, call, values);
						});
						return value;
					}
					return proxy(PreparedStatement.class, (jdbc, values) -> {
						String jdbcName = jdbc.getName();
						if (jdbcName.equals("getConnection")) return handle[0];
						if (jdbcName.equals("close")) this.statementCloses.incrementAndGet();
						if (jdbcName.equals("executeLargeUpdate") || jdbcName.equals("executeUpdate")) this.executions.incrementAndGet();
						if (jdbcName.equals("getParameterMetaData")) {
							if (site == Site.METADATA) fault.run(jdbc, values);
							ParameterMetaData metadata = (ParameterMetaData) invoke(statement, jdbc, values);
							return proxy(ParameterMetaData.class, (getter, getterArgs) -> {
								if (site == Site.PARAMETER_TYPE && getter.getName().equals("getParameterType")
										|| site == Site.PARAMETER_NAME && getter.getName().equals("getParameterTypeName")) fault.run(getter, getterArgs);
								return invoke(metadata, getter, getterArgs);
							});
						}
						if ((site == Site.TYPED || site == Site.UNTYPED) && jdbcName.equals("setObject")) fault.run(jdbc, values);
						if (site == Site.PLAIN_NULL && jdbcName.equals("setNull")) fault.run(jdbc, values);
						if (site == Site.POSTGRES && jdbcName.equals("setObject") && values[1] != null
								&& values[1].getClass().getName().equals("org.postgresql.util.PGobject")) {
							// Preserve real HSQL storage after exercising the production PostgreSQL factory and binding path.
							values = values.clone(); values[1] = values[1].toString();
						}
						return invoke(statement, jdbc, values);
					});
				});
				return handle[0];
			});
			this.database = Database.withDataSource(wrapped).databaseType(site == Site.UNTYPED ? DatabaseType.DUCK_DB
					: site == Site.POSTGRES ? DatabaseType.POSTGRESQL : DatabaseType.GENERIC).build();
		}

		private void work() {
			this.attempts.incrementAndGet();
			this.database.useRawConnection(connection -> {
				try (Statement statement = connection.createStatement()) { statement.executeUpdate("INSERT INTO work(id) VALUES(1)"); }
				return Optional.empty();
			});
			if (this.site == Site.TYPED || this.site == Site.UNTYPED) this.database
					.query("INSERT INTO work(id,value_timestamp) VALUES(2,:value)").bind("value", LocalDateTime.of(2024, 1, 2, 3, 4)).execute();
			else if (this.site == Site.POSTGRES) this.database.query("INSERT INTO work(id,payload) VALUES(2,:value)")
					.bind("value", Parameters.json("{}")).execute();
			else this.database.query("INSERT INTO work(id,value_integer) VALUES(2,:value)").bind("value", null).execute();
		}
		private List<Integer> ids() { return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class); }
		private void assertAborted(int attempts) {
			assertEquals(List.of(), ids());
			assertEquals(attempts, this.attempts.get());
			assertEquals(0, this.executions.get());
			assertEquals(0, this.commits.get());
			assertEquals(attempts, this.rollbacks.get());
			assertClosed(attempts);
		}
		private void assertClosed(int attempts) {
			assertEquals(attempts, this.statementCloses.get());
			assertEquals(attempts, this.rawCloses.get());
			assertEquals(attempts, this.connectionCloses.get());
			assertTrue(this.database.currentTransaction().isEmpty());
		}
	}

	@FunctionalInterface private interface Call { Object run(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) { return proxy(type, type.getClassLoader(), call); }
	private static <T> T proxy(Class<T> type, ClassLoader loader, Call call) {
		return type.cast(Proxy.newProxyInstance(loader, new Class<?>[]{type}, (proxy, method, args) -> call.run(method, args)));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
