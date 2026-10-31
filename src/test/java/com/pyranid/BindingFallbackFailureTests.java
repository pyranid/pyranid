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
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.Types;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BindingFallbackFailureTests {
	@AfterEach
	void clearInterruptFlag() {
		Thread.interrupted();
	}

	@Test
	void parameterMetadataRollbackCannotCommitOnlyTheLaterStatement() {
		for (Site site : Site.METADATA_SITES) {
			SQLException failure = new SQLTransactionRollbackException("metadata aborted the transaction", "40001");
			Fixture fixture = new Fixture(site, Shape.PLAIN, 0, failure, true);

			DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction, site.name());

			assertSame(failure, thrown.getCause());
			assertTrue(thrown.isSerializationFailure());
			fixture.assertAborted(1);
			assertFalse(Thread.currentThread().isInterrupted());
		}
	}

	@Test
	void plainAndContainerNullFallbacksCannotHideARollbackAtAnyFallbackStep() {
		for (Shape shape : Shape.values()) {
			// Container nulls try a named SQL type, its JDBC type, then the metadata's type before Types.NULL.
			for (int attempt = 0; attempt < (shape == Shape.PLAIN ? 1 : 3); ++attempt) {
				SQLException failure = new SQLTransactionRollbackException("null bind aborted the transaction", "40001");
				Fixture fixture = new Fixture(Site.SET_NULL, shape, attempt, failure, true);

				DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction,
						shape + " null attempt " + attempt);

				assertSame(failure, thrown.getCause());
				assertEquals(attempt + 1, fixture.nullCalls.get(), "No null bind may follow a rollback report");
				fixture.assertAborted(1);
			}
		}
	}

	@Test
	void cancellationFromMetadataOrNullBindingRestoresInterruptionBeforeRetryAdmission() {
		for (Site site : Site.values()) {
			for (Shape shape : site == Site.SET_NULL ? Shape.values() : new Shape[]{Shape.PLAIN}) {
				SQLException failure = new SQLException("binding cancelled", "57014", new InterruptedException("cancelled"));
				Fixture fixture = new Fixture(site, shape, 0, failure, false);
				AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
				RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), problem -> {
					conditions.incrementAndGet();
					return true;
				});

				DatabaseException thrown = assertThrows(DatabaseException.class,
						() -> fixture.database.transactionWithRetry(policy, () -> {
							attempts.incrementAndGet();
							fixture.executeStatements();
						}));

				// Inspect at the boundary, before a fresh checkout can consume the restored flag.
				boolean interruptedAtExit = Thread.interrupted();
				assertSame(failure, thrown.getCause());
				assertTrue(interruptedAtExit, site + " " + shape);
				assertEquals(1, attempts.get());
				assertEquals(0, conditions.get());
				fixture.assertAborted(1);
			}
		}
	}

	@Test
	void terminalEvidenceInCausesNextExceptionsAndCyclesPreventsMetadataFallback() {
		SQLException caused = new SQLException("optional metadata unavailable", "HY000",
				new SQLTransactionRollbackException("transaction rolled back", "40001"));
		SQLException chained = new SQLException("optional metadata unavailable", "HY000");
		chained.setNextException(new SQLException("connection lost", "08006"));
		SQLException cyclic = new SQLException("optional metadata unavailable", "HY000");
		SQLException detail = new SQLException("transaction rolled back", "40001");
		cyclic.setNextException(detail);
		detail.setNextException(cyclic);
		SQLException suppressedCancellation = new SQLException("optional metadata unavailable", "HY000");
		suppressedCancellation.addSuppressed(new InterruptedException("cancelled"));

		for (SQLException failure : List.of(caused, chained, cyclic, suppressedCancellation)) {
			Fixture fixture = new Fixture(Site.METADATA, Shape.PLAIN, 0, failure, false);

			DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction);

			boolean interruptedAtExit = Thread.interrupted();
			assertSame(failure, thrown.getCause());
			assertEquals(failure == suppressedCancellation, interruptedAtExit);
			fixture.assertAborted(1);
		}
	}

	@Test
	void terminalJdbcExceptionTypesAndUnreadableDiagnosticsAreNotOptionalMetadata() {
		List<SQLException> failures = List.of(
				new SQLTransactionRollbackException("transaction rolled back"),
				new SQLNonTransientConnectionException("connection lost"),
				new SQLTransientConnectionException("connection unavailable"),
				new SQLRecoverableException("connection must be replaced"),
				new SQLTimeoutException("metadata timed out"),
				new SQLException("SQLState is unreadable", "HY000") {
					@Override public String getSQLState() { throw new IllegalStateException("driver diagnostic failure"); }
				});

		for (SQLException failure : failures) {
			Fixture fixture = new Fixture(Site.METADATA, Shape.PLAIN, 0, failure, false);

			DatabaseException thrown = assertThrows(DatabaseException.class, fixture::executeInTransaction);

			assertSame(failure, thrown.getCause());
			fixture.assertAborted(1);
			assertFalse(Thread.currentThread().isInterrupted());
		}
	}

	@Test
	void serializationFailureRetriesTheEntireClosureAfterRollback() {
		SQLException failure = new SQLTransactionRollbackException("metadata aborted the transaction", "40001");
		Fixture fixture = new Fixture(Site.METADATA, Shape.PLAIN, 0, failure, true);
		AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), problem -> {
			conditions.incrementAndGet();
			return problem.isSerializationFailure();
		});

		TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(policy, () -> {
			attempts.incrementAndGet();
			fixture.executeStatements();
		});

		assertEquals(2, result.getAttemptCount());
		assertEquals(2, attempts.get());
		assertEquals(1, conditions.get());
		assertSame(failure, result.getFailures().get(0).getCause());
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(1, fixture.secondExecutions.get());
		assertEquals(1, fixture.commits.get());
		assertEquals(1, fixture.rollbacks.get());
		fixture.assertClosed(2);
	}

	@Test
	void benignMetadataAndTypeSpecificNullFailuresStillUseCompatibilityFallbacks() {
		for (Site site : Site.values()) {
			for (Shape shape : site == Site.SET_NULL ? Shape.values() : new Shape[]{Shape.PLAIN}) {
				SQLException unavailable = new SQLException("driver cannot supply this optional hint", "HY000");
				Fixture fixture = new Fixture(site, shape, 0, unavailable, false);

				fixture.executeInTransaction();

				assertEquals(List.of(1, 2), fixture.ids());
				assertEquals(1, fixture.secondExecutions.get());
				assertEquals(1, fixture.commits.get());
				assertEquals(0, fixture.rollbacks.get());
				assertEquals(1, fixture.faultCalls.get());
				if (site == Site.SET_NULL)
					assertEquals(2, fixture.nullCalls.get());
				fixture.assertClosed(1);
			}
		}
	}

	private enum Site {
		METADATA, PARAMETER_TYPE, PARAMETER_TYPE_NAME, SET_NULL;
		static final List<Site> METADATA_SITES = List.of(METADATA, PARAMETER_TYPE, PARAMETER_TYPE_NAME);
	}

	private enum Shape {
		PLAIN, ARRAY, STRUCT;

		Object parameter() {
			return switch (this) {
				case PLAIN -> null;
				case ARRAY -> Parameters.sqlArrayOf("VARCHAR", (Object[]) null);
				case STRUCT -> Parameters.sqlStructOf("review_struct", (Object[]) null);
			};
		}
	}

	private static final class Fixture {
		private final Database database, inspectionDatabase;
		private final Shape shape;
		private final AtomicInteger nullCalls = new AtomicInteger(), faultCalls = new AtomicInteger();
		private final AtomicInteger secondExecutions = new AtomicInteger(), commits = new AtomicInteger(), rollbacks = new AtomicInteger();
		private final AtomicInteger statementsClosed = new AtomicInteger(), connectionsClosed = new AtomicInteger();

		Fixture(Site site, Shape shape, int failingNullAttempt, SQLException failure, boolean driverRollback) {
			this.shape = shape;
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:binding_fallback_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			this.inspectionDatabase = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
			this.inspectionDatabase.query("CREATE TABLE binding_work (id INT PRIMARY KEY, value_text VARCHAR(40))").execute();
			AtomicBoolean armed = new AtomicBoolean(true);
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object value = invoke(source, method, args);
				if (!(value instanceof Connection connection)) return value;
				return proxy(Connection.class, (operation, parameters) -> {
					Object result = invoke(connection, operation, parameters);
					if (operation.getName().equals("commit")) commits.incrementAndGet();
					if (operation.getName().equals("rollback")) rollbacks.incrementAndGet();
					if (operation.getName().equals("close")) connectionsClosed.incrementAndGet();
					if (!(result instanceof PreparedStatement statement)) return result;
					boolean second = String.valueOf(parameters[0]).contains("VALUES (2,");
					AtomicInteger nullAttempt = new AtomicInteger();
					return proxy(PreparedStatement.class, (binding, values) -> {
						String name = binding.getName();
						if (second && name.equals("getParameterMetaData") && site == Site.METADATA && armed.compareAndSet(true, false))
							fail(connection, failure, driverRollback);
						if (second && name.equals("setNull")) {
							nullCalls.incrementAndGet();
							int attempt = nullAttempt.getAndIncrement();
							if (site == Site.SET_NULL && armed.get()) {
								if (attempt < failingNullAttempt)
									throw new SQLException("driver rejects this null type hint", "HY004");
								if (armed.compareAndSet(true, false)) fail(connection, failure, driverRollback);
							}
							// The storage column is VARCHAR; wrappers emulate drivers accepting container null hints.
							statement.setNull((Integer) values[0], Types.NULL);
							return null;
						}
						if (second && (name.equals("executeLargeUpdate") || name.equals("executeUpdate")))
							secondExecutions.incrementAndGet();
						Object bound = invoke(statement, binding, values);
						if (name.equals("close")) statementsClosed.incrementAndGet();
						if (second && bound instanceof ParameterMetaData metadata)
							return proxy(ParameterMetaData.class, (read, indexes) -> {
								boolean selected = site == Site.PARAMETER_TYPE && read.getName().equals("getParameterType")
										|| site == Site.PARAMETER_TYPE_NAME && read.getName().equals("getParameterTypeName");
								if (selected && armed.compareAndSet(true, false)) fail(connection, failure, driverRollback);
								return invoke(metadata, read, indexes);
							});
						return bound;
					});
				});
			});
			// DuckDB's dialect enables the supported STRUCT binding path; storage remains HSQLDB behind the proxy.
			this.database = Database.withDataSource(wrapped)
					.databaseType(shape == Shape.STRUCT ? DatabaseType.DUCK_DB : DatabaseType.GENERIC).build();
		}

		private void fail(Connection connection, SQLException failure, boolean rollback) throws SQLException {
			this.faultCalls.incrementAndGet();
			if (rollback) connection.rollback();
			throw failure;
		}

		private void executeStatements() {
			this.database.query("INSERT INTO binding_work (id, value_text) VALUES (1, 'earlier work')").execute();
			this.database.query("INSERT INTO binding_work (id, value_text) VALUES (2, :value)").bind("value", this.shape.parameter()).execute();
		}

		private void executeInTransaction() {
			this.database.transaction(() -> { executeStatements(); });
		}

		private List<Integer> ids() {
			return this.inspectionDatabase.query("SELECT id FROM binding_work ORDER BY id").fetchList(Integer.class);
		}

		private void assertAborted(int transactions) {
			assertEquals(List.of(), ids());
			assertEquals(0, this.secondExecutions.get());
			assertEquals(0, this.commits.get());
			assertEquals(transactions, this.rollbacks.get());
			assertEquals(1, this.faultCalls.get());
			assertTrue(this.database.currentTransaction().isEmpty());
			assertClosed(transactions);
		}

		private void assertClosed(int transactions) {
			assertEquals(transactions * 2, this.statementsClosed.get());
			assertEquals(transactions, this.connectionsClosed.get());
		}
	}

	@FunctionalInterface
	private interface Invocation {
		Object invoke(Method method, Object[] args) throws Throwable;
	}

	private static <T> T proxy(Class<T> type, Invocation invocation) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> invocation.invoke(method, args)));
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try {
			return method.invoke(target, args);
		} catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}
}
