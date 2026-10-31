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
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class TransactionDiagnosticFailureTests {
	@AfterEach
	void clearInterruptFlag() {
		Thread.interrupted();
	}

	@Test
	void cancelledBeginRetainsItsFailureAndCannotAdmitAnotherAttempt() throws SQLException {
		for (String accessor : List.of("readable", "sqlState", "errorCode", "message", "cause")) {
			Fixture fixture = new Fixture();
			SQLException original = driverFailure(accessor, "57014", true);
			fixture.beginFailure = original;
			AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				conditions.incrementAndGet();
				return true;
			});

			try {
				DatabaseException thrown = assertThrows(DatabaseException.class,
						() -> fixture.database.transactionWithRetry(policy, () -> {
							attempts.incrementAndGet();
							fixture.database.query("VALUES (1)").fetchObject(Integer.class);
						}), accessor);

				assertTrue(Thread.currentThread().isInterrupted(), accessor);
				assertSame(original, thrown.getCause(), accessor);
				assertEquals(1, attempts.get(), accessor);
				assertEquals(0, conditions.get(), accessor);
				assertEquals(0, fixture.commits.get(), accessor);
				assertEquals(0, fixture.fullRollbacks.get(), accessor);
				assertEquals(1, fixture.aborts.get(), accessor);
				fixture.assertClosed();
			} finally {
				Thread.interrupted();
			}
		}
	}

	@Test
	void savepointRollbackDiagnosticsCannotReplaceTheBodyOrSkipRelease() throws SQLException {
		for (String accessor : List.of("readable", "sqlState", "errorCode", "message", "cause")) {
			Fixture fixture = new Fixture();
			SQLException original = driverFailure(accessor, "57014", true);
			fixture.savepointRollbackFailure = original;
			IllegalStateException business = new IllegalStateException("business failure");

			try {
				assertSame(business, assertThrows(IllegalStateException.class, () -> fixture.database.transaction(() ->
						fixture.database.currentTransaction().orElseThrow().withSavepoint(() -> { throw business; }))));

				assertTrue(Thread.currentThread().isInterrupted(), accessor);
				assertEquals(1, business.getSuppressed().length, accessor);
				assertSame(original, business.getSuppressed()[0].getCause(), accessor);
				assertEquals(1, fixture.savepointRollbacks.get(), accessor);
				assertEquals(1, fixture.releases.get(), accessor);
				assertEquals(1, fixture.fullRollbacks.get(), accessor);
				assertEquals(0, fixture.commits.get(), accessor);
				fixture.assertClosed();
			} finally {
				Thread.interrupted();
			}
		}
	}

	@Test
	void savepointReleaseDiagnosticsRemainSuppressedOnTheBusinessFailure() throws SQLException {
		for (String accessor : List.of("sqlState", "errorCode")) {
			Fixture fixture = new Fixture();
			SQLException original = driverFailure(accessor, "57014", true);
			fixture.releaseFailure = original;
			IllegalStateException business = new IllegalStateException("business failure");

			try {
				assertSame(business, assertThrows(IllegalStateException.class, () -> fixture.database.transaction(() ->
						fixture.database.currentTransaction().orElseThrow().withSavepoint(() -> { throw business; }))));

				assertTrue(Thread.currentThread().isInterrupted());
				assertSame(original, business.getSuppressed()[0].getCause());
				assertEquals(1, fixture.savepointRollbacks.get());
				assertEquals(1, fixture.releases.get());
				assertEquals(1, fixture.fullRollbacks.get());
				fixture.assertClosed();
			} finally {
				Thread.interrupted();
			}
		}
	}

	@Test
	void savepointCleanupSkipsTheSameFailureWithoutCreatingSuppressionCycles() throws SQLException {
		for (Throwable shared : List.of(new DatabaseException("shared database failure"), new AssertionError("shared fatal failure"))) {
			Fixture fixture = new Fixture();
			fixture.savepointRollbackFailure = shared;
			fixture.releaseFailure = shared;

			Throwable thrown = assertThrows(Throwable.class, () -> fixture.database.transaction(() ->
					fixture.database.currentTransaction().orElseThrow().withSavepoint(() -> {
						if (shared instanceof Error error)
							throw error;
						throw (RuntimeException) shared;
					})));

			assertSame(shared, thrown);
			assertArrayEquals(new Throwable[0], shared.getSuppressed());
			assertEquals(1, fixture.releases.get());
			assertEquals(1, fixture.fullRollbacks.get());
			fixture.assertClosed();
		}
	}

	@Test
	void unreadableCommitClassificationStaysIndeterminateAndNeverRetries() throws SQLException {
		for (String accessor : List.of("sqlState", "errorCode")) {
			Fixture fixture = new Fixture();
			SQLException original = driverFailure(accessor, "40001", false);
			fixture.commitFailure = original;
			AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
			AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
			TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completion::set).build();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				conditions.incrementAndGet();
				return true;
			});

			DatabaseException thrown = assertThrows(DatabaseException.class,
					() -> fixture.database.transactionWithRetry(policy, options, () -> {
						attempts.incrementAndGet();
						fixture.database.query("VALUES (1)").fetchObject(Integer.class);
					}));

			assertSame(original, thrown.getCause());
			assertFalse(thrown.isSerializationFailure());
			assertTrue(thrown.isTransactionOutcomeIndeterminate());
			assertEquals(TransactionResult.IN_DOUBT, completion.get().getResult());
			assertEquals(1, attempts.get());
			assertEquals(0, conditions.get());
			assertEquals(1, fixture.commits.get());
			assertEquals(1, fixture.fullRollbacks.get());
			assertEquals(1, fixture.aborts.get());
			fixture.assertClosed();
		}
	}

	@Test
	void unreadableFullRollbackPreservesTheBodyAndInterruptEvidence() throws SQLException {
		Fixture fixture = new Fixture();
		SQLException original = driverFailure("sqlState", "57014", true);
		fixture.fullRollbackFailure = original;
		IllegalStateException business = new IllegalStateException("business failure");
		AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
		TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completion::set).build();

		assertSame(business, assertThrows(IllegalStateException.class, () -> fixture.database.transaction(options, () -> {
			fixture.database.query("VALUES (1)").fetchObject(Integer.class);
			throw business;
		})));

		assertTrue(Thread.currentThread().isInterrupted());
		assertSame(original, business.getSuppressed()[0].getCause());
		assertEquals(TransactionResult.IN_DOUBT, completion.get().getResult());
		assertEquals(1, fixture.fullRollbacks.get());
		assertEquals(1, fixture.aborts.get());
		fixture.assertClosed();
	}

	@Test
	void transactionFailureWithoutACauseStillHasItsContext() {
		Fixture fixture = new Fixture();
		Transaction transaction = new Transaction(fixture.source, TransactionOptions.withReadOnly(null).build(),
				new MetricsCollectorDispatcher(null), DatabaseType.GENERIC);

		DatabaseException thrown = assertThrows(DatabaseException.class, () -> transaction.setAutoCommit(true));

		assertEquals("Transaction has no connection", thrown.getMessage());
		assertNull(thrown.getCause());
		assertEquals(0, fixture.connections.size());
	}

	private static SQLException driverFailure(String accessor, String sqlState, boolean cancelled) {
		InterruptedException interruption = cancelled ? new InterruptedException("cancelled") : null;
		SQLException failure = new SQLException("driver failure", sqlState, interruption) {
			@Override public String getSQLState() {
				if (accessor.equals("sqlState")) throw new IllegalStateException("unreadable SQLState");
				return super.getSQLState();
			}
			@Override public int getErrorCode() {
				if (accessor.equals("errorCode")) throw new IllegalStateException("unreadable error code");
				return super.getErrorCode();
			}
			@Override public String getMessage() {
				if (accessor.equals("message")) throw new IllegalStateException("unreadable message");
				return super.getMessage();
			}
			@Override public Throwable getCause() {
				if (accessor.equals("cause")) throw new IllegalStateException("unreadable cause");
				return super.getCause();
			}
		};
		// A broken cause accessor must not hide independent suppressed cancellation evidence.
		if (accessor.equals("cause") && interruption != null)
			failure.addSuppressed(interruption);
		return failure;
	}

	private static final class Fixture {
		private final JDBCDataSource source;
		private final Database database;
		private final List<Connection> connections = new ArrayList<>();
		private final AtomicInteger commits = new AtomicInteger(), fullRollbacks = new AtomicInteger();
		private final AtomicInteger savepointRollbacks = new AtomicInteger(), releases = new AtomicInteger();
		private final AtomicInteger closes = new AtomicInteger(), aborts = new AtomicInteger();
		private Throwable beginFailure, savepointRollbackFailure, releaseFailure, commitFailure, fullRollbackFailure;

		private Fixture() {
			this.source = new JDBCDataSource();
			this.source.setUrl("jdbc:hsqldb:mem:transaction_diagnostic_failure_" + UUID.randomUUID());
			this.source.setUser("sa");
			this.source.setPassword("");
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(this.source, method, args);
				if (!(result instanceof Connection connection))
					return result;
				this.connections.add(connection);
				return proxy(Connection.class, (operation, parameters) -> {
					String name = operation.getName();
					if (name.equals("getAutoCommit") && this.beginFailure != null) {
						Throwable failure = this.beginFailure;
						this.beginFailure = null;
						throw failure;
					}
					if (name.equals("commit")) {
						this.commits.incrementAndGet();
						if (this.commitFailure != null) throw this.commitFailure;
					}
					if (name.equals("rollback")) {
						if (parameters == null || parameters.length == 0) {
							this.fullRollbacks.incrementAndGet();
							if (this.fullRollbackFailure != null) throw this.fullRollbackFailure;
						} else {
							this.savepointRollbacks.incrementAndGet();
							if (this.savepointRollbackFailure != null) throw this.savepointRollbackFailure;
						}
					}
					if (name.equals("releaseSavepoint")) {
						this.releases.incrementAndGet();
						if (this.releaseFailure != null) throw this.releaseFailure;
					}
					if (name.equals("abort")) {
						this.aborts.incrementAndGet();
						return null;
					}
					Object value = invoke(connection, operation, parameters);
					if (name.equals("close")) this.closes.incrementAndGet();
					return value;
				});
			});
			this.database = Database.withDataSource(wrapped).databaseType(DatabaseType.GENERIC).build();
		}

		private void assertClosed() throws SQLException {
			assertEquals(1, this.connections.size());
			assertEquals(1, this.closes.get());
			assertTrue(this.connections.get(0).isClosed());
			assertTrue(this.database.currentTransaction().isEmpty());
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
