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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTransactionRollbackException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class SavepointReleaseFailureTests {
	@AfterEach
	void clearInterruptFlag() { Thread.interrupted(); }

	@Test
	void optionalReleaseCannotHideRollbackAndCommitOnlyLaterWrites() {
		for (String edge : List.of("next", "cause", "suppressed", "cycle", "unreadable")) {
			SQLException original = optionalFailure(edge, new SQLTransactionRollbackException("server rolled back", "40001"));
			Fixture fixture = new Fixture((connection, release) -> {
				if (release == 1) {
					connection.rollback();
					throw original;
				}
			});

			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transaction(() -> {
				fixture.inSavepoint(() -> fixture.insert(1));
				fixture.insert(2);
			}), edge);

			assertTrue(contains(thrown, original), edge);
			assertEquals(List.of(), fixture.ids());
			assertEquals(0, fixture.commits.get());
			assertEquals(1, fixture.fullRollbacks.get());
			assertEquals(1, fixture.savepointRollbacks.get());
			fixture.assertClosed(1);
		}
	}

	@Test
	void optionalReleaseCancellationCannotAdmitAnotherAttempt() {
		for (String edge : List.of("cause", "next", "suppressed", "cycle", "unreadable")) {
			SQLException original = optionalFailure(edge, new SQLException("cancelled", "57014", new InterruptedException("release cancelled")));
			Fixture fixture = new Fixture((connection, release) -> { throw original; });
			AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();

			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(
					retry(conditions), () -> {
						attempts.incrementAndGet();
						fixture.inSavepoint(() -> fixture.insert(1));
						fixture.insert(2);
					}), edge);

			assertTrue(Thread.interrupted(), edge);
			assertTrue(contains(thrown, original), edge);
			assertEquals(1, attempts.get());
			assertEquals(0, conditions.get());
			assertEquals(List.of(), fixture.ids());
			assertEquals(0, fixture.commits.get());
			assertEquals(2, fixture.releases.get());
			assertEquals(1, fixture.fullRollbacks.get());
			fixture.assertClosed(1);
		}
	}

	@Test
	void cleanupReleaseCancellationRetainsTheBodyAndFencesRetryAfterCleanup() {
		for (String edge : List.of("cause", "next", "suppressed", "cycle", "unreadable")) {
			SQLException original = optionalFailure(edge, new SQLException("cancelled", "57014", new InterruptedException("release cancelled")));
			DatabaseException body = new DatabaseException(new SQLTransactionRollbackException("retryable body", "40001"));
			Fixture fixture = new Fixture((connection, release) -> { throw original; });
			AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
			AtomicBoolean interruptedInside = new AtomicBoolean(), retainedInside = new AtomicBoolean();

			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(
					retry(conditions), () -> {
						attempts.incrementAndGet();
						try {
							fixture.inSavepoint(() -> { fixture.insert(1); throw body; });
						} catch (DatabaseException failure) {
							interruptedInside.set(Thread.currentThread().isInterrupted());
							retainedInside.set(contains(failure, original));
							throw failure;
						}
					}), edge);

			assertSame(body, thrown);
			assertTrue(Thread.interrupted(), edge);
			assertFalse(interruptedInside.get(), "Guard must not publish cancellation inside the transaction");
			assertTrue(retainedInside.get());
			assertEquals(1, attempts.get());
			assertEquals(0, conditions.get());
			assertEquals(1, fixture.releases.get());
			assertEquals(1, fixture.savepointRollbacks.get());
			assertEquals(1, fixture.fullRollbacks.get());
			assertEquals(0, fixture.commits.get());
			assertEquals(List.of(), fixture.ids());
			fixture.assertClosed(1);
		}
	}

	@Test
	void releaseSerializationFailureRetriesTheEntireClosure() {
		SQLException original = optionalFailure("next", new SQLTransactionRollbackException("server rolled back", "40001"));
		Fixture fixture = new Fixture((connection, release) -> {
			if (release == 1) { connection.rollback(); throw original; }
		});
		AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();

		TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(retry(conditions), () -> {
			attempts.incrementAndGet();
			fixture.inSavepoint(() -> fixture.insert(1));
			fixture.insert(2);
		});

		assertEquals(2, attempts.get());
		assertEquals(2, result.getAttemptCount());
		assertEquals(1, conditions.get());
		assertTrue(contains(result.getFailures().get(0), original));
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(1, fixture.commits.get());
		assertEquals(1, fixture.fullRollbacks.get());
		fixture.assertClosed(2);
	}

	@Test
	void fatalEvidenceInOptionalReleaseCannotBecomeSuccessfulWork() {
		AssertionError fatal = new AssertionError("fatal release");
		SQLException original = optionalFailure("suppressed", fatal);
		Fixture fixture = new Fixture((connection, release) -> { throw original; });
		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transaction(
				() -> fixture.inSavepoint(() -> fixture.insert(1))));

		assertTrue(contains(thrown, fatal));
		assertEquals(List.of(), fixture.ids());
		assertEquals(0, fixture.commits.get());
		fixture.assertClosed(1);
	}

	@Test
	void benignUnsupportedReleaseKeepsSuccessfulScopesWorking() {
		Fixture fixture = new Fixture((connection, release) -> {
			throw new SQLFeatureNotSupportedException("optional release", "0A000");
		});

		fixture.database.transaction(() -> {
			fixture.inSavepoint(() -> fixture.insert(1));
			fixture.insert(2);
		});

		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(1, fixture.commits.get());
		assertEquals(0, fixture.fullRollbacks.get());
		assertFalse(Thread.currentThread().isInterrupted());
		fixture.assertClosed(1);
	}

	@Test
	void benignUnsupportedCleanupReleaseDoesNotBlockWholeTransactionRetry() {
		Fixture fixture = new Fixture((connection, release) -> {
			throw new SQLFeatureNotSupportedException("optional release", "0A000");
		});
		AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();

		TransactionRetryResult<Void> result = fixture.database.transactionWithRetry(retry(conditions), () -> {
			int attempt = attempts.incrementAndGet();
			fixture.inSavepoint(() -> {
				fixture.insert(1);
				if (attempt == 1) throw new DatabaseException(new SQLTransactionRollbackException("retry body", "40001"));
			});
			fixture.insert(2);
		});

		assertEquals(2, attempts.get());
		assertEquals(2, result.getAttemptCount());
		assertEquals(1, conditions.get());
		assertArrayEquals(new Throwable[0], result.getFailures().get(0).getSuppressed());
		assertEquals(List.of(1, 2), fixture.ids());
		assertEquals(1, fixture.commits.get());
		fixture.assertClosed(2);
	}

	@Test
	void handledBodyCannotCommitPartialWorkAfterTerminalCleanupReleaseRollback() {
		for (boolean typed : new boolean[]{true, false}) {
			SQLException original = typed
					? optionalFailure("next", new SQLTransactionRollbackException("server rolled back", "40001"))
					: new SQLTransactionRollbackException("server rolled back", "40001");
			Fixture fixture = new Fixture((connection, release) -> { connection.rollback(); throw original; });
			IllegalStateException body = new IllegalStateException("handled body failure");
			AtomicReference<Throwable> caught = new AtomicReference<>();
			AtomicBoolean rollbackOnly = new AtomicBoolean(), interruptedInside = new AtomicBoolean();

			fixture.database.transaction(() -> {
				fixture.insert(0);
				try {
					fixture.inSavepoint(() -> { fixture.insert(1); throw body; });
				} catch (IllegalStateException failure) {
					caught.set(failure);
					rollbackOnly.set(fixture.database.currentTransaction().orElseThrow().isRollbackOnly());
					interruptedInside.set(Thread.currentThread().isInterrupted());
				}
				fixture.insert(2);
			});

			assertEquals(List.of(), fixture.ids(), "Handled body failure must not hide loss of outer writes");
			assertSame(body, caught.get());
			assertTrue(contains(body, original));
			assertTrue(rollbackOnly.get());
			assertFalse(interruptedInside.get());
			assertFalse(Thread.currentThread().isInterrupted());
			assertEquals(0, fixture.commits.get());
			assertEquals(1, fixture.fullRollbacks.get());
			assertEquals(1, fixture.savepointRollbacks.get());
			fixture.assertClosed(1);
		}
	}

	@Test
	void handledCleanupReleaseCancellationMarksRollbackOnlyWithoutPublishingInterrupt() {
		for (boolean typed : new boolean[]{true, false}) {
			SQLException cancelled = new SQLException("release cancelled", "57014", new InterruptedException("cancelled"));
			SQLException original = typed ? optionalFailure("cause", cancelled) : cancelled;
			Fixture fixture = new Fixture((connection, release) -> { throw original; });
			IllegalStateException body = new IllegalStateException("handled body failure");
			AtomicReference<Throwable> caught = new AtomicReference<>();
			AtomicBoolean interruptedInside = new AtomicBoolean();

			fixture.database.transaction(() -> {
				fixture.insert(0);
				try {
					fixture.inSavepoint(() -> { fixture.insert(1); throw body; });
				} catch (IllegalStateException failure) {
					caught.set(failure);
					interruptedInside.set(Thread.currentThread().isInterrupted());
				}
				fixture.insert(2);
			});

			assertEquals(List.of(), fixture.ids());
			assertSame(body, caught.get());
			assertTrue(contains(body, original));
			assertFalse(interruptedInside.get());
			// A caller that handles cancellation inside the closure retains the existing publication boundary.
			assertFalse(Thread.currentThread().isInterrupted());
			assertEquals(0, fixture.commits.get());
			assertEquals(1, fixture.fullRollbacks.get());
			fixture.assertClosed(1);
		}
	}

	@Test
	void benignUnsupportedCleanupAfterHandledBodyKeepsOuterWork() {
		Fixture fixture = new Fixture((connection, release) -> {
			throw new SQLFeatureNotSupportedException("optional release", "0A000");
		});
		IllegalStateException body = new IllegalStateException("handled body failure");
		AtomicReference<Throwable> caught = new AtomicReference<>();
		AtomicBoolean rollbackOnly = new AtomicBoolean();

		fixture.database.transaction(() -> {
			fixture.insert(0);
			try {
				fixture.inSavepoint(() -> { fixture.insert(1); throw body; });
			} catch (IllegalStateException failure) {
				caught.set(failure);
				rollbackOnly.set(fixture.database.currentTransaction().orElseThrow().isRollbackOnly());
			}
			fixture.insert(2);
		});

		assertEquals(List.of(0, 2), fixture.ids());
		assertSame(body, caught.get());
		assertFalse(rollbackOnly.get());
		assertArrayEquals(new Throwable[0], body.getSuppressed());
		assertFalse(Thread.currentThread().isInterrupted());
		assertEquals(1, fixture.commits.get());
		assertEquals(0, fixture.fullRollbacks.get());
		fixture.assertClosed(1);
	}

	private static RetryPolicy retry(AtomicInteger conditions) {
		return RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			conditions.incrementAndGet();
			return true;
		});
	}

	private static SQLException optionalFailure(String edge, Throwable terminal) {
		SQLException original = edge.equals("unreadable")
				? new SQLFeatureNotSupportedException("optional release", "0A000") {
					@Override public String getSQLState() { throw new IllegalStateException("unreadable release state"); }
				} : new SQLFeatureNotSupportedException("optional release", "0A000");
		switch (edge) {
			case "cause" -> original.initCause(terminal);
			case "next", "cycle" -> {
				SQLException sql = (SQLException) terminal;
				original.setNextException(sql);
				if (edge.equals("cycle")) sql.setNextException(original);
			}
			default -> original.addSuppressed(terminal);
		}
		return original;
	}

	private static boolean contains(Throwable failure, Throwable original) {
		return ThrowableGraph.inspect(failure, node -> node == original ? 1 : 0, 0) != 0;
	}

	private static final class Fixture {
		private final Database database, inspection;
		private final AtomicInteger connections = new AtomicInteger(), closes = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger(), fullRollbacks = new AtomicInteger();
		private final AtomicInteger savepointRollbacks = new AtomicInteger(), releases = new AtomicInteger();

		private Fixture(ReleaseFault fault) {
			JDBCDataSource nativeSource = new JDBCDataSource();
			nativeSource.setUrl("jdbc:hsqldb:mem:savepoint_release_fallback_" + UUID.randomUUID());
			nativeSource.setUser("sa");
			nativeSource.setPassword("");
			this.inspection = Database.withDataSource(nativeSource).databaseType(DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
			DataSource source = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(nativeSource, method, args);
				if (!(result instanceof Connection connection)) return result;
				this.connections.incrementAndGet();
				return proxy(Connection.class, (operation, parameters) -> {
					String name = operation.getName();
					if (name.equals("commit")) this.commits.incrementAndGet();
					if (name.equals("rollback")) {
						if (parameters == null || parameters.length == 0) this.fullRollbacks.incrementAndGet();
						else this.savepointRollbacks.incrementAndGet();
					}
					if (name.equals("close")) this.closes.incrementAndGet();
					if (name.equals("releaseSavepoint")) fault.beforeRelease(connection, this.releases.incrementAndGet());
					return invoke(connection, operation, parameters);
				});
			});
			this.database = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
		}

		private void inSavepoint(TransactionalOperation operation) {
			this.database.currentTransaction().orElseThrow().withSavepoint(operation);
		}

		private void insert(int id) { this.database.query("INSERT INTO work VALUES (" + id + ")").execute(); }
		private List<Integer> ids() { return this.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class); }
		private void assertClosed(int expected) {
			assertEquals(expected, this.connections.get());
			assertEquals(expected, this.closes.get());
			assertTrue(this.database.currentTransaction().isEmpty());
		}
	}

	@FunctionalInterface
	private interface ReleaseFault { void beforeRelease(Connection connection, int release) throws Throwable; }
	@FunctionalInterface
	private interface Invocation { Object call(Method method, Object[] arguments) throws Throwable; }

	private static <T> T proxy(Class<T> type, Invocation invocation) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(object, method, arguments) -> invocation.call(method, arguments)));
	}
	private static Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
		try { return method.invoke(target, arguments); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
