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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.1.0
 */
public class TransactionLifecycleTests {
	@Test
	public void testNonTransactionalOperationsDoNotCreateTransactionStack() throws Exception {
		Database db = Database.withDataSource(createInMemoryDataSource("transaction_threadlocal_absent")).build();
		ThreadLocal<?> transactionStackHolder = transactionStackHolder();

		transactionStackHolder.remove();

		try {
			Assertions.assertTrue(db.currentTransaction().isEmpty());
			Assertions.assertNull(transactionStackHolder.get(), "currentTransaction() must not install an empty transaction stack");

			Integer value = db.query("SELECT 1 FROM (VALUES (0)) AS t(x)").fetchObject(Integer.class).orElseThrow();

			Assertions.assertEquals(1, value);
			Assertions.assertNull(transactionStackHolder.get(), "Non-transactional statements must not install an empty transaction stack");
		} finally {
			transactionStackHolder.remove();
		}
	}

	@Test
	public void testCompletedTransactionRejectsMutatingAndJdbcTouchingOperations() {
		Database db = Database.withDataSource(createInMemoryDataSource("transaction_terminal_state")).build();
		AtomicReference<Transaction> transactionReference = new AtomicReference<>();
		AtomicReference<Savepoint> savepointReference = new AtomicReference<>();
		Consumer<TransactionResult> postTransactionOperation = result -> {};

		db.transaction(() -> {
			Transaction transaction = db.currentTransaction().orElseThrow();
			transactionReference.set(transaction);
			db.query("SELECT 1 FROM (VALUES (0)) AS t(x)").fetchObject(Integer.class);
			savepointReference.set(transaction.createSavepoint());
		});

		Transaction transaction = transactionReference.get();
		Savepoint savepoint = savepointReference.get();

		Assertions.assertTrue(transaction.isCompleted(), "Expected transaction to be marked complete");
		Assertions.assertFalse(transaction.hasConnection(), "Expected completed transaction to clear its connection reference");
		Assertions.assertThrows(IllegalStateException.class, () -> transaction.setRollbackOnly(true));
		Assertions.assertThrows(IllegalStateException.class, () -> transaction.addPostTransactionOperation(postTransactionOperation));
		Assertions.assertThrows(IllegalStateException.class, () -> transaction.removePostTransactionOperation(postTransactionOperation));
		Assertions.assertThrows(IllegalStateException.class, transaction::getConnection);
		Assertions.assertThrows(IllegalStateException.class, transaction::createSavepoint);
		Assertions.assertThrows(IllegalStateException.class, () -> transaction.rollback(savepoint));
		Assertions.assertThrows(IllegalStateException.class, () -> transaction.releaseSavepoint(savepoint));
		Assertions.assertThrows(IllegalStateException.class, () -> transaction.withSavepoint(() -> {}));
	}

	@Test
	public void testWaitingParticipantCannotAcquireFreshConnectionAfterTransactionCompletes() throws Exception {
		DataSource dataSource = createInMemoryDataSource("transaction_waiting_participant_completion");
		Database db = Database.withDataSource(dataSource).build();
		Transaction transaction = new Transaction(dataSource, TransactionOptions.DEFAULT, db.getMetricsCollectorDispatcher(), db.peekDatabaseType());
		AtomicBoolean acquiredConnection = new AtomicBoolean(false);
		AtomicReference<Throwable> failure = new AtomicReference<>();

		transaction.getConnectionLock().lock();

		Thread participant = new Thread(() -> {
			try {
				transaction.getConnection();
				acquiredConnection.set(true);
			} catch (Throwable t) {
				failure.set(t);
			}
		});

		try {
			participant.start();
			awaitQueuedTransactionThread(transaction);
			transaction.markCompleted();
		} finally {
			transaction.getConnectionLock().unlock();
		}

		participant.join(1_000L);

		Assertions.assertFalse(participant.isAlive(), "Expected waiting participant to finish");
		Assertions.assertFalse(acquiredConnection.get(), "Completed transaction must not open a fresh connection");
		Assertions.assertInstanceOf(IllegalStateException.class, failure.get());
		Assertions.assertFalse(transaction.hasConnection());
	}

	@Test
	public void testParticipateRejectsCompletedTransactionBeforeRunningOperation() {
		DataSource dataSource = createInMemoryDataSource("participate_completed_transaction");
		Database db = Database.withDataSource(dataSource).build();
		Transaction transaction = new Transaction(dataSource, TransactionOptions.DEFAULT, db.getMetricsCollectorDispatcher(), db.peekDatabaseType());
		AtomicBoolean operationRan = new AtomicBoolean(false);

		transaction.markCompleted();

		IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () ->
				db.participate(transaction, () -> operationRan.set(true)));

		Assertions.assertTrue(exception.getMessage().contains("cannot participate"));
		Assertions.assertFalse(operationRan.get());
	}

	@Test
	public void testParticipatePreservesOriginalExceptionWhenTransactionCompletesBeforeRollbackOnlyMark() {
		DataSource dataSource = createInMemoryDataSource("participate_completion_race");
		Database db = Database.withDataSource(dataSource).build();
		Transaction transaction = new Transaction(dataSource, TransactionOptions.DEFAULT, db.getMetricsCollectorDispatcher(), db.peekDatabaseType());
		RuntimeException boom = new RuntimeException("participant boom");

		RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () ->
				db.participate(transaction, () -> {
					transaction.markCompleted();
					throw boom;
				}));

		Assertions.assertSame(boom, exception);
		assertSuppressedMessageContains(exception, "already completed");
	}

	@Test
	public void testNestedTransactionsRemainIndependent() {
		Database db = Database.withDataSource(createInMemoryDataSource("nested_transactions_independent")).build();

		db.query("CREATE TABLE outer_events (id INT)").execute();
		db.query("CREATE TABLE inner_events (id INT)").execute();

		Assertions.assertThrows(RuntimeException.class, () -> db.transaction(() -> {
			db.query("INSERT INTO outer_events(id) VALUES (1)").execute();

			db.transaction(() -> db.query("INSERT INTO inner_events(id) VALUES (2)").execute());

			throw new RuntimeException("outer rollback");
		}));

		Long outerCount = db.query("SELECT COUNT(*) FROM outer_events").fetchObject(Long.class).orElseThrow();
		Long innerCount = db.query("SELECT COUNT(*) FROM inner_events").fetchObject(Long.class).orElseThrow();

		Assertions.assertEquals(0L, outerCount, "Outer transaction should roll back");
		Assertions.assertEquals(1L, innerCount, "Inner independent transaction should commit");
	}

	@Test
	public void testWithSavepointSuccessCommitsSavepointWork() {
		Database db = Database.withDataSource(createInMemoryDataSource("savepoint_success")).build();

		db.query("CREATE TABLE events (id INT)").execute();

		db.transaction(() -> {
			Transaction transaction = db.currentTransaction().orElseThrow();
			db.query("INSERT INTO events(id) VALUES (1)").execute();
			transaction.withSavepoint(() -> db.query("INSERT INTO events(id) VALUES (2)").execute());
			db.query("INSERT INTO events(id) VALUES (3)").execute();
		});

		Assertions.assertEquals(List.of(1, 2, 3), db.query("SELECT id FROM events ORDER BY id").fetchList(Integer.class));
	}

	@Test
	public void testWithSavepointRuntimeExceptionRollsBackToSavepoint() {
		Database db = Database.withDataSource(createInMemoryDataSource("savepoint_runtime_rollback")).build();
		RuntimeException boom = new RuntimeException("boom");

		db.query("CREATE TABLE events (id INT)").execute();

		db.transaction(() -> {
			Transaction transaction = db.currentTransaction().orElseThrow();
			db.query("INSERT INTO events(id) VALUES (1)").execute();

			RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> transaction.withSavepoint(() -> {
				db.query("INSERT INTO events(id) VALUES (2)").execute();
				throw boom;
			}));

			Assertions.assertSame(boom, ex);
			db.query("INSERT INTO events(id) VALUES (3)").execute();
		});

		Assertions.assertEquals(List.of(1, 3), db.query("SELECT id FROM events ORDER BY id").fetchList(Integer.class));
	}

	@Test
	public void testWithSavepointErrorPropagatesAndRollsBackToSavepoint() {
		Database db = Database.withDataSource(createInMemoryDataSource("savepoint_error_rollback")).build();
		AssertionError boom = new AssertionError("boom");

		db.query("CREATE TABLE events (id INT)").execute();

		db.transaction(() -> {
			Transaction transaction = db.currentTransaction().orElseThrow();

			AssertionError ex = Assertions.assertThrows(AssertionError.class, () -> transaction.withSavepoint(() -> {
				db.query("INSERT INTO events(id) VALUES (1)").execute();
				throw boom;
			}));

			Assertions.assertSame(boom, ex);
			db.query("INSERT INTO events(id) VALUES (2)").execute();
		});

		Assertions.assertEquals(List.of(2), db.query("SELECT id FROM events ORDER BY id").fetchList(Integer.class));
	}

	@Test
	public void testWithSavepointCheckedExceptionIsWrappedAndRollsBack() {
		Database db = Database.withDataSource(createInMemoryDataSource("savepoint_checked_rollback")).build();
		Exception boom = new Exception("boom");

		db.query("CREATE TABLE events (id INT)").execute();

		db.transaction(() -> {
			Transaction transaction = db.currentTransaction().orElseThrow();

			RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> transaction.withSavepoint(() -> {
				db.query("INSERT INTO events(id) VALUES (1)").execute();
				throw boom;
			}));

			Assertions.assertSame(boom, ex.getCause());
			db.query("INSERT INTO events(id) VALUES (2)").execute();
		});

		Assertions.assertEquals(List.of(2), db.query("SELECT id FROM events ORDER BY id").fetchList(Integer.class));
	}

	@Test
	public void testNestedWithSavepointInnerFailsOuterSucceeds() {
		Database db = Database.withDataSource(createInMemoryDataSource("savepoint_inner_fails")).build();

		db.query("CREATE TABLE events (id INT)").execute();

		db.transaction(() -> {
			Transaction transaction = db.currentTransaction().orElseThrow();

			transaction.withSavepoint(() -> {
				db.query("INSERT INTO events(id) VALUES (1)").execute();

				Assertions.assertThrows(RuntimeException.class, () -> transaction.withSavepoint(() -> {
					db.query("INSERT INTO events(id) VALUES (2)").execute();
					throw new RuntimeException("inner rollback");
				}));

				db.query("INSERT INTO events(id) VALUES (3)").execute();
			});
		});

		Assertions.assertEquals(List.of(1, 3), db.query("SELECT id FROM events ORDER BY id").fetchList(Integer.class));
	}

	@Test
	public void testNestedWithSavepointInnerSucceedsOuterFails() {
		Database db = Database.withDataSource(createInMemoryDataSource("savepoint_outer_fails")).build();

		db.query("CREATE TABLE events (id INT)").execute();

		db.transaction(() -> {
			Transaction transaction = db.currentTransaction().orElseThrow();

			Assertions.assertThrows(RuntimeException.class, () -> transaction.withSavepoint(() -> {
				db.query("INSERT INTO events(id) VALUES (1)").execute();
				transaction.withSavepoint(() -> db.query("INSERT INTO events(id) VALUES (2)").execute());
				throw new RuntimeException("outer savepoint rollback");
			}));

			db.query("INSERT INTO events(id) VALUES (3)").execute();
		});

		Assertions.assertEquals(List.of(3), db.query("SELECT id FROM events ORDER BY id").fetchList(Integer.class));
	}

	@Test
	public void testWithSavepointSwallowsUnsupportedReleaseOnSuccess() {
		Database db = Database.withDataSource(new ReleaseSavepointThrowingDataSource(
				createInMemoryDataSource("savepoint_release_unsupported"), new SQLFeatureNotSupportedException("release unsupported")))
				.build();

		db.query("CREATE TABLE events (id INT)").execute();

		Assertions.assertDoesNotThrow(() -> db.transaction(() -> {
			Transaction transaction = db.currentTransaction().orElseThrow();
			transaction.withSavepoint(() -> db.query("INSERT INTO events(id) VALUES (1)").execute());
		}));

		Assertions.assertEquals(List.of(1), db.query("SELECT id FROM events ORDER BY id").fetchList(Integer.class));
	}

	@Test
	public void testWithSavepointThrowsReleaseFailureOnSuccess() {
		Database db = Database.withDataSource(new ReleaseSavepointThrowingDataSource(
				createInMemoryDataSource("savepoint_release_failure"), new SQLException("release failed")))
				.build();

		db.query("CREATE TABLE events (id INT)").execute();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () -> db.transaction(() -> {
			Transaction transaction = db.currentTransaction().orElseThrow();
			transaction.withSavepoint(() -> db.query("INSERT INTO events(id) VALUES (1)").execute());
		}));

		Assertions.assertTrue(ex.getMessage().contains("Unable to release savepoint"));
	}

	@Test
	public void testWithSavepointSuppressesRollbackFailure() {
		Database db = Database.withDataSource(new RollbackSavepointThrowingDataSource(
				createInMemoryDataSource("savepoint_rollback_failure"), new SQLException("rollback failed")))
				.build();
		RuntimeException boom = new RuntimeException("boom");

		db.query("CREATE TABLE events (id INT)").execute();

		RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> db.transaction(() -> {
			Transaction transaction = db.currentTransaction().orElseThrow();
			transaction.withSavepoint(() -> {
				db.query("INSERT INTO events(id) VALUES (1)").execute();
				throw boom;
			});
		}));

		Assertions.assertSame(boom, ex);
		assertSuppressedMessage(ex, "Unable to roll back to savepoint");
	}

	@Test
	public void testWithSavepointSuppressesReleaseFailureAfterRollback() {
		Database db = Database.withDataSource(new ReleaseSavepointThrowingDataSource(
				createInMemoryDataSource("savepoint_release_failure_after_rollback"), new SQLException("release failed")))
				.build();
		RuntimeException boom = new RuntimeException("boom");

		db.query("CREATE TABLE events (id INT)").execute();

		RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> db.transaction(() -> {
			Transaction transaction = db.currentTransaction().orElseThrow();
			transaction.withSavepoint(() -> {
				db.query("INSERT INTO events(id) VALUES (1)").execute();
				throw boom;
			});
		}));

		Assertions.assertSame(boom, ex);
		assertSuppressedMessage(ex, "Unable to release savepoint");
	}

	private void assertSuppressedMessage(@NonNull Throwable throwable,
																			 @NonNull String message) {
		requireNonNull(throwable);
		requireNonNull(message);

		Assertions.assertTrue(List.of(throwable.getSuppressed()).stream()
						.anyMatch(suppressed -> message.equals(suppressed.getMessage())),
				format("Expected suppressed exception message '%s'", message));
	}

	private void assertSuppressedMessageContains(@NonNull Throwable throwable,
																							 @NonNull String message) {
		requireNonNull(throwable);
		requireNonNull(message);

		Assertions.assertTrue(List.of(throwable.getSuppressed()).stream()
						.anyMatch(suppressed -> suppressed.getMessage() != null && suppressed.getMessage().contains(message)),
				format("Expected suppressed exception message containing '%s'", message));
	}

	private void awaitQueuedTransactionThread(@NonNull Transaction transaction) throws InterruptedException {
		requireNonNull(transaction);

		for (int i = 0; i < 100 && !transaction.getConnectionLock().hasQueuedThreads(); ++i)
			Thread.sleep(10L);

		Assertions.assertTrue(transaction.getConnectionLock().hasQueuedThreads(), "Expected participant thread to be waiting on transaction lock");
	}

	@NonNull
	private static ThreadLocal<?> transactionStackHolder() throws NoSuchFieldException, IllegalAccessException {
		Field transactionStackHolderField = Database.class.getDeclaredField("TRANSACTION_STACK_HOLDER");
		transactionStackHolderField.setAccessible(true);

		return (ThreadLocal<?>) transactionStackHolderField.get(null);
	}

	@NonNull
	private static DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}

	private abstract static class SavepointThrowingDataSource implements DataSource {
		@NonNull
		private final DataSource delegate;
		@NonNull
		private final SQLException exception;

		private SavepointThrowingDataSource(@NonNull DataSource delegate,
																				@NonNull SQLException exception) {
			this.delegate = requireNonNull(delegate);
			this.exception = requireNonNull(exception);
		}

		@Override
		public Connection getConnection() throws SQLException {
			return wrapConnection(this.delegate.getConnection());
		}

		@Override
		public Connection getConnection(String username,
																		String password) throws SQLException {
			return wrapConnection(this.delegate.getConnection(username, password));
		}

		@Override
		public PrintWriter getLogWriter() throws SQLException {
			return this.delegate.getLogWriter();
		}

		@Override
		public void setLogWriter(PrintWriter out) throws SQLException {
			this.delegate.setLogWriter(out);
		}

		@Override
		public void setLoginTimeout(int seconds) throws SQLException {
			this.delegate.setLoginTimeout(seconds);
		}

		@Override
		public int getLoginTimeout() throws SQLException {
			return this.delegate.getLoginTimeout();
		}

		@Override
		public Logger getParentLogger() {
			return Logger.getLogger(DataSource.class.getName());
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			return this.delegate.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return this.delegate.isWrapperFor(iface);
		}

		@NonNull
		protected SQLException getException() {
			return this.exception;
		}

		@NonNull
		private Connection wrapConnection(@NonNull Connection connection) {
			requireNonNull(connection);

			return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(proxy, method, args) -> {
						if (shouldThrow(method.getName(), args))
							throw getException();

						try {
							return method.invoke(connection, args);
						} catch (InvocationTargetException e) {
							throw e.getCause();
						}
					});
		}

		protected abstract boolean shouldThrow(@NonNull String methodName,
																					 @Nullable Object[] args);
	}

	private static final class ReleaseSavepointThrowingDataSource extends SavepointThrowingDataSource {
		private ReleaseSavepointThrowingDataSource(@NonNull DataSource delegate,
																							 @NonNull SQLException exception) {
			super(delegate, exception);
		}

		@Override
		protected boolean shouldThrow(@NonNull String methodName,
																	@Nullable Object[] args) {
			return "releaseSavepoint".equals(methodName);
		}
	}

	private static final class RollbackSavepointThrowingDataSource extends SavepointThrowingDataSource {
		private RollbackSavepointThrowingDataSource(@NonNull DataSource delegate,
																								@NonNull SQLException exception) {
			super(delegate, exception);
		}

		@Override
		protected boolean shouldThrow(@NonNull String methodName,
																	@Nullable Object[] args) {
			return "rollback".equals(methodName) && args != null && args.length == 1 && args[0] instanceof Savepoint;
		}
	}
}
