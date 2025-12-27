/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2025 Revetware LLC.
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.jspecify.annotations.NonNull;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 */
public class TransactionConcurrencyTests {
	@Test
	public void testParticipateSerializesConnectionUsage() throws Exception {
		String databaseName = "txn_concurrency";
		ConcurrencyGuard guard = new ConcurrencyGuard();
		DataSource dataSource = new GuardedDataSource(createInMemoryDataSource(databaseName), guard);
		Database database = Database.withDataSource(dataSource).build();

		TestQueries.execute(database, "CREATE TABLE t (id INT)");

		guard.enable();

		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			CyclicBarrier barrier = new CyclicBarrier(2);
			AtomicReference<Throwable> failure = new AtomicReference<>();

			Thread first = new Thread(() -> runParticipate(database, transaction, barrier, failure, 1));
			Thread second = new Thread(() -> runParticipate(database, transaction, barrier, failure, 2));

			first.start();
			second.start();

			try {
				if (!guard.awaitFirstEntered(1, TimeUnit.SECONDS))
					throw new RuntimeException("Timed out waiting for concurrent execution");

				guard.awaitSecondEntered(200, TimeUnit.MILLISECONDS);
				guard.releaseFirst();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}

			try {
				first.join();
				second.join();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}

			if (failure.get() != null)
				throw new RuntimeException(failure.get());
		});

		guard.disable();

		Assertions.assertFalse(guard.isConcurrent(), "Expected serialized connection usage within transaction");
		Long count = database.query("SELECT COUNT(*) FROM t")
				.fetchObject(Long.class)
				.orElse(0L);
		Assertions.assertEquals(2L, count);
	}

	private void runParticipate(@NonNull Database database,
															@NonNull Transaction transaction,
															@NonNull CyclicBarrier barrier,
															@NonNull AtomicReference<Throwable> failure,
															int id) {
		requireNonNull(database);
		requireNonNull(transaction);
		requireNonNull(barrier);
		requireNonNull(failure);

		try {
			database.participate(transaction, () -> {
				try {
					barrier.await(1, TimeUnit.SECONDS);
					TestQueries.execute(database, "INSERT INTO t (id) VALUES (?)", id);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		} catch (RuntimeException e) {
			failure.compareAndSet(null, e);
		}
	}

	private DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}

	private static final class ConcurrencyGuard {
		private final AtomicBoolean enabled;
		private final AtomicBoolean concurrent;
		private final AtomicInteger active;
		private volatile CountDownLatch firstEntered;
		private volatile CountDownLatch secondEntered;
		private volatile CountDownLatch allowProceed;

		private ConcurrencyGuard() {
			this.enabled = new AtomicBoolean(false);
			this.concurrent = new AtomicBoolean(false);
			this.active = new AtomicInteger(0);
			this.firstEntered = new CountDownLatch(1);
			this.secondEntered = new CountDownLatch(1);
			this.allowProceed = new CountDownLatch(1);
		}

		private void enable() {
			this.concurrent.set(false);
			this.active.set(0);
			this.firstEntered = new CountDownLatch(1);
			this.secondEntered = new CountDownLatch(1);
			this.allowProceed = new CountDownLatch(1);
			this.enabled.set(true);
		}

		private void disable() {
			this.enabled.set(false);
		}

		private void enter() {
			if (!this.enabled.get())
				return;

			int current = this.active.incrementAndGet();

			if (current == 1) {
				this.firstEntered.countDown();

				try {
					this.allowProceed.await(200, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			} else {
				this.concurrent.set(true);
				this.secondEntered.countDown();
			}
		}

		private void exit() {
			if (!this.enabled.get())
				return;

			this.active.decrementAndGet();
		}

		private boolean awaitFirstEntered(long timeout, @NonNull TimeUnit unit) throws InterruptedException {
			requireNonNull(unit);
			return this.firstEntered.await(timeout, unit);
		}

		private boolean awaitSecondEntered(long timeout, @NonNull TimeUnit unit) throws InterruptedException {
			requireNonNull(unit);
			return this.secondEntered.await(timeout, unit);
		}

		private void releaseFirst() {
			this.allowProceed.countDown();
		}

		private boolean isConcurrent() {
			return this.concurrent.get();
		}
	}

	private static final class GuardedDataSource implements DataSource {
		@NonNull
		private final DataSource delegate;
		@NonNull
		private final ConcurrencyGuard guard;

		private GuardedDataSource(@NonNull DataSource delegate,
															@NonNull ConcurrencyGuard guard) {
			this.delegate = requireNonNull(delegate);
			this.guard = requireNonNull(guard);
		}

		@Override
		public Connection getConnection() throws SQLException {
			return wrapConnection(this.delegate.getConnection());
		}

		@Override
		public Connection getConnection(String username, String password) throws SQLException {
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
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			return this.delegate.getParentLogger();
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			if (iface.isInstance(this))
				return iface.cast(this);

			return this.delegate.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return iface.isInstance(this) || this.delegate.isWrapperFor(iface);
		}

		private Connection wrapConnection(@NonNull Connection connection) {
			requireNonNull(connection);

			return (Connection) Proxy.newProxyInstance(
					Connection.class.getClassLoader(),
					new Class<?>[]{Connection.class},
					(proxy, method, args) -> {
						Object result = invoke(method, connection, args);

						if (result instanceof PreparedStatement && "prepareStatement".equals(method.getName()))
							return wrapPreparedStatement((PreparedStatement) result);

						return result;
					});
		}

		private PreparedStatement wrapPreparedStatement(@NonNull PreparedStatement preparedStatement) {
			requireNonNull(preparedStatement);

			return (PreparedStatement) Proxy.newProxyInstance(
					PreparedStatement.class.getClassLoader(),
					new Class<?>[]{PreparedStatement.class},
					(proxy, method, args) -> {
						if (isExecuteMethod(method)) {
							this.guard.enter();
							try {
								return invoke(method, preparedStatement, args);
							} finally {
								this.guard.exit();
							}
						}

						return invoke(method, preparedStatement, args);
					});
		}

		private boolean isExecuteMethod(@NonNull Method method) {
			requireNonNull(method);
			String name = method.getName();
			return name.equals("execute")
					|| name.equals("executeQuery")
					|| name.equals("executeUpdate")
					|| name.equals("executeLargeUpdate")
					|| name.equals("executeBatch")
					|| name.equals("executeLargeBatch");
		}

		private Object invoke(@NonNull Method method,
													@NonNull Object target,
													Object[] args) throws Throwable {
			requireNonNull(method);
			requireNonNull(target);

			try {
				return method.invoke(target, args);
			} catch (InvocationTargetException e) {
				throw e.getCause();
			}
		}
	}
}
