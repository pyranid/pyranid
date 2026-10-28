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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class TransactionUnwindSafetyTests {
	@Test
	void cyclicCausesCannotBlockConnectionReleaseOrCompletionObservation() throws Exception {
		Fixture fixture = new Fixture();
		AtomicBoolean cyclic = new AtomicBoolean(true);
		AtomicReference<Throwable> next = new AtomicReference<>();
		RuntimeException primary = new IllegalStateException("original failure") {
			@Override public Throwable getCause() { return cyclic.get() ? next.get() : null; }
		};
		next.set(new IllegalStateException("cause", primary));
		AtomicReference<Throwable> thrown = new AtomicReference<>();
		AtomicBoolean bindingCleared = new AtomicBoolean(), interrupted = new AtomicBoolean();
		Thread worker = new Thread(() -> {
			try { fixture.fail(primary); }
			catch (Throwable failure) { thrown.set(failure); }
			bindingCleared.set(fixture.database.currentTransaction().isEmpty());
			interrupted.set(Thread.currentThread().isInterrupted());
		}, "transaction-cyclic-unwind-test");
		worker.setDaemon(true);
		worker.start();
		try {
			assertTrue(fixture.rolledBack.await(5, TimeUnit.SECONDS));
			worker.join(5_000);
			assertFalse(worker.isAlive(), "cyclic cause inspection prevented transaction cleanup");
			assertSame(primary, thrown.get());
			assertTrue(bindingCleared.get()); assertFalse(interrupted.get());
			fixture.assertCompleted(primary);
		} finally {
			// A regression must fail without leaving a spinning worker in the test JVM.
			cyclic.set(false);
			worker.join(5_000);
		}
	}

	@Test
	void throwingCauseAccessorsCannotReplacePrimaryOrHideSuppressedInterrupts() {
		for (Throwable inspectionFailure : List.of(new IllegalStateException("cause accessor failed"),
				new AssertionError("cause accessor failed"))) {
			for (boolean interrupted : List.of(false, true)) {
				Fixture fixture = new Fixture();
				RuntimeException primary = new IllegalStateException("original failure") {
					@Override public Throwable getCause() {
						if (inspectionFailure instanceof RuntimeException runtime) throw runtime;
						throw (Error) inspectionFailure;
					}
				};
				InterruptedException interrupt = new InterruptedException("interrupted operation");
				if (interrupted) primary.addSuppressed(interrupt);
				try {
					assertSame(primary, assertThrows(RuntimeException.class, () -> fixture.fail(primary)));
					assertEquals(interrupted, Thread.currentThread().isInterrupted());
					assertTrue(fixture.database.currentTransaction().isEmpty());
					assertArrayEquals(interrupted ? new Throwable[]{interrupt} : new Throwable[0], primary.getSuppressed());
					fixture.assertCompleted(primary);
				} finally {
					Thread.interrupted();
				}
			}
		}
	}

	@Test
	void jdbcNextExceptionsRestoreInterruptsAndPreservePrimaryDuringCleanup() {
		for (boolean unreadableCause : List.of(false, true)) {
			Fixture fixture = new Fixture();
			SQLException first = new SQLException("first") {
				@Override public Throwable getCause() {
					if (unreadableCause) throw new AssertionError("cause accessor failed");
					return super.getCause();
				}
			};
			SQLException second = new SQLException("second");
			InterruptedException interrupted = new InterruptedException("driver interrupted");
			second.initCause(interrupted);
			first.setNextException(second); second.setNextException(first);
			RuntimeException primary = new IllegalStateException("operation failed", first);
			try {
				assertSame(primary, assertThrows(RuntimeException.class, () -> fixture.fail(primary)));
				assertTrue(Thread.currentThread().isInterrupted());
				assertSame(second, first.getNextException()); assertSame(first, second.getNextException());
				assertSame(interrupted, second.getCause());
				fixture.assertCompleted(primary);
			} finally {
				Thread.interrupted();
			}
		}
	}

	@Test
	void unreadableJdbcNextExceptionDoesNotReplacePrimaryOrInterruptWithoutEvidence() {
		Fixture fixture = new Fixture();
		SQLException diagnostic = new SQLException("diagnostic") {
			@Override public SQLException getNextException() { throw new AssertionError("next accessor failed"); }
		};
		RuntimeException primary = new IllegalStateException("original failure", diagnostic);
		assertSame(primary, assertThrows(RuntimeException.class, () -> fixture.fail(primary)));
		assertFalse(Thread.currentThread().isInterrupted());
		assertSame(diagnostic, primary.getCause());
		fixture.assertCompleted(primary);
	}

	private static final class Fixture {
		private final AtomicInteger closes = new AtomicInteger(), observations = new AtomicInteger(), hooks = new AtomicInteger();
		private final AtomicReference<TransactionCompletion> observed = new AtomicReference<>();
		private final CountDownLatch rolledBack = new CountDownLatch(1);
		private final Database database;

		private Fixture() {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:unwind_safety_" + UUID.randomUUID());
			source.setUser("sa"); source.setPassword("");
			DataSource wrapped = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> {
						Object value = invoke(source, method, args);
						if (value instanceof Connection connection)
							return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
									(connectionProxy, operation, parameters) -> {
										Object result = invoke(connection, operation, parameters);
										if (operation.getName().equals("rollback")) this.rolledBack.countDown();
										if (operation.getName().equals("close")) this.closes.incrementAndGet();
										return result;
									});
						return value;
					});
			this.database = Database.withDataSource(wrapped).databaseType(DatabaseType.GENERIC).build();
		}

		private void fail(RuntimeException primary) {
			TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completion -> {
				this.observations.incrementAndGet(); this.observed.set(completion);
			}).build();
			this.database.transaction(options, () -> {
				this.database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> this.hooks.incrementAndGet());
				this.database.query("VALUES (1)").fetchObject(Integer.class);
				throw primary;
			});
		}

		private void assertCompleted(Throwable primary) {
			assertEquals(0, this.rolledBack.getCount()); assertEquals(1, this.closes.get());
			assertEquals(1, this.observations.get()); assertEquals(1, this.hooks.get());
			assertEquals(TransactionResult.ROLLED_BACK, this.observed.get().getResult());
			assertSame(primary, this.observed.get().getFailure().orElseThrow());
		}
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
