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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class AcquisitionDiagnosticFailureTests {
	private static final List<String> ACCESSORS = List.of("readable", "sqlState", "errorCode", "message", "cause");

	@AfterEach
	void clearInterruptFlag() {
		Thread.interrupted();
	}

	@Test
	void rawMetadataAndHealthCheckoutRetainCancellationDespiteBrokenDiagnostics() {
		for (String accessor : ACCESSORS) {
			for (String entry : List.of("raw", "typedRaw", "metadata", "health")) {
				Fixture fixture = new Fixture(true, null);
				SQLException original = cancellation(accessor);
				fixture.checkoutFailure = original;
				try {
					DatabaseException thrown = assertThrows(DatabaseException.class, () -> {
						switch (entry) {
							case "raw" -> fixture.database.useRawConnection(connection -> Optional.empty());
							case "typedRaw" -> fixture.database.useRawConnection(Connection.class, connection -> Optional.empty());
							case "metadata" -> fixture.database.readDatabaseMetaData(metadata -> fail("Checkout failed"));
							case "health" -> fixture.database.performHealthCheck(Duration.ofSeconds(1));
						}
					});

					assertSame(original, thrown.getCause(), accessor + " " + entry);
					assertTrue(Thread.currentThread().isInterrupted(), accessor + " " + entry);
					assertEquals(0, fixture.connections.get());
					assertEquals(0, fixture.closes.get());
					assertTrue(fixture.database.currentTransaction().isEmpty());
				} finally {
					Thread.interrupted();
				}
			}
		}
	}

	@Test
	void healthValidationRetainsCancellationAndClosesTheBorrowedConnection() {
		for (String accessor : ACCESSORS) {
			Fixture fixture = new Fixture(true, null);
			SQLException original = cancellation(accessor);
			fixture.healthFailure = original;
			try {
				DatabaseException thrown = assertThrows(DatabaseException.class,
						() -> fixture.database.performHealthCheck(Duration.ofSeconds(1)));

				assertSame(original, thrown.getCause(), accessor);
				assertTrue(Thread.currentThread().isInterrupted(), accessor);
				assertEquals(1, fixture.connections.get());
				assertEquals(1, fixture.closes.get());
			} finally {
				Thread.interrupted();
			}
		}
	}

	@Test
	void statementAndStreamCheckoutRetainTheDriverFailureAndReportAcquisitionMetrics() {
		for (String accessor : ACCESSORS) {
			for (boolean stream : new boolean[]{false, true}) {
				AtomicInteger reports = new AtomicInteger();
				AtomicReference<Throwable> observed = new AtomicReference<>();
				MetricsCollector collector = new MetricsCollector() {
					@Override
					public void didFailToAcquireStatementConnection(StatementContext<?> context, DatabaseType type,
							Duration elapsed, Throwable failure) {
						assertNotNull(elapsed);
						reports.incrementAndGet();
						observed.set(failure);
					}
				};
				Fixture fixture = new Fixture(true, collector);
				SQLException original = cancellation(accessor);
				fixture.checkoutFailure = original;

				DatabaseException thrown = assertThrows(DatabaseException.class, () -> {
					if (stream) fixture.database.query("VALUES (1)").fetchStream(Integer.class, values -> values.findFirst());
					else fixture.database.query("VALUES (1)").fetchObject(Integer.class);
				});

				assertSame(original, thrown.getCause());
				assertEquals(1, reports.get());
				assertSame(original, observed.get().getCause());
				assertEquals(0, fixture.connections.get());
				assertEquals(0, fixture.closes.get());
				// Statement entry points retain their existing publication boundary; normalization itself sets no flag.
				assertFalse(Thread.currentThread().isInterrupted());
			}
		}
	}

	@Test
	void freshMetadataCheckoutCancellationExitingATransactionCannotAdmitAnotherAttempt() {
		Fixture fixture = new Fixture(true, null);
		Database inspection = Database.withDataSource(fixture.nativeSource).databaseType(DatabaseType.GENERIC).build();
		inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
		SQLException original = cancellation("sqlState");
		AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			conditions.incrementAndGet();
			return true;
		});

		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(policy, () -> {
			attempts.incrementAndGet();
			fixture.database.query("INSERT INTO work VALUES (1)").execute();
			fixture.checkoutFailure = original;
			fixture.database.readDatabaseMetaData(metadata -> fail("Fresh checkout failed"));
		}));

		boolean interruptedAtExit = Thread.interrupted();
		assertSame(original, thrown.getCause());
		assertTrue(interruptedAtExit);
		assertEquals(1, attempts.get());
		assertEquals(0, conditions.get());
		assertEquals(List.of(), inspection.query("SELECT id FROM work").fetchList(Integer.class));
		assertEquals(1, fixture.rollbacks.get());
		assertEquals(0, fixture.commits.get());
		assertEquals(1, fixture.closes.get());
		assertTrue(fixture.database.currentTransaction().isEmpty());
	}

	@Test
	void databaseTypeDetectionRetainsFailureContextAndClosesItsCandidate() throws SQLException {
		for (String accessor : ACCESSORS) {
			for (String entry : List.of("source", "connection", "database")) {
				Fixture fixture = new Fixture(false, null);
				SQLException original = cancellation(accessor);
				fixture.metadataFailure = original;

				DatabaseException thrown = assertThrows(DatabaseException.class, () -> {
					switch (entry) {
						case "source" -> DatabaseType.fromDataSource(fixture.source);
						case "connection" -> {
							try (Connection connection = fixture.source.getConnection()) {
								DatabaseType.fromConnection(connection);
							}
						}
						case "database" -> fixture.database.getDatabaseType();
					}
				});

				assertTrue(contains(thrown, original), entry + " " + accessor);
				assertTrue(thrown.getMessage().contains(entry.equals("database")
						? "Unable to determine database type automatically" : "Unable to inspect database metadata"));
				assertEquals(1, fixture.connections.get());
				assertEquals(1, fixture.closes.get());
				assertFalse(Thread.currentThread().isInterrupted());
			}
		}
	}

	@Test
	void malformedAbortDiagnosticsDoNotReplaceCommitFailureOrEraseCleanupCancellation() {
		Fixture fixture = new Fixture(true, null);
		SQLException commit = new SQLException("commit outcome unknown", "08006");
		SQLException abort = cancellation("sqlState");
		fixture.commitFailure = commit;
		fixture.abortFailure = abort;
		AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
		TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(completion::set).build();

		DatabaseException thrown = assertThrows(DatabaseException.class,
				() -> fixture.database.transaction(options, () -> fixture.database.query("VALUES (1)").fetchObject(Integer.class)));

		assertSame(commit, thrown.getCause());
		assertTrue(contains(thrown, abort));
		assertTrue(Thread.currentThread().isInterrupted());
		assertEquals(TransactionResult.IN_DOUBT, completion.get().getResult());
		assertEquals(1, fixture.aborts.get());
		assertEquals(1, fixture.closes.get());
	}

	@Test
	void successfulRawMetadataAndHealthCallsDoNotFabricateInterruption() {
		Fixture fixture = new Fixture(true, null);
		assertEquals(Optional.of(7), fixture.database.useRawConnection(connection -> Optional.of(7)));
		fixture.database.readDatabaseMetaData(metadata -> assertNotNull(metadata.getDatabaseProductName()));
		fixture.database.performHealthCheck(Duration.ofSeconds(1));

		assertFalse(Thread.currentThread().isInterrupted());
		assertEquals(3, fixture.connections.get());
		assertEquals(3, fixture.closes.get());
	}

	private static boolean contains(Throwable failure, Throwable original) {
		return ThrowableGraph.inspect(failure, current -> current == original ? 1 : 0, 0) != 0;
	}

	private static SQLException cancellation(String broken) {
		InterruptedException interruption = new InterruptedException("cancelled");
		SQLException original = new SQLException("driver cancellation", "57014", interruption) {
			@Override public String getSQLState() {
				if (broken.equals("sqlState")) throw new IllegalStateException("unreadable SQLState");
				return super.getSQLState();
			}
			@Override public int getErrorCode() {
				if (broken.equals("errorCode")) throw new IllegalStateException("unreadable error code");
				return super.getErrorCode();
			}
			@Override public String getMessage() {
				if (broken.equals("message")) throw new IllegalStateException("unreadable message");
				return super.getMessage();
			}
			@Override public Throwable getCause() {
				if (broken.equals("cause")) throw new IllegalStateException("unreadable cause");
				return super.getCause();
			}
		};
		if (broken.equals("cause")) original.addSuppressed(interruption);
		return original;
	}

	private static final class Fixture {
		private final JDBCDataSource nativeSource = new JDBCDataSource();
		private final DataSource source;
		private final Database database;
		private final AtomicInteger connections = new AtomicInteger(), closes = new AtomicInteger();
		private final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger(), aborts = new AtomicInteger();
		private SQLException checkoutFailure, healthFailure, metadataFailure, commitFailure, abortFailure;

		private Fixture(boolean explicitType, MetricsCollector collector) {
			this.nativeSource.setUrl("jdbc:hsqldb:mem:acquisition_diagnostics_" + UUID.randomUUID());
			this.nativeSource.setUser("sa");
			this.nativeSource.setPassword("");
			this.source = proxy(DataSource.class, (method, args) -> {
				if (method.getName().equals("getConnection") && this.checkoutFailure != null) {
					SQLException failure = this.checkoutFailure;
					this.checkoutFailure = null;
					throw failure;
				}
				Object result = invoke(this.nativeSource, method, args);
				if (!(result instanceof Connection connection)) return result;
				this.connections.incrementAndGet();
				return proxy(Connection.class, (operation, parameters) -> {
					String name = operation.getName();
					if (name.equals("isValid") && this.healthFailure != null) throw this.healthFailure;
					if (name.equals("getMetaData") && this.metadataFailure != null) throw this.metadataFailure;
					if (name.equals("commit")) {
						this.commits.incrementAndGet();
						if (this.commitFailure != null) throw this.commitFailure;
					}
					if (name.equals("rollback")) this.rollbacks.incrementAndGet();
					if (name.equals("abort")) {
						this.aborts.incrementAndGet();
						if (this.abortFailure != null) throw this.abortFailure;
						return null;
					}
					Object value = invoke(connection, operation, parameters);
					if (name.equals("close")) this.closes.incrementAndGet();
					return value;
				});
			});
			Database.Builder builder = Database.withDataSource(this.source);
			if (explicitType) builder.databaseType(DatabaseType.GENERIC);
			if (collector != null) builder.metricsCollector(collector);
			this.database = builder.build();
		}
	}

	@FunctionalInterface
	private interface Invocation { Object invoke(Method method, Object[] args) throws Throwable; }

	private static <T> T proxy(Class<T> type, Invocation invocation) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> invocation.invoke(method, args)));
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
