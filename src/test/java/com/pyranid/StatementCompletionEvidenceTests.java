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
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class StatementCompletionEvidenceTests {
	@Test
	void internalSettingsAndUnusedCallbacksDoNotRequireValidation() {
		Fixture fixture = new Fixture(builder -> builder.preparedStatementBinder(customBinder()));
		fixture.database.transaction(() -> {
			fixture.database.query("INSERT INTO work VALUES (2)").queryTimeout(Duration.ofSeconds(1)).execute();
			fixture.database.query("SELECT id FROM work WHERE id = -1").resultSetMapper(integerMapper(false))
					.fetchList(Integer.class);
		});
		assertEquals(0, fixture.validations.get());
	}

	@Test
	void topLevelAndDefaultNestedCallbacksRequireOnlyOneValidationPerBoundary() {
		for (String callback : List.of("binder", "mapper", "customizer")) {
			for (boolean databaseWide : new boolean[]{false, true}) {
				Fixture fixture = new Fixture(builder -> {
					if (databaseWide && callback.equals("binder")) builder.preparedStatementBinder(customBinder());
					if (databaseWide && callback.equals("mapper")) builder.resultSetMapper(integerMapper(false));
				});
				fixture.database.transaction(() -> {
					if (callback.equals("mapper")) {
						Query select = fixture.database.query("SELECT id FROM work");
						if (!databaseWide) select.resultSetMapper(integerMapper(false));
						assertEquals(List.of(1), select.fetchList(Integer.class));
					} else {
						Query insert = fixture.database.query("INSERT INTO work VALUES (:id)").bind("id", 2);
						if (callback.equals("customizer")) insert.customize((context, statement) -> {});
						else if (!databaseWide) insert.preparedStatementBinder(customBinder());
						insert.execute();
					}
				});
				assertEquals(1, fixture.validations.get(), callback + " databaseWide=" + databaseWide);
			}
		}

		CustomParameterBinder binder = new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(Integer.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement,
					Integer index, Object value) { return BindingResult.fallback(); }
			@Override public BindingResult bindNull(StatementContext<?> context, PreparedStatement statement,
					Integer index, TargetType type, Integer sqlType) throws SQLException {
				statement.setNull(index, Types.INTEGER);
				return BindingResult.handled();
			}
		};
		CustomColumnMapper mapper = new CustomColumnMapper() {
			@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(Integer.class); }
			@Override public MappingResult map(StatementContext<?> context, ResultSet result, Object value,
					TargetType type, Integer index, String label, InstanceProvider instances) {
				return MappingResult.fallback();
			}
		};
		for (String callback : List.of("bind", "bindNull", "map")) {
			Fixture nested = new Fixture(builder -> {
				if (callback.equals("map"))
					builder.resultSetMapper(ResultSetMapper.withCustomColumnMappers(List.of(mapper)).build());
				else builder.preparedStatementBinder(PreparedStatementBinder.withCustomParameterBinders(List.of(binder)));
			});
			nested.database.transaction(() -> {
				if (callback.equals("bind")) nested.database.query("INSERT INTO work VALUES (:id)").bind("id", 2).execute();
				else if (callback.equals("bindNull")) nested.database.query("INSERT INTO nullable_work VALUES (:id)")
						.bind("id", new TypedParameter() {
							@Override public Type getExplicitType() { return Integer.class; }
							@Override public Optional<Object> getValue() { return Optional.empty(); }
						}).execute();
				else assertEquals(List.of(1), nested.database.query("SELECT id FROM work").fetchList(Integer.class));
			});
			assertEquals(1, nested.validations.get(), callback);
		}
	}

	@Test
	void mapperCanHandleAnAbortWithoutLosingValidationOrInventingACause() {
		Fixture fixture = new Fixture(builder -> {});
		TransactionOutcome<Integer> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.database.query("INSERT INTO work VALUES (2)").execute();
			return fixture.database.query("SELECT id FROM work WHERE id = 1")
					.resultSetMapper(integerMapper(true)).fetchObject(Integer.class);
		});
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		assertEquals(Optional.of(1), outcome.getAttemptedValue());
		assertNull(outcome.getFailure().orElseThrow().getCause());
		assertEquals(1, fixture.validations.get());
		assertEquals(List.of(1), fixture.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class));
	}

	@Test
	void streamMapperUsesItsCapturedOwnerWhileAnIndependentTransactionIsBound() {
		Fixture fixture = new Fixture(builder -> {});
		fixture.database.transaction(() -> {
			fixture.database.query("SELECT id FROM work")
					.resultSetMapper(integerMapper(false)).fetchStream(Integer.class, stream -> {
						fixture.database.transaction(() -> assertEquals(1, stream.iterator().next()));
						return 1;
					});
		});
		assertEquals(1, fixture.validations.get());
	}

	@Test
	void handledStreamFetchFailureIsRecordedBeforeCursorClose() {
		Fixture fixture = new Fixture(builder -> {});
		fixture.failFetch.set(true);
		fixture.database.transaction(() -> {
			fixture.database.query("SELECT id FROM work").fetchStream(Integer.class, stream -> {
				assertThrows(DatabaseException.class, () -> stream.iterator().hasNext());
				return 1;
			});
		});
		assertEquals(1, fixture.validations.get());
	}

	@Test
	void streamFetchFailureRecordsAgainstItsOwnerWhileAnIndependentTransactionIsBound() {
		Fixture fixture = new Fixture(builder -> {});
		fixture.failFetch.set(true);
		fixture.database.transaction(() -> {
			fixture.database.query("SELECT id FROM work").fetchStream(Integer.class, stream -> {
				fixture.database.transaction(() -> {
					assertThrows(DatabaseException.class, () -> stream.iterator().hasNext());
				});
				return 1;
			});
		});
		assertEquals(1, fixture.validations.get());
	}

	@Test
	void participantFailureIsRecordedBeforeUnlockEvenAfterCompletionBegins() throws Exception {
		CountDownLatch completionBegan = new CountDownLatch(1);
		CountDownLatch loggerReached = new CountDownLatch(1);
		CountDownLatch releaseLogger = new CountDownLatch(1);
		AtomicReference<Exception> loggedFailure = new AtomicReference<>();
		Fixture fixture = new Fixture(builder -> builder.statementLogger(log -> {
			StatementLog<?> event = log;
			if (event.getException().isEmpty()) return;
			loggedFailure.set(event.getException().orElseThrow());
			loggerReached.countDown();
			await(releaseLogger);
		}));
		ExecutorService workers = Executors.newFixedThreadPool(2);
		AtomicReference<Future<?>> participant = new AtomicReference<>();
		try {
			Future<TransactionOutcome<Void>> owner = workers.submit(() -> fixture.database.transactionWithOutcome(() -> {
				fixture.database.query("INSERT INTO work VALUES (2)").execute();
				Transaction transaction = fixture.database.currentTransaction().orElseThrow();
				fixture.failWorker.set(true);
				participant.set(workers.submit(() -> fixture.database.participate(transaction, () -> {
					// Handle inside participation so rollback-only cannot hide missing failure recording.
					assertThrows(DatabaseException.class,
							() -> fixture.database.query("INSERT INTO work VALUES (3)").execute());
				})));
				await(fixture.workerDriverEntered);
				// Explicitly close admission while the participant owns the JDBC lock. Completion repeats this idempotently.
				transaction.beginCompletion();
				completionBegan.countDown();
			}));
			assertTrue(completionBegan.await(10, TimeUnit.SECONDS));
			fixture.releaseWorkerFailure.countDown();
			assertTrue(loggerReached.await(10, TimeUnit.SECONDS));
			// The worker is suspended after releasing its lease and cannot record a failure on return to application code.
			TransactionOutcome<Void> outcome = owner.get(10, TimeUnit.SECONDS);
			assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
			assertSame(loggedFailure.get(), outcome.getFailure().orElseThrow().getCause());
			assertEquals(1, fixture.validations.get());
			assertEquals(List.of(1), fixture.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class));
			releaseLogger.countDown();
			participant.get().get(10, TimeUnit.SECONDS);
		} finally {
			fixture.releaseWorkerFailure.countDown();
			releaseLogger.countDown();
			workers.shutdownNow();
			assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
		}
	}

	@Test
	void handledCleanupFailureIsRetainedBeforeUnlockWithItsEscapingIdentity() {
		for (boolean stream : new boolean[]{false, true}) {
			Fixture fixture = new Fixture(builder -> {});
			fixture.failClose.set(true);
			RuntimeException[] handled = new RuntimeException[1];
			TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
				if (stream) fixture.database.query("SELECT id FROM work").fetchStream(Integer.class, rows -> {
					handled[0] = assertThrows(RuntimeException.class, rows::close);
					return 1;
				});
				else handled[0] = assertThrows(DatabaseException.class,
						() -> fixture.database.query("INSERT INTO work VALUES (2)").execute());
			});
			assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
			assertSame(handled[0], outcome.getFailure().orElseThrow().getCause());
			assertEquals(1, fixture.validations.get());
		}
	}

	@Test
	void swallowedCapabilitySignalRetainsItsScrubbedStatementCause() {
		Fixture fixture = new Fixture(builder -> {});
		fixture.failLarge.set(true);
		TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() ->
				fixture.database.query("INSERT INTO work VALUES (:id)").bind("id", Parameters.secure(2)).execute());
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		DatabaseException cause = assertInstanceOf(DatabaseException.class, outcome.getFailure().orElseThrow().getCause());
		assertSame(fixture.capabilityFailure, cause.getCause());
		assertTrue(cause.getMessage().contains("INSERT INTO work"));
		assertFalse(cause.getMessage().contains("parameters=[2]"));
		assertEquals(1, fixture.validations.get());
	}

	@Test
	void postgresFallbackRecordsCapabilityWithoutReadingItsMessage() {
		Fixture fixture = new Fixture(DatabaseType.POSTGRESQL, builder -> {});
		AtomicInteger messageReads = new AtomicInteger();
		fixture.capabilityFailure = new SQLFeatureNotSupportedException("optional method unavailable", "0A000") {
			@Override public String getMessage() {
				messageReads.incrementAndGet();
				throw new AssertionError("Capability diagnostics must not read the driver's message");
			}
			@Override public String toString() { return "SQLFeatureNotSupportedException[unreadable message]"; }
		};
		fixture.invalidateOnCapabilityFailure = false;
		fixture.failLarge.set(true);
		fixture.database.transaction(() -> {
			fixture.database.query("INSERT INTO work VALUES (2)").execute();
			fixture.database.query("INSERT INTO work VALUES (3)").execute();
		});
		assertEquals(0, messageReads.get());
		assertEquals(1, fixture.validations.get());
		assertEquals(List.of(1, 2, 3), fixture.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class));
	}

	@Test
	void duckDbDiagnosticMessageFailureConservativelyRefusesCapabilityFallback() {
		Fixture fixture = new Fixture(builder -> {});
		AtomicInteger messageReads = new AtomicInteger();
		AssertionError diagnostic = new AssertionError("driver message unavailable");
		fixture.capabilityFailure = new SQLFeatureNotSupportedException("optional method unavailable", "0A000") {
			@Override public String getMessage() {
				messageReads.incrementAndGet();
				throw diagnostic;
			}
			@Override public String toString() { return "SQLFeatureNotSupportedException[unreadable message]"; }
		};
		fixture.invalidateOnCapabilityFailure = false;
		fixture.failLarge.set(true);
		TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.database.query("INSERT INTO work VALUES (2)").execute();
		});
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		assertSame(fixture.capabilityFailure, outcome.getFailure().orElseThrow().getCause());
		assertTrue(List.of(fixture.capabilityFailure.getSuppressed()).contains(diagnostic));
		assertTrue(messageReads.get() > 0);
		assertEquals(0, fixture.standardExecutions.get());
		assertEquals(0, fixture.validations.get());
		assertEquals(List.of(1), fixture.inspection.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class));
	}

	@Test
	void checkedCleanupWrappingDoesNotInvokeForeignToStringOrRetainTheLock() {
		Fixture fixture = new Fixture(builder -> {});
		AtomicInteger descriptions = new AtomicInteger();
		AtomicBoolean armed = new AtomicBoolean(true);
		fixture.cleanupException = new SQLException("checked cleanup failure", "HY000") {
			@Override public String toString() {
				if (armed.get()) {
					descriptions.incrementAndGet();
					throw new AssertionError("Cleanup wrapping must not invoke foreign toString");
				}
				return "SQLException[checked cleanup failure]";
			}
		};
		fixture.invalidateOnCloseFailure = false;
		fixture.failClose.set(true);
		Transaction[] owner = new Transaction[1];
		TransactionOutcome<Void> outcome;
		try {
			outcome = fixture.database.transactionWithOutcome(() -> {
				owner[0] = fixture.database.currentTransaction().orElseThrow();
				fixture.database.query("SELECT id FROM work").fetchStream(Integer.class, rows -> {
					RuntimeException handled = assertThrows(RuntimeException.class, rows::close);
					assertSame(fixture.cleanupException, handled.getCause());
					return 1;
				});
			});
		} finally {
			// Reporters must be able to render a future regression after the operation has escaped.
			armed.set(false);
		}
		assertEquals(TransactionResult.COMMITTED, outcome.getResult());
		assertTrue(outcome.getFailure().isEmpty());
		assertEquals(0, descriptions.get());
		assertEquals(1, fixture.validations.get());
		assertFalse(owner[0].getConnectionLock().isLocked());
	}

	@Test
	void capabilityDiagnosticsThatVetoFallbackPreserveTheHandledFailureIdentity() {
		Fixture fixture = new Fixture(builder -> {});
		AtomicInteger stateReads = new AtomicInteger();
		fixture.capabilityFailure = new SQLFeatureNotSupportedException("optional method unavailable", "0A000") {
			@Override public String getSQLState() {
				return stateReads.getAndIncrement() == 0 ? "0A000" : "23505";
			}
		};
		fixture.failLarge.set(true);
		DatabaseException[] handled = new DatabaseException[1];
		TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
			handled[0] = assertThrows(DatabaseException.class,
					() -> fixture.database.query("INSERT INTO work VALUES (2)").execute());
		});
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		assertSame(fixture.capabilityFailure, handled[0].getCause());
		assertSame(handled[0], outcome.getFailure().orElseThrow().getCause());
		assertEquals(0, fixture.standardExecutions.get());
		assertEquals(1, fixture.validations.get());
	}

	@Test
	void capabilityDiagnosticsThatVetoLegacyFallbackPreserveTheHandledErrorIdentity() {
		Fixture fixture = new Fixture(builder -> {});
		AtomicInteger causeReads = new AtomicInteger();
		fixture.capabilityFailure = new AbstractMethodError("optional method unavailable") {
			@Override public Throwable getCause() {
				return causeReads.getAndIncrement() == 0 ? null : new SQLException("execution failed", "23505");
			}
		};
		fixture.failLarge.set(true);
		Error[] handled = new Error[1];
		TransactionOutcome<Void> outcome = fixture.database.transactionWithOutcome(() -> {
			handled[0] = assertThrows(AbstractMethodError.class,
					() -> fixture.database.query("INSERT INTO work VALUES (2)").execute());
		});
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		assertSame(fixture.capabilityFailure, handled[0]);
		assertSame(handled[0], outcome.getFailure().orElseThrow().getCause());
		assertEquals(0, fixture.standardExecutions.get());
		assertEquals(1, fixture.validations.get());
	}

	private static PreparedStatementBinder customBinder() {
		return new PreparedStatementBinder() {
			@Override public <T> void bindParameter(StatementContext<T> context, PreparedStatement statement,
					Integer index, Object value) throws SQLException { statement.setObject(index, value); }
		};
	}

	private static ResultSetMapper integerMapper(boolean abort) {
		return new ResultSetMapper() {
			@Override public <T> Optional<T> map(StatementContext<T> context, ResultSet result, Class<T> type,
					InstanceProvider instances) throws SQLException {
				T value = type.cast(result.getInt(1));
				if (abort) {
					try (java.sql.Statement statement = result.getStatement().getConnection().createStatement()) {
						try { statement.execute("SIMULATE_ABORT"); }
						catch (SQLException ignored) {}
					}
				}
				return Optional.of(value);
			}
		};
	}

	private static final class Fixture {
		final Database database;
		final Database inspection;
		final AtomicInteger validations = new AtomicInteger();
		final AtomicInteger standardExecutions = new AtomicInteger();
		final AtomicBoolean failFetch = new AtomicBoolean();
		final AtomicBoolean failClose = new AtomicBoolean();
		final AtomicBoolean failLarge = new AtomicBoolean();
		final AtomicBoolean failWorker = new AtomicBoolean();
		final CountDownLatch workerDriverEntered = new CountDownLatch(1);
		final CountDownLatch releaseWorkerFailure = new CountDownLatch(1);
		Throwable capabilityFailure = new SQLFeatureNotSupportedException("large method unavailable", "0A000");
		boolean invalidateOnCapabilityFailure = true;
		SQLException cleanupException = new SQLException("statement cleanup failed", "HY000");
		boolean invalidateOnCloseFailure = true;

		Fixture(Consumer<Database.Builder> configure) {
			this(DatabaseType.DUCK_DB, configure);
		}

		Fixture(DatabaseType databaseType, Consumer<Database.Builder> configure) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:statement_completion_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			this.inspection = Database.with(source, DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work(id INT PRIMARY KEY)").execute();
			this.inspection.query("CREATE TABLE nullable_work(id INT)").execute();
			this.inspection.query("INSERT INTO work VALUES (1)").execute();
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object value = invoke(source, method, args);
				if (!(value instanceof Connection connection)) return value;
				AtomicBoolean invalid = new AtomicBoolean();
				Connection[] handle = new Connection[1];
				handle[0] = proxy(Connection.class, (call, parameters) -> {
					if (call.getName().equals("rollback")) invalid.set(false);
					Object result = invoke(connection, call, parameters);
					if (!(result instanceof java.sql.Statement statement)) return result;
					boolean prepared = statement instanceof PreparedStatement;
					java.sql.Statement[] statementHandle = new java.sql.Statement[1];
					Call dispatch = (jdbc, values) -> {
						String name = jdbc.getName();
						if (name.equals("executeUpdate") || name.equals("executeBatch")) standardExecutions.incrementAndGet();
						if (name.equals("getConnection")) return handle[0];
						if (values != null && values.length > 0 && "SIMULATE_ABORT".equals(values[0])) {
							invalid.set(true);
							throw new SQLException("simulated database abort", "23505");
						}
						boolean validation = name.equals("executeQuery") && values != null && values.length > 0
								&& "SELECT 1".equals(values[0]);
						if (validation) {
							validations.incrementAndGet();
							if (invalid.get()) throw new SQLException("transaction is aborted", "25P02");
						}
						if (name.equals("executeLargeUpdate") && failLarge.compareAndSet(true, false)) {
							if (invalidateOnCapabilityFailure) invalid.set(true);
							throw capabilityFailure;
						}
						if (name.equals("executeLargeUpdate") && failWorker.compareAndSet(true, false)) {
							workerDriverEntered.countDown();
							await(releaseWorkerFailure);
							invalid.set(true);
							throw new SQLException("participant execution aborted the transaction", "40001");
						}
						Object[] driverArguments = values;
						if (validation) {
							driverArguments = values.clone();
							driverArguments[0] = "VALUES (1)";
						}
						Object answer = invoke(statement, jdbc, driverArguments);
						if (prepared && name.equals("close") && failClose.compareAndSet(true, false)) {
							if (invalidateOnCloseFailure) invalid.set(true);
							throw cleanupException;
						}
						if (!(answer instanceof ResultSet resultSet)) return answer;
						return proxy(ResultSet.class, (read, arguments) -> {
							if (read.getName().equals("getStatement")) return statementHandle[0];
							if (prepared && read.getName().equals("next") && failFetch.compareAndSet(true, false))
								throw new SQLException("fetch failed", "HY000");
							return invoke(resultSet, read, arguments);
						});
					};
					statementHandle[0] = prepared ? proxy(PreparedStatement.class, dispatch) : proxy(java.sql.Statement.class, dispatch);
					return statementHandle[0];
				});
				return handle[0];
			});
			Database.Builder builder = Database.with(wrapped, databaseType);
			configure.accept(builder);
			this.database = builder.build();
		}
	}

	@FunctionalInterface private interface Call { Object run(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(handle, method, args) -> call.run(method, args)));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
	private static void await(CountDownLatch latch) {
		try {
			if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("Timed out coordinating transaction completion");
		} catch (InterruptedException failure) {
			Thread.currentThread().interrupt();
			throw new AssertionError(failure);
		}
	}
}
