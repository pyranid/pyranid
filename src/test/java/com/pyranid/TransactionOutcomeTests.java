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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class TransactionOutcomeTests {
	@AfterEach
	void clearInterruption() { Thread.interrupted(); }

	@Test
	void allOverloadsRunOnceAndLogicalCompletionRemainsLazy() {
		Fixture fixture = new Fixture();
		AtomicInteger calls = new AtomicInteger();
		AtomicInteger observations = new AtomicInteger();
		TransactionOptions options = TransactionOptions.withIsolation(TransactionIsolation.SERIALIZABLE)
				.readOnly(true).completionObserver(ignored -> observations.incrementAndGet()).build();

		TransactionOutcome<Void> voidDefault = fixture.database.transactionWithOutcome(() -> { calls.incrementAndGet(); });
		TransactionOutcome<Void> voidOptions = fixture.database.transactionWithOutcome(options, () -> {
			calls.incrementAndGet();
			assertEquals(TransactionIsolation.SERIALIZABLE, fixture.database.currentTransaction().orElseThrow().getTransactionIsolation());
		});
		TransactionOutcome<String> returningDefault = fixture.database.transactionWithOutcome(() -> {
			calls.incrementAndGet(); return Optional.of("value");
		});
		TransactionOutcome<String> returningOptions = fixture.database.transactionWithOutcome(options, () -> {
			calls.incrementAndGet(); return Optional.of("supplied");
		});
		TransactionOutcome<String> empty = fixture.database.transactionWithOutcome(() -> Optional.<String>empty());
		TransactionOutcome<String> nullOptional = fixture.database.transactionWithOutcome(() -> (Optional<String>) null);
		for (TransactionOutcome<?> outcome : List.of(voidDefault, voidOptions, returningDefault, returningOptions, empty, nullOptional)) {
			assertEquals(TransactionResult.COMMITTED, outcome.getResult());
			assertTrue(outcome.getFailure().isEmpty());
			assertTrue(outcome.getCompletion().getEnclosingTransactionId().isEmpty());
		}
		assertTrue(voidDefault.getAttemptedValue().isEmpty());
		assertTrue(voidOptions.getAttemptedValue().isEmpty());
		assertEquals(Optional.of("value"), returningDefault.getAttemptedValue());
		assertEquals(Optional.of("supplied"), returningOptions.getAttemptedValue());
		assertTrue(empty.getAttemptedValue().isEmpty());
		assertTrue(nullOptional.getAttemptedValue().isEmpty());
		assertEquals(4, calls.get());
		assertEquals(2, observations.get());
		assertEquals(0, fixture.acquisitions.get());
	}

	@Test
	void commitAndRollbackOnlyRetainCallbackValuesIndependentlyOfFailures() {
		Fixture fixture = new Fixture();
		TransactionOutcome<String> committed = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(1); return Optional.of("committed");
		});
		assertOutcome(committed, TransactionResult.COMMITTED, "committed", null);
		assertEquals(List.of(1), fixture.ids());

		TransactionOutcome<String> rolledBack = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(2);
			fixture.database.currentTransaction().orElseThrow().setRollbackOnly(true);
			return Optional.of("attempted");
		});
		assertOutcome(rolledBack, TransactionResult.ROLLED_BACK, "attempted", null);
		assertEquals(List.of(1), fixture.ids());
		assertEquals(1, fixture.commits.get());
		assertEquals(1, fixture.rollbacks.get());
		assertEquals(Optional.of("traditional"), fixture.database.transaction(() -> {
			fixture.database.currentTransaction().orElseThrow().setRollbackOnly(true);
			return Optional.of("traditional");
		}));
	}

	@Test
	void callbackFailuresReturnTheirExistingIdentityAndCheckedWrappingWithoutInventingValues() {
		for (boolean checked : new boolean[]{false, true}) {
			Fixture fixture = new Fixture();
			Exception original = checked ? new Exception("checked callback") : new NullPointerException("programming error");
			TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(() -> {
				fixture.insert(1); throw original;
			});
			assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
			assertTrue(outcome.getAttemptedValue().isEmpty());
			RuntimeException failure = outcome.getFailure().orElseThrow();
			assertSame(checked ? original : failure, checked ? failure.getCause() : original);
			assertSame(failure, outcome.getCompletion().getFailure().orElseThrow());
			assertSame(failure, assertThrows(RuntimeException.class,
					() -> outcome.getFailure().ifPresent(retained -> { throw retained; })));
			assertEquals(List.of(), fixture.ids());
			assertEquals(0, fixture.commits.get());
			assertEquals(1, fixture.rollbacks.get());
		}
	}

	@Test
	void failedCommitRetainsValueAndUsesAffirmativeRollbackEvidenceOnly() {
		for (boolean rollbackFails : new boolean[]{false, true}) {
			Fixture fixture = new Fixture();
			fixture.commitFailure = new SQLException("serialization abort", "40001");
			if (rollbackFails) fixture.rollbackFailure = new SQLException("rollback failed", "08006");
			AtomicInteger calls = new AtomicInteger();
			TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(() -> {
				calls.incrementAndGet(); fixture.insert(1); return Optional.of("attempted");
			});
			assertEquals(rollbackFails ? TransactionResult.IN_DOUBT : TransactionResult.ROLLED_BACK, outcome.getResult());
			assertEquals(Optional.of("attempted"), outcome.getAttemptedValue());
			assertSame(fixture.commitFailure, outcome.getFailure().orElseThrow().getCause());
			if (rollbackFails)
				assertTrue(ThrowableGraph.inspect(outcome.getFailure().orElseThrow(), current -> current == fixture.rollbackFailure ? 1 : 0, 0) != 0);
			assertEquals(1, calls.get(), "The outcome API must never retry");
			assertEquals(1, fixture.commits.get());
			assertEquals(1, fixture.rollbacks.get());
			assertEquals(List.of(), fixture.ids());
		}
	}

	@Test
	void acknowledgedUncertaintyDoesNotErasePersistedWorkOrTheAttemptedValue() {
		Fixture fixture = new Fixture();
		fixture.commitBeforeFailure = true;
		fixture.commitFailure = new SQLException("acknowledgment lost", "08006");
		fixture.commitFailure.setNextException(new SQLException("secondary serialization detail", "40001"));
		TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(1); return Optional.of("receipt");
		});
		assertEquals(TransactionResult.IN_DOUBT, outcome.getResult());
		assertEquals(Optional.of("receipt"), outcome.getAttemptedValue());
		assertSame(fixture.commitFailure, outcome.getFailure().orElseThrow().getCause());
		assertTrue(((DatabaseException) outcome.getFailure().orElseThrow()).isSerializationFailure());
		assertEquals(List.of(1), fixture.ids());
		assertEquals(1, fixture.commits.get());
	}

	@Test
	void aBodyFailureAndFailedRollbackRemainInDoubtWithoutAnAttemptedValue() {
		Fixture fixture = new Fixture();
		fixture.rollbackFailure = new SQLException("cannot roll back", "08006");
		IllegalStateException original = new IllegalStateException("callback failed");
		TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(1); throw original;
		});
		assertOutcome(outcome, TransactionResult.IN_DOUBT, null, original);
		assertEquals(1, original.getSuppressed().length);
		assertSame(fixture.rollbackFailure, original.getSuppressed()[0].getCause());
	}

	@Test
	void postCommitCleanupAndHookFailuresKeepCommittedValueAndFinalFailureAssembly() {
		for (boolean closeFails : new boolean[]{false, true}) {
			Fixture fixture = new Fixture();
			IllegalStateException hookFailure = new IllegalStateException("hook failed");
			if (closeFails) fixture.closeFailure = new SQLException("close failed", "08006");
			AtomicReference<TransactionCompletion> observed = new AtomicReference<>();
			TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(options(observed), () -> {
				fixture.insert(1);
				fixture.database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> { throw hookFailure; });
				return Optional.of("receipt");
			});
			assertEquals(TransactionResult.COMMITTED, outcome.getResult());
			assertEquals(Optional.of("receipt"), outcome.getAttemptedValue());
			RuntimeException failure = outcome.getFailure().orElseThrow();
			assertSame(observed.get(), outcome.getCompletion());
			assertSame(failure, observed.get().getFailure().orElseThrow());
			assertSame(closeFails ? fixture.closeFailure : hookFailure, failure.getCause());
			if (closeFails) assertSame(hookFailure, failure.getSuppressed()[0].getCause());
			assertEquals(List.of(1), fixture.ids());
		}
	}

	@Test
	void rollbackOnlyCompletionCanRetainAValueAndAHookFailure() {
		Fixture fixture = new Fixture();
		IllegalStateException hookFailure = new IllegalStateException("rollback hook failed");
		TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(1);
			Transaction transaction = fixture.database.currentTransaction().orElseThrow();
			transaction.setRollbackOnly(true);
			transaction.addPostTransactionOperation(result -> { throw hookFailure; });
			return Optional.of("attempted");
		});
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		assertEquals(Optional.of("attempted"), outcome.getAttemptedValue());
		assertSame(hookFailure, outcome.getFailure().orElseThrow().getCause());
		assertEquals(List.of(), fixture.ids());
	}

	@Test
	void finalErrorsEscapeUnchangedAfterRollbackAndObservationWhileHookErrorsKeepTheirWrapper() {
		Fixture fixture = new Fixture();
		AssertionError error = new AssertionError("fatal callback");
		AtomicReference<TransactionCompletion> observed = new AtomicReference<>();
		assertSame(error, assertThrows(AssertionError.class, () -> fixture.database.transactionWithOutcome(options(observed), () -> {
			fixture.insert(1); throw error;
		})));
		assertEquals(TransactionResult.ROLLED_BACK, observed.get().getResult());
		assertSame(error, observed.get().getFailure().orElseThrow());
		assertEquals(List.of(), fixture.ids());
		TransactionOutcome<Void> hookOutcome = fixture.database.transactionWithOutcome(() -> {
			fixture.database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> { throw error; });
		});
		assertEquals(TransactionResult.COMMITTED, hookOutcome.getResult());
		assertInstanceOf(PostTransactionOperationException.class, hookOutcome.getFailure().orElseThrow());
		assertSame(error, hookOutcome.getFailure().orElseThrow().getCause());
	}

	@Test
	void argumentValidationThrowsButOperationalAcquisitionAndBeginFailuresProduceOutcomes() {
		Fixture fixture = new Fixture();
		AtomicReference<TransactionCompletion> observed = new AtomicReference<>();
		assertThrows(NullPointerException.class, () -> fixture.database.transactionWithOutcome((TransactionalOperation) null));
		assertThrows(NullPointerException.class, () -> fixture.database.transactionWithOutcome((ReturningTransactionalOperation<String>) null));
		assertThrows(NullPointerException.class, () -> fixture.database.transactionWithOutcome(null, (TransactionalOperation) () -> {}));
		assertThrows(NullPointerException.class, () -> fixture.database.transactionWithOutcome(options(observed), (ReturningTransactionalOperation<String>) null));
		assertNull(observed.get());
		assertEquals(0, fixture.acquisitions.get());

		for (boolean begin : new boolean[]{false, true}) {
			Fixture failing = new Fixture();
			SQLException failure = new SQLException("cannot start transaction", "08006");
			if (begin) failing.beginFailure = failure;
			else failing.acquisitionFailure = failure;
			AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
			TransactionOutcome<String> outcome = failing.database.transactionWithOutcome(options(completion), () -> {
				failing.insert(1); return Optional.of("unreachable");
			});
			assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
			assertTrue(outcome.getAttemptedValue().isEmpty());
			assertSame(failure, outcome.getFailure().orElseThrow().getCause());
			assertSame(completion.get(), outcome.getCompletion());
			assertEquals(0, failing.commits.get());
			assertEquals(List.of(), failing.ids());
		}
	}

	@Test
	void observerRunsOnceAfterCleanupAndHooksAndCannotChangeTheOutcome() {
		Fixture fixture = new Fixture();
		List<String> events = new ArrayList<>();
		AtomicReference<TransactionCompletion> observed = new AtomicReference<>();
		IllegalStateException failure = new IllegalStateException("callback failed");
		TransactionOptions options = TransactionOptions.withIsolation(TransactionIsolation.DEFAULT).completionObserver(completion -> {
			assertEquals(1, fixture.closes.get());
			assertEquals(List.of("hook"), events);
			events.add("observer"); observed.set(completion);
			throw new AssertionError("ignored observer failure");
		}).build();
		TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(options, () -> {
			fixture.insert(1);
			fixture.database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> events.add("hook"));
			throw failure;
		});
		assertOutcome(outcome, TransactionResult.ROLLED_BACK, null, failure);
		assertSame(observed.get(), outcome.getCompletion());
		assertEquals(List.of("hook", "observer"), events);
		assertEquals(0, failure.getSuppressed().length);
	}

	@Test
	void returnedInterruptionRestoresOnlyAtAnUnenclosedBoundaryIncludingCleanupAndHooks() {
		for (String phase : List.of("callback", "close", "hook")) {
			for (boolean enclosed : new boolean[]{false, true}) {
				Fixture outer = new Fixture();
				Fixture inner = new Fixture();
				InterruptedException interruption = new InterruptedException("interrupted " + phase);
				if (phase.equals("close")) inner.closeFailure = new SQLException("interrupted close", interruption);
				Runnable operation = () -> {
					TransactionOutcome<String> outcome = inner.database.transactionWithOutcome(() -> {
						inner.insert(1);
						if (phase.equals("callback")) throw interruption;
						if (phase.equals("hook")) inner.database.currentTransaction().orElseThrow()
								.addPostTransactionOperation(result -> { throw new IllegalStateException(interruption); });
						return Optional.of("receipt");
					});
					assertTrue(outcome.getFailure().isPresent());
					assertEquals(!enclosed, Thread.currentThread().isInterrupted());
				};
				if (enclosed) {
					outer.database.transaction(() -> {
						outer.database.currentTransaction().orElseThrow().withSavepoint(operation::run);
						outer.insert(2);
					});
					assertEquals(List.of(2), outer.ids());
				} else operation.run();
				assertEquals(!enclosed, Thread.interrupted());
			}
		}
	}

	@Test
	void anAlreadyPendingInterruptSurvivesAHandledInnerOutcome() {
		Fixture fixture = new Fixture();
		fixture.database.transaction(() -> {
			Thread.currentThread().interrupt();
			TransactionOutcome<Void> inner = fixture.database.transactionWithOutcome(() -> { throw new IllegalStateException("handled"); });
			assertTrue(inner.getFailure().isPresent());
			assertTrue(Thread.interrupted());
			fixture.insert(1);
		});
		assertEquals(List.of(1), fixture.ids());
	}

	@Test
	void independentBoundariesAndSameDataSourceParticipationKeepTheirIdentities() {
		Fixture fixture = new Fixture();
		Database alias = Database.with(fixture.source, DatabaseType.GENERIC).build();
		AtomicReference<TransactionOutcome<String>> inner = new AtomicReference<>();
		TransactionOutcome<Void> outer = fixture.database.transactionWithOutcome(() -> {
			Transaction transaction = fixture.database.currentTransaction().orElseThrow();
			alias.participate(transaction, () -> {
				assertSame(transaction, alias.currentTransaction().orElseThrow());
			});
			inner.set(fixture.database.transactionWithOutcome(() -> Optional.of("inner")));
			assertSame(transaction, fixture.database.currentTransaction().orElseThrow());
		});
		assertNotEquals(outer.getCompletion().getTransactionId(), inner.get().getCompletion().getTransactionId());
		assertEquals(Optional.of(outer.getCompletion().getTransactionId()), inner.get().getCompletion().getEnclosingTransactionId());
		assertTrue(outer.getCompletion().getEnclosingTransactionId().isEmpty());
		assertTrue(fixture.database.currentTransaction().isEmpty());
	}

	@Test
	void handledUnsafeInnerOutcomesFenceALaterOuterSerializationRetry() {
		for (boolean uncertain : new boolean[]{false, true}) {
			Fixture fixture = new Fixture();
			if (uncertain) {
				fixture.commitBeforeFailure = true;
				fixture.commitFailure = new SQLException("unknown acknowledgment", "08006");
			}
			AtomicInteger attempts = new AtomicInteger(), policyCalls = new AtomicInteger();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				policyCalls.incrementAndGet(); return true;
			});
			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(policy, () -> {
				attempts.incrementAndGet();
				TransactionOutcome<String> inner = fixture.database.transactionWithOutcome(() -> {
					fixture.insert(1);
					if (!uncertain) fixture.database.currentTransaction().orElseThrow()
							.addPostTransactionOperation(result -> { throw new IllegalStateException("committed hook failure"); });
					return Optional.of("receipt");
				});
				assertEquals(uncertain ? TransactionResult.IN_DOUBT : TransactionResult.COMMITTED, inner.getResult());
				assertTrue(inner.getFailure().isPresent());
				throw new DatabaseException("outer serialization", new SQLException("outer abort", "40001"));
			}));
			assertTrue(thrown.isSerializationFailure());
			assertEquals(1, attempts.get());
			assertEquals(0, policyCalls.get());
			assertEquals(List.of(1), fixture.ids());
		}
	}

	@Test
	void constructorRejectsFailuresThatCannotBeReturnedByTheOutcomeApi() {
		assertThrows(IllegalArgumentException.class, () -> new TransactionOutcome<>(
				new TransactionCompletion(1L, null, TransactionResult.ROLLED_BACK, new Exception("checked")), Optional.empty()));
		assertThrows(IllegalArgumentException.class, () -> new TransactionOutcome<>(
				new TransactionCompletion(1L, null, TransactionResult.ROLLED_BACK, new AssertionError("fatal")), Optional.empty()));
	}

	private static TransactionOptions options(AtomicReference<TransactionCompletion> observed) {
		return TransactionOptions.withIsolation(TransactionIsolation.DEFAULT).completionObserver(observed::set).build();
	}

	private static void assertOutcome(TransactionOutcome<?> outcome, TransactionResult result, Object value, RuntimeException failure) {
		assertEquals(result, outcome.getResult());
		assertEquals(Optional.ofNullable(value), outcome.getAttemptedValue());
		assertEquals(Optional.ofNullable(failure), outcome.getFailure());
		assertEquals(outcome.getFailure(), outcome.getCompletion().getFailure());
	}

	private static final class Fixture {
		private final DataSource source;
		private final Database database;
		private final Database inspection;
		private final AtomicInteger acquisitions = new AtomicInteger(), commits = new AtomicInteger();
		private final AtomicInteger rollbacks = new AtomicInteger(), closes = new AtomicInteger();
		private SQLException acquisitionFailure, beginFailure, commitFailure, rollbackFailure, closeFailure;
		private boolean commitBeforeFailure;

		private Fixture() {
			JDBCDataSource delegate = new JDBCDataSource();
			delegate.setUrl("jdbc:hsqldb:mem:transaction_outcome_" + UUID.randomUUID());
			delegate.setUser("sa"); delegate.setPassword("");
			this.inspection = Database.with(delegate, DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE events (id INT PRIMARY KEY)").execute();
			this.source = proxy(DataSource.class, (method, args) -> {
				if (method.getName().equals("getConnection")) {
					this.acquisitions.incrementAndGet();
					if (this.acquisitionFailure != null) throw this.acquisitionFailure;
				}
				Object value = invoke(delegate, method, args);
				if (!(value instanceof Connection connection)) return value;
				return proxy(Connection.class, (jdbc, parameters) -> {
					String name = jdbc.getName();
					if (name.equals("setAutoCommit") && Boolean.FALSE.equals(parameters[0]) && this.beginFailure != null)
						throw this.beginFailure;
					if (name.equals("commit")) {
						this.commits.incrementAndGet();
						if (this.commitFailure != null) {
							if (this.commitBeforeFailure) invoke(connection, jdbc, parameters);
							throw this.commitFailure;
						}
					}
					if (name.equals("rollback")) {
						this.rollbacks.incrementAndGet();
						if (this.rollbackFailure != null) throw this.rollbackFailure;
					}
					if (name.equals("close")) {
						this.closes.incrementAndGet();
						Object result = invoke(connection, jdbc, parameters);
						if (this.closeFailure != null) throw this.closeFailure;
						return result;
					}
					return invoke(connection, jdbc, parameters);
				});
			});
			this.database = Database.with(this.source, DatabaseType.GENERIC).build();
		}

		private void insert(int id) { this.database.query("INSERT INTO events VALUES (:id)").bind("id", id).execute(); }
		private List<Integer> ids() { return this.inspection.query("SELECT id FROM events ORDER BY id").fetchList(Integer.class); }
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
