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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.4.0
 */
public class TransactionRetryTests {
	@Test
	public void testTransactionWithRetryRetriesToSuccessAndReturnsValue() {
		Database database = database("retry_success");
		AtomicInteger attempts = new AtomicInteger();
		List<Long> transactionIds = new ArrayList<>();
		RetryPolicy retryPolicy = retryPolicy(3);

		Optional<String> result = database.transactionWithRetry(retryPolicy, () -> {
			transactionIds.add(database.currentTransaction().orElseThrow().id());

			if (attempts.incrementAndGet() == 1)
				throw serializationFailure("first");

			return Optional.of("success");
		});

		Assertions.assertEquals(Optional.of("success"), result);
		Assertions.assertEquals(2, attempts.get());
		Assertions.assertEquals(2, transactionIds.size());
		Assertions.assertNotEquals(transactionIds.get(0), transactionIds.get(1),
				"Each retry attempt should use a fresh Transaction");
	}

	@Test
	public void testTransactionWithRetryExhaustionThrowsFinalFailureWithPriorSuppressed() {
		Database database = database("retry_exhaustion");
		RetryPolicy retryPolicy = retryPolicy(3);
		DatabaseException first = serializationFailure("first");
		DatabaseException second = serializationFailure("second");
		DatabaseException third = serializationFailure("third");
		List<DatabaseException> failures = List.of(first, second, third);
		AtomicInteger attempts = new AtomicInteger();

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class, () ->
				database.transactionWithRetry(retryPolicy, () -> {
					throw failures.get(attempts.getAndIncrement());
				}));

		Assertions.assertSame(third, thrown);
		Assertions.assertArrayEquals(new Throwable[]{first, second}, thrown.getSuppressed());
	}

	@Test
	public void testTransactionWithRetryDoesNotCallConditionOnFinalFailedAttempt() {
		Database database = database("retry_final_no_condition");
		AtomicInteger conditionCalls = new AtomicInteger();
		RetryPolicy retryPolicy = RetryPolicy.of(1, failure -> {
			conditionCalls.incrementAndGet();
			return true;
		}, RetryPolicy.Backoff.fixed(Duration.ZERO));

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class, () ->
				database.transactionWithRetry(retryPolicy, () -> {
					throw serializationFailure("final");
				}));

		Assertions.assertTrue(thrown.isSerializationFailure());
		Assertions.assertEquals(0, conditionCalls.get());
	}

	@Test
	public void testTransactionWithRetryStopsOnNonRetryableFailureAfterPriorRetry() {
		Database database = database("retry_nonretryable");
		RetryPolicy retryPolicy = retryPolicy(3);
		DatabaseException first = serializationFailure("first");
		DatabaseException second = syntaxFailure("second");
		AtomicInteger attempts = new AtomicInteger();

		DatabaseException thrown = Assertions.assertThrows(DatabaseException.class, () ->
				database.transactionWithRetry(retryPolicy, () -> {
					if (attempts.getAndIncrement() == 0)
						throw first;

					throw second;
				}));

		Assertions.assertSame(second, thrown);
		Assertions.assertArrayEquals(new Throwable[]{first}, thrown.getSuppressed());
		Assertions.assertEquals(2, attempts.get());
	}

	@Test
	public void testTransactionWithRetryFailsInsideExistingTransaction() {
		Database database = database("retry_existing_transaction");
		RetryPolicy retryPolicy = retryPolicy(3);

		Assertions.assertThrows(IllegalStateException.class, () ->
				database.transaction(() ->
						database.transactionWithRetry(retryPolicy, () -> Optional.of("nested"))));
	}

	@Test
	public void testTransactionWithRetryInterruptedBackoffRestoresInterruptFlag() {
		Database database = database("retry_interrupt");
		RetryPolicy retryPolicy = RetryPolicy.of(3, RetryPolicy.Condition.serializationFailure(),
				RetryPolicy.Backoff.fixed(Duration.ofMillis(100)));
		DatabaseException failure = serializationFailure("interrupted");

		try {
			Thread.currentThread().interrupt();

			DatabaseException thrown = Assertions.assertThrows(DatabaseException.class, () ->
					database.transactionWithRetry(retryPolicy, () -> {
						throw failure;
					}));

			Assertions.assertSame(failure, thrown);
			Assertions.assertTrue(Thread.currentThread().isInterrupted());
			Assertions.assertEquals(1, thrown.getSuppressed().length);
			Assertions.assertTrue(thrown.getSuppressed()[0] instanceof InterruptedException);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	public void testTransactionWithRetryConditionFailurePreservesDatabaseFailure() {
		Database database = database("retry_condition_failure");
		RuntimeException conditionFailure = new RuntimeException("condition");
		DatabaseException databaseFailure = serializationFailure("serialization");
		RetryPolicy retryPolicy = RetryPolicy.of(3, failure -> {
			throw conditionFailure;
		}, RetryPolicy.Backoff.fixed(Duration.ZERO));

		RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, () ->
				database.transactionWithRetry(retryPolicy, () -> {
					throw databaseFailure;
				}));

		Assertions.assertSame(conditionFailure, thrown);
		Assertions.assertArrayEquals(new Throwable[]{databaseFailure}, thrown.getSuppressed());
	}

	@Test
	public void testTransactionWithRetryBackoffFailurePreservesDatabaseFailure() {
		Database database = database("retry_backoff_failure");
		RuntimeException backoffFailure = new RuntimeException("backoff");
		DatabaseException databaseFailure = serializationFailure("serialization");
		RetryPolicy retryPolicy = RetryPolicy.of(3, RetryPolicy.Condition.serializationFailure(), (attempt, failure) -> {
			throw backoffFailure;
		});

		RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, () ->
				database.transactionWithRetry(retryPolicy, () -> {
					throw databaseFailure;
				}));

		Assertions.assertSame(backoffFailure, thrown);
		Assertions.assertArrayEquals(new Throwable[]{databaseFailure}, thrown.getSuppressed());
	}

	@Test
	public void testTransactionWithRetryPostTransactionOperationsArePerAttempt() {
		Database database = database("retry_post_transaction");
		RetryPolicy retryPolicy = retryPolicy(3);
		AtomicInteger attempts = new AtomicInteger();
		List<TransactionResult> transactionResults = new ArrayList<>();

		database.transactionWithRetry(retryPolicy, () -> {
			database.currentTransaction().orElseThrow().addPostTransactionOperation(transactionResults::add);

			if (attempts.incrementAndGet() == 1)
				throw serializationFailure("first");
		});

		Assertions.assertEquals(List.of(TransactionResult.ROLLED_BACK, TransactionResult.COMMITTED), transactionResults);
	}

	@Test
	public void testTransactionWithRetryPostTransactionFailureAfterRetryableFailureDoesNotPreventRetry() {
		Database database = database("retry_post_transaction_failure_after_retryable_failure");
		RetryPolicy retryPolicy = retryPolicy(3);
		AtomicInteger attempts = new AtomicInteger();
		RuntimeException postFailure = new RuntimeException("post");

		database.transactionWithRetry(retryPolicy, () -> {
			if (attempts.incrementAndGet() == 1) {
				database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> {
					throw postFailure;
				});

				throw serializationFailure("first");
			}
		});

		Assertions.assertEquals(2, attempts.get());
	}

	@Test
	public void testTransactionWithRetryPostTransactionFailureAfterCommitDoesNotRetryCommittedWork() {
		Database database = database("retry_post_transaction_failure_after_commit");
		RetryPolicy retryPolicy = retryPolicy(3);
		AtomicInteger attempts = new AtomicInteger();
		RuntimeException postFailure = new RuntimeException("post");

		PostTransactionOperationException thrown = Assertions.assertThrows(PostTransactionOperationException.class, () ->
				database.transactionWithRetry(retryPolicy, () -> {
					attempts.incrementAndGet();
					database.currentTransaction().orElseThrow().addPostTransactionOperation(result -> {
						throw postFailure;
					});
				}));

		Assertions.assertEquals(1, attempts.get());
		Assertions.assertSame(postFailure, thrown.getCause());
		Assertions.assertEquals(TransactionResult.COMMITTED, thrown.getTransactionResult());
	}

	@Test
	public void testTransactionWithRetryAppliesOptionsEachAttempt() {
		Database database = database("retry_options");
		RetryPolicy retryPolicy = retryPolicy(3);
		TransactionOptions transactionOptions = TransactionOptions.withIsolation(TransactionIsolation.SERIALIZABLE).build();
		AtomicInteger attempts = new AtomicInteger();
		List<TransactionIsolation> isolations = new ArrayList<>();

		database.transactionWithRetry(retryPolicy, transactionOptions, () -> {
			isolations.add(database.currentTransaction().orElseThrow().getTransactionIsolation());

			if (attempts.incrementAndGet() == 1)
				throw serializationFailure("first");
		});

		Assertions.assertEquals(List.of(TransactionIsolation.SERIALIZABLE, TransactionIsolation.SERIALIZABLE), isolations);
	}

	@Test
	public void testTransactionWithRetryBackoffNullOrNegativeFailsFast() {
		Database database = database("retry_backoff_bad_value");
		DatabaseException nullDelayFailure = serializationFailure("null");
		RetryPolicy nullDelayPolicy = RetryPolicy.of(3, RetryPolicy.Condition.serializationFailure(), (attempt, failure) -> null);

		NullPointerException nullDelay = Assertions.assertThrows(NullPointerException.class, () ->
				database.transactionWithRetry(nullDelayPolicy, () -> {
					throw nullDelayFailure;
				}));
		Assertions.assertArrayEquals(new Throwable[]{nullDelayFailure}, nullDelay.getSuppressed());

		DatabaseException negativeDelayFailure = serializationFailure("negative");
		RetryPolicy negativeDelayPolicy = RetryPolicy.of(3, RetryPolicy.Condition.serializationFailure(),
				(attempt, failure) -> Duration.ofNanos(-1));

		IllegalArgumentException negativeDelay = Assertions.assertThrows(IllegalArgumentException.class, () ->
				database.transactionWithRetry(negativeDelayPolicy, () -> {
					throw negativeDelayFailure;
				}));
		Assertions.assertArrayEquals(new Throwable[]{negativeDelayFailure}, negativeDelay.getSuppressed());
	}

	private RetryPolicy retryPolicy(Integer maxAttempts) {
		return RetryPolicy.of(maxAttempts, RetryPolicy.Condition.serializationFailure(), RetryPolicy.Backoff.fixed(Duration.ZERO));
	}

	private DatabaseException serializationFailure(String message) {
		return new DatabaseException(message, new SQLException(message, "40001", 0), DatabaseType.GENERIC.dialect());
	}

	private DatabaseException syntaxFailure(String message) {
		return new DatabaseException(message, new SQLException(message, "42000", 0), DatabaseType.GENERIC.dialect());
	}

	private Database database(String databaseName) {
		return Database.withDataSource(createInMemoryDataSource(databaseName)).build();
	}

	private DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}
}
