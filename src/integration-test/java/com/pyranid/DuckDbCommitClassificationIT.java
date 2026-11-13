/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbCommitClassificationIT {
	@TempDir Path directory;

	@Test
	void competingPrimaryAndUniqueCommitsExposeConstraintFailuresWithoutUnsafeRetries() throws Exception {
		for (boolean primaryKey : new boolean[]{false, true}) {
			for (boolean outcomeApi : new boolean[]{false, true}) {
				Database database = Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
						"jdbc:duckdb:" + this.directory.resolve("commit_" + primaryKey + "_" + outcomeApi + ".duckdb"), null, null),
						DatabaseType.DUCK_DB).build();
				database.query("CREATE TABLE work (id INT PRIMARY KEY, name VARCHAR UNIQUE)").execute();
				CountDownLatch inserted = new CountDownLatch(1), winnerFinished = new CountDownLatch(1);
				AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger();
				AtomicReference<TransactionCompletion> observed = new AtomicReference<>();
				TransactionOptions options = TransactionOptions.withReadOnly(null).completionObserver(observed::set).build();
				ExecutorService worker = Executors.newSingleThreadExecutor();
				try {
					Future<Failure> losing = worker.submit(() -> {
						ReturningTransactionalOperation<String> operation = () -> {
							attempts.incrementAndGet();
							database.query("INSERT INTO work VALUES (10, 'earlier work')").execute();
							database.query("INSERT INTO work VALUES (1, 'conflict value')").execute();
							inserted.countDown();
							assertTrue(winnerFinished.await(10, TimeUnit.SECONDS), "Winning transaction did not finish");
							return Optional.of("attempted receipt");
						};
						try {
							if (outcomeApi) {
								TransactionOutcome<String> outcome = database.transactionWithOutcome(options, operation);
								return new Failure((DatabaseException) outcome.getFailure().orElseThrow(), outcome.getCompletion(), outcome.getAttemptedValue());
							}
							RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
								policies.incrementAndGet();
								return failure.isUniqueConstraintViolation() || failure.isSerializationFailure();
							});
							DatabaseException failure = assertThrows(DatabaseException.class,
									() -> database.transactionWithRetry(policy, options, operation));
							return new Failure(failure, observed.get(), Optional.empty());
						} finally {
							inserted.countDown();
						}
					});
					assertTrue(inserted.await(10, TimeUnit.SECONDS), "Losing transaction did not insert");
					assertFalse(losing.isDone(), "Losing transaction failed before the competing commit");
					try {
						database.transaction(() -> database.query(primaryKey
								? "INSERT INTO work VALUES (1, 'winner')" : "INSERT INTO work VALUES (2, 'conflict value')").execute());
					} finally {
						winnerFinished.countDown();
					}
					Failure result = losing.get(10, TimeUnit.SECONDS);
					assertTrue(result.failure().isUniqueConstraintViolation());
					assertFalse(result.failure().isSerializationFailure());
					assertFalse(result.failure().isTransient());
					assertEquals(TransactionResult.IN_DOUBT, result.completion().getResult());
					assertSame(result.failure(), result.completion().getFailure().orElseThrow());
					assertEquals(outcomeApi ? Optional.of("attempted receipt") : Optional.empty(), result.value());
					assertEquals(1, attempts.get());
					assertEquals(0, policies.get());
					assertEquals(List.of(primaryKey ? 1 : 2), database.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class));
				} finally {
					winnerFinished.countDown();
					worker.shutdownNow();
					assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
				}
			}
		}
	}

	private record Failure(DatabaseException failure, TransactionCompletion completion, Optional<String> value) {}
}
