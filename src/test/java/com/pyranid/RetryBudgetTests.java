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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class RetryBudgetTests {
	@Test
	public void budgetCopiesAndValidationLeaveOriginalPolicyUnchanged() {
		RetryPolicy original = policy(Duration.ZERO);
		RetryPolicy bounded = original.withMaxElapsedTime(Duration.ofNanos(1));
		assertTrue(original.getMaxElapsedTime().isEmpty());
		assertEquals(Optional.of(Duration.ofNanos(1)), bounded.getMaxElapsedTime());
		assertFalse(original.toString().contains("maxElapsedTime="));
		assertTrue(bounded.toString().contains("maxElapsedTime=PT0.000000001S"));
		assertSame(original.getCondition(), bounded.getCondition());
		assertSame(original.getBackoff(), bounded.getBackoff());
		assertEquals(original.getMaxAttempts(), bounded.getMaxAttempts());
		assertTrue(bounded.withoutMaxElapsedTime().getMaxElapsedTime().isEmpty());
		assertEquals(Optional.of(Duration.ofNanos(1)), bounded.getMaxElapsedTime());
		assertThrows(NullPointerException.class, () -> original.withMaxElapsedTime(null));
		assertThrows(IllegalArgumentException.class, () -> original.withMaxElapsedTime(Duration.ZERO));
		assertThrows(IllegalArgumentException.class, () -> original.withMaxElapsedTime(Duration.ofNanos(-1)));
		assertEquals(Duration.ofSeconds(Long.MAX_VALUE, 999_999_999),
				original.withMaxElapsedTime(Duration.ofSeconds(Long.MAX_VALUE, 999_999_999)).getMaxElapsedTime().orElseThrow());
	}

	@Test
	public void firstAttemptAlwaysRunsAndSuccessMayFinishAfterBudget() {
		AtomicLong clock = new AtomicLong(100);
		AtomicInteger attempts = new AtomicInteger();
		TransactionRetryResult<String> result = executor(clock, delay -> fail()).execute(policy(Duration.ZERO).withMaxElapsedTime(Duration.ofNanos(1)), () -> {
			attempts.incrementAndGet(); clock.addAndGet(20); return Optional.of("completed");
		});
		assertEquals(1, attempts.get());
		assertEquals(Optional.of("completed"), result.getValue());
	}

	@Test
	public void firstFailureAfterBudgetPropagatesOriginalWithoutPolicyOrSleep() {
		AtomicLong clock = new AtomicLong();
		DatabaseException failure = new DatabaseException("failure");
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2, (attempt, error) -> { fail(); return Duration.ZERO; }, error -> {
			fail(); return true;
		}).withMaxElapsedTime(Duration.ofNanos(5));
		assertSame(failure, assertThrows(DatabaseException.class, () -> executor(clock, delay -> fail()).execute(policy, () -> {
			clock.addAndGet(5); throw failure;
		})));
	}

	@Test
	public void successfulRetryReturnsEarlierFailuresAndIncludesWaiting() {
		AtomicLong clock = new AtomicLong();
		List<Duration> sleeps = new ArrayList<>();
		AtomicInteger attempts = new AtomicInteger();
		DatabaseException failure = new DatabaseException("first");
		TransactionRetryResult<String> result = executor(clock, delay -> { sleeps.add(delay); clock.addAndGet(delay.toNanos()); })
				.execute(policy(Duration.ofNanos(3)).withMaxElapsedTime(Duration.ofNanos(10)), () -> {
					clock.addAndGet(2); if (attempts.incrementAndGet() == 1) throw failure; return Optional.of("ok");
				});
		assertEquals(2, result.getAttemptCount());
		assertEquals(List.of(failure), result.getFailures());
		assertEquals(List.of(Duration.ofNanos(3)), sleeps);
		assertEquals(7L, clock.get());
	}

	@Test
	public void delayEqualToOrGreaterThanRemainingBudgetStopsWithoutSleeping() {
		for (Duration delay : List.of(Duration.ofNanos(6), Duration.ofNanos(7), Duration.ofSeconds(Long.MAX_VALUE))) {
			AtomicLong clock = new AtomicLong();
			AtomicInteger attempts = new AtomicInteger();
			DatabaseException failure = new DatabaseException("last");
			assertSame(failure, assertThrows(DatabaseException.class, () -> executor(clock, ignored -> fail()).execute(
					policy(delay).withMaxElapsedTime(Duration.ofNanos(10)), () -> {
						attempts.incrementAndGet(); clock.addAndGet(4); throw failure;
					})));
			assertEquals(1, attempts.get());
			assertEquals(0, failure.getSuppressed().length);
		}
	}

	@Test
	public void policyEvaluationTimeCountsBeforeSleeping() {
		AtomicLong clock = new AtomicLong();
		DatabaseException failure = new DatabaseException("failure");
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2,
				(attempt, error) -> { clock.addAndGet(4); return Duration.ofNanos(2); },
				error -> { clock.addAndGet(4); return true; }).withMaxElapsedTime(Duration.ofNanos(10));
		assertSame(failure, assertThrows(DatabaseException.class, () -> executor(clock, delay -> fail()).execute(policy, () -> { throw failure; })));
		assertEquals(8L, clock.get());
	}

	@Test
	public void conditionThatConsumesRemainingBudgetDoesNotInvokeBackoff() {
		AtomicLong clock = new AtomicLong();
		DatabaseException failure = new DatabaseException("failure");
		RetryPolicy policy = RetryPolicy.ofMaxAttempts(2,
				(attempt, error) -> { fail("backoff must not run after the budget expires"); return Duration.ZERO; },
				error -> { clock.addAndGet(10); return true; }).withMaxElapsedTime(Duration.ofNanos(10));
		assertSame(failure, assertThrows(DatabaseException.class, () -> executor(clock, delay -> fail()).execute(policy, () -> { throw failure; })));
	}

	@Test
	public void sleepOvershootDoesNotAdmitAnotherAttemptAndKeepsPriorFailures() {
		AtomicLong clock = new AtomicLong();
		AtomicInteger sleeps = new AtomicInteger();
		AtomicInteger attempts = new AtomicInteger();
		DatabaseException first = new DatabaseException("first"), second = new DatabaseException("second");
		DatabaseException thrown = assertThrows(DatabaseException.class, () -> executor(clock, delay -> {
			clock.addAndGet(sleeps.incrementAndGet() == 1 ? 1 : 100);
		}).execute(policy(Duration.ofNanos(1)).withMaxElapsedTime(Duration.ofNanos(10)), () -> {
			if (attempts.incrementAndGet() == 1) throw first; throw second;
		}));
		assertSame(second, thrown);
		assertArrayEquals(new Throwable[]{first}, thrown.getSuppressed());
		assertEquals(2, attempts.get());
		assertEquals(2, sleeps.get());
	}

	@Test
	public void zeroDelayStillRechecksBudgetBeforeNextAttempt() {
		AtomicInteger reads = new AtomicInteger();
		AtomicInteger attempts = new AtomicInteger();
		DatabaseException failure = new DatabaseException("failure");
		RetryExecutor executor = new RetryExecutor(() -> reads.incrementAndGet() < 5 ? 0 : 10, delay -> fail());
		assertSame(failure, assertThrows(DatabaseException.class, () -> executor.execute(
				policy(Duration.ZERO).withMaxElapsedTime(Duration.ofNanos(10)), () -> { attempts.incrementAndGet(); throw failure; })));
		assertEquals(1, attempts.get());
	}

	@Test
	public void elapsedSubtractionHandlesMonotonicClockWraparound() {
		AtomicLong clock = new AtomicLong(Long.MAX_VALUE - 2);
		DatabaseException failure = new DatabaseException("failure");
		AtomicInteger attempts = new AtomicInteger();
		assertSame(failure, assertThrows(DatabaseException.class, () -> executor(clock, delay -> fail()).execute(
				policy(Duration.ZERO).withMaxElapsedTime(Duration.ofNanos(5)), () -> {
					attempts.incrementAndGet(); clock.addAndGet(5); throw failure;
				})));
		assertEquals(1, attempts.get());
	}

	@Test
	public void hugeBudgetDoesNotOverflowAndNoBudgetAvoidsClockEntirely() {
		for (boolean bounded : List.of(true, false)) {
			RetryPolicy policy = policy(Duration.ZERO);
			if (bounded) policy = policy.withMaxElapsedTime(Duration.ofSeconds(Long.MAX_VALUE, 999_999_999));
			AtomicInteger attempts = new AtomicInteger();
			RetryExecutor executor = new RetryExecutor(() -> { if (!bounded) fail(); return Long.MAX_VALUE; }, delay -> fail());
			assertEquals(2, executor.execute(policy, () -> {
				if (attempts.incrementAndGet() == 1) throw new DatabaseException("first"); return Optional.empty();
			}).getAttemptCount());
		}
	}

	@Test
	public void interruptionPreservesFlagOriginalFailureAndPriorFailures() {
		AtomicLong clock = new AtomicLong();
		AtomicInteger attempts = new AtomicInteger();
		AtomicInteger sleeps = new AtomicInteger();
		DatabaseException first = new DatabaseException("first"), second = new DatabaseException("second");
		InterruptedException interruption = new InterruptedException("stop");
		try {
			DatabaseException thrown = assertThrows(DatabaseException.class, () -> executor(clock, delay -> {
				if (sleeps.incrementAndGet() == 2) throw interruption;
			}).execute(policy(Duration.ofNanos(1)).withMaxElapsedTime(Duration.ofNanos(10)), () -> {
				if (attempts.incrementAndGet() == 1) throw first; throw second;
			}));
			assertSame(second, thrown);
			assertArrayEquals(new Throwable[]{first, interruption}, thrown.getSuppressed());
			assertTrue(Thread.currentThread().isInterrupted());
			assertEquals(2, attempts.get());
		} finally { Thread.interrupted(); }
	}

	@Test
	public void unsafeOutcomesBypassPolicyEvenWithAvailableBudget() {
		for (boolean committed : List.of(true, false)) {
			DatabaseException failure = new DatabaseException("unsafe");
			if (committed) failure.markTransactionOutcomeCommitted(); else failure.markTransactionOutcomeIndeterminate();
			RetryPolicy policy = RetryPolicy.ofMaxAttempts(3, (attempt, error) -> { fail(); return Duration.ZERO; }, error -> {
				fail(); return true;
			}).withMaxElapsedTime(Duration.ofDays(1));
			assertSame(failure, assertThrows(DatabaseException.class, () -> executor(new AtomicLong(), delay -> fail()).execute(policy, () -> { throw failure; })));
		}
	}

	private static RetryPolicy policy(Duration delay) {
		return RetryPolicy.ofMaxAttempts(3, RetryPolicy.Backoff.fixed(delay), failure -> true);
	}
	private static RetryExecutor executor(AtomicLong clock, RetryExecutor.Sleeper sleeper) {
		return new RetryExecutor(clock::get, sleeper);
	}
}
