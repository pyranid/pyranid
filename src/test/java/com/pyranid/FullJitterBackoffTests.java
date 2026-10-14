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
import java.util.List;
import java.util.Random;
import java.util.random.RandomGenerator;

import static org.junit.jupiter.api.Assertions.*;

public class FullJitterBackoffTests {
	@Test
	public void jitterRejectsInvalidDelegateResultsAndArguments() {
		DatabaseException failure = new DatabaseException("failure");
		assertThrows(NullPointerException.class, () -> RetryPolicy.Backoff.fullJitter(null));
		assertThrows(NullPointerException.class, () -> RetryPolicy.Backoff.fullJitter((attempt, error) -> null).delayAfterFailedAttempt(1, failure));
		assertThrows(IllegalArgumentException.class, () -> RetryPolicy.Backoff.fullJitter((attempt, error) -> Duration.ofNanos(-1)).delayAfterFailedAttempt(1, failure));
		RetryPolicy.Backoff backoff = RetryPolicy.Backoff.fullJitter(RetryPolicy.Backoff.fixed(Duration.ofSeconds(1)));
		assertThrows(NullPointerException.class, () -> backoff.delayAfterFailedAttempt(null, failure));
		assertThrows(NullPointerException.class, () -> backoff.delayAfterFailedAttempt(1, null));
		assertThrows(IllegalArgumentException.class, () -> backoff.delayAfterFailedAttempt(0, failure));
	}

	@Test
	public void zeroCapNeverConsultsRandomAndOneNanosecondOnlyReturnsZero() {
		RandomGenerator forbidden = () -> { throw new AssertionError("random called"); };
		assertEquals(Duration.ZERO, FullJitterBackoff.sample(Duration.ZERO, forbidden));
		assertEquals(Duration.ZERO, RetryPolicy.Backoff.fullJitter(RetryPolicy.Backoff.fixed(Duration.ZERO))
				.delayAfterFailedAttempt(1, new DatabaseException("failure")));
		assertEquals(Duration.ZERO, FullJitterBackoff.sample(Duration.ofNanos(1), new Random(41)));
	}

	@Test
	public void deterministicSamplesStayWithinTinyOrdinaryAndExtremeCaps() {
		Random random = new Random(421);
		for (Duration cap : List.of(Duration.ofNanos(7), Duration.ofSeconds(1), Duration.ofSeconds(1, 1),
				Duration.ofSeconds(Long.MAX_VALUE, 999_999_999), Duration.ofSeconds(Long.MAX_VALUE), Duration.ofSeconds(Long.MAX_VALUE - 1))) {
			for (int i = 0; i < 1_000; ++i) {
				Duration sample = FullJitterBackoff.sample(cap, random);
				assertFalse(sample.isNegative());
				assertTrue(sample.compareTo(cap) < 0, () -> sample + " must be below " + cap);
			}
		}
	}

	@Test
	public void incompleteLastSecondIsRejectedRatherThanBiasedOrClamped() {
		RandomGenerator random = new RandomGenerator() {
			private int calls;
			@Override public long nextLong() { throw new AssertionError(); }
			@Override public long nextLong(long bound) { assertEquals(2L, bound); return 1; }
			@Override public int nextInt(int bound) { assertEquals(1_000_000_000, bound); return calls++ == 0 ? 1 : 0; }
		};
		assertEquals(Duration.ofSeconds(1), FullJitterBackoff.sample(Duration.ofSeconds(1, 1), random));
	}

	@Test
	public void maximumDurationCanSampleTheLastRepresentableNanosecondBelowCap() {
		RandomGenerator random = new RandomGenerator() {
			@Override public long nextLong() { return -1L; }
			@Override public int nextInt(int bound) { return 999_999_998; }
		};
		assertEquals(Duration.ofSeconds(Long.MAX_VALUE, 999_999_998),
				FullJitterBackoff.sample(Duration.ofSeconds(Long.MAX_VALUE, 999_999_999), random));
	}
}
