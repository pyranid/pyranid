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

import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;

import static java.util.Objects.requireNonNull;

/** Overflow-safe uniform sampling without converting an arbitrary Duration to a long nanosecond count. */
final class FullJitterBackoff implements RetryPolicy.Backoff {
	private final RetryPolicy.Backoff delegate;

	FullJitterBackoff(RetryPolicy.Backoff delegate) {
		this.delegate = requireNonNull(delegate);
	}

	@Override
	@NonNull
	public Duration delayAfterFailedAttempt(@NonNull Integer failedAttemptNumber, @NonNull DatabaseException failure) {
		requireNonNull(failedAttemptNumber);
		requireNonNull(failure);
		if (failedAttemptNumber < 1)
			throw new IllegalArgumentException("failedAttemptNumber must be at least 1");
		Duration cap = requireNonNull(this.delegate.delayAfterFailedAttempt(failedAttemptNumber, failure),
				"RetryPolicy.Backoff returned null");
		if (cap.isNegative())
			throw new IllegalArgumentException("RetryPolicy.Backoff returned a negative delay");
		return sample(cap, ThreadLocalRandom.current());
	}

	static Duration sample(Duration cap, RandomGenerator random) {
		if (cap.isZero())
			return Duration.ZERO;
		if (cap.getSeconds() == 0)
			return Duration.ofNanos(random.nextLong(cap.getNano()));

		// Sample the enclosing seconds/nanos rectangle, rejecting the incomplete final second. Every retained
		// nanosecond has equal probability. Acceptance is at least one half, even for an exact one-second cap.
		for (;;) {
			long seconds = cap.getSeconds() == Long.MAX_VALUE ? random.nextLong() >>> 1
					: random.nextLong(cap.getSeconds() + 1);
			int nanos = random.nextInt(1_000_000_000);
			if (seconds < cap.getSeconds() || nanos < cap.getNano())
				return Duration.ofSeconds(seconds, nanos);
		}
	}
}
