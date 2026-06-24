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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.4.0
 */
public class RetryPolicyTests {
	@Test
	public void testRetryPolicyValidation() {
		Assertions.assertThrows(NullPointerException.class, () ->
				RetryPolicy.ofMaxAttempts(null, RetryPolicy.Backoff.fixed(Duration.ZERO), RetryPolicy.Condition.serializationFailureOrDeadlock()));
		Assertions.assertThrows(NullPointerException.class, () ->
				RetryPolicy.ofMaxAttempts(1, RetryPolicy.Backoff.fixed(Duration.ZERO), null));
		Assertions.assertThrows(NullPointerException.class, () ->
				RetryPolicy.ofMaxAttempts(1, null, RetryPolicy.Condition.serializationFailureOrDeadlock()));
		Assertions.assertThrows(IllegalArgumentException.class, () ->
				RetryPolicy.ofMaxAttempts(0, RetryPolicy.Backoff.fixed(Duration.ZERO), RetryPolicy.Condition.serializationFailureOrDeadlock()));
	}

	@Test
	public void testBuiltInConditions() {
		DatabaseException serializationFailure = databaseException(new SQLException("serialization", "40001", 0));
		DatabaseException deadlock = databaseException(new SQLException("deadlock", "40P01", 0), DatabaseType.POSTGRESQL);
		DatabaseException timeout = databaseException(new SQLException("timeout", "57014", 0));
		DatabaseException syntax = databaseException(new SQLException("syntax", "42000", 0));

		Assertions.assertTrue(RetryPolicy.Condition.serializationFailureOrDeadlock().shouldRetry(serializationFailure));
		Assertions.assertTrue(RetryPolicy.Condition.serializationFailureOrDeadlock().shouldRetry(deadlock));
		Assertions.assertFalse(RetryPolicy.Condition.serializationFailureOrDeadlock().shouldRetry(timeout));
		Assertions.assertTrue(RetryPolicy.Condition.serializationFailure().shouldRetry(serializationFailure));
		Assertions.assertFalse(RetryPolicy.Condition.serializationFailure().shouldRetry(deadlock));
		Assertions.assertTrue(RetryPolicy.Condition.deadlock().shouldRetry(deadlock));
		Assertions.assertFalse(RetryPolicy.Condition.deadlock().shouldRetry(serializationFailure));
		Assertions.assertTrue(RetryPolicy.Condition.timeout().shouldRetry(timeout));
		Assertions.assertFalse(RetryPolicy.Condition.timeout().shouldRetry(syntax));
	}

	@Test
	public void testFixedBackoff() {
		Duration delay = Duration.ofMillis(25);
		RetryPolicy.Backoff backoff = RetryPolicy.Backoff.fixed(delay);

		Assertions.assertEquals(delay, backoff.delayAfterFailedAttempt(1, databaseException(new SQLException())));
		Assertions.assertEquals(delay, backoff.delayAfterFailedAttempt(2, databaseException(new SQLException())));
		Assertions.assertThrows(NullPointerException.class, () -> RetryPolicy.Backoff.fixed(null));
		Assertions.assertThrows(IllegalArgumentException.class, () -> RetryPolicy.Backoff.fixed(Duration.ofNanos(-1)));
		Assertions.assertThrows(NullPointerException.class, () ->
				backoff.delayAfterFailedAttempt(null, databaseException(new SQLException())));
		Assertions.assertThrows(NullPointerException.class, () -> backoff.delayAfterFailedAttempt(1, null));
		Assertions.assertThrows(IllegalArgumentException.class, () ->
				backoff.delayAfterFailedAttempt(0, databaseException(new SQLException())));
	}

	@Test
	public void testExponentialBackoff() {
		RetryPolicy.Backoff backoff = RetryPolicy.Backoff.exponential(Duration.ofMillis(25), Duration.ofMillis(90));
		DatabaseException failure = databaseException(new SQLException());

		Assertions.assertEquals(Duration.ofMillis(25), backoff.delayAfterFailedAttempt(1, failure));
		Assertions.assertEquals(Duration.ofMillis(50), backoff.delayAfterFailedAttempt(2, failure));
		Assertions.assertEquals(Duration.ofMillis(90), backoff.delayAfterFailedAttempt(3, failure));
		Assertions.assertEquals(Duration.ofMillis(90), backoff.delayAfterFailedAttempt(4, failure));

		Assertions.assertThrows(NullPointerException.class, () -> RetryPolicy.Backoff.exponential(null, Duration.ZERO));
		Assertions.assertThrows(NullPointerException.class, () -> RetryPolicy.Backoff.exponential(Duration.ZERO, null));
		Assertions.assertThrows(IllegalArgumentException.class, () ->
				RetryPolicy.Backoff.exponential(Duration.ofNanos(-1), Duration.ZERO));
		Assertions.assertThrows(IllegalArgumentException.class, () ->
				RetryPolicy.Backoff.exponential(Duration.ZERO, Duration.ofNanos(-1)));
		Assertions.assertThrows(IllegalArgumentException.class, () ->
				RetryPolicy.Backoff.exponential(Duration.ofMillis(2), Duration.ofMillis(1)));
	}

	@Test
	public void testExponentialBackoffOverflowSaturatesAtMaxDelay() {
		RetryPolicy.Backoff backoff = RetryPolicy.Backoff.exponential(Duration.ofSeconds(Long.MAX_VALUE / 2),
				Duration.ofSeconds(Long.MAX_VALUE));

		Assertions.assertEquals(Duration.ofSeconds(Long.MAX_VALUE),
				backoff.delayAfterFailedAttempt(3, databaseException(new SQLException())));
	}

	private DatabaseException databaseException(SQLException cause) {
		return databaseException(cause, DatabaseType.GENERIC);
	}

	private DatabaseException databaseException(SQLException cause,
																						 DatabaseType databaseType) {
		return new DatabaseException(cause.getMessage(), cause, databaseType.dialect());
	}
}
