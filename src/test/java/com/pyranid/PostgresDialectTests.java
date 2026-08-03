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

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PostgresDialectTests {
	@Test
	public void failedStreamingRollbackSkipsAutoCommitRestoreAndAbortsOnce() {
		assertFailedStreamingCompletionSkipsAutoCommitRestoreAndAbortsOnce(false, "rollback");
	}

	@Test
	public void failedStreamingCommitSkipsAutoCommitRestoreAndAbortsOnce() {
		assertFailedStreamingCompletionSkipsAutoCommitRestoreAndAbortsOnce(true, "commit");
	}

	private void assertFailedStreamingCompletionSkipsAutoCommitRestoreAndAbortsOnce(boolean streamSucceeded,
																												String terminalOperation) {
		SQLException terminalFailure = new SQLException(terminalOperation + " failed");
		List<String> events = new ArrayList<>();
		AtomicInteger autoCommitRestoreAttempts = new AtomicInteger();
		AtomicInteger aborts = new AtomicInteger();

		Connection connection = (Connection) Proxy.newProxyInstance(
				Connection.class.getClassLoader(),
				new Class<?>[]{Connection.class},
				(proxy, method, args) -> {
					String methodName = method.getName();

					if (terminalOperation.equals(methodName)) {
						events.add(methodName);
						throw terminalFailure;
					}

					if ("setAutoCommit".equals(methodName)) {
						events.add("setAutoCommit");
						autoCommitRestoreAttempts.incrementAndGet();
						return null;
					}

					if ("abort".equals(methodName)) {
						events.add("abort");
						aborts.incrementAndGet();
						return null;
					}

					throw new AssertionError("Unexpected connection method: " + methodName);
				});

		Throwable cleanupFailure = PostgresDialect.INSTANCE.completeStreamingConnection(
				connection,
				DatabaseStreamState.managedTransaction(true),
				streamSucceeded,
				null);

		Assertions.assertSame(terminalFailure, cleanupFailure);
		Assertions.assertEquals(0, autoCommitRestoreAttempts.get());
		Assertions.assertEquals(1, aborts.get());
		Assertions.assertEquals(List.of(terminalOperation, "abort"), events);
	}
}
