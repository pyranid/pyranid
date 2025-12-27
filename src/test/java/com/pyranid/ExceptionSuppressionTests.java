/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2025 Revetware LLC.
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
import java.util.Arrays;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.0.0
 */
public class ExceptionSuppressionTests {
	@Test
	public void testStatementLoggerExceptionSuppressedWhenOperationFails() {
		RuntimeException loggerFailure = new RuntimeException("logger failed");
		StatementLogger statementLogger = (statementLog) -> {
			throw loggerFailure;
		};

		Database db = Database.withDataSource(createInMemoryDataSource("logger_suppressed"))
				.statementLogger(statementLogger)
				.build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class,
				() -> db.query("SELECT * FROM missing_table").fetchList(String.class));

		Assertions.assertTrue(
				Arrays.stream(ex.getSuppressed()).anyMatch(suppressed -> "logger failed".equals(suppressed.getMessage())),
				"Expected statement logger failure to be suppressed");
	}

	@Test
	public void testPostTransactionOperationExceptionSuppressedWhenOperationFails() {
		Database db = Database.withDataSource(createInMemoryDataSource("txn_suppressed")).build();
		RuntimeException boom = new RuntimeException("boom");
		RuntimeException postFailure = new RuntimeException("post");

		RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> {
			db.transaction(() -> {
				db.currentTransaction().orElseThrow()
						.addPostTransactionOperation(result -> {
							throw postFailure;
						});
				throw boom;
			});
		});

		Assertions.assertSame(boom, ex, "Expected original exception to be thrown");
		Assertions.assertTrue(
				Arrays.stream(ex.getSuppressed()).anyMatch(suppressed -> "post".equals(suppressed.getMessage())),
				"Expected post-transaction failure to be suppressed");
	}

	@NonNull
	private DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}
}
