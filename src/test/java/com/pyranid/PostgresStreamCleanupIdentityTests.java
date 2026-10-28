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

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PostgresStreamCleanupIdentityTests {
	@Test
	void sharedResourceAndRollbackFailureStillAbortsClosesAndPreservesCallbackFailure() {
		for (Throwable shared : List.of(new SQLException("connection lost", "08006"), new AssertionError("driver failed"))) {
			Fixture fixture = new Fixture(shared);
			IllegalStateException original = new IllegalStateException("callback failed");
			assertSame(original, assertThrows(IllegalStateException.class,
					() -> fixture.database.query("SELECT 1").fetchStream(Integer.class, stream -> { throw original; })));
			assertArrayEquals(new Throwable[]{shared}, original.getSuppressed());
			assertArrayEquals(new Throwable[0], shared.getSuppressed());
			assertEquals(List.of("resultSet.close", "statement.close", "rollback", "abort", "connection.close"), fixture.cleanup);
		}
	}

	@Test
	void sharedResourceAndCommitFailureStillAbortsAndCloses() {
		for (Throwable shared : List.of(new SQLException("connection lost", "08006"), new AssertionError("driver failed"))) {
			Fixture fixture = new Fixture(shared);
			Throwable thrown = assertThrows(Throwable.class,
					() -> fixture.database.query("SELECT 1").fetchStream(Integer.class, stream -> "finished"));
			if (shared instanceof Error) assertSame(shared, thrown);
			else assertSame(shared, thrown.getCause());
			assertArrayEquals(new Throwable[0], shared.getSuppressed());
			assertEquals(List.of("resultSet.close", "statement.close", "commit", "abort", "connection.close"), fixture.cleanup);
		}
	}

	private static final class Fixture {
		private final Database database;
		private final List<String> cleanup = new ArrayList<>();

		private Fixture(Throwable shared) {
			ResultSet result = proxy(ResultSet.class, (method, args) -> {
				if (method.getName().equals("close")) {
					this.cleanup.add("resultSet.close");
					throw shared;
				}
				throw new AssertionError("Unexpected result set method: " + method.getName());
			});
			PreparedStatement statement = proxy(PreparedStatement.class, (method, args) -> switch (method.getName()) {
				case "executeQuery" -> result;
				case "getFetchSize" -> 0;
				case "setFetchSize" -> null;
				case "close" -> { this.cleanup.add("statement.close"); throw shared; }
				default -> throw new AssertionError("Unexpected statement method: " + method.getName());
			});
			Connection connection = proxy(Connection.class, (method, args) -> switch (method.getName()) {
				case "getAutoCommit" -> true;
				case "setAutoCommit" -> {
					assertEquals(false, args[0], "Failed completion must not restore auto-commit");
					yield null;
				}
				case "prepareStatement" -> statement;
				case "commit", "rollback", "abort" -> { this.cleanup.add(method.getName()); throw shared; }
				case "close" -> { this.cleanup.add("connection.close"); yield null; }
				default -> throw new AssertionError("Unexpected connection method: " + method.getName());
			});
			DataSource source = proxy(DataSource.class, (method, args) -> {
				if (method.getName().equals("getConnection")) return connection;
				throw new AssertionError("Unexpected data source method: " + method.getName());
			});
			this.database = Database.withDataSource(source).databaseType(DatabaseType.POSTGRESQL).build();
		}
	}

	private interface Call { Object invoke(Method method, Object[] args) throws Throwable; }

	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> call.invoke(method, args)));
	}
}
