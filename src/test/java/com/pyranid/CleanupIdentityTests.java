/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CleanupIdentityTests {
	@Test
	void repeatedArrayCleanupFailureDoesNotSkipLaterFrees() {
		Fixture fixture = new Fixture();
		RuntimeException thrown = assertThrows(RuntimeException.class, () -> fixture.query().execute());
		assertSame(fixture.cleanupFailure, thrown.getCause());
		assertArrayEquals(new Throwable[0], fixture.cleanupFailure.getSuppressed());
		fixture.assertClosed(false);
	}

	@Test
	void repeatedArrayCleanupFailurePreservesExecutionFailure() {
		Fixture fixture = new Fixture();
		fixture.operationFailure = new SQLException("original execution failure", "42000");
		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.query().execute());
		assertSame(fixture.operationFailure, thrown.getCause());
		assertArrayEquals(new Throwable[]{fixture.cleanupFailure}, thrown.getSuppressed());
		fixture.assertClosed(false);
	}

	@Test
	void repeatedArrayCleanupFailureDoesNotSkipStreamResources() {
		Fixture fixture = new Fixture();
		RuntimeException thrown = assertThrows(RuntimeException.class,
				() -> fixture.query().fetchStream(Integer.class, stream -> stream.findFirst()));
		assertSame(fixture.cleanupFailure, thrown.getCause());
		fixture.assertClosed(true);
	}

	@Test
	void repeatedFailureAcrossEveryStreamResourceStillReleasesAllOfThem() {
		Fixture fixture = new Fixture();
		fixture.closeFailure = fixture.cleanupFailure;
		RuntimeException thrown = assertThrows(RuntimeException.class,
				() -> fixture.query().fetchStream(Integer.class, stream -> stream.findFirst()));
		assertSame(fixture.cleanupFailure, thrown.getCause());
		fixture.assertClosed(true);
	}

	@Test
	void distinctCleanupFailuresRemainSuppressedInOrder() {
		for (boolean stream : new boolean[]{false, true}) {
			Fixture fixture = new Fixture();
			SQLException second = new SQLException("second cleanup failure", "08006");
			fixture.secondCleanupFailure = second;
			RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
				if (stream) fixture.query().fetchStream(Integer.class, values -> values.findFirst());
				else fixture.query().execute();
			});
			assertSame(fixture.cleanupFailure, thrown.getCause());
			assertArrayEquals(new Throwable[]{second}, fixture.cleanupFailure.getSuppressed());
			fixture.assertClosed(stream);
		}
	}

	@Test
	void executionAndCleanupCanThrowTheSameError() {
		for (boolean stream : new boolean[]{false, true}) {
			Fixture fixture = new Fixture();
			AssertionError shared = new AssertionError("execution and cleanup");
			fixture.operationFailure = shared;
			fixture.cleanupFailure = shared;
			AssertionError thrown = assertThrows(AssertionError.class, () -> {
				if (stream) fixture.query().fetchStream(Integer.class, values -> values.findFirst());
				else fixture.query().execute();
			});
			assertSame(shared, thrown);
			assertArrayEquals(new Throwable[0], thrown.getSuppressed());
			fixture.assertClosed(stream);
		}
	}

	@Test
	void callbackAndCleanupCanThrowTheSameError() {
		Fixture fixture = new Fixture();
		AssertionError shared = new AssertionError("callback and cleanup");
		fixture.cleanupFailure = shared;
		assertSame(shared, assertThrows(AssertionError.class,
				() -> fixture.query().fetchStream(Integer.class, stream -> { throw shared; })));
		assertArrayEquals(new Throwable[0], shared.getSuppressed());
		fixture.assertClosed(true);
	}

	@Test
	void loggingAndCleanupCanThrowTheSameError() {
		for (boolean stream : new boolean[]{false, true}) {
			Fixture fixture = new Fixture();
			AssertionError shared = new AssertionError("logger and cleanup");
			fixture.cleanupFailure = shared;
			fixture.logFailure = shared;
			assertSame(shared, assertThrows(AssertionError.class, () -> {
				if (stream) fixture.query().fetchStream(Integer.class, values -> values.findFirst());
				else fixture.query().execute();
			}));
			assertArrayEquals(new Throwable[0], shared.getSuppressed());
			fixture.assertClosed(stream);
		}
	}

	@Test
	void rawOperationAndConnectionCloseCanThrowTheSameError() {
		for (boolean participating : new boolean[]{false, true}) {
			Fixture fixture = new Fixture();
			AssertionError shared = new AssertionError("operation and close");
			fixture.closeFailure = shared;
			assertSame(shared, assertThrows(AssertionError.class, () -> {
				if (participating) fixture.database.useRawConnection(connection -> { throw shared; });
				else fixture.database.readDatabaseMetaData(metadata -> { throw shared; });
			}));
			assertArrayEquals(new Throwable[0], shared.getSuppressed());
			assertEquals(1, fixture.connections.get());
		}
	}

	@Test
	void parameterArraysAreReleasedAfterTheStreamCursorAndStatement() {
		List<String> releaseOrder = new ArrayList<>();
		Connection[] connection = new Connection[1];
		ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (method, args) -> switch (method.getName()) {
			case "getColumnCount" -> 1;
			case "getColumnLabel", "getColumnName" -> "value";
			case "getColumnType" -> Types.INTEGER;
			case "getColumnTypeName" -> "INTEGER";
			default -> zero(method.getReturnType());
		});
		AtomicInteger rows = new AtomicInteger();
		ResultSet result = proxy(ResultSet.class, (method, args) -> switch (method.getName()) {
			case "getMetaData" -> metadata;
			case "next" -> rows.getAndIncrement() == 0;
			case "getInt", "getObject" -> 7;
			case "close" -> { releaseOrder.add("cursor"); yield null; }
			default -> zero(method.getReturnType());
		});
		PreparedStatement statement = proxy(PreparedStatement.class, (method, args) -> switch (method.getName()) {
			case "getConnection" -> connection[0];
			case "executeQuery" -> result;
			case "close" -> { releaseOrder.add("statement"); yield null; }
			default -> zero(method.getReturnType());
		});
		connection[0] = proxy(Connection.class, (method, args) -> switch (method.getName()) {
			case "prepareStatement" -> statement;
			case "createArrayOf" -> proxy(java.sql.Array.class, (arrayMethod, arrayArgs) -> {
				if (arrayMethod.getName().equals("free")) {
					releaseOrder.add("array");
					return null;
				}
				return zero(arrayMethod.getReturnType());
			});
			case "getAutoCommit" -> true;
			default -> zero(method.getReturnType());
		});
		DataSource source = proxy(DataSource.class, (method, args) -> method.getName().equals("getConnection")
				? connection[0] : zero(method.getReturnType()));
		Database database = Database.withDataSource(source).databaseType(DatabaseType.DUCK_DB).build();

		assertEquals(Optional.of(7), database.query("SELECT 7 AS value WHERE :embedding IS NOT NULL")
				.bind("embedding", Parameters.vectorOfDoubles(new double[]{0.25, 0.5}))
				.fetchStream(Integer.class, stream -> stream.findFirst()));

		// A parameter resource is released after the statement that was handed it, never before
		assertEquals(List.of("cursor", "statement", "array"), releaseOrder);
	}

	@Test
	void aStreamCursorCloseFailureOutranksAParameterReleaseFailure() {
		SQLException cursorFailure = new SQLException("driver lost the cursor while closing it", "08006");
		SQLException arrayFailure = new SQLException("driver lost the array while freeing it", "08006");
		Connection[] connection = new Connection[1];
		ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (method, args) -> switch (method.getName()) {
			case "getColumnCount" -> 1;
			case "getColumnLabel", "getColumnName" -> "value";
			case "getColumnType" -> Types.INTEGER;
			case "getColumnTypeName" -> "INTEGER";
			default -> zero(method.getReturnType());
		});
		AtomicInteger rows = new AtomicInteger();
		ResultSet result = proxy(ResultSet.class, (method, args) -> switch (method.getName()) {
			case "getMetaData" -> metadata;
			case "next" -> rows.getAndIncrement() == 0;
			case "getInt", "getObject" -> 7;
			case "close" -> throw cursorFailure;
			default -> zero(method.getReturnType());
		});
		PreparedStatement statement = proxy(PreparedStatement.class, (method, args) -> switch (method.getName()) {
			case "getConnection" -> connection[0];
			case "executeQuery" -> result;
			default -> zero(method.getReturnType());
		});
		connection[0] = proxy(Connection.class, (method, args) -> switch (method.getName()) {
			case "prepareStatement" -> statement;
			case "createArrayOf" -> proxy(java.sql.Array.class, (arrayMethod, arrayArgs) -> {
				if (arrayMethod.getName().equals("free")) throw arrayFailure;
				return zero(arrayMethod.getReturnType());
			});
			case "getAutoCommit" -> true;
			default -> zero(method.getReturnType());
		});
		DataSource source = proxy(DataSource.class, (method, args) -> method.getName().equals("getConnection")
				? connection[0] : zero(method.getReturnType()));
		Database database = Database.withDataSource(source).databaseType(DatabaseType.DUCK_DB).build();

		RuntimeException thrown = assertThrows(RuntimeException.class,
				() -> database.query("SELECT 7 AS value WHERE :embedding IS NOT NULL")
						.bind("embedding", Parameters.vectorOfDoubles(new double[]{0.25, 0.5}))
						.fetchStream(Integer.class, stream -> stream.findFirst()));

		// Cleanup runs in release order and keeps the first failure, so closing the cursor - which happens
		// before a parameter resource may be released - is what reports, and the release is suppressed onto it
		assertSame(cursorFailure, thrown.getCause());
		assertArrayEquals(new Throwable[]{arrayFailure}, cursorFailure.getSuppressed());
	}

	private static final class Fixture {
		final AtomicInteger frees = new AtomicInteger();
		final AtomicInteger statements = new AtomicInteger();
		final AtomicInteger connections = new AtomicInteger();
		final AtomicInteger results = new AtomicInteger();
		Throwable cleanupFailure = new SQLException("driver connection lost while freeing arrays", "08006");
		Throwable secondCleanupFailure;
		Throwable operationFailure;
		Throwable closeFailure;
		Error logFailure;
		final Database database;

		Fixture() {
			Connection[] connection = new Connection[1];
			ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (method, args) -> switch (method.getName()) {
				case "getColumnCount" -> 1;
				case "getColumnLabel", "getColumnName" -> "value";
				case "getColumnType" -> Types.INTEGER;
				case "getColumnTypeName" -> "INTEGER";
				default -> zero(method.getReturnType());
			});
			AtomicInteger rows = new AtomicInteger();
			ResultSet result = proxy(ResultSet.class, (method, args) -> switch (method.getName()) {
				case "getMetaData" -> metadata;
				case "next" -> {
					if (this.operationFailure != null) throw this.operationFailure;
					yield rows.getAndIncrement() == 0;
				}
				case "getInt", "getObject" -> 7;
				case "close" -> { this.results.incrementAndGet(); failClose(); yield null; }
				default -> zero(method.getReturnType());
			});
			PreparedStatement statement = proxy(PreparedStatement.class, (method, args) -> switch (method.getName()) {
				case "getConnection" -> connection[0];
				case "executeLargeUpdate" -> { failOperation(); yield 1L; }
				case "executeUpdate" -> { failOperation(); yield 1; }
				case "executeQuery" -> result;
				case "close" -> { this.statements.incrementAndGet(); failClose(); yield null; }
				default -> zero(method.getReturnType());
			});
			connection[0] = proxy(Connection.class, (method, args) -> switch (method.getName()) {
				case "prepareStatement" -> statement;
				case "createArrayOf" -> proxy(java.sql.Array.class, (arrayMethod, arrayArgs) -> {
					if (arrayMethod.getName().equals("free")) {
						int count = this.frees.incrementAndGet();
						if (count == 1) throw this.cleanupFailure;
						if (count == 2) throw this.secondCleanupFailure == null ? this.cleanupFailure : this.secondCleanupFailure;
						return null;
					}
					if (arrayMethod.getName().equals("getArray")) return args[1];
					return zero(arrayMethod.getReturnType());
				});
				case "getAutoCommit" -> true;
				case "close" -> { this.connections.incrementAndGet(); failClose(); yield null; }
				default -> zero(method.getReturnType());
			});
			DataSource source = proxy(DataSource.class, (method, args) -> method.getName().equals("getConnection")
					? connection[0] : zero(method.getReturnType()));
			this.database = Database.withDataSource(source).databaseType(DatabaseType.GENERIC)
					.statementLogger(log -> { if (this.logFailure != null) throw this.logFailure; }).build();
		}

		Query query() {
			return this.database.query("SELECT 7 AS value WHERE :a IS NOT NULL AND :b IS NOT NULL AND :c IS NOT NULL")
					.bind("a", Parameters.sqlArrayOf("INTEGER", List.of(1)))
					.bind("b", Parameters.sqlArrayOf("INTEGER", List.of(2)))
					.bind("c", Parameters.sqlArrayOf("INTEGER", List.of(3)));
		}

		void failOperation() throws Throwable { if (this.operationFailure != null) throw this.operationFailure; }
		void failClose() throws Throwable { if (this.closeFailure != null) throw this.closeFailure; }

		void assertClosed(boolean stream) {
			assertAll(() -> assertEquals(3, this.frees.get()), () -> assertEquals(1, this.statements.get()),
					() -> assertEquals(1, this.connections.get()), () -> assertEquals(stream ? 1 : 0, this.results.get()));
		}
	}

	private interface Call { Object call(Method method, Object[] args) throws Throwable; }

	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> call.call(method, args)));
	}

	private static Object zero(Class<?> type) {
		if (type == boolean.class) return false;
		if (type == int.class) return 0;
		if (type == long.class) return 0L;
		if (type == short.class) return (short) 0;
		if (type == byte.class) return (byte) 0;
		if (type == float.class) return 0F;
		if (type == double.class) return 0D;
		if (type == char.class) return (char) 0;
		return null;
	}
}
