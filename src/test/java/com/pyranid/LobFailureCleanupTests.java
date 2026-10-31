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
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class LobFailureCleanupTests {
	public record BinaryPair(byte[] first, byte[] second) {}
	public record TextPair(String first, String second) {}

	@Test
	void reusedReadAndStreamCloseErrorsNeverReachTheRetryCondition() {
		for (boolean character : new boolean[]{false, true}) {
			for (boolean cached : new boolean[]{false, true}) {
				for (boolean stream : new boolean[]{false, true}) {
					AssertionError fatal = new AssertionError("cached LOB read failure");
					FaultLob lob = new FaultLob(character, fatal, fatal, null);
					Fixture fixture = new Fixture(cached, lob);

					fixture.assertFatalWithoutRetry(fatal, stream);

					assertEquals(0, fatal.getSuppressed().length);
					fixture.assertReleased();
				}
			}
		}
	}

	@Test
	void reusedReadAndLobFreeErrorsNeverReachTheRetryCondition() {
		for (boolean character : new boolean[]{false, true}) {
			for (boolean cached : new boolean[]{false, true}) {
				for (boolean stream : new boolean[]{false, true}) {
					AssertionError fatal = new AssertionError("cached LOB read failure");
					FaultLob lob = new FaultLob(character, fatal, null, fatal);
					Fixture fixture = new Fixture(cached, lob);

					fixture.assertFatalWithoutRetry(fatal, stream);

					assertEquals(0, fatal.getSuppressed().length);
					fixture.assertReleased();
				}
			}
		}
	}

	@Test
	void reusedFailuresDoNotPreventOtherOwnedLobsFromBeingReleased() {
		for (boolean character : new boolean[]{false, true}) {
			for (boolean cached : new boolean[]{false, true}) {
				AssertionError fatal = new AssertionError("cached read and cleanup failure");
				FaultLob first = new FaultLob(character, null, null, fatal);
				FaultLob second = new FaultLob(character, fatal, fatal, fatal);
				Fixture fixture = new Fixture(cached, first, second);

				fixture.assertFatalWithoutRetry(fatal, false);

				assertEquals(0, fatal.getSuppressed().length);
				fixture.assertReleased();
			}
		}
	}

	@Test
	void distinctStreamAndLobCleanupFailuresRemainSuppressedOnTheReadFailure() {
		for (boolean character : new boolean[]{false, true}) {
			AssertionError fatal = new AssertionError("read failed");
			IOException streamFailure = new IOException("stream close failed");
			SQLException freeFailure = new SQLException("LOB free failed", "08006");
			Fixture fixture = new Fixture(true, new FaultLob(character, fatal, streamFailure, freeFailure));

			fixture.assertFatalWithoutRetry(fatal, false);

			assertArrayEquals(new Throwable[]{streamFailure, freeFailure}, fatal.getSuppressed());
			fixture.assertReleased();
		}
	}

	@Test
	void cleanupOnlyErrorsPreserveIdentityAndCannotBeRetried() {
		for (boolean character : new boolean[]{false, true}) {
			for (boolean failStreamClose : new boolean[]{false, true}) {
				AssertionError fatal = new AssertionError("cached cleanup failure");
				Fixture fixture = new Fixture(true, new FaultLob(character, null, failStreamClose ? fatal : null, fatal));

				fixture.assertFatalWithoutRetry(fatal, false);

				assertEquals(0, fatal.getSuppressed().length);
				fixture.assertReleased();
			}
		}
	}

	@Test
	void reusedCheckedReadAndCloseFailureRetainsItsCauseAndDistinctFreeFailure() {
		for (boolean character : new boolean[]{false, true}) {
			IOException readFailure = new IOException("cached checked read failure");
			SQLException freeFailure = new SQLException("free failed", "08006");
			Fixture fixture = new Fixture(true, new FaultLob(character, readFailure, readFailure, freeFailure));

			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.read(false));

			assertSame(readFailure, thrown.getCause());
			assertArrayEquals(new Throwable[]{freeFailure}, thrown.getSuppressed());
			assertEquals(0, readFailure.getSuppressed().length);
			fixture.assertReleased();
		}
	}

	@Test
	void detachedConversionScopeAlsoPreservesReusedReadAndFreeErrors() {
		for (boolean character : new boolean[]{false, true}) {
			AssertionError fatal = new AssertionError("detached read and free failure");
			FaultLob lob = new FaultLob(character, fatal, null, fatal);
			Fixture fixture = new Fixture(true, lob);
			DefaultResultSetMapper mapper = (DefaultResultSetMapper) fixture.mapper;
			StatementContext<?> context = StatementContext.with(Statement.of("detached-lob", "VALUES (1)"), fixture.database).build();

			assertSame(fatal, assertThrows(AssertionError.class, () -> mapper.convertResultSetValueToTargetType(
					context, lob.handle, TargetType.of(character ? String.class : byte[].class))));

			lob.assertReleased();
			assertEquals(0, fatal.getSuppressed().length);
		}
	}

	@Test
	void cleanupOnlyRuntimeFailuresKeepTheirExistingMappingContext() {
		for (boolean character : new boolean[]{false, true}) {
			IllegalStateException freeFailure = new IllegalStateException("unchecked LOB release failed");
			Fixture fixture = new Fixture(true, new FaultLob(character, null, null, freeFailure));

			DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.read(false));

			assertSame(freeFailure, thrown.getCause());
			assertTrue(thrown.getMessage().contains("Unable to map JDBC"));
			fixture.assertReleased();
		}
	}

	@Test
	void failedCleanupDoesNotRetainTheRowScopeForTheNextQuery() {
		for (boolean character : new boolean[]{false, true}) {
			AssertionError fatal = new AssertionError("cached cleanup failure");
			FaultLob first = new FaultLob(character, fatal, fatal, fatal);
			Fixture fixture = new Fixture(true, first);
			assertSame(fatal, assertThrows(AssertionError.class, () -> fixture.read(false)));
			FaultLob next = new FaultLob(character, null, null, null);
			fixture.lobs.set(0, next);

			assertDoesNotThrow(() -> fixture.read(false));

			first.assertReleased();
			next.assertReleased();
			assertEquals(2, fixture.resultSetCloses.get());
			assertEquals(2, fixture.statementCloses.get());
			assertEquals(2, fixture.connectionCloses.get());
		}
	}

	private static final class Fixture {
		private final ResultSetMapper mapper;
		private final Database database;
		private final List<FaultLob> lobs;
		private final AtomicInteger resultSetCloses = new AtomicInteger(), statementCloses = new AtomicInteger();
		private final AtomicInteger connectionCloses = new AtomicInteger();

		private Fixture(boolean cached, FaultLob... lobs) {
			this.lobs = new ArrayList<>(List.of(lobs));
			this.mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:lob_failure_cleanup_" + UUID.randomUUID());
			source.setUser("sa");
			source.setPassword("");
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object result = invoke(source, method, args);
				if (!(result instanceof Connection connection)) return result;
				return proxy(Connection.class, (operation, parameters) -> {
					Object value = invoke(connection, operation, parameters);
					if (operation.getName().equals("close")) connectionCloses.incrementAndGet();
					if (!(value instanceof PreparedStatement statement)) return value;
					return proxy(PreparedStatement.class, (statementMethod, statementArgs) -> {
						Object statementResult = invoke(statement, statementMethod, statementArgs);
						if (statementMethod.getName().equals("close")) statementCloses.incrementAndGet();
						if (!(statementResult instanceof ResultSet rows)) return statementResult;
						return proxy(ResultSet.class, (rowMethod, rowArgs) -> {
							if (rowMethod.getName().equals("getObject") && rowArgs[0] instanceof Integer column)
								return this.lobs.get(column - 1).handle;
							Object rowValue = invoke(rows, rowMethod, rowArgs);
							if (rowMethod.getName().equals("close")) resultSetCloses.incrementAndGet();
							return rowValue;
						});
					});
				});
			});
			this.database = Database.withDataSource(wrapped).databaseType(DatabaseType.GENERIC).resultSetMapper(this.mapper).build();
		}

		private void read(boolean stream) {
			boolean character = this.lobs.get(0).character;
			String literal = character ? "CAST('x' AS CLOB)" : "CAST(X'01' AS BLOB)";
			String sql = this.lobs.size() == 1 ? "VALUES (" + literal + ")"
					: "SELECT " + literal + " AS \"first\", " + literal + " AS \"second\" FROM (VALUES (0))";
			Class<?> target = this.lobs.size() == 1 ? (character ? String.class : byte[].class)
					: (character ? TextPair.class : BinaryPair.class);
			if (stream) this.database.query(sql).fetchStream(target, values -> values.findFirst());
			else this.database.query(sql).fetchObject(target);
		}

		private void assertFatalWithoutRetry(AssertionError fatal, boolean stream) {
			AtomicInteger attempts = new AtomicInteger(), conditionCalls = new AtomicInteger();
			RetryPolicy retry = RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
				conditionCalls.incrementAndGet();
				return true;
			});
			assertSame(fatal, assertThrows(AssertionError.class, () -> this.database.transactionWithRetry(retry, () -> {
				attempts.incrementAndGet();
				read(stream);
			})));
			assertEquals(1, attempts.get());
			assertEquals(0, conditionCalls.get());
		}

		private void assertReleased() {
			for (FaultLob lob : this.lobs) lob.assertReleased();
			assertEquals(1, this.resultSetCloses.get());
			assertEquals(1, this.statementCloses.get());
			assertEquals(1, this.connectionCloses.get());
		}
	}

	private static final class FaultLob {
		private final boolean character;
		private final Object handle;
		private final AtomicInteger opens = new AtomicInteger(), closes = new AtomicInteger(), frees = new AtomicInteger();

		private FaultLob(boolean character, Throwable readFailure, Throwable closeFailure, Throwable freeFailure) {
			this.character = character;
			Class<?> lobClass = character ? Clob.class : Blob.class;
			this.handle = proxy(lobClass, (method, args) -> {
				if (method.getName().equals("getBinaryStream")) {
					opens.incrementAndGet();
					return new InputStream() {
						private final InputStream delegate = new ByteArrayInputStream(new byte[]{1});
						@Override public int read() throws IOException { failStream(readFailure); return this.delegate.read(); }
						@Override public int read(byte[] bytes, int offset, int length) throws IOException {
							failStream(readFailure); return this.delegate.read(bytes, offset, length);
						}
						@Override public void close() throws IOException { closes.incrementAndGet(); failStream(closeFailure); }
					};
				}
				if (method.getName().equals("getCharacterStream")) {
					opens.incrementAndGet();
					return new Reader() {
						private final Reader delegate = new StringReader("x");
						@Override public int read(char[] chars, int offset, int length) throws IOException {
							failStream(readFailure); return this.delegate.read(chars, offset, length);
						}
						@Override public void close() throws IOException { closes.incrementAndGet(); failStream(closeFailure); }
					};
				}
				if (method.getName().equals("free")) {
					frees.incrementAndGet();
					if (freeFailure != null) throw freeFailure;
					return null;
				}
				throw new AssertionError("Unexpected LOB call: " + method.getName());
			});
		}

		private void assertReleased() {
			assertEquals(1, this.opens.get());
			assertEquals(1, this.closes.get());
			assertEquals(1, this.frees.get());
		}
	}

	private static void failStream(Throwable failure) throws IOException {
		if (failure instanceof IOException io) throw io;
		if (failure instanceof RuntimeException runtime) throw runtime;
		if (failure instanceof Error error) throw error;
	}

	@FunctionalInterface
	private interface Invocation { Object invoke(Method method, Object[] args) throws Throwable; }

	private static <T> T proxy(Class<T> type, Invocation invocation) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> invocation.invoke(method, args)));
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
