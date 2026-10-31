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

import com.pyranid.testdriver.util.PGobject;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MappingDiagnosticFailureTests {
	public record BinaryValue(byte[] value) {}
	public record TextValue(String value) {}
	public record ConstructorRecord(String value) {
		public ConstructorRecord { throwConstructorFailure(); }
	}
	public static class ConstructorBean {
		private String value;
		public ConstructorBean() { throwConstructorFailure(); }
		public String getValue() { return this.value; }
		public void setValue(String value) { this.value = value; }
	}
	private static final ThreadLocal<Throwable> CONSTRUCTOR_FAILURE = new ThreadLocal<>();
	private static final List<String> ACCESSORS = List.of("readable", "state", "code", "message", "cause", "cycle");

	@AfterEach
	void clearInterruptFlag() {
		Thread.interrupted();
		CONSTRUCTOR_FAILURE.remove();
	}

	@Test
	void cancelledLobAcquisitionRetainsFailureAndDoesNotRetry() {
		for (boolean character : new boolean[]{false, true}) {
			for (boolean cached : new boolean[]{false, true}) {
				for (boolean streamed : new boolean[]{false, true}) {
					for (String accessor : ACCESSORS) {
						SQLException original = cancelled(accessor);
						Fixture fixture = new Fixture(Mode.LOB, character, cached, original);

						fixture.assertCancelled(original, () -> fixture.read(streamed));

						assertEquals(1, fixture.opens.get());
						assertEquals(0, fixture.streamCloses.get(), "A rejected stream acquisition owns no stream");
						assertEquals(1, fixture.frees.get());
						fixture.assertJdbcReleased();
					}
				}
			}
		}
	}

	@Test
	void cancelledCustomMapperRetainsMessageAndMetadataFailureEvidence() {
		for (boolean cached : new boolean[]{false, true}) {
			for (boolean streamed : new boolean[]{false, true}) {
				for (String accessor : ACCESSORS) {
					SQLException original = cancelled(accessor);
					Fixture fixture = new Fixture(Mode.CUSTOM, true, cached, original);

					DatabaseException thrown = fixture.assertCancelled(original, () -> fixture.read(streamed));

					assertSame(original, thrown.getCause());
					if (accessor.equals("message"))
						assertTrue(thrown.getMessage().contains("Unable to map custom JDBC column"));
					fixture.assertJdbcReleased();
				}
			}
		}
	}

	@Test
	void cleanupOnlyRuntimeWrappersRetainNestedDriverCancellation() {
		for (boolean character : new boolean[]{false, true}) {
			SQLException original = cancelled("state");
			RuntimeException releaseFailure = new IllegalStateException("release failed", original);
			Fixture fixture = new Fixture(Mode.FREE, character, true, releaseFailure);

			DatabaseException thrown = fixture.assertCancelled(original, () -> fixture.read(false));

			assertSame(releaseFailure, thrown.getCause());
			assertTrue(thrown.getMessage().contains("Unable to map JDBC"));
			assertEquals(1, fixture.streamCloses.get());
			assertEquals(1, fixture.frees.get());
			fixture.assertJdbcReleased();
		}
	}

	@Test
	void detachedMappingCleanupRetainsItsDriverFailure() {
		for (boolean character : new boolean[]{false, true}) {
			for (String accessor : ACCESSORS) {
				SQLException original = cancelled(accessor);
				Fixture fixture = new Fixture(Mode.FREE, character, true, original);
				DefaultResultSetMapper mapper = (DefaultResultSetMapper) fixture.mapper;
				StatementContext<?> context = StatementContext.with(Statement.of("detached-diagnostics", "VALUES (1)"), fixture.database).build();

				fixture.assertCancelled(original, () -> mapper.convertResultSetValueToTargetType(context, fixture.lob,
						TargetType.of(character ? String.class : byte[].class)));

				assertEquals(1, fixture.streamCloses.get());
				assertEquals(1, fixture.frees.get());
				assertEquals(0, fixture.connectionCloses.get());
			}
		}
	}

	@Test
	void reflectedPostgresValueFailureRetainsCancellationWithoutRetry() {
		for (String accessor : ACCESSORS) {
			SQLException original = cancelled(accessor);
			Fixture fixture = new Fixture(Mode.POSTGRES, true, true, original);

			fixture.assertCancelled(original, () -> fixture.read(false));

			fixture.assertJdbcReleased();
		}
	}

	@Test
	void reflectedPostgresFatalErrorsKeepTheirIdentity() {
		AssertionError fatal = new AssertionError("fatal PGobject read");
		Fixture fixture = new Fixture(Mode.POSTGRES, true, true, fatal);
		AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();

		assertSame(fatal, assertThrows(AssertionError.class, () -> fixture.database.transactionWithRetry(
				policy(conditions), () -> { attempts.incrementAndGet(); fixture.read(false); })));

		assertEquals(1, attempts.get());
		assertEquals(0, conditions.get());
		fixture.assertJdbcReleased();
	}

	@Test
	void defaultBeanAndRecordConstructionRetainsNestedDriverCancellation() {
		for (Mode mode : List.of(Mode.BEAN, Mode.RECORD)) {
			for (boolean cached : new boolean[]{false, true}) {
				for (String accessor : ACCESSORS) {
					SQLException original = cancelled(accessor);
					Fixture fixture = new Fixture(mode, true, cached, original);
					CONSTRUCTOR_FAILURE.set(original);

					fixture.assertCancelled(original, () -> fixture.read(false));

					fixture.assertJdbcReleased();
					assertNull(CONSTRUCTOR_FAILURE.get());
				}
			}
		}
	}

	@Test
	void readableCustomSqlFailuresKeepClassificationAndMessage() {
		SQLException original = new SQLException("readable driver failure", "40001");
		Fixture fixture = new Fixture(Mode.CUSTOM, true, true, original);

		DatabaseException thrown = assertThrows(DatabaseException.class, () -> fixture.database.transaction(() -> fixture.read(false)));

		assertSame(original, thrown.getCause());
		assertEquals("readable driver failure", thrown.getMessage());
		assertTrue(thrown.isSerializationFailure());
		assertFalse(Thread.currentThread().isInterrupted());
		fixture.assertJdbcReleased();
	}

	private static SQLException cancelled(String accessor) {
		InterruptedException interruption = new InterruptedException("cancelled");
		SQLException original = new SQLException("driver mapping cancelled", "57014", interruption) {
			@Override public String getSQLState() {
				if (accessor.equals("state")) throw new IllegalStateException("unreadable SQLState");
				return super.getSQLState();
			}
			@Override public int getErrorCode() {
				if (accessor.equals("code")) throw new IllegalStateException("unreadable error code");
				return super.getErrorCode();
			}
			@Override public String getMessage() {
				if (accessor.equals("message")) throw new IllegalStateException("unreadable message");
				return super.getMessage();
			}
			@Override public Throwable getCause() {
				if (accessor.equals("cause")) throw new IllegalStateException("unreadable cause");
				if (accessor.equals("cycle")) return this;
				return super.getCause();
			}
		};
		if (accessor.equals("cause") || accessor.equals("cycle")) original.addSuppressed(interruption);
		return original;
	}

	private static RetryPolicy policy(AtomicInteger conditions) {
		return RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			conditions.incrementAndGet(); return true;
		});
	}

	private enum Mode { LOB, CUSTOM, FREE, POSTGRES, BEAN, RECORD }

	private static final class Fixture {
		private final Database database;
		private final ResultSetMapper mapper;
		private final Mode mode;
		private final boolean character;
		private final Object lob;
		private final AtomicInteger opens = new AtomicInteger(), streamCloses = new AtomicInteger(), frees = new AtomicInteger();
		private final AtomicInteger resultSetCloses = new AtomicInteger(), statementCloses = new AtomicInteger(), connectionCloses = new AtomicInteger();

		private Fixture(Mode mode, boolean character, boolean cached, Throwable failure) {
			this.mode = mode;
			this.character = character;
			AtomicBoolean fault = new AtomicBoolean(true);
			Class<?> lobType = character ? Clob.class : Blob.class;
			this.lob = proxy(lobType, (method, args) -> {
				if (method.getName().equals("getBinaryStream") || method.getName().equals("getCharacterStream")) {
					opens.incrementAndGet();
					if (mode == Mode.LOB && fault.compareAndSet(true, false)) throw failure;
					return character ? new StringReader("x") { @Override public void close() { streamCloses.incrementAndGet(); } }
							: new ByteArrayInputStream(new byte[]{1}) { @Override public void close() { streamCloses.incrementAndGet(); } };
				}
				if (method.getName().equals("free")) {
					frees.incrementAndGet();
					if (mode == Mode.FREE && fault.compareAndSet(true, false)) throw failure;
					return null;
				}
				throw new AssertionError("Unexpected LOB method: " + method.getName());
			});
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:mapping_diagnostic_failure_" + UUID.randomUUID());
			source.setUser("sa"); source.setPassword("");
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
							if (rowMethod.getName().equals("getObject")) {
								if (mode == Mode.LOB || mode == Mode.FREE) return this.lob;
								if (mode == Mode.POSTGRES) return new PGobject(failure);
							}
							Object rowValue = invoke(rows, rowMethod, rowArgs);
							if (rowMethod.getName().equals("close")) resultSetCloses.incrementAndGet();
							return rowValue;
						});
					});
				});
			});
			ResultSetMapper.Builder builder = ResultSetMapper.withPlanCachingEnabled(cached);
			if (mode == Mode.CUSTOM) builder.customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.getRawClass() == String.class; }
				@Override public MappingResult map(StatementContext<?> context, ResultSet rows, Object value, TargetType target,
						Integer index, String label, InstanceProvider provider) throws SQLException {
					if (fault.compareAndSet(true, false)) throw (SQLException) failure;
					return MappingResult.fallback();
				}
			}));
			this.mapper = builder.build();
			this.database = Database.withDataSource(wrapped).databaseType(mode == Mode.POSTGRES ? DatabaseType.POSTGRESQL : DatabaseType.GENERIC)
					.resultSetMapper(this.mapper).build();
		}

		private void read(boolean streamed) {
			String literal = this.character ? "CAST('x' AS CLOB)" : "CAST(X'01' AS BLOB)";
			String sql = this.mode == Mode.CUSTOM || this.mode == Mode.POSTGRES ? "VALUES ('x')"
					: this.mode == Mode.BEAN || this.mode == Mode.RECORD ? "SELECT 'x' AS \"value\" FROM (VALUES (0))"
					: "SELECT " + literal + " AS \"value\" FROM (VALUES (0))";
			Class<?> target = this.mode == Mode.POSTGRES ? Map.class : this.mode == Mode.CUSTOM ? String.class
					: this.mode == Mode.BEAN ? ConstructorBean.class : this.mode == Mode.RECORD ? ConstructorRecord.class
					: this.mode == Mode.FREE ? (this.character ? TextValue.class : BinaryValue.class)
					: this.character ? String.class : byte[].class;
			if (streamed) this.database.query(sql).fetchStream(target, stream -> stream.findFirst());
			else this.database.query(sql).fetchObject(target);
		}

		private DatabaseException assertCancelled(SQLException original, Runnable operation) {
			AtomicInteger attempts = new AtomicInteger(), conditions = new AtomicInteger();
			DatabaseException thrown = assertThrows(DatabaseException.class, () -> this.database.transactionWithRetry(policy(conditions), () -> {
				attempts.incrementAndGet(); operation.run();
			}));
			boolean interruptedAtExit = Thread.interrupted();
			assertTrue(interruptedAtExit);
			assertEquals(1, ThrowableGraph.inspect(thrown, current -> current == original ? 1 : 0, 0));
			assertEquals(1, attempts.get());
			assertEquals(0, conditions.get());
			return thrown;
		}

		private void assertJdbcReleased() {
			assertEquals(1, this.resultSetCloses.get());
			assertEquals(1, this.statementCloses.get());
			assertEquals(1, this.connectionCloses.get());
			assertTrue(this.database.currentTransaction().isEmpty());
		}
	}

	private static void throwConstructorFailure() {
		Throwable failure = CONSTRUCTOR_FAILURE.get();
		CONSTRUCTOR_FAILURE.remove();
		if (failure != null) MappingDiagnosticFailureTests.<RuntimeException>throwUnchecked(failure);
	}

	@SuppressWarnings("unchecked")
	private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T { throw (T) failure; }

	@FunctionalInterface
	private interface Invocation { Object apply(Method method, Object[] args) throws Throwable; }

	private static <T> T proxy(Class<T> type, Invocation invocation) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> invocation.apply(method, args)));
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
