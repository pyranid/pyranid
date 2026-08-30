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

import com.pyranid.testdriver.jdbc.ForkConnection;
import com.pyranid.testdriver.util.PGobject;
import com.pyranid.testdriver.util.PSQLException;
import com.pyranid.testdriver.util.ServerErrorMessage;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Verifies that PostgreSQL-specific behavior resolves the driver's own {@code PGobject} and
 * {@code PSQLException} types rather than pgjdbc's by name, so repackaged forks of pgjdbc work.
 * YugabyteDB's "smart driver" is the motivating case: it ships {@code com.yugabyte.util.PGobject}, and
 * hard-coding {@code org.postgresql.util.PGobject} failed with {@code NoClassDefFoundError} when pgjdbc was
 * absent, or bound an unrecognized object when both drivers were present.
 * <p>
 * The fixtures under {@code com.pyranid.testdriver.util} stand in for such a fork.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
public class PostgresDriverForkTests {
	@Test
	public void testPgObjectIsRecognizedByPackageShapeNotByDriverName() {
		PGobject forkObject = new PGobject();
		forkObject.setType("jsonb");
		forkObject.setValue("{\"a\":1}");

		Assertions.assertTrue(PostgresDriverSupport.isPgObject(forkObject),
				"A fork's PGobject should be recognized by class name and ...util package shape");
		Assertions.assertEquals("{\"a\":1}", PostgresDriverSupport.pgObjectValue(forkObject));

		// PostgresDialect must unwrap it to its text form, exactly as it does for pgjdbc's PGobject
		Assertions.assertEquals("{\"a\":1}", PostgresDialect.INSTANCE.unwrapResultSetValue(forkObject));
	}

	@Test
	public void testUnrelatedValuesAreNotMistakenForPgObjects() {
		Assertions.assertFalse(PostgresDriverSupport.isPgObject("plain"));
		Assertions.assertFalse(PostgresDriverSupport.isPgObject(42));
		Assertions.assertEquals("plain", PostgresDialect.INSTANCE.unwrapResultSetValue("plain"));

		// A class named PGobject outside a ...util package must not match
		Assertions.assertFalse(PostgresDriverSupport.isPgObject(new PGobject() {}),
				"A subclass declared outside a ...util package should not be treated as a driver PGobject");
	}

	@Test
	public void testJsonBindsUsingTheConnectionsOwnPgObjectClass() throws Exception {
		List<Object> boundValues = new ArrayList<>();
		PreparedStatement preparedStatement = forkPreparedStatement(boundValues);

		PostgresDialect.INSTANCE.bindJson(preparedStatement, 1, "{\"kind\":\"fork\"}",
				JsonParameter.BindingPreference.BINARY);

		Assertions.assertEquals(1, boundValues.size());
		Object bound = boundValues.get(0);
		Assertions.assertInstanceOf(PGobject.class, bound,
				"Binding should construct the PGobject belonging to the connection's own driver, not pgjdbc's");
		Assertions.assertEquals("jsonb", ((PGobject) bound).getType());
		Assertions.assertEquals("{\"kind\":\"fork\"}", ((PGobject) bound).getValue());
	}

	@Test
	public void testVectorBindsUsingTheConnectionsOwnPgObjectClass() throws Exception {
		List<Object> boundValues = new ArrayList<>();
		PreparedStatement preparedStatement = forkPreparedStatement(boundValues);

		PostgresDialect.INSTANCE.bindVector(preparedStatement, 1, new double[]{0.5, 1.5}, null);

		Assertions.assertEquals(1, boundValues.size());
		Object bound = boundValues.get(0);
		Assertions.assertInstanceOf(PGobject.class, bound);
		Assertions.assertEquals("vector", ((PGobject) bound).getType());
		Assertions.assertEquals("[0.5, 1.5]", ((PGobject) bound).getValue());
	}

	@Test
	public void testBindingUnwrapsPooledConnectionsToReachTheRealDriver() throws Exception {
		// A connection pool hands out a wrapper whose own package says nothing about the driver beneath it -
		// HikariCP's HikariProxyConnection, DBCP2's DelegatingConnection, Spring's proxies, p6spy and the
		// OpenTelemetry JDBC wrapper all behave this way, and PreparedStatement::getConnection returns the
		// wrapper rather than the physical connection. Resolution must unwrap rather than read the package.
		List<Object> boundValues = new ArrayList<>();
		// The physical connection is a real pgjdbc PGConnection; the pool fronts it with its own wrapper
		Connection physical = (Connection) Proxy.newProxyInstance(
				PostgresDriverForkTests.class.getClassLoader(),
				new Class<?>[]{Connection.class, org.postgresql.PGConnection.class},
				(proxy, method, args) -> null);
		Connection pooled = pooledWrapperAround(physical);
		PreparedStatement preparedStatement = preparedStatementOn(pooled, boundValues);

		PostgresDialect.INSTANCE.bindJson(preparedStatement, 1, "{\"kind\":\"pooled\"}",
				JsonParameter.BindingPreference.BINARY);

		Assertions.assertEquals(1, boundValues.size());
		Assertions.assertEquals("org.postgresql.util.PGobject", boundValues.get(0).getClass().getName(),
				"A pooled connection must unwrap to reach the underlying driver's PGobject");
		Assertions.assertEquals("{\"kind\":\"pooled\"}",
				PostgresDriverSupport.pgObjectValue(boundValues.get(0)));
	}

	@Test
	public void testBindingFallsBackWhenAWrapperRefusesToUnwrap() throws Exception {
		// Some wrappers refuse to unwrap at all. Rather than fail a bind that used to work, fall back to a
		// known driver on the classpath - which is what Pyranid did unconditionally before it supported forks.
		List<Object> boundValues = new ArrayList<>();
		Connection opaque = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
				new Class<?>[]{Connection.class},
				(proxy, method, args) -> "isWrapperFor".equals(method.getName()) ? Boolean.FALSE : null);
		PreparedStatement preparedStatement = preparedStatementOn(opaque, boundValues);

		PostgresDialect.INSTANCE.bindJson(preparedStatement, 1, "{}", JsonParameter.BindingPreference.BINARY);

		Assertions.assertEquals(1, boundValues.size());
		Assertions.assertNotNull(boundValues.get(0));
		Assertions.assertEquals("PGobject", boundValues.get(0).getClass().getSimpleName(),
				"Expected a fallback to a known driver's PGobject, got " + boundValues.get(0).getClass().getName());
	}

	/**
	 * A wrapper in the shape a connection pool hands out: its own class reveals nothing about the driver, but
	 * it unwraps faithfully.
	 */
	@NonNull
	private Connection pooledWrapperAround(@NonNull Connection delegate) {
		requireNonNull(delegate);

		return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
				new Class<?>[]{Connection.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(delegate);
					case "unwrap" -> ((Class<?>) args[0]).cast(delegate);
					default -> null;
				});
	}

	@NonNull
	private PreparedStatement preparedStatementOn(@NonNull Connection connection,
																								@NonNull List<Object> boundValues) {
		requireNonNull(connection);
		requireNonNull(boundValues);

		return (PreparedStatement) Proxy.newProxyInstance(
				PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> {
					if ("getConnection".equals(method.getName()))
						return connection;

					if ("setObject".equals(method.getName()) && args.length == 2) {
						boundValues.add(args[1]);
						return null;
					}

					return null;
				});
	}

	@Test
	public void testExceptionMetadataIsReadFromAForksPsqlException() {
		PSQLException forkException = new PSQLException(
				"duplicate key value violates unique constraint", "23505", new ServerErrorMessage());

		Assertions.assertTrue(PostgresDriverSupport.isPsqlException(forkException),
				"A fork's PSQLException should be recognized by class name and ...util package shape");

		DatabaseExceptionMetadata metadata = PostgresDialect.INSTANCE.databaseExceptionMetadata(forkException);

		Assertions.assertEquals("23505", metadata.sqlState);
		Assertions.assertEquals("employee_email_key", metadata.constraint);
		Assertions.assertEquals("employee", metadata.table);
		Assertions.assertEquals("email", metadata.column);
		Assertions.assertEquals("ERROR", metadata.severity);
		Assertions.assertEquals(434, metadata.line);

		// The exception-cause dialect lookup must route a fork's exception to PostgresDialect too
		Assertions.assertSame(PostgresDialect.INSTANCE, DatabaseDialect.forExceptionCause(forkException));

		// And classification must still work end to end
		DatabaseException databaseException = new DatabaseException(
				forkException.getMessage(), forkException, PostgresDialect.INSTANCE);
		Assertions.assertTrue(databaseException.isUniqueConstraintViolation());
	}

	@Test
	public void testPlainSqlExceptionIsNotTreatedAsADriverException() {
		SQLException plain = new SQLException("boom", "23505");

		Assertions.assertFalse(PostgresDriverSupport.isPsqlException(plain));
		Assertions.assertSame(GenericDialect.INSTANCE, DatabaseDialect.forExceptionCause(plain));

		// Metadata still falls back to the SQLState carried by the exception itself
		Assertions.assertEquals("23505", PostgresDialect.INSTANCE.databaseExceptionMetadata(plain).sqlState);
	}

	/**
	 * A {@link java.sql.PreparedStatement} whose connection belongs to the fork fixture's package tree, so
	 * {@code PGobject} resolution finds {@code com.pyranid.testdriver.util.PGobject}.
	 */
	@NonNull
	private PreparedStatement forkPreparedStatement(@NonNull List<Object> boundValues) {
		Connection connection = new ForkConnection();

		return (PreparedStatement) Proxy.newProxyInstance(
				PreparedStatement.class.getClassLoader(),
				new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> {
					if ("getConnection".equals(method.getName()))
						return connection;

					if ("setObject".equals(method.getName()) && args.length == 2) {
						boundValues.add(args[1]);
						return null;
					}

					return null;
				});
	}
}
