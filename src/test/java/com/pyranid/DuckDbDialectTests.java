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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Unit coverage for DuckDB dialect behavior that does not require the DuckDB driver: the generated-key
 * guard, vector binding, and result-set value unwrapping of non-driver values. Driver-dependent behavior
 * (STRUCT/JSON/BLOB unwrapping, message classification against live errors) is covered by
 * {@code DuckDbIntegrationIT}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
public class DuckDbDialectTests {
	@Test
	public void testDuckDbDatabaseTypeUsesDuckDbDialect() {
		Assertions.assertSame(DuckDbDialect.INSTANCE, DatabaseType.DUCK_DB.dialect());
	}

	@Test
	public void testGeneratedKeysGuardThrowsActionableException() {
		Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
				new Class<?>[]{Connection.class},
				(proxy, method, args) -> {
					throw new UnsupportedOperationException("The generated-keys guard should reject before touching the connection");
				});

		StatementContext<Long> statementContext = StatementContext.<Long>with(
						Statement.of("duckdb-generated-keys-test", "INSERT INTO t (name) VALUES (:name)"), database())
				.resultSetRowType(Long.class)
				.build();

		DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
				DuckDbDialect.INSTANCE.prepareGeneratedKeysStatement(connection, statementContext, new String[0]));

		Assertions.assertTrue(exception.getMessage().contains("DatabaseType.DUCK_DB"),
				"Unexpected message: " + exception.getMessage());
		Assertions.assertTrue(exception.getMessage().contains("RETURNING"),
				"Guard message should point users at DuckDB's INSERT ... RETURNING support");
	}

	@Test
	public void testBindVectorBindsDoubleSqlArray() throws Exception {
		List<String> createArrayOfTypeNames = new ArrayList<>();
		List<Object[]> createArrayOfElements = new ArrayList<>();
		List<Object> boundValues = new ArrayList<>();

		Array sqlArray = (Array) Proxy.newProxyInstance(Array.class.getClassLoader(), new Class<?>[]{Array.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "toString" -> "Array<test>";
					default -> null;
				});

		Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
				new Class<?>[]{Connection.class},
				(proxy, method, args) -> {
					if ("createArrayOf".equals(method.getName())) {
						createArrayOfTypeNames.add((String) args[0]);
						createArrayOfElements.add((Object[]) args[1]);
						return sqlArray;
					}
					return null;
				});

		PreparedStatement preparedStatement = (PreparedStatement) Proxy.newProxyInstance(
				PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> {
					switch (method.getName()) {
						case "getConnection" -> {
							return connection;
						}
						case "setObject" -> {
							boundValues.add(args[1]);
							return null;
						}
						default -> {
							return null;
						}
					}
				});

		DuckDbDialect.INSTANCE.bindVector(preparedStatement, vectorStatementContext(), 1, new double[]{0.25, 0.5, 0.75}, null);

		Assertions.assertEquals(List.of("DOUBLE"), createArrayOfTypeNames);
		Assertions.assertEquals(1, createArrayOfElements.size());
		Assertions.assertArrayEquals(new Object[]{0.25, 0.5, 0.75}, createArrayOfElements.get(0));
		Assertions.assertEquals(List.of(sqlArray), boundValues);
	}

	@Test
	public void testBindVectorBindsNullAsArrayTypedNull() throws Exception {
		List<int[]> setNullCalls = new ArrayList<>();

		PreparedStatement preparedStatement = (PreparedStatement) Proxy.newProxyInstance(
				PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> {
					if ("setNull".equals(method.getName()) && args.length == 2) {
						setNullCalls.add(new int[]{(Integer) args[0], (Integer) args[1]});
						return null;
					}
					return null;
				});

		DuckDbDialect.INSTANCE.bindVector(preparedStatement, vectorStatementContext(), 3, null, null);

		Assertions.assertEquals(1, setNullCalls.size());
		Assertions.assertArrayEquals(new int[]{3, Types.ARRAY}, setNullCalls.get(0));
	}

	@Test
	public void testBindVectorDefersArrayFreeToStatementCleanup() throws Exception {
		List<String> sqlArrayCalls = new ArrayList<>();
		List<Object> boundValues = new ArrayList<>();
		Array sqlArray = recordingSqlArray(sqlArrayCalls, null);
		PreparedStatement preparedStatement = vectorPreparedStatement(sqlArray, boundValues, null);
		StatementContext<Long> statementContext = vectorStatementContext();

		DuckDbDialect.INSTANCE.bindVector(preparedStatement, statementContext, 1, new double[]{0.25, 0.5}, null);

		Assertions.assertEquals(List.of(sqlArray), boundValues);
		Assertions.assertEquals(List.of(), sqlArrayCalls,
				"The driver may read the array at execute time, so binding must not free it");
		Assertions.assertEquals(1, statementContext.getCleanupOperations().size(),
				"A bound array should be handed to the statement's deferred cleanup");

		Assertions.assertNull(Database.closeStatementContextResources(statementContext, null));
		Assertions.assertEquals(List.of("free"), sqlArrayCalls,
				"Statement cleanup should free the array exactly once");
		Assertions.assertTrue(statementContext.getCleanupOperations().isEmpty());
	}

	@Test
	public void testBindVectorFreesArrayWhenBindFails() {
		List<String> sqlArrayCalls = new ArrayList<>();
		SQLException bindFailure = new SQLException("bind failed");
		Array sqlArray = recordingSqlArray(sqlArrayCalls, null);
		PreparedStatement preparedStatement = vectorPreparedStatement(sqlArray, new ArrayList<>(), bindFailure);
		StatementContext<Long> statementContext = vectorStatementContext();

		SQLException exception = Assertions.assertThrows(SQLException.class, () ->
				DuckDbDialect.INSTANCE.bindVector(preparedStatement, statementContext, 1, new double[]{0.25}, null));

		Assertions.assertSame(bindFailure, exception);
		Assertions.assertEquals(List.of("free"), sqlArrayCalls,
				"An array that never reached the statement must be freed before the failure propagates");
		Assertions.assertEquals(0, exception.getSuppressed().length);
		Assertions.assertTrue(statementContext.getCleanupOperations().isEmpty(),
				"A failed bind must not leave deferred cleanup behind");
	}

	@Test
	public void testBindVectorSuppressesArrayFreeFailureWhenBindFails() {
		List<String> sqlArrayCalls = new ArrayList<>();
		SQLException bindFailure = new SQLException("bind failed");
		Array sqlArray = recordingSqlArray(sqlArrayCalls, new SQLException("free failed"));
		PreparedStatement preparedStatement = vectorPreparedStatement(sqlArray, new ArrayList<>(), bindFailure);
		StatementContext<Long> statementContext = vectorStatementContext();

		SQLException exception = Assertions.assertThrows(SQLException.class, () ->
				DuckDbDialect.INSTANCE.bindVector(preparedStatement, statementContext, 1, new double[]{0.25}, null));

		Assertions.assertSame(bindFailure, exception, "A failing free() must not replace the bind failure");
		Assertions.assertEquals(List.of("free"), sqlArrayCalls);
		Assertions.assertEquals(1, exception.getSuppressed().length);
		Assertions.assertEquals("free failed", exception.getSuppressed()[0].getMessage());
		Assertions.assertTrue(statementContext.getCleanupOperations().isEmpty());
	}

	@Test
	public void testBindVectorDoesNotSelfSuppressWhenTheDriverReusesOneException() {
		// Drivers reuse exception instances; addSuppressed(self) throws IllegalArgumentException
		List<String> sqlArrayCalls = new ArrayList<>();
		SQLException sharedFailure = new SQLException("bind and free share one instance");
		Array sqlArray = recordingSqlArray(sqlArrayCalls, sharedFailure);
		PreparedStatement preparedStatement = vectorPreparedStatement(sqlArray, new ArrayList<>(), sharedFailure);
		StatementContext<Long> statementContext = vectorStatementContext();

		SQLException exception = Assertions.assertThrows(SQLException.class, () ->
				DuckDbDialect.INSTANCE.bindVector(preparedStatement, statementContext, 1, new double[]{0.25}, null));

		Assertions.assertSame(sharedFailure, exception);
		Assertions.assertEquals(List.of("free"), sqlArrayCalls);
		Assertions.assertEquals(0, exception.getSuppressed().length);
		Assertions.assertTrue(statementContext.getCleanupOperations().isEmpty());
	}

	@Test
	public void testBindVectorBindsNullWhenTheDriverCreatesNoArray() throws Exception {
		List<Integer> setObjectParameterIndexes = new ArrayList<>();
		Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
				new Class<?>[]{Connection.class}, (proxy, method, args) -> null);
		PreparedStatement preparedStatement = (PreparedStatement) Proxy.newProxyInstance(
				PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> {
					if ("getConnection".equals(method.getName()))
						return connection;

					if ("setObject".equals(method.getName())) {
						Assertions.assertNull(args[1]);
						setObjectParameterIndexes.add((Integer) args[0]);
					}

					return null;
				});
		StatementContext<Long> statementContext = vectorStatementContext();

		DuckDbDialect.INSTANCE.bindVector(preparedStatement, statementContext, 2, new double[]{0.25}, null);

		Assertions.assertEquals(List.of(2), setObjectParameterIndexes,
				"A driver that creates no array must still bind, as it did before the array was owned");
		Assertions.assertTrue(statementContext.getCleanupOperations().isEmpty(),
				"There is no array to release, so nothing should be deferred");
	}

	@NonNull
	private StatementContext<Long> vectorStatementContext() {
		return StatementContext.<Long>with(
						Statement.of("duckdb-vector-bind-test", "INSERT INTO t (embedding) VALUES (:embedding)"), database())
				.resultSetRowType(Long.class)
				.build();
	}

	@NonNull
	private Array recordingSqlArray(@NonNull List<String> sqlArrayCalls,
																	@Nullable SQLException freeFailure) {
		requireNonNull(sqlArrayCalls);

		return (Array) Proxy.newProxyInstance(Array.class.getClassLoader(), new Class<?>[]{Array.class},
				(proxy, method, args) -> {
					if ("free".equals(method.getName())) {
						sqlArrayCalls.add("free");

						if (freeFailure != null)
							throw freeFailure;

						return null;
					}

					return "toString".equals(method.getName()) ? "Array<test>" : null;
				});
	}

	@NonNull
	private PreparedStatement vectorPreparedStatement(@NonNull Array sqlArray,
																										@NonNull List<Object> boundValues,
																										@Nullable SQLException bindFailure) {
		requireNonNull(sqlArray);
		requireNonNull(boundValues);

		Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
				new Class<?>[]{Connection.class},
				(proxy, method, args) -> "createArrayOf".equals(method.getName()) ? sqlArray : null);

		return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
				new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> {
					switch (method.getName()) {
						case "getConnection" -> {
							return connection;
						}
						case "setObject" -> {
							if (bindFailure != null)
								throw bindFailure;

							boundValues.add(args[1]);
							return null;
						}
						default -> {
							return null;
						}
					}
				});
	}

	@Test
	public void testUnwrapResultSetValuePassesNonDuckDbValuesThrough() {
		Assertions.assertEquals("plain", DuckDbDialect.INSTANCE.unwrapResultSetValue("plain"));
		Assertions.assertEquals(42, DuckDbDialect.INSTANCE.unwrapResultSetValue(42));

		Map<String, Object> map = Map.of("key", "value");
		Assertions.assertSame(map, DuckDbDialect.INSTANCE.unwrapResultSetValue(map));

		byte[] bytes = new byte[]{1, 2, 3};
		Assertions.assertSame(bytes, DuckDbDialect.INSTANCE.unwrapResultSetValue(bytes));

		// Object arrays with no driver-specific elements come back as the SAME instance (no copying)
		Object[] plainElements = new Object[]{"a", 1, null};
		Assertions.assertSame(plainElements, DuckDbDialect.INSTANCE.unwrapResultSetValue(plainElements));
	}

	public record RecordedBind(String method, Object[] args) {}

	@Test
	public void testTemporalParametersBindWithoutTargetSqlTypeOnDuckDb() throws Exception {
		// DuckDB's driver rejects setObject(idx, value, targetSqlType) with "Unknown target type";
		// the binder must route java.time values through untyped setObject there, while other
		// databases keep their typed binds. OffsetTime binds as an ISO-8601 string because the driver
		// defers its "Unsupported parameter type" failure to execute time.
		PreparedStatementBinder binder = PreparedStatementBinder.withDefaultConfiguration();

		List<RecordedBind> duckDbBinds = new ArrayList<>();
		binder.bindParameter(temporalStatementContext(DatabaseType.DUCK_DB), recordingPreparedStatement(duckDbBinds), 1,
				java.time.LocalDate.of(2026, 8, 27));
		binder.bindParameter(temporalStatementContext(DatabaseType.DUCK_DB), recordingPreparedStatement(duckDbBinds), 2,
				java.time.LocalTime.of(10, 30, 15));
		binder.bindParameter(temporalStatementContext(DatabaseType.DUCK_DB), recordingPreparedStatement(duckDbBinds), 3,
				java.time.LocalDateTime.of(2026, 8, 27, 10, 30, 15));

		Assertions.assertEquals(3, duckDbBinds.size());

		for (RecordedBind bind : duckDbBinds) {
			Assertions.assertEquals("setObject", bind.method());
			Assertions.assertEquals(2, bind.args().length,
					"DuckDB temporal binds should use untyped setObject(index, value); saw args " + Arrays.toString(bind.args()));
		}

		List<RecordedBind> offsetTimeBinds = new ArrayList<>();
		binder.bindParameter(temporalStatementContext(DatabaseType.DUCK_DB), recordingPreparedStatement(offsetTimeBinds), 1,
				java.time.OffsetTime.of(10, 30, 15, 0, java.time.ZoneOffset.ofHours(-4)));

		Assertions.assertEquals(1, offsetTimeBinds.size());
		Assertions.assertEquals("setString", offsetTimeBinds.get(0).method());
		Assertions.assertEquals("10:30:15-04:00", offsetTimeBinds.get(0).args()[1]);

		List<RecordedBind> genericBinds = new ArrayList<>();
		binder.bindParameter(temporalStatementContext(DatabaseType.GENERIC), recordingPreparedStatement(genericBinds), 1,
				java.time.LocalDate.of(2026, 8, 27));

		Assertions.assertEquals(1, genericBinds.size());
		Assertions.assertEquals("setObject", genericBinds.get(0).method());
		Assertions.assertEquals(3, genericBinds.get(0).args().length,
				"Non-DuckDB temporal binds must keep the explicit target SQL type");
	}

	@NonNull
	private StatementContext<Long> temporalStatementContext(@NonNull DatabaseType databaseType) {
		DataSource dataSource = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
				new Class<?>[]{DataSource.class},
				(proxy, method, args) -> {
					throw new UnsupportedOperationException("This test should not touch the DataSource");
				});

		Database db = Database.withDataSource(dataSource)
				.databaseType(databaseType)
				.build();

		return StatementContext.<Long>with(Statement.of("duckdb-temporal-bind-test", "SELECT 1"), db)
				.resultSetRowType(Long.class)
				.build();
	}

	@NonNull
	private PreparedStatement recordingPreparedStatement(@NonNull List<RecordedBind> sink) {
		return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
				new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> {
					if (method.getName().startsWith("set"))
						sink.add(new RecordedBind(method.getName(), args == null ? new Object[0] : args));
					return null;
				});
	}

	@Test
	public void testVectorSupportIsEnabled() {
		Assertions.assertTrue(DuckDbDialect.INSTANCE.supportsVector());
		Assertions.assertTrue(DuckDbDialect.INSTANCE.supportsSqlArray(),
				"DuckDB's driver implements createArrayOf, so SQL ARRAY binding should remain enabled");
	}

	@NonNull
	private Database database() {
		DataSource dataSource = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
				new Class<?>[]{DataSource.class},
				(proxy, method, args) -> {
					throw new UnsupportedOperationException("This test should not touch the DataSource");
				});

		return Database.withDataSource(dataSource)
				.databaseType(DatabaseType.DUCK_DB)
				.build();
	}
}
