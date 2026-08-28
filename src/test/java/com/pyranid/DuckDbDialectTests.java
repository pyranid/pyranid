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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
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

		DuckDbDialect.INSTANCE.bindVector(preparedStatement, 1, new double[]{0.25, 0.5, 0.75}, null);

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

		DuckDbDialect.INSTANCE.bindVector(preparedStatement, 3, null, null);

		Assertions.assertEquals(1, setNullCalls.size());
		Assertions.assertArrayEquals(new int[]{3, Types.ARRAY}, setNullCalls.get(0));
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
