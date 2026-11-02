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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseConfigurationTests {
	@Test
	void constructionAndTypeAccessNeverAcquireConnections() {
		AtomicInteger acquisitions = new AtomicInteger();
		DataSource source = proxy(DataSource.class, (method, args) -> {
			if (method.getName().equals("getConnection")) acquisitions.incrementAndGet();
			throw new AssertionError("Construction and configuration access must not use the data source");
		});

		for (DatabaseType type : DatabaseType.values()) {
			Database database = Database.with(source, type).build();
			assertSame(type, database.getDatabaseType());
			assertSame(type.dialect(), database.getDatabaseDialect());
			assertSame(source, database.getDataSource());
		}
		assertEquals(0, acquisitions.get());
	}

	@Test
	void bothRequiredArgumentsRejectNullBeforeBuilding() {
		DataSource source = proxy(DataSource.class, (method, args) -> {
			throw new AssertionError("Invalid configuration must not access the data source");
		});
		assertThrows(NullPointerException.class, () -> Database.with(null, DatabaseType.GENERIC));
		assertThrows(NullPointerException.class, () -> Database.with(source, null));
	}

	@Test
	void statementDiagnosticsAlwaysExposeTheConfiguredTypeWithoutConnecting() {
		DataSource source = proxy(DataSource.class, (method, args) -> {
			throw new AssertionError("Diagnostics must not access the data source");
		});
		Database database = Database.with(source, DatabaseType.POSTGRESQL).build();
		Statement statement = Statement.of("diagnostic", "SELECT :value");
		StatementContext<Integer> context = StatementContext.<Integer>with(statement, database)
				.parameters(42).resultSetRowType(Integer.class).build();
		StatementContext<Integer> sameContext = StatementContext.<Integer>with(statement, database)
				.parameters(42).resultSetRowType(Integer.class).build();

		assertSame(DatabaseType.POSTGRESQL, context.getDatabaseType());
		assertEquals(context, sameContext);
		assertEquals(context.hashCode(), sameContext.hashCode());
		assertTrue(context.toString().contains("databaseType=POSTGRESQL"));
		assertTrue(StatementLog.withStatementContext(context).executionDuration(Duration.ZERO)
				.build().toString().contains("POSTGRESQL"));
	}

	@Test
	void acquisitionFailuresUseTheConfiguredDialectAndMetricsType() {
		AtomicInteger acquisitions = new AtomicInteger();
		List<DatabaseType> reportedTypes = new ArrayList<>();
		DataSource source = proxy(DataSource.class, (method, args) -> {
			if (method.getName().equals("getConnection")) {
				acquisitions.incrementAndGet();
				throw new SQLException("duplicate key", "23000", 2627);
			}
			throw new AssertionError("Unexpected data source access: " + method);
		});
		Database database = Database.with(source, DatabaseType.SQL_SERVER).metricsCollector(new MetricsCollector() {
			@Override
			public void didFailToAcquireStatementConnection(StatementContext<?> ctx, DatabaseType type,
					Duration duration, Throwable failure) {
				reportedTypes.add(type);
			}
		}).build();

		DatabaseException failure = assertThrows(DatabaseException.class,
				() -> database.query("SELECT 1").fetchObject(Integer.class));

		assertTrue(failure.isUniqueConstraintViolation());
		assertEquals(List.of(DatabaseType.SQL_SERVER), reportedTypes);
		assertEquals(1, acquisitions.get());
	}

	@Test
	void statementsStreamsAndTransactionsNeverReadDatabaseMetadata() {
		JDBCDataSource nativeSource = new JDBCDataSource();
		nativeSource.setUrl("jdbc:hsqldb:mem:explicit_type_" + UUID.randomUUID());
		nativeSource.setUser("sa");
		nativeSource.setPassword("");
		AtomicInteger metadataCalls = new AtomicInteger();
		List<DatabaseType> transactionTypes = new ArrayList<>();
		List<DatabaseType> statementTypes = new ArrayList<>();
		DataSource source = proxy(DataSource.class, (method, args) -> {
			Object result = invoke(nativeSource, method, args);
			if (!(result instanceof Connection connection)) return result;
			return proxy(Connection.class, (call, values) -> {
				if (call.getName().equals("getMetaData")) {
					metadataCalls.incrementAndGet();
					throw new AssertionError("Explicit database types must not require database metadata");
				}
				return invoke(connection, call, values);
			});
		});
		Database database = Database.with(source, DatabaseType.GENERIC).metricsCollector(new MetricsCollector() {
			@Override
			public void didEnterTransactionClosure(Transaction transaction, TransactionIsolation isolation, DatabaseType type) {
				transactionTypes.add(type);
			}
			@Override
			public void willAcquireTransactionConnection(Transaction transaction, DatabaseType type) {
				transactionTypes.add(type);
			}
			@Override
			public void willExecuteStatement(StatementContext<?> context) {
				statementTypes.add(context.getDatabaseType());
			}
		}).build();

		database.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
		database.transaction(() -> {
			database.query("INSERT INTO work VALUES (:id)").bind("id", 1).execute();
			assertEquals(Optional.of(1), database.query("SELECT id FROM work").fetchObject(Integer.class));
			assertEquals(List.of(1), database.query("SELECT id FROM work")
					.fetchStream(Integer.class, stream -> stream.toList()));
		});
		DatabaseException failure = assertThrows(DatabaseException.class,
				() -> database.query("INSERT INTO work VALUES (1)").execute());

		assertTrue(failure.isUniqueConstraintViolation());
		assertEquals(List.of(1), database.query("SELECT id FROM work").fetchList(Integer.class));
		assertEquals(List.of(DatabaseType.GENERIC, DatabaseType.GENERIC), transactionTypes);
		assertFalse(statementTypes.isEmpty());
		assertTrue(statementTypes.stream().allMatch(type -> type == DatabaseType.GENERIC));
		assertEquals(0, metadataCalls.get());
	}

	@FunctionalInterface
	private interface Call { Object run(Method method, Object[] args) throws Throwable; }

	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, args) -> call.run(method, args)));
	}

	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
