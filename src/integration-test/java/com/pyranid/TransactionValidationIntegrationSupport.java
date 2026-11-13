/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package com.pyranid;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class TransactionValidationIntegrationSupport {
	private TransactionValidationIntegrationSupport() {}

	static final class Fixture {
		final Database database, inspection;
		final DatabaseType type;
		final boolean postgresFallback;
		final String table = "outcome_" + UUID.randomUUID().toString().replace("-", "");
		final AtomicInteger commits = new AtomicInteger(), rollbacks = new AtomicInteger(), validations = new AtomicInteger();
		final AtomicReference<TransactionCompletion> observed = new AtomicReference<>();
		final TransactionOptions options = TransactionOptions.withIsolation(TransactionIsolation.DEFAULT)
				.completionObserver(this.observed::set).build();

		Fixture(DataSource delegate, DatabaseType type) { this(delegate, type, false, null); }

		Fixture(DataSource delegate, DatabaseType type, boolean hidePostgresState, String duckPolicy) {
			this.type = type;
			this.postgresFallback = hidePostgresState;
			this.inspection = Database.with(delegate, type).build();
			this.inspection.query("CREATE TABLE " + this.table + " (id INT PRIMARY KEY)").execute();
			this.inspection.query("INSERT INTO " + this.table + " VALUES (1)").execute();
			DataSource wrapped = proxy(DataSource.class, (method, arguments) -> {
				Object result = invoke(delegate, method, arguments);
				if (!(result instanceof Connection connection)) return result;
				if (duckPolicy != null) {
					try (Statement settings = connection.createStatement()) {
						settings.execute("SET default_transaction_invalidation_policy = '" + duckPolicy + "'");
					}
				}
				return proxy(Connection.class, (jdbc, parameters) -> {
					String name = jdbc.getName();
					if (name.equals("commit")) this.commits.incrementAndGet();
					if (name.equals("rollback")) this.rollbacks.incrementAndGet();
					if (hidePostgresState && name.equals("isWrapperFor")) return false;
					Object value = invoke(connection, jdbc, parameters);
					if (!name.equals("createStatement") || !(value instanceof Statement statement)) return value;
					return proxy(Statement.class, (call, values) -> {
						if (call.getName().equals("executeQuery") && values != null && "SELECT 1".equals(values[0]))
							this.validations.incrementAndGet();
						return invoke(statement, call, values);
					});
				});
			});
			this.database = Database.with(wrapped, type).build();
		}

		void insert(int id) { this.database.query("INSERT INTO " + this.table + " VALUES (:id)").bind("id", id).execute(); }
		List<Integer> ids() { return this.inspection.query("SELECT id FROM " + this.table + " ORDER BY id").fetchList(Integer.class); }
		int normalValidationCount() { return this.type == DatabaseType.DUCK_DB || this.postgresFallback ? 1 : 0; }

		void assertHandledStatementAbort(boolean outcomeApi) {
			AtomicReference<DatabaseException> handled = new AtomicReference<>();
			ReturningTransactionalOperation<String> operation = () -> {
				insert(2);
				handled.set(assertThrows(DatabaseException.class, () -> insert(1)));
				return Optional.of("attempted receipt");
			};
			RuntimeException failure;
			if (outcomeApi) {
				TransactionOutcome<String> outcome = this.database.transactionWithOutcome(this.options, operation);
				assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
				assertEquals(Optional.of("attempted receipt"), outcome.getAttemptedValue());
				assertSame(this.observed.get(), outcome.getCompletion());
				failure = outcome.getFailure().orElseThrow();
			} else failure = assertThrows(DatabaseException.class, () -> this.database.transaction(this.options, operation));
			assertNotSame(handled.get(), failure);
			assertSame(handled.get(), failure.getCause());
			assertEquals(0, handled.get().getSuppressed().length);
			assertSame(failure, this.observed.get().getFailure().orElseThrow());
			assertEquals(TransactionResult.ROLLED_BACK, this.observed.get().getResult());
			assertEquals(List.of(1), ids());
			assertEquals(0, this.commits.get());
			assertEquals(1, this.rollbacks.get());
			assertEquals(normalValidationCount(), this.validations.get());
		}

		void assertRawHandledAbort(boolean escapesRawOperation) {
			AtomicReference<DatabaseException> handled = new AtomicReference<>();
			TransactionOutcome<String> outcome = this.database.transactionWithOutcome(this.options, () -> {
				insert(2);
				try {
					this.database.useRawConnection(connection -> {
						try (Statement statement = connection.createStatement()) {
							try { statement.executeUpdate("INSERT INTO " + this.table + " VALUES (1)"); }
							catch (SQLException failure) { if (escapesRawOperation) throw failure; }
						}
						return Optional.empty();
					});
				} catch (DatabaseException failure) { handled.set(failure); }
				return Optional.of("attempted receipt");
			});
			assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
			assertEquals(Optional.of("attempted receipt"), outcome.getAttemptedValue());
			assertSame(handled.get(), outcome.getFailure().orElseThrow().getCause());
			assertEquals(List.of(1), ids());
			assertEquals(0, this.commits.get());
			assertEquals(1, this.rollbacks.get());
			assertEquals(normalValidationCount(), this.validations.get());
		}

		void assertLastRowMapperHandledAbort() {
			ResultSetMapper mapper = new ResultSetMapper() {
				@Override public <T> Optional<T> map(StatementContext<T> context, java.sql.ResultSet result,
						Class<T> rowType, InstanceProvider provider) throws SQLException {
					int value = result.getInt(1);
					try (Statement statement = result.getStatement().getConnection().createStatement()) {
						try { statement.executeUpdate("INSERT INTO " + table + " VALUES (1)"); }
						catch (SQLException handled) { /* The mapping callback handles the driver's abort itself. */ }
					}
					return Optional.of(rowType.cast(value));
				}
			};
			TransactionOutcome<Integer> outcome = this.database.transactionWithOutcome(this.options, () -> {
				insert(2);
				return this.database.query("SELECT 42").resultSetMapper(mapper).fetchObject(Integer.class);
			});
			assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
			assertEquals(Optional.of(42), outcome.getAttemptedValue());
			DatabaseException failure = assertInstanceOf(DatabaseException.class, outcome.getFailure().orElseThrow());
			assertNull(failure.getCause());
			assertTrue(failure.getSqlState().isEmpty());
			assertEquals(0, this.commits.get());
			assertEquals(List.of(1), ids());
			assertEquals(normalValidationCount(), this.validations.get());
		}
	}

	@FunctionalInterface private interface Call { Object run(Method method, Object[] arguments) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
				(proxy, method, arguments) -> call.run(method, arguments)));
	}
	private static Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
		try { return method.invoke(target, arguments); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
