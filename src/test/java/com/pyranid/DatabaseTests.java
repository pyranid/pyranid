/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2023 Revetware LLC.
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
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 2.0.0
 */
public class DatabaseTests {
	public record EmployeeRecord(@DatabaseColumn("name") String displayName, String emailAddress) {}

	public static class EmployeeClass {
		private @DatabaseColumn("name") String displayName;
		private String emailAddress;

		public String getDisplayName() {
			return this.displayName;
		}

		public void setDisplayName(String displayName) {
			this.displayName = displayName;
		}

		public String getEmailAddress() {
			return this.emailAddress;
		}

		public void setEmailAddress(String emailAddress) {
			this.emailAddress = emailAddress;
		}
	}

	@Test
	public void testBasicQueries() {
		Database database = Database.forDataSource(createInMemoryDataSource()).build();

		createTestSchema(database);

		database.execute("INSERT INTO employee VALUES (1, 'Employee One', 'employee-one@company.com')");
		database.execute("INSERT INTO employee VALUES (2, 'Employee Two', NULL)");

		List<EmployeeRecord> employeeRecords = database.queryForList("SELECT * FROM employee ORDER BY name", EmployeeRecord.class);
		Assert.assertEquals("Wrong number of employees", 2, employeeRecords.size());
		Assert.assertEquals("Didn't detect DB column name override", "Employee One", employeeRecords.get(0).displayName());

		List<EmployeeClass> employeeClasses = database.queryForList("SELECT * FROM employee ORDER BY name", EmployeeClass.class);
		Assert.assertEquals("Wrong number of employees", 2, employeeClasses.size());
		Assert.assertEquals("Didn't detect DB column name override", "Employee One", employeeClasses.get(0).getDisplayName());
	}

	public record Product(Long productId, String name, BigDecimal price) {}

	@Test
	public void testTransactions() {
		Database database = Database.forDataSource(createInMemoryDataSource()).build();

		database.execute("CREATE TABLE product (product_id BIGINT, name VARCHAR(255) NOT NULL, price DECIMAL)");

		AtomicBoolean ranPostTransactionOperation = new AtomicBoolean(false);

		database.transaction(() -> {
			database.currentTransaction().get().addPostTransactionOperation((transactionResult -> {
				Assert.assertEquals("Wrong transaction result", TransactionResult.COMMITTED, transactionResult);
				ranPostTransactionOperation.set(true);
			}));

			database.execute("INSERT INTO product VALUES (1, 'VR Goggles', 3500.99)");

			Product product = database.queryForObject("""
					SELECT * 
					FROM product 
					WHERE product_id=?
					""", Product.class, 1L).orElse(null);

			Assert.assertNotNull("Product failed to insert", product);

			database.currentTransaction().get().rollback();

			product = database.queryForObject("""
					SELECT * 
					FROM product 
					WHERE product_id=?
					""", Product.class, 1L).orElse(null);

			Assert.assertNull("Product failed to roll back", product);
		});

		Assert.assertTrue("Did not run post-transaction operation", ranPostTransactionOperation.get());
	}

	@Test
	public void testCustomDatabase() {
		DataSource dataSource = createInMemoryDataSource();

		InstanceProvider instanceProvider = new DefaultInstanceProvider() {
			@Override
			@Nonnull
			public <T> T provide(@Nonnull StatementContext<T> statementContext,
													 @Nonnull Class<T> instanceClass) {
				if (Objects.equals("employee-query", statementContext.getStatement().getId()))
					System.out.printf("Creating instance of %s for Employee Query: %s\n",
							instanceClass.getSimpleName(), statementContext);

				return super.provide(statementContext, instanceClass);
			}
		};

		ResultSetMapper resultSetMapper = new DefaultResultSetMapper(instanceProvider) {
			@Nonnull
			@Override
			public <T> Optional<T> map(@Nonnull StatementContext<T> statementContext,
																 @Nonnull ResultSet resultSet,
																 @Nonnull Class<T> resultSetRowType) {
				if (Objects.equals("employee-query", statementContext.getStatement().getId()))
					System.out.printf("Mapping ResultSet for Employee Query: %s\n", statementContext);

				return super.map(statementContext, resultSet, resultSetRowType);
			}
		};

		PreparedStatementBinder preparedStatementBinder = new DefaultPreparedStatementBinder() {
			@Override
			public <T> void bind(@Nonnull StatementContext<T> statementContext,
													 @Nonnull PreparedStatement preparedStatement) {
				if (Objects.equals("employee-query", statementContext.getStatement().getId()))
					System.out.printf("Binding Employee Query: %s\n", statementContext);

				super.bind(statementContext, preparedStatement);
			}
		};

		StatementLogger statementLogger = new StatementLogger() {
			@Override
			public void log(StatementLog statementLog) {
				// Send log to whatever output sink you'd like
				if (Objects.equals("employee-query", statementLog.getStatementContext().getStatement().getId()))
					System.out.printf("Completed Employee Query: %s\n", statementLog);
			}
		};

		Database customDatabase = Database.forDataSource(dataSource)
				.timeZone(ZoneId.of("UTC")) // Override JVM default timezone
				.instanceProvider(instanceProvider)
				.resultSetMapper(resultSetMapper)
				.preparedStatementBinder(preparedStatementBinder)
				.statementLogger(statementLogger)
				.build();

		createTestSchema(customDatabase);

		customDatabase.execute("INSERT INTO employee VALUES (?, 'Employee One', 'employee-one@company.com')", 1);
		customDatabase.execute("INSERT INTO employee VALUES (2, 'Employee Two', NULL)");

		List<EmployeeRecord> employeeRecords = customDatabase.queryForList("SELECT * FROM employee ORDER BY name", EmployeeRecord.class);
		Assert.assertEquals("Wrong number of employees", 2, employeeRecords.size());
		Assert.assertEquals("Didn't detect DB column name override", "Employee One", employeeRecords.get(0).displayName());

		List<EmployeeClass> employeeClasses = customDatabase.queryForList("SELECT * FROM employee ORDER BY name", EmployeeClass.class);
		Assert.assertEquals("Wrong number of employees", 2, employeeClasses.size());
		Assert.assertEquals("Didn't detect DB column name override", "Employee One", employeeClasses.get(0).getDisplayName());

		EmployeeClass employee = customDatabase.queryForObject(new Statement("employee-query", """
				SELECT *
				FROM employee
				WHERE email_address=?
				"""), EmployeeClass.class, "employee-one@company.com").orElse(null);

		Assert.assertNotNull("Could not find employee", employee);
	}

	protected void createTestSchema(@Nonnull Database database) {
		requireNonNull(database);
		database.execute("CREATE TABLE employee (employee_id BIGINT, name VARCHAR(255) NOT NULL, email_address VARCHAR(255))");
	}

	@Nonnull
	protected DataSource createInMemoryDataSource() {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl("jdbc:hsqldb:mem:example");
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}
}
