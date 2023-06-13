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
import java.util.List;

/**
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.1.0
 */
public class DatabaseTests {
	public record Employee(@DatabaseColumn("name") String displayName, String emailAddress) {}

	@Test
	public void testRecords() {
		Database database = Database.forDataSource(createInMemoryDataSource()).build();

		database.execute("CREATE TABLE employee (employee_id BIGINT, name VARCHAR(255) NOT NULL, email_address VARCHAR(255))");
		database.execute("INSERT INTO employee VALUES (1, 'Employee One', 'employee-one@company.com')");
		database.execute("INSERT INTO employee VALUES (2, 'Employee Two', NULL)");

		List<Employee> employees = database.queryForList("SELECT * FROM employee ORDER BY name", Employee.class);
		Assert.assertEquals("Wrong number of employees", 2, employees.size());
		Assert.assertEquals("Didn't detect DB column name override", "Employee One", employees.get(0).displayName());
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
