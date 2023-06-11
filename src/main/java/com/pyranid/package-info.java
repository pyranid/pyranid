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

/**
 * <a target="_blank" href="https://www.pyranid.com">Pyranid</a> is a minimalist JDBC interface for modern Java applications.
 * <p>
 * See <a target="_blank" href="https://www.pyranid.com">https://www.pyranid.com</a> for more detailed documentation and code samples.
 *
 * <pre>
 * // Minimal setup, uses defaults
 * Database database = Database.forDataSource(dataSource).build();
 *
 * // Customized setup
 * Database customDatabase = Database.forDataSource(dataSource).instanceProvider(new InstanceProvider() {
 * &#064;Override
 * public &lt;T&gt; T provide(Class&lt;T&gt; instanceClass) {
 * // You might have your DI framework vend resultset row instances
 * return guiceInjector.getInstance(instanceClass);
 * }
 * }).resultSetMapper(new ResultSetMapper() {
 * &#064;Override
 * public &lt;T&gt; T map(ResultSet rs, Class&lt;T&gt; resultClass) {
 * // Do some custom mapping here
 * }
 * }).preparedStatementBinder(new PreparedStatementBinder() {
 * &#064;Override
 * public void bind(PreparedStatement ps, List&lt;Object&gt; parameters) {
 * // Do some custom binding here
 * }
 * }).statementLogger(new StatementLogger() {
 * &#064;Override
 * public void log(StatementLog statementLog) {
 * // Send log to whatever output sink you'd like
 * out.println(statementLog);
 * }
 * }).build();
 *
 * // Queries
 * Optional&lt;Car&gt; specificCar = database.queryForObject("SELECT * FROM car WHERE id = ?", Car.class, 123);
 * List&lt;Car&gt; blueCars = database.queryForList("SELECT * FROM car WHERE color = ?", Car.class, Color.BLUE);
 * Optional&lt;UUID&gt; id = database.queryForObject("SELECT id FROM widget LIMIT 1", UUID.class);
 * List&lt;BigDecimal&gt; balances = database.queryForList("SELECT balance FROM account", BigDecimal.class);
 *
 * // Statements
 * int updateCount = database.execute("UPDATE car SET color = ?", Color.RED);
 * Optional&lt;UUID&gt; id = database.executeReturning("INSERT INTO book VALUES (?) RETURNING id", UUID.class, "The Stranger");
 *
 * // Transactions
 * database.transaction(() -&gt; {
 * BigDecimal balance1 = database.queryForObject("SELECT balance FROM account WHERE id = 1", BigDecimal.class).get();
 * BigDecimal balance2 = database.queryForObject("SELECT balance FROM account WHERE id = 2", BigDecimal.class).get();
 *
 * balance1 = balance1.subtract(amount);
 * balance2 = balance2.add(amount);
 *
 * database.execute("UPDATE account SET balance = ? WHERE id = 1", balance1);
 * database.execute("UPDATE account SET balance = ? WHERE id = 2", balance2);
 * });
 * </pre>
 *
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
package com.pyranid;