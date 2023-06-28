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
 * <a target="_blank" href="https://www.pyranid.com">Pyranid</a> is a zero-dependency JDBC interface for modern Java applications.
 * <p>
 * See <a target="_blank" href="https://www.pyranid.com">https://www.pyranid.com</a> for more detailed documentation and code samples.
 *
 * <pre>
 * // Minimal setup, uses defaults
 * Database database = Database.forDataSource(dataSource).build();
 *
 * // Customized setup
 * // Controls how Pyranid creates instances of objects that represent ResultSet rows
 * InstanceProvider instanceProvider = new DefaultInstanceProvider() {
 *   &#064;Override
 *   &#064;Nonnull
 *   public &lt;T&gt; T provide(&#064;Nonnull StatementContext&lt;T&gt; statementContext,
 *                        &#064;Nonnull Class&lt;T&gt; instanceType) {
 *     // You might have your DI framework vend regular object instances
 *     return guiceInjector.getInstance(instanceType);
 *   }
 *
 *   &#064;Override
 *   &#064;Nonnull
 *   public &lt;T extends Record&gt; T provideRecord(&#064;Nonnull StatementContext&lt;T&gt; statementContext,
 *                                             &#064;Nonnull Class&lt;T&gt; recordType,
 *                                             &#064;Nullable Object... initargs) {
 *     // If you use Record types, customize their instantiation here.
 *     // Default implementation will use the canonical constructor
 *     return super.provideRecord(statementContext, recordType, initargs);
 *   }
 * };
 *
 * // Copies data from a ResultSet row to an instance of the specified type
 * ResultSetMapper resultSetMapper = new DefaultResultSetMapper(instanceProvider) {
 *   &#064;Nonnull
 *   &#064;Override
 *   public &lt;T&gt; Optional&lt;T&gt; map(&#064;Nonnull StatementContext&lt;T&gt; statementContext,
 *                              &#064;Nonnull ResultSet resultSet,
 *                              &#064;Nonnull Class&lt;T&gt; resultSetRowType) {
 *     return super.map(statementContext, resultSet, resultSetRowType);
 *   }
 * };
 *
 * // Binds parameters to a SQL PreparedStatement
 * PreparedStatementBinder preparedStatementBinder = new DefaultPreparedStatementBinder() {
 *   &#064;Override
 *   public &lt;T&gt; void bind(&#064;Nonnull StatementContext&lt;T&gt; statementContext,
 *                        &#064;Nonnull PreparedStatement preparedStatement,
 *                        &#064;Nonnull List&lt;Object&gt; parameters) {
 *     super.bind(statementContext, preparedStatement, parameters);
 *   }
 * };
 *
 * // Optionally logs SQL statements
 * StatementLogger statementLogger = new StatementLogger() {
 *   &#064;Override
 *   public void log(&#064;Nonnull StatementLog statementLog) {
 *     // Send to whatever output sink you'd like
 *     System.out.println(statementLog);
 *   }
 * };
 *
 * Database customDatabase = Database.forDataSource(dataSource)
 *   .timeZone(ZoneId.of("UTC")) // Override JVM default timezone
 *   .instanceProvider(instanceProvider)
 *   .resultSetMapper(resultSetMapper)
 *   .preparedStatementBinder(preparedStatementBinder)
 *   .statementLogger(statementLogger)
 *   .build();
 *
 * // Queries
 * Optional&lt;Car&gt; specificCar = database.queryForObject("SELECT * FROM car WHERE id = ?", Car.class, 123);
 * List&lt;Car&gt; blueCars = database.queryForList("SELECT * FROM car WHERE color = ?", Car.class, Color.BLUE);
 * Optional&lt;UUID&gt; id = database.queryForObject("SELECT id FROM widget LIMIT 1", UUID.class);
 * List&lt;BigDecimal&gt; balances = database.queryForList("SELECT balance FROM account", BigDecimal.class);
 *
 * // Statements
 * long updateCount = database.execute("UPDATE car SET color = ?", Color.RED);
 * Optional&lt;UUID&gt; id = database.executeForObject("INSERT INTO book VALUES (?) RETURNING id", UUID.class, "The Stranger");
 *
 * // Transactions
 * database.transaction(() -&gt; {
 *   BigDecimal balance1 = database.queryForObject("SELECT balance FROM account WHERE id = 1", BigDecimal.class).get();
 *   BigDecimal balance2 = database.queryForObject("SELECT balance FROM account WHERE id = 2", BigDecimal.class).get();
 *
 *   balance1 = balance1.subtract(amount);
 *   balance2 = balance2.add(amount);
 *
 *   database.execute("UPDATE account SET balance = ? WHERE id = 1", balance1);
 *   database.execute("UPDATE account SET balance = ? WHERE id = 2", balance2);
 * });
 * </pre>
 *
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
package com.pyranid;