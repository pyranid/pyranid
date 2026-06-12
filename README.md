<a href="https://www.pyranid.com">
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="https://cdn.pyranid.com/pyranid-gh-logo-dark-v3.png">
        <img alt="Pyranid" src="https://cdn.pyranid.com/pyranid-gh-logo-light-v3.png" width="300" height="94">
    </picture>
</a>

### What Is It?

A zero-dependency JDBC interface for modern Java applications, powering production systems since 2015.

Pyranid takes care of boilerplate and lets you focus on writing and thinking in SQL.  It is not an ORM.

Pyranid makes working with JDBC pleasant and puts your relational database first. Writing SQL "just works". Closure-based transactions are simple to reason about. Modern Java language features are supported.

No new query languages to learn. No leaky object-relational abstractions. No kitchen-sink frameworks that come along for the ride. No magic.

Full documentation is available at [https://www.pyranid.com](https://www.pyranid.com).

### Design Goals

* Small codebase
* Customizable
* Threadsafe
* No dependencies
* DI-friendly

### License

[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)

### Do Zero-Dependency Libraries Interest You?

Similarly-flavored commercially-friendly OSS libraries are available.

* [Soklet](https://www.soklet.com) - DI-friendly HTTP 1.1 (and SSE) server that supports [JEP 444 Virtual Threads](https://openjdk.org/jeps/444)
* [Lokalized](https://www.lokalized.com) - natural-sounding translations (i18n) via expression language

### Maven Installation

Java 17+

```xml
<dependency>
  <groupId>com.pyranid</groupId>
  <artifactId>pyranid</artifactId>
  <version>4.2.0-SNAPSHOT</version>
</dependency>
```

Java 8+ (legacy; only critical fixes will be applied)

```xml
<dependency>
  <groupId>com.pyranid</groupId>
  <artifactId>pyranid</artifactId>
  <version>1.0.17</version>
</dependency>
```

### Direct Download

For released builds, you can download the Pyranid jar directly from [Maven Central](https://repo1.maven.org/maven2/com/pyranid/pyranid/).  No other dependencies are required by the core artifact.

## Configuration

### Minimal setup

```java
// Create a Database backed by a DataSource
DataSource dataSource = ...;
Database database = Database.withDataSource(dataSource).build();
```

### Customized setup

```java
// Controls how Pyranid creates instances of objects that represent ResultSet rows
InstanceProvider instanceProvider = new InstanceProvider() {
  @Override
  @NonNull
  public <T> T provide(@NonNull StatementContext<T> statementContext,
                       @NonNull Class<T> instanceType) {
    // You might have your DI framework vend regular object instances
    return guiceInjector.getInstance(instanceType);
  }
  
  @Override
  @NonNull
  public <T extends Record> T provideRecord(@NonNull StatementContext<T> statementContext,
                                            @NonNull Class<T> recordType,
                                            Object @Nullable ... initargs) {
    // If you use Record types, customize their instantiation here.
    // Default implementation will use the canonical constructor
    return super.provideRecord(statementContext, recordType, initargs);
  }
};

// Handles copying data from a ResultSet row to an instance of the specified type.
// Supports JavaBeans, records, and standard JDK types out-of-the-box.
// Plan caching (on by default) trades memory for faster mapping of wide ResultSets.
// Normalization locale should match the language of your database tables/column names.
// CustomColumnMappers supply "surgical" overrides to handle custom types.
// If multiple mappers apply, Pyranid tries them in list order.
ResultSetMapper resultSetMapper = ResultSetMapper.withPlanCachingEnabled(false)
  .normalizationLocale(Locale.forLanguageTag("pt-BR"))
  .customColumnMappers(List.of(new CustomColumnMapper() {
    @NonNull
    @Override
    public Boolean appliesTo(@NonNull TargetType targetType) {
      // Can also apply to parameterized types, e.g.
      // targetType.matchesParameterizedType(List.class, UUID.class) for List<UUID>
      return targetType.matchesClass(Money.class);
    }

    @NonNull
    @Override
    public MappingResult map(
      @NonNull StatementContext<?> statementContext,
      @NonNull ResultSet resultSet,
      @NonNull Object resultSetValue,
      @NonNull TargetType targetType,
      @NonNull Integer columnIndex,
      @Nullable String columnLabel,
      @NonNull InstanceProvider instanceProvider
    ) {
      // Convert the ResultSet column's value to the "appliesTo" Java type.
      // Don't need null checks - this method is only invoked when the value is non-null
      String moneyAsString = resultSetValue.toString();
      Money money = Money.parse(moneyAsString);

      // Or return MappingResult.fallback() to let the next applicable custom mapper run.
      // If none handles the value, Pyranid continues with normal mapping behavior.
      return MappingResult.of(money);
    }
  }))
  .build();

// Binds parameters to a SQL PreparedStatement.
// CustomParameterBinders supply "surgical" overrides to handle custom types.
// If multiple binders apply, Pyranid tries them in list order.
// Here, we transform Money instances into a DB-friendly string representation 
PreparedStatementBinder preparedStatementBinder = PreparedStatementBinder.withCustomParameterBinders(List.of(
  new CustomParameterBinder() {
    @NonNull
    @Override
    public Boolean appliesTo(@NonNull TargetType targetType) {
      return targetType.matchesClass(Money.class);
    }		
			
    @NonNull
    @Override
    public BindingResult bind(
      @NonNull StatementContext<?> statementContext, 
      @NonNull PreparedStatement preparedStatement,
      @NonNull Integer parameterIndex,
      @NonNull Object parameter
    ) throws SQLException {
      // Convert Money to a string representation for binding.
      // Don't need null checks - this method is only invoked when the value is non-null
      Money money = (Money) parameter;
      String moneyAsString = money.stringValue();
			
      // Bind to the PreparedStatement
      preparedStatement.setString(parameterIndex, moneyAsString);

      // Or return BindingResult.fallback() to let the next applicable custom binder run.
      // If none handles the value, Pyranid's normal binding rules apply.
      return BindingResult.handled();
    }
  }  
));

// Optionally logs SQL statements
StatementLogger statementLogger = new StatementLogger() {
  @Override
  public void log(@NonNull StatementLog statementLog) {
    // Send to whatever output sink you'd like
    out.println(statementLog);
  }
};

// Useful if your JVM's default timezone doesn't match your Database's default timezone
ZoneId timeZone = ZoneId.of("UTC");

Database customDatabase = Database.withDataSource(dataSource)
  .timeZone(timeZone)
  .ambiguousTimestampBindingStrategy(TIMESTAMP_WITH_TIME_ZONE)
  .instanceProvider(instanceProvider)
  .resultSetMapper(resultSetMapper)
  .preparedStatementBinder(preparedStatementBinder)
  .statementLogger(statementLogger)
  // Cache parsed SQL strings (0 disables caching).
  .parsedSqlCacheCapacity(1024)
  .build();
```

### Obtaining a DataSource

Pyranid works with any [`DataSource`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/javax/sql/DataSource.html) implementation. If you have the freedom to choose, [HikariCP](https://github.com/brettwooldridge/HikariCP) (application-level) and [PgBouncer](https://www.pgbouncer.org/) (external; Postgres-only) are good options.

```java
// HikariCP
DataSource hikariDataSource = new HikariDataSource(new HikariConfig() {{
  setJdbcUrl("jdbc:postgresql://localhost:5432/my-database");
  setUsername("example");
  setPassword("secret");
  setConnectionInitSql("SET TIME ZONE 'UTC'");
}});

// PgBouncer (using Postgres' JDBC driver-provided DataSource impl)
DataSource pgBouncerDataSource = new PGSimpleDataSource() {{
  setServerNames(new String[] {"localhost"});
  setPortNumber(5432);
  setDatabaseName("my-database");
  setUser("example");
  setPassword("secret");
  setPreferQueryMode(PreferQueryMode.SIMPLE);
}};
```

## Queries

Pyranid supports named parameters (e.g. `:id`) only; positional `?` parameters are not supported.

### SQL Parsing Rules

Pyranid scans SQL before handing it to JDBC so it can translate named parameters into `PreparedStatement` placeholders. It ignores parameter-looking text inside string literals, quoted identifiers, comments, PostgreSQL dollar-quoted strings, and SQL Server-style bracket-quoted identifiers. Unterminated quotes, dollar-quoted strings, and block comments fail fast with an `IllegalArgumentException`.

PostgreSQL JSONB/hstore `?`, `?|`, and `?&` operators are supported. When the `Database` is configured or detected as PostgreSQL, Pyranid automatically emits pgjdbc's escaped `??` form for those operators while preserving named-parameter binding.

Suppose we have a custom `Car` like this:

```java
enum Color { BLUE, RED }

// Follows JavaBean conventions for getters/setters
class Car {
  Long id;
  Color color;

  Long getId() { return this.id; }
  void setId(Long id) { this.id = id; }

  Color getColor() { return this.color; }
  void setColor(Color color) { this.color = color; }
}
```

We might query for it like this:

```java
// A single car
Optional<Car> car = database.query("SELECT * FROM car LIMIT 1")
  .fetchObject(Car.class);

// A single car, binding named parameters
Optional<Car> specificCar = database.query("SELECT * FROM car WHERE id=:id")
  .bind("id", 123)
  .fetchObject(Car.class);

// Multiple cars
List<Car> blueCars = database.query("SELECT * FROM car WHERE color=:color")
  .bind("color", Color.BLUE)
  .fetchList(Car.class);

// In addition to custom types, you can map to primitives and many JDK builtins out of the box.
// See 'ResultSet Mapping' section for details
Optional<UUID> id = database.query("SELECT id FROM widget LIMIT 1").fetchObject(UUID.class);
List<BigDecimal> balances = database.query("SELECT balance FROM account").fetchList(BigDecimal.class);
```

[`Record`](https://openjdk.org/jeps/395) types are also supported:

```java
record Employee(String name, @DatabaseColumn("email") String emailAddress) {}

Optional<Employee> employee = database.query("""
  SELECT *
  FROM employee
  WHERE email=:email
  """)
  .bind("email", "name@example.com")
  .fetchObject(Employee.class);
```

By default, Pyranid will invoke the canonical constructor for `Record` types. 

### Streaming Results

If you'd like to process large resultsets without loading everything into memory, use `Query::fetchStream(Class<T>, Function<Stream<T>, R>)`. The `Stream<T>` passed to your callback is backed by the underlying `ResultSet`, and Pyranid closes JDBC resources automatically when the callback returns (or throws).

```java
List<Employee> employees = database.query("""
  SELECT *
  FROM employee
  WHERE department_id=:departmentId
  """)
  .bind("departmentId", 42)
  .fetchStream(Employee.class, stream ->
    stream.filter(employee -> employee.departmentId() == 42)
      .toList());
```

The stream must be consumed within the scope of the transaction or connection that created it. Do not let the stream escape from the callback.

For PostgreSQL, Pyranid automatically configures streaming reads with an autocommit-disabled connection and a positive JDBC fetch size when no Pyranid transaction is active. Use [`Query::customize(...)`](https://javadoc.pyranid.com/com/pyranid/Query.html#customize(com.pyranid.PreparedStatementCustomizer)) to override the fetch size when needed. Other cursor-based drivers may require driver-specific transaction or fetch-size setup.

```java
List<Employee> employees = database.query("""
  SELECT *
  FROM employee
  ORDER BY employee_id
  """)
  .customize((statementContext, preparedStatement) ->
    preparedStatement.setFetchSize(1_000))
  .fetchStream(Employee.class, stream ->
    stream.limit(10_000).toList());
```

## Statements

```java
// General-purpose DML statement execution (INSERTs, UPDATEs, DELETEs, functions...)
long updateCount = database.query("UPDATE car SET color=:color")
  .bind("color", Color.RED)
  .execute();

// Return single or multiple objects via the SQL RETURNING clause for DML statements.
// Applicable for Postgres and Oracle.  SQL Sever uses an OUTPUT clause
Optional<BigInteger> insertedCarId = database.query("""
  INSERT INTO car (id, color)
  VALUES (nextval('car_seq'), :color)
  RETURNING id
  """)
  .bind("color", Color.GREEN)
  .executeForObject(BigInteger.class);

List<Car> repaintedCars = database.query("""
  UPDATE car
  SET color=:newColor
  WHERE color=:oldColor
  RETURNING *
  """)
  .bind("newColor", Color.GREEN)
  .bind("oldColor", Color.BLUE)
  .executeForList(Car.class);

// Batch operations can be more efficient than execution of discrete statements.
// Useful for inserting a lot of data at once
List<Map<String, Object>> parameterGroups = List.of(
  Map.of("id", 123, "color", Color.BLUE),
  Map.of("id", 456, "color", Color.RED)
);

// Insert both cars
List<Long> updateCounts = database.query("INSERT INTO car VALUES (:id, :color)")
  .executeBatch(parameterGroups);
```

Each parameter group must provide a complete set of values after merging with any values bound on the query, and all groups must expand to the same number of JDBC parameters (e.g., IN-list sizes must match).

Pyranid will automatically determine if your JDBC driver supports "large" updates and batch operations
and uses them if available.

## Transactions

### Design goals

* Closure-based API: rollback if exception bubbles out, commit at end of closure otherwise
* Data access APIs (e.g. [`Database::query`](https://javadoc.pyranid.com/com/pyranid/Database.html#query(java.lang.String))) automatically participate in transactions
* No [`Connection`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/Connection.html) is fetched from the [`DataSource`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/javax/sql/DataSource.html) until the first data access operation occurs
* Must be able to share a transaction across multiple threads

### Basics

```java
// Any code that runs inside of the closure operates within the context of a transaction.
// Pyranid will set autocommit=false for the duration of the transaction if necessary
database.transaction(() -> {
  // Pull initial account balances
  BigDecimal balance1 = database.query("SELECT balance FROM account WHERE id=:id")
    .bind("id", 1)
    .fetchObject(BigDecimal.class).orElseThrow();
  BigDecimal balance2 = database.query("SELECT balance FROM account WHERE id=:id")
    .bind("id", 2)
    .fetchObject(BigDecimal.class).orElseThrow();
  
  // Debit one and credit the other 
  balance1 = balance1.subtract(amount);
  balance2 = balance2.add(amount);

  // Persist changes.
  // Extra credit: this is a good candidate for query.executeBatch()
  database.query("UPDATE account SET balance=:balance WHERE id=:id")
    .bind("balance", balance1)
    .bind("id", 1)
    .execute();
  database.query("UPDATE account SET balance=:balance WHERE id=:id")
    .bind("balance", balance2)
    .bind("id", 2)
    .execute();
});

// For convenience, transactional operations may return values
Optional<BigDecimal> newBalance = database.transaction(() -> {
  // Make some changes
  database.query("UPDATE account SET balance=balance - 10 WHERE id=1")
    .execute();
  database.query("UPDATE account SET balance=balance + 10 WHERE id=2")
    .execute();

  // Return the new value
  return database.query("SELECT balance FROM account WHERE id=:id")
    .bind("id", 2)
    .fetchObject(BigDecimal.class);
});
```

### Context

```java
// Gets a handle to the current transaction, if any.
// The handle is useful for creating savepoints, forcing rollback, etc.
Optional<Transaction> transaction = database.currentTransaction();

// Output is "false"
out.println(transaction.isPresent());

database.transaction(() -> {
  // A transaction only exists for the life of the closure
  Optional<Transaction> actualTransaction = database.currentTransaction();

  // Output is "true"
  out.println(actualTransaction.isPresent());
});
```

Transaction handles are scoped to the transaction closure. After the closure completes, mutating or JDBC-touching methods on a captured `Transaction` handle throw `IllegalStateException`.

### Multi-threaded Transactions

Internally, Database manages a threadlocal stack of [`Transaction`](https://javadoc.pyranid.com/com/pyranid/Transaction.html) instances to simplify single-threaded usage.  Should you need to share the same transaction across multiple threads, use the [`Database::participate`](https://javadoc.pyranid.com/com/pyranid/Database.html#participate(com.pyranid.Transaction,com.pyranid.TransactionalOperation)) API.

Transactions are scoped to the `DataSource` instance that created them. A second `Database` built with the same `DataSource` instance can participate in the transaction; a `Database` built with a different `DataSource` fails fast instead of silently joining the wrong connection.

On Java 21+, a clean way to do this is with a virtual-thread executor:

```java
database.transaction(() -> {
  database.query("UPDATE account SET balance=balance - 10 WHERE id=1")
    .execute();

  // Get a handle to the current transaction
  Transaction transaction = database.currentTransaction().orElseThrow();

  try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
    CompletableFuture.runAsync(() -> {
      // In a different thread and participating in the existing transaction.
      // No commit or rollback will occur when the closure completes, but if an 
      // exception bubbles out the transaction will be marked as rollback-only
      database.participate(transaction, () -> {
        database.query("UPDATE account SET balance=balance + 10 WHERE id=2")
          .execute();
      });
    }, executor).join();
  }
});
```

### Rolling Back

```java
// Any exception that bubbles out will cause a rollback
database.transaction(() -> {
  database.query("UPDATE account SET balance=balance - 10 WHERE id=1")
    .execute();
  throw new IllegalStateException("Something's wrong!");
});

// You may mark a transaction as rollback-only, and it will roll back after the 
// closure execution has completed
database.transaction(() -> {
  database.query("UPDATE account SET balance=balance - 10 WHERE id=1")
    .execute();

  // Hmm...I changed my mind
  Transaction transaction = database.currentTransaction().orElseThrow();
  transaction.setRollbackOnly(true);
});

// You may roll back part of a transaction with a savepoint
database.transaction(() -> {
  Transaction transaction = database.currentTransaction().orElseThrow();

  try {
    transaction.withSavepoint(() -> {
      database.query("UPDATE account SET balance=balance - 10 WHERE id=1")
        .execute();

      // Hmm...I changed my mind about this part of the transaction
      throw new IllegalStateException("Undo the savepoint work");
    });
  } catch (IllegalStateException ignored) {
    // Work inside the savepoint was rolled back, and the outer transaction continues
  }
});
```

The lower-level `createSavepoint()`, `rollback(Savepoint)`, and `releaseSavepoint(Savepoint)` methods remain available for callers that need manual control. Prefer `withSavepoint(...)` for common partial-rollback workflows because it handles rollback, release, and suppressed cleanup failures consistently.

### Nesting

```java
// Each nested transaction is independent. There is no parent-child relationship
database.transaction(() -> {
  database.query("UPDATE account SET balance=balance - 10 WHERE id=1")
    .execute();

  // This transaction will commit
  database.transaction(() -> {
    database.query("UPDATE account SET balance=balance + 10 WHERE id=2")
      .execute();
  });

  // This transaction will not!
  throw new IllegalStateException("I should not have used nested transactions here...");
});
```

Nested transactions borrow separate physical connections and can pressure small connection pools. If your intent is to join work to an existing transaction, pass the current `Transaction` handle to `Database::participate(...)` instead of calling `transaction(...)` again.

### Isolation

```java
// You may specify the normal isolation levels per-transaction as needed:
// READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, and SERIALIZABLE.
// If not specified, DEFAULT is assumed (whatever your DBMS prefers)
database.transaction(TransactionIsolation.SERIALIZABLE, () -> {
  database.query("UPDATE account SET balance=balance - 10 WHERE id=1")
    .execute();
  database.query("UPDATE account SET balance=balance + 10 WHERE id=2")
    .execute();
});
```

### Post-Transaction Operations

It is useful to be able to schedule code to run after a transaction has completed.  Often, transaction management happens at a higher layer of code than business logic (e.g. a transaction-per-web-request pattern), so it is helpful to have a mechanism to "warp" local logic out to the higher layer.

Without this, you might run into subtle bugs like

* Write to database
* Send out notifications of system state change
* (Sometime later) transaction is rolled back
* Notification consumers are in an inconsistent state because they were notified of a change that was reversed by the rollback

```java
// Business logic
class EmployeeService {
  public void giveEveryoneRaises() {
    database.query("UPDATE employee SET salary=salary * 2")
      .execute();
    payrollSystem.startLengthyWarmupProcess();

    // Only send emails after the current transaction ends
    database.currentTransaction().orElseThrow().addPostTransactionOperation((transactionResult) -> {
      if(transactionResult == TransactionResult.COMMITTED) {
        // Successful commit? email everyone with the good news
        for(Employee employee : findAllEmployees())
          sendCongratulationsEmail(employee);
      } else if(transactionResult == TransactionResult.ROLLED_BACK) {
        // Rolled back? We can clean up
        payrollSystem.cancelLengthyWarmupProcess();	
      } else if(transactionResult == TransactionResult.IN_DOUBT) {
        // Commit was attempted but the final database outcome is unknown.
        // Avoid commit-only side effects and reconcile separately.
        payrollSystem.queueReconciliation();
      }
    });
  }
	
  // Rest of implementation elided
}

// Servlet filter which wraps requests in transactions
class DatabaseTransactionFilter implements Filter {
  @Override
  public void doFilter(ServletRequest servletRequest,
                       ServletResponse servletResponse,
                       FilterChain filterChain) throws IOException, ServletException {
    database.transaction(() -> {
      // Above business logic would happen somewhere down the filter chain
      filterChain.doFilter(servletRequest, servletResponse);

      // Business logic has completed at this point but post-transaction
      // operations will not run until the closure exits
    });

    // By this point, post-transaction operations will have been run
  }

  // Rest of implementation elided
}
```

Post-transaction callbacks receive a [`TransactionResult`](https://javadoc.pyranid.com/com/pyranid/TransactionResult.html) value:

* [`COMMITTED`](https://javadoc.pyranid.com/com/pyranid/TransactionResult.html#COMMITTED) if commit completed successfully
* [`ROLLED_BACK`](https://javadoc.pyranid.com/com/pyranid/TransactionResult.html#ROLLED_BACK) if the transaction completed on the rollback path before commit was attempted
* [`IN_DOUBT`](https://javadoc.pyranid.com/com/pyranid/TransactionResult.html#IN_DOUBT) if Pyranid attempted commit but the commit call failed, so the final database outcome is unknown

Post-transaction callbacks are fail-fast. If a callback throws, Pyranid wraps the failure in a [`PostTransactionOperationException`](https://javadoc.pyranid.com/com/pyranid/PostTransactionOperationException.html). When there is no primary transaction failure, `transaction()` throws this exception directly. When the transaction operation or commit already failed, Pyranid suppresses it onto the primary exception. Check [`getTransactionResult()`](https://javadoc.pyranid.com/com/pyranid/PostTransactionOperationException.html#getTransactionResult()) to distinguish a successful commit followed by callback failure from a failed or in-doubt transaction.

## ResultSet Mapping

The out-of-the-box [`ResultSetMapper`](https://javadoc.pyranid.com/com/pyranid/ResultSetMapper.html) implementation supports user-defined types that follow the JavaBean getter/setter conventions, primitives, and some additional common JDK types.

[`Record`](https://openjdk.org/jeps/395) types are also supported.

### Standard Types

When querying for a single column, e.g. a SQL `COUNT`, it's often useful to map to a standard type like `String` or `Integer` or `Boolean`.

There's no need to create a custom "row" type to hold the result.

```java
// Returns Optional<Long>, which we immediately unwrap because COUNT(*) is never null
Long count = database.query("SELECT COUNT(*) FROM car")
  .fetchObject(Long.class).orElseThrow();

// Standard primitives and JDK types are supported by default
Optional<UUID> id = database.query("SELECT id FROM employee LIMIT 1")
  .fetchObject(UUID.class);

// Lists work as you would expect
List<String> names = database.query("SELECT name FROM employee")
  .fetchList(String.class);
```

### User-defined Types

In the case of user-defined types and Records, the standard [`ResultSetMapper`](https://javadoc.pyranid.com/com/pyranid/ResultSetMapper.html) examines the names of columns in the [`ResultSet`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/ResultSet.html) and matches them to corresponding fields via reflection.  The [`@DatabaseColumn`](https://javadoc.pyranid.com/com/pyranid/DatabaseColumn.html) annotation allows per-field customization of mapping behavior.

By default, column names are assumed to be separated by `_` characters and are mapped to their camel-case equivalent.  For example:

```java
class Car {
  Long carId;
  Color color;
  
  // For schema flexibility, Pyranid will match both "deposit_amount1" and "deposit_amount_1" column names
  BigDecimal depositAmount1;
  
  // Use this annotation to specify variants if the field name doesn't match the column name
  @DatabaseColumn({"systok", "sys_tok"})
  UUID systemToken;

  Long getCarId() { return this.carId; }
  void setCarId(Long carId) { this.carId = carId; }

  Color getColor() { return this.color; }
  void setColor(Color color) { this.color = color; }
  
  BigDecimal getDepositAmount1() { return this.depositAmount1; }
  void setDepositAmount1(BigDecimal depositAmount1) { this.depositAmount1 = depositAmount1; }  

  UUID getSystemToken() { return this.systemToken; }
  void setSystemToken(UUID systemToken) { this.systemToken = systemToken; }
}

Car car = database.query("SELECT car_id, color, systok FROM car LIMIT 1")
  .fetchObject(Car.class).orElseThrow();

// Output might be "Car ID is 123 and color is BLUE. Token is d73c523a-8344-44ef-819c-40467662d619"
out.printf("Car ID is %s and color is %s. Token is %s\n",
                   car.getCarId(), car.getColor(), car.getSystemToken());

// Column names will work with wildcard queries as well
car = database.query("SELECT * FROM car LIMIT 1")
  .fetchObject(Car.class).orElseThrow();

// Column aliases work too
car = database.query("SELECT some_id AS car_id, some_color AS color FROM car LIMIT 1")
  .fetchObject(Car.class).orElseThrow();
```

### Supported Primitives

* [`Byte`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Byte.html)
* [`Short`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Short.html)
* [`Integer`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Integer.html)
* [`Long`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Long.html)
* [`Float`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Float.html)
* [`Double`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Double.html)
* [`Boolean`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Boolean.html)
* [`Character`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Character.html)
* [`String`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/String.html)
* [`byte[]`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Byte.html)

### Supported JDK Types

* [`Enum<E>`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Enum.html)
* [`UUID`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/UUID.html)
* [`BigDecimal`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/math/BigDecimal.html)
* [`BigInteger`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/math/BigInteger.html)
* [`Date`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/Date.html)
* [`Instant`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/Instant.html)
* [`LocalDate`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/LocalDate.html) for `DATE`
* [`LocalTime`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/LocalTime.html) for `TIME`
* [`LocalDateTime`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/LocalDateTime.html) for `TIMESTAMP`
* [`OffsetTime`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/OffsetTime.html) for `TIME WITH TIMEZONE`
* [`OffsetDateTime`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/OffsetDateTime.html) for `TIMESTAMP WITH TIMEZONE`
* [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/ZonedDateTime.html) for `TIMESTAMP` and `TIMESTAMP WITH TIMEZONE`
* [`java.sql.Timestamp`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/Timestamp.html)
* [`java.sql.Date`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/Date.html)
* [`ZoneId`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/ZoneId.html)
* [`TimeZone`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/TimeZone.html)
* [`Locale`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/Locale.html) (IETF BCP 47 "language tag" format)
* [`Currency`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/Currency.html)

Temporal result-set mapping uses JDBC `ResultSetMetaData` for the returned column. Zone-less `TIMESTAMP` values are wall-clock values; when mapping them to `Instant`, `OffsetDateTime`, `ZonedDateTime`, or `Date`, Pyranid interprets that wall clock in `Database.Builder::timeZone(...)`. Mapping a zone-less `TIMESTAMP` to `LocalDateTime` keeps the wall clock unchanged. `TIMESTAMP WITH TIME ZONE` values already identify an instant; mapping to `Instant` preserves that instant, and mapping to `ZonedDateTime` represents it in the configured `timeZone(...)`.

Pyranid preserves the fractional-second precision returned by your JDBC driver; it does not round or truncate `Instant`, `OffsetDateTime`, or `ZonedDateTime` values to milliseconds. The maximum precision is still determined by the database column and driver (for example, PostgreSQL timestamps are stored at microsecond precision).

### Custom Mapping

Fine-grained control of mapping is supported by registering [`CustomColumnMapper`](https://javadoc.pyranid.com/com/pyranid/CustomColumnMapper.html) instances.  For example, you might want to "inflate" a `JSONB` column into a Java type:

When multiple custom column mappers apply, Pyranid tries them in the order supplied. Returning `MappingResult.fallback()` lets the next applicable mapper run; if none handles the value, normal mapping continues.

```java
// Your application-specific type
class MySpecialType {
  List<UUID> uuids;
  Currency currency;	 
}
```

Just add a [`CustomColumnMapper`](https://javadoc.pyranid.com/com/pyranid/CustomColumnMapper.html) that handles it:

```java
ResultSetMapper resultSetMapper = ResultSetMapper.withCustomColumnMappers(List.of(new CustomColumnMapper() {
  @NonNull
  @Override
  public Boolean appliesTo(@NonNull TargetType targetType) {
    // Can also apply to parameterized types, e.g.
    // targetType.matchesParameterizedType(List.class, UUID.class) for List<UUID>
    return targetType.matchesClass(MySpecialType.class);
  }

  @NonNull
  @Override
  public MappingResult map(
    @NonNull StatementContext<?> statementContext,
    @NonNull ResultSet resultSet,
    @NonNull Object resultSetValue,
    @NonNull TargetType targetType,
    @NonNull Integer columnIndex,
    @Nullable String columnLabel,
    @NonNull InstanceProvider instanceProvider
  ) {
    // Pull JSON String data from the ResultSet and inflate it
    String json = resultSetValue.toString();
    MySpecialType mySpecialType = GSON.fromJson(json, MySpecialType.class);

    // Or return MappingResult.fallback() to let the next applicable custom mapper run.
    // If none handles the value, Pyranid continues with normal mapping behavior.
    return MappingResult.of(mySpecialType);
  }
}))
.build();

// Construct your database with the custom mapper
Database database = Database.withDataSource(...)
  .resultSetMapper(resultSetMapper)
  .build();
```

With the custom mapper in place, and a table like this...

```sql
CREATE TABLE row (
  row_id UUID PRIMARY KEY,
  my_special_type JSONB NOT NULL
);

INSERT INTO row (row_id, my_special_type) VALUES (
  'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
  '
    {
      "uuids": ["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"],
      "currency": "BRL"
    }
  '::jsonb
);
```

...your application code might look like this:

```java
// A ResultSet row with our special type as a column 
record MyRow(UUID rowId, MySpecialType mySpecialType) {}

// Query for data
List<MyRow> rows = database.query("SELECT * FROM row")
  .fetchList(MyRow.class);

// Examine the first row of the ResultSet
MyRow myRow = rows.getFirst();
// Our custom mapper has instantiated this for us
MySpecialType mySpecialType = myRow.mySpecialType();
// Prints contents of List<UUID>, as expected
out.println(mySpecialType.uuids);
// e.g. "Real brasileiro" for Brazilian Real
out.println(mySpecialType.currency.getDisplayName(Locale.forLanguageTag("pt-BR")));
```

Your [`CustomColumnMapper`](https://javadoc.pyranid.com/com/pyranid/CustomColumnMapper.html) also works for the single-column "Standard Type" scenario.

```java
// Pull back the column for a single row
Optional<MySpecialType> mySpecialType =
  database.query("SELECT my_special_type FROM row LIMIT 1")
    .fetchObject(MySpecialType.class);

// Pull back a list of just the column values 
List<MySpecialType> mySpecialTypes = 
  database.query("SELECT my_special_type FROM row")
    .fetchList(MySpecialType.class);
```

## Parameter Binding

The out-of-the-box [`PreparedStatementBinder`](https://javadoc.pyranid.com/com/pyranid/PreparedStatementBinder.html) implementation supports binding common JDK types and generally "just works" as you would expect.

For example:

```java
UUID departmentId = ...;
Long accountId = ...;

List<Employee> employees = database.query("""
  SELECT *
  FROM employee
  WHERE department_id=:departmentId
""")
  .bind("departmentId", departmentId)
  .fetchList(Employee.class);

database.query("""
  INSERT INTO account_award (
    account_id, 
    award_type
  ) VALUES (:accountId, :awardType)
  """)
  .bind("accountId", accountId)
  .bind("awardType", AwardType.BIG)
  .execute();
```

### PreparedStatementCustomizer

If you need to tweak the JDBC `PreparedStatement` before execution (for example, to set query hints), use `Query::customize(...)`.

```java
List<Employee> employees = database.query("""
  SELECT *
  FROM employee
  WHERE department_id=:departmentId
  """)
  .bind("departmentId", departmentId)
  .customize((statementContext, preparedStatement) -> {
    preparedStatement.setFetchSize(500);
  })
  .fetchList(Employee.class);
```

### IN-list parameters

Use `Parameters.inList(...)` to expand values into a SQL `IN (...)` list.
This is useful for SQL `IN` lists:

```java
List<String> emails = List.of("a@example.com", "b@example.com");

List<Employee> employees = database.query("""
  SELECT *
  FROM employee
  WHERE email IN (:emails)
""")
  .bind("emails", Parameters.inList(emails))
  .fetchList(Employee.class);
```

Notes:
* Empty collections/arrays throw `IllegalArgumentException`.
* `null` elements and empty `Optional` elements throw `IllegalArgumentException`. SQL `IN` does not match `NULL`; use an explicit `IS NULL` predicate when null matching is required.
* Expansion is context-agnostic; using it outside of `IN (...)` is allowed but may produce invalid SQL.
* Raw `Collection`/array values are rejected; use `Parameters.inList(...)`.
* Primitive arrays are supported via overloads (e.g. `int[]`, `long[]`).
* If you want SQL ARRAY binding, use `Parameters.sqlArrayOf(...)`.
* If you want to bind a typed collection via a custom binder, use `Parameters.listOf(...)`/`Parameters.setOf(...)`.
* If you want to bind a typed array via a custom binder, use `Parameters.arrayOf(Class, ...)`.

### Supported Primitives

* [`Byte`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Byte.html)
* [`Short`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Short.html)
* [`Integer`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Integer.html)
* [`Long`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Long.html)
* [`Float`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Float.html)
* [`Double`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Double.html)
* [`Boolean`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Boolean.html)
* [`Character`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Character.html)
* [`String`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/String.html)
* [`byte[]`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Byte.html)

### Supported JDK Types

* [`Enum<E>`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/Enum.html)
* [`UUID`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/UUID.html)
* [`BigDecimal`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/math/BigDecimal.html)
* [`BigInteger`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/math/BigInteger.html)
* [`Date`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/Date.html)
* [`Instant`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/Instant.html)
* [`LocalDate`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/LocalDate.html)
* [`LocalTime`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/LocalTime.html)
* [`LocalDateTime`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/LocalDateTime.html)
* [`OffsetTime`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/OffsetTime.html)
* [`OffsetDateTime`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/OffsetDateTime.html)
* [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/ZonedDateTime.html)
* [`java.sql.Timestamp`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/Timestamp.html)
* [`java.sql.Date`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/Date.html)
* [`java.sql.Time`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/Time.html)
* [`ZoneId`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/time/ZoneId.html)
* [`TimeZone`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/TimeZone.html)
* [`Locale`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/Locale.html) (IETF BCP 47 "language tag" format)
* [`Currency`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/Currency.html)

Temporal binding uses JDBC `ParameterMetaData` when available to distinguish `TIMESTAMP` from `TIMESTAMP WITH TIME ZONE` targets. For known zone-less `TIMESTAMP` targets, Pyranid converts `Instant` and `OffsetDateTime` parameters through `Database.Builder::timeZone(...)` and binds the resulting local timestamp. For known `TIMESTAMP WITH TIME ZONE` targets, Pyranid binds them as time-zone-aware timestamps. `ZonedDateTime` parameters are normalized to `OffsetDateTime` first and follow the same rules.

If parameter metadata is unavailable or non-identifying, `Instant` and `OffsetDateTime` default to `TIMESTAMP_WITH_TIME_ZONE`. For drivers or proxies that cannot provide identifying parameter metadata when your target columns are zone-less `TIMESTAMP` values, configure `ambiguousTimestampBindingStrategy(TIMESTAMP_WITHOUT_TIME_ZONE)`.

### Special Parameters

Special support is provided for IN-list, JSON/JSONB, vector, and SQL ARRAY parameters.

#### JSON/JSONB

Useful for storing "stringified" JSON data by taking advantage of the DBMS' native JSON storage facilities, if available (e.g. PostgreSQL's `JSONB` type).

Supported methods:

* [`Parameters::json(String)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#json(java.lang.String))
* [`Parameters::json(String, BindingPreference)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#json(java.lang.String,com.pyranid.JsonParameter.BindingPreference))

You might create a JSONB storage table...

```sql
CREATE TABLE example (
  example_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  data JSONB NOT NULL
);
```

...and write JSON to it:

```java
String json = "{\"testing\": 123}";

database.query("INSERT INTO example (data) VALUES (:data)")
  .bind("data", Parameters.json(json))
  .execute();
```

By default, Pyranid will use your database's binary JSON format if supported and fall back to a text representation otherwise.

If you want to force text storage (e.g. if whitespace is important), specify a binding preference like this:

```java
database.query("INSERT INTO example (data) VALUES (:data)")
  .bind("data", Parameters.json(json, BindingPreference.TEXT))
  .execute();
```

#### Vector

Useful for storing [`pgvector`](https://github.com/pgvector/pgvector) data, often used for Artificial Intelligence tasks. Currently only supported for PostgreSQL.

Supported methods:

* [`Parameters::vectorOfDoubles(double[])`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#vectorOfDoubles(double%5B%5D))
* [`Parameters::vectorOfDoubles(List<Double>)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#vectorOfDoubles(java.util.List))
* [`Parameters::vectorOfFloats(float[])`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#vectorOfFloats(float%5B%5D))
* [`Parameters::vectorOfFloats(List<Float>)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#vectorOfFloats(java.util.List))
* [`Parameters::vectorOfBigDecimals(List<BigDecimal>)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#vectorOfBigDecimals(java.util.List))

You might create a vector storage table...

```sql
CREATE TABLE vector_embedding (
  vector_embedding_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  embedding VECTOR(1536) NOT NULL,
  content TEXT NOT NULL
);
```

...and write vector data to it:

```java
double[] embedding = ...;
String content = "...";

database.query("INSERT INTO vector_embedding (embedding, content) VALUES (:embedding, :content)")
  .bind("embedding", Parameters.vectorOfDoubles(embedding))
  .bind("content", content)
  .execute();
```

#### SQL ARRAY

Single-dimension array binding is supported out-of-the-box.

Supported methods:

* [`Parameters::sqlArrayOf(String, E[])`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#sqlArrayOf(java.lang.String,E%5B%5D))
* [`Parameters::sqlArrayOf(String, List<E>)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#sqlArrayOf(java.lang.String,java.util.List))

You might create a table with some array columns...

```sql
CREATE TABLE product (
  product_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  name TEXT NOT NULL,
  vendor_flags INTEGER[] NOT NULL,
  tags VARCHAR[]
);
```

...and write array data to it:

```java
String name = "...";
Integer[] vendorFlags = { 1, 2, 3 };
List<String> tags = List.of("alpha", "beta");

database.query("""
  INSERT INTO product (
    name,
    vendor_flags,
    tags
  ) VALUES (:name, :vendorFlags, :tags)
""")
  .bind("name", name)
  .bind("vendorFlags", Parameters.sqlArrayOf("INTEGER", vendorFlags))
  .bind("tags", Parameters.sqlArrayOf("VARCHAR", tags))
  .execute();
```

If you need support for multidimensional array binding, implement a [`CustomParameterBinder`](https://javadoc.pyranid.com/com/pyranid/CustomParameterBinder.html) as outlined below. 

### Custom Parameters

You may register instances of [`CustomParameterBinder`](https://javadoc.pyranid.com/com/pyranid/CustomParameterBinder.html) to bind application-specific types to [`java.sql.PreparedStatement`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/PreparedStatement.html) however you like.

This allows you to use your objects as-is with Pyranid instead of sprinkling "convert this object to database format" code throughout your system.

When multiple custom parameter binders apply, Pyranid tries them in the order supplied. Returning `BindingResult.fallback()` lets the next applicable binder run; if none handles the value, Pyranid's normal binding rules apply.

#### Arbitrary Types

Let's define a simple type.

```java
class HexColor {
  int r, g, b;

  HexColor(int r, int g, int b) {
    this.r = r; this.g = g; this.b = b;
  }

  String toHexString() {
    return String.format("#%02X%02X%02X", r, g, b);
  }

  static HexColor fromHexString(String s) {
    int r = Integer.parseInt(s.substring(1, 3), 16);
    int g = Integer.parseInt(s.substring(3, 5), 16);
    int b = Integer.parseInt(s.substring(5, 7), 16);
    return new HexColor(r, g, b);
  }
}
```

Then, we'll register a [`CustomParameterBinder`](https://javadoc.pyranid.com/com/pyranid/CustomParameterBinder.html) to handle binding it:

```java
PreparedStatementBinder preparedStatementBinder = PreparedStatementBinder.withCustomParameterBinders(List.of(
  new CustomParameterBinder() {
    @NonNull
    @Override
    public Boolean appliesTo(@NonNull TargetType targetType) {
      return targetType.matchesClass(HexColor.class);
    }		
			
    @NonNull
    @Override
    public BindingResult bind(
      @NonNull StatementContext<?> statementContext, 
      @NonNull PreparedStatement preparedStatement,
      @NonNull Integer parameterIndex,
      @NonNull Object parameter
    ) throws SQLException {
      HexColor hexColor = (HexColor) parameter; 

      // Bind to the PreparedStatement as a value like "#6a5acd"
      preparedStatement.setString(parameterIndex, hexColor.toHexString());

      // Or return BindingResult.fallback() to let the next applicable custom binder run.
      // If none handles the value, Pyranid's normal binding rules apply.
      return BindingResult.handled();
    }
  }  
));

Database database = Database.withDataSource(dataSource)
  .preparedStatementBinder(preparedStatementBinder)
  .build();
```

With the custom binder in place, your application code might look like this:

```java
// Given a reference to a hex color...
UUID themeId = ...;
HexColor backgroundColor = HexColor.fromHexString("#6a5acd");

// ...we use the reference as-is and Pyranid will apply the custom binder
database.query("""
  UPDATE theme
  SET background_color=:backgroundColor
  WHERE theme_id=:themeId 
""")
  .bind("backgroundColor", backgroundColor)
  .bind("themeId", themeId)
  .execute();
```

#### Standard Collections

Runtime binding of generic types is made difficult by type erasure.  For convenience, Pyranid offers special parameters that perform type capture for standard [`List<E>`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/List.html), [`Set<E>)`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/Set.html), and [`Map<K,V>)`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/Map.html) types:

* [`Parameters::listOf(Class<E>, List<E>)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#listOf(java.lang.Class,java.util.List))
* [`Parameters::setOf(Class<E>, Set<E>)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#setOf(java.lang.Class,java.util.List))
* [`Parameters::mapOf(Class<K>, Class<V>, Map<K,V>)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#mapOf(java.lang.Class,java.lang.Class,java.util.Map))

This makes it easy to create custom binders for common scenarios.

For example, this code...

```java
List<UUID> ids = List.of(
  UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
  UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
);

database.query("INSERT INTO t(v) VALUES (:v)")
  .bind("v", Parameters.listOf(UUID.class, ids))
  .execute();
```

...would be handled by this custom binder, because `targetType.matchesParameterizedType(List.class, UUID.class)` returns `true` thanks to runtime type capturing:

```java
PreparedStatementBinder preparedStatementBinder = PreparedStatementBinder.withCustomParameterBinders(List.of(
  new CustomParameterBinder() {
    @NonNull
    @Override
    public Boolean appliesTo(@NonNull TargetType targetType) {
      // For Parameters::mapOf(Class<K>, Class<V>, Map<K,V>), you'd say:
      // matchesParameterizedType(Map.class, MyKey.class, MyValue.class)
      return targetType.matchesParameterizedType(List.class, UUID.class);
    }		
			
    @NonNull
    @Override
    public BindingResult bind(
      @NonNull StatementContext<?> statementContext, 
      @NonNull PreparedStatement preparedStatement,
      @NonNull Integer parameterIndex,
      @NonNull Object parameter
    ) throws SQLException {
      // Convert UUIDs to a comma-delimited string, or null for the empty list 
      List<UUID> uuids = (List<UUID>) param;
      String uuidsAsString = uuids.isEmpty()
        ? null
        : uuids.stream().map(Object::toString).collect(Collectors.joining(","));

      // Bind to the PreparedStatement
      preparedStatement.setString(parameterIndex, uuidsAsString);
			
      return BindingResult.handled();
    }
  }  
));
```

##### Heads Up!

If you use [`Parameters::listOf(Class<E>, List<E>)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#listOf(java.lang.Class,java.util.List)), [`Parameters::setOf(Class<E>, Set<E>)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#setOf(java.lang.Class,java.util.List)), or [`Parameters::mapOf(Class<K>, Class<V>, Map<K,V>)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#mapOf(java.lang.Class,java.lang.Class,java.util.Map)), you must define a corresponding [`CustomParameterBinder`](https://javadoc.pyranid.com/com/pyranid/CustomParameterBinder.html) to handle them.  These special parameter types do not automatically work out-of-the-box because Pyranid cannot reliably guess how you intend to bind them.
This applies even when the wrapped value is `null`; implement `CustomParameterBinder#bindNull(...)` if you want typed nulls to bind successfully.

Pyranid will detect this missing-binder scenario and throw an exception to indicate programmer error.

#### Typed Arrays

If you need array component types at runtime for a custom binder, use `Parameters.arrayOf(Class, ...)`.
This captures the array element type (including primitives) so your binder can match via `TargetType.isArray()`/`getArrayComponentType()`.
Typed arrays require a corresponding [`CustomParameterBinder`](https://javadoc.pyranid.com/com/pyranid/CustomParameterBinder.html); otherwise binding fails fast.
This applies even when the wrapped value is `null`; implement `CustomParameterBinder#bindNull(...)` if you want typed nulls to bind successfully.
For SQL ARRAY binding, use `Parameters.sqlArrayOf(...)`.

```java
String[] names = {"alpha", "beta"};

database.query("INSERT INTO t(v) VALUES (:v)")
  .bind("v", Parameters.arrayOf(String.class, names))
  .execute();
```

## Error Handling

In general, a runtime [`DatabaseException`](https://javadoc.pyranid.com/com/pyranid/DatabaseException.html) will be thrown when errors occur.  Often this will wrap the checked [`java.sql.SQLException`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/SQLException.html).

For convenience, [`DatabaseException`](https://javadoc.pyranid.com/com/pyranid/DatabaseException.html) exposes additional properties, which are populated if provided by the underlying [`java.sql.SQLException`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/SQLException.html):

* `errorCode` (optional)
* `sqlState` (optional)

For PostgreSQL, the following properties are also available:

* `column` (optional)
* `constraint` (optional)
* `datatype` (optional)
* `detail` (optional)
* `file` (optional)
* `hint` (optional)
* `internalPosition` (optional)
* `internalQuery` (optional)
* `line` (optional)
* `dbmsMessage` (optional)
* `position` (optional)
* `routine` (optional)
* `schema` (optional)
* `severity` (optional)
* `table` (optional)
* `where` (optional)

### Practical Application

Here we detect if a specific constraint was violated by examining [`DatabaseException`](https://javadoc.pyranid.com/com/pyranid/DatabaseException.html).
We then handle that case specially by rolling back to a known-good savepoint.

```java
// Gives someone at most one big award
database.transaction(() -> {
  Transaction transaction = database.currentTransaction().orElseThrow();
  Savepoint savepoint = transaction.createSavepoint();

  try {
    // We don't want to give someone the same award twice!
    // Let the DBMS worry about constraint checking to avoid race conditions
    database.query("INSERT INTO account_award (account_id, award_type) VALUES (:accountId, :awardType)")
      .bind("accountId", accountId)
      .bind("awardType", AwardType.BIG)
      .execute();
  } catch(DatabaseException e) {
    // Detect a unique constraint violation and gracefully continue on.
    if("account_award_unique_idx".equals(e.getConstraint().orElse(null)) {
      out.printf("The %s award was already given to account ID %s\n", AwardType.BIG, accountId); 
      // Puts transaction back in good state (prior to constraint violation)
      transaction.rollback(savepoint);
    } else {      
      // There must have been some other problem, bubble out
      throw e;
    }
  }
});
```

## Logging and Diagnostics

### Statement Logging

You may customize your [`Database`](https://javadoc.pyranid.com/com/pyranid/Database.html) with a [`StatementLogger`](https://javadoc.pyranid.com/com/pyranid/StatementLogger.html).

Examples of usage include:

* Writing queries and timing information to your logging system
* Picking out slow queries for special logging/reporting
* Collecting a set of queries executed across a unit of work for bulk analysis (e.g. a [`ThreadLocal`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/ThreadLocal.html) scoped to a single web request)

```java
Database database = Database.withDataSource(dataSource)
  .statementLogger(new StatementLogger() {
    Duration SLOW_QUERY_THRESHOLD = Duration.ofMillis(500);

    @Override
    public void log(@NonNull StatementLog statementLog) {
      if(statementLog.getTotalDuration().compareTo(SLOW_QUERY_THRESHOLD) > 0)
        out.printf("Slow query: %s\n", statementLog);
    }
  }).build();
```

`StatementLogger` failures are intentionally fail-fast. If your logger throws after a statement otherwise succeeds, that exception propagates to the caller. Inside a Pyranid transaction, logger failures participate in normal transaction failure handling and cause rollback. If the statement itself failed, the logger failure is attached as a suppressed exception to the primary failure.

Keep logger implementations lightweight and failure-safe. If logging must never affect database writes, catch and handle exceptions inside your logger implementation, or use [`MetricsCollector`](https://javadoc.pyranid.com/com/pyranid/MetricsCollector.html) for best-effort observability.

[`StatementLog`](https://javadoc.pyranid.com/com/pyranid/StatementLog.html) instances give you access to the following for each SQL statement executed:

* `statementContext`
* `connectionAcquisitionDuration` (optional)
* `preparationDuration` (optional)
* `executionDuration` (optional)
* `resultSetMappingDuration` (optional)
* `batchSize` (optional)
* `exception` (optional)

### Statement Identifiers

For any data access method that accepts a `sql` parameter, you may alternatively provide a [`Statement`](https://javadoc.pyranid.com/com/pyranid/Statement.html), which permits you to specify an arbitrary identifier for the SQL.

If you do not explicitly provide a [`Statement`](https://javadoc.pyranid.com/com/pyranid/Statement.html), Pyranid will create one for you and generate its own identifier.

```java
// Regular SQL
database.query("SELECT * FROM car LIMIT 1").fetchObject(Car.class);

// Regular SQL with an explicit identifier
database.query("SELECT * FROM car LIMIT 1")
  .id("random-car")
  .fetchObject(Car.class);
```

This is useful for tagging queries that should be handled specially. Some examples are:

* Marking a query as "hot" so we don't pollute logs with it
* Marking a query as "known to be slow" so we don't flag slow query alerts for it
* Your [`InstanceProvider`](https://javadoc.pyranid.com/com/pyranid/InstanceProvider.html) might provide custom instances based on ResultSet data
* Your [`ResultSetMapper`](https://javadoc.pyranid.com/com/pyranid/ResultSetMapper.html) might perform special mapping
* Your [`PreparedStatementBinder`](https://javadoc.pyranid.com/com/pyranid/PreparedStatementBinder.html) might perform special binding

```java
// Custom tagging system
enum QueryTag {
  HOT_QUERY,
  SLOW_QUERY
};

// This query fires every 3 seconds - let's mark it HOT_QUERY so we know not to log it.
Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
  database.transaction(() -> {
    List<Message> messages = database.query("""
      SELECT *
      FROM message_queue
      WHERE message_status_id=:messageStatusId
      LIMIT :batchSize
      FOR UPDATE
      SKIP LOCKED
    """)
      .id(QueryTag.HOT_QUERY)
      .bind("messageStatusId", MessageStatusId.UNPROCESSED)
      .bind("batchSize", BATCH_SIZE)
      .fetchList(Message.class);

    // Implementation not shown
    processMessages(messages);
  });
}, 0, 3, TimeUnit.SECONDS);
```

A corresponding [`Database`](https://javadoc.pyranid.com/com/pyranid/Database.html) setup:

```java
// Ensure our StatementLogger implementation takes HOT_QUERY into account 
Database database = Database.withDataSource(dataSource)
  .statementLogger(new StatementLogger() {
    @Override
    public void log(@NonNull StatementLog statementLog) {
      // Log everything except HOT_QUERY
      if(statementLog.getStatementContext().getStatement().getId() != HOT_QUERY)
        out.println(statementLog);
    }
  }).build();
```

## Development Verification

Before cutting a release, run the local verification gates from the `pyranid/` project directory:

```bash
mvn -q verify
mvn -q javadoc:javadoc
mvn -q -P integration verify
```

The `integration` Maven profile runs Docker-backed PostgreSQL integration tests with Testcontainers and requires a working local Docker environment. The initial PostgreSQL image is pinned to `postgres:17-alpine`.

The PostgreSQL integration profile currently covers core pgjdbc behavior such as JSONB, SQL arrays, `RETURNING`, temporal binding/mapping, and exception metadata. It does not run pgvector extension tests; verify pgvector manually if your release depends on that feature.

Artifact signing and Maven Central publishing are isolated in the `release` profile. Use `mvn -P release deploy` only when publishing a release or snapshot; normal local and CI `verify` runs do not require GPG credentials.

## Production Notes

Pyranid is a zero-runtime-dependency library, but applications still need to provide the JDBC driver for their database. PostgreSQL-specific helpers such as JSONB, `text[]`, and pgvector parameters require pgjdbc on the application classpath; pgvector also requires the database extension to be installed.

[`Database.build()`](https://javadoc.pyranid.com/com/pyranid/Database.Builder.html#build()) does not validate connectivity or inspect JDBC metadata. If you do not configure [`Database.Builder::databaseType(...)`](https://javadoc.pyranid.com/com/pyranid/Database.Builder.html#databaseType(com.pyranid.DatabaseType)), Pyranid detects the database type lazily when code first requests database-type-sensitive behavior. Calling [`Database::getDatabaseType()`](https://javadoc.pyranid.com/com/pyranid/Database.html#getDatabaseType()) outside an active query may acquire a fresh connection; configure `databaseType(...)` explicitly when using small pools, database proxies, or startup paths that must avoid surprise connection checkouts.

`Database.Builder::timeZone(...)` controls how zone-less `TIMESTAMP` values are interpreted when mapping to instant-based Java types. It also controls binding if `ambiguousTimestampBindingStrategy(TIMESTAMP_WITHOUT_TIME_ZONE)` is enabled for drivers that cannot report identifying timestamp parameter metadata.

A [`Database`](https://javadoc.pyranid.com/com/pyranid/Database.html) instance has one effective [`DatabaseType`](https://javadoc.pyranid.com/com/pyranid/DatabaseType.html). If one application talks to multiple database products, create one `Database` per product and configure each explicitly.

Each call to [`transaction(...)`](https://javadoc.pyranid.com/com/pyranid/Database.html#transaction(com.pyranid.TransactionalOperation)) opens an independent transaction with its own physical connection. Nested `transaction(...)` calls do not auto-join an outer transaction and can pressure small pools. Use [`participate(...)`](https://javadoc.pyranid.com/com/pyranid/Database.html#participate(com.pyranid.Transaction,com.pyranid.TransactionalOperation)) to run work on an existing transaction from another thread, and make sure participating workers complete before the owning transaction closure returns; coordinate with application primitives such as [`CompletableFuture::join`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/concurrent/CompletableFuture.html#join()), [`ExecutorService::awaitTermination`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/concurrent/ExecutorService.html#awaitTermination(long,java.util.concurrent.TimeUnit)), or [`CountDownLatch`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/concurrent/CountDownLatch.html).

[`fetchStream(...)`](https://javadoc.pyranid.com/com/pyranid/Query.html#fetchStream(java.lang.Class,java.util.function.Function)) streams rows only for as long as the callback is executing. Do not return the stream or consume it asynchronously after the callback returns. PostgreSQL streams automatically use an autocommit-disabled connection and a positive fetch size outside explicit Pyranid transactions; [`Query::customize(...)`](https://javadoc.pyranid.com/com/pyranid/Query.html#customize(com.pyranid.PreparedStatementCustomizer)) can override that fetch size. For other cursor-based drivers, follow the driver's transaction and fetch-size requirements.

Savepoints are stack-like on most drivers. Prefer [`Transaction::withSavepoint(...)`](https://javadoc.pyranid.com/com/pyranid/Transaction.html#withSavepoint(com.pyranid.TransactionalOperation)) for nested savepoint workflows, and avoid manual out-of-order [`rollback(Savepoint)`](https://javadoc.pyranid.com/com/pyranid/Transaction.html#rollback(java.sql.Savepoint)) / [`releaseSavepoint(Savepoint)`](https://javadoc.pyranid.com/com/pyranid/Transaction.html#releaseSavepoint(java.sql.Savepoint)) calls unless your driver documents the behavior you need.

Pyranid uses JDBC `executeLargeUpdate(...)` / `executeLargeBatch(...)` when supported and falls back to standard update APIs when the driver reports lack of support. That support decision is cached per `Database` instance.

### Metrics Collection

Metrics collection is disabled by default. Configure a [`MetricsCollector`](https://javadoc.pyranid.com/com/pyranid/MetricsCollector.html) at build time to collect statement, transaction, connection, savepoint, stream, and post-transaction counters.

```java
MetricsCollector metricsCollector = MetricsCollector.inMemoryInstance();

Database database = Database.withDataSource(dataSource)
  .metricsCollector(metricsCollector)
  .build();
```

Use `com.pyranid:pyranid-otel` for OpenTelemetry export:

```xml
<dependency>
  <groupId>com.pyranid</groupId>
  <artifactId>pyranid-otel</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

OTel support is optional; the core Pyranid jar remains dependency-free.

## About

Pyranid was created by [Mark Allen](https://www.revetkn.com) and sponsored by [Transmogrify LLC](https://www.xmog.com) and [Revetware LLC](https://www.revetware.com).
