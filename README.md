<a href="https://www.pyranid.com">
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="https://cdn.pyranid.com/pyranid-gh-logo-dark-v3.png">
        <img alt="Pyranid" src="https://cdn.pyranid.com/pyranid-gh-logo-light-v3.png" width="300" height="94">
    </picture>
</a>

### What Is It?

A zero-dependency JDBC interface for modern Java applications, powering production systems since 2015.

Pyranid takes care of boilerplate and lets you focus on writing and thinking in SQL.  It is not an ORM.

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

* [Soklet](https://www.soklet.com) - DI-friendly HTTP 1.1 server that supports [JEP 444 Virtual Threads](https://openjdk.org/jeps/444)
* [Lokalized](https://www.lokalized.com) - natural-sounding translations (i18n) via expression language

### Maven Installation

Java 16+

```xml
<dependency>
  <groupId>com.pyranid</groupId>
  <artifactId>pyranid</artifactId>
  <version>2.0.0</version>
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

If you don't use Maven, you can drop [pyranid-2.0.0.jar](https://repo1.maven.org/maven2/com/pyranid/pyranid/2.0.0/pyranid-2.0.0.jar) directly into your project.  No other dependencies are required.

## Configuration

### Minimal setup

```java
// Create a Database backed by a DataSource
DataSource dataSource = obtainDataSource();
Database database = Database.forDataSource(dataSource).build();
```

### Customized setup

```java
// Useful if your JVM's default timezone doesn't match your Database's default timezone
ZoneId timeZone = ZoneId.of("UTC");

// Controls how Pyranid creates instances of objects that represent ResultSet rows
InstanceProvider instanceProvider = new DefaultInstanceProvider() {
  @Override
  @Nonnull
  public <T> T provide(@Nonnull StatementContext<T> statementContext,
                       @Nonnull Class<T> instanceType) {
    // You might have your DI framework vend regular object instances
    return guiceInjector.getInstance(instanceType);
  }
  
  @Override
  @Nonnull
  public <T extends Record> T provideRecord(@Nonnull StatementContext<T> statementContext,
                                            @Nonnull Class<T> recordType,
                                            @Nullable Object... initargs) {
    // If you use Record types, customize their instantiation here.
    // Default implementation will use the canonical constructor
    return super.provideRecord(statementContext, recordType, initargs);
  }
};

// Copies data from a ResultSet row to an instance of the specified type
ResultSetMapper resultSetMapper = new DefaultResultSetMapper(timeZone) {
  @Nonnull
  @Override
  public <T> Optional<T> map(@Nonnull StatementContext<T> statementContext,
                             @Nonnull ResultSet resultSet,
                             @Nonnull Class<T> resultSetRowType,
                             @Nonnull InstanceProvider instanceProvider) {
    // Customize mapping here if needed
    return super.map(statementContext, resultSet, resultSetRowType, instanceProvider);
  }
};

// Binds parameters to a SQL PreparedStatement
PreparedStatementBinder preparedStatementBinder = new DefaultPreparedStatementBinder(timeZone) {
  @Override
  public <T> void bind(@Nonnull StatementContext<T> statementContext,
                       @Nonnull PreparedStatement preparedStatement,
                       @Nonnull List<Object> parameters) {
    // Customize parameter binding here if needed
    super.bind(statementContext, preparedStatement, parameters);
  }
};

// Optionally logs SQL statements
StatementLogger statementLogger = new StatementLogger() {
  @Override
  public void log(@Nonnull StatementLog statementLog) {
    // Send to whatever output sink you'd like
    out.println(statementLog);
  }
};

Database customDatabase = Database.forDataSource(dataSource)
  .timeZone(timeZone)
  .instanceProvider(instanceProvider)
  .resultSetMapper(resultSetMapper)
  .preparedStatementBinder(preparedStatementBinder)
  .statementLogger(statementLogger)
  .build();
```

### Obtaining a DataSource

Pyranid works with any [`DataSource`](https://docs.oracle.com/en/java/javase/20/docs/api/java.sql/javax/sql/DataSource.html) implementation. If you have the freedom to choose, [HikariCP](https://github.com/brettwooldridge/HikariCP) (application-level) and [PgBouncer](https://www.pgbouncer.org/) (external; Postgres-only) are good options.

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
Optional<Car> car = database.queryForObject("SELECT * FROM car LIMIT 1", Car.class);

// A single car, passing prepared statement parameters via varargs
Optional<Car> specificCar = database.queryForObject("SELECT * FROM car WHERE id=?", Car.class, 123);

// Multiple cars
List<Car> blueCars = database.queryForList("SELECT * FROM car WHERE color=?", Car.class, Color.BLUE);

// In addition to custom types, you can map to primitives and many JDK builtins out of the box.
// See 'ResultSet Mapping' section for details
Optional<UUID> id = database.queryForObject("SELECT id FROM widget LIMIT 1", UUID.class);
List<BigDecimal> balances = database.queryForList("SELECT balance FROM account", BigDecimal.class);
```

[`Record`](https://openjdk.org/jeps/395) types are also supported:

```java
record Employee(String name, @DatabaseColumn("email") String emailAddress) {}

Optional<Employee> employee = database.queryForObject("""
  SELECT *
  FROM employee
  WHERE email=?
  """, Employee.class, "name@example.com");
```

By default, Pyranid will invoke the canonical constructor for `Record` types. 

## Statements

```java
// General-purpose DML statement execution (INSERTs, UPDATEs, DELETEs, functions...)
long updateCount = database.execute("UPDATE car SET color=?", Color.RED);

// Return single or multiple objects via the SQL RETURNING clause for DML statements.
// Applicable for Postgres and Oracle.  SQL Sever uses an OUTPUT clause
Optional<BigInteger> insertedCarId = database.executeForObject("""
  INSERT INTO car (id, color)
  VALUES (nextval('car_seq'), ?)
  RETURNING id
  """, BigInteger.class, Color.GREEN);

List<Car> repaintedCars = database.executeForList("""
  UPDATE car
  SET color=?
  WHERE color=?
  RETURNING *
  """, Car.class, Color.GREEN, Color.BLUE);

// Batch operations can be more efficient than execution of discrete statements.
// Useful for inserting a lot of data at once
List<List<Object>> parameterGroups = List.of(
  List.of(123, Color.BLUE),
  List.of(456, Color.RED)
);

// Insert both cars
List<Long> updateCounts = database.executeBatch("INSERT INTO car VALUES (?,?)", parameterGroups);
```

Pyranid will automatically determine if your JDBC driver supports "large" updates and batch operations
and uses them if available.

## Transactions

### Design goals

* Closure-based API: rollback if exception bubbles out, commit at end of closure otherwise
* Data access APIs (e.g. [`Database::queryForObject`](https://pyranid.com/javadoc/com/pyranid/Database.html#queryForObject(java.lang.String,java.lang.Class,java.lang.Object...)) and friends) automatically participate in transactions
* No [`Connection`](https://docs.oracle.com/en/java/javase/20/docs/api/java.sql/java/sql/Connection.html) is fetched from the [`DataSource`](https://docs.oracle.com/en/java/javase/20/docs/api/java.sql/javax/sql/DataSource.html) until the first data access operation occurs
* Must be able to share a transaction across multiple threads

### Basics

```java
// Any code that runs inside of the closure operates within the context of a transaction.
// Pyranid will set autocommit=false for the duration of the transaction if necessary
database.transaction(() -> {
  // Pull initial account balances
  BigDecimal balance1 = database.queryForObject("SELECT balance FROM account WHERE id=1", 
                                                BigDecimal.class).get();
  BigDecimal balance2 = database.queryForObject("SELECT balance FROM account WHERE id=2", 
                                                BigDecimal.class).get();
  
  // Debit one and credit the other 
  balance1 = balance1.subtract(amount);
  balance2 = balance2.add(amount);

  // Persist changes.
  // Extra credit: this is a good candidate for database.executeBatch()
  database.execute("UPDATE account SET balance=? WHERE id=1", balance1);
  database.execute("UPDATE account SET balance=? WHERE id=2", balance2);
});

// For convenience, transactional operations may return values
Optional<BigDecimal> newBalance = database.transaction(() -> {
  // Make some changes
  database.execute("UPDATE account SET balance=balance - 10 WHERE id=1");
  database.execute("UPDATE account SET balance=balance + 10 WHERE id=2");

  // Return the new value
  return database.queryForObject("SELECT balance FROM account WHERE id=2", BigDecimal.class);
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

### Multi-threaded Transactions

Internally, Database manages a threadlocal stack of [`Transaction`](https://pyranid.com/javadoc/com/pyranid/Transaction.html) instances to simplify single-threaded usage.  Should you need to share the same transaction across multiple threads, use the [`Database::participate`](https://pyranid.com/javadoc/com/pyranid/Database.html#participate(com.pyranid.Transaction,com.pyranid.TransactionalOperation)) API.

```java
database.transaction(() -> {
  database.execute("UPDATE account SET balance=balance - 10 WHERE id=1");

  // Get a handle to the current transaction
  Transaction transaction = database.currentTransaction().get();

  new Thread(() -> {
    // In a different thread and participating in the existing transaction.
    // No commit or rollback will occur when the closure completes, but if an 
    // exception bubbles out the transaction will be marked as rollback-only
    database.participate(transaction, () -> {
      database.execute("UPDATE account SET balance=balance + 10 WHERE id=2");
    });
  }).run();

  // Wait a bit for the other thread to finish
  // (Don't do this in real systems)
  sleep(1000);
});
```

### Rolling Back

```java
// Any exception that bubbles out will cause a rollback
database.transaction(() -> {
  database.execute("UPDATE account SET balance=balance - 10 WHERE id=1");
  throw new IllegalStateException("Something's wrong!");
});

// You may mark a transaction as rollback-only, and it will roll back after the 
// closure execution has completed
database.transaction(() -> {
  database.execute("UPDATE account SET balance=balance - 10 WHERE id=1");

  // Hmm...I changed my mind
  Transaction transaction = database.currentTransaction().get();
  transaction.setRollbackOnly(true);
});

// You may roll back to a savepoint
database.transaction(() -> {
  Transaction transaction = database.currentTransaction().get();
  Savepoint savepoint = transaction.createSavepoint();

  database.execute("UPDATE account SET balance=balance - 10 WHERE id=1");

  // Hmm...I changed my mind
  transaction.rollback(savepoint);
});
```

### Nesting

```java
// Each nested transaction is independent. There is no parent-child relationship
database.transaction(() -> {
  database.execute("UPDATE account SET balance=balance - 10 WHERE id=1");

  // This transaction will commit
  database.transaction(() -> {
    database.execute("UPDATE account SET balance=balance + 10 WHERE id=2");
  });

  // This transaction will not!
  throw new IllegalStateException("I should not have used nested transactions here...");
});
```

### Isolation

```java
// You may specify the normal isolation levels per-transaction as needed:
// READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, and SERIALIZABLE.
// If not specified, DEFAULT is assumed (whatever your DBMS prefers)
database.transaction(TransactionIsolation.SERIALIZABLE, () -> {
  database.execute("UPDATE account SET balance=balance - 10 WHERE id=1");
  database.execute("UPDATE account SET balance=balance + 10 WHERE id=2");
});
```

### Post-Transaction Operations

It is useful to be able to schedule code to run after a transaction has been fully committed or rolled back.  Often, transaction management happens at a higher layer of code than business logic (e.g. a transaction-per-web-request pattern), so it is helpful to have a mechanism to "warp" local logic out to the higher layer.

Without this, you might run into subtle bugs like

* Write to database
* Send out notifications of system state change
* (Sometime later) transaction is rolled back
* Notification consumers are in an inconsistent state because they were notified of a change that was reversed by the rollback

```java
// Business logic
class EmployeeService {
  public void giveEveryoneRaises() {
    database.execute("UPDATE employee SET salary=salary * 2");
    payrollSystem.startLengthyWarmupProcess();

    // Only send emails after the current transaction ends
    database.currentTransaction().get().addPostTransactionOperation((transactionResult) -> {
      if(transactionResult == TransactionResult.COMMITTED) {
        // Successful commit? email everyone with the good news
        for(Employee employee : findAllEmployees())
          sendCongratulationsEmail(employee);
      } else if(transactionResult == TransactionResult.ROLLED_BACK) {
        // Rolled back? We can clean up
        payrollSystem.cancelLengthyWarmupProcess();	
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

## ResultSet Mapping

The [`DefaultResultSetMapper`](https://pyranid.com/javadoc/com/pyranid/DefaultResultSetMapper.html) supports user-defined types that follow the JavaBean getter/setter conventions, primitives, and some additional common JDK types.

[`Record`](https://openjdk.org/jeps/395) types are also supported.

### User-defined Types

In the case of user-defined types and Records, [`DefaultResultSetMapper`](https://pyranid.com/javadoc/com/pyranid/DefaultResultSetMapper.html) examines the names of columns in the [`ResultSet`](https://docs.oracle.com/en/java/javase/20/docs/api/java.sql/javax/sql/ResultSet.html) and matches them to corresponding fields via reflection.  The [`@DatabaseColumn`](https://pyranid.com/javadoc/com/pyranid/DatabaseColumn.html) annotation allows per-field customization of mapping behavior.

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

Car car = database.queryForObject("SELECT car_id, color, systok FROM car LIMIT 1", Car.class).get();

// Output might be "Car ID is 123 and color is BLUE. Token is d73c523a-8344-44ef-819c-40467662d619"
out.printf("Car ID is %s and color is %s. Token is %s\n",
                   car.getCarId(), car.getColor(), car.getSystemToken());

// Column names will work with wildcard queries as well
car = database.queryForObject("SELECT * FROM car LIMIT 1", Car.class).get();

// Column aliases work too
car = database.queryForObject("SELECT some_id AS car_id, some_color AS color FROM car LIMIT 1",
                              Car.class).get();
```

### Supported Primitives

* `Byte`
* `Short`
* `Integer`
* `Long`
* `Float`
* `Double`
* `Boolean`
* `Char`
* `String`
* `byte[]`

### Supported JDK Types

* `Enum<E>`
* `UUID`
* `BigDecimal`
* `BigInteger`
* `Date`
* `Instant`
* `LocalDate` for `DATE`
* `LocalTime` for `TIME`
* `LocalDateTime` for `TIMESTAMP`
* `OffsetTime` for `TIME WITH TIMEZONE`
* `OffsetDateTime` for `TIMESTAMP WITH TIMEZONE`
* `ZoneId`
* `TimeZone`
* `Locale` (IETF BCP 47 "language tag" format)

### Other Types

* Store Postgres JSONB data using a SQL cast of `String`, e.g. `CAST(? AS JSONB)`. Retrieve JSONB data using `String`

### Kotlin Types

#### Data Class

Kotlin data class result set mapping is possible through the primary constructor of the data class.

* Nullable and non-null columns are supported.
* Default parameters are supported.
* Data classes support the same list of JDK types as above
* Extension functions for direct `KClass` support are provided

```kotlin
data class Car(carId: UUID, color: Color = Color.BLUE, ownerId: String?)

val cars = database.queryForList("SELECT * FROM cars", Car::class)
```

When query parameters are supplied as a list they must be flattened first, either as separate lists or one big list:

```kotlin
val cars = database.queryForList("SELECT * FROM cars WHERE car_id IN (?, ?) LIMIT ?",
                                 Car::class,
                                 car1Id, car2Id, 10)

val cars = database.queryForList("SELECT * FROM cars WHERE car_id IN (?, ?) LIMIT ?",
                                 Car::class,
                                 *listOf(car1Id, car2Id).toTypedArray(), 10)

val cars = database.queryForList("SELECT * FROM cars WHERE car_id IN (?, ?) LIMIT ?",
                                 Car::class,
                                 *listOf(car1Id, car2Id, 10).toTypedArray())
```

## Error Handling

In general, a runtime [`DatabaseException`](https://pyranid.com/javadoc/com/pyranid/DatabaseException.html) will be thrown when errors occur.  Often this will wrap the checked [`java.sql.SQLException`](https://docs.oracle.com/en/java/javase/20/docs/api/java.sql/java/sql/SQLException.html).

For convenience, [`DatabaseException`](https://pyranid.com/javadoc/com/pyranid/DatabaseException.html) exposes additional properties, which are populated if provided by the underlying [`java.sql.SQLException`](https://docs.oracle.com/en/java/javase/20/docs/api/java.sql/java/sql/SQLException.html):

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

Extended property support for Oracle and MySQL is planned.

### Practical Application

Here we detect if a specific constraint was violated by examining [`DatabaseException`](https://pyranid.com/javadoc/com/pyranid/DatabaseException.html).
We then handle that case specially by rolling back to a known-good savepoint.

```java
// Gives someone at most one big award
database.transaction(() -> {
  Transaction transaction = database.currentTransaction().get();
  Savepoint savepoint = transaction.createSavepoint();

  try {
    // We don't want to give someone the same award twice!
    // Let the DBMS worry about constraint checking to avoid race conditions
    database.execute("INSERT INTO account_award (account_id, award_type) VALUES (?,?)",
                     accountId, AwardType.BIG);
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

### StatementLogger

You may customize your [`Database`](https://pyranid.com/javadoc/com/pyranid/Database.html) with a [`StatementLogger`](https://pyranid.com/javadoc/com/pyranid/StatementLogger.html).

Examples of usage include:

* Writing queries and timing information to your logging system
* Picking out slow queries for special logging/reporting
* Collecting a set of queries executed across a unit of work for bulk analysis (e.g. a [`ThreadLocal`](https://docs.oracle.com/en/java/javase/20/docs/api/java.base/java/lang/ThreadLocal.html) scoped to a single web request)

```java
Database database = Database.forDataSource(dataSource)
  .statementLogger(new StatementLogger() {
    @Override
    public void log(@Nonnull StatementLog statementLog) {
      Duration SLOW_QUERY_THRESHOLD = Duration.ofMillis(500);

      if(statementLog.getTotalDuration().compareTo(SLOW_QUERY_THRESHOLD) > 0)
        out.printf("Slow query: %s\n", statementLog);
    }
  }).build();
```

[`StatementLog`](https://pyranid.com/javadoc/com/pyranid/StatementLog.html) instances give you access to the following for each SQL statement executed:

* `statementContext`
* `connectionAcquisitionDuration` (optional)
* `preparationDuration` (optional)
* `executionDuration` (optional)
* `resultSetMappingDuration` (optional)
* `batchSize` (optional)
* `exception` (optional)

### Statement Identifiers

For any data access method that accepts a `sql` parameter, you may alternatively provide a [`Statement`](https://pyranid.com/javadoc/com/pyranid/Statement.html), which permits you to specify an arbitrary identifier for the SQL.

If you do not explicitly provide a [`Statement`](https://pyranid.com/javadoc/com/pyranid/Statement.html), Pyranid will create one for you and generate its own identifier.

```java
// Regular SQL
database.queryForObject("SELECT * FROM car LIMIT 1", Car.class);

// SQL in a Statement
database.queryForObject(Statement.of("random-car", "SELECT * FROM car LIMIT 1"), Car.class);
```

This is useful for tagging queries that should be handled specially. Some examples are:

* Marking a query as "hot" so we don't pollute logs with it
* Marking a query as "known to be slow" so we don't flag slow query alerts for it
* Your [`InstanceProvider`](https://pyranid.com/javadoc/com/pyranid/InstanceProvider.html) might provide custom instances based on resultset data

```java
// Custom tagging system
enum QueryTag {
  HOT_QUERY,
  SLOW_QUERY
};

// This query fires every 3 seconds - let's mark it HOT_QUERY so we know not to log it.
Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
  database.transaction(() -> {
    List<Message> messages = database.queryForList(Statement.of(QueryTag.HOT_QUERY, """
      SELECT *
      FROM message_queue
      WHERE message_status_id=?
      LIMIT ?
      FOR UPDATE
      SKIP LOCKED
    """), Message.class, MessageStatusId.UNPROCESSED, BATCH_SIZE);

    // Implementation not shown
    processMessages(messages);
  });
}, 0, 3, TimeUnit.SECONDS);
```

A corresponding [`Database`](https://pyranid.com/javadoc/com/pyranid/Database.html) setup:

```java
// Ensure our StatementLogger implementation takes HOT_QUERY into account 
Database database = Database.forDataSource(dataSource)
  .statementLogger(new StatementLogger() {
    @Override
    public void log(@Nonnull StatementLog statementLog) {
      // Log everything except HOT_QUERY
      if(statementLog.getStatementContext().getStatement().getId() != HOT_QUERY)
        out.println(statementLog);
    }
  }).build();
```

### java.util.Logging

Pyranid uses [`java.util.Logging`](https://docs.oracle.com/javase/8/docs/api/java/util/logging/package-summary.html) internally.  The usual way to hook into this is with [SLF4J](http://slf4j.org), which can funnel all the different logging mechanisms in your app through a single one, normally [Logback](http://logback.qos.ch).  Your Maven configuration might look like this:

```xml
<dependency>
  <groupId>ch.qos.logback</groupId>
  <artifactId>logback-classic</artifactId>
  <version>1.4.8</version>
</dependency>

<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>jul-to-slf4j</artifactId>
  <version>2.0.7</version>
</dependency>
```

You might have code like this which runs at startup:

```java
// Bridge all java.util.logging to SLF4J
java.util.logging.Logger rootLogger = java.util.logging.LogManager.getLogManager().getLogger("");
for (Handler handler : rootLogger.getHandlers())
  rootLogger.removeHandler(handler);

SLF4JBridgeHandler.install();
```

Don't forget to uninstall the bridge at shutdown time:

```java
// Sometime later
SLF4JBridgeHandler.uninstall();
```

Note: [`SLF4JBridgeHandler`](https://www.slf4j.org/api/org/slf4j/bridge/SLF4JBridgeHandler.html) can impact performance.  You can mitigate that with Logback's [`LevelChangePropagator`](https://logback.qos.ch/apidocs/ch/qos/logback/classic/jul/LevelChangePropagator.html) configuration option [as described here](http://logback.qos.ch/manual/configuration.html#LevelChangePropagator).

## TODOs

* Formalize BLOB/CLOB handling
* Work around more Oracle quirks

## About

Pyranid was created by [Mark Allen](https://www.revetkn.com) and sponsored by [Transmogrify LLC](https://www.xmog.com) and [Revetware LLC](https://www.revetware.com).