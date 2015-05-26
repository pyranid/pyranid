## Pyranid

#### What Is It?

A minimalist JDBC interface for modern Java applications.

#### Design Goals

* Simple
* Customizable
* Threadsafe
* No dependencies
* DI-friendly
* Java 8+

#### License

[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)

#### Maven Installation

```xml
<dependency>
  <groupId>com.pyranid</groupId>
  <artifactId>pyranid</artifactId>
  <version>1.0.2</version>
</dependency>
```

#### Direct Download

If you don't use Maven, you can drop [pyranid-1.0.2.jar](http://central.maven.org/maven2/com/pyranid/pyranid/1.0.2/pyranid-1.0.2.jar) directly into your project.  No other dependencies are required.

## Configuration

```java
// Minimal setup, uses defaults
Database database = Database.forDataSource(dataSource).build();

// Customized setup
Database customDatabase = Database.forDataSource(dataSource)
                                  .instanceProvider(new InstanceProvider() {
                                    @Override
                                    public <T> T provide(Class<T> instanceClass) {
                                      // You might have your DI framework vend resultset row instances
                                      return guiceInjector.getInstance(instanceClass);
                                    }
                                  })
                                  .resultSetMapper(new ResultSetMapper() {
                                    @Override
                                    public <T> T map(ResultSet rs, Class<T> resultClass) {
                                       // Do some custom mapping here
                                    }
                                  })
                                  .preparedStatementBinder(new PreparedStatementBinder() {
                                    @Override
                                    public void bind(PreparedStatement ps, List<Object> parameters) {
                                       // Do some custom binding here
                                    }
                                  })
                                  .statementLogger(new StatementLogger() {
                                    @Override
                                    public void log(StatementLog statementLog) {
                                      // Send log to whatever output sink you'd like
                                      out.println(statementLog);
                                    }
                                  }).build();
```

#### Obtaining a DataSource

Pyranid works with any ```DataSource``` implementation. If you have the freedom to choose, [HikariCP](https://github.com/brettwooldridge/HikariCP) is a great option.

```java
DataSource dataSource = new HikariDataSource(new HikariConfig() {
  {
    setJdbcUrl("jdbc:postgresql://localhost:5432/my-database");
    setUsername("example");
    setPassword("secret");
  }
});
```

## Queries

Suppose we have a custom ```Car``` like this:

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
Optional<Car> specificCar = database.queryForObject("SELECT * FROM car WHERE id = ?", Car.class, 123);

// Multiple cars
List<Car> blueCars = database.queryForList("SELECT * FROM car WHERE color = ?", Car.class, Color.BLUE);

// In addition to custom types, you can map to primitives and some JDK builtins out of the box.
// See 'ResultSet Mapping' section for details
Optional<UUID> id = database.queryForObject("SELECT id FROM widget LIMIT 1", UUID.class);
List<BigDecimal> balances = database.queryForList("SELECT balance FROM account", BigDecimal.class);
```

## Statements

```java
// General-purpose statement execution (CREATEs, UPDATEs, function calls...)
long updateCount = database.execute("UPDATE car SET color = ?", Color.RED);

// Statement execution that provides a value via RETURNING.
// Useful for INSERTs with autogenerated keys, among other things
Optional<UUID> id = database.executeReturning("INSERT INTO book VALUES (?) RETURNING id",
                                              UUID.class, "The Stranger");

// Batch operations can be more efficient than execution of discrete statements.
// Useful for inserting a lot of data at once
List<List<Object>> parameterGroups = new ArrayList<>();

// Blue car
parameterGroups.add(new ArrayList<Object>() {
  {
    add(123);
    add(Color.BLUE);
  }
});

// Red car
parameterGroups.add(new ArrayList<Object>() {
  {
    add(456);
    add(Color.RED);
  }
});

// Insert both cars
long[] updateCounts = database.executeBatch("INSERT INTO car VALUES (?,?)", parameterGroups);
```

## Transactions

#### Design goals

* Minimal closure-based API: rollback if exception bubbles out, commit at end of closure otherwise
* Standard data access APIs (```queryForObject()``` and friends) automatically participate in transactions
* No ```Connection``` is fetched from the ```DataSource``` until the first data access operation occurs
* Must be able to share a transaction across multiple threads

#### Basics

```java
// Any code that runs inside of the closure operates within the context of a transaction.
// Pyranid will set autocommit=false for the duration of the transaction if necessary
database.transaction(() -> {
  // Pull initial account balances
  BigDecimal balance1 = database.queryForObject("SELECT balance FROM account WHERE id = 1", 
                                                BigDecimal.class).get();
  BigDecimal balance2 = database.queryForObject("SELECT balance FROM account WHERE id = 2", 
                                                BigDecimal.class).get();
  
  // Debit one and credit the other 
  balance1 = balance1.subtract(amount);
  balance2 = balance2.add(amount);

  // Persist changes.
  // Extra credit: this is a good candidate for database.executeBatch()
  database.execute("UPDATE account SET balance = ? WHERE id = 1", balance1);
  database.execute("UPDATE account SET balance = ? WHERE id = 2", balance2);
});

// For convenience, transactional operations may return values
Optional<BigDecimal> newBalance = database.transaction(() -> {
  // Make some changes
  database.execute("UPDATE account SET balance = balance - 10 WHERE id = 1");
  database.execute("UPDATE account SET balance = balance + 10 WHERE id = 2");

  // Return the new value
  return database.queryForObject("SELECT balance FROM account WHERE id = 2", BigDecimal.class);
});
```

#### Context

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

#### Multi-threaded Transactions

Internally, ```Database``` manages a threadlocal stack of ```Transaction``` instances to simplify single-threaded usage.  Should you need to share the same transaction across multiple threads, use the ```participate()``` API.

```java
database.transaction(() -> {
  database.execute("UPDATE account SET balance = balance - 10 WHERE id = 1");

  // Get a handle to the current transaction
  Transaction transaction = database.currentTransaction().get();

  new Thread(() -> {
    // In a different thread and participating in the existing transaction.
    // No commit or rollback will occur when the closure completes, but if an 
    // exception bubbles out the transaction will be marked as rollback-only
    database.participate(transaction, () -> {
      database.execute("UPDATE account SET balance = balance + 10 WHERE id = 2");
    });
  }).run();

  // Wait a bit for the other thread to finish
  sleep(1000);
});
```

#### Rolling Back

```java
// Any exception that bubbles out will cause a rollback
database.transaction(() -> {
  database.execute("UPDATE account SET balance = balance - 10 WHERE id = 1");
  throw new IllegalStateException("Something's wrong!");
});

// You may mark a transaction as rollback-only, and it will roll back after the 
// closure execution has completed
database.transaction(() -> {
  database.execute("UPDATE account SET balance = balance - 10 WHERE id = 1");

  // Hmm...I changed my mind
  Transaction transaction = database.currentTransaction().get();
  transaction.setRollbackOnly(true);
});

// You may roll back to a savepoint
database.transaction(() -> {
  Transaction transaction = database.currentTransaction().get();
  Savepoint savepoint = transaction.createSavepoint();

  database.execute("UPDATE account SET balance = balance - 10 WHERE id = 1");

  // Hmm...I changed my mind
  transaction.rollback(savepoint);
});
```

#### Nesting

```java
// Each nested transaction is independent. There is no parent-child relationship
database.transaction(() -> {
  database.execute("UPDATE account SET balance = balance - 10 WHERE id = 1");

  // This transaction will commit
  database.transaction(() -> {
    database.execute("UPDATE account SET balance = balance + 10 WHERE id = 2");
  });

  // This transaction will not!
  throw new IllegalStateException("I should not have used nested transactions here...");
});
```

#### Isolation

```java
// You may specify the normal isolation levels per-transaction as needed:
// READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, and SERIALIZABLE.
// If not specified, DEFAULT is assumed (whatever your DBMS prefers)
database.transaction(TransactionIsolation.SERIALIZABLE, () -> {
  database.execute("UPDATE account SET balance = balance - 10 WHERE id = 1");
  database.execute("UPDATE account SET balance = balance + 10 WHERE id = 2");
});
```

## ResultSet Mapping

The ```DefaultResultSetMapper``` supports user-defined types that follow the JavaBean getter/setter conventions, primitives, and some additional common JDK types.

#### User-defined Types

By default, database column names are assumed to be separated by ```_``` characters and are mapped to their camel-case equivalent.  For example:

```java
class Car {
  Long carId;
  String colorName;
  
  // Use this annotation to specify variants if the field name doesn't match the column name
  @DatabaseColumn({"systok", "sys_tok"})
  UUID systemToken;

  Long getCarId() { return this.carId; }
  void setCarId(Long carId) { this.carId = carId; }

  String getColorName() { return this.colorName; }
  void setColorName(String colorName) { this.colorName = colorName; }

  UUID getSystemToken() { return this.systemToken; }
  void setSystemToken(UUID systemToken) { this.systemToken = systemToken; }
}

Car car = database.queryForObject("SELECT car_id, color_name, systok FROM car LIMIT 1", Car.class).get();

// Output might be "Car ID is 123 and color name is blue. Token is d73c523a-8344-44ef-819c-40467662d619"
out.println(format("Car ID is %s and color name is %s. Token is %s",
                   car.getCarId(), car.getColorName(), car.getSystemToken()));

// Column names will work with wildcard queries as well
car = database.queryForObject("SELECT * FROM car LIMIT 1", Car.class).get();

// Column aliases work too
car = database.queryForObject("SELECT some_id AS car_id, some_color AS color_name FROM car LIMIT 1",
                              Car.class).get();
```

#### Supported Primitives

* ```Byte```
* ```Short```
* ```Integer```
* ```Long```
* ```Float```
* ```Double```
* ```Boolean```
* ```Char```
* ```String```
* ```byte[]```

#### Supported JDK Types

* ```Enum<E>```
* ```UUID```
* ```BigDecimal```
* ```BigInteger```
* ```Date```
* ```Instant```
* ```LocalDate``` for ```DATE```
* ```LocalTime``` for ```TIME```
* ```LocalDateTime``` for ```TIMESTAMP```
* ```OffsetTime``` for ```TIME WITH TIMEZONE```
* ```OffsetDateTime``` for ```TIMESTAMP WITH TIMEZONE```
* ```ZoneId```
* ```TimeZone```

## Error Handling

In general, a runtime ```DatabaseException``` will be thrown when errors occur.  Often this will wrap the checked ```java.sql.SQLException```.

For convenience, ```DatabaseException``` exposes two additional properties, which are only populated if provided by the underlying ```java.sql.SQLException```:

* ```errorCode``` (optional)
* ```sqlState``` (optional)

#### Practical Application

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
    // The 23505 code is specific to Postgres, other DBMSes will be different.
    // See all Postgres codes at http://www.postgresql.org/docs/9.4/static/errcodes-appendix.html
    if("23505".equals(e.sqlState().orElse(null)) {
      out.println(format("The %s award was already given to account ID %s", AwardType.BIG, accountId)); 
      // Puts transaction back in good state (prior to constraint violation)
      transaction.rollback(savepoint);
    } else {      
      // There must have been some other problem
      throw e;
    }
  }
});
```

## Logging and Diagnostics

#### StatementLogger

You may customize your ```Database``` with a ```StatementLogger```.

```java
Database database = Database.forDataSource(dataSource)
                            .statementLogger(new StatementLogger() {
                              @Override
                              public void log(StatementLog statementLog) {
                                // Do anything you'd like here
                                out.println(statementLog);
                              }
                            }).build();
```

```StatementLog``` instances give you access to the following for each SQL statement executed.  All time values are in nanoseconds.

* ```sql```
* ```parameters```
* ```connectionAcquisitionTime``` (optional)
* ```preparationTime``` (optional)
* ```executionTime``` (optional)
* ```resultSetMappingTime``` (optional)
* ```batchSize``` (optional)
* ```exception``` (optional)

Given this query:

```java
Optional<Car> car = database.queryForObject("SELECT * FROM car WHERE color = ?", Car.class, Color.BLUE);
```

The log output for ```DefaultStatementLogger``` might look like:

```
SELECT * FROM car WHERE color = ?
Parameters: 'BLUE'
0.04ms acquiring connection, 0.03ms preparing statement, 0.82ms executing statement, 0.40ms processing resultset
````

#### java.util.Logging

Pyranid uses ```java.util.Logging``` internally.  The usual way to hook into this is with [SLF4J](http://slf4j.org), which can funnel all the different logging mechanisms in your app through a single one, normally [Logback](http://logback.qos.ch).  Your Maven configuration might look like this:

```xml
<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>jul-to-slf4j</artifactId>
  <version>1.7.7</version>
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

Note: ```SLF4JBridgeHandler``` can impact performance.  You can mitigate that with Logback's ```LevelChangePropagator``` configuration option [as described here](http://logback.qos.ch/manual/configuration.html#LevelChangePropagator).

## TODOs

* Formalize BLOB/CLOB handling
* Work around more Oracle quirks

## About

Pyranid was created by [Mark Allen](http://revetkn.com) and sponsored by [Transmogrify, LLC.](http://xmog.com)

Development was aided by

* [SomaFM](http://somafm.com)
* [No Fun at All](https://www.youtube.com/watch?v=O6V2sin-buE)
* [Street Dogs](https://www.youtube.com/watch?v=bIx4Tezuklk)