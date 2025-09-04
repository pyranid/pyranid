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
  <version>3.0.0</version>
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

If you don't use Maven, you can drop [pyranid-3.0.0.jar](https://repo1.maven.org/maven2/com/pyranid/pyranid/3.0.0/pyranid-3.0.0.jar) directly into your project.  No other dependencies are required.

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

// Handles copying data from a ResultSet row to an instance of the specified type.
// Plan caching trades memory for faster mapping of wide ResultSets.
// Normalization locale should match the language of your database tables/column names.
// CustomColumnMappers supply "surgical" overrides to handle custom types
ResultSetMapper resultSetMapper = ResultSetMapper.withPlanCachingEnabled(false)
  .normalizationLocale(Locale.forLanguageTag("pt-BR"))
  .customColumnMappers(List.of(new CustomColumnMapper() {
    @Nonnull
    @Override
    public Boolean appliesTo(@Nonnull TargetType targetType) {
      // Can also apply to parameterized types, e.g.
      // targetType.matchesParameterizedType(List.class, UUID.class) for List<UUID>
      return targetType.matchesClass(Money.class);
    }

    @Nonnull
    @Override
    public MappingResult map(
      @Nonnull StatementContext<?> statementContext,
      @Nonnull ResultSet resultSet,
      @Nonnull Object resultSetValue,
      @Nonnull TargetType targetType,
      @Nonnull Integer columnIndex,
      @Nullable String columnLabel,
      @Nonnull InstanceProvider instanceProvider
    ) {
      // Convert the ResultSet column's value to the "appliesTo" Java type.
      // Don't need null checks - this method is only invoked when the value is non-null
      String moneyAsString = resultSetValue.toString();
      Money money = Money.parse(moneyAsString);

      // Or return MappingResult.fallback() to indicate "I don't want to do custom mapping"
      // and Pyranid will fall back to the registered ResultSetMapper's mapping behavior
      return MappingResult.of(money);
    }
  }))
  .build();

// Binds parameters to a SQL PreparedStatement.
// CustomParameterBinders supply "surgical" overrides to handle custom types.
// Here, we transform Money instances into a DB-friendly string representation 
PreparedStatementBinder preparedStatementBinder = PreparedStatementBinder.withCustomParameterBinders(List.of(
  new CustomParameterBinder() {
    @Nonnull
    @Override
    public Boolean appliesTo(@Nonnull TargetType targetType) {
      return targetType.matchesClass(Money.class);
    }		
			
    @Nonnull
    @Override
    public BindingResult bind(
      @Nonnull StatementContext<?> statementContext, 
      @Nonnull PreparedStatement preparedStatement,
      @Nonnull Integer parameterIndex,
      @Nonnull Object parameter
    ) throws SQLException {
      // Convert Money to a string representation for binding.
      // Don't need null checks - this method is only invoked when the value is non-null
      Money money = (Money) parameter;
      String moneyAsString = money.stringValue();
			
      // Bind to the PreparedStatement
      preparedStatement.setString(parameterIndex, moneyAsString);

      // Or return BindingResult.fallback() to indicate "I don't want to do custom binding"
      // and Pyranid will fall back to the registered PreparedStatementBinder's binding behavior
      return BindingResult.handled();
    }
  }  
));

// Optionally logs SQL statements
StatementLogger statementLogger = new StatementLogger() {
  @Override
  public void log(@Nonnull StatementLog statementLog) {
    // Send to whatever output sink you'd like
    out.println(statementLog);
  }
};

// Useful if your JVM's default timezone doesn't match your Database's default timezone
ZoneId timeZone = ZoneId.of("UTC");

Database customDatabase = Database.withDataSource(dataSource)
  .timeZone(timeZone)
  .instanceProvider(instanceProvider)
  .resultSetMapper(resultSetMapper)
  .preparedStatementBinder(preparedStatementBinder)
  .statementLogger(statementLogger)
  .build();
```

### Obtaining a DataSource

Pyranid works with any [`DataSource`](https://docs.oracle.com/en/java/javase/21/docs/api/java.sql/javax/sql/DataSource.html) implementation. If you have the freedom to choose, [HikariCP](https://github.com/brettwooldridge/HikariCP) (application-level) and [PgBouncer](https://www.pgbouncer.org/) (external; Postgres-only) are good options.

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
* Data access APIs (e.g. [`Database::queryForObject`](https://javadoc.pyranid.com/com/pyranid/Database.html#queryForObject(java.lang.String,java.lang.Class,java.lang.Object...)) and friends) automatically participate in transactions
* No [`Connection`](https://docs.oracle.com/en/java/javase/21/docs/api/java.sql/java/sql/Connection.html) is fetched from the [`DataSource`](https://docs.oracle.com/en/java/javase/21/docs/api/java.sql/javax/sql/DataSource.html) until the first data access operation occurs
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

Internally, Database manages a threadlocal stack of [`Transaction`](https://javadoc.pyranid.com/com/pyranid/Transaction.html) instances to simplify single-threaded usage.  Should you need to share the same transaction across multiple threads, use the [`Database::participate`](https://javadoc.pyranid.com/com/pyranid/Database.html#participate(com.pyranid.Transaction,com.pyranid.TransactionalOperation)) API.

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

The out-of-the-box [`ResultSetMapper`](https://javadoc.pyranid.com/com/pyranid/ResultSetMapper.html) implementation supports user-defined types that follow the JavaBean getter/setter conventions, primitives, and some additional common JDK types.

[`Record`](https://openjdk.org/jeps/395) types are also supported.

### User-defined Types

In the case of user-defined types and Records, the standard [`ResultSetMapper`](https://javadoc.pyranid.com/com/pyranid/ResultSetMapper.html) examines the names of columns in the [`ResultSet`](https://docs.oracle.com/en/java/javase/21/docs/api/java.sql/javax/sql/ResultSet.html) and matches them to corresponding fields via reflection.  The [`@DatabaseColumn`](https://javadoc.pyranid.com/com/pyranid/DatabaseColumn.html) annotation allows per-field customization of mapping behavior.

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
* `Character`
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

### Custom Mapping

Fine-grained control of mapping is supported by registering [`CustomColumnMapper`](https://javadoc.pyranid.com/com/pyranid/CustomColumnMapper.html) instances.  For example, you might want to "inflate" a `JSONB` column into a Java type:

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
  @Nonnull
  @Override
  public Boolean appliesTo(@Nonnull TargetType targetType) {
    return targetType.matchesClass(MySpecialType.class);
  }

  @Nonnull
  @Override
  public MappingResult map(
    @Nonnull StatementContext<?> statementContext,
    @Nonnull ResultSet resultSet,
    @Nonnull Object resultSetValue,
    @Nonnull TargetType targetType,
    @Nonnull Integer columnIndex,
    @Nullable String columnLabel,
    @Nonnull InstanceProvider instanceProvider
  ) {
    String json = resultSetValue.toString();
    MySpecialType mySpecialType = GSON.fromJson(json, MySpecialType.class);

    // Or return MappingResult.fallback() to indicate "I don't want to do custom mapping"
    // and Pyranid will fall back to the registered ResultSetMapper's mapping behavior
    return MappingResult.of(mySpecialType);
  }
  }))
  .build();

// Construct your database with the custom mapper
Database database = Database.withDataSource(...)
  .resultSetMapper(resultSetMapper)
  .build();
```

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

## Parameter Binding

The out-of-the-box [`PreparedStatementBinder`](https://javadoc.pyranid.com/com/pyranid/PreparedStatementBinder.html) implementation supports binding common JDK types to `?` placeholders and generally "just works" as you would expect.

For example:

```java
UUID departmentId = ...;

List<Employee> = database.queryForList("""
  SELECT *
  FROM employee
  WHERE department_id=?
""", Employee.class, departmentId);

database.execute("""
  INSERT INTO account_award (
    account_id, 
    award_type
  ) VALUES (?,?)
  """, accountId, AwardType.BIG);
```

### Special Parameters

Special support is provided for JSON/JSONB, vector, and SQL ARRAY parameters.

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

database.execute("INSERT INTO example (data) VALUES (?)",
  Parameters.json(json)
);
```

By default, Pyranid will use your database's binary JSON format if supported and fall back to a text representation otherwise.

If you want to force text storage (e.g. if whitespace is important), specify a binding preference like this:

```java
database.execute("INSERT INTO example (data) VALUES (?)",
  Parameters.json(json, BindingPreference.TEXT)
);
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

database.execute("INSERT INTO vector_embedding (embedding, content) VALUES (?,?)",
  Parameters.vectorOfDoubles(embedding), content
);
```

#### SQL ARRAY

Single-dimension array binding is supported out-of-the-box.

Supported methods:

* [`Parameters::arrayOf(String, E[])`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#arrayOf(java.lang.String,E%5B%5D))
* [`Parameters::arrayOf(String, List<E>)`](https://javadoc.pyranid.com/com/pyranid/Parameters.html#arrayOf(java.lang.String,java.util.List))

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
int[] vendorFlags = { 1, 2, 3 };
List<String> tags = List.of("alpha", "beta");

database.execute("""
  INSERT INTO product (
    name,
    vendor_flags,
    tags
  ) VALUES (?,?,?)
""", 
  name,
  Parameters.arrayOf("INTEGER", vendorFlags),
  Parameters.arrayOf("VARCHAR", tags)
);
```

If you need support for multidimensional array binding, implement a [`CustomParameterBinder`](https://javadoc.pyranid.com/com/pyranid/CustomParameterBinder.html) as outlined below. 

### Custom Parameters

You may register instances of [`CustomParameterBinder`](https://javadoc.pyranid.com/com/pyranid/CustomParameterBinder.html) to bind application-specific types to [`java.sql.PreparedStatement`](https://docs.oracle.com/en/java/javase/24/docs/api/java.sql/java/sql/PreparedStatement.html) however you like.

This allows you to use your objects as-is with Pyranid instead of sprinkling "convert this object to database format" code throughout your system.

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
    @Nonnull
    @Override
    public Boolean appliesTo(@Nonnull TargetType targetType) {
      return targetType.matchesClass(HexColor.class);
    }		
			
    @Nonnull
    @Override
    public BindingResult bind(
      @Nonnull StatementContext<?> statementContext, 
      @Nonnull PreparedStatement preparedStatement,
      @Nonnull Integer parameterIndex,
      @Nonnull Object parameter
    ) throws SQLException {
      HexColor hexColor = (HexColor) parameter; 

      // Bind to the PreparedStatement as a value like "6a5acd"
      preparedStatement.setString(parameterIndex, hexColor.toHexString());

      // Or return BindingResult.fallback() to indicate "I don't want to do custom binding"
      // and Pyranid will fall back to the registered PreparedStatementBinder's binding behavior
      return BindingResult.handled();
    }
  }  
));

Database database = Database.withDataSource(dataSource)
  .preparedStatementBinder(preparedStatementBinder)
  .build();
```

#### Standard Collections

Runtime binding of generic types is made difficult by type erasure.  For convenience, Pyranid offers special parameters that perform type capture for standard [`List<E>`](https://docs.oracle.com/en/java/javase/24/docs/api/java.base/java/util/List.html), [`Set<E>)`](https://docs.oracle.com/en/java/javase/24/docs/api/java.base/java/util/Set.html), and [`Map<K,V>)`](https://docs.oracle.com/en/java/javase/24/docs/api/java.base/java/util/Map.html) types:

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

database.execute("INSERT INTO t(v) VALUES (?)", Parameters.listOf(UUID.class, ids));
```

...would be handled by this custom binder, because `targetType.matchesParameterizedType(List.class, UUID.class)` returns `true` thanks to runtime type capturing:

```java
PreparedStatementBinder preparedStatementBinder = PreparedStatementBinder.withCustomParameterBinders(List.of(
  new CustomParameterBinder() {
    @Nonnull
    @Override
    public Boolean appliesTo(@Nonnull TargetType targetType) {
      // For Parameters::mapOf(Class<K>, Class<V>, Map<K,V>), you'd say:
      // matchesParameterizedType(Map.class, MyKey.class, MyValue.class)
      return targetType.matchesParameterizedType(List.class, UUID.class);
    }		
			
    @Nonnull
    @Override
    public BindingResult bind(
      @Nonnull StatementContext<?> statementContext, 
      @Nonnull PreparedStatement preparedStatement,
      @Nonnull Integer parameterIndex,
      @Nonnull Object parameter
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

## Error Handling

In general, a runtime [`DatabaseException`](https://javadoc.pyranid.com/com/pyranid/DatabaseException.html) will be thrown when errors occur.  Often this will wrap the checked [`java.sql.SQLException`](https://docs.oracle.com/en/java/javase/24/docs/api/java.sql/java/sql/SQLException.html).

For convenience, [`DatabaseException`](https://javadoc.pyranid.com/com/pyranid/DatabaseException.html) exposes additional properties, which are populated if provided by the underlying [`java.sql.SQLException`](https://docs.oracle.com/en/java/javase/24/docs/api/java.sql/java/sql/SQLException.html):

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

### Statement Logging

You may customize your [`Database`](https://javadoc.pyranid.com/com/pyranid/Database.html) with a [`StatementLogger`](https://javadoc.pyranid.com/com/pyranid/StatementLogger.html).

Examples of usage include:

* Writing queries and timing information to your logging system
* Picking out slow queries for special logging/reporting
* Collecting a set of queries executed across a unit of work for bulk analysis (e.g. a [`ThreadLocal`](https://docs.oracle.com/en/java/javase/24/docs/api/java.base/java/lang/ThreadLocal.html) scoped to a single web request)

```java
Database database = Database.withDataSource(dataSource)
  .statementLogger(new StatementLogger() {
    Duration SLOW_QUERY_THRESHOLD = Duration.ofMillis(500);

    @Override
    public void log(@Nonnull StatementLog statementLog) {
      if(statementLog.getTotalDuration().compareTo(SLOW_QUERY_THRESHOLD) > 0)
        out.printf("Slow query: %s\n", statementLog);
    }
  }).build();
```

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
database.queryForObject("SELECT * FROM car LIMIT 1", Car.class);

// SQL in a Statement
database.queryForObject(Statement.of("random-car", "SELECT * FROM car LIMIT 1"), Car.class);
```

This is useful for tagging queries that should be handled specially. Some examples are:

* Marking a query as "hot" so we don't pollute logs with it
* Marking a query as "known to be slow" so we don't flag slow query alerts for it
* Your [`InstanceProvider`](https://javadoc.pyranid.com/com/pyranid/InstanceProvider.html) might provide custom instances based on resultset data
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

A corresponding [`Database`](https://javadoc.pyranid.com/com/pyranid/Database.html) setup:

```java
// Ensure our StatementLogger implementation takes HOT_QUERY into account 
Database database = Database.withDataSource(dataSource)
  .statementLogger(new StatementLogger() {
    @Override
    public void log(@Nonnull StatementLog statementLog) {
      // Log everything except HOT_QUERY
      if(statementLog.getStatementContext().getStatement().getId() != HOT_QUERY)
        out.println(statementLog);
    }
  }).build();
```

## About

Pyranid was created by [Mark Allen](https://www.revetkn.com) and sponsored by [Transmogrify LLC](https://www.xmog.com) and [Revetware LLC](https://www.revetware.com).