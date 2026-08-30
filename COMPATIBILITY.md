# Compatibility Policy

Pyranid follows [Semantic Versioning](https://semver.org/):

* **Major** (`5.0.0`): may remove or incompatibly change public API. Migration notes provided in the CHANGELOG.
* **Minor** (`4.6.0`): additive public API and behavior changes documented under "Migration Notes" in the CHANGELOG.
* **Patch** (`4.6.1`): fixes only; no new public API.

## What counts as public API

Everything `public` in the `com.pyranid` package of the `pyranid` artifact. Package-private types and members
are internal and may change at any time - same-package access is not a supported integration point.
Diagnostic *text* (exception messages, `toString()` renderings, log formats) is not API; when it changes in a
way that could break log parsers, the CHANGELOG says so, but such changes may occur in minor releases.

## How the policy is enforced

API compatibility is machine-checked on every build by the
[japicmp](https://siom79.github.io/japicmp/) Maven plugin (see `pom.xml`), configured to fail the build on
binary- or source-incompatible changes and to validate the version number against the nature of the change
(`breakBuildBasedOnSemanticVersioning`). The comparison baseline is pinned via the `japicmp.baseline.version`
property.

### Intentional breaks in 4.7.0

4.7.0 changes two public members from primitives to their boxed wrapper types so the whole public API
expresses nullness uniformly with JSpecify annotations, which cannot be applied to primitives:

* `ParameterRedactor#redactParameter(...)` - the `parameterIndex` parameter changed from `int` to
  `@NonNull Integer`, matching every sibling SPI (`PreparedStatementBinder#bindParameter(...)`,
  `CustomColumnMapper#map(...)`, `CustomParameterBinder#bind(...)`), each of which already took `Integer`.
* `ResultSetMapper#DEFAULT_PLAN_CACHE_CAPACITY` - changed from `int` to `Integer`. Beyond consistency,
  this removes a compile-time-constant hazard: a `static final int` is inlined into callers, so retuning
  the default would silently not reach an application until it was recompiled.
  `ResultSetMapper#DEFAULT_PREFERRED_COLUMN_MAPPER_CACHE_CAPACITY` deliberately stays `int`: it is already
  deprecated for removal in 5.0.0 and has no effect, so neither argument for boxing applies to it.

These are binary-incompatible, so code compiled against 4.6.x must be recompiled. Ordinary method calls
continue to compile via autoboxing, and inferred lambda parameters need no source change. Named or anonymous
`ParameterRedactor` implementations must change the overriding parameter from `int` to `Integer`; explicitly
typed lambda parameters must do the same. The boxed `DEFAULT_PLAN_CACHE_CAPACITY` is no longer a constant
variable, so source that used it in an annotation value, `case` label, or another compile-time constant
expression must replace that use with a primitive constant.

4.7.0 also generalizes `RawConnectionOperation<R>` to
`RawConnectionOperation<C extends Connection, R>` so the typed `useRawConnection(Class<C>, ...)` overload can
expose concrete driver connection APIs without a second callback abstraction. The functional method still
erases to `(Connection) -> Optional`, so ordinary previously compiled call sites retain JVM linkage and inline
one-argument lambdas remain source-compatible. This is not a blanket guarantee for reflective consumers:
inspecting the generic interfaces of a callback implementation compiled against the old one-argument form can
fail on the new two-argument declaration. Recompile such implementations. Source that explicitly names the
interface must add `Connection` as its first type argument.

Because these changes are deliberate, japicmp narrowly excludes the boxed member signatures plus the
`RawConnectionOperation` interface and the established one-argument `Database#useRawConnection(...)`
signature whose generic metadata changed. The new typed overload and every other public member remain gated.
Remove the exclusions once the 4.7.0 baseline is in place.

### Historical compatibility notes

The following intentional changes predate the current japicmp baseline and are kept here for the record:

* `Database#transaction(TransactionIsolation, ...)` (two overloads) - replaced by
  `TransactionOptions`-based overloads.
* `DatabaseException#isUniqueConstraintViolation()`, `#isForeignKeyViolation()`, `#isDeadlock()`,
  `#isTransient()` - return types changed from primitive `boolean` to boxed `Boolean` in 4.4.0, shortly
  after their 4.3.0 introduction and before meaningful adoption.

## Database compatibility

Pyranid has a first-class [`DatabaseType`](https://javadoc.pyranid.com/com/pyranid/DatabaseType.html) for the
databases below. Everything else falls back to `GENERIC`, which still provides named parameters, result
mapping, transactions, and batching, but classifies exceptions only by standard SQLState.

| Database | `DatabaseType` | Integration leg |
| --- | --- | --- |
| PostgreSQL | `POSTGRESQL` | Testcontainers, gating |
| MySQL | `MYSQL` | Testcontainers, gating |
| MariaDB | `MARIA_DB` | Testcontainers, gating |
| SQLite | `SQLITE` | in-process, gating |
| DuckDB | `DUCK_DB` | in-process, gating |
| SQL Server | `SQL_SERVER` | Testcontainers, advisory |
| Oracle | `ORACLE` | Testcontainers, advisory |

### Wire-compatible databases

Several databases speak another database's wire protocol and are therefore detected as that database.
Pyranid does not test these and makes no compatibility promise about them, but because detection is driven
by JDBC metadata they work to the extent that the detected dialect's assumptions hold. Treat the table as a
starting point for your own verification rather than a support statement.

Every observation below was measured on **2026-08-29** against these versions. Nothing re-verifies them
automatically, so an entry is only as current as that date - check your own versions before relying on it.

| Engine | Version observed | Driver |
| --- | --- | --- |
| CockroachDB | v26.2.6 (CCL) | pgjdbc 42.7.13 |
| YugabyteDB | 15.12-YB-2026.1.1.1-b0 | pgjdbc 42.7.13, and the `com.yugabyte` smart driver |
| TiDB | v7.5.1 and v8.5.3 | MySQL Connector/J |
| Vitess / PlanetScale | 8.0.43-Vitess | MySQL Connector/J |
| StarRocks | reports MySQL 8.0.33 | MySQL Connector/J |
| H2 | 2.4.240 | bundled |
| HSQLDB | 2.7.4 | bundled |
| Apache Derby | 10.17.1.0 | bundled |

| Database | Detected as | Observed behavior |
| --- | --- | --- |
| CockroachDB | `POSTGRESQL` | Constraint, timeout, generated-key, JSONB, and streaming behavior match PostgreSQL. Two divergences below. |
| YugabyteDB | `POSTGRESQL` | Matches PostgreSQL with either stock pgjdbc or the "smart driver" fork. No `LISTEN`/`NOTIFY` - see below. |
| Amazon Aurora DSQL, Neon, Supabase, AlloyDB, TimescaleDB | `POSTGRESQL` | Genuine PostgreSQL wire and engine; no known divergence. Not examined directly. |
| TiDB | `MYSQL` | MySQL vendor error codes come through; deadlock, lock-wait timeout, duplicate key, and foreign key all classify. One retry gap below. |
| PlanetScale / Vitess | `MYSQL` | Classification matches MySQL. `queryTimeout(...)` does not work - see below. |
| StarRocks | `MYSQL` | **Unsafe. See the warning below.** |
| H2, HSQLDB, Apache Derby | `GENERIC` | Standard SQLStates; constraint and retry classification work. |

#### StarRocks is not safe to use with Pyranid

StarRocks reports itself as MySQL in every field Pyranid inspects, so it is detected as `MYSQL` and receives
MySQL dialect behavior - but it does not honor the guarantees that implies. Observed: `Connection::rollback()`
is a no-op (the row survives a rolled-back transaction), and inserting a duplicate primary key raises no error
at all (the row is silently upserted). Pyranid's closure-based transaction contract - the body throws, Pyranid
rolls back, your data is untouched - therefore does not hold, and no exception is raised to warn you. Do not
use StarRocks as a transactional database through Pyranid. The same caution applies to other analytics engines
that adopt the MySQL wire protocol without MySQL's transactional semantics.

#### Divergences worth knowing

* **CockroachDB has no `LISTEN`/`NOTIFY`.** `PostgresDialect` advertises notification support unconditionally,
  so [`Database::withNotificationSession(...)`](https://javadoc.pyranid.com/com/pyranid/Database.html) will
  appear available and then fail at runtime.
* **CockroachDB never reports a deadlock.** Lock-cycle aborts arrive as SQLState `40001`, not `40P01`, so
  [`DatabaseException::isDeadlock()`](https://javadoc.pyranid.com/com/pyranid/DatabaseException.html#isDeadlock())
  never returns `true`. Key retry logic on `isSerializationFailure()` or `isTransient()`, both of which do fire.
  The same applies to any optimistic-concurrency engine, including Amazon Aurora DSQL.
* **TiDB optimistic write conflicts are unclassified.** They arrive as SQLState `HY000` with vendor code
  `9007` ("try again later"), which matches none of Pyranid's predicates - `isTransient()` included - so a
  `RetryPolicy` will not retry them. Supply a custom `RetryPolicy` that inspects the vendor code if you run
  TiDB in optimistic transaction mode.
* **Neither CockroachDB nor YugabyteDB implements `LISTEN`/`NOTIFY`.** Both are detected as `POSTGRESQL`, so
  the dialect offers notifications, and both accept `UNLISTEN *` as a no-op before rejecting the statements
  that matter - CockroachDB with SQLState `42601`/`42883`, YugabyteDB with `0A000` ("LISTEN/NOTIFY is
  disabled"). As of 4.7.0 Pyranid recognizes those failures on its own generated notification SQL and reports
  them as `UnsupportedOperationException` with an explanatory message rather than an opaque syntax error.
* **The YugabyteDB smart driver works as of 4.7.0.** Earlier releases referred to
  `org.postgresql.util.PGobject`, `PSQLException`, `PGConnection` and `PGNotification` by name, so on the
  `com.yugabyte` fork `Parameters.json(...)` and `Parameters.vectorOfDoubles(...)` failed and notification
  support reported itself unavailable. Those types are now resolved at runtime from whichever driver is
  actually in use, so either driver works.
* **`queryTimeout(...)` does not work on Vitess/PlanetScale.** Connector/J implements it with `KILL QUERY`,
  which vtgate rejects (`VT07001`). Express timeouts in the proxy or the query instead.
* **H2 and HSQLDB report referential integrity in both directions.** Each uses SQLState `23503` for one
  direction of a foreign-key violation and a sibling code for the other (H2 `23506`, HSQLDB `23504`). As of
  4.7.0 `GENERIC` recognizes all three, so `isForeignKeyViolation()` is correct on both engines.

## Supported JDKs

* **Source/binary baseline:** Java 17 (`<release>17</release>`).
* **Tested continuously:** JDK 17, 21, and 25 (unit suite); database integration tests run on JDK 21
  against PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle via Testcontainers, and SQLite and DuckDB
  in-process. The SQL Server and Oracle legs are advisory (non-gating) in CI.
* Pyranid uses no `sun.misc.Unsafe`, no `setAccessible`, and reflects over public members only -
  `--add-opens` is never required, and strong-encapsulation tightening in future JDKs is not expected to
  affect it.

## Dependencies

The core `pyranid` artifact declares **zero runtime dependencies** - enforced at build time by a
Maven Enforcer `bannedDependencies` rule and visible in the published POM. Compile-time-only dependencies are
`provided` scope and never required for ordinary runtime use. The PostgreSQL driver is used for optional rich
error metadata and notification receive; PostgreSQL notification sending remains pure SQL.

The tested DuckDB JDBC driver baseline is **1.5.5.1**. Pyranid uses it only in test scope and does not force
it onto application classpaths; applications that connect to DuckDB must provide the driver themselves.

The supported and tested pgjdbc baseline for notification receive is **42.7.13**. pgjdbc remains a Maven
`provided` dependency, so applications that receive PostgreSQL notifications must supply pgjdbc 42.7.13 or newer
at runtime. Ordinary Pyranid use and PostgreSQL notification sending do not require the receive adapter.

## Notification integration topology

Notification integration CI runs on JDK 21 and covers these PostgreSQL topologies and failure modes:

* direct PostgreSQL notification delivery, transaction behavior, interruption, and listener termination through
  `pg_terminate_backend` using the configured `pgvector/pgvector:pg17` PostgreSQL image;
* two bounded, non-TLS scripted-proxy cases that fragment PostgreSQL asynchronous-notification frames and verify
  the pgjdbc receive-integrity guard;
* a listener `Database` backed by PgBouncer session pooling using the configured
  `edoburu/pgbouncer:v1.25.2-p0` image; and
* an ordinary application `Database` backed by PgBouncer transaction pooling together with a distinct
  session-pooled listener `Database`, both routed to the same PostgreSQL primary.

CI does not use PgBouncer transaction or statement pooling as a listener source and does not claim deterministic
rejection of those unsupported modes. It also does not currently provision a PostgreSQL recovery standby or the
optional half-open Toxiproxy topology.
