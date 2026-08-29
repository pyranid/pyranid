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

### Intentional break in 4.7.0

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

These are binary-incompatible. Ordinary source that calls or implements these members continues to compile
via autoboxing, but code compiled against 4.6.x must be recompiled. Because the changes are deliberate, the
three affected member signatures are the japicmp configuration's only exclusions (see the `<excludes>` block
in `pom.xml`, which names them individually so every other member stays gated); the exclusions should be
removed once the 4.7.0 baseline is in place.

### Historical compatibility notes

The following intentional changes predate the current japicmp baseline and are kept here for the record:

* `Database#transaction(TransactionIsolation, ...)` (two overloads) - replaced by
  `TransactionOptions`-based overloads.
* `DatabaseException#isUniqueConstraintViolation()`, `#isForeignKeyViolation()`, `#isDeadlock()`,
  `#isTransient()` - return types changed from primitive `boolean` to boxed `Boolean` in 4.4.0, shortly
  after their 4.3.0 introduction and before meaningful adoption.

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
