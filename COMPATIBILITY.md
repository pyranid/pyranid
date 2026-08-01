# Compatibility Policy

Pyranid follows [Semantic Versioning](https://semver.org/):

* **Major** (`5.0.0`): may remove or incompatibly change public API. Migration notes provided in the CHANGELOG.
* **Minor** (`4.6.0`): additive public API and behavior changes documented under "Migration Notes" in the CHANGELOG.
* **Patch** (`4.4.1`): fixes only; no new public API.

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

### Historical compatibility notes

The following intentional changes predate the current 4.5.0 japicmp baseline. They are kept here for the record;
the current japicmp configuration has no API exclusions:

* `Database#transaction(TransactionIsolation, ...)` (two overloads) - replaced by
  `TransactionOptions`-based overloads.
* `DatabaseException#isUniqueConstraintViolation()`, `#isForeignKeyViolation()`, `#isDeadlock()`,
  `#isTransient()` - return types changed from primitive `boolean` to boxed `Boolean` in 4.4.0, shortly
  after their 4.3.0 introduction and before meaningful adoption.

## Supported JDKs

* **Source/binary baseline:** Java 17 (`<release>17</release>`).
* **Tested continuously:** JDK 17, 21, and 25 (unit suite); database integration tests run on JDK 21
  against PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle via Testcontainers, and SQLite in-process.
  The SQL Server and Oracle legs are advisory (non-gating) in CI.
* Pyranid uses no `sun.misc.Unsafe`, no `setAccessible`, and reflects over public members only -
  `--add-opens` is never required, and strong-encapsulation tightening in future JDKs is not expected to
  affect it.

## Dependencies

The core `pyranid` artifact declares **zero runtime dependencies** - enforced at build time by a
Maven Enforcer `bannedDependencies` rule and visible in the published POM. Compile-time-only dependencies are
`provided` scope and never required for ordinary runtime use. The PostgreSQL driver is used for optional rich
error metadata and notification receive; PostgreSQL notification sending remains pure SQL.

The supported and tested pgjdbc baseline for notification receive is **42.7.11**. pgjdbc remains a Maven
`provided` dependency, so applications that receive PostgreSQL notifications must supply pgjdbc 42.7.11 or newer
at runtime. Ordinary Pyranid use and PostgreSQL notification sending do not require the receive adapter.

## Notification integration topology

Notification integration CI runs on JDK 21 and covers these PostgreSQL topologies and failure modes:

* direct PostgreSQL notification delivery, transaction behavior, interruption, and listener termination through
  `pg_terminate_backend` using the configured `pgvector/pgvector:pg17` PostgreSQL image;
* two bounded, non-TLS scripted-proxy cases that fragment PostgreSQL asynchronous-notification frames and verify
  the pgjdbc receive-integrity guard;
* a listener `Database` backed by PgBouncer session pooling using the configured
  `edoburu/pgbouncer:v1.24.1-p1` image; and
* an ordinary application `Database` backed by PgBouncer transaction pooling together with a distinct
  session-pooled listener `Database`, both routed to the same PostgreSQL primary.

CI does not use PgBouncer transaction or statement pooling as a listener source and does not claim deterministic
rejection of those unsupported modes. It also does not currently provision a PostgreSQL recovery standby or the
optional half-open Toxiproxy topology.
