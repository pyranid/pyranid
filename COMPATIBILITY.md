# Compatibility Policy

Pyranid follows [Semantic Versioning](https://semver.org/):

* **Major** (`5.0.0`): may remove or incompatibly change public API. Migration notes provided in the CHANGELOG.
* **Minor** (`4.5.0`): additive public API and behavior changes documented under "Migration Notes" in the CHANGELOG.
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

### Intentional historical exceptions

The japicmp configuration excludes a small set of intentional changes, kept for the record:

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
maven-enforcer `bannedDependencies` rule and provable from the published CycloneDX SBOM. Compile-time-only
dependencies (annotations, the PostgreSQL driver for optional rich error metadata) are `provided` scope and
never required at runtime.
