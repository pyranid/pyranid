# Changelog

All notable changes to Pyranid will be documented in this file.

## 4.3.0

### Added

- Added first-class database type detection and dialect behavior for MySQL, MariaDB, SQLite, and SQL Server, with more robust Oracle detection.
- Added conservative `DatabaseException` classification predicates for unique constraint violations, foreign-key violations, deadlocks, and transient failures across supported database types.
- Added dialect-specific UUID binding for MySQL, MariaDB, SQLite, SQL Server, Oracle, and PostgreSQL.
- Added guarded SQL ARRAY binding so unsupported databases fail with a clear Pyranid exception instead of leaking driver-specific failures.
- Added MySQL/MariaDB streaming setup, SQL Server `datetimeoffset` mapping, Oracle timestamp-with-time-zone handling, and stricter generated-key handling for Oracle.
- Added SQL Server, Oracle, and MariaDB integration-test profiles, plus broader portable integration coverage for vendor-specific JSON, UUID, temporal, generated-key, exception, numeric, and DML-returning behavior.

## 4.2.0

### Added

- Added the `MetricsCollector` API, disabled and in-memory collectors, statement result metadata, and build-time `Database.Builder.metricsCollector(...)` configuration.
- Added metrics callbacks for statement execution, statement/transaction connection acquisition and release, logical and physical transaction lifecycle, savepoints, streaming result sets, and post-transaction operations.

## 4.1.0

### Changed

- Updated the provided PostgreSQL JDBC driver dependency to `42.7.11`.
- `Database.build()` no longer opens a connection to detect database type when `databaseType(...)` is not explicitly configured. Automatic detection now happens lazily when `databaseType` is first requested.
- Numeric result mapping now rejects lossy narrowing for integer-like targets instead of silently truncating or overflowing.
- Numeric mapping to `Boolean` now treats fractional values between `-1` and `1`, excluding zero, as `true`; these previously mapped to `false` due to integer truncation.
- Database operation failures now include bounded SQL and parameter count context without logging raw parameter values.
- Rollback failures are now attached as suppressed exceptions when user transaction code has already thrown.
- Completed transaction handles now reject mutating and JDBC-touching operations with `IllegalStateException`.
- Internal LRU caches now force maintenance after a small bounded over-capacity window to avoid unbounded growth under bursty unique-key writes.

### Added

- Added `Transaction.withSavepoint(...)` helpers for closure-scoped partial rollback.
- Added `Transaction.releaseSavepoint(Savepoint)` for explicit raw-savepoint cleanup.

### Migration Notes

- Applications that relied on construction-time database connectivity validation should validate connectivity separately or explicitly call a database operation at startup.
- Applications that relied on lossy numeric narrowing should update target types or handle the new `DatabaseException`.
  Affected cases include:
  - `BIGINT`/`NUMERIC` values outside the destination primitive wrapper range, such as mapping `BIGINT` to `Integer`.
  - Fractional `NUMERIC`/`DECIMAL` values mapped to integer-like targets such as `Integer`, `Long`, or `BigInteger`.
  - Non-finite floating point values mapped to `Float` or `Double`.
  - Numeric-to-`Character` mappings outside the valid `char` range or with a fractional value.
- Applications that retain `Transaction` handles beyond the transaction closure should stop doing so; mutating and JDBC-touching methods now fail after completion.
