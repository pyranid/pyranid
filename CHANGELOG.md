# Changelog

All notable changes to Pyranid will be documented in this file.

## 4.1.0

### Changed

- Updated the provided PostgreSQL JDBC driver dependency to `42.7.11`.
- `Database.build()` no longer opens a connection to detect database type when `databaseType(...)` is not explicitly configured. Automatic detection now happens lazily when `databaseType` is first requested.
- Numeric result mapping now rejects lossy narrowing for integer-like targets instead of silently truncating or overflowing.
- Numeric mapping to `Boolean` now treats fractional values between `-1` and `1`, excluding zero, as `true`; these previously mapped to `false` due to integer truncation.
- Database operation failures now include bounded SQL and parameter count context without logging raw parameter values.
- Rollback failures are now attached as suppressed exceptions when user transaction code has already thrown.

### Migration Notes

- Applications that relied on construction-time database connectivity validation should validate connectivity separately or explicitly call a database operation at startup.
- Applications that relied on lossy numeric narrowing should update target types or handle the new `DatabaseException`.
  Affected cases include:
  - `BIGINT`/`NUMERIC` values outside the destination primitive wrapper range, such as mapping `BIGINT` to `Integer`.
  - Fractional `NUMERIC`/`DECIMAL` values mapped to integer-like targets such as `Integer`, `Long`, or `BigInteger`.
  - Non-finite floating point values mapped to `Float` or `Double`.
  - Numeric-to-`Character` mappings outside the valid `char` range or with a fractional value.
