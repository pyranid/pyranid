# Changelog

All notable changes to Pyranid will be documented in this file.

## 4.5.0 (unreleased)

### Fixed

- `DatabaseException` messages and DBMS metadata fields (e.g. `getDetail()`, `getDbmsMessage()`) now
  best-effort scrub verbatim occurrences of `SecureParameter` values echoed by the database driver
  (for example, PostgreSQL constraint-violation detail such as `Key (email)=(...) already exists`).
  The original driver exception remains fully intact as the `cause`: any sink that renders the stack
  trace or walks `getCause()` (log appenders, error trackers) can still contain the raw value - treat
  the cause chain as sensitive. The scrub is verbatim-only and skips `null`, `Boolean`, very short
  values (to avoid corrupting unrelated diagnostics), and vector parameters (drivers render vectors as
  literals that never match a Java rendering).
- `StatementLog` now carries Pyranid's wrapped `DatabaseException` on driver-failure paths (previously
  the raw driver exception on some paths), so statement logs render scrubbed, redaction-aware
  diagnostics consistently.

### Added

- `SELECT` rows can now be fetched as insertion-ordered maps via the `Query.mapRowType()` result type
  token, e.g. `List<Map<String, Object>> rows = database.query("SELECT * FROM car").fetchList(Query.mapRowType())`.
  Raw `Map.class`/`LinkedHashMap.class` tokens are also accepted (yielding raw result types due to
  type erasure). Keys are normalized (lowercase) column labels so lookups are portable across all
  supported databases; values are extracted with the same dialect-aware logic as record/bean mapping.
  Duplicate column labels fail fast - use aliases to disambiguate.
- `Query.resultSetMapper(...)` and `Query.preparedStatementBinder(...)` for per-query overrides of the
  database-wide mapping/binding SPIs - for example, inline-mapping an ad-hoc join projection with a
  `ResultSetMapper` lambda without configuring it database-wide.
- Added regression guards for Pyranid's virtual-thread story: a source-policy test banning `synchronized`
  from main source, and a JFR-based test asserting a concurrent virtual-thread workload produces no
  `jdk.VirtualThreadPinned` events attributable to Pyranid code.
- Added a portable integration test asserting microsecond timestamp precision survives round trips on
  every supported database (fractional-second columns such as `DATETIME(6)`/`DATETIME2(6)`).
- Added `java.time.Year` and `java.time.YearMonth` scalar support: `Year` binds as `INTEGER` and maps
  back from integer-like, string, or `DATE`-surfaced year columns (e.g. MySQL `YEAR`); `YearMonth` binds
  as its ISO-8601 string form (e.g. `2027-12`) and maps back from strings. Both work as single-column
  targets and record/bean properties.
- Vector columns (e.g. pgvector) now read back into `float[]` and `double[]` targets - Pyranid parses
  the vector literal (such as `[0.1,0.2,0.3]`) that drivers surface for vector columns. Vector support
  is now symmetric with the existing `Parameters.vectorOfFloats(...)`/`vectorOfDoubles(...)` bind side.

### Migration Notes

- `StatementLog.getException()` on driver-failure paths now returns the Pyranid `DatabaseException`
  wrapper; the raw driver exception remains available via its `getCause()`.
- `Map.class` and `LinkedHashMap.class` result type tokens are now intercepted by the built-in map-row
  handling before custom row-level mapping runs: a `CustomColumnMapper` registered for raw `Map` targets
  is no longer consulted for these tokens, `LinkedHashMap.class` now returns populated rows (previously
  an empty bean-mapped instance), and the well-known JDK map tokens `HashMap.class`, `TreeMap.class`,
  `Hashtable.class`, `ConcurrentHashMap.class`, `SortedMap.class`, `NavigableMap.class`, and
  `ConcurrentMap.class` now fail fast with a clear `DatabaseException` (previously they bean-mapped to
  empty instances). Other `Map`-implementing classes keep their JavaBean-path behavior.
- The secure-parameter scrub applies to exceptions raised during statement execution. Exceptions raised
  outside a statement context - commit/rollback time (e.g. deferred constraint violations), connection
  acquisition, and raw-connection operations - are not scrubbed and may carry driver-echoed values in
  their message and DBMS metadata fields.

## 4.4.0

### Added

- Added `SecureParameter` and `Parameters.secure(...)` for display-only masking of sensitive bound parameter values in Pyranid diagnostics while preserving normal JDBC binding behavior.
- Added `ParameterRedactor`, `ParameterRedactor.redactAll()`, `Database.Builder.parameterRedactor(...)`, and `Database.getParameterRedactor()` for opt-in database-wide redaction of non-secure parameters.
- Added `StatementContext.getRedactedParameters()` for safe diagnostic rendering; `SecureParameter` masks take precedence over any configured redactor.
- Added `RetryPolicy` with explicit retry attempts, retry conditions, and fixed/exponential backoff strategies.
- Added `Database.transactionWithRetry(...)` overloads for retrying whole transaction closures after retryable database failures.
- Added `TransactionRetryResult` for successful retry diagnostics, including the successful value and failures recovered before success.
- Added `DatabaseException.isSerializationFailure()` and `DatabaseException.isTimeout()` classification predicates.
- Released artifacts now include a CycloneDX SBOM (`bom.json` and `bom.xml`).

### Changed

- `DatabaseException` classification predicates now return non-null boxed `Boolean` values.
- `StatementContext.toString()` and `StatementLog.toString()` now render redacted parameters instead of raw parameter values.
- `DatabaseException` statement diagnostics now include bounded parameter display values in addition to SQL. Secure parameters are masked, and non-secure values are rendered through the configured `ParameterRedactor`.
- Default generated statement IDs now render as numeric counters instead of `com.pyranid.N` strings.
- Batch statement diagnostics now render a bounded batch summary instead of expanding every parameter group. Raw batch values remain available through `StatementContext.getParameters()` for trusted programmatic consumers.

### Migration Notes

- `DatabaseException` messages now include `parameters=[...]` instead of `parameterCount=...`. Under the default `ParameterRedactor.none()`, non-secure, non-batch values render verbatim and are bounded for size; wrap sensitive values with `Parameters.secure(...)` or configure `Database.Builder.parameterRedactor(ParameterRedactor.redactAll())` if exception text may leave a trusted boundary.
- `DatabaseException.isUniqueConstraintViolation()`, `isForeignKeyViolation()`, `isDeadlock()`, and `isTransient()` now return boxed `Boolean` instead of primitive `boolean`.
- Default generated statement IDs now render as numeric counters instead of `com.pyranid.N` strings. Update log parsers or assertions that match the old format.
- Batch diagnostics now render a bounded summary such as `<batch: 2000 groups x 3 parameters>` instead of individual group values. Use `StatementContext.getParameters()` only in trusted code that intentionally needs raw batch groups.

## 4.3.1

### Fixed

- Corrected a broken Maven Central direct-download link in the README.
- Fixed a non-compiling custom parameter binder code sample in the README.

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
