# Changelog

All notable changes to Pyranid will be documented in this file.

## 4.8.0-SNAPSHOT

### Added

- Core `NotificationSupervisor` provides application-executor-owned listener startup, readiness, reconnection,
  reconciliation, cooperative stop, and termination. It creates no threads or fallback scheduler.
- `TransactionOptions.completionObserver(...)` reports immutable `TransactionCompletion` records after cleanup
  and post-transaction hooks, with correlated scope IDs, database outcome, and the final throwable.
- `RetryPolicy.Backoff.fullJitter(...)` and optional `withMaxElapsedTime(...)` retry budgets. Defaults are unchanged;
  budgets govern admission of another attempt, not cancellation of an in-flight JDBC operation.
- `StatementLog.getElapsedDuration()` reports time from executor entry through resource release, separately
  from the existing phase-sum `getTotalDuration()`.
- Configurable bounded LOB materialization through `ResultSetMapper.Builder.maxLobBytes(...)`,
  `maxLobCharacters(...)`, and `unboundedLobMaterialization()`.

### Changed

- SQL compilation, query preparation, statement/stream execution, transaction coordination, retries, and
  notification session handling have dedicated internal components. Statements and streams share connection
  ownership and release logic; stream resources still close at exhaustion or explicit early closure.
- Cached and uncached typed row mapping share the same plan compiler and execution path. Cache configuration
  changes retention, not conversion behavior. Scalar, record, and JavaBean conversions share built-in rules.
- API compatibility now compares against the verified 4.7.0 release, without the old 4.7.0 exclusions.

### Fixed

- Transaction retries retain unsafe outcome evidence through exception wrapping, JDBC next-exception chains,
  nested transactions, and participating threads. A committed transaction's cleanup failure or an uncertain
  outcome cannot become retryable merely because application code wraps the original failure.
- Transaction completion closes admission before JDBC commit or rollback begins. Captured handles reject
  late mutations, JDBC work, and new participation during completion, including rollback after a body failure.
- Notification supervision treats fatal errors and unsupported receive capabilities as terminal even when
  they appear inside causes, suppressed failures, or JDBC next-exception chains; retry conditions and
  backoffs are not invoked. Unsupported optional abort/close cleanup does not disable reconnection after a
  transient failure. Generated LISTEN capability checks include JDBC next exceptions and tolerate throwing
  exception accessors without skipping connection cleanup.
- Transaction failure inspection handles cyclic exception graphs and throwing cause accessors without
  hanging cleanup or replacing the primary failure. Reachable interrupt evidence restores thread interruption,
  including failures introduced by connection cleanup or post-transaction hooks. An interrupted thread cannot
  admit another retry, including when the backoff is zero or a retry-policy callback sets the interrupt flag.
- Resource cleanup continues when drivers reuse the same exception instance across failures. The original
  operation failure is retained, and later arrays, result sets, statements, and connections are still released.
- Removing a post-transaction hook makes bounded progress when its equality comparison adds or removes
  other hooks, while retaining the guard against mutations after transaction completion begins.
- Scalar cross-type LOB conversions retain JDBC getter behavior, including DuckDB BLOB-to-`String` mapping.
  Bounded built-in BLOB-to-`byte[]` and CLOB/NCLOB-to-`String` materialization remains unchanged.
- Visible raw JDBC handles returned by custom mappers or reentrant scalar/map mapping remain borrowed,
  preventing row cleanup from freeing a handle returned to the caller while its cursor is still open.
  Ownership tracking does not iterate custom Iterable or Map outputs, preserving lazy and single-use results.
- Typed SQL ARRAY temporal elements follow scalar conversion semantics, preserving SQL element type,
  configured database time zone, timestamp nanoseconds, and requested JDBC date/time subtypes.
  PostgreSQL `timestamptz` arrays retain their instant semantics even when the driver reports `TIMESTAMP`
  as their JDBC base type. Nested arrays receive element conversion even when the driver's array subtype is
  assignable to the requested type. PostgreSQL `timetz` scalars and arrays preserve their original offset
  and fractional seconds through typed JDBC reads. Typed DuckDB STRUCT timestamp attributes also use their SQL temporal type,
  so zone-less attributes follow the configured database time zone instead of the JVM default.
- Zone-less SQL timestamps fetched as scalar `OffsetDateTime` values now use the configured database time zone,
  consistent with record and JavaBean properties, even when the driver can attach its own session time zone.
- Temporal record and JavaBean properties targeting `java.sql.Timestamp` or `java.sql.Date` now select the JDBC
  subtype conversion before the general `java.util.Date` conversion, retaining timestamp nanoseconds. Zoned
  timestamps mapped to `java.sql.Date` consistently use the configured database time zone across row shapes.
- `TimeZone` mapping preserves valid `UTC` and `UT` offsets instead of silently returning the GMT fallback.
- SQL Server and SQLite bracket-quoted identifiers remain quoted in predicates and implicit aliases, so colons
  inside identifiers no longer become phantom named parameters. Other dialects retain their subscript rules.
- Scoped savepoints reject JDBC work from other participating threads while the scope is active, preventing
  rollback of another participant's successful statements. Same-thread nesting remains supported, and ownership
  is released when the scope returns or throws. Manually managed savepoints still require caller coordination.
- Statement diagnostics and metrics retain preparation, execution, and mapping time when the corresponding
  operation fails, including failed stream opening and iteration.

### Migration Notes

- Built-in BLOB-to-`byte[]` and CLOB/NCLOB-to-`String` materialization now defaults to at most 16 MiB of bytes
  or 8,388,608 UTF-16 code units per value. This deliberate 4.8.0 behavior change can reject values accepted
  by earlier versions. Set larger positive limits or explicitly opt into unbounded materialization. Limits
  apply before Pyranid's eager dialect conversion; driver buffering, custom mappers, ordinary VARCHAR values,
  and aggregate result-list size are outside their scope.
- Raw LOB/Object/Map results retain their driver and cursor lifetime. A resource-bearing value passed to a
  custom mapper is borrowed even if that mapper declines it; Pyranid cannot know whether the mapper retained
  the value. Custom column mapper order, handled-null semantics, and nested-attribute behavior are unchanged.
- Completion observer failures are contained. Existing post-transaction business hooks still propagate;
  a committed transaction can therefore report `COMMITTED` with a failure. Observe each retry attempt separately.
- Notification readiness means registration plus initial reconciliation succeeded at least once. It is not
  durable delivery or a continuing health signal. Keep independent reconciliation polling and make it idempotent.
- Elapsed timing includes consumer time while a cursor remains open and ends at early closure. Existing
  phase timings and OpenTelemetry `db.client.operation.duration` retain their previous meanings.
- JDBC work from another thread during `Transaction::withSavepoint(...)` now fails before reaching the driver.
  Coordinate that work after the savepoint scope ends. As before, an exception escaping `Database::participate(...)`
  marks the transaction rollback-only; handle an expected ownership conflict inside the participating callback
  if the transaction should continue.

## 4.7.0

### Added

- COMPATIBILITY.md documents which databases are detected as which `DatabaseType`, including wire-compatible
  databases that Pyranid does not test (CockroachDB, YugabyteDB, TiDB, Vitess/PlanetScale, H2, HSQLDB and
  others), the divergences worth knowing about each, and a warning that StarRocks is not safe to use as a
  transactional database through Pyranid despite being detected as MySQL.
- DuckDB support: `DatabaseType.DUCK_DB` with automatic detection from JDBC product name (`DuckDB`), URL
  (`jdbc:duckdb:`), and driver name signals, plus a dedicated internal dialect. DuckDB runs in-process, so
  integration coverage requires no container support.
- SQL STRUCT-to-record mapping: dialects that surface STRUCT columns as attribute-name-to-value maps
  (currently DuckDB) map them onto Java records, recursively - nested STRUCTs map to nested records,
  STRUCT lists map to `List<RecordType>`/array components, and attribute names follow the same
  `camelCase`/`snake_case` and `@DatabaseColumn` matching rules as result columns. A single-column STRUCT
  result maps directly to a record target (`SELECT person FROM ...` with `fetchObject(Person.class)`), with
  ordinary by-name row mapping always taking precedence. `Map` rows surface STRUCT values as plain
  insertion-ordered maps with no driver types, including elements of top-level STRUCT/JSON list columns.
  Top-level row records - the single-column STRUCT fallback included - are constructed via the configured
  `InstanceProvider`; nested records are constructed via their canonical constructors, and
  `CustomColumnMapper`s are not consulted for individual STRUCT attributes.
- SQL STRUCT parameter binding through `Parameters::sqlStructOf(...)`, backed by JDBC
  `Connection::createStruct(...)` on supported dialects (currently DuckDB). STRUCT parameters support
  typed `NULL`, defensive attribute copies, nested STRUCTs, SQL ARRAYs nested inside STRUCTs, and STRUCTs
  nested inside SQL ARRAYs while preserving Pyranid's normal parameter normalization and secure-diagnostic
  scrubbing. DuckDB instant-bearing attributes require a complete inline STRUCT declaration so Pyranid can
  distinguish `TIMESTAMP` from `TIMESTAMPTZ` and apply the configured database time zone. Named `CREATE TYPE`
  aliases do not expose their attribute types through this binding path and therefore fail fast for those
  values rather than risking a silent timezone-dependent conversion.
- Nested SQL ARRAY parameters: `Parameters::sqlArrayOf(...)` recursively materializes nested
  `SqlArrayParameter` values. Multidimensional binding is integration-verified on DuckDB when every
  dimension is represented by its own parameter and each containing level's base type names its element
  type (for example, outer `VARCHAR[]` and inner `VARCHAR`). DuckDB instant-bearing elements require an
  explicit `TIMESTAMP` or `TIMESTAMPTZ` base type; named aliases fail fast because their underlying timestamp
  semantics are unavailable to this binding path. Other drivers may impose different rules.
- A typed `Database::useRawConnection(Class<C>, RawConnectionOperation<? super C, T>)` overload for callback-scoped
  access to concrete driver connection APIs such as DuckDB Appender, Arrow, chunked results, profiling,
  progress, and Java UDF registration. Unlike the existing one-argument overload's guarded JDBC handle,
  this advanced overload exposes the unwrapped driver connection and therefore cannot enforce Pyranid's
  lifecycle protections; the connection and every resource derived from it must remain inside the callback.
  Driver-extension transaction rules still apply: in particular, a DuckDB Appender used as the first operation
  in a transaction requires an ordinary JDBC statement to start DuckDB's lazy native transaction before append.
- `VectorParameter` support on DuckDB: `Parameters.vectorOfFloats(...)`/`vectorOfDoubles(...)` bind to
  fixed-size `FLOAT[n]`/`DOUBLE[n]` ARRAY columns via the driver's SQL ARRAY support. Similarity
  comparisons require an explicit cast of the bound parameter, e.g.
  `array_cosine_distance(embedding, CAST(:probe AS FLOAT[3]))`.
- DuckDB exception classification: the driver reports no SQLStates or vendor codes, so
  `isUniqueConstraintViolation()`/`isForeignKeyViolation()` match DuckDB's stable `Constraint Error:`
  message shapes, and optimistic-concurrency write-write conflicts (`TransactionContext Error: ... conflict`)
  classify as retryable serialization failures, integrating with `RetryPolicy`. Query timeouts surface as
  `SQLTimeoutException` and classify via the generic path.
- DuckDB result-set niceties: `JSON` columns surface as `String`, `BLOB` columns as `byte[]`, `HUGEINT` as
  `BigInteger`, and `UUID` columns bind and map natively. Nested lists and maps are unwrapped recursively so
  map-shaped results do not retain DuckDB driver types.
- DuckDB list literals and subscripts use a DuckDB-aware SQL lexical mode, so named parameters work inside
  expressions such as `[:first, :second]` without being mistaken for SQL Server bracket-quoted identifiers.
- JDBC generated-key retrieval (`executeReturningGeneratedKey(...)`) is guarded on DuckDB with a clear
  `DatabaseException` directing callers to `INSERT ... RETURNING`, instead of the driver's unhelpful
  `SQLFeatureNotSupportedException`.

### Fixed

- PostgreSQL-specific parameters, exception metadata, and notification receive now work with pgjdbc and the
  explicitly supported YugabyteDB smart driver. `PostgresDialect` resolved `PGobject` and `PSQLException` by
  their pgjdbc names, so on YugabyteDB's
  "smart driver" - which ships the same classes under `com.yugabyte.util` - `Parameters.json(...)` and
  `Parameters.vectorOfDoubles(...)` failed with `NoClassDefFoundError` when pgjdbc was absent, or bound an
  object the driver did not recognize when both drivers were on the classpath. Notification receive was
  affected too: `PostgresNotificationTransport` statically linked `PGConnection`/`PGNotification`, so
  `Database::isNotificationListeningSupported()` reported `false` on a fork even though the driver was
  present and capable. Pyranid now resolves the supported driver types at runtime without static
  driver-type linkage, using `Connection::unwrap` when a pool or proxy exposes the underlying driver API.
  This release explicitly supports pgjdbc and the `com.yugabyte` smart driver; arbitrary repackaged forks are
  not part of the compatibility contract. An advisory integration leg covers JSONB binding plus enabled
  notification send/receive on YugabyteDB 2026.1.1.1 with smart driver 42.7.3-yb-4.
- Notification sends and listener sessions against a PostgreSQL-compatible server where `LISTEN`/`NOTIFY` is
  unavailable in the current version or configuration now fail with the documented
  `UnsupportedOperationException` and an explanatory message instead of an opaque syntax or feature error.
  CockroachDB does not implement the feature and rejects Pyranid's generated SQL with `42601`/`42883`.
  YugabyteDB offers it as an Early Access feature in v2025.2.3 and later, disabled by default behind
  `ysql_yb_enable_listen_notify` on Masters and TServers; the disabled configuration reports `0A000`.
- `DatabaseException::isForeignKeyViolation()` now recognizes SQLStates `23504` and `23506` in addition to the
  SQL-standard `23503`, so referential-integrity violations classify correctly on engines that report the two
  directions of the violation under different codes. H2 uses `23506` when a child row points at a missing
  parent and HSQLDB uses `23504` when a parent row that still has children is deleted; each previously
  classified as "not a foreign-key violation" for that direction. PostgreSQL, MySQL-family, Oracle, SQL Server,
  and SQLite are unaffected, as none of them emit `23504` or `23506`.
- Bracket-quoted identifiers following `SET` are now recognized as identifiers rather than subscripts, so
  named-parameter parsing no longer looks inside them. `UPDATE t SET [a:b] = :id` previously parsed `:b` as
  a named parameter and demanded a binding for it; it now binds only `:id`. This affects every database
  whose SQL uses bracket-quoted identifiers (primarily SQL Server); subscript expressions such as
  `arr[a:b]` are unchanged and still parse `:b`.

### Changed

- Two public members changed from primitives to boxed wrapper types so the public API expresses nullness
  uniformly with JSpecify annotations, which cannot be applied to primitives:
  `ParameterRedactor::redactParameter`'s `parameterIndex` parameter (`int` -> `@NonNull Integer`, matching
  every sibling SPI, all of which already took `Integer`) and `ResultSetMapper::DEFAULT_PLAN_CACHE_CAPACITY`
  (`int` -> `Integer`). `ResultSetMapper::DEFAULT_PREFERRED_COLUMN_MAPPER_CACHE_CAPACITY` is deliberately
  left alone - it is already deprecated for removal in 5.0.0 and has no effect. See "Intentional breaks in
  4.7.0" in COMPATIBILITY.md.
- `RawConnectionOperation` now takes the callback connection type as well as the result type
  (`RawConnectionOperation<C extends Connection, R>`). Inline `useRawConnection(...)` lambdas are unchanged,
  and the erased functional-method descriptor remains `(Connection) -> Optional`, but source that names the
  functional-interface type explicitly must add `Connection` as its first type argument.
- Temporal parameters (`LocalDate`, `LocalTime`, `LocalDateTime`, `OffsetDateTime`, `Instant`) now bind
  with untyped `PreparedStatement::setObject` on dialects whose drivers reject explicit target SQL types
  (currently DuckDB), and `OffsetTime` binds there as its ISO-8601 string form (DuckDB's driver accepts
  `setObject(OffsetTime)` but rejects it at execute time, so try-bind fallback cannot detect it); other
  databases continue to bind exactly as before.

### Migration Notes

- The two public members boxed in this release (see "Changed") are binary-incompatible, so code compiled
  against 4.6.x must be recompiled against 4.7.0. Ordinary calls to `ParameterRedactor::redactParameter(...)`
  continue to compile via autoboxing, and inferred lambda parameters need no source change. Named or anonymous
  implementations must change the overriding parameter from `int` to `Integer`; an explicitly typed lambda
  parameter (`(context, int index, parameter) -> ...`) must likewise use `Integer` or become inferred.
  `ResultSetMapper::DEFAULT_PLAN_CACHE_CAPACITY` is no longer a constant variable, so it cannot be used in
  annotation values, `case` labels, or other compile-time constant-expression contexts.
- Source that explicitly names `RawConnectionOperation<R>` must change to
  `RawConnectionOperation<Connection, R>`. Inline one-argument `useRawConnection(...)` lambdas do not change,
  and ordinary previously compiled call sites remain JVM-linkage-compatible because the callback method's
  erased descriptor is unchanged. Code or frameworks that reflect on a callback implementation's generic
  interfaces may reject the old one-argument generic signature; recompile those callback implementations.
- `DatabaseType` gained the `DUCK_DB` constant. Exhaustive `switch` expressions over `DatabaseType` in
  application code must add a `DUCK_DB` (or `default`) arm when recompiling against 4.7.0.
- A single `NULL` SQL STRUCT fetched directly as a record produces `Optional.empty()` from single-object
  fetches and a `null` element in list/stream results, consistent with existing `NULL` standard-type handling.
- DuckDB SQL that uses `:` in struct literals (`{'key':expr}`) or list slicing (`list[a:b]`) collides with
  Pyranid's named-parameter syntax when the character after `:` can start an identifier. Write a space
  after the colon (`{'key': expr}`), or use `struct_pack(key := expr)` / `list_slice(...)` instead.
  DuckDB's `::` casts and `->`/`->>` JSON operators need no escaping. Named parameters inside DuckDB list
  literals (`[:first, :second]`) now bind as expected.
- SQL containing `[` whose DuckDB lexical parse differs from the standard parse - for example a SQL Server
  bracket-quoted identifier that contains a colon, as in `[a:ignored]` - now requires a known
  `DatabaseType`, so such a statement may trigger lazy database-type detection (and its connection
  checkout) where it previously did not. Ordinary bracket-quoted identifiers such as `[order]` parse
  identically in both modes and are unaffected. Configure `Database.Builder::databaseType(...)`
  explicitly to avoid the lookup. This matches the existing behavior for MySQL-specific lexical variants.

## 4.6.1

### Fixed

- Transaction attempts that fail while beginning the physical transaction no longer manufacture an illegal rollback,
  report a false `IN_DOUBT` outcome, or suppress a policy-approved retry before transactional database work could execute.
  Failed begin candidates are discarded, and a begin failure caught inside the closure is retained and rethrown before
  commit.
- When the JDBC connection supports network-timeout inspection and mutation, PostgreSQL notification cleanup now caps the
  `UNLISTEN *` statement round trip at 30 seconds while preserving any stricter positive timeout. Explicit capability
  failures before timeout mutation retain the legacy cleanup path. Other timeout, command, statement-close, or restoration
  failures mark the listener connection uncertain and route it through abort-then-close instead of returning it to the pool.

### Documentation

- Clarified late-interrupt reconciliation in the notification receive Javadocs and corrected the supervisor example so
  bounded Pyranid transactions run with the interrupt temporarily cleared and restore the cancellation signal afterward.
- Documented PostgreSQL listener `socketTimeout` guidance and the distinct bounds for statement cleanup versus partial
  protocol-frame receive stalls.

### Migration Notes

- Post-transaction operations can receive `ROLLED_BACK` for a failed physical-transaction begin whose candidate connection
  Pyranid discarded before application transaction work could execute, even when no JDBC `rollback()` call was made. Treat
  `ROLLED_BACK` as a determinate guarantee that no application transaction work committed. `IN_DOUBT` remains reserved for
  failures where application transaction work could have executed and Pyranid cannot prove the final database outcome.

## 4.6.0

### Added

- Added generic notification APIs: `Notification`, `NotificationSession`,
  `NotificationSessionOperation`, `Database.sendNotification(...)`,
  `Database.withNotificationSession(...)`, and
  `Database.isNotificationListeningSupported()`. PostgreSQL `LISTEN`/`NOTIFY` is
  the only currently supported implementation. Listening uses one caller-owned,
  callback-scoped physical session with no automatic reconnect; applications
  own durable reconciliation, retry, supervision, and topology. Notification
  payloads are nullable, and their null/empty-string behavior remains
  database-specific.
- Added low-cardinality notification-session lifecycle callbacks and
  `NotificationSnapshot` to `MetricsCollector`.

### Fixed

- Fixed a transaction-completion data-integrity risk on rollback failure.
  Cleanup could restore JDBC auto-commit after rollback failed, which could
  implicitly commit still-active work that the caller expected to be rolled
  back even though the transaction reported failure. The failed-rollback route
  dates to Pyranid's original transaction implementation; an additional route
  through a pending thread interrupt was introduced in 4.5.0.
- Transaction completion now preserves pending interrupts while still running
  commit or rollback. Connections whose final outcome is indeterminate are
  aborted and closed without restoring auto-commit or other mutable state,
  post-transaction hooks receive `IN_DOUBT`, and retry policies do not replay
  the work. A commit-phase serialization failure remains eligible for a
  policy-approved retry only when Pyranid's follow-up rollback succeeds; a
  failed rollback remains `IN_DOUBT` and terminal. If connection-state
  restoration fails after an otherwise definite commit or rollback, Pyranid
  stops further restoration and conservatively discards that connection too.

### Changed

- Updated the provided pgjdbc dependency and PostgreSQL notification-receive
  baseline to 42.7.13.

### Migration Notes

- Treat `TransactionResult.IN_DOUBT` as terminal uncertainty, not as a synonym
  for rollback. It now includes rollback failures; reconcile authoritative
  state instead of running commit-only or rollback-only side effects.
  `transactionWithRetry(...)` never replays an `IN_DOUBT` attempt. A
  commit-phase serialization failure followed by a successful rollback is a
  definite `ROLLED_BACK` result and remains eligible for the configured retry
  policy.
- Transaction connections are now discarded after an indeterminate completion
  or failed connection-state restoration instead of being returned to the pool
  with uncertain state. Pools may create replacement connections if these
  failures occur.
- Notification delivery is lossy and non-durable. Treat notifications as hints
  to reconcile authoritative state. PostgreSQL receive support requires pgjdbc
  42.7.13 or newer at runtime; sending remains pure SQL and does not require the
  receive adapter.
- Listener connections require backend-session affinity. Direct PostgreSQL and
  PgBouncer session pooling are supported listener sources; PgBouncer transaction
  or statement pooling is unsupported and can appear to register successfully
  before notification delivery silently stops.
- On Java 21 or newer, interrupting a virtual thread during an applicable
  system-default socket read can close the listener connection and surface as a
  terminal `DatabaseException` instead of a clean `InterruptedException`.

## 4.5.0

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
- Transaction retries no longer replay a closure after the database commit has succeeded but connection
  cleanup or a post-transaction callback fails. JDBC `Error` instances also remain the primary failure
  when connection cleanup produces a secondary exception.
- Automatic database-type detection now uses the active transaction connection, so transaction metrics
  and exception classification report the real dialect without borrowing a second connection.
- Transaction connection and savepoint lock acquisition is now interruptible. A failed rollback to a
  savepoint marks the transaction rollback-only instead of allowing a later commit attempt.
- Named-parameter parsing now handles MySQL `#` comments and backslash-escaped strings, Oracle alternative
  quoting, and whitespace before PostgreSQL array subscripts without misclassifying parameters. Ambiguous
  MySQL syntax is resolved after dialect detection and cached per lexical mode.
- Nested secure/optional IN-list values now unwrap consistently, reject null elements before JDBC work,
  and retain element-level diagnostic masks.
- Planned and non-planned row mapping now share additive column-alias semantics, unwrap driver wrapper
  objects before assigning `Object` properties, and fall back when typed temporal getters are unsupported.
- Guarded raw-connection vendor-interface unwrapping can no longer expose a lifecycle-capable physical
  `Connection`, and any returned proxy expires when the callback finishes.

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

### Changed

- Preferred custom-column-mapper caching was removed because cache keys could not safely represent mapper
  applicability. The related 4.x builder setting and constant remain as deprecated no-ops for compatibility.
- CI now enforces instruction and branch coverage floors, validates JDK 21 virtual-thread pin detection with
  a positive control, and exposes the long-running heap-stability test through a manual workflow run.

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
- `ResultSetMapper.Builder.preferredColumnMapperCacheCapacity(...)` and
  `DEFAULT_PREFERRED_COLUMN_MAPPER_CACHE_CAPACITY` are deprecated no-ops in 4.5.0 and are planned for
  removal in 5.0.0.

## 4.4.0

### Added

- Added `SecureParameter` and `Parameters.secure(...)` for display-only masking of sensitive bound parameter values in Pyranid diagnostics while preserving normal JDBC binding behavior.
- Added `ParameterRedactor`, `ParameterRedactor.redactAll()`, `Database.Builder.parameterRedactor(...)`, and `Database.getParameterRedactor()` for opt-in database-wide redaction of non-secure parameters.
- Added `StatementContext.getRedactedParameters()` for safe diagnostic rendering; `SecureParameter` masks take precedence over any configured redactor.
- Added `RetryPolicy` with explicit retry attempts, retry conditions, and fixed/exponential backoff strategies.
- Added `Database.transactionWithRetry(...)` overloads for retrying whole transaction closures after retryable database failures.
- Added `TransactionRetryResult` for successful retry diagnostics, including the successful value and failures recovered before success.
- Added `DatabaseException.isSerializationFailure()` and `DatabaseException.isTimeout()` classification predicates.

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
