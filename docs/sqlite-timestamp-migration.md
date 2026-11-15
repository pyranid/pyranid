# Migrate SQLite timestamp text for Pyranid 4.9

Pyranid 4.9 writes local timestamp text with nine fractional digits and instant-like values as UTC text with nine fractional digits and a `Z` suffix. Reading legacy text still works, but binding a value just read uses its new spelling. Existing rows can consequently fail equality lookups, joins, range predicates and optimistic-lock updates. Text `UNIQUE`/`ON CONFLICT` keys can admit a logically duplicate timestamp, and `GROUP BY`/`DISTINCT` can split equivalent instants until stored values are normalized.

This is a data-format migration. Update every writer and normalize related stored keys before relying on text comparison across formats. Original offsets are not retained by UTC normalization; store them separately first if the application needs them.

## Supported example schema

The complete [Java 17 application example](../examples/SqliteTimestampMigration.java) targets exactly this schema:

```sql
CREATE TABLE event_stamp (
  id INTEGER PRIMARY KEY,
  account_id INTEGER NOT NULL,
  local_at TEXT,
  instant_at TEXT,
  UNIQUE(account_id, instant_at)
);
```

The application has declared `local_at` to be an unzoned wall-clock value and `instant_at` to be an instant. SQLite's `TEXT`, `TIMESTAMP` or `DATETIME` affinity alone does not establish that business meaning. The example assumes the default `BINARY` collation, the declared composite unique key, and no foreign keys or triggers that require rewriting related timestamp keys.

Adapt the code and its tests to the application's actual schema. Include every unique key, including composite, partial, expression and generated-column keys. Review checks, triggers, foreign keys and dependent tables. The sample does not discover or migrate arbitrary database schemas. It does not disable or drop constraints. Foreign-key checking is enabled, so an incompatible update fails instead of bypassing referential integrity.

## Input and output rules

| Column | Accepted text | Normalized text |
| --- | --- | --- |
| `local_at` | Strict ISO local timestamp, including legacy omitted seconds and 0–9 fractional digits; no offset or zone | `2024-03-10T02:30:00.000000000` |
| `instant_at` | Strict ISO timestamp with an explicit numeric offset or `Z`, including omitted seconds and 0–9 fractional digits | `2024-03-10T06:30:15.000000000Z` |
| Either | SQL NULL | SQL NULL |

The sample supports canonical years `0000` through `9999`. Invalid dates, surrounding whitespace, offset-free instant text, offset/zone text in the local column, expanded years, excess fractional precision and non-text storage fail explicitly. Integer epochs, Julian values, custom driver date strings and zone-free values previously written under a WITHOUT TIME ZONE strategy require a separate, explicit conversion policy. Do not guess their units or time zone.

Java parsing and formatting preserve nanoseconds. Local wall-clock values are never resolved through a time zone: a value in a daylight-saving gap or overlap keeps its local fields. Instant text such as `2024-03-10T15:30:16.123456789+08:00` becomes `2024-03-10T07:30:16.123456789Z`.

Avoid a generic SQLite `datetime`/`strftime` rewrite when nanosecond preservation is required. This recipe parses the raw text in Java rather than reducing it through SQLite's date functions or JDBC millisecond date getters.

## Run and validate

1. Stop all writers, including older Pyranid processes and background jobs. Take a verified, consistent SQLite backup and test restoring it. A live database may have a WAL; copying only the main file is not a backup procedure. Use SQLite's backup facilities or a correctly shut down database.
2. Rehearse on a copy with the actual schema and representative legacy/canonical values. Extend the validation for every constraint and related key in that schema.
3. If original offsets are needed, preserve them in a separate application column before normalizing `instant_at`.
4. Compile/run the example with Java 17+ and the application's sqlite-jdbc runtime classpath, including its logging dependencies:

```sh
javac --release 17 -d /tmp/sqlite-migration examples/SqliteTimestampMigration.java
java -cp "/tmp/sqlite-migration:$SQLITE_JDBC_CLASSPATH" SqliteTimestampMigration /path/to/existing.sqlite
```

The source-file launcher also works:

```sh
java -cp "$SQLITE_JDBC_CLASSPATH" examples/SqliteTimestampMigration.java /path/to/existing.sqlite
```

The application owns a fresh, dedicated connection. It configures sqlite-jdbc's `transaction_mode=IMMEDIATE` and then disables autocommit, acquiring the writer reservation before reading. A temporary staging table holds normalized text, so Java does not retain the entire source table in memory. Provision space for the staging table and journal/WAL.

Every value is staged before any source row is updated. The normalized `UNIQUE(account_id, instant_at)` key is checked before updates, respecting SQLite's allowance for multiple null values. A collision identifies row IDs and aborts without choosing a winner or deleting data. Resolve the duplicate according to application rules before rerunning. Because canonicalization is idempotent, a source key that would transiently collide with another source spelling would also be a final normalized collision for this exact key; the precheck catches it.

After validation, one null-aware update rewrites only changed source rows, preserving IDs and leaving already canonical rows untouched. The method returns the changed-row count after commit; rerunning a successful migration returns zero.

Parsing, collision and update failures before commit trigger rollback. The regression suite verifies that those failures leave source rows unchanged. A commit/storage failure or an exception after commit can leave the final result uncertain: inspect the database or restore the verified backup before deciding to retry. Do not infer that every thrown exception means no changes committed.

Before restarting writers, verify equality and optimistic-lock predicates using newly bound values, uniqueness and conflict handling, joins across every related timestamp key, nulls, exact fractional values, row counts and application invariants. Keep older-format writers stopped after migration; otherwise they can reintroduce mixed text.

## Regression coverage

`SqliteTimestampMigrationIT` compiles and executes the exact standalone example with `--release 17`. It verifies the pre-migration equality/uniqueness/optimistic-lock failures and their post-migration repair, join and DISTINCT behavior, null handling, nanosecond preservation, offset normalization, unchanged local DST-gap fields, idempotence, collision preflight, malformed/ambiguous/non-text rejection and rollback after an injected update failure.

References: [SQLite transactions](https://www.sqlite.org/lang_transaction.html), [SQLite backup facilities](https://www.sqlite.org/backup.html), [sqlite-jdbc transaction configuration](https://github.com/xerial/sqlite-jdbc/blob/3.54.1.0/src/main/java/org/sqlite/SQLiteConfig.java).
