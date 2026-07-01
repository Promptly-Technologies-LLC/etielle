## UpsertFlushStrategy


Streaming strategy with database-level conflict handling (SQLAlchemy).


Usage

``` python
UpsertFlushStrategy()
```


Where the default [KeyCompleteFlushStrategy](KeyCompleteFlushStrategy.md#etielle.KeyCompleteFlushStrategy) uses plain `session.add()` (a duplicate row aborts the chunk's transaction with `IntegrityError`), this strategy resolves conflicts against rows that are *already stored*:

- `on_conflict="update"` (default): each instance is persisted with `session.merge()`. If a row with the same primary key exists, its columns are overwritten with the incoming values (last write wins); otherwise the row is inserted. Suited to idempotent re-runs.
- `on_conflict="skip"`: each instance is inserted inside a per-row `SAVEPOINT`; a row that raises `IntegrityError` (duplicate primary key or unique constraint, including a concurrent-insert race) is rolled back and skipped while the rest of the chunk proceeds. Suited to on-conflict-skip deduplication of streaming ingest.

Documented limitations:

- **SQLAlchemy only.** For Supabase, pass `load(upsert=True, upsert_on=...)` with the default strategy; the Supabase adapter performs native upserts.
- **Not a cross-chunk merge substitute.** Merge policies ([AddPolicy](AddPolicy.md#etielle.AddPolicy) etc.) run only within a chunk's mapping pass; across re-runs, [update](InstanceBuilder.update.md#etielle.InstanceBuilder.update) mode overwrites whole rows (including `None` values) rather than merging fields.
- **[update](InstanceBuilder.update.md#etielle.InstanceBuilder.update) mode detects conflicts by primary key.** Instances without primary key values are inserted as new rows, and each merge issues a per-row SELECT when the row is not already in the session's identity map. A concurrent insert between that SELECT and the INSERT can still raise `IntegrityError`; use `skip` mode where races must be tolerated.
- **`skip` mode swallows every `IntegrityError`,** not only duplicate keys (e.g. a NOT NULL violation also skips the row), and pays a per-row SAVEPOINT + flush round trip. A child bound to a skipped parent is itself skipped, because the cascaded parent insert reproduces the conflict inside the child's SAVEPOINT. Skipped rows are counted in `mapped` stats but in neither `inserted` nor `failed`.
- **Plain-dict rows** (string table targets) are not persisted, matching the default strategy.
