## BufferedKeyFlushStrategy


Streaming strategy that merges late-arriving rows for recently seen keys.


Usage

``` python
BufferedKeyFlushStrategy()
```


Keeps a bounded LRU cache of the last `max_keys` flushed `(table, join key)` -\> instance entries. When a later chunk maps a row whose key is still cached, the row is *not* inserted again; instead its non-None scalar attribute values are copied onto the already-persisted instance, which SQLAlchemy turns into an UPDATE at the next flush. Rows with new keys are inserted normally and recorded in the cache.

Children mapped alongside a re-appearing parent are relinked to the originally persisted parent instance, so no duplicate parent row is inserted through relationship cascades.

Documented limitations:

- **Correctness is a heuristic bounded by `max_keys`.** The cache assumes key reappearance distance is bounded: once a key is evicted, a reappearing row is inserted as a new row (or raises `IntegrityError` under a unique constraint). This is not a guarantee - size the cache for the worst-case reappearance gap or fall back to [ExternalPartitionChunkSource](ExternalPartitionChunkSource.md#etielle.ExternalPartitionChunkSource) for exact grouping.
- **Requires natural keys.** Only tables with `join_on` participate; auto-keyed rows are always inserted because auto keys restart per chunk and would collide spuriously.
- **Merge is last-non-None-write-wins per scalar attribute.** Merge policies ([AddPolicy](AddPolicy.md#etielle.AddPolicy) etc.) run only within a chunk's mapping pass; collection-valued attributes (e.g. [backlink()](PipelineBuilder.backlink.md#etielle.PipelineBuilder.backlink) lists) are not merged across chunks.
- **Relationship completeness is still validated per chunk.** A child whose parent appears only in an earlier chunk is rejected before the strategy runs; the cache merges repeated *rows*, it does not relax the chunk contract for relationships.
- **Stateful across the run.** Use a fresh instance per pipeline run; merged (deduplicated) rows are counted in `mapped` stats but not in `inserted`.
- **SQLAlchemy only**; plain-dict rows are not persisted, matching the default strategy.


## Parameters


`max_keys: int = ``10000`  
Maximum number of `(table, key)` entries to retain. Bounds strategy memory to at most `max_keys` live instances.
