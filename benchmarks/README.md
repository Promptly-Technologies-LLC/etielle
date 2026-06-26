# Issue 9A benchmarks

Benchmark harness for component-scoped flush/evict (issue #74).

## Run before/after

Capture baseline on `main`:

```bash
uv run python benchmarks/bench_issue_9a.py run --scale 500 --output artifacts/bench-before.json
uv run python benchmarks/bench_issue_9a.py run --scale 500 --load --output artifacts/bench-before-load.json
```

After implementing issue 9A:

```bash
uv run python benchmarks/bench_issue_9a.py run --scale 500 --output artifacts/bench-after.json
uv run python benchmarks/bench_issue_9a.py run --scale 500 --load --output artifacts/bench-after-load.json
```

Compare:

```bash
uv run python benchmarks/bench_issue_9a.py compare \
  artifacts/bench-before-load.json artifacts/bench-after-load.json \
  --markdown artifacts/bench-comparison.md
```

## Metrics

- **wall_seconds**: end-to-end pipeline time
- **heap_peak_bytes**: Python heap peak via `tracemalloc`
- **rss_peak_kb**: process RSS peak via `resource.getrusage`
- **rows_mapped**: total mapped rows from `PipelineResult.stats`

## Scenarios

1. **no_relationships** — independent tables, no relationship graph
2. **many_components** — many isolated parent/child pairs (capped at 100 components)
3. **single_component** — one connected users/posts graph
4. **eager_dimension_without_eager** — shared tag table without `load_eager`
5. **eager_dimension_with_eager** — same shape with `load_eager(Tag)`

Use `--load` to measure load-mode behavior where flushed instances are not retained
in `PipelineResult.tables`.

# Issue 75 benchmark (streaming bounded memory)

`bench_issue_75.py` loads the same dataset two ways and reports Python heap peak
(`tracemalloc`) and process RSS peak during the run:

- **resident** — `etl(all_data).load(session).run()` (the only pre-#75 option)
- **streaming** — `stream(chunks).load(session).run()`

```bash
uv run python benchmarks/bench_issue_75.py --scale 8000
```

## Sample result (scale=8000, 2 KiB/row payload, in-memory SQLite)

| mode      | heap peak | rss peak  | wall  |
|-----------|-----------|-----------|-------|
| resident  | 45.5 MiB  | 227.9 MiB | 2.4s  |
| streaming | 0.23 MiB  | 227.9 MiB | 13.1s |

Streaming holds a flat ~0.23 MiB heap peak independent of dataset size (resident grows
linearly: 17.9 MiB at scale 3000, 45.5 MiB at scale 8000), confirming the bounded-memory
property. On the SQLAlchemy side this needs no explicit eviction: the identity map references
persistent instances weakly, so the GC reclaims them once etielle releases its per-chunk
accumulators. RSS is a process high-water mark and does not fall, so heap peak is the
meaningful signal here. The wall-time cost is the per-chunk flush overhead of streaming,
traded for the memory bound.
