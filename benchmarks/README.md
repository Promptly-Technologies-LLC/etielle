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
