## FlushContext


Inputs for a flush at a component boundary.


Usage

``` python
FlushContext(
    scope_tables,
    bind_context,
    local_results,
    dep_graph,
    link_to_rels,
    backlink_rels,
    stats,
    on_event,
    session,
    is_supabase,
    builder
)
```


The public fields provide everything a custom [FlushStrategy](FlushStrategy.md#etielle.FlushStrategy) needs to persist a component without touching engine internals:

- `scope_tables`: tables this flush is responsible for.
- `bind_context`: mapped results for the scope plus any resident/eager tables, used to resolve relationship parents.
- `local_results`: mapped results scoped to `scope_tables` only.
- `dep_graph`: child -\> parents dependency graph (use `etielle.utils.topological_sort` for flush order).
- `link_to_rels` / `backlink_rels`: relationship specs in scope.
- [stats](PipelineResult.md#etielle.PipelineResult.stats) / `on_event`: stats accumulator and telemetry sink.
- `session`: the SQLAlchemy session or Supabase client from [load()](PipelineBuilder.load.md#etielle.PipelineBuilder.load).
- `is_supabase`: whether `session` is a Supabase client.

`builder` is the engine handle that the built-in strategies use to reuse etielle's standard insert/bind logic. Custom strategies should rely on the public fields above and implement their own persistence rather than calling builder internals.


## Parameter Attributes


`scope_tables: set[str]`  

`bind_context: dict[str, Any]`  

`local_results: dict[str, Any]`  

`dep_graph: dict[str, set[str]]`  

`link_to_rels: list[dict[str, Any]]`  

`backlink_rels: list[dict[str, Any]]`  

`stats: dict[str, TableStats]`  

`on_event: TelemetryCallback | None`  

`session: Any`  

`is_supabase: bool`  

`builder: PipelineBuilder`
