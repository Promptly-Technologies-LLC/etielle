## stream()


Entry point for streaming/chunked E→T→L pipelines.


Usage

``` python
stream(
    source,
    *,
    eager_roots=None,
    flush_strategy=None,
    errors="collect",
    indices=None
)
```


Each chunk must be key-complete and relationship-complete. Streaming execution requires [load()](PipelineBuilder.load.md#etielle.PipelineBuilder.load) before [run()](PipelineBuilder.run.md#etielle.PipelineBuilder.run).


## Parameters


`source: Any`  
A [ChunkSource](ChunkSource.md#etielle.ChunkSource) or iterable of JSON roots (one root per chunk).

`eager_roots: Any | tuple[Any, …] | None = None`  
Optional resident JSON root(s) for [load_eager()](PipelineBuilder.load_eager.md#etielle.PipelineBuilder.load_eager) tables.

`flush_strategy: Any | None = None`  
Optional flush strategy (defaults to [KeyCompleteFlushStrategy](KeyCompleteFlushStrategy.md#etielle.KeyCompleteFlushStrategy)).

`errors: ErrorMode = ``"collect"`  
Error handling mode - `collect` (default) or `fail_fast`.

`indices: dict[str, dict[Any, Any]] | None = None`  
Pre-built lookup indices for use with [lookup()](lookup.md#etielle.lookup) transform.


## Returns


`PipelineBuilder`  
A [PipelineBuilder](PipelineBuilder.md#etielle.PipelineBuilder) for chaining navigation and mapping calls.
