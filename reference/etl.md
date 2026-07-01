## etl()


Entry point for fluent E→T→L pipelines.


Usage

``` python
etl(
    *roots,
    errors="collect",
    indices=None,
)
```


## Parameters


`*roots: Any`  
One or more JSON objects to process.

`errors: ErrorMode = ``"collect"`  
Error handling mode - "collect" (default) or "fail_fast".

`indices: dict[str, dict[Any, Any]] | None = None`  
Pre-built lookup indices for use with lookup() transform.


## Returns


`PipelineBuilder`  
A PipelineBuilder for chaining navigation and mapping calls.


## Example

result = ( etl(data) .goto("users").each() .map_to(table=User, fields=\[ Field("name", get("name")) \]) .run() )
