## PipelineBuilder


Fluent builder for E→T→L pipelines.


Usage

``` python
PipelineBuilder(
    roots,
    error_mode="collect",
    indices=None,
    *,
    chunk_source=None,
    flush_strategy=None,
    streaming=False
)
```


Use etl() to create instances of this class.


## Example

result = ( etl(data) .goto("users").each() .map_to(table=User, fields=\[…\]) .run() )
