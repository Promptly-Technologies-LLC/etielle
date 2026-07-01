## PipelineBuilder


Fluent builder for E→T→L pipelines.


Usage

``` python
PipelineBuilder()
```


Use etl() to create instances of this class.


## Example

result = ( etl(data) .goto("users").each() .map_to(table=User, fields=\[…\]) .run() )
