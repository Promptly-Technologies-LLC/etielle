## PipelineBuilder.map_to()


Emit rows to a table from the current traversal position.


Usage

``` python
PipelineBuilder.map_to(
    table,
    fields,
    join_on=None,
    errors=None,
)
```


## Parameters


`table: str | type`  
Table name (string) or model class.

`fields: Sequence[FieldUnion]`  
List of Field and TempField definitions.

`join_on: Sequence[str] | None = None`  
Field names to use as composite key for merging. Required for subsequent map_to calls to the same table.

`errors: ErrorMode | None = None`  
Override the pipeline's error mode for this table.


## Returns


`PipelineBuilder`  
Self for method chaining.


## Example

.map_to(table=User, fields=\[ Field("name", get("name")), TempField("id", get("id"))\])
