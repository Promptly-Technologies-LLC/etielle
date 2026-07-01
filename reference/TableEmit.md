## TableEmit


Describes how to produce rows for a table from a given traversal context.


Usage

``` python
TableEmit(
    table,
    fields,
    join_keys,
)
```


- table: table name
- fields: list of computed fields
- join_keys: functions that compute the composite key for merging rows


## Parameter Attributes


`table: str`  

`fields: Sequence[Field]`  

`join_keys: Sequence[Transform[Any]]`
