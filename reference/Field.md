## Field


A field that will be persisted to the output table.


Usage

``` python
Field(
    name,
    transform,
    merge=None,
)
```


## Parameters


`name: str`  
The column/attribute name in the output.

`transform: Transform[Any]`  
How to compute the value from the current context.

`merge: MergePolicy | None = None`  
Optional policy for merging values when rows are combined.
