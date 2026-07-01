## TempField


A field used only for joining/linking, not persisted.


Usage

``` python
TempField(
    name,
    transform,
)
```


TempFields are used to: - Compute join keys for merging rows - Store parent IDs for relationship linking

They do not appear in the final output objects.


## Parameters


`name: str`  
The field name (used in join_on and link_to).

`transform: Transform[Any]`  
How to compute the value from the current context.
