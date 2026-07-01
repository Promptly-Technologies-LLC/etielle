## parent_index()


Return the list index of an ancestor context.


Usage

``` python
parent_index(
    ctx,
    depth=1,
)
```


## Parameters


`depth: int = ``1`  
How many levels up to look (1 = parent, 2 = grandparent).


## Returns


`int | None`  
The index if the ancestor was iterating a list, None otherwise.


## Example

Data: {"rows": \[\[1, 2\], \[3, 4\]\]}

.goto("rows").each().each() .map_to(table=Cell, fields=\[ Field("row_num", parent_index()), \# 0 or 1 Field("value", node())\])
