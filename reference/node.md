## node()


Return the current node value.


Usage

``` python
node(ctx)
```


Useful when iterating and the node itself is the value you want.


## Example

Data: {"ids": \[1, 2, 3\]}

.goto("ids").each() .map_to(table=Item, fields=\[ Field("id", node())\])
