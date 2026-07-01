## transform()


Decorator to create custom transforms with curried arguments.


Usage

``` python
transform(func)
```


The decorated function must have `ctx: Context` as its first parameter. Any additional parameters become factory arguments.


## Example

<span class="citation" cites="transform">@transform</span> def split_id(ctx: Context, field: str, index: int) -\> str: return ctx.node\[field\].split("\_")\[index\]

Usage in field definition

TempField("user_id", split_id("composite_id", 0))


## Parameters


`func: Callable[…, Any]`  
A function with signature (ctx: Context, \*args) -\> T


## Returns


`Callable[…, Transform[Any]]`  
A factory function that returns Transform\[T\]
