## lookup()


Look up a value in a named index.


Usage

``` python
lookup(
    index_name,
    key_transform,
    *,
    default=None,
)
```


## Parameters


`index_name: str`  
Name of the index to query

`key_transform: Transform[Any]`  
Transform that computes the lookup key

`default: Any = None`  
Value to return if key not found (default: None)


## Returns


`Transform[Any]`  
Transform that returns the looked-up value


## Raises


`ValueError`  
If the index doesn't exist
