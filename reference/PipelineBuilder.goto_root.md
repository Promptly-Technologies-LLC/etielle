## PipelineBuilder.goto_root()


Navigate to a specific JSON root.


Usage

``` python
PipelineBuilder.goto_root(index=0)
```


Resets the current path and iteration state.


## Parameters


`index: int = ``0`  
Which root to navigate to (0-indexed, defaults to 0).


## Returns


`PipelineBuilder`  
Self for method chaining.


## Raises


`IndexError`  
If index is out of range.


## Example

etl(users_json, posts_json) .goto_root(0).goto("users").each() \# Process users .goto_root(1).goto("posts").each() \# Process posts
