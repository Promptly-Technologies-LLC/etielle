## PipelineBuilder.goto()


Navigate to a relative path from the current position.


Usage

``` python
PipelineBuilder.goto(path)
```


## Parameters


`path: str | list[str]`  
Path segments as string (dot-separated) or list.


## Returns


`PipelineBuilder`  
Self for method chaining.


## Example

.goto("users") \# Single segment .goto("data.users") \# Dot notation .goto(\["data", "users"\]) \# List of segments
