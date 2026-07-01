## PipelineBuilder.load_eager()


Mark a table as eagerly loaded and kept resident across components.


Usage

``` python
PipelineBuilder.load_eager(table)
```


Shared dimension tables referenced by many independent subgraphs should be loaded eagerly so component partitioning can proceed without collapsing the entire graph into one component.


## Parameters


`table: str | type`  
Model class or table name string declared via map_to().


## Returns


`PipelineBuilder`  
Self for method chaining.


## Example

etl(data) .goto("tags").each() .map_to(table=Tag, fields=\[…\]) .load_eager(Tag) .goto_root() …
