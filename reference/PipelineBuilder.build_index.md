## PipelineBuilder.build_index()


Build or seed a lookup index.


Usage

``` python
PipelineBuilder.build_index(
    name,
    *,
    from_dict=None,
    key=None,
    value=None,
)
```


Two modes: 1. from_dict: Seed index from an external dictionary 2. key + value: Build index from current traversal (must call after .each())


## Parameters


`name: str`  
Name for the index (used in lookup() calls)

`from_dict: dict[Any, Any] | None = None`  
External dictionary to use as the index

`key: Transform[Any] | None = None`  
Transform to compute index keys (traversal mode)

`value: Transform[Any] | None = None`  
Transform to compute index values (traversal mode)


## Returns


`PipelineBuilder`  
Self for method chaining.


Example (external dict): .build_index("db_ids", from_dict={"Q1": 42, "Q2": 43})

Example (traversal): .goto("questions").each() .goto("choice_ids").each() .build_index("parent_by_child", key=node(), value=get_from_parent("id"))
