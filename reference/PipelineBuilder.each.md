## PipelineBuilder.each()


Iterate over items at the current path.


Usage

``` python
PipelineBuilder.each()
```


For lists: iterates by index. For dicts: iterates key-value pairs.

Can be chained for nested iteration: - `.each().each()` on dict-of-lists: first iterates dict keys, second iterates list values. Use [parent_key()](parent_key.md#etielle.parent_key) to get the dict key. - `.each().each()` on list-of-lists: first iterates outer list, second iterates inner lists. Use [parent_index()](parent_index.md#etielle.parent_index) to get outer index. - `.each().goto("field").each()`: iterates outer container, then navigates to a nested field before iterating.


## Returns


`PipelineBuilder`  
Self for method chaining.


## Example

.goto("users").each() \# Iterate list .goto("mapping").each().each() \# Dict of lists .goto("grid").each().each() \# 2D array .goto("items").each().goto("tags").each() \# Nested objects
