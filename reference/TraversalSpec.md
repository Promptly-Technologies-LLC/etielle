## TraversalSpec


How to reach and iterate a collection of nodes under root.


Usage

``` python
TraversalSpec()
```


Supports N-level nested iteration through the [levels](TraversalSpec.md#etielle.TraversalSpec.levels) parameter: - Each level represents one .each() call - Levels can have paths (for .goto().each()) or empty paths (for .each().each())

Legacy parameters (path, mode, inner_path, inner_mode) are still supported for backward compatibility and are converted to levels internally.

- path: list of keys from root to the outer container (e.g., \["blocks"\])
- mode: how to iterate the outer container: "auto" (default), "items" (dict key/value), or "single" (treat as one node)
- inner_path: optional path inside each outer node to reach an inner container (e.g., \["elements"\]). If provided, iterate that container instead of the outer node
- inner_mode: how to iterate the inner container when inner_path is provided: "auto" (default), "items", or "single"
- levels: list of IterationLevel for N-level nested iteration (overrides path/inner_path if provided)
- emits: table emitters to run for each yielded node


## Parameter Attributes


`path: Sequence[str]`  

`emits: Sequence[TableEmit | ``"InstanceEmit[Any]"]`  

`mode: Literal[``"auto", `<span class="st">`"items"``, ``"single"``]`</span>` = ``"auto"`  

`inner_path: Optional[Sequence[str]] = None`  

`inner_mode: Literal[``"auto", `<span class="st">`"items"``, ``"single"``]`</span>` = ``"auto"`  

`levels: Optional[Sequence[IterationLevel]] = None`  


## Methods

| Name | Description |
|----|----|
| [get_levels()](#get_levels) | Get iteration levels, converting from legacy format if needed. |

------------------------------------------------------------------------


#### get_levels()


Get iteration levels, converting from legacy format if needed.


Usage

``` python
get_levels()
```
