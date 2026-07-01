## Context


Runtime context while traversing the JSON structure.


Usage

``` python
Context(
    root,
    node,
    path,
    parent,
    key,
    index,
    slots=dict(),
)
```


During traversal, a base context is created that is shared by all nodes. Subsequent contexts created during iteration extend the parent's path and link to the parent context. Each new context gets fresh slots, but you can walk up the chain with get_from_parent.

- root: original full JSON payload
- node: current node under iteration
- path: absolute path from root to this node (tuple of str\|int)
- parent: parent context if any
- key: current mapping key when iterating dicts (stringified)
- index: current index when iterating lists
- slots: scratchpad for intermediate identifiers if needed by transforms


## Parameter Attributes


`root: Any`  

`node: Any`  

`path: Tuple[str | int, …]`  

`parent: Optional[``"Context"]`  

`key: Optional[str]`  

`index: Optional[int]`  

`slots: dict[str, Any] = dict()`
