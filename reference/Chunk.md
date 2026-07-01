## Chunk


A key-complete batch of JSON roots to map together.


Usage

``` python
Chunk(
    roots,
    sequential=False,
)
```


## Parameter Attributes


`roots: RootTuple`  

`sequential: bool = ``False`  


## Attributes


`roots: RootTuple`  
One or more JSON payloads for this chunk.

`sequential: bool`  
If True, every root is mapped against pipeline root index 0 with shared auto-key counters (group-by / repeated single-root records). If False, root at position *i* maps to pipeline root index *i* (multi-root [goto_root()](PipelineBuilder.goto_root.md#etielle.PipelineBuilder.goto_root) semantics).
