## OneRecordPerChunkSource


Wrap an iterable of JSON roots; each root becomes its own chunk.


Usage

``` python
OneRecordPerChunkSource()
```


A re-iterable input (e.g. a list) can be streamed more than once. A single-use iterator (e.g. a generator or an `ijson` stream) is consumed on the first pass, matching the single-consumption nature of streaming sources; running the pipeline again would yield no chunks.


## Attributes

| Name | Description |
|----|----|
| [emits_sequential_only](#emits_sequential_only) | Returns True when the argument is true, False otherwise. |

------------------------------------------------------------------------


#### emits_sequential_only


Returns True when the argument is true, False otherwise.


`emits_sequential_only: bool = ``True`


The builtins True and False are the only two instances of the class bool. The class bool is a subclass of the class int, and cannot be subclassed.
