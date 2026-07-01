## OneRecordPerChunkSource


Wrap an iterable of JSON roots; each root becomes its own chunk.


Usage

``` python
OneRecordPerChunkSource()
```


A re-iterable input (e.g. a list) can be streamed more than once. A single-use iterator (e.g. a generator or an `ijson` stream) is consumed on the first pass, matching the single-consumption nature of streaming sources; running the pipeline again would yield no chunks.
