## PreSegmentedChunkSource


Pass an already-segmented iterable of chunks through unchanged.


Usage

``` python
PreSegmentedChunkSource()
```


Use this when the caller has already partitioned input into key-complete, relationship-complete [Chunk](Chunk.md#etielle.Chunk) objects (e.g. a producer that knows its own boundaries). The chunks are yielded in order without buffering, so peak memory is whatever the upstream iterable holds at a time.

A re-iterable input (e.g. a list) can be streamed more than once; a single-use iterator is consumed on the first pass.


## Parameters


`chunks: Iterator[Chunk] | Iterable[Chunk]`  
An iterable (or single-use iterator) of [Chunk](Chunk.md#etielle.Chunk) objects.
