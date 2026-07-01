## GroupByChunkSource


Group consecutive records that share a key into one chunk each.


Usage

``` python
GroupByChunkSource()
```


This is the single-pass, streaming group-by chunker. It reads the input once, accumulates consecutive records that map to the same [key](key.md#etielle.key), and emits a chunk whenever the key changes. Peak memory is one chunk: only the records for the current key are held at a time.

Each emitted chunk is [sequential](Chunk.md#etielle.Chunk.sequential) - every record in the group is mapped against pipeline root index 0 with shared auto-key counters, so a group of related records merges into one component (the repeated single-root shape).


## Grouped-Input Requirement

Correctness depends on the input *already being grouped (or sorted) by [key](key.md#etielle.key)*, which is the common shape for paginated APIs and "one parent subtree at a time" feeds. Because grouping is consecutive only, records that share a key but are separated by records with a different key land in *separate* chunks. With a relationship key that is fine for key-completeness but breaks relationship-completeness; the runtime relationship-completeness check raises if a chunk is missing endpoints. For unsorted input, sort by [key](key.md#etielle.key) first or use [ExternalPartitionChunkSource](ExternalPartitionChunkSource.md#etielle.ExternalPartitionChunkSource), the disk-backed partitioner that handles arbitrarily-ordered input.


## Choosing A Relationship-Complete Key

Pick a key that is a *complete component root* - coarse enough that every record reachable through a relationship from one record sharing the key also shares it (e.g. the owning entity id), not merely a fine merge key. Grouping guarantees key-completeness for whatever key you choose; the runtime validation catches a key that is too fine.


## Parameters


`records: Iterator[Any] | Iterable[Any]`  
An iterable (or single-use iterator) of JSON roots. Consumed exactly once.

`key: Callable[[Any], Any]`  
Function mapping a record to its grouping key. Adjacent records with equal keys are batched into the same chunk.
