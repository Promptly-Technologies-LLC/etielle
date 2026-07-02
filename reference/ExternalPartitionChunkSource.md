## ExternalPartitionChunkSource


Partition arbitrarily-ordered input into key-complete chunks via disk.


Usage

``` python
ExternalPartitionChunkSource()
```


This is the two-pass, disk-backed partitioner: pass one serializes every record to a temporary spill file and builds an in-memory key -\> offsets index; pass two emits one chunk per distinct key by reading that key's records back from disk. Unlike [GroupByChunkSource](GroupByChunkSource.md#etielle.GroupByChunkSource) it does not require the input to be grouped or sorted - records that share a key are collected into the same chunk no matter how far apart they arrive.


## Trade-Offs

Peak record memory is one chunk (the current partition), but the full dataset is written to temporary storage, and the offset index holds a few machine words per record for the duration of the stream. Pass two performs random reads, so a fast local temp filesystem is preferable.


## Serialization

Records are serialized with `dumps` (default `json.dumps`) and deserialized with `loads` (default `json.loads`), so chunks yield *reconstructed copies* of the input records. Non-JSON-serializable records need custom `dumps`/`loads` callables.


## Emission Order

Chunks are emitted in first-appearance order of their keys. Each chunk is [sequential](Chunk.md#etielle.Chunk.sequential) - every record in the partition is mapped against pipeline root index 0 with shared auto-key counters, matching [GroupByChunkSource](GroupByChunkSource.md#etielle.GroupByChunkSource).


## Choosing A Relationship-Complete Key

As with [GroupByChunkSource](GroupByChunkSource.md#etielle.GroupByChunkSource), pick a key that is a complete component root; partitioning guarantees key-completeness for the chosen key, and the runtime relationship-completeness check catches a key that is too fine.


## Parameters


`records: Iterator[Any] | Iterable[Any]`  
An iterable (or single-use iterator) of JSON roots. Consumed exactly once per [chunks()](ChunkSource.md#etielle.ChunkSource.chunks) iteration.

`key: Callable[[Any], Any]`  
Function mapping a record to its partition key. Must return a hashable value.

`dir: str | None = None`  
Optional directory for the temporary spill file (defaults to the platform temp directory). The file is deleted when iteration finishes or the iterator is closed.

`dumps: Callable[[Any], str] | None = None`  
Serializer from record to `str` (default `json.dumps`).

`loads: Callable[[str], Any] | None = None`  
Deserializer from `str` to record (default `json.loads`).


## Attributes

| Name | Description |
|----|----|
| [emits_sequential_only](#emits_sequential_only) | Returns True when the argument is true, False otherwise. |

------------------------------------------------------------------------


#### emits_sequential_only


Returns True when the argument is true, False otherwise.


`emits_sequential_only: bool = ``True`


The builtins True and False are the only two instances of the class bool. The class bool is a subclass of the class int, and cannot be subclassed.
