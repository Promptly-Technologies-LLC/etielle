## MappingResult


Unified result for both classic table rows and instance builders.


Usage

``` python
MappingResult(
    instances,
    update_errors,
    finalize_errors,
    stats,
    indices=dict(),
    lookup_values=dict()
)
```


- instances: mapping from composite join key tuple to instance/row payload
- update_errors: per-key errors recorded during incremental updates
- finalize_errors: per-key errors recorded while finalizing/validating instances
- stats: simple counters to aid diagnostics (keys: num_instances, num_update_errors, num_finalize_errors)
- indices: secondary indices for relationship linking {field_name: {value: instance}}
- lookup_values: per-key field values captured during mapping for relationship binding


## Parameter Attributes


`instances: dict[tuple[Any, …], T]`  

`update_errors: dict[tuple[Any, …], list[str]]`  

`finalize_errors: dict[tuple[Any, …], list[str]]`  

`stats: dict[str, int]`  

`indices: dict[str, dict[Any, T]] = dict()`\  

`lookup_values: dict[tuple[Any, …], dict[str, Any]] = dict()`\
