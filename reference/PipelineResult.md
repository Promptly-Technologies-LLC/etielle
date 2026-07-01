## PipelineResult


Result from running a pipeline.


Usage

``` python
PipelineResult()
```


## Parameter Attributes


`tables: dict[str, dict[tuple[Any, …], Any]]`  

`errors: dict[str, dict[tuple[Any, …], list[str]]]`  

`_table_class_map: dict[str, type] | None = None`  

`_raw_results: dict[str, Any] | None = None`  

`_stats: dict[str, TableStats] | None = None`  


## Attributes


`tables: _TablesProxy`  
Access tables by string name or model class.

`errors: dict[str, dict[tuple[Any, …], list[str]]]`  
Validation/transform errors keyed by table then row key.

`stats: dict[str, TableStats]`  
Per-table statistics (mapped, errors, inserted, failed).


## Attributes

| Name | Description |
|----|----|
| [stats](#stats) | Per-table statistics. |

------------------------------------------------------------------------


#### stats


Per-table statistics.


`stats: dict[str, TableStats]`


Returns a dict mapping table names to TableStats objects with: - mapped: instances created during mapping - errors: validation/transform errors - inserted: rows successfully flushed to DB - failed: rows that failed during flush
