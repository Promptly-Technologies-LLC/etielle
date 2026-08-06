## FlushFailed


Emitted when an entire batch/table flush fails.


Usage

``` python
FlushFailed(
    table,
    error,
    affected_count,
)
```


## Attributes


`table: str`  
The table name that failed.

`error: str`  
Error message describing the failure.

`affected_count: int`  
Number of rows that were in the failed batch.
