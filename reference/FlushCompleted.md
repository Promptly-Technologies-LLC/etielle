## FlushCompleted


Emitted when flush completes for a table (or batch).


Usage

``` python
FlushCompleted(
    table,
    inserted,
    failed,
    batch_num,
    batch_total,
    upsert,
)
```


## Parameter Attributes


`table: str`  

`inserted: int`  

`failed: int`  

`batch_num: int`  

`batch_total: int`  

`upsert: bool`  


## Attributes


`table: str`  
The table name being flushed.

`inserted: int`  
Number of rows successfully inserted/upserted.

`failed: int`  
Number of rows that failed.

`batch_num: int`  
Which batch (1-indexed).

`batch_total: int`  
Total number of batches for this table.

`upsert: bool`  
True if this was an upsert operation.
