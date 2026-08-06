## MapCompleted


Emitted when mapping completes for a table.


Usage

``` python
MapCompleted(
    table,
    count,
    error_count,
)
```


## Attributes


`table: str`  
The table name being mapped.

`count: int`  
Number of instances created.

`error_count: int`  
Number of validation/transform errors.
