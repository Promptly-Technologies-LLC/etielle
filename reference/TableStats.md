## TableStats


Statistics for a single table after pipeline execution.


Usage

``` python
TableStats(
    mapped,
    errors,
    inserted,
    failed,
)
```


## Parameter Attributes


`mapped: int`  

`errors: int`  

`inserted: int`  

`failed: int`  


## Attributes


`mapped: int`  
Number of instances created during the mapping phase.

`errors: int`  
Number of validation/transform errors during mapping.

`inserted: int`  
Number of rows successfully written to DB (0 if no session).

`failed: int`  
Number of rows that failed during flush.
