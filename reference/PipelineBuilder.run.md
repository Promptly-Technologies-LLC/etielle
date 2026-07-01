## PipelineBuilder.run()


Execute the pipeline and return results.


Usage

``` python
PipelineBuilder.run(
    *,
    on_event=None,
)
```


If load() was called, also persists to the database.


## Parameters


`on_event: TelemetryCallback | None = None`  
Optional callback for telemetry events. Called with MapStarted, MapCompleted, FlushStarted, FlushCompleted, or FlushFailed events during pipeline execution.


## Returns


`PipelineResult`  
PipelineResult with tables, errors, and stats. When load() was

configured, flushed instances are not retained in result.tables

(stats and errors are always returned).
