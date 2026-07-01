## bind_many_to_one()


Bind child -\> parent object references in-place using plain attribute assignment.


Usage

``` python
bind_many_to_one(
    results,
    specs,
    child_to_parent,
    *,
    fail_on_missing=True,
)
```


- results: output of executor.run_mapping(root, spec)
- specs: relationship specs
- child_to_parent: sidecar keys as returned by [compute_relationship_keys](compute_relationship_keys.md#etielle.compute_relationship_keys)
- fail_on_missing: if True, raise RuntimeError aggregating missing parents
