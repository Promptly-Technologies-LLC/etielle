## compute_relationship_keys()


Compute child-\>parent composite keys for each ManyToOneSpec by re-walking the


Usage

``` python
compute_relationship_keys(
    root,
    traversals,
    specs,
)
```


MappingSpec traversals. This avoids mutating domain objects and keeps the computed keys in a sidecar map keyed by the child's composite key.

Returns a dict keyed by the index of each ManyToOneSpec in `specs`, containing a mapping of child_composite_key -\> parent_composite_key for that spec.
