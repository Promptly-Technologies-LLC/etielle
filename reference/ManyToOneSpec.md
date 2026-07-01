## ManyToOneSpec


Declarative specification for a many-to-one relationship.


Usage

``` python
ManyToOneSpec(
    child_table, parent_table, attr, child_to_parent_key, required=True
)
```


- child_table: name used in `InstanceEmit.table` when emitting child instances
- parent_table: name used in `InstanceEmit.table` when emitting parent instances
- attr: attribute name on the child instance that references the parent instance
- child_to_parent_key: transforms evaluated in the child's traversal context that produce the composite logical key of the parent. Keys are computed during a dedicated traversal pass (see [compute_relationship_keys](compute_relationship_keys.md#etielle.compute_relationship_keys)).
- required: if True, binding fails when a parent cannot be found


## Parameter Attributes


`child_table: str`  

`parent_table: str`  

`attr: str`  

`child_to_parent_key: Sequence[Transform[Any]]`  

`required: bool = ``True`
