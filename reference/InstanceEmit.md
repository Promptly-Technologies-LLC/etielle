## InstanceEmit


InstanceEmit(table: str, join_keys: Sequence\[Callable\[\[etielle.core.Context\], Any\]\], fields: Sequence\[etielle.instances.FieldSpec\[+T\]\], builder: etielle.instances.InstanceBuilder\[+T\], policies: Mapping\[str, etielle.instances.MergePolicy\] = , strict_fields: bool = True, allow_extras: bool = False, strict_mode: str = 'collect_all', temp_fields: frozenset\[str\] = )


Usage

``` python
InstanceEmit(
    table,
    join_keys,
    fields,
    builder,
    policies=dict(),
    strict_fields=True,
    allow_extras=False,
    strict_mode="collect_all",
    temp_fields=frozenset()
)
```


## Parameter Attributes


`table: str`  

`join_keys: Sequence[Transform[Any]]`  

`fields: Sequence[FieldSpec[T]]`  

`builder: InstanceBuilder[T]`  

`policies: Mapping[str, MergePolicy] = dict()`    

`strict_fields: bool = ``True`  

`allow_extras: bool = ``False`  

`strict_mode: str = ``"collect_all"`  

`temp_fields: frozenset[str] = frozenset()`
