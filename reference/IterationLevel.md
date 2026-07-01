## IterationLevel


Represents a single level of iteration in a traversal.


Usage

``` python
IterationLevel(
    path,
    mode="auto",
)
```


- path: path segments to navigate before iterating (can be empty for consecutive .each() calls on the same container)
- mode: how to iterate: "auto" (detect list/dict), "items", or "single"


## Parameter Attributes


`path: Sequence[str]`  

`mode: Literal[``"auto", `<span class="st">`"items"``, ``"single"``]`</span>` = ``"auto"`
