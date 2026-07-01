## field_of()


Resolve a model field name from a type-checked selector lambda.


Usage

``` python
field_of(
    model,
    selector,
)
```


## Example

field_of(UserModel, lambda u: u.email) -\> "email"


## Constraints Enforced At Runtime

- Exactly one attribute access must occur.
- No method calls, indexing, or chained attribute access.
