## PipelineBuilder.link_to()


Define a relationship from the current table to a parent table.


Usage

``` python
PipelineBuilder.link_to(
    parent,
    by,
    fk=None,
)
```


The `by` dict maps child field names to parent field names. Both Field and TempField names can be used.


## Parameters


`parent: type | str`  
The parent model class or table name string.

`by: dict[str, str]`  
Mapping of {child_field: parent_field}.

`fk: dict[str, str] | None = None`  
Mapping of {child_column: parent_column} for FK population (Supabase only). After inserting parents, child's FK column is populated with parent's generated ID.


## Returns


`PipelineBuilder`  
Self for method chaining.


## Raises


`ValueError`  
If called without a preceding map_to().


## Example

.map_to(table=Post, fields=\[ TempField("user_id", get("author_id"))\]) .link_to(User, by={"user_id": "id"})

Or with table names:

.map_to(table="posts", fields=\[…\]) .link_to("users", by={"user_id": "id"})

With DB-generated parent IDs (Supabase):

.map_to(table="posts", fields=\[ TempField("\_parent_key", get_from_parent("name"))\]) .link_to("users", by={"\_parent_key": "\_key"}, fk={"user_id": "id"})
