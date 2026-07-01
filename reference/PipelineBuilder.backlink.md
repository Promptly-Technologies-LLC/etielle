## PipelineBuilder.backlink()


Define a many-to-many relationship where parent has a list of children.


Usage

``` python
PipelineBuilder.backlink(
    parent,
    child,
    attr,
    by,
)
```


This is used with ORMs like SQLModel/SQLAlchemy that handle junction tables automatically. After running the pipeline, the parent objects will have their list attribute populated with matching child objects.


## Parameters


`parent: type | str`  
The parent model class or table name that will hold the list.

`child: type | str`  
The child model class or table name.

`attr: str`  
The attribute name on the parent that holds the list of children.

`by: dict[str, str]`  
Mapping of {parent_field: child_field} where parent_field contains a list of values that match child_field on the child objects.


## Returns


`PipelineBuilder`  
Self for method chaining.


## Raises


`ValueError`  
If used with Supabase adapter (not supported).


## Example

Parent has choice_ids list, child has id field

.map_to(table=Question, fields=\[ Field("text", get("text")), TempField("id", get("id")), TempField("choice_ids", get("choice_ids")), \# list of child IDs\]) .goto_root() .goto("choices").each() .map_to(table=Choice, fields=\[ Field("text", get("text")), TempField("id", get("id")),\]) .backlink( parent=Question, child=Choice, attr="choices", \# sets question.choices = \[…\] by={"choice_ids": "id"}, \# parent's choice_ids contains child's id )
