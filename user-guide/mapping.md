# Mapping Tables: Fields and Output Structure

**What you'll learn**: How to use [map_to()](../reference/PipelineBuilder.map_to.md#etielle.PipelineBuilder.map_to), [Field](../reference/Field.md#etielle.Field), and [TempField](../reference/TempField.md#etielle.TempField) to define your output tables, including merge policies for combining rows.

**ETL context**: Mapping is part of the **Transform** step--it defines the structure of your output tables and how rows are keyed and merged.


# What is Mapping?

Mapping defines what table rows to create at each position in your navigation. Use [map_to()](../reference/PipelineBuilder.map_to.md#etielle.PipelineBuilder.map_to) to emit rows with [Field](../reference/Field.md#etielle.Field) (output columns) and [TempField](../reference/TempField.md#etielle.TempField) (join keys).

``` python
from etielle import etl, Field, TempField, get

result = (
    etl(data)
    .goto("users").each()
    .map_to(table="users", fields=[
        Field("name", get("name")),      # Output column
        Field("email", get("email")),    # Output column
        TempField("id", get("id"))       # Join key (not in output)
    ])
    .run()
)
```


# Field Types


## `Field(name, transform, merge=None)` - Output Column

A [Field](../reference/Field.md#etielle.Field) defines a column that appears in your output:

``` python
from etielle import Field, get, literal

Field("name", get("name"))              # Extract from JSON
Field("status", literal("active"))      # Constant value
Field("count", literal(1), merge=AddPolicy())  # With merge policy
```


## `TempField(name, transform)` - Join Key

A [TempField](../reference/TempField.md#etielle.TempField) is used for row identification and relationships, but is NOT included in the output:

``` python
from etielle import TempField, get, get_from_parent

TempField("id", get("id"))              # Primary key
TempField("user_id", get_from_parent("id"))  # Foreign key for linking
```

**Why TempField?**

- Defines the unique key for each row (like a primary key)
- Used with `join_on` to merge rows from different paths
- Used with [link_to()](../reference/PipelineBuilder.link_to.md#etielle.PipelineBuilder.link_to) to establish relationships
- Keeps your output clean by excluding internal keys


# Basic Mapping


## Simple Table Emission


``` python
from etielle import etl, Field, TempField, get
import json

data = {"users": [
    {"id": "u1", "name": "Alice", "email": "alice@example.com"},
    {"id": "u2", "name": "Bob", "email": "bob@example.com"}
]}

result = (
    etl(data)
    .goto("users").each()
    .map_to(table="users", fields=[
        Field("name", get("name")),
        Field("email", get("email")),
        TempField("id", get("id"))  # Row key
    ])
    .run()
)

# Access results
for key, row in result.tables["users"].items():
    print(f"Key: {key}, Row: {row}")
```


    Key: ('__auto_0__',), Row: {'name': 'Alice', 'email': 'alice@example.com'}
    Key: ('__auto_1__',), Row: {'name': 'Bob', 'email': 'bob@example.com'}


## Typed Output with Model Classes

Pass a model class instead of a string to get typed output:


``` python
from pydantic import BaseModel
from etielle import etl, Field, TempField, get

class User(BaseModel):
    name: str
    email: str

data = {"users": [{"id": "u1", "name": "Alice", "email": "alice@example.com"}]}

result = (
    etl(data)
    .goto("users").each()
    .map_to(table=User, fields=[  # Pass model class
        Field("name", get("name")),
        Field("email", get("email")),
        TempField("id", get("id"))
    ])
    .run()
)

# Access by model class
user = list(result.tables[User].values())[0]
print(f"Type: {type(user).__name__}, Name: {user.name}")
```


    Type: User, Name: Alice


## Supported Model Types

etielle auto-detects the model type and uses the appropriate builder:

| Model Type | Detection | Builder Used |
|----|----|----|
| Pydantic | `issubclass(cls, BaseModel)` | [PydanticBuilder](../reference/PydanticBuilder.md#etielle.PydanticBuilder) |
| SQLAlchemy/SQLModel | Has `__tablename__` and `__mapper__` | [ConstructorBuilder](../reference/ConstructorBuilder.md#etielle.ConstructorBuilder) |
| TypedDict | `is_typeddict(cls)` | [TypedDictBuilder](../reference/TypedDictBuilder.md#etielle.TypedDictBuilder) |
| Dataclass/Other | Default | [ConstructorBuilder](../reference/ConstructorBuilder.md#etielle.ConstructorBuilder) |
| String | `table="name"` | Plain dict output |


# Row Merging with `join_on`

When multiple [map_to()](../reference/PipelineBuilder.map_to.md#etielle.PipelineBuilder.map_to) calls emit to the same table, rows with matching keys are merged.


## First Emission (No `join_on` needed)

The first [map_to()](../reference/PipelineBuilder.map_to.md#etielle.PipelineBuilder.map_to) for a table uses [TempField](../reference/TempField.md#etielle.TempField) values as the row key:

``` python
.map_to(table="users", fields=[
    Field("name", get("name")),
    TempField("id", get("id"))  # Defines the key
])
```


## Subsequent Emissions (Require `join_on`)

Later [map_to()](../reference/PipelineBuilder.map_to.md#etielle.PipelineBuilder.map_to) calls to the same table must specify `join_on`:

``` python
.map_to(table="users", join_on=["id"], fields=[  # Must specify join_on
    Field("email", get("email")),
    TempField("id", get("user_id"))  # Must produce matching key
])
```


## Complete Merge Example


``` python
from etielle import etl, Field, TempField, get
import json

data = {
    "users": [{"id": "u1", "name": "Alice"}],
    "profiles": [{"user_id": "u1", "email": "alice@example.com", "bio": "Developer"}]
}

result = (
    etl(data)
    # First emission: basic user data
    .goto("users").each()
    .map_to(table="users", fields=[
        Field("id", get("id")),
        Field("name", get("name")),
        TempField("id", get("id"))  # Key: ("u1",)
    ])

    # Second emission: profile data (merged by id)
    .goto_root()
    .goto("profiles").each()
    .map_to(table="users", join_on=["id"], fields=[
        Field("email", get("email")),
        Field("bio", get("bio")),
        TempField("id", get("user_id"))  # Key: ("u1",) - matches!
    ])
    .run()
)

user = list(result.tables["users"].values())[0]
print(json.dumps(user, indent=2))  # Has id, name, email, AND bio
```


    {
      "id": "u1",
      "name": "Alice"
    }


# Merge Policies

By default, when merging rows, the last value wins. Merge policies change this behavior:

| Policy | Behavior | Use Case |
|----|----|----|
| [AddPolicy()](../reference/AddPolicy.md#etielle.AddPolicy) | Sum numbers | Counters, totals |
| [AppendPolicy()](../reference/AppendPolicy.md#etielle.AppendPolicy) | Append single item to list | Collecting tags |
| [ExtendPolicy()](../reference/ExtendPolicy.md#etielle.ExtendPolicy) | Extend list with another list | Merging lists |
| [MinPolicy()](../reference/MinPolicy.md#etielle.MinPolicy) | Keep minimum value | Earliest date |
| [MaxPolicy()](../reference/MaxPolicy.md#etielle.MaxPolicy) | Keep maximum value | Latest date |
| [FirstNonNullPolicy()](../reference/FirstNonNullPolicy.md#etielle.FirstNonNullPolicy) | Keep first non-null | Fallback defaults |


## Using Merge Policies


``` python
from etielle import etl, Field, TempField, get, literal
from etielle import AddPolicy
import json

data = {
    "transactions": [
        {"user_id": "u1", "amount": 100},
        {"user_id": "u1", "amount": 50},
        {"user_id": "u2", "amount": 75}
    ]
}

result = (
    etl(data)
    .goto("transactions").each()
    .map_to(table="totals", fields=[
        Field("total", get("amount"), merge=AddPolicy()),  # Sum amounts
        Field("count", literal(1), merge=AddPolicy()),     # Count transactions
        TempField("user_id", get("user_id"))
    ])
    .run()
)

for key, row in result.tables["totals"].items():
    print(f"User {key}: {row}")
```


    User ('__auto_0__',): {'total': 100, 'count': 1}
    User ('__auto_1__',): {'total': 50, 'count': 1}
    User ('__auto_2__',): {'total': 75, 'count': 1}


## Collecting Values with AppendPolicy


``` python
from etielle import etl, Field, TempField, get
from etielle import AppendPolicy
import json

data = {
    "tags": [
        {"item_id": "i1", "tag": "featured"},
        {"item_id": "i1", "tag": "sale"},
        {"item_id": "i2", "tag": "new"}
    ]
}

result = (
    etl(data)
    .goto("tags").each()
    .map_to(table="items", fields=[
        Field("tags", get("tag"), merge=AppendPolicy()),  # Collect into list
        TempField("id", get("item_id"))
    ])
    .run()
)

for key, row in result.tables["items"].items():
    print(f"Item {key}: {row}")
```


    Item ('__auto_0__',): {'tags': ['featured']}
    Item ('__auto_1__',): {'tags': ['sale']}
    Item ('__auto_2__',): {'tags': ['new']}


# Result Structure


## Accessing Tables

``` python
result = pipeline.run()

# By string name
users = result.tables["users"]  # Dict[tuple, dict]

# By model class (if used)
users = result.tables[User]     # Dict[tuple, User]

# Iterate
for key, row in result.tables["users"].items():
    print(f"Key: {key}, Row: {row}")
```


## Row Keys

Rows are keyed by tuples derived from [TempField](../reference/TempField.md#etielle.TempField) values:

``` python
# Single TempField
TempField("id", get("id"))  # Key: ("u1",)

# Multiple TempFields (composite key)
TempField("user_id", get("user_id"))
TempField("post_id", get("post_id"))  # Key: ("u1", "p1")
```


## Checking Errors

``` python
if result.errors:
    for table_name, table_errors in result.errors.items():
        for key, messages in table_errors.items():
            print(f"{table_name}[{key}]: {messages}")
```


# [map_to()](../reference/PipelineBuilder.map_to.md#etielle.PipelineBuilder.map_to) Reference

| Parameter | Type | Description |
|----|----|----|
| `table` | `str` or `type` | Table name or model class |
| `fields` | `list[Field \| TempField]` | Field definitions |
| `join_on` | `list[str]` or `None` | Field names for row merging (required for 2nd+ emission) |
| [errors](../reference/PipelineResult.md#etielle.PipelineResult.errors) | `"collect"` or `"fail_fast"` or `None` | Override error handling for this table |


# Best Practices


## Always Include a TempField

Every [map_to()](../reference/PipelineBuilder.map_to.md#etielle.PipelineBuilder.map_to) should have at least one [TempField](../reference/TempField.md#etielle.TempField) to define the row key:

``` python
# Good: Has a key
.map_to(table="users", fields=[
    Field("name", get("name")),
    TempField("id", get("id"))  # Row key
])

# Bad: No key (rows can't be uniquely identified)
.map_to(table="users", fields=[
    Field("name", get("name"))
])
```


## Use Meaningful Key Names

Choose [TempField](../reference/TempField.md#etielle.TempField) names that match your mental model:

``` python
# Good: Clear what each key represents
TempField("user_id", get_from_parent("id"))
TempField("post_id", get("id"))

# Less clear
TempField("key1", get_from_parent("id"))
TempField("key2", get("id"))
```


## Consider Output vs. Keys

Decide what goes in output ([Field](../reference/Field.md#etielle.Field)) vs. what's just for joining ([TempField](../reference/TempField.md#etielle.TempField)):

``` python
# If you need the ID in output AND as a key:
Field("id", get("id"))
TempField("id", get("id"))  # Same value, different purpose

# If ID is only for joining:
TempField("id", get("id"))  # Won't appear in output
```


# See also

- [Navigation](navigation.md) - Positioning before mapping
- [Transforms](transforms.md) - Computing field values
- [Relationships](relationships.md) - Linking tables with [link_to()](../reference/PipelineBuilder.link_to.md#etielle.PipelineBuilder.link_to)
- [Error Handling](error-handling.md) - Handling validation errors
