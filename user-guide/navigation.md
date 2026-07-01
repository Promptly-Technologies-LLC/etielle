# Navigation: Traversing JSON Structure

**What you'll learn**: How to use [goto()](../reference/PipelineBuilder.goto.md#etielle.PipelineBuilder.goto), [each()](../reference/PipelineBuilder.each.md#etielle.PipelineBuilder.each), and [goto_root()](../reference/PipelineBuilder.goto_root.md#etielle.PipelineBuilder.goto_root) to navigate nested JSON structures and control iteration behavior.

**ETL context**: Navigation is the **Extract** step--it tells etielle *where* to find data in your JSON and *how* to iterate through it.


# What is Navigation?

Navigation defines a path through your JSON structure. Think of it as giving directions: "Start at the 'users' key, then loop through each item in that array."

``` python
from etielle import etl, Field, TempField, get

result = (
    etl(data)
    .goto("users")   # Navigate to the "users" key
    .each()          # Iterate over each item
    .map_to(table="users", fields=[...])
    .run()
)
```


# Path Navigation with [goto()](../reference/PipelineBuilder.goto.md#etielle.PipelineBuilder.goto)

The [goto()](../reference/PipelineBuilder.goto.md#etielle.PipelineBuilder.goto) method navigates to nested locations in your JSON:


``` python
from etielle import etl

# Simple path: data["users"]
etl(data).goto("users")

# Dot notation: data["response"]["data"]["users"]
etl(data).goto("response.data.users")

# List syntax: data["pages"][0]["items"]
etl(data).goto(["pages", 0, "items"])

# Chained calls work too
etl(data).goto("response").goto("data").goto("users")
```


## Path Syntax Options

| Syntax       | Example                   | Result                  |
|--------------|---------------------------|-------------------------|
| Single key   | `goto("users")`           | `data["users"]`         |
| Dot notation | `goto("data.users")`      | `data["data"]["users"]` |
| List of keys | `goto(["data", "users"])` | `data["data"]["users"]` |
| With index   | `goto(["pages", 0])`      | `data["pages"][0]`      |


# Iteration with [each()](../reference/PipelineBuilder.each.md#etielle.PipelineBuilder.each)

The [each()](../reference/PipelineBuilder.each.md#etielle.PipelineBuilder.each) method iterates over items at the current path:

- **For lists**: iterates by index (0, 1, 2, …)
- **For dicts**: iterates key-value pairs


``` python
from etielle import etl, Field, TempField, get, key
import json

# Example: Iterating a dict by key-value pairs
data = {
    "settings": {
        "theme": "dark",
        "language": "en",
        "notifications": "enabled"
    }
}

result = (
    etl(data)
    .goto("settings").each()  # Iterate key-value pairs
    .map_to(table="settings", fields=[
        Field("name", key()),       # "theme", "language", etc.
        Field("value", get([])),    # "dark", "en", etc. (empty path = current node)
        TempField("name", key())
    ])
    .run()
)

print(json.dumps(list(result.tables["settings"].values()), indent=2))
```


    [
      {
        "name": "theme",
        "value": "dark"
      },
      {
        "name": "language",
        "value": "en"
      },
      {
        "name": "notifications",
        "value": "enabled"
      }
    ]


# Nested Iteration

For parent-child relationships (users -\> posts, orders -\> items), chain [goto()](../reference/PipelineBuilder.goto.md#etielle.PipelineBuilder.goto) and [each()](../reference/PipelineBuilder.each.md#etielle.PipelineBuilder.each):


``` python
from etielle import etl, Field, TempField, get, get_from_parent
import json

data = {
    "users": [
        {"id": "u1", "name": "Alice", "posts": [
            {"id": "p1", "title": "Hello"},
            {"id": "p2", "title": "World"}
        ]},
        {"id": "u2", "name": "Bob", "posts": []}
    ]
}

result = (
    etl(data)
    # Outer iteration: users
    .goto("users").each()
    .map_to(table="users", fields=[
        Field("id", get("id")),
        Field("name", get("name")),
        TempField("id", get("id"))
    ])

    # Inner iteration: posts within each user
    .goto("posts").each()
    .map_to(table="posts", fields=[
        Field("id", get("id")),
        Field("user_id", get_from_parent("id")),  # Link to parent user
        Field("title", get("title")),
        TempField("id", get("id"))
    ])
    .run()
)

print("Users:", list(result.tables["users"].values()))
print("Posts:", list(result.tables["posts"].values()))
```


    Users: [{'id': 'u1', 'name': 'Alice'}, {'id': 'u2', 'name': 'Bob'}]
    Posts: [{'id': 'p1', 'user_id': 'u1', 'title': 'Hello'}, {'id': 'p2', 'user_id': 'u1', 'title': 'World'}]


# Deep Nesting (Arbitrary Depth)

Handle arbitrarily deep structures with chained navigation. Use the `depth` parameter in [get_from_parent()](../reference/get_from_parent.md#etielle.get_from_parent) to access ancestors:

- `get_from_parent("id")` or `depth=1` -\> immediate parent
- `get_from_parent("id", depth=2)` -\> grandparent
- `get_from_parent("id", depth=3)` -\> great-grandparent


``` python
# servers -> channels -> messages -> reactions (3 levels deep)
result = (
    etl(data)
    .goto("servers").each()
    .map_to(table="servers", fields=[
        Field("id", get("id")),
        Field("name", get("name")),
        TempField("id", get("id"))
    ])

    .goto("channels").each()
    .map_to(table="channels", fields=[
        Field("id", get("id")),
        Field("server_id", get_from_parent("id")),  # depth=1
        Field("name", get("name")),
        TempField("id", get("id"))
    ])

    .goto("messages").each()
    .map_to(table="messages", fields=[
        Field("id", get("id")),
        Field("channel_id", get_from_parent("id")),        # depth=1
        Field("server_id", get_from_parent("id", depth=2)), # depth=2
        TempField("id", get("id"))
    ])

    .goto("reactions").each()
    .map_to(table="reactions", fields=[
        Field("emoji", get("emoji")),
        Field("message_id", get_from_parent("id")),        # depth=1
        Field("channel_id", get_from_parent("id", depth=2)), # depth=2
        Field("server_id", get_from_parent("id", depth=3)),  # depth=3
        TempField("id", get("id"))
    ])
    .run()
)
```


# Multiple JSON Roots with [goto_root()](../reference/PipelineBuilder.goto_root.md#etielle.PipelineBuilder.goto_root)

When processing multiple JSON objects, use `goto_root(index)` to switch between them:


``` python
from etielle import etl, Field, TempField, get

users_json = {"users": [{"id": "u1", "name": "Alice"}]}
profiles_json = {"profiles": [{"user_id": "u1", "bio": "Developer"}]}

result = (
    etl(users_json, profiles_json)  # Pass multiple JSON objects

    # Process first root (index 0)
    .goto_root(0)
    .goto("users").each()
    .map_to(table="users", fields=[
        Field("name", get("name")),
        TempField("id", get("id"))
    ])

    # Switch to second root (index 1)
    .goto_root(1)
    .goto("profiles").each()
    .map_to(table="users", join_on=["id"], fields=[
        Field("bio", get("bio")),
        TempField("id", get("user_id"))  # Matches users by id
    ])

    .run()
)

user = list(result.tables["users"].values())[0]
print(user)  # Has both name and bio merged together
```


    {'name': 'Alice'}


## [goto_root()](../reference/PipelineBuilder.goto_root.md#etielle.PipelineBuilder.goto_root) Behavior

- Resets the current navigation path
- Resets the iteration state
- Defaults to index 0 if no argument provided
- Raises `IndexError` if index is out of range


# Resetting Navigation with [goto_root()](../reference/PipelineBuilder.goto_root.md#etielle.PipelineBuilder.goto_root)

Use [goto_root()](../reference/PipelineBuilder.goto_root.md#etielle.PipelineBuilder.goto_root) (without arguments) to return to the root of the current JSON and start a new navigation path:


``` python
from etielle import etl, Field, TempField, get

data = {
    "users": [{"id": "u1", "name": "Alice"}],
    "products": [{"id": "prod1", "name": "Widget"}]
}

result = (
    etl(data)
    # First path: users
    .goto("users").each()
    .map_to(table="users", fields=[
        Field("id", get("id")),
        Field("name", get("name")),
        TempField("id", get("id"))
    ])

    # Reset to root, then navigate to products
    .goto_root()
    .goto("products").each()
    .map_to(table="products", fields=[
        Field("id", get("id")),
        Field("name", get("name")),
        TempField("id", get("id"))
    ])
    .run()
)

print("Users:", list(result.tables["users"].values()))
print("Products:", list(result.tables["products"].values()))
```


    Users: [{'id': 'u1', 'name': 'Alice'}]
    Products: [{'id': 'prod1', 'name': 'Widget'}]


# Row Merging with `join_on`

When multiple [map_to()](../reference/PipelineBuilder.map_to.md#etielle.PipelineBuilder.map_to) calls emit to the same table, rows with matching keys are merged:


``` python
from etielle import etl, Field, TempField, get

data = {
    "users": [{"id": "u1", "name": "Alice"}],
    "profiles": [{"user_id": "u1", "email": "alice@example.com"}]
}

result = (
    etl(data)
    # First emission: basic user data
    .goto("users").each()
    .map_to(table="users", fields=[
        Field("id", get("id")),
        Field("name", get("name")),
        TempField("id", get("id"))  # Key: u1
    ])

    # Second emission: add profile data to same table
    .goto_root()
    .goto("profiles").each()
    .map_to(table="users", join_on=["id"], fields=[  # join_on required for second emission
        Field("email", get("email")),
        TempField("id", get("user_id"))  # Key: u1 (matches above)
    ])
    .run()
)

user = list(result.tables["users"].values())[0]
print(user)  # {"id": "u1", "name": "Alice", "email": "alice@example.com"}
```


    {'id': 'u1', 'name': 'Alice'}


## `join_on` Rules

- First [map_to](../reference/PipelineBuilder.map_to.md#etielle.PipelineBuilder.map_to) for a table doesn't need `join_on`
- Subsequent [map_to](../reference/PipelineBuilder.map_to.md#etielle.PipelineBuilder.map_to) calls for the same table require `join_on`
- `join_on` references field names (from [Field](../reference/Field.md#etielle.Field) or [TempField](../reference/TempField.md#etielle.TempField))
- Rows with matching keys are merged


# Navigation Reference

| Method | Purpose | Example |
|----|----|----|
| `goto(path)` | Navigate to a nested path | `.goto("users")` or `.goto("data.users")` |
| [each()](../reference/PipelineBuilder.each.md#etielle.PipelineBuilder.each) | Iterate over items at current path | `.goto("users").each()` |
| `goto_root(index)` | Switch to a different JSON root | `.goto_root(1)` for second root |
| [goto_root()](../reference/PipelineBuilder.goto_root.md#etielle.PipelineBuilder.goto_root) | Reset to root of current JSON | `.goto_root()` to start fresh |


# See also

- [Transforms](transforms.md) - Extracting values at each navigation position
- [Mapping Tables](mapping.md) - Defining output structure with [Field](../reference/Field.md#etielle.Field) and [TempField](../reference/TempField.md#etielle.TempField)
