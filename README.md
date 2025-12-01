# Quickstart: Declarative JSON-to-Relational Mapping in Python with `etielle`

`etielle` is a simple, powerful Python library for reshaping nested [JSON](https://en.wikipedia.org/wiki/JSON) data, typically from an API, into relational tables that fit your database schema. Think of `etielle` as a "JSON extractor" that you program with clear instructions: "Go here in the JSON, pull this data, and put it in that table." The library's name is a play on [ETL ("Extract, Transform, Load")](docs/introduction-to-etl.qmd), which is the technical term for this set of operations.

- **Repository**: [Promptly-Technologies-LLC/etielle](https://github.com/Promptly-Technologies-LLC/etielle)
- **PyPI**: [`etielle`](https://pypi.org/project/etielle/)
- **Python**: ≥ 3.13

## Why Use `etielle`? (For Beginners)

JSON data from APIs (Application Program Interfaces — web services that typically return JSON) is often deeply nested and requires complicated parsing. `etielle` helps by:

- **Traversing nested structures**: Walk through arrays-within-dictionaries-within-arrays to any arbitrary depth.
- **Performing arbitrary transformations**: Use the provided functions to perform common operations (like getting the key or index of the current item or its parent), or define your own custom ones.
- **Building relationships**: Link records across your different output tables and emit ORM relationships or foreign keys.
- **Emitting to arbitrary formats**: Emit data to Pydantic models, TypedDicts, or ORM objects directly instead of plain dicts, with validation and type safety.
- **Optionally loading data into a database**: Load data into a database using SQLAlchemy or SQLModel with performant one-shot flushing.

## Learning Path

1. [**Quickstart**](index.qmd): Quick and dirty introduction to `etielle` and how to use it.
2. [**Introduction to ETL**](docs/introduction-to-etl.qmd): The problem `etielle` is solving: JSON data ETL (Extract, Transform, and Load).
3. [**Traversals**](docs/traversals.qmd): How to tell `etielle` how to traverse your JSON data.
4. [**Transforms**](docs/transforms.qmd): Getting and altering values from the JSON data and mapping them in a type-safe way to your output tables.
5. [**Emissions**](docs/emissions.qmd): Outputting data to dictionaries, TypedDicts, Pydantic models, or ORM objects, with merge logic to construct single rows from different parts of the input JSON data.
6. [**Database upserts**](docs/loading-data-into-a-database.qmd): Optionally, creating relationships in memory and flushing data into a database with performant one-shot flushing.

## Installation

We recommend using `uv` for faster installs, but `pip` works too.

```bash
uv add etielle
# or
pip install etielle
```

### Optional: Install with ORM adapters

If you plan to bind relationships and flush to your database via SQLAlchemy or SQLModel, install with the optional extra for your ORM:

```bash
uv add "etielle[sqlalchemy]"
# or
uv add "etielle[sqlmodel]"
```

## Quick Start: Your First Mapping

Let's start with a simple example. Suppose you have this JSON:

```python
import json

data = {
  "users": [
    {"id": "u1", "name": "Alice", "posts": [{"id": "p1", "title": "Hello"}, {"id": "p2", "title": "World"}]},
    {"id": "u2", "name": "Bob", "posts": []}
  ]
}
```

We want two tables: "users" (id, name) and "posts" (id, user_id, title).

Here's the code using the **fluent API** (v3.x):

```python
from etielle import etl, Field, TempField, get, get_from_parent

# The fluent API reads in Extract → Transform → Load order
result = (
    etl(data)
    .goto("users").each()  # Extract: navigate to users array, iterate items
    .map_to(table="users", fields=[  # Transform: map to users table
        Field("id", get("id")),
        Field("name", get("name")),
        TempField("user_id", get("id"))  # TempField for joining, not persisted
    ])
    .goto("posts").each()  # Extract: nested navigation into posts
    .map_to(table="posts", fields=[  # Transform: map to posts table
        Field("id", get("id")),
        Field("user_id", get_from_parent("id")),  # Link to parent user
        Field("title", get("title"))
    ])
    .run()  # Load: execute the pipeline
)

# result.tables is a dict-like object keyed by table name
users_table = result.tables["users"]  # Dict[tuple, dict]
posts_table = result.tables["posts"]

# Convert to simple lists for display
out = {table: list(instances.values()) for table, instances in result.tables.items()}
print(json.dumps(out, indent=2))
```

Output:

```json
{
  "users": [
    {
      "id": "u1",
      "name": "Alice"
    },
    {
      "id": "u2",
      "name": "Bob"
    }
  ],
  "posts": [
    {
      "id": "p1",
      "user_id": "u1",
      "title": "Hello"
    },
    {
      "id": "p2",
      "user_id": "u1",
      "title": "World"
    }
  ]
}
```

Congrats! You've mapped your first JSON.

## Core Concepts: The Fluent API (v3.x)

The fluent API mirrors the conceptual Extract → Transform → Load flow:

### Pipeline Methods

| Method | Purpose |
|--------|---------|
| `etl(*roots, errors="collect")` | Entry point, accepts 1+ JSON objects |
| `.goto_root(index=0)` | Navigate to specific root (defaults to 0) |
| `.goto(path)` | Navigate relative path (string, dot-notation, or list) |
| `.each()` | Iterate list items or dict key-value pairs |
| `.map_to(table, fields, join_on=None, errors=None)` | Emit rows to table |
| `.link_to(Parent, by={...})` | Define relationship to parent table |
| `.load(session)` | Configure DB session for persistence |
| `.run()` | Execute pipeline, return result |

### Field Types

```python
from etielle import Field, TempField, get, literal
from etielle import AddPolicy

# Persisted field - appears in output
Field("name", get("name"))

# Persisted field with merge policy
Field("count", literal(1), merge=AddPolicy())

# Temporary field - used for joining/linking only, not persisted
TempField("id", get("id"))
```

### Built-in Transforms

| Transform | Purpose |
|-----------|---------|
| `get(path)` | Navigate from current node |
| `get_from_root(path)` | Navigate from JSON root |
| `get_from_parent(path, depth=1)` | Navigate from parent context |
| `key()` | Current dict key |
| `index()` | Current list index |
| `parent_key(depth=1)` | Parent's dict key |
| `parent_index(depth=1)` | Parent's list index |
| `node()` | Current node value |
| `literal(value)` | Constant value |
| `concat(*parts)` | String concatenation |
| `coalesce(*transforms)` | First non-null |
| `format_id(*parts, sep="_")` | Join with separator |

### Custom Transforms

Define your own transforms using the `@transform` decorator:

```python
from etielle import transform

@transform
def split_id(ctx, field: str, index: int) -> str:
    """Extract part of a composite ID."""
    return ctx.node[field].split("_")[index]

# Usage in field definition
TempField("user_id", split_id("composite_id", 0))
```

The `@transform` decorator handles currying automatically: `ctx` is always the first parameter, and remaining parameters become factory arguments.

### Navigation Examples

```python
# Single path segment
.goto("users")

# Dot notation
.goto("data.users.active")

# List of segments
.goto(["data", "users"])

# Iteration
.goto("users").each()  # Iterate over list/dict

# Nested iteration
.goto("users").each().goto("posts").each()  # Iterate users, then posts within each user

# Multiple roots
etl(users_json, profiles_json)
.goto_root(0).goto("users").each()  # First root
.goto_root(1).goto("profiles").each()  # Second root
```

### Merging Rows with join_on

When the same table is mapped from multiple traversals, rows with matching keys are merged:

```python
# First mapping (defines key implicitly via TempField)
.map_to(table="users", fields=[
    Field("name", get("name")),
    TempField("id", get("id"))
])

# Second mapping (requires join_on to specify which fields to match)
.map_to(table="users", join_on=["id"], fields=[
    Field("email", get("email")),
    TempField("id", get("user_id"))
])
```

### Merge Policies

Control how fields are updated when rows merge:

```python
from etielle import AddPolicy, ExtendPolicy, MaxPolicy

Field("count", literal(1), merge=AddPolicy())      # Accumulate numbers
Field("tags", get("tag"), merge=ExtendPolicy())    # Append to list
Field("score", get("score"), merge=MaxPolicy())    # Keep highest
```

Available: `AddPolicy`, `AppendPolicy`, `ExtendPolicy`, `MinPolicy`, `MaxPolicy`, `FirstNonNullPolicy`

### Relationships

Link child tables to parent tables:

```python
from etielle import etl, Field, TempField, get, get_from_parent

result = (
    etl(data)
    .goto("users").each()
    .map_to(table=User, fields=[
        Field("name", get("name")),
        TempField("id", get("id"))
    ])

    .goto("posts").each()
    .map_to(table=Post, fields=[
        Field("title", get("title")),
        TempField("id", get("id")),
        TempField("user_id", get_from_parent("id"))
    ])
    .link_to(User, by={"user_id": "id"})  # child.user_id == parent.id
    .run()
)

# Access posts with relationships bound
for key, post in result.tables[Post].items():
    print(f"{post.title} by {post.user.name}")
```

Multiple parents:

```python
.map_to(table=Comment, fields=[...])
.link_to(User, by={"author_id": "id"})
.link_to(Post, by={"post_id": "id"})
```

### Database Loading

```python
from sqlalchemy.orm import Session

# In-memory only - returns PipelineResult
result = pipeline.run()

# With database persistence - flushes to DB
result = (
    pipeline
    .load(session)  # Provide SQLAlchemy/SQLModel session
    .run()         # Builds instances, binds relationships, flushes to DB
)

session.commit()  # User controls transaction
```

### Error Handling

```python
# Global default
etl(data, errors="collect")      # Collect all errors (default)
etl(data, errors="fail_fast")    # Stop on first error

# Per-table override
.map_to(table=User, fields=[...], errors="fail_fast")

# Check errors in result
if result.errors:
    for table, errors in result.errors.items():
        for key, messages in errors.items():
            print(f"{table}[{key}]: {messages}")
```

### Model Type Detection

When using model classes with `.map_to(table=ModelClass, ...)`:

```python
from pydantic import BaseModel
from dataclasses import dataclass

class User(BaseModel):
    name: str
    email: str

# Automatically uses PydanticBuilder
result = (
    etl(data)
    .goto("users").each()
    .map_to(table=User, fields=[
        Field("name", get("name")),
        Field("email", get("email"))
    ])
    .run()
)

users = result.tables[User]  # Dict[tuple, User] - actual Pydantic instances
```

Auto-detection:
- `issubclass(cls, pydantic.BaseModel)` → PydanticBuilder
- `hasattr(cls, '__tablename__')` → SQLAlchemy/SQLModel ORM
- `is_typeddict(cls)` → TypedDictBuilder
- Otherwise → ConstructorBuilder
- `table="string"` → dict output (no validation)

## Complete Example: Users and Posts with Relationships

```python
from etielle import etl, Field, TempField, get, get_from_parent, transform
from etielle import AddPolicy
from sqlalchemy.orm import Session
from my_models import User, Post  # Your ORM models

# Define custom transform
@transform
def split_composite(ctx, field: str, index: int) -> str:
    return ctx.node[field].split("_")[index]

# Build and execute pipeline
result = (
    etl(data)

    # Extract users
    .goto("users").each()
    .map_to(table=User, fields=[
        Field("name", get("name")),
        Field("email", get("email")),
        TempField("id", get("id"))
    ])

    # Extract posts (nested under users)
    .goto("posts").each()
    .map_to(table=Post, fields=[
        Field("title", get("title")),
        Field("body", get("body")),
        TempField("id", get("id")),
        TempField("user_id", get_from_parent("id"))
    ])
    .link_to(User, by={"user_id": "id"})

    # Load to database
    .load(session)
    .run()
)

session.commit()

# Access with relationships
for key, post in result.tables[Post].items():
    print(f"{post.title} by {post.user.name}")
```

## Advanced Topics

- **Lazy Evaluation**: Transforms don't compute until executed, adapting to the current spot in JSON.
- **Custom Transforms**: Define your own functions that take Context and return values. See [Transforms](docs/transforms.qmd).
- **Row Merging Rules**: Rows with matching join keys merge; last write wins for duplicate fields.
- **Type-safe field selectors**: IDE autocomplete and compile-time typo detection. See [Developing with etielle](docs/developing-with-etielle.qmd).
- **Typed emissions**: Build Pydantic/TypedDict/ORM instances directly instead of dicts. See [Emissions](docs/emissions.qmd).
- **Merge policies**: Sum/append/min/max instead of overwrite when multiple traversals update the same field. See [Emissions](docs/emissions.qmd).
- **Error reporting**: Per-key diagnostics in results. See [Developing with etielle](docs/developing-with-etielle.qmd).
- **Relationships without extra round trips**: Bind in-memory, flush once. See [Relationships](docs/relationships.qmd) and [Database upserts](docs/loading-data-into-a-database.qmd).
- **Performance**: Efficient for large JSON; traversals are independent.

## Common Mistakes

- **Empty results?**
  - Check your `path` matches the JSON structure exactly
  - Verify the data type at that path matches expectations
  - Use `.goto()` and `.each()` in the right order
- **Missing parent data?**
  - Check the `depth` parameter in `get_from_parent()`
  - Ensure the parent context exists in your traversal
- **Duplicate or missing rows?**
  - Verify TempFields or join_keys are unique for each row
  - Check that join_keys don't contain `None` values (these rows are skipped)
- **Fields not persisting?**
  - Use `Field()` for persisted data, `TempField()` for join keys only

---

## Legacy API (v2.x)

The spec-based API from v2.x is still supported for backward compatibility but will be removed in v4.0. We recommend migrating to the fluent API for new projects.

### Legacy Quick Start

```python
from etielle.core import MappingSpec, TraversalSpec, TableEmit, Field
from etielle.transforms import get, get_from_parent
from etielle.executor import run_mapping

# A TraversalSpec tells etielle how to walk through your JSON
users_traversal = TraversalSpec(
    path=["users"],  # Path to the array
    mode="auto",  # Iterate automatically based on container
    emits=[
        TableEmit(
            table="users",
            join_keys=[get("id")],  # Unique key for the row
            fields=[
                Field("id", get("id")),
                Field("name", get("name"))
            ]
        )
    ]
)

# Nested traversal for posts
posts_traversal = TraversalSpec(
    path=["users"],
    mode="auto",
    inner_path=["posts"],  # Nested path inside each user
    inner_mode="auto",
    emits=[
        TableEmit(
            table="posts",
            join_keys=[get("id")],
            fields=[
                Field("id", get("id")),
                Field("user_id", get_from_parent("id")),  # Link to parent user
                Field("title", get("title"))
            ]
        )
    ]
)

spec = MappingSpec(traversals=[users_traversal, posts_traversal])
result = run_mapping(data, spec)

# result is a dict: {"users": MappingResult, "posts": MappingResult}
out = {table: list(mr.instances.values()) for table, mr in result.items()}
print(json.dumps(out, indent=2))
```

### Legacy Core Concepts

#### Context: Your "Location" in the JSON

Imagine traversing a JSON tree—Context is your GPS:

- `root`: The entire JSON.
- `node`: The current spot (e.g., a user object).
- `path`: Directions to get here (e.g., ("users", 0)).
- `parent`: The previous spot (for looking "up").
- `key`/`index`: If in a dict/list, the current key or index.
- `slots`: A notepad for temporary notes.

Contexts are created automatically as you traverse and are immutable (unchangeable) for safety.

#### Transforms: Smart Data Extractors

Transforms are like mini-functions that pull values from Context. They're "lazy"—they don't run until needed, and they adapt to the current Context.

Examples:

- `get("name")`: Get "name" from current node → `"Alice"` when node is `{"name": "Alice"}`
- `get_from_parent("id")`: Get "id" from parent context → `"u1"` when processing a post under user u1
- `index()`: Current list position → `0` for first item, `1` for second, etc.
- `concat(literal("user_"), get("id"))`: Combine strings → `"user_u1"`

#### TraversalSpec: How to Walk the JSON

This says: "Start here, then go deeper if needed, and do this for each item."

- `path`: Starting path (list of strings, e.g., ["users"]).
- `mode`: Iteration mode for the outer container: "auto" (default), "items", or "single".
- `inner_path`: Optional deeper path (e.g., ["posts"] for nesting).
- `inner_mode`: Iteration mode for the inner container: "auto" (default), "items", or "single".
- `emits`: What tables to create from each item.

You can have multiple Traversals in one MappingSpec—they run independently.

Here's a visual representation:

```
JSON structure:
root
└── users []                    ← path=["users"]
    ├── [0] {"id": "u1", ...}
    │   └── posts []            ← inner_path=["posts"]
    │       ├── [0] {"id": "p1", "title": "Hello"}
    │       └── [1] {"id": "p2", "title": "World"}
    └── [1] {"id": "u2", ...}
```

#### TableEmit and Fields: Building Your Tables

- `table`: Name of the table.
- `fields`: List of Field(name, transform) – columns and how to compute them.
- `join_keys`: List of transforms for unique row IDs (like primary keys). Same keys across traversals merge rows.

### Legacy Transform Cheatsheet

- **`get(path)`**: From current node (dot notation or list, e.g., "user.name" or ["user", 0]).
- **`get_from_parent(path, depth=1)`**: From ancestor.
- **`get_from_root(path)`**: From top-level JSON.
- **`key()`**: Current dict key.
- **`index()`**: Current list index.
- **`literal(value)`**: Constant value.
- **`concat(*parts)`**: Join strings.
- **`format_id(*parts, sep="_")`**: Join non-empty parts with separator.
- **`coalesce(*transforms)`**: First non-None value.
- **`len_of(inner)`**: Length of a list/dict/string.

Transforms compose naturally:

```python
user_key = concat(literal("user_"), get("id"))           # "user_123"
full_name = concat(get("first"), literal(" "), get("last"))  # "Alice Smith"
```

### Migration Guide (v2.x → v3.x)

| v2.x (Legacy) | v3.x (Fluent) |
|---------------|---------------|
| `MappingSpec(traversals=[...])` | `etl(data).goto(...).map_to(...)` |
| `TraversalSpec(path=["users"], mode="auto")` | `.goto("users").each()` |
| `TableEmit(table="users", ...)` | `.map_to(table="users", ...)` |
| `join_keys=[get("id")]` | `TempField("id", get("id"))` |
| `inner_path=["posts"]` | `.goto("posts").each()` |
| `run_mapping(data, spec)` | `.run()` |

---

## Glossary

- **Context**: Your current position while traversing the JSON tree
- **Transform**: A function that extracts values from a Context
- **Traversal**: Instructions for walking through part of the JSON
- **Emit**: Creating a table row from the current context
- **Join keys**: Values that uniquely identify a row (like primary keys)
- **Depth**: How many parent levels to traverse upward
- **Field**: A persisted column in the output table
- **TempField**: A temporary field used only for joining/linking, not persisted
- **Fluent API**: Builder pattern where methods chain together to describe the pipeline
- **Pipeline**: The complete extraction, transformation, and loading workflow

## License

MIT

Need help? Open an issue on GitHub!
