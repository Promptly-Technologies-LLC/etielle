# `etielle`: Declarative JSON-to-Relational Mapping in Python


`etielle` is a simple, powerful Python library for reshaping nested
[JSON](https://en.wikipedia.org/wiki/JSON) data, typically from an API,
into relational tables that fit your database schema. Think of `etielle`
as a “JSON extractor” that you program with clear instructions: “Go here
in the JSON, pull this data, and put it in that table.” The library’s
name is a play on ETL (“Extract, Transform, Load”), which is the
technical term for this set of operations.

- **Repository**:
  [Promptly-Technologies-LLC/etielle](https://github.com/Promptly-Technologies-LLC/etielle)
- **PyPI**: [`etielle`](https://pypi.org/project/etielle/)
- **Python**: ≥ 3.13

## Why Use `etielle`? (For Beginners)

JSON data from APIs (Application Program Interfaces—web services that
typically return JSON) is often deeply nested and requires complicated
parsing. `etielle` helps by:

- **Declaring what you want**: Write Python code to describe your tables
  and how to fill them.
- **Traversing nested structures**: Walk through
  arrays-within-dictionaries-within-arrays to any arbitrary depth.
- **Performing arbitrary transformations**: Use the provided functions
  to perform common operations (like getting the key or index of the
  current item or its parent), or define your own.
- **Building relationships**: Link records across your different output
  tables and emit ORM relationships or foreign keys.
- **Being beginner-friendly**: Everything is type-safe (Python checks
  your types), composable (build complex things from simple pieces), and
  easy to debug.

## Learning Path

1.  **Start here**: Follow the Quick Start example below to see basic
    mapping
2.  **Understand the pieces**: Read Core Concepts to learn about
    Context, Transforms, and TraversalSpec
3.  **Go deeper**: Explore the detailed examples for nesting and merging
4.  **Advanced features**: Check out the docs/ folder for instance
    emission, relationships, and more

## Installation

We recommend using `uv` for faster installs, but `pip` works too.

### With uv (Recommended for Speed)

For your project:

``` bash
uv add etielle
```

For one-off use:

``` bash
uv pip install etielle
```

### With pip

``` bash
pip install etielle
```

### Optional: SQLAlchemy adapter

If you plan to bind relationships and flush via SQLAlchemy in one go,
install the optional extra:

``` bash
uv add "etielle[sqlalchemy]"
```

### Optional: SQLModel adapter

If you plan to bind relationships and flush via SQLModel in one go,
install the optional extra:

``` bash
uv add "etielle[sqlmodel]"
```

## Quick Start: Your First Mapping

Let’s start with a simple example. Suppose you have this JSON:

``` python
import json

data = {
  "users": [
    {"id": "u1", "name": "Alice", "posts": [{"id": "p1", "title": "Hello"}, {"id": "p2", "title": "World"}]},
    {"id": "u2", "name": "Bob", "posts": []}
  ]
}
```

We want two tables: “users” (id, name) and “posts” (id, user_id, title).

Here’s the code:

``` python
from etielle.core import MappingSpec, TraversalSpec, TableEmit, Field  # Core building blocks
from etielle.transforms import get, get_from_parent  # Functions to pull data from JSON
from etielle.executor import run_mapping  # The engine that runs everything

# A TraversalSpec tells etielle how to walk through your JSON. Think of it as
# giving directions: "Start at the 'users' key, then loop through each item in that array."

# Traverse users array
users_traversal = TraversalSpec(
    path=["users"],  # Path to the array
    mode="auto",  # Iterate automatically based on container
    emits=[
        # The join_keys identify each unique row—like a primary key in a database.
        # Rows with matching keys will be merged together.
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

# This second traversal is nested: first we navigate to each user,
# then for each user we go into their posts array using inner_path.
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
# Each MappingResult has .instances (a dict keyed by join_keys)
# Let's convert to simple lists for display:
out = {table: list(mr.instances.values()) for table, mr in result.items()}
print(json.dumps(out, indent=2))
```

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

Congrats! You’ve mapped your first JSON.

## Core Concepts: Breaking It Down

Let’s explain the building blocks like you’re learning for the first
time.

### 1. Context: Your “Location” in the JSON

Imagine traversing a JSON tree—Context is your GPS:

- `root`: The entire JSON.
- `node`: The current spot (e.g., a user object).
- `path`: Directions to get here (e.g., (“users”, 0)).
- `parent`: The previous spot (for looking “up”).
- `key`/`index`: If in a dict/list, the current key or index.
- `slots`: A notepad for temporary notes.

Contexts are created automatically as you traverse and are immutable
(unchangeable) for safety.

### 2. Transforms: Smart Data Extractors

Transforms are like mini-functions that pull values from Context.
They’re “lazy”—they don’t run until needed, and they adapt to the
current Context.

Examples:

- `get("name")`: Get “name” from current node → `"Alice"` when node is
  `{"name": "Alice"}`
- `get_from_parent("id")`: Get “id” from parent context → `"u1"` when
  processing a post under user u1
- `index()`: Current list position → `0` for first item, `1` for second,
  etc.
- `concat(literal("user_"), get("id"))`: Combine strings → `"user_u1"`

Full list in the Cheatsheet below.

### 3. TraversalSpec: How to Walk the JSON

This says: “Start here, then go deeper if needed, and do this for each
item.”

- `path`: Starting path (list of strings, e.g., \[“users”\]).
- `mode`: Iteration mode for the outer container: “auto” (default),
  “items”, or “single”.
- `inner_path`: Optional deeper path (e.g., \[“posts”\] for nesting).
- `inner_mode`: Iteration mode for the inner container: “auto”
  (default), “items”, or “single”.
- `emits`: What tables to create from each item.

You can have multiple Traversals in one MappingSpec—they run
independently.

Here’s a visual representation of how traversals work:

    JSON structure:
    root
    └── users []                    ← path=["users"]
        ├── [0] {"id": "u1", ...}
        │   └── posts []            ← inner_path=["posts"]
        │       ├── [0] {"id": "p1", "title": "Hello"}
        │       └── [1] {"id": "p2", "title": "World"}
        └── [1] {"id": "u2", ...}

### 4. TableEmit and Fields: Building Your Tables

- `table`: Name of the table.
- `fields`: List of Field(name, transform) – columns and how to compute
  them.
- `join_keys`: List of transforms for unique row IDs (like primary
  keys). Same keys across traversals merge rows.

### 5. Executor: Running It All

`run_mapping(json_data, spec)` executes everything and returns a dict of
tables.

## Detailed Examples

### Example 1: Composite Keys for Merging Data

Merge user info from two parts of JSON:

``` python
spec = MappingSpec(traversals=[
    TraversalSpec(  # Basic user data
        path=["users"],
        mode="auto",
        emits=[TableEmit(
            table="users",
            join_keys=[get("id")],
            fields=[Field("id", get("id")), Field("name", get("name"))]
        )]
    ),
    TraversalSpec(  # Add email from another section
        path=["profiles"],
        mode="auto",
        emits=[TableEmit(
            table="users",  # Same table!
            join_keys=[get("user_id")],  # Matches previous keys
            fields=[Field("email", get("email"))]
        )]
    )
])
```

Rows with matching keys merge: e.g., add “email” to existing user row.

### Example 2: Deep Nesting (Arbitrary Depth)

No limit to depth—use longer `inner_path`. The `depth` parameter
controls how many levels up to look:

- `get_from_parent("id")` or `depth=1` → immediate parent
- `get_from_parent("id", depth=2)` → grandparent
- `get_from_parent("id", depth=3)` → great-grandparent

``` python
spec = MappingSpec(traversals=[
    TraversalSpec(
        path=["servers"],
        mode="auto",
        inner_path=["channels", "messages", "reactions"],  # 3 levels deep!
        inner_mode="auto",
        emits=[TableEmit(
            table="reactions",
            join_keys=[get_from_parent("id", depth=3), get_from_parent("id", depth=2), get_from_parent("id"), get("id")],
            fields=[
                Field("server_id", get_from_parent("id", depth=3)),
                Field("channel_id", get_from_parent("id", depth=2)),
                Field("message_id", get_from_parent("id")),
                Field("reaction", get("emoji"))
            ]
        )]
    )
])
```

## Transform Cheatsheet

- **`get(path)`**: From current node (dot notation or list, e.g.,
  “user.name” or \[“user”, 0\]).
- **`get_from_parent(path, depth=1)`**: From ancestor.
- **`get_from_root(path)`**: From top-level JSON.
- **`key()`**: Current dict key.
- **`index()`**: Current list index.
- **`literal(value)`**: Constant value.
- **`concat(*parts)`**: Join strings.
- **`format_id(*parts, sep="_")`**: Join non-empty parts with separator.
- **`coalesce(*transforms)`**: First non-None value.
- **`len_of(inner)`**: Length of a list/dict/string.

Pro Tip: Transforms are lazy—they run in the “context” of where they’re
used, making them super flexible.

Transforms compose naturally:

``` python
user_key = concat(literal("user_"), get("id"))           # "user_123"
full_name = concat(get("first"), literal(" "), get("last"))  # "Alice Smith"
```

## Common Mistakes

- **Empty results?**
  - Check your `path` matches the JSON structure exactly
  - Verify the data type at that path matches expectations
- **Missing parent data?**
  - Check the `depth` parameter in `get_from_parent()`
  - Ensure the parent context exists in your traversal
- **Duplicate or missing rows?**
  - Verify `join_keys` are unique for each row
  - Check that join_keys don’t contain `None` values (these rows are
    skipped)

## Advanced Topics

- **Lazy Evaluation**: Transforms don’t compute until executed, adapting
  to the current spot in JSON.
- **Custom Transforms**: Define your own functions that take Context and
  return values.
- **Row Merging Rules**: Last write wins for duplicate fields; missing
  keys skip rows.
- **Field selectors**: Type-safe field references. See [Field
  selectors](docs/field-selectors.qmd).
- **Instance emission**: Build Pydantic/TypedDict/ORM instances directly
  instead of dicts. See [Instance emission](docs/instance-emission.qmd).
- **Merge policies**: Sum/append/min/max instead of overwrite when
  multiple traversals update the same field. See [Merge
  policies](docs/merge-policies.qmd).
- **Error reporting**: Per-key diagnostics in results. See [Error
  reporting](docs/error-reporting.qmd).
- **Relationships without extra round trips**: Bind in-memory, flush
  once. See [Relationships](docs/relationships.qmd) and [SQLAlchemy
  adapter](docs/sqlalchemy-adapter.qmd).
- **Performance**: Efficient for large JSON; traversals are independent.

## Roadmap Ideas

- Database integrations (e.g., SQLAlchemy).
- More examples and benchmarks.
- Visual mapping tools.

## Glossary

- **Context**: Your current position while traversing the JSON tree
- **Transform**: A function that extracts values from a Context
- **Traversal**: Instructions for walking through part of the JSON
- **Emit**: Creating a table row from the current context
- **Join keys**: Values that uniquely identify a row (like primary keys)
- **Depth**: How many parent levels to traverse upward

## License

MIT

Need help? Open an issue on GitHub!
