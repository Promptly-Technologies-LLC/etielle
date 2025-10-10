## Etielle

Declarative, type-safe Python DSL for mapping complex nested JSON to relational database schemas.

- **Repository**: [Promptly-Technologies-LLC/etielle](https://github.com/Promptly-Technologies-LLC/etielle)
- **PyPI**: [`etielle`](https://pypi.org/project/etielle/)
- **Python**: ≥ 3.13

### Why Etielle?

- **Declarative relational mapping DSL**: Define tables, fields, and join keys explicitly—no stringly-typed query languages.
- **Context-aware transforms**: Access root, parent, current key/index, and path to compute values robustly.
- **Flexible traversal**: Navigate outer/inner paths and choose whether to iterate dict items or list values.
- **Row merging by composite keys**: Multiple traversals can contribute to the same logical row via `join_keys`.
- **Multi-table output**: One run can emit rows for multiple tables with correct foreign keys.

Unlike automatic “flatteners” (e.g., AWS Glue-like relationalize), Etielle is relational-first and schema-driven. Compared with general restructuring tools like `glom`, Etielle is purpose-built for producing clean, relational outputs.

### Installation

Prefer installing with `uv` for speed and reproducibility. `pip` works as well.

#### Install with uv (recommended)

Project usage (adds to your project’s dependencies):

```bash
uv add etielle
```

Ad-hoc/one-off usage:

```bash
uv pip install etielle
```

#### Install with pip

```bash
pip install etielle
```

### Quick start

The core concepts you’ll use:

- **TraversalSpec**: how to reach and iterate parts of your JSON (outer and optional inner paths).
- **TableEmit**: how to produce rows for a table from the current traversal context, including composite `join_keys`.
- **Field**: a named value computed via a transform.
- **Transforms**: small functions that read from the current `Context` (e.g., `get()`, `get_from_parent()`).

#### Minimal example

```python
from etielle.core import (
    MappingSpec,
    TraversalSpec,
    TableEmit,
    Field
)
from etielle.transforms import get, get_from_parent
from etielle.executor import run_mapping

data = {
    "users": [
        {
            "id": "u1",
            "name": "Ada",
            "posts": [
                {"id": "p1", "title": "Hello"},
                {"id": "p2", "title": "World"},
            ],
        },
        {"id": "u2", "name": "Linus", "posts": []},
    ]
}

# 1) Emit the users table
users_traversal = TraversalSpec(
    path=["users"],
    iterate_items=False,  # iterate list values
    emits=[
        TableEmit(
            table="users",
            join_keys=[get("id")],  # single join key; becomes id if not set explicitly
            fields=[
                Field("id", get("id")),
                Field("name", get("name")),
            ],
        )
    ],
)

# 2) Emit the posts table (inner traversal under each user)
posts_traversal = TraversalSpec(
    path=["users"],
    iterate_items=False,
    inner_path=["posts"],
    inner_iterate_items=False,  # iterate list values
    emits=[
        TableEmit(
            table="posts",
            join_keys=[get("id")],
            fields=[
                Field("id", get("id")),
                Field("user_id", get_from_parent("id")),  # FK to users.id
                Field("title", get("title")),
            ],
        )
    ],
)

spec = MappingSpec(traversals=[users_traversal, posts_traversal])

result = run_mapping(data, spec)
print(result)
# {
#   'users': [
#     {'id': 'u1', 'name': 'Ada'},
#     {'id': 'u2', 'name': 'Linus'},
#   ],
#   'posts': [
#     {'id': 'p1', 'user_id': 'u1', 'title': 'Hello'},
#     {'id': 'p2', 'user_id': 'u1', 'title': 'World'},
#   ]
# }
```

### Transform cheatsheet

- **get(path)**: read a value relative to the current node; `path` accepts dot notation or a list (ints allowed for list indices).
- **get_from_parent(path, depth=1)**: read a value from the parent node (or ancestor via `depth`).
- **get_from_root(path)**: read a value from the root payload.
- **key() / index()**: return the current dict key or list index if iterating.
- **literal(value)**, **concat(...)**, **format_id(..., sep="_")**, **coalesce(...)**, **len_of(inner)**: helpers for building values.

Notes:
- Rows are merged per-table by the composite `join_keys`. If a table has a single join key and you don’t set an `id` field, `run_mapping()` will populate `id` from that join key.
- If any join key part is missing/empty, the row is skipped.

### How this differs from other tools

- **Not just flattening**: You control the schema, relationships, and keys—useful for analytics-ready models.
- **Relational-first**: Purpose-built for JSON → relational mappings; compare to `glom` (general restructuring) or automatic flatteners (less control).
- **Composable and testable**: Transforms are regular Python callables and type-friendly.

### Roadmap ideas

- Optional adapters for inserting into ORMs/DB layers (e.g., SQLAlchemy, SQLModel).
- Benchmarks for large datasets; performance guidance.
- Expanded cookbook of mapping recipes.

### License

MIT

