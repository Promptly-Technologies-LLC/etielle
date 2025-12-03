"""Fluent E→T→L API for etielle.

This module provides a builder-pattern API that mirrors the conceptual
Extract → Transform → Load flow.

Example:
    result = (
        etl(data)
        .goto("users").each()
        .map_to(table=User, fields=[
            Field("name", get("name")),
            TempField("id", get("id"))
        ])
        .run()
    )
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from etielle.core import Context
from etielle.telemetry import (
    FlushCompleted,
    FlushFailed,
    FlushStarted,
    MapCompleted,
    MapStarted,
    TelemetryCallback,
    _emit,
)

if TYPE_CHECKING:
    from etielle.core import Transform, TraversalSpec
    from etielle.instances import InstanceEmit, MergePolicy

ErrorMode = Literal["collect", "fail_fast"]


@dataclass(frozen=True)
class Field:
    """A field that will be persisted to the output table.

    Args:
        name: The column/attribute name in the output.
        transform: How to compute the value from the current context.
        merge: Optional policy for merging values when rows are combined.
    """

    name: str
    transform: Transform[Any]
    merge: MergePolicy | None = None


@dataclass(frozen=True)
class TempField:
    """A field used only for joining/linking, not persisted.

    TempFields are used to:
    - Compute join keys for merging rows
    - Store parent IDs for relationship linking

    They do not appear in the final output objects.

    Args:
        name: The field name (used in join_on and link_to).
        transform: How to compute the value from the current context.
    """

    name: str
    transform: Transform[Any]


FieldUnion = Field | TempField
"""Type alias for fields that can appear in map_to()."""


def transform(func: Callable[..., Any]) -> Callable[..., Transform[Any]]:
    """Decorator to create custom transforms with curried arguments.

    The decorated function must have `ctx: Context` as its first parameter.
    Any additional parameters become factory arguments.

    Example:
        @transform
        def split_id(ctx: Context, field: str, index: int) -> str:
            return ctx.node[field].split("_")[index]

        # Usage in field definition
        TempField("user_id", split_id("composite_id", 0))

    Args:
        func: A function with signature (ctx: Context, *args) -> T

    Returns:
        A factory function that returns Transform[T]
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    # Validate first param is ctx
    if not params or params[0].name != "ctx":
        raise ValueError(f"@transform function must have 'ctx' as first parameter, got: {func}")

    # Get extra params (everything after ctx)
    params[1:]

    @functools.wraps(func)
    def factory(*args: Any, **kwargs: Any) -> Transform[Any]:
        # Bind the extra arguments
        def transform_fn(ctx: Context) -> Any:
            return func(ctx, *args, **kwargs)
        return transform_fn

    return factory


@transform
def node(ctx: Context) -> Any:
    """Return the current node value.

    Useful when iterating and the node itself is the value you want.

    Example:
        # Data: {"ids": [1, 2, 3]}
        .goto("ids").each()
        .map_to(table=Item, fields=[
            Field("id", node())
        ])
    """
    return ctx.node


@transform
def parent_index(ctx: Context, depth: int = 1) -> int | None:
    """Return the list index of an ancestor context.

    Args:
        depth: How many levels up to look (1 = parent, 2 = grandparent).

    Returns:
        The index if the ancestor was iterating a list, None otherwise.

    Example:
        # Data: {"rows": [[1, 2], [3, 4]]}
        .goto("rows").each().each()
        .map_to(table=Cell, fields=[
            Field("row_num", parent_index()),  # 0 or 1
            Field("value", node())
        ])
    """
    current = ctx
    for _ in range(depth):
        if current.parent is None:
            return None
        current = current.parent
    return current.index


@dataclass
class TableStats:
    """Statistics for a single table after pipeline execution.

    Attributes:
        mapped: Number of instances created during the mapping phase.
        errors: Number of validation/transform errors during mapping.
        inserted: Number of rows successfully written to DB (0 if no session).
        failed: Number of rows that failed during flush.
    """

    mapped: int
    errors: int
    inserted: int
    failed: int


class _TablesProxy:
    """Proxy for accessing tables by string name or model class."""

    def __init__(
        self,
        tables: dict[str, dict[tuple[Any, ...], Any]],
        class_map: dict[str, type] | None = None
    ) -> None:
        self._tables = tables
        self._class_map = class_map or {}
        self._reverse_map = {v: k for k, v in self._class_map.items()}

    def __getitem__(self, key: str | type) -> dict[tuple[Any, ...], Any]:
        if isinstance(key, str):
            return self._tables[key]
        # It's a class - look up by tablename or class name
        table_name = self._reverse_map.get(key)
        if table_name is None:
            # Try __tablename__ attribute
            table_name = getattr(key, "__tablename__", key.__name__.lower())
        return self._tables[table_name]

    def __contains__(self, key: str | type) -> bool:
        try:
            self[key]
            return True
        except KeyError:
            return False

    def items(self):
        return self._tables.items()

    def keys(self):
        return self._tables.keys()

    def values(self):
        return self._tables.values()


@dataclass
class PipelineResult:
    """Result from running a pipeline.

    Attributes:
        tables: Access tables by string name or model class.
        errors: Validation/transform errors keyed by table then row key.
        stats: Per-table statistics (mapped, errors, inserted, failed).
    """

    _tables: dict[str, dict[tuple[Any, ...], Any]]
    _errors: dict[str, dict[tuple[Any, ...], list[str]]]
    _table_class_map: dict[str, type] | None = None
    _raw_results: dict[str, Any] | None = None
    _stats: dict[str, TableStats] | None = None

    def __init__(
        self,
        tables: dict[str, dict[tuple[Any, ...], Any]],
        errors: dict[str, dict[tuple[Any, ...], list[str]]],
        _table_class_map: dict[str, type] | None = None,
        _raw_results: dict[str, Any] | None = None,
        _stats: dict[str, TableStats] | None = None,
    ) -> None:
        self._tables = tables
        self._errors = errors
        self._table_class_map = _table_class_map
        self._raw_results = _raw_results
        self._stats = _stats

    @property
    def tables(self) -> _TablesProxy:
        """Access extracted tables by name or model class."""
        return _TablesProxy(self._tables, self._table_class_map)

    @property
    def stats(self) -> dict[str, TableStats]:
        """Per-table statistics.

        Returns a dict mapping table names to TableStats objects with:
        - mapped: instances created during mapping
        - errors: validation/transform errors
        - inserted: rows successfully flushed to DB
        - failed: rows that failed during flush
        """
        if self._stats is not None:
            return self._stats
        # Fallback: compute from existing data (no flush tracking)
        return {
            name: TableStats(
                mapped=len(rows),
                errors=len(self._errors.get(name, {})),
                inserted=len(rows),  # Assume all succeeded if no explicit stats
                failed=0,
            )
            for name, rows in self._tables.items()
        }

    @property
    def errors(self) -> dict[str, dict[tuple[Any, ...], list[str]]]:
        """Validation errors keyed by table name, then row key."""
        return self._errors


def _detect_builder(table_class: type | None) -> Any:
    """Detect appropriate builder for a model class.

    Returns:
        Builder instance or None for plain dicts.
    """
    if table_class is None:
        return None  # Use TableEmit for dicts

    # Check for SQLAlchemy/SQLModel ORM FIRST
    # SQLModel inherits from Pydantic BaseModel, but we want to use ConstructorBuilder
    # to allow SQLModel's special handling of auto-generated primary keys
    if hasattr(table_class, "__tablename__") and hasattr(table_class, "__mapper__"):
        from etielle.instances import ConstructorBuilder
        return ConstructorBuilder(table_class)

    # Check for Pydantic (after SQLModel check)
    try:
        from pydantic import BaseModel
        if issubclass(table_class, BaseModel):
            from etielle.instances import PydanticBuilder
            return PydanticBuilder(table_class)
    except ImportError:
        pass

    # Check for TypedDict
    import typing
    if hasattr(typing, "is_typeddict") and typing.is_typeddict(table_class):
        from etielle.instances import TypedDictBuilder
        return TypedDictBuilder(table_class)

    # Default: ConstructorBuilder for dataclasses and other classes
    from etielle.instances import ConstructorBuilder
    return ConstructorBuilder(table_class)


class PipelineBuilder:
    """Fluent builder for E→T→L pipelines.

    Use etl() to create instances of this class.

    Example:
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table=User, fields=[...])
            .run()
        )
    """

    def __init__(
        self,
        roots: tuple[Any, ...],
        error_mode: ErrorMode = "collect"
    ) -> None:
        self._roots = roots
        self._error_mode = error_mode
        # Navigation state
        self._current_root_index: int = 0
        self._current_path: list[str] = []
        self._iteration_depth: int = 0
        self._iteration_points: list[list[str]] = []
        # Accumulated specs
        self._emissions: list[dict[str, Any]] = []
        self._relationships: list[dict[str, Any]] = []
        # Session for loading
        self._session: Any | None = None
        # Supabase-specific options
        self._upsert: bool = False
        self._upsert_on: dict[str, str | list[str]] | None = None
        self._batch_size: int = 1000

    def goto_root(self, index: int = 0) -> PipelineBuilder:
        """Navigate to a specific JSON root.

        Resets the current path and iteration state.

        Args:
            index: Which root to navigate to (0-indexed, defaults to 0).

        Returns:
            Self for method chaining.

        Raises:
            IndexError: If index is out of range.

        Example:
            etl(users_json, posts_json)
            .goto_root(0).goto("users").each()  # Process users
            .goto_root(1).goto("posts").each()  # Process posts
        """
        if index < 0 or index >= len(self._roots):
            raise IndexError(f"Root index {index} out of range (have {len(self._roots)} roots)")
        self._current_root_index = index
        self._current_path = []
        self._iteration_depth = 0
        self._iteration_points = []
        return self

    def goto(self, path: str | list[str]) -> PipelineBuilder:
        """Navigate to a relative path from the current position.

        Args:
            path: Path segments as string (dot-separated) or list.

        Returns:
            Self for method chaining.

        Example:
            .goto("users")           # Single segment
            .goto("data.users")      # Dot notation
            .goto(["data", "users"]) # List of segments
        """
        if isinstance(path, str):
            segments = path.split(".") if "." in path else [path]
        else:
            segments = list(path)
        self._current_path.extend(segments)
        return self

    def each(self) -> PipelineBuilder:
        """Iterate over items at the current path.

        For lists: iterates by index.
        For dicts: iterates key-value pairs.

        Can be chained for nested iteration.

        Returns:
            Self for method chaining.

        Example:
            .goto("users").each()           # Iterate list
            .goto("userPosts").each().each() # Dict of lists
        """
        self._iteration_depth += 1
        # Record where this iteration occurs
        self._iteration_points.append(list(self._current_path))
        return self

    def map_to(
        self,
        table: str | type,
        fields: Sequence[FieldUnion],
        join_on: Sequence[str] | None = None,
        errors: ErrorMode | None = None
    ) -> PipelineBuilder:
        """Emit rows to a table from the current traversal position.

        Args:
            table: Table name (string) or model class.
            fields: List of Field and TempField definitions.
            join_on: Field names to use as composite key for merging.
                    Required for subsequent map_to calls to the same table.
            errors: Override the pipeline's error mode for this table.

        Returns:
            Self for method chaining.

        Example:
            .map_to(table=User, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
        """
        # Resolve table name and class
        if isinstance(table, str):
            table_name = table
            table_class = None
        else:
            table_class = table
            table_name = getattr(table, "__tablename__", table.__name__.lower())

        emission = {
            "table": table_name,
            "table_class": table_class,
            "fields": list(fields),
            "join_on": list(join_on) if join_on else None,
            "errors": errors,
            "path": list(self._current_path),
            "iteration_depth": self._iteration_depth,
            "iteration_points": [list(p) for p in self._iteration_points],
            "root_index": self._current_root_index,
        }
        self._emissions.append(emission)
        return self

    def link_to(
        self,
        parent: type | str,
        by: dict[str, str],
        fk: dict[str, str] | None = None,
    ) -> PipelineBuilder:
        """Define a relationship from the current table to a parent table.

        The `by` dict maps child field names to parent field names.
        Both Field and TempField names can be used.

        Args:
            parent: The parent model class or table name string.
            by: Mapping of {child_field: parent_field}.
            fk: Mapping of {child_column: parent_column} for FK population
                (Supabase only). After inserting parents, child's FK column
                is populated with parent's generated ID.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If called without a preceding map_to().

        Example:
            .map_to(table=Post, fields=[
                TempField("user_id", get("author_id"))
            ])
            .link_to(User, by={"user_id": "id"})

            # Or with table names:
            .map_to(table="posts", fields=[...])
            .link_to("users", by={"user_id": "id"})

            # With DB-generated parent IDs (Supabase):
            .map_to(table="posts", fields=[
                TempField("_parent_key", get_from_parent("name"))
            ])
            .link_to("users", by={"_parent_key": "_key"}, fk={"user_id": "id"})
        """
        if not self._emissions:
            raise ValueError("link_to() must follow a map_to() call")

        last_emission = self._emissions[-1]

        # Handle both class and string for parent
        if isinstance(parent, str):
            parent_table = parent
            parent_class = None
        else:
            parent_table = getattr(parent, "__tablename__", parent.__name__.lower())
            parent_class = parent

        relationship = {
            "child_table": last_emission["table"],
            "parent_class": parent_class,
            "parent_table": parent_table,
            "by": dict(by),
            "fk": dict(fk) if fk else None,
            "emission_index": len(self._emissions) - 1,
        }
        self._relationships.append(relationship)
        return self

    def load(
        self,
        session: Any,
        *,
        upsert: bool = False,
        upsert_on: dict[str, str | list[str]] | None = None,
        batch_size: int = 1000,
    ) -> PipelineBuilder:
        """Configure database session or client for persistence.

        When load() is called before run(), the pipeline will:
        1. Build all instances in memory
        2. Bind relationships
        3. Add instances to the session (SQLAlchemy) or insert to database (Supabase)
        4. Flush (but not commit for SQLAlchemy)

        The caller controls the transaction (commit/rollback) for SQLAlchemy.

        Args:
            session: SQLAlchemy/SQLModel session or Supabase client.
            upsert: If True, use upsert instead of insert (Supabase only).
                Uses the table's primary key for conflict detection by default.
            upsert_on: Override conflict columns per table (Supabase only).
                Only used when upsert=True. Maps table names to conflict columns:
                - Single column: {"users": "email"}
                - Composite key: {"posts": ["user_id", "slug"]}
            batch_size: Maximum rows per insert batch (Supabase only).

        Returns:
            Self for method chaining.

        Example (SQLAlchemy):
            result = (
                etl(data)
                .goto("users").each()
                .map_to(table=User, fields=[...])
                .load(session)
                .run()
            )
            session.commit()  # Caller controls transaction

        Example (Supabase with default conflict):
            result = (
                etl(data)
                .goto("users").each()
                .map_to(table="users", fields=[...])
                .load(supabase_client, upsert=True)
                .run()
            )

        Example (Supabase with custom conflict columns):
            result = (
                etl(data)
                .goto("users").each()
                .map_to(table="users", fields=[...])
                .load(supabase_client, upsert=True, upsert_on={
                    "users": "email",
                    "posts": ["user_id", "slug"],
                })
                .run()
            )
        """
        self._session = session
        self._upsert = upsert
        self._upsert_on = upsert_on
        self._batch_size = batch_size
        return self

    def _is_supabase_client(self, obj: Any) -> bool:
        """Check if the object is a Supabase client."""
        module = type(obj).__module__
        return module.startswith("supabase") or module.startswith("postgrest")

    def _flush_to_supabase(
        self,
        tables: dict[str, dict[tuple[Any, ...], Any]],
        flush_order: list[str],
        child_lookup_values: dict[str, dict[tuple[Any, ...], dict[str, Any]]],
        stats: dict[str, TableStats],
        on_event: TelemetryCallback | None,
    ) -> None:
        """Flush tables to Supabase in dependency order with two-phase insert.

        For tables with children that have `fk` relationships, this method:
        1. Inserts parent rows and captures returned rows (with generated IDs)
        2. Updates original rows with generated IDs
        3. Populates child FK columns before inserting children

        Args:
            tables: Dict mapping table names to {key: row_dict}.
            flush_order: Tables in topological order (parents first).
            child_lookup_values: Pre-computed {child_table: {child_key: {field: value}}}.
            stats: Stats dict to update with inserted/failed counts.
            on_event: Optional telemetry callback.
        """
        from etielle.adapters.supabase_adapter import insert_batches

        # Build mapping of parent tables to their fk relationships
        # {parent_table: [(relationship, child_table), ...]}
        fk_children: dict[str, list[tuple[dict[str, Any], str]]] = {}
        for rel in self._relationships:
            if rel.get("fk"):
                parent_table = rel["parent_table"]
                child_table = rel["child_table"]
                fk_children.setdefault(parent_table, []).append((rel, child_table))

        for table_name in flush_order:
            if table_name not in tables:
                continue

            # Get rows preserving key order for matching with returned rows
            table_data = tables[table_name]
            keys = list(table_data.keys())
            rows = [table_data[k] for k in keys]

            if not rows:
                continue

            # Determine on_conflict for this table
            on_conflict: str | None = None
            if self._upsert and self._upsert_on:
                conflict_spec = self._upsert_on.get(table_name)
                if conflict_spec is not None:
                    # Convert list to comma-separated string
                    if isinstance(conflict_spec, list):
                        on_conflict = ",".join(conflict_spec)
                    else:
                        on_conflict = conflict_spec

            # Emit FlushStarted event
            _emit(FlushStarted(table=table_name, count=len(rows)), on_event)

            # Track inserted count for this table
            table_inserted = 0
            is_upsert = self._upsert

            def on_batch(batch_num: int, batch_total: int, inserted: int) -> None:
                nonlocal table_inserted
                table_inserted += inserted
                _emit(
                    FlushCompleted(
                        table=table_name,
                        inserted=inserted,
                        failed=0,
                        batch_num=batch_num,
                        batch_total=batch_total,
                        upsert=is_upsert,
                    ),
                    on_event,
                )

            # Insert rows and capture returned data
            try:
                returned = insert_batches(
                    self._session,
                    table_name,
                    rows,
                    upsert=self._upsert,
                    on_conflict=on_conflict,
                    batch_size=self._batch_size,
                    on_batch=on_batch,
                )
                # Update stats
                if table_name in stats:
                    stats[table_name] = TableStats(
                        mapped=stats[table_name].mapped,
                        errors=stats[table_name].errors,
                        inserted=table_inserted,
                        failed=len(rows) - table_inserted,
                    )
            except Exception as e:
                _emit(
                    FlushFailed(
                        table=table_name,
                        error=str(e),
                        affected_count=len(rows),
                    ),
                    on_event,
                )
                # Update stats to show all rows failed
                if table_name in stats:
                    stats[table_name] = TableStats(
                        mapped=stats[table_name].mapped,
                        errors=stats[table_name].errors,
                        inserted=0,
                        failed=len(rows),
                    )
                raise

            # Two-phase: if this table has children with fk, update originals with generated IDs
            if table_name in fk_children:
                if len(returned) != len(rows):
                    raise ValueError(
                        f"Row count mismatch for table '{table_name}': "
                        f"sent {len(rows)}, received {len(returned)}"
                    )

                # Copy generated columns from returned rows to originals
                # Overwrite all columns to capture DB-generated values (like UUIDs)
                for original, returned_row in zip(rows, returned):
                    for col, value in returned_row.items():
                        original[col] = value

                # For each child table with fk relationship, populate FK columns
                for rel, child_table in fk_children[table_name]:
                    if child_table not in tables:
                        continue

                    by_mapping = rel["by"]  # {child_field: parent_field}
                    fk_mapping = rel["fk"]  # {child_col: parent_col}

                    child_data = tables[child_table]
                    child_lookup = child_lookup_values.get(child_table, {})
                    parent_lookup = child_lookup_values.get(table_name, {})

                    # Build parent index using computed TempField values
                    # For each by mapping (typically one), build index and populate
                    for child_field, parent_field in by_mapping.items():
                        # Build parent index: {parent_key_value: parent_row}
                        # Use parent_lookup to get TempField values that aren't in rows
                        parent_index: dict[Any, dict[str, Any]] = {}
                        for parent_key, parent_row in table_data.items():
                            # Get the parent's TempField value from parent_lookup
                            parent_values = parent_lookup.get(parent_key, {})
                            parent_key_value = parent_values.get(parent_field)
                            if parent_key_value is not None:
                                parent_index[parent_key_value] = parent_row

                        # Populate FK columns in child rows
                        for child_key, child_row in child_data.items():
                            child_values = child_lookup.get(child_key, {})
                            lookup_value = child_values.get(child_field)

                            if lookup_value is None:
                                continue

                            parent_row = parent_index.get(lookup_value)
                            if parent_row is None:
                                continue

                            # Set the FK column(s) on the child row
                            for child_col, parent_col in fk_mapping.items():
                                generated_value = parent_row.get(parent_col)
                                if generated_value is not None:
                                    child_row[child_col] = generated_value

    def _build_traversal_specs(self) -> list[TraversalSpec]:
        """Convert accumulated emissions to TraversalSpec objects."""
        from etielle.core import TraversalSpec, TableEmit, Field as CoreField

        specs = []

        for emission in self._emissions:
            # Determine path and iteration mode
            path: list[str] = emission["path"]
            iteration_points: list[list[str]] = emission["iteration_points"]

            # Build fields and join_keys from Field/TempField
            fields: list[CoreField] = []
            temp_fields_list: list[CoreField] = []  # Track actual TempField CoreFields
            join_keys: list[Any] = []
            merge_policies: dict[str, Any] = {}
            temp_field_names: set[str] = set()  # Track TempField names (for InstanceEmit)
            field_map = {f.name: f.transform for f in emission["fields"]}

            # If join_on specified, use those field names to build join_keys
            if emission["join_on"]:
                for key_name in emission["join_on"]:
                    if key_name in field_map:
                        join_keys.append(field_map[key_name])
                # Add ALL fields to output (both Field and TempField)
                for f in emission["fields"]:
                    core_field = CoreField(f.name, f.transform)
                    fields.append(core_field)
                    if isinstance(f, TempField):
                        temp_field_names.add(f.name)
                        temp_fields_list.append(core_field)
                    elif f.merge is not None:
                        merge_policies[f.name] = f.merge
            else:
                # No explicit join_on - NO default join key
                # Each iteration creates a distinct instance (auto-generated key)
                # ALL fields go to output (both Field and TempField)
                for f in emission["fields"]:
                    core_field = CoreField(f.name, f.transform)
                    fields.append(core_field)
                    if isinstance(f, TempField):
                        temp_field_names.add(f.name)
                        temp_fields_list.append(core_field)
                    elif f.merge is not None:
                        merge_policies[f.name] = f.merge

            outer_path: list[str] = path
            outer_mode: Literal["auto", "items", "single"] = "auto"
            inner_path: list[str] | None = None
            inner_mode: Literal["auto", "items", "single"] = "auto"

            # Handle outer/inner path split based on iteration points
            if len(iteration_points) == 0:
                # No iteration
                outer_path = path
                outer_mode = "single"
                inner_path = None
                inner_mode = "auto"
            elif len(iteration_points) == 1:
                # Single iteration
                outer_path = iteration_points[0]
                outer_mode = "auto"
                # Inner path is what comes after the iteration point
                inner_start = len(iteration_points[0])
                inner_path = path[inner_start:] if inner_start < len(path) else None
                inner_mode = "auto"
            else:
                # Nested iteration - outer at first point, inner continues
                outer_path = iteration_points[0]
                outer_mode = "auto"
                # Inner path starts after first iteration point
                inner_start = len(iteration_points[0])
                remaining_path = path[inner_start:]
                inner_path = remaining_path if remaining_path else None
                inner_mode = "auto"

            # Choose emit type based on whether we have a model class or merge policies
            table_class = emission["table_class"]
            builder = _detect_builder(table_class)

            # Determine error mode (emission can override pipeline level)
            error_mode = emission.get("errors") or self._error_mode
            # Map to strict_mode parameter
            strict_mode = "fail_fast" if error_mode == "fail_fast" else "collect_all"

            table_emit: InstanceEmit[Any] | TableEmit

            if builder or merge_policies:
                # Use InstanceEmit with appropriate builder
                from etielle.instances import InstanceEmit, FieldSpec

                # Convert CoreField to FieldSpec
                field_specs: list[FieldSpec] = [
                    FieldSpec(selector=f.name, transform=f.transform) for f in fields
                ]

                # If we have merge policies but no model class, use TypedDictBuilder for plain dicts
                if not builder and merge_policies:
                    from etielle.instances import TypedDictBuilder
                    builder = TypedDictBuilder(lambda d: d)

                # No default join key - if join_on is not specified, instances are
                # stored in a list without keying (no merging, no deduplication).
                # Each iteration creates a distinct instance.
                default_join_key = ()

                table_emit = InstanceEmit(
                    table=emission["table"],
                    join_keys=tuple(join_keys) if join_keys else default_join_key,
                    fields=tuple(field_specs),
                    builder=builder,
                    policies=merge_policies,
                    strict_mode=strict_mode,
                    temp_fields=frozenset(temp_field_names)
                )
            else:
                # Use simpler TableEmit when no model class or merge policies needed
                # No default join key - if join_on is not specified, instances are
                # stored in a list without keying (no merging, no deduplication).
                default_join_key = ()

                # For TableEmit, exclude TempFields from output (they're not persisted)
                # Use object identity to handle case where Field and TempField share the same name
                temp_field_set = set(temp_fields_list)
                non_temp_fields = [f for f in fields if f not in temp_field_set]

                table_emit = TableEmit(
                    table=emission["table"],
                    join_keys=tuple(join_keys) if join_keys else default_join_key,
                    fields=tuple(non_temp_fields)
                )

            spec = TraversalSpec(
                path=tuple(outer_path),
                mode=outer_mode,
                inner_path=tuple(inner_path) if inner_path else None,
                inner_mode=inner_mode,
                emits=(table_emit,)
            )
            specs.append(spec)

        return specs

    def _build_dependency_graph(self) -> dict[str, set[str]]:
        """Build dependency graph from link_to relationships.

        Returns:
            Dict mapping child_table -> set of parent_tables.
        """
        graph: dict[str, set[str]] = {}

        for rel in self._relationships:
            child = rel["child_table"]
            parent = rel["parent_table"]
            graph.setdefault(child, set()).add(parent)

        return graph

    def _get_linkable_fields(self) -> dict[str, set[str]]:
        """Extract fields that are used for relationship linking.

        Returns:
            Dict mapping parent table name to set of field names used in link_to.
        """
        linkable: dict[str, set[str]] = {}
        for rel in self._relationships:
            parent_table = rel["parent_table"]
            # The 'by' dict maps child_field -> parent_field
            # We need to index parent by the parent_field values
            for parent_field in rel["by"].values():
                linkable.setdefault(parent_table, set()).add(parent_field)
        return linkable

    def run(
        self,
        *,
        on_event: TelemetryCallback | None = None,
    ) -> PipelineResult:
        """Execute the pipeline and return results.

        If load() was called, also persists to the database.

        Args:
            on_event: Optional callback for telemetry events. Called with
                MapStarted, MapCompleted, FlushStarted, FlushCompleted, or
                FlushFailed events during pipeline execution.

        Returns:
            PipelineResult with tables, errors, and stats.
        """
        from etielle.core import MappingSpec
        from etielle.executor import run_mapping

        # Extract linkable fields from link_to declarations
        linkable_fields = self._get_linkable_fields()

        # Group emissions by root index
        emissions_by_root: dict[int, list[dict[str, Any]]] = {}
        for emission in self._emissions:
            root_idx = emission["root_index"]
            emissions_by_root.setdefault(root_idx, []).append(emission)

        # Execute for each root and merge results
        all_raw_results: dict[str, Any] = {}

        for root_idx in sorted(emissions_by_root.keys()):
            emissions = emissions_by_root[root_idx]
            root = self._roots[root_idx]

            # Build specs for this root's emissions only
            specs = []
            for emission in emissions:
                # Temporarily set self._emissions to only this emission's list
                # to reuse _build_traversal_specs logic
                original_emissions = self._emissions
                self._emissions = [emission]
                emission_specs = self._build_traversal_specs()
                self._emissions = original_emissions
                specs.extend(emission_specs)

            mapping_spec = MappingSpec(traversals=tuple(specs))
            raw_results = run_mapping(root, mapping_spec, linkable_fields=linkable_fields)

            # Merge into combined results
            for table_name, mapping_result in raw_results.items():
                if table_name not in all_raw_results:
                    all_raw_results[table_name] = mapping_result
                else:
                    # Merge instances from this root with existing ones
                    existing = all_raw_results[table_name]
                    for key, row in mapping_result.instances.items():
                        if key in existing.instances:
                            # Update existing row with new fields
                            if hasattr(existing.instances[key], 'update'):
                                existing.instances[key].update(row)
                            elif hasattr(existing.instances[key], '__dict__'):
                                # For object instances, merge attributes
                                for k, v in (row.__dict__ if hasattr(row, '__dict__') else row).items():
                                    setattr(existing.instances[key], k, v)
                        else:
                            existing.instances[key] = row
                    # Merge errors
                    for key, msgs in mapping_result.update_errors.items():
                        existing.update_errors.setdefault(key, []).extend(msgs)
                    for key, msgs in mapping_result.finalize_errors.items():
                        existing.finalize_errors.setdefault(key, []).extend(msgs)

        raw_results = all_raw_results

        # Emit mapping events and initialize stats
        stats: dict[str, TableStats] = {}
        for table_name, mapping_result in raw_results.items():
            _emit(MapStarted(table=table_name), on_event)
            error_count = (
                len(mapping_result.update_errors) +
                len(mapping_result.finalize_errors)
            )
            instance_count = len(mapping_result.instances)
            _emit(
                MapCompleted(
                    table=table_name,
                    count=instance_count,
                    error_count=error_count,
                ),
                on_event,
            )
            stats[table_name] = TableStats(
                mapped=instance_count,
                errors=error_count,
                inserted=0,  # Updated during flush
                failed=0,
            )

        # Handle relationship binding and staged flushing
        rel_specs: list[Any] = []
        child_lookup_values: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = {}

        if self._relationships:
            from etielle.relationships import ManyToOneSpec, bind_relationships_via_index, compute_child_lookup_values

            # Build ManyToOneSpec objects from recorded relationships
            for rel in self._relationships:
                # Find the child emission to get the transforms for parent key computation
                child_emission = self._emissions[rel["emission_index"]]

                # Build list of transforms that compute parent key from child context
                child_to_parent_transforms = []
                for child_field, parent_field in rel["by"].items():
                    # Find the transform for this child field
                    for f in child_emission["fields"]:
                        if f.name == child_field:
                            child_to_parent_transforms.append(f.transform)
                            break

                # Infer attr name from parent table name (singular, lowercase)
                # e.g., "users" -> "user", "posts" -> "post"
                parent_table_name = rel["parent_table"]
                attr_name = parent_table_name.rstrip("s") if parent_table_name.endswith("s") else parent_table_name

                spec = ManyToOneSpec(
                    child_table=rel["child_table"],
                    parent_table=rel["parent_table"],
                    attr=attr_name,
                    child_to_parent_key=tuple(child_to_parent_transforms),
                    required=False  # Don't fail on missing parents by default
                )
                rel_specs.append(spec)

            # Compute relationship keys by traversing all roots
            # We need to rebuild the traversal specs for relationship computation
            all_specs = []
            for root_idx in sorted(emissions_by_root.keys()):
                emissions = emissions_by_root[root_idx]
                for emission in emissions:
                    original_emissions = self._emissions
                    self._emissions = [emission]
                    emission_specs = self._build_traversal_specs()
                    self._emissions = original_emissions
                    all_specs.extend(emission_specs)

            # Compute child lookup values (TempField values) by re-traversing
            first_root = self._roots[0] if self._roots else {}
            # This is needed for both session and non-session paths
            child_lookup_values = compute_child_lookup_values(
                first_root, all_specs, self._relationships, self._emissions
            )

            # If no session, bind all relationships now (non-DB use case)
            # Use secondary indices for binding instead of join key computation
            if self._session is None:
                bind_relationships_via_index(
                    raw_results, self._relationships, child_lookup_values, fail_on_missing=False
                )

        # Convert to PipelineResult format
        tables: dict[str, dict[tuple[Any, ...], Any]] = {}
        errors: dict[str, dict[tuple[Any, ...], list[str]]] = {}
        table_class_map: dict[str, type] = {}

        for table_name, mapping_result in raw_results.items():
            tables[table_name] = dict(mapping_result.instances)
            # Collect errors
            all_errors: dict[tuple[Any, ...], list[str]] = {}
            for key, msgs in mapping_result.update_errors.items():
                all_errors.setdefault(key, []).extend(msgs)
            for key, msgs in mapping_result.finalize_errors.items():
                all_errors.setdefault(key, []).extend(msgs)
            if all_errors:
                errors[table_name] = all_errors

        # Build class map from emissions
        for emission in self._emissions:
            if emission["table_class"]:
                table_class_map[emission["table"]] = emission["table_class"]

        # Check if we should fail fast on errors
        if self._error_mode == "fail_fast" and errors:
            # Raise an exception with details about the first error
            for table_name, table_errors in errors.items():
                for key, msgs in table_errors.items():
                    error_msg = f"Validation failed for table '{table_name}' key {key}:\n" + "\n".join(msgs)
                    raise ValueError(error_msg)

        # If session/client provided, flush in dependency order
        if self._session is not None:
            from etielle.utils import topological_sort

            # Build flush order from dependency graph (parents before children)
            dep_graph = self._build_dependency_graph()
            all_tables = set(tables.keys())
            flush_order = topological_sort(dep_graph, all_tables)

            # Check if this is a Supabase client
            if self._is_supabase_client(self._session):
                self._flush_to_supabase(
                    tables, flush_order, child_lookup_values, stats, on_event
                )
            else:
                # SQLAlchemy/SQLModel path
                # Warn if any relationships use fk (Supabase-only feature)
                for rel in self._relationships:
                    if rel.get("fk"):
                        import warnings
                        warnings.warn(
                            f"fk parameter on link_to() is only supported for Supabase. "
                            f"Ignoring fk={rel['fk']} for {rel['child_table']} -> {rel['parent_table']}.",
                            UserWarning,
                            stacklevel=2,
                        )

                # Build pending bindings index: {child_table: [(rel_spec_idx, parent_table)]}
                # This tracks which relationships need to be bound when processing each child
                pending_bindings: dict[str, list[tuple[int, str]]] = {}
                for idx, spec in enumerate(rel_specs):
                    pending_bindings.setdefault(spec.child_table, []).append(
                        (idx, spec.parent_table)
                    )

                # Process tables in dependency order (parents before children):
                # 1. Add this table's instances to session
                # 2. Bind relationships where this table is the CHILD (parents already flushed)
                # 3. Flush this table
                for table_name in flush_order:
                    if table_name not in tables:
                        continue

                    row_count = len(tables[table_name])
                    _emit(FlushStarted(table=table_name, count=row_count), on_event)

                    # Add this table's instances to session
                    for key, instance in tables[table_name].items():
                        if not isinstance(instance, dict):
                            self._session.add(instance)

                    # Bind pending parent relationships for this table using secondary indices
                    # At this point, all parents have been flushed and have IDs
                    for idx, parent_table in pending_bindings.get(table_name, []):
                        if parent_table not in raw_results:
                            continue

                        # Find the relationship spec for this binding
                        rel = self._relationships[idx]
                        parent_result = raw_results[parent_table]
                        child_result = raw_results[table_name]

                        # Infer attr name from parent table name (singular)
                        attr_name = parent_table.rstrip("s") if parent_table.endswith("s") else parent_table

                        # Get pre-computed child lookup values
                        lookup_values = child_lookup_values.get(table_name, {})

                        # Bind each child to its parent using secondary indices
                        for child_key, child_obj in child_result.instances.items():
                            if isinstance(child_obj, dict):
                                continue
                            child_values = lookup_values.get(child_key, {})

                            for child_field, parent_field in rel["by"].items():
                                lookup_value = child_values.get(child_field)
                                if lookup_value is None:
                                    continue

                                parent_index = parent_result.indices.get(parent_field, {})
                                parent_obj = parent_index.get(lookup_value)

                                if parent_obj is not None:
                                    setattr(child_obj, attr_name, parent_obj)

                    # Flush this table - relationships are bound, FKs will be set
                    try:
                        self._session.flush()
                        _emit(
                            FlushCompleted(
                                table=table_name,
                                inserted=row_count,
                                failed=0,
                                batch_num=1,
                                batch_total=1,
                                upsert=False,
                            ),
                            on_event,
                        )
                        # Update stats
                        if table_name in stats:
                            stats[table_name] = TableStats(
                                mapped=stats[table_name].mapped,
                                errors=stats[table_name].errors,
                                inserted=row_count,
                                failed=0,
                            )
                    except Exception as e:
                        _emit(
                            FlushFailed(
                                table=table_name,
                                error=str(e),
                                affected_count=row_count,
                            ),
                            on_event,
                        )
                        # Update stats
                        if table_name in stats:
                            stats[table_name] = TableStats(
                                mapped=stats[table_name].mapped,
                                errors=stats[table_name].errors,
                                inserted=0,
                                failed=row_count,
                            )
                        raise

        return PipelineResult(
            tables=tables,
            errors=errors,
            _table_class_map=table_class_map,
            _raw_results=raw_results,
            _stats=stats,
        )


def etl(*roots: Any, errors: ErrorMode = "collect") -> PipelineBuilder:
    """Entry point for fluent E→T→L pipelines.

    Args:
        *roots: One or more JSON objects to process.
        errors: Error handling mode - "collect" (default) or "fail_fast".

    Returns:
        A PipelineBuilder for chaining navigation and mapping calls.

    Example:
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table=User, fields=[
                Field("name", get("name"))
            ])
            .run()
        )
    """
    return PipelineBuilder(roots, errors)
