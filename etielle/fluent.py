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
    extra_params = params[1:]

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
    """Result from running a pipeline without database loading.

    Attributes:
        tables: Access tables by string name or model class.
        errors: Validation/transform errors keyed by table then row key.
    """

    _tables: dict[str, dict[tuple[Any, ...], Any]]
    _errors: dict[str, dict[tuple[Any, ...], list[str]]]
    _table_class_map: dict[str, type] | None = None

    def __init__(
        self,
        tables: dict[str, dict[tuple[Any, ...], Any]],
        errors: dict[str, dict[tuple[Any, ...], list[str]]],
        _table_class_map: dict[str, type] | None = None
    ) -> None:
        self._tables = tables
        self._errors = errors
        self._table_class_map = _table_class_map

    @property
    def tables(self) -> _TablesProxy:
        """Access extracted tables by name or model class."""
        return _TablesProxy(self._tables, self._table_class_map)

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

    # Check for Pydantic
    try:
        from pydantic import BaseModel
        if issubclass(table_class, BaseModel):
            from etielle.instances import PydanticBuilder
            return PydanticBuilder(table_class)
    except ImportError:
        pass

    # Check for SQLAlchemy/SQLModel ORM
    if hasattr(table_class, "__tablename__") and hasattr(table_class, "__mapper__"):
        from etielle.instances import ConstructorBuilder
        return ConstructorBuilder(table_class)

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
        parent: type,
        by: dict[str, str]
    ) -> PipelineBuilder:
        """Define a relationship from the current table to a parent table.

        The `by` dict maps child field names to parent field names.
        Both Field and TempField names can be used.

        Args:
            parent: The parent model class.
            by: Mapping of {child_field: parent_field}.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If called without a preceding map_to().

        Example:
            .map_to(table=Post, fields=[
                TempField("user_id", get("author_id"))
            ])
            .link_to(User, by={"user_id": "id"})
        """
        if not self._emissions:
            raise ValueError("link_to() must follow a map_to() call")

        last_emission = self._emissions[-1]
        relationship = {
            "child_table": last_emission["table"],
            "parent_class": parent,
            "parent_table": getattr(parent, "__tablename__", parent.__name__.lower()),
            "by": dict(by),
            "emission_index": len(self._emissions) - 1,
        }
        self._relationships.append(relationship)
        return self

    def load(self, session: Any) -> PipelineBuilder:
        """Configure database session for persistence.

        When load() is called before run(), the pipeline will:
        1. Build all instances in memory
        2. Bind relationships
        3. Add instances to the session
        4. Flush (but not commit)

        The caller controls the transaction (commit/rollback).

        Args:
            session: SQLAlchemy/SQLModel session.

        Returns:
            Self for method chaining.

        Example:
            result = (
                etl(data)
                .goto("users").each()
                .map_to(table=User, fields=[...])
                .load(session)
                .run()
            )
            session.commit()  # Caller controls transaction
        """
        self._session = session
        return self

    def _build_traversal_specs(self) -> list[TraversalSpec]:
        """Convert accumulated emissions to TraversalSpec objects."""
        from etielle.core import MappingSpec, TraversalSpec, TableEmit, Field as CoreField
        from etielle.transforms import literal

        specs = []

        for emission in self._emissions:
            # Determine path and iteration mode
            path: list[str] = emission["path"]
            iteration_points: list[list[str]] = emission["iteration_points"]

            # Build fields and join_keys from Field/TempField
            fields = []
            join_keys = []
            merge_policies = {}
            field_map = {f.name: f.transform for f in emission["fields"]}

            # If join_on specified, use those field names to build join_keys
            if emission["join_on"]:
                for key_name in emission["join_on"]:
                    if key_name in field_map:
                        join_keys.append(field_map[key_name])
                # Only add non-join_on Fields to output
                for f in emission["fields"]:
                    if isinstance(f, Field) and f.name not in emission["join_on"]:
                        fields.append(CoreField(f.name, f.transform))
                        if f.merge is not None:
                            merge_policies[f.name] = f.merge
            else:
                # No explicit join_on - use TempField/Field distinction
                for f in emission["fields"]:
                    if isinstance(f, TempField):
                        # TempFields become join keys
                        join_keys.append(f.transform)
                    else:
                        # Regular Fields go to output
                        fields.append(CoreField(f.name, f.transform))
                        if f.merge is not None:
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

                table_emit = InstanceEmit(
                    table=emission["table"],
                    join_keys=tuple(join_keys) if join_keys else (literal(None),),
                    fields=tuple(field_specs),
                    builder=builder,
                    policies=merge_policies,
                    strict_mode=strict_mode
                )
            else:
                # Use simpler TableEmit when no model class or merge policies needed
                table_emit = TableEmit(
                    table=emission["table"],
                    join_keys=tuple(join_keys) if join_keys else (literal(None),),
                    fields=tuple(fields)
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

    def run(self) -> PipelineResult:
        """Execute the pipeline and return results.

        If load() was called, also persists to the database.

        Returns:
            PipelineResult with tables and errors.
        """
        from etielle.core import MappingSpec, TraversalSpec, TableEmit, Field as CoreField
        from etielle.executor import run_mapping

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
            raw_results = run_mapping(root, mapping_spec)

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

        # Handle relationship binding and staged flushing
        rel_specs: list[Any] = []
        child_to_parent: dict[int, dict[tuple[Any, ...], tuple[Any, ...]]] = {}

        if self._relationships:
            from etielle.relationships import compute_relationship_keys, bind_many_to_one, ManyToOneSpec

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

            # For now, use the first root for relationship key computation
            # This works when all data is in one root, but may need enhancement
            # for cross-root relationships in the future
            first_root = self._roots[0] if self._roots else {}
            child_to_parent = compute_relationship_keys(first_root, all_specs, rel_specs)

            # If no session, bind all relationships now (non-DB use case)
            if self._session is None:
                bind_many_to_one(raw_results, rel_specs, child_to_parent, fail_on_missing=False)

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

        # If session provided, flush in dependency order
        if self._session is not None:
            from etielle.utils import topological_sort

            # Build flush order from dependency graph (parents before children)
            dep_graph = self._build_dependency_graph()
            all_tables = set(tables.keys())
            flush_order = topological_sort(dep_graph, all_tables)

            for table_name in flush_order:
                if table_name not in tables:
                    continue

                # Add and flush this table's instances
                for key, instance in tables[table_name].items():
                    if not isinstance(instance, dict):
                        self._session.add(instance)
                self._session.flush()

                # After flush, this table's instances have IDs
                # Bind relationships where this table is the parent
                for idx, spec in enumerate(rel_specs):
                    if spec.parent_table == table_name:
                        child_table = spec.child_table
                        if child_table not in tables:
                            continue
                        key_map = child_to_parent.get(idx, {})
                        parent_instances = tables[table_name]
                        child_instances = tables[child_table]
                        for child_key, child_obj in child_instances.items():
                            if isinstance(child_obj, dict):
                                continue
                            parent_key = key_map.get(child_key)
                            if parent_key and parent_key in parent_instances:
                                parent_obj = parent_instances[parent_key]
                                if not isinstance(parent_obj, dict):
                                    setattr(child_obj, spec.attr, parent_obj)

        return PipelineResult(
            tables=tables,
            errors=errors,
            _table_class_map=table_class_map
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
