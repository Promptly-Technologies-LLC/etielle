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
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from etielle.core import Context

if TYPE_CHECKING:
    from etielle.core import Transform
    from etielle.instances import MergePolicy

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
        return self


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
