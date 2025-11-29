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
from typing import TYPE_CHECKING, Any

from etielle.core import Context

if TYPE_CHECKING:
    from etielle.core import Transform
    from etielle.instances import MergePolicy


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
