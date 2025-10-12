from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol, Sequence, Tuple, TypeVar, Generic


# -----------------------------
# Core DSL types
# -----------------------------


@dataclass(frozen=True)
class Context:
    """
    Runtime context while traversing the JSON structure.
    During traversal, a base context is created that is shared by all nodes.
    Subsequent contexts created during iteration extend the parent's path and link to the parent context.
    Each new context gets fresh slots, but you can walk up the chain with get_from_parent.

    - root: original full JSON payload
    - node: current node under iteration
    - path: absolute path from root to this node (tuple of str|int)
    - parent: parent context if any
    - key: current mapping key when iterating dicts (stringified)
    - index: current index when iterating lists
    - slots: scratchpad for intermediate identifiers if needed by transforms
    """

    root: Any
    node: Any
    path: Tuple[str | int, ...]
    parent: Optional["Context"]
    key: Optional[str]
    index: Optional[int]
    slots: Mapping[str, Any] = field(default_factory=dict)


T = TypeVar("T")


class Transform(Protocol, Generic[T]):
    """
    Transforms are functions that take a Context and return a value.
    They're composable, side-effect free, and lazily evaluated in the context of the current traversal step.
    """
    def __call__(self, ctx: Context) -> T:  # pragma: no cover - interface only
        ...


@dataclass(frozen=True)
class Field:
    name: str
    transform: Transform[Any]


@dataclass(frozen=True)
class TableEmit:
    """
    Describes how to produce rows for a table from a given traversal context.

    - table: table name
    - fields: list of computed fields
    - join_keys: functions that compute the composite key for merging rows
    """

    table: str
    fields: Sequence[Field]
    join_keys: Sequence[Transform[Any]]


@dataclass(frozen=True)
class TraversalSpec:
    """
    How to reach and iterate a collection of nodes under root.

    - path: list of keys from root to the outer container (e.g., ["blocks"])
    - iterate_items: if True, iterate dict items (key, value); else iterate list values on the outer container
    - inner_path: optional path inside each outer node to reach an inner container (e.g., ["elements"]). If provided, iterate that container instead of the outer node
    - inner_iterate_items: if True, iterate dict items for inner_path; else list values
    - emits: table emitters to run for each yielded node
    """

    path: Sequence[str]
    iterate_items: bool
    emits: Sequence[TableEmit]
    inner_path: Optional[Sequence[str]] = None
    inner_iterate_items: Optional[bool] = None


@dataclass(frozen=True)
class MappingSpec:
    traversals: Sequence[TraversalSpec]
