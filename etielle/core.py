from __future__ import annotations
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Generic,
    cast,
    TYPE_CHECKING,
    Dict,
    List,
    Literal,
)

if TYPE_CHECKING:
    # Avoid runtime import cycle; only for typing
    from .instances import InstanceEmit


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
    slots: dict[str, Any] = field(default_factory=dict)


T = TypeVar("T")
U = TypeVar("U")


Transform = Callable[[Context], T]
"""
Transforms are functions that take a Context and return a value.
They're composable, side-effect free, and lazily evaluated in the context of the current traversal step.
"""


# -----------------------------
# Field selector API
# -----------------------------


Attr = Callable[[T], U]


class _SelectorInvalid(Exception):
    def __init__(self, reason: str, path: Tuple[str, ...] | None = None) -> None:
        self.reason = reason
        self.path = path or ()


class _FieldTrace:
    """
    Lightweight tracer that records attribute access chains and rejects operations
    other than attribute access. This allows us to validate that a selector lambda
    consists of exactly one attribute access, with no method calls, indexing, or
    chained attributes.
    """

    __slots__ = ("_path",)

    def __init__(self, path: Tuple[str, ...] = ()) -> None:
        self._path = path

    # Attribute access builds up the path
    def __getattr__(self, name: str) -> "_FieldTrace":
        return _FieldTrace(self._path + (name,))

    # Reject any attempts to call the selected attribute
    def __call__(
        self, *args: Any, **kwargs: Any
    ) -> Any:  # pragma: no cover - defensive
        raise _SelectorInvalid("method call on attribute selector", self._path)

    # Reject indexing / item access
    def __getitem__(self, _: Any) -> Any:  # pragma: no cover - defensive
        raise _SelectorInvalid("indexing on attribute selector", self._path)

    # Common coercions that might be attempted inside the lambda
    def __bool__(self) -> bool:  # pragma: no cover - defensive
        raise _SelectorInvalid("truthiness check on attribute selector", self._path)

    def __str__(self) -> str:  # pragma: no cover - defensive
        raise _SelectorInvalid("stringification of attribute selector", self._path)

    def __int__(self) -> int:  # pragma: no cover - defensive
        raise _SelectorInvalid("numeric coercion of attribute selector", self._path)

    # Expose path for inspection after lambda returns
    @property
    def path(self) -> Tuple[str, ...]:
        return self._path


def field_of(model: type[T], selector: Attr[T, Any]) -> str:
    """
    Resolve a model field name from a type-checked selector lambda.

    Example:
        field_of(UserModel, lambda u: u.email) -> "email"

    Constraints enforced at runtime:
      - Exactly one attribute access must occur.
      - No method calls, indexing, or chained attribute access.
    """
    trace = cast(Any, _FieldTrace())
    try:
        result = selector(trace)
    except _SelectorInvalid as err:
        path_str = ".".join(err.path) if err.path else "<root>"
        raise ValueError(
            f"Invalid field selector: {err.reason} at '{path_str}'"
        ) from None

    if isinstance(result, _FieldTrace):
        if len(result.path) == 1:
            return result.path[0]
        if len(result.path) == 0:
            raise ValueError("The selector must access exactly one attribute")
        raise ValueError(
            f"The selector must access exactly one attribute; got chained path '{'.'.join(result.path)}'"
        )

    # If the lambda returned a non-trace value, then it didn't simply return an attribute access
    raise ValueError("The selector must directly return a single attribute access")


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
    - mode: how to iterate the outer container: "auto" (default), "items" (dict key/value), or "single" (treat as one node)
    - inner_path: optional path inside each outer node to reach an inner container (e.g., ["elements"]). If provided, iterate that container instead of the outer node
    - inner_mode: how to iterate the inner container when inner_path is provided: "auto" (default), "items", or "single"
    - emits: table emitters to run for each yielded node
    """

    path: Sequence[str]
    emits: Sequence[TableEmit | "InstanceEmit[Any]"]
    mode: Literal["auto", "items", "single"] = "auto"
    inner_path: Optional[Sequence[str]] = None
    inner_mode: Literal["auto", "items", "single"] = "auto"


@dataclass(frozen=True)
class MappingSpec:
    traversals: Sequence[TraversalSpec]


# -----------------------------
# Results
# -----------------------------


@dataclass(frozen=True)
class MappingResult(Generic[T]):
    """
    Unified result for both classic table rows and instance builders.

    - instances: mapping from composite join key tuple to instance/row payload
    - update_errors: per-key errors recorded during incremental updates
    - finalize_errors: per-key errors recorded while finalizing/validating instances
    - stats: simple counters to aid diagnostics (keys: num_instances, num_update_errors, num_finalize_errors)
    - indices: secondary indices for relationship linking {field_name: {value: instance}}
    """

    instances: Dict[Tuple[Any, ...], T]
    update_errors: Dict[Tuple[Any, ...], List[str]]
    finalize_errors: Dict[Tuple[Any, ...], List[str]]
    stats: Dict[str, int]
    indices: Dict[str, Dict[Any, T]] = field(default_factory=dict)
