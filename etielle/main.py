from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple, TypeVar, Generic, Union, cast


# -----------------------------
# Core DSL types
# -----------------------------


@dataclass(frozen=True)
class Context:
    """
    Runtime context while traversing the JSON structure.

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


# -----------------------------
# Helpers
# -----------------------------


def _resolve_path(obj: Any, path: Sequence[str | int]) -> Any:
    value: Any = obj
    for segment in path:
        if isinstance(value, Mapping):
            value = value.get(segment, None)  # type: ignore[arg-type]
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            if isinstance(segment, int):
                if 0 <= segment < len(value):
                    value = value[segment]
                else:
                    return None
            else:
                return None
        else:
            return None
    return value


def _iter_nodes(root: Any, path: Sequence[str]) -> Iterable[Tuple[Context, Any]]:
    """
    Yields (context, node) pairs by walking to the container at `path` and
    returning it. The caller decides how to iterate the container.
    """
    container = _resolve_path(root, path)
    base_ctx = Context(root=root, node=container, path=tuple(path), parent=None, key=None, index=None, slots={})
    yield base_ctx, container


# -----------------------------
# Transform library
# -----------------------------


U = TypeVar("U")


def _ensure_transform(value: Union[Transform[U], U]) -> Transform[U]:
    if callable(value):
        return cast(Transform[U], value)

    def _lit(_: Context) -> U:
        return cast(U, value)

    return cast(Transform[U], _lit)


def literal(value: U) -> Transform[U]:
    return _ensure_transform(value)


def key() -> Transform[Optional[str]]:
    def _t(ctx: Context) -> Optional[str]:
        return ctx.key

    return _t


def index() -> Transform[Optional[int]]:
    def _t(ctx: Context) -> Optional[int]:
        return ctx.index

    return _t


def get(path: Union[str, Sequence[Union[str, int]]]) -> Transform[Any]:
    """
    Resolve a value relative to the current node using a dot-separated path
    (or an explicit sequence of segments). Supports list indices when an int
    segment is provided.
    """

    if isinstance(path, str):
        segments: List[Union[str, int]] = [int(seg) if seg.isdigit() else seg for seg in path.split(".") if seg != ""]
    else:
        segments = list(path)

    def _t(ctx: Context) -> Any:
        value: Any = ctx.node
        for seg in segments:
            if isinstance(value, Mapping):
                value = value.get(seg, None)  # type: ignore[arg-type]
            elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                if isinstance(seg, int):
                    if 0 <= seg < len(value):
                        value = value[seg]
                    else:
                        return None
                else:
                    return None
            else:
                return None
        return value

    return _t


def get_from_root(path: Union[str, Sequence[Union[str, int]]]) -> Transform[Any]:
    if isinstance(path, str):
        segments: List[Union[str, int]] = [int(seg) if seg.isdigit() else seg for seg in path.split(".") if seg != ""]
    else:
        segments = list(path)

    def _t(ctx: Context) -> Any:
        return _resolve_path(ctx.root, segments)

    return _t


def get_from_parent(path: Union[str, Sequence[Union[str, int]]], depth: int = 1) -> Transform[Any]:
    if isinstance(path, str):
        segments: List[Union[str, int]] = [int(seg) if seg.isdigit() else seg for seg in path.split(".") if seg != ""]
    else:
        segments = list(path)

    def _t(ctx: Context) -> Any:
        parent = ctx.parent
        for _ in range(depth - 1):
            parent = parent.parent if parent else None
        base = parent.node if parent else None
        return _resolve_path(base, segments)

    return _t


def parent_key(depth: int = 1) -> Transform[Optional[str]]:
    def _t(ctx: Context) -> Optional[str]:
        parent = ctx.parent
        for _ in range(depth - 1):
            parent = parent.parent if parent else None
        return parent.key if parent else None

    return _t


def len_of(inner: Transform[Any]) -> Transform[Optional[int]]:
    def _t(ctx: Context) -> Optional[int]:
        value = inner(ctx)
        if isinstance(value, (Mapping, Sequence, str)) and not isinstance(value, (bytes, bytearray)):
            return len(value)  # type: ignore[arg-type]
        return None

    return _t


def concat(*parts: Union[str, Transform[Any]]) -> Transform[str]:
    transforms: List[Transform[Any]] = [_ensure_transform(p) for p in parts]

    def _t(ctx: Context) -> str:
        values = ["" if v is None else str(v) for v in (tr(ctx) for tr in transforms)]
        return "".join(values)

    return _t


def format_id(*parts: Union[str, Transform[Any]], sep: str = "_") -> Transform[str]:
    transforms: List[Transform[Any]] = [_ensure_transform(p) for p in parts]

    def _t(ctx: Context) -> str:
        values = [str(v) for v in (tr(ctx) for tr in transforms) if v is not None and v != ""]
        return sep.join(values)

    return _t


def coalesce(*inners: Transform[Any]) -> Transform[Any]:
    def _t(ctx: Context) -> Any:
        for tr in inners:
            v = tr(ctx)
            if v is not None:
                return v
        return None

    return _t


# -----------------------------
# Executor
# -----------------------------


def _iter_traversal_nodes(root: Any, spec: TraversalSpec) -> Iterable[Context]:
    for base_ctx, outer in _iter_nodes(root, spec.path):
        def yield_from_container(parent_ctx: Context, container: Any, iterate_items: bool) -> Iterable[Context]:
            if iterate_items:
                if isinstance(container, Mapping):
                    for k, v in container.items():
                        yield Context(
                            root=root,
                            node=v,
                            path=parent_ctx.path + (str(k),),
                            parent=parent_ctx,
                            key=str(k),
                            index=None,
                            slots={},
                        )
            else:
                if isinstance(container, Sequence) and not isinstance(container, (str, bytes)):
                    for i, v in enumerate(container):
                        yield Context(
                            root=root,
                            node=v,
                            path=parent_ctx.path + (i,),
                            parent=parent_ctx,
                            key=None,
                            index=i,
                            slots={},
                        )
                else:
                    # Emit a single context for non-iterable container (e.g., root object)
                    yield Context(
                        root=root,
                        node=container,
                        path=parent_ctx.path,
                        parent=parent_ctx,
                        key=None,
                        index=None,
                        slots={},
                    )

        # If no inner path, iterate outer container directly
        if not spec.inner_path:
            yield from yield_from_container(base_ctx, outer, spec.iterate_items)
            continue

        # Iterate outer container first, then inner container under each outer node
        for outer_ctx in yield_from_container(base_ctx, outer, spec.iterate_items):
            inner_container = _resolve_path(outer_ctx.node, spec.inner_path)
            inner_iter_items = bool(spec.inner_iterate_items)
            for inner_ctx in yield_from_container(outer_ctx, inner_container, inner_iter_items):
                yield inner_ctx


def run_mapping(root: Any, spec: MappingSpec) -> Dict[str, List[Dict[str, Any]]]:
    """
    Execute mapping spec against root JSON, returning rows per table.

    Rows are merged by composite join keys per table. If any join-key part is
    None/empty, the row is skipped.
    """
    table_to_index: Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]] = {}

    for traversal in spec.traversals:
        for ctx in _iter_traversal_nodes(root, traversal):
            for emit in traversal.emits:
                # Compute join key
                key_parts: List[Any] = [tr(ctx) for tr in emit.join_keys]
                if any(part is None or part == "" for part in key_parts):
                    continue
                composite_key = tuple(key_parts)

                row = table_to_index.setdefault(emit.table, {}).setdefault(composite_key, {})

                # Compute fields
                for fld in emit.fields:
                    value = fld.transform(ctx)
                    row[fld.name] = value

    # Convert indexes to lists, and ensure an 'id' exists if single join key is provided
    result: Dict[str, List[Dict[str, Any]]] = {}
    for table, index in table_to_index.items():
        rows: List[Dict[str, Any]] = []
        for key_tuple, data in index.items():
            if len(key_tuple) == 1 and "id" not in data:
                data["id"] = key_tuple[0]
            rows.append(data)
        result[table] = rows
    return result
