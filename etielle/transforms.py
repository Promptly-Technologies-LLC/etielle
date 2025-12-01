from typing import Any, Callable, List, Optional, Sequence, Union, cast, TypeVar, Iterable, Tuple
from .core import Context, Transform
from collections.abc import Mapping

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
    base_ctx = Context(
        root=root,
        node=container,
        path=tuple(path),
        parent=None,
        key=None,
        index=None,
        slots={},
    )
    yield base_ctx, container


# -----------------------------
# Transform library
# -----------------------------


U = TypeVar("U")
V = TypeVar("V")


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
        segments: List[Union[str, int]] = [
            int(seg) if seg.isdigit() else seg for seg in path.split(".") if seg != ""
        ]
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
        segments: List[Union[str, int]] = [
            int(seg) if seg.isdigit() else seg for seg in path.split(".") if seg != ""
        ]
    else:
        segments = list(path)

    def _t(ctx: Context) -> Any:
        return _resolve_path(ctx.root, segments)

    return _t


def get_from_parent(
    path: Union[str, Sequence[Union[str, int]]], depth: int = 1
) -> Transform[Any]:
    if isinstance(path, str):
        segments: List[Union[str, int]] = [
            int(seg) if seg.isdigit() else seg for seg in path.split(".") if seg != ""
        ]
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
        if isinstance(value, (Mapping, Sequence, str)) and not isinstance(
            value, (bytes, bytearray)
        ):
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
        values = [
            str(v) for v in (tr(ctx) for tr in transforms) if v is not None and v != ""
        ]
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


def apply(func: Callable[[U], V], inner: Transform[U]) -> Transform[V]:
    """Apply a function to the result of another transform."""

    def _t(ctx: Context) -> V:
        return func(inner(ctx))

    return _t
