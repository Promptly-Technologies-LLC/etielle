from etielle.core import (
    Context
)
from etielle.transforms import (
    concat,
    coalesce,
    format_id,
    get,
    get_from_parent,
    get_from_root,
    index,
    key,
    len_of,
)

from typing import TypedDict


class _Grand(TypedDict):
    id: str


class _Child(TypedDict):
    id: str
    grand: _Grand


class _Root(TypedDict):
    id: str
    child: _Child


def make_ctx(
    *,
    root: object,
    node: object,
    path: tuple[str | int, ...] = (),
    parent: Context | None = None,
    dict_key: str | None = None,
    list_index: int | None = None,
)-> Context:
    return Context(
        root=root,
        node=node,
        path=path,
        parent=parent,
        key=dict_key,
        index=list_index,
        slots={},
    )


def test_get_with_dot_paths_and_list_indices():
    data = {"user": {"names": ["Ada", "Lovelace"]}}
    ctx = make_ctx(root=data, node=data)

    assert get("user")(ctx) == {"names": ["Ada", "Lovelace"]}
    assert get("user.names")(ctx) == ["Ada", "Lovelace"]
    assert get("user.names.0")(ctx) == "Ada"
    assert get(["user", "names", 1])(ctx) == "Lovelace"


def test_get_from_root_and_parent():
    root: _Root = {"id": "root-1", "child": {"id": "child-1", "grand": {"id": "grand-1"}}}
    parent_ctx = make_ctx(root=root, node=root["child"], path=("child",))
    ctx = make_ctx(root=root, node=root["child"]["grand"], path=("child", "grand"), parent=parent_ctx)

    assert get_from_root("id")(ctx) == "root-1"
    assert get_from_parent("id")(ctx) == "child-1"


def test_key_and_index_helpers():
    ctx_key = make_ctx(root={}, node={}, dict_key="alpha")
    assert key()(ctx_key) == "alpha"

    ctx_index = make_ctx(root={}, node={}, list_index=3)
    assert index()(ctx_index) == 3


def test_concat_format_coalesce_len_of():
    data = {"user": {"first": "Ada", "last": "Lovelace", "tags": ["a", "b"]}}
    ctx = make_ctx(root=data, node=data["user"]) 

    assert concat("Hello, ", get("first"))(ctx) == "Hello, Ada"
    assert format_id(get("first"), get("last"), sep="-")(ctx) == "Ada-Lovelace"

    prefers_first = coalesce(get("middle"), get("first"), get("last"))
    assert prefers_first(ctx) == "Ada"

    assert len_of(get("tags"))(ctx) == 2
    assert len_of(get("first"))(ctx) == 3
    # bytes should not report length
    ctx_bytes = make_ctx(root={}, node={"b": b"abc"})
    assert len_of(get("b"))(ctx_bytes) is None


