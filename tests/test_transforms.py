from etielle.core import Context
import pytest

from etielle.transforms import (
    apply,
    concat,
    coalesce,
    format_id,
    get,
    get_from_parent,
    get_from_root,
    index,
    key,
    len_of,
    literal,
    parent_key,
)

from typing import Any, TypedDict


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
) -> Context:
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
    root: _Root = {
        "id": "root-1",
        "child": {"id": "child-1", "grand": {"id": "grand-1"}},
    }
    parent_ctx = make_ctx(root=root, node=root["child"], path=("child",))
    ctx = make_ctx(
        root=root,
        node=root["child"]["grand"],
        path=("child", "grand"),
        parent=parent_ctx,
    )

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


# --- apply transform tests ---


def test_apply_int_coercion():
    ctx = make_ctx(root={}, node={"age": "42"})
    assert apply(int, get("age"))(ctx) == 42


def test_apply_float_coercion():
    ctx = make_ctx(root={}, node={"price": "19.99"})
    assert apply(float, get("price"))(ctx) == 19.99


def test_apply_bool_coercion():
    ctx = make_ctx(root={}, node={"count": 1})
    assert apply(bool, get("count"))(ctx) is True


def test_apply_string_method():
    ctx = make_ctx(root={}, node={"name": "  alice  "})
    assert apply(str.strip, get("name"))(ctx) == "alice"


def test_apply_propagates_exceptions():
    ctx = make_ctx(root={}, node={"age": "not a number"})
    with pytest.raises(ValueError):
        apply(int, get("age"))(ctx)


def test_apply_none_input_raises():
    ctx = make_ctx(root={}, node={"age": None})
    with pytest.raises(TypeError):
        apply(int, get("age"))(ctx)


def test_apply_custom_callable():
    ctx = make_ctx(root={}, node={"val": "5"})
    assert apply(lambda x: int(x) * 2, get("val"))(ctx) == 10


# --- edge case tests ---


def test_get_empty_path_returns_current_node():
    """get([]) with empty path should return the current node"""
    data = {"name": "Alice", "age": 30}
    ctx = make_ctx(root={}, node=data)
    assert get([])(ctx) == data


def test_get_missing_field_returns_none():
    """get() on a missing field should return None"""
    data = {"name": "Alice"}
    ctx = make_ctx(root={}, node=data)
    assert get("missing")(ctx) is None
    assert get("name.nested.deep")(ctx) is None


def test_get_out_of_bounds_index_returns_none():
    """get() with out of bounds index should return None"""
    data = {"items": ["a", "b", "c"]}
    ctx = make_ctx(root={}, node=data)
    assert get("items.10")(ctx) is None
    assert get(["items", 100])(ctx) is None
    assert get(["items", -1])(ctx) is None


def test_get_from_parent_depth_2_grandparent():
    """get_from_parent(field, depth=2) should access grandparent"""
    root: dict[str, Any] = {"id": "root", "child": {"id": "child", "grand": {"id": "grand"}}}

    # Build context chain: root -> child -> grand
    # Need to include root level in the chain
    root_ctx = make_ctx(root=root, node=root, path=())
    child_ctx = make_ctx(
        root=root, node=root["child"], path=("child",), parent=root_ctx
    )
    grand_ctx = make_ctx(
        root=root,
        node=root["child"]["grand"],
        path=("child", "grand"),
        parent=child_ctx,
    )

    # From grand, depth=2 should reach root
    assert get_from_parent("id", depth=2)(grand_ctx) == "root"


def test_get_from_parent_depth_3_great_grandparent():
    """get_from_parent(field, depth=3) should access great-grandparent"""
    root: dict[str, Any] = {
        "id": "root",
        "child": {
            "id": "child",
            "grand": {"id": "grand", "great": {"id": "great"}},
        },
    }

    # Build context chain: root -> child -> grand -> great
    root_ctx = make_ctx(root=root, node=root, path=())
    child_ctx = make_ctx(
        root=root, node=root["child"], path=("child",), parent=root_ctx
    )
    grand_ctx = make_ctx(
        root=root,
        node=root["child"]["grand"],
        path=("child", "grand"),
        parent=child_ctx,
    )
    great_ctx = make_ctx(
        root=root,
        node=root["child"]["grand"]["great"],
        path=("child", "grand", "great"),
        parent=grand_ctx,
    )

    # From great, depth=3 should reach root
    assert get_from_parent("id", depth=3)(great_ctx) == "root"


def test_get_from_parent_no_parent_returns_none():
    """get_from_parent() when no parent exists should return None"""
    ctx = make_ctx(root={}, node={"id": "node"}, parent=None)
    assert get_from_parent("id")(ctx) is None


def test_get_from_parent_depth_exceeds_chain_returns_none():
    """get_from_parent() when depth exceeds chain should return None"""
    root = {"id": "root", "child": {"id": "child"}}
    parent_ctx = make_ctx(root=root, node=root, path=())
    child_ctx = make_ctx(
        root=root, node=root["child"], path=("child",), parent=parent_ctx
    )

    # Only 2 levels in chain, depth=5 should return None
    assert get_from_parent("id", depth=5)(child_ctx) is None


def test_parent_key_basic_usage():
    """parent_key() should return the key from parent context"""
    root = {"users": {"alice": {"name": "Alice"}}}
    parent_ctx = make_ctx(
        root=root, node=root["users"], path=("users",), dict_key="users"
    )
    child_ctx = make_ctx(
        root=root,
        node=root["users"]["alice"],
        path=("users", "alice"),
        parent=parent_ctx,
        dict_key="alice",
    )

    assert parent_key()(child_ctx) == "users"


def test_parent_key_depth_2_grandparent():
    """parent_key(depth=2) should return grandparent's key"""
    root = {"level1": {"level2": {"level3": {}}}}

    level1_ctx = make_ctx(
        root=root, node=root["level1"], path=("level1",), dict_key="level1"
    )
    level2_ctx = make_ctx(
        root=root,
        node=root["level1"]["level2"],
        path=("level1", "level2"),
        parent=level1_ctx,
        dict_key="level2",
    )
    level3_ctx = make_ctx(
        root=root,
        node=root["level1"]["level2"]["level3"],
        path=("level1", "level2", "level3"),
        parent=level2_ctx,
        dict_key="level3",
    )

    assert parent_key(depth=2)(level3_ctx) == "level1"


def test_parent_key_no_parent_returns_none():
    """parent_key() when no parent should return None"""
    ctx = make_ctx(root={}, node={}, parent=None)
    assert parent_key()(ctx) is None


def test_literal_with_various_types():
    """literal() should work with None, bool, dict, list"""
    ctx = make_ctx(root={}, node={})

    assert literal(None)(ctx) is None
    assert literal(True)(ctx) is True
    assert literal(False)(ctx) is False
    assert literal({"key": "value"})(ctx) == {"key": "value"}
    assert literal([1, 2, 3])(ctx) == [1, 2, 3]
    assert literal(42)(ctx) == 42
    assert literal("hello")(ctx) == "hello"


def test_key_when_not_iterating_dict_returns_none():
    """key() when not iterating a dict should return None"""
    ctx = make_ctx(root={}, node={}, dict_key=None)
    assert key()(ctx) is None


def test_index_when_not_iterating_list_returns_none():
    """index() when not iterating a list should return None"""
    ctx = make_ctx(root={}, node={}, list_index=None)
    assert index()(ctx) is None


def test_concat_with_none_values():
    """concat() should convert None values to empty strings"""
    data = {"first": "Ada", "middle": None, "last": "Lovelace"}
    ctx = make_ctx(root={}, node=data)

    result = concat(get("first"), " ", get("middle"), " ", get("last"))(ctx)
    # None becomes empty string: "Ada" + " " + "" + " " + "Lovelace" = "Ada  Lovelace"
    assert result == "Ada  Lovelace"


def test_concat_with_numbers():
    """concat() should auto-convert numbers to strings"""
    data = {"count": 42, "price": 19.99}
    ctx = make_ctx(root={}, node=data)

    assert concat("Count: ", get("count"))(ctx) == "Count: 42"
    assert concat("Price: $", get("price"))(ctx) == "Price: $19.99"


def test_format_id_skips_none_values():
    """format_id() should skip None values"""
    data = {"first": "Ada", "middle": None, "last": "Lovelace"}
    ctx = make_ctx(root={}, node=data)

    result = format_id(get("first"), get("middle"), get("last"))(ctx)
    assert result == "Ada_Lovelace"


def test_format_id_skips_empty_strings():
    """format_id() should skip empty strings"""
    data = {"first": "Ada", "middle": "", "last": "Lovelace"}
    ctx = make_ctx(root={}, node=data)

    result = format_id(get("first"), get("middle"), get("last"))(ctx)
    assert result == "Ada_Lovelace"


def test_format_id_default_underscore_separator():
    """format_id() should use underscore as default separator"""
    data = {"prefix": "user", "id": "123"}
    ctx = make_ctx(root={}, node=data)

    result = format_id(get("prefix"), get("id"))(ctx)
    assert result == "user_123"


def test_coalesce_when_first_succeeds():
    """coalesce() should return first non-None value"""
    data = {"first": "Ada", "second": "Lovelace"}
    ctx = make_ctx(root={}, node=data)

    result = coalesce(get("first"), get("second"), literal("default"))(ctx)
    assert result == "Ada"


def test_coalesce_when_all_return_none():
    """coalesce() should return None when all transforms return None"""
    data = {}
    ctx = make_ctx(root={}, node=data)

    result = coalesce(get("missing1"), get("missing2"), get("missing3"))(ctx)
    assert result is None


def test_coalesce_with_literal_fallback():
    """coalesce() should fall back to literal value"""
    data = {}
    ctx = make_ctx(root={}, node=data)

    result = coalesce(get("missing"), literal("default"))(ctx)
    assert result == "default"


def test_len_of_with_dict():
    """len_of() should return length of dict"""
    data = {"items": {"a": 1, "b": 2, "c": 3}}
    ctx = make_ctx(root={}, node=data)

    assert len_of(get("items"))(ctx) == 3


def test_len_of_with_empty_collections():
    """len_of() should return 0 for empty collections"""
    data = {"empty_list": [], "empty_dict": {}, "empty_string": ""}
    ctx = make_ctx(root={}, node=data)

    assert len_of(get("empty_list"))(ctx) == 0
    assert len_of(get("empty_dict"))(ctx) == 0
    assert len_of(get("empty_string"))(ctx) == 0


def test_len_of_with_none_input():
    """len_of() should return None when input is None"""
    data = {"value": None}
    ctx = make_ctx(root={}, node=data)

    assert len_of(get("value"))(ctx) is None
    assert len_of(get("missing"))(ctx) is None
