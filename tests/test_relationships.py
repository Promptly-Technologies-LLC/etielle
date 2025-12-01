from dataclasses import dataclass
from typing import Any, Dict

import pytest

from etielle.core import MappingSpec, MappingResult, TraversalSpec
from etielle.transforms import get
from etielle.instances import InstanceEmit, FieldSpec, TypedDictBuilder
from etielle.executor import run_mapping
from etielle.relationships import (
    ManyToOneSpec,
    compute_relationship_keys,
    bind_many_to_one,
    bind_many_to_one_via_index,
)


@dataclass
class User:
    id: str
    name: str


@dataclass
class Post:
    id: str
    title: str
    user: User | None = None


@dataclass
class Category:
    id: str
    name: str


@dataclass
class PostWithCategory:
    id: str
    title: str
    user: User | None = None
    category: Category | None = None


def _user_factory(payload: Dict[str, Any]) -> User:
    return User(id=str(payload["id"]), name=str(payload.get("name", "")))


def _post_factory(payload: Dict[str, Any]) -> Post:
    return Post(id=str(payload["id"]), title=str(payload.get("title", "")))


def _category_factory(payload: Dict[str, Any]) -> Category:
    return Category(id=str(payload["id"]), name=str(payload.get("name", "")))


def _post_with_category_factory(payload: Dict[str, Any]) -> PostWithCategory:
    return PostWithCategory(
        id=str(payload["id"]),
        title=str(payload.get("title", "")),
    )


def test_bind_many_to_one_success():
    root = {
        "users": [
            {"id": "u1", "name": "Alice"},
            {"id": "u2", "name": "Bob"},
        ],
        "posts": [
            {"id": "p1", "title": "Hello", "user_id": "u1"},
            {"id": "p2", "title": "World", "user_id": "u2"},
        ],
    }

    users_emit = InstanceEmit[User](
        table="users",
        join_keys=[get("id")],
        fields=[
            FieldSpec(selector="id", transform=get("id")),
            FieldSpec(selector="name", transform=get("name")),
        ],
        builder=TypedDictBuilder(_user_factory),
    )

    posts_emit = InstanceEmit[Post](
        table="posts",
        join_keys=[get("id")],
        fields=[
            FieldSpec(selector="id", transform=get("id")),
            FieldSpec(selector="title", transform=get("title")),
        ],
        builder=TypedDictBuilder(_post_factory),
    )

    mapping = MappingSpec(
        traversals=[
            TraversalSpec(path=["users"], mode="auto", emits=[users_emit]),
            TraversalSpec(path=["posts"], mode="auto", emits=[posts_emit]),
        ]
    )

    rels = [
        ManyToOneSpec(
            child_table="posts",
            parent_table="users",
            attr="user",
            child_to_parent_key=[get("user_id")],
            required=True,
        )
    ]

    results = run_mapping(root, mapping)
    sidecar = compute_relationship_keys(root, mapping.traversals, rels)
    bind_many_to_one(results, rels, sidecar, fail_on_missing=True)

    users = results["users"].instances
    posts = results["posts"].instances

    assert posts[("p1",)].user is users[("u1",)]
    assert posts[("p2",)].user is users[("u2",)]


def test_bind_many_to_one_missing_parent_raises():
    root = {
        "users": [
            {"id": "u1", "name": "Alice"},
        ],
        "posts": [
            {"id": "p1", "title": "Hello", "user_id": "u1"},
            {"id": "p2", "title": "World", "user_id": "u_missing"},
        ],
    }

    users_emit = InstanceEmit[User](
        table="users",
        join_keys=[get("id")],
        fields=[
            FieldSpec(selector="id", transform=get("id")),
            FieldSpec(selector="name", transform=get("name")),
        ],
        builder=TypedDictBuilder(_user_factory),
    )

    posts_emit = InstanceEmit[Post](
        table="posts",
        join_keys=[get("id")],
        fields=[
            FieldSpec(selector="id", transform=get("id")),
            FieldSpec(selector="title", transform=get("title")),
        ],
        builder=TypedDictBuilder(_post_factory),
    )

    mapping = MappingSpec(
        traversals=[
            TraversalSpec(path=["users"], mode="auto", emits=[users_emit]),
            TraversalSpec(path=["posts"], mode="auto", emits=[posts_emit]),
        ]
    )

    rels = [
        ManyToOneSpec(
            child_table="posts",
            parent_table="users",
            attr="user",
            child_to_parent_key=[get("user_id")],
            required=True,
        )
    ]

    results = run_mapping(root, mapping)
    sidecar = compute_relationship_keys(root, mapping.traversals, rels)
    with pytest.raises(RuntimeError):
        bind_many_to_one(results, rels, sidecar, fail_on_missing=True)


def test_bind_many_to_one_multiple_specs_same_child_table():
    root = {
        "users": [
            {"id": "u1", "name": "Alice"},
            {"id": "u2", "name": "Bob"},
        ],
        "categories": [
            {"id": "c1", "name": "News"},
            {"id": "c2", "name": "Tech"},
        ],
        "posts": [
            {
                "id": "p1",
                "title": "Hello",
                "user_id": "u1",
                "category_id": "c1",
            },
            {
                "id": "p2",
                "title": "World",
                "user_id": "u2",
                "category_id": "c2",
            },
        ],
    }

    users_emit = InstanceEmit[User](
        table="users",
        join_keys=[get("id")],
        fields=[
            FieldSpec(selector="id", transform=get("id")),
            FieldSpec(selector="name", transform=get("name")),
        ],
        builder=TypedDictBuilder(_user_factory),
    )

    categories_emit = InstanceEmit[Category](
        table="categories",
        join_keys=[get("id")],
        fields=[
            FieldSpec(selector="id", transform=get("id")),
            FieldSpec(selector="name", transform=get("name")),
        ],
        builder=TypedDictBuilder(_category_factory),
    )

    posts_emit = InstanceEmit[PostWithCategory](
        table="posts",
        join_keys=[get("id")],
        fields=[
            FieldSpec(selector="id", transform=get("id")),
            FieldSpec(selector="title", transform=get("title")),
        ],
        builder=TypedDictBuilder(_post_with_category_factory),
    )

    mapping = MappingSpec(
        traversals=[
            TraversalSpec(path=["users"], mode="auto", emits=[users_emit]),
            TraversalSpec(path=["categories"], mode="auto", emits=[categories_emit]),
            TraversalSpec(path=["posts"], mode="auto", emits=[posts_emit]),
        ]
    )

    rels = [
        ManyToOneSpec(
            child_table="posts",
            parent_table="users",
            attr="user",
            child_to_parent_key=[get("user_id")],
            required=True,
        ),
        ManyToOneSpec(
            child_table="posts",
            parent_table="categories",
            attr="category",
            child_to_parent_key=[get("category_id")],
            required=True,
        ),
    ]

    results = run_mapping(root, mapping)
    sidecar = compute_relationship_keys(root, mapping.traversals, rels)
    bind_many_to_one(results, rels, sidecar, fail_on_missing=True)

    users = results["users"].instances
    categories = results["categories"].instances
    posts = results["posts"].instances

    assert posts[("p1",)].user is users[("u1",)]
    assert posts[("p1",)].category is categories[("c1",)]
    assert posts[("p2",)].user is users[("u2",)]
    assert posts[("p2",)].category is categories[("c2",)]


def test_bind_many_to_one_via_index():
    """Relationship binding should use secondary indices instead of join keys."""

    @dataclass
    class Parent:
        external_id: str

    @dataclass
    class Child:
        parent_ref: str
        parent: Parent | None = None

    parent1 = Parent(external_id="P1")
    parent2 = Parent(external_id="P2")
    child1 = Child(parent_ref="P1")
    child2 = Child(parent_ref="P2")

    # Parents indexed by external_id (secondary index)
    parent_result = MappingResult(
        instances={},  # No join key
        update_errors={},
        finalize_errors={},
        stats={},
        indices={"external_id": {"P1": parent1, "P2": parent2}},
    )

    # Children in a list (no join key)
    children_list = [child1, child2]

    # Bind using secondary index
    errors = bind_many_to_one_via_index(
        parent_result=parent_result,
        children=children_list,
        parent_index_field="external_id",
        child_lookup_field="parent_ref",
        relationship_attr="parent",
    )

    assert errors == []
    assert child1.parent is parent1
    assert child2.parent is parent2


def test_bind_many_to_one_via_index_missing_parent():
    """Should collect errors when parent not found in index."""

    @dataclass
    class Parent:
        external_id: str

    @dataclass
    class Child:
        parent_ref: str
        parent: Parent | None = None

    parent1 = Parent(external_id="P1")
    child1 = Child(parent_ref="P1")
    child2 = Child(parent_ref="P_MISSING")

    # Only P1 in index
    parent_result = MappingResult(
        instances={},
        update_errors={},
        finalize_errors={},
        stats={},
        indices={"external_id": {"P1": parent1}},
    )

    children_list = [child1, child2]

    # Bind with required=True (default)
    errors = bind_many_to_one_via_index(
        parent_result=parent_result,
        children=children_list,
        parent_index_field="external_id",
        child_lookup_field="parent_ref",
        relationship_attr="parent",
    )

    assert len(errors) == 1
    assert "P_MISSING" in errors[0]
    assert child1.parent is parent1
    assert child2.parent is None


def test_bind_many_to_one_via_index_missing_lookup_value():
    """Should handle children with None lookup values."""

    @dataclass
    class Parent:
        external_id: str

    @dataclass
    class Child:
        parent_ref: str | None
        parent: Parent | None = None

    parent1 = Parent(external_id="P1")
    child1 = Child(parent_ref="P1")
    child2 = Child(parent_ref=None)

    parent_result = MappingResult(
        instances={},
        update_errors={},
        finalize_errors={},
        stats={},
        indices={"external_id": {"P1": parent1}},
    )

    children_list = [child1, child2]

    # Bind with required=True
    errors = bind_many_to_one_via_index(
        parent_result=parent_result,
        children=children_list,
        parent_index_field="external_id",
        child_lookup_field="parent_ref",
        relationship_attr="parent",
    )

    assert len(errors) == 1
    assert "no value for parent_ref" in errors[0]
    assert child1.parent is parent1
    assert child2.parent is None


def test_bind_many_to_one_via_index_not_required():
    """Should not collect errors when required=False."""

    @dataclass
    class Parent:
        external_id: str

    @dataclass
    class Child:
        parent_ref: str
        parent: Parent | None = None

    parent1 = Parent(external_id="P1")
    child1 = Child(parent_ref="P1")
    child2 = Child(parent_ref="P_MISSING")

    parent_result = MappingResult(
        instances={},
        update_errors={},
        finalize_errors={},
        stats={},
        indices={"external_id": {"P1": parent1}},
    )

    children_list = [child1, child2]

    # Bind with required=False
    errors = bind_many_to_one_via_index(
        parent_result=parent_result,
        children=children_list,
        parent_index_field="external_id",
        child_lookup_field="parent_ref",
        relationship_attr="parent",
        required=False,
    )

    assert errors == []
    assert child1.parent is parent1
    assert child2.parent is None
