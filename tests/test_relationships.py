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
    bind_backlinks,
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


def test_bind_backlinks_success():
    """bind_backlinks should populate parent's list attribute with matching children."""
    from dataclasses import field

    @dataclass
    class Question:
        text: str
        choices: list = field(default_factory=list)

    @dataclass
    class Choice:
        text: str

    question1 = Question(text="Q1")
    question2 = Question(text="Q2")
    choice1 = Choice(text="A")
    choice2 = Choice(text="B")
    choice3 = Choice(text="C")

    # Parent results with questions keyed
    parent_result = MappingResult(
        instances={
            ("q1",): question1,
            ("q2",): question2,
        },
        update_errors={},
        finalize_errors={},
        stats={},
        indices={},
    )

    # Child results with choices indexed by ID
    child_result = MappingResult(
        instances={
            ("c1",): choice1,
            ("c2",): choice2,
            ("c3",): choice3,
        },
        update_errors={},
        finalize_errors={},
        stats={},
        indices={"id": {10: choice1, 11: choice2, 12: choice3}},
    )

    raw_results = {
        "questions": parent_result,
        "choices": child_result,
    }

    # Backlink spec: question's choice_ids contains choice's id
    backlinks = [
        {
            "type": "backlink",
            "parent_table": "questions",
            "child_table": "choices",
            "attr": "choices",
            "by": {"choice_ids": "id"},
        }
    ]

    # Parent lookup values: each question has a choice_ids list
    parent_lookup_values = {
        "questions": {
            ("q1",): {"choice_ids": [10, 11]},  # Q1 has choices A and B
            ("q2",): {"choice_ids": [12]},      # Q2 has choice C
        }
    }

    errors = bind_backlinks(
        raw_results, backlinks, parent_lookup_values, fail_on_missing=False
    )

    assert errors == []
    assert len(question1.choices) == 2
    assert choice1 in question1.choices
    assert choice2 in question1.choices
    assert len(question2.choices) == 1
    assert choice3 in question2.choices


def test_bind_backlinks_missing_children():
    """bind_backlinks should handle missing children gracefully."""
    from dataclasses import field

    @dataclass
    class Question:
        text: str
        choices: list = field(default_factory=list)

    @dataclass
    class Choice:
        text: str

    question = Question(text="Q1")
    choice = Choice(text="A")

    parent_result = MappingResult(
        instances={("q1",): question},
        update_errors={},
        finalize_errors={},
        stats={},
        indices={},
    )

    child_result = MappingResult(
        instances={("c1",): choice},
        update_errors={},
        finalize_errors={},
        stats={},
        indices={"id": {10: choice}},  # Only choice 10 exists
    )

    raw_results = {
        "questions": parent_result,
        "choices": child_result,
    }

    backlinks = [
        {
            "type": "backlink",
            "parent_table": "questions",
            "child_table": "choices",
            "attr": "choices",
            "by": {"choice_ids": "id"},
        }
    ]

    # Q1 references choice 10 (exists) and 999 (missing)
    parent_lookup_values = {
        "questions": {
            ("q1",): {"choice_ids": [10, 999]},
        }
    }

    # With fail_on_missing=False, errors are returned but no exception raised
    errors = bind_backlinks(
        raw_results, backlinks, parent_lookup_values, fail_on_missing=False
    )

    # The missing child (999) should still be noted but no exception
    # Since we don't fail on missing, the list should still be set with existing children
    assert len(question.choices) == 1
    assert choice in question.choices

    # Test with fail_on_missing=True to verify exception is raised
    question2 = Question(text="Q2")
    parent_result2 = MappingResult(
        instances={("q2",): question2},
        update_errors={},
        finalize_errors={},
        stats={},
        indices={},
    )
    raw_results2 = {
        "questions": parent_result2,
        "choices": child_result,
    }
    parent_lookup_values2 = {
        "questions": {
            ("q2",): {"choice_ids": [999]},  # Only missing child
        }
    }

    import pytest
    with pytest.raises(RuntimeError, match="backlink binding failed"):
        bind_backlinks(
            raw_results2, backlinks, parent_lookup_values2, fail_on_missing=True
        )


def test_bind_backlinks_skips_non_backlink_rels():
    """bind_backlinks should ignore relationships without type='backlink'."""
    from dataclasses import field

    @dataclass
    class Question:
        text: str
        choices: list = field(default_factory=list)

    question = Question(text="Q1")

    parent_result = MappingResult(
        instances={("q1",): question},
        update_errors={},
        finalize_errors={},
        stats={},
        indices={},
    )

    raw_results = {"questions": parent_result}

    # Mix of link_to and backlink relationships
    relationships = [
        {
            # link_to style - should be skipped
            "parent_table": "users",
            "child_table": "posts",
            "by": {"user_id": "id"},
        },
        {
            # backlink style - should be processed (but no matching data)
            "type": "backlink",
            "parent_table": "questions",
            "child_table": "choices",
            "attr": "choices",
            "by": {"choice_ids": "id"},
        },
    ]

    parent_lookup_values = {
        "questions": {
            ("q1",): {"choice_ids": []},
        }
    }

    # Should not error, just skip the non-backlink rel
    errors = bind_backlinks(
        raw_results, relationships, parent_lookup_values, fail_on_missing=False
    )

    assert errors == []
