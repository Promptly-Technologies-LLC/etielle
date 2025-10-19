from dataclasses import dataclass
from typing import Any, Dict

import pytest

from etielle.core import MappingSpec, TraversalSpec
from etielle.transforms import get
from etielle.instances import InstanceEmit, FieldSpec, TypedDictBuilder
from etielle.executor import run_mapping
from etielle.relationships import (
    ManyToOneSpec,
    compute_relationship_keys,
    bind_many_to_one,
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


def _user_factory(payload: Dict[str, Any]) -> User:
    return User(id=str(payload["id"]), name=str(payload.get("name", "")))


def _post_factory(payload: Dict[str, Any]) -> Post:
    return Post(id=str(payload["id"]), title=str(payload.get("title", "")))


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

    users_emit = InstanceEmit[
        User
    ](
        table="users",
        join_keys=[get("id")],
        fields=[
            FieldSpec(selector="id", transform=get("id")),
            FieldSpec(selector="name", transform=get("name")),
        ],
        builder=TypedDictBuilder(_user_factory),
    )

    posts_emit = InstanceEmit[
        Post
    ](
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

    users_emit = InstanceEmit[
        User
    ](
        table="users",
        join_keys=[get("id")],
        fields=[
            FieldSpec(selector="id", transform=get("id")),
            FieldSpec(selector="name", transform=get("name")),
        ],
        builder=TypedDictBuilder(_user_factory),
    )

    posts_emit = InstanceEmit[
        Post
    ](
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


