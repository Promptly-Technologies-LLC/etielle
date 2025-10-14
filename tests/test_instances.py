import pydantic
from etielle.core import MappingSpec, TraversalSpec
from etielle.transforms import get
from etielle.instances import (
    InstanceEmit,
    FieldSpec,
    TypedDictBuilder,
    AddPolicy,
    PydanticBuilder,
)


def test_typed_dict_builder_basic():
    data = {
        "users": [
            {"id": "u1", "email": "ada@example.com", "name": "Ada"},
            {"id": "u2", "email": "linus@example.com", "name": "Linus"},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["users"],
                iterate_items=False,
                emits=[
                    InstanceEmit[
                        dict
                    ](
                        table="user_models",
                        join_keys=[get("id")],
                        fields=[
                            FieldSpec(selector="id", transform=get("id")),
                            FieldSpec(selector="email", transform=get("email")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = sorted(result["user_models"], key=lambda r: r["id"])  
    assert got == [
        {"id": "u1", "email": "ada@example.com"},
        {"id": "u2", "email": "linus@example.com"},
    ]


def test_merge_policy_add_across_multiple_updates():
    data = {
        "events": [
            {"user_id": "u1"},
            {"user_id": "u1"},
            {"user_id": "u2"},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                iterate_items=False,
                emits=[
                    InstanceEmit[
                        dict
                    ](
                        table="user_counts",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="count", transform=lambda ctx: 1),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"count": AddPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = sorted(result["user_counts"], key=lambda r: r["user_id"])  
    assert got == [
        {"user_id": "u1", "count": 2},
        {"user_id": "u2", "count": 1},
    ]


def test_pydantic_builder_with_typed_selectors():
    class User(pydantic.BaseModel):
        id: str
        email: str

    data = {
        "users": [
            {"id": "u1", "email": "ada@example.com"},
            {"id": "u2", "email": "linus@example.com"},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["users"],
                iterate_items=False,
                emits=[
                    InstanceEmit[User](
                        table="users_pydantic",
                        join_keys=[get("id")],
                        fields=[
                            FieldSpec(selector=(lambda u: u.id), transform=get("id")),
                            FieldSpec(selector=(lambda u: u.email), transform=get("email")),
                        ],
                        builder=PydanticBuilder(User),
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    users = sorted(result["users_pydantic"], key=lambda u: u.id)  
    assert users[0].id == "u1" and users[0].email == "ada@example.com"
    assert users[1].id == "u2" and users[1].email == "linus@example.com"
