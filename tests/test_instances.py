import pydantic
from etielle.core import MappingSpec, TraversalSpec
from etielle.transforms import get
from etielle.instances import (
    InstanceEmit,
    FieldSpec,
    TypedDictBuilder,
    AddPolicy,
    AppendPolicy,
    ExtendPolicy,
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
                mode="auto",
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
    got = sorted(list(result["user_models"].instances.values()), key=lambda r: r["id"])  
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
                mode="auto",
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
    got = sorted(list(result["user_counts"].instances.values()), key=lambda r: r["user_id"])  
    assert got == [
        {"user_id": "u1", "count": 2},
        {"user_id": "u2", "count": 1},
    ]


def test_append_and_extend_policies_ordering():
    data = {
        "events": [
            {"user_id": "u1", "tag": "a", "tags": ["x"]},
            {"user_id": "u1", "tag": "b", "tags": ["y", "z"]},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[
                        dict
                    ](
                        table="user_tags",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="tag", transform=get("tag")),
                            FieldSpec(selector="tags_accum", transform=get("tags")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={
                            "tag": AppendPolicy(),
                            "tags_accum": ExtendPolicy(),
                        },
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_tags"].instances.values())[0]
    assert got["tag"] == ["a", "b"]
    assert got["tags_accum"] == ["x", "y", "z"]


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
                mode="auto",
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
    users = sorted(list(result["users_pydantic"].instances.values()), key=lambda u: u.id)  
    assert users[0].id == "u1" and users[0].email == "ada@example.com"
    assert users[1].id == "u2" and users[1].email == "linus@example.com"


def test_unknown_field_suggestion_and_error_collection():
    class User(pydantic.BaseModel):
        id: str
        email: str

    data = {
        "users": [
            {"id": "u1", "email": "ada@example.com"},
            {"id": "u2", "email": "linus@example.com"},
        ]
    }

    from etielle.executor import run_mapping
    from etielle.transforms import get
    from etielle.instances import InstanceEmit, FieldSpec, PydanticBuilder

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["users"],
                mode="auto",
                emits=[
                    InstanceEmit[User](
                        table="users_pydantic_bad_field",
                        join_keys=[get("id")],
                        fields=[
                            FieldSpec(selector="id", transform=get("id")),
                            # misspelled selector name
                            FieldSpec(selector="emali", transform=get("email")),
                        ],
                        builder=PydanticBuilder(User),
                        # default strict_mode = collect_all
                    )
                ],
            )
        ]
    )

    res = run_mapping(data, spec)["users_pydantic_bad_field"]
    # Update errors should contain suggestion for 'email'
    all_update_msgs = [m for msgs in res.update_errors.values() for m in msgs]
    assert any("did you mean email" in m for m in all_update_msgs)
    # Finalize errors should report missing required field 'email'
    all_finalize_msgs = [m for msgs in res.finalize_errors.values() for m in msgs]
    # Accept common pydantic error shapes
    assert any(
        ("field required" in m) or ("Missing" in m) or ("Input should" in m) or ("validation error" in m)
        for m in all_finalize_msgs
    )


def test_fail_fast_on_unknown_field():
    class User(pydantic.BaseModel):
        id: str
        email: str

    data = {"users": [{"id": "u1", "email": "ada@example.com"}]}

    from etielle.executor import run_mapping
    from etielle.transforms import get
    from etielle.instances import InstanceEmit, FieldSpec, PydanticBuilder

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["users"],
                mode="auto",
                emits=[
                    InstanceEmit[User](
                        table="users_pydantic_fail_fast",
                        join_keys=[get("id")],
                        fields=[
                            FieldSpec(selector="id", transform=get("id")),
                            FieldSpec(selector="emali", transform=get("email")),
                        ],
                        builder=PydanticBuilder(User),
                        strict_mode="fail_fast",
                    )
                ],
            )
        ]
    )

    import pytest

    with pytest.raises(RuntimeError):
        run_mapping(data, spec)
