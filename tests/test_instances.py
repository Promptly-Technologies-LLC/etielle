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
    MinPolicy,
    MaxPolicy,
    FirstNonNullPolicy,
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
                    InstanceEmit[dict](
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
                    InstanceEmit[dict](
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
    got = sorted(
        list(result["user_counts"].instances.values()), key=lambda r: r["user_id"]
    )
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
                    InstanceEmit[dict](
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
                            FieldSpec(
                                selector=(lambda u: u.email), transform=get("email")
                            ),
                        ],
                        builder=PydanticBuilder(User),
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    users = sorted(
        list(result["users_pydantic"].instances.values()), key=lambda u: u.id
    )
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
        ("field required" in m)
        or ("Missing" in m)
        or ("Input should" in m)
        or ("validation error" in m)
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


# -----------------------------
# MinPolicy Tests
# -----------------------------


def test_min_policy_basic_numeric():
    """Test MinPolicy keeps minimum value across multiple updates."""
    data = {
        "events": [
            {"id": "e1", "user_id": "u1", "score": 10},
            {"id": "e2", "user_id": "u1", "score": 5},
            {"id": "e3", "user_id": "u1", "score": 15},
            {"id": "e4", "user_id": "u2", "score": 20},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_min_scores",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="min_score", transform=get("score")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"min_score": MinPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = sorted(
        list(result["user_min_scores"].instances.values()), key=lambda r: r["user_id"]
    )
    assert got == [
        {"user_id": "u1", "min_score": 5},
        {"user_id": "u2", "min_score": 20},
    ]


def test_min_policy_with_none_values():
    """Test MinPolicy handles None values correctly - None should be ignored."""
    data = {
        "events": [
            {"user_id": "u1", "score": 10},
            {"user_id": "u1", "score": None},
            {"user_id": "u1", "score": 8},
            {"user_id": "u2", "score": None},
            {"user_id": "u2", "score": 15},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_min_scores",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="min_score", transform=get("score")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"min_score": MinPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = sorted(
        list(result["user_min_scores"].instances.values()), key=lambda r: r["user_id"]
    )
    assert got == [
        {"user_id": "u1", "min_score": 8},
        {"user_id": "u2", "min_score": 15},
    ]


def test_min_policy_all_none():
    """Test MinPolicy when all values are None - should keep None."""
    data = {
        "events": [
            {"user_id": "u1", "score": None},
            {"user_id": "u1", "score": None},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_min_scores",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="min_score", transform=get("score")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"min_score": MinPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_min_scores"].instances.values())[0]
    assert got == {"user_id": "u1", "min_score": None}


def test_min_policy_single_value():
    """Test MinPolicy with a single value - no merge needed."""
    data = {
        "events": [
            {"user_id": "u1", "score": 42},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_min_scores",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="min_score", transform=get("score")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"min_score": MinPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_min_scores"].instances.values())[0]
    assert got == {"user_id": "u1", "min_score": 42}


def test_min_policy_with_floats():
    """Test MinPolicy works with floating-point numbers."""
    data = {
        "events": [
            {"user_id": "u1", "temperature": 98.6},
            {"user_id": "u1", "temperature": 97.2},
            {"user_id": "u1", "temperature": 99.1},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_temps",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(
                                selector="min_temp", transform=get("temperature")
                            ),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"min_temp": MinPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_temps"].instances.values())[0]
    assert got == {"user_id": "u1", "min_temp": 97.2}


def test_min_policy_with_dates():
    """Test MinPolicy works with date/datetime objects."""
    from datetime import date

    data = {
        "events": [
            {"user_id": "u1", "event_date": date(2024, 1, 15)},
            {"user_id": "u1", "event_date": date(2024, 1, 10)},
            {"user_id": "u1", "event_date": date(2024, 1, 20)},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_dates",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(
                                selector="first_date", transform=get("event_date")
                            ),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"first_date": MinPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_dates"].instances.values())[0]
    assert got == {"user_id": "u1", "first_date": date(2024, 1, 10)}


def test_min_policy_with_strings():
    """Test MinPolicy works with strings (lexicographic ordering)."""
    data = {
        "events": [
            {"user_id": "u1", "code": "delta"},
            {"user_id": "u1", "code": "alpha"},
            {"user_id": "u1", "code": "charlie"},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_codes",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="min_code", transform=get("code")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"min_code": MinPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_codes"].instances.values())[0]
    assert got == {"user_id": "u1", "min_code": "alpha"}


# -----------------------------
# MaxPolicy Tests
# -----------------------------


def test_max_policy_basic_numeric():
    """Test MaxPolicy keeps maximum value across multiple updates."""
    data = {
        "events": [
            {"id": "e1", "user_id": "u1", "score": 10},
            {"id": "e2", "user_id": "u1", "score": 25},
            {"id": "e3", "user_id": "u1", "score": 15},
            {"id": "e4", "user_id": "u2", "score": 20},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_max_scores",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="max_score", transform=get("score")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"max_score": MaxPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = sorted(
        list(result["user_max_scores"].instances.values()), key=lambda r: r["user_id"]
    )
    assert got == [
        {"user_id": "u1", "max_score": 25},
        {"user_id": "u2", "max_score": 20},
    ]


def test_max_policy_with_none_values():
    """Test MaxPolicy handles None values correctly - None should be ignored."""
    data = {
        "events": [
            {"user_id": "u1", "score": 10},
            {"user_id": "u1", "score": None},
            {"user_id": "u1", "score": 18},
            {"user_id": "u2", "score": None},
            {"user_id": "u2", "score": 15},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_max_scores",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="max_score", transform=get("score")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"max_score": MaxPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = sorted(
        list(result["user_max_scores"].instances.values()), key=lambda r: r["user_id"]
    )
    assert got == [
        {"user_id": "u1", "max_score": 18},
        {"user_id": "u2", "max_score": 15},
    ]


def test_max_policy_all_none():
    """Test MaxPolicy when all values are None - should keep None."""
    data = {
        "events": [
            {"user_id": "u1", "score": None},
            {"user_id": "u1", "score": None},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_max_scores",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="max_score", transform=get("score")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"max_score": MaxPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_max_scores"].instances.values())[0]
    assert got == {"user_id": "u1", "max_score": None}


def test_max_policy_single_value():
    """Test MaxPolicy with a single value - no merge needed."""
    data = {
        "events": [
            {"user_id": "u1", "score": 42},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_max_scores",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="max_score", transform=get("score")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"max_score": MaxPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_max_scores"].instances.values())[0]
    assert got == {"user_id": "u1", "max_score": 42}


def test_max_policy_with_floats():
    """Test MaxPolicy works with floating-point numbers."""
    data = {
        "events": [
            {"user_id": "u1", "temperature": 98.6},
            {"user_id": "u1", "temperature": 100.2},
            {"user_id": "u1", "temperature": 99.1},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_temps",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(
                                selector="max_temp", transform=get("temperature")
                            ),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"max_temp": MaxPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_temps"].instances.values())[0]
    assert got == {"user_id": "u1", "max_temp": 100.2}


def test_max_policy_with_dates():
    """Test MaxPolicy works with date/datetime objects."""
    from datetime import date

    data = {
        "events": [
            {"user_id": "u1", "event_date": date(2024, 1, 15)},
            {"user_id": "u1", "event_date": date(2024, 1, 25)},
            {"user_id": "u1", "event_date": date(2024, 1, 20)},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_dates",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(
                                selector="last_date", transform=get("event_date")
                            ),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"last_date": MaxPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_dates"].instances.values())[0]
    assert got == {"user_id": "u1", "last_date": date(2024, 1, 25)}


def test_max_policy_with_strings():
    """Test MaxPolicy works with strings (lexicographic ordering)."""
    data = {
        "events": [
            {"user_id": "u1", "code": "delta"},
            {"user_id": "u1", "code": "alpha"},
            {"user_id": "u1", "code": "zulu"},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_codes",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(selector="max_code", transform=get("code")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"max_code": MaxPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_codes"].instances.values())[0]
    assert got == {"user_id": "u1", "max_code": "zulu"}


# -----------------------------
# FirstNonNullPolicy Tests
# -----------------------------


def test_first_non_null_policy_basic():
    """Test FirstNonNullPolicy keeps first non-null value across multiple updates."""
    data = {
        "events": [
            {"user_id": "u1", "email": "first@example.com"},
            {"user_id": "u1", "email": "second@example.com"},
            {"user_id": "u1", "email": "third@example.com"},
            {"user_id": "u2", "email": "other@example.com"},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_emails",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(
                                selector="first_email", transform=get("email")
                            ),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"first_email": FirstNonNullPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = sorted(
        list(result["user_emails"].instances.values()), key=lambda r: r["user_id"]
    )
    assert got == [
        {"user_id": "u1", "first_email": "first@example.com"},
        {"user_id": "u2", "first_email": "other@example.com"},
    ]


def test_first_non_null_policy_with_none_values():
    """Test FirstNonNullPolicy skips None values and takes first non-null."""
    data = {
        "events": [
            {"user_id": "u1", "email": None},
            {"user_id": "u1", "email": None},
            {"user_id": "u1", "email": "valid@example.com"},
            {"user_id": "u1", "email": "another@example.com"},
            {"user_id": "u2", "email": None},
            {"user_id": "u2", "email": "other@example.com"},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_emails",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(
                                selector="first_email", transform=get("email")
                            ),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"first_email": FirstNonNullPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = sorted(
        list(result["user_emails"].instances.values()), key=lambda r: r["user_id"]
    )
    assert got == [
        {"user_id": "u1", "first_email": "valid@example.com"},
        {"user_id": "u2", "first_email": "other@example.com"},
    ]


def test_first_non_null_policy_all_none():
    """Test FirstNonNullPolicy when all values are None - should keep None."""
    data = {
        "events": [
            {"user_id": "u1", "email": None},
            {"user_id": "u1", "email": None},
            {"user_id": "u1", "email": None},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_emails",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(
                                selector="first_email", transform=get("email")
                            ),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"first_email": FirstNonNullPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_emails"].instances.values())[0]
    assert got == {"user_id": "u1", "first_email": None}


def test_first_non_null_policy_single_value():
    """Test FirstNonNullPolicy with a single value - no merge needed."""
    data = {
        "events": [
            {"user_id": "u1", "email": "only@example.com"},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_emails",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(
                                selector="first_email", transform=get("email")
                            ),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"first_email": FirstNonNullPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = list(result["user_emails"].instances.values())[0]
    assert got == {"user_id": "u1", "first_email": "only@example.com"}


def test_first_non_null_policy_with_various_types():
    """Test FirstNonNullPolicy works with different data types."""
    data = {
        "events": [
            {"user_id": "u1", "value": None},
            {"user_id": "u1", "value": 42},
            {"user_id": "u1", "value": 100},
            {"user_id": "u2", "value": "text"},
            {"user_id": "u2", "value": "other"},
            {"user_id": "u3", "value": None},
            {"user_id": "u3", "value": False},  # False is not None
            {"user_id": "u3", "value": True},
        ]
    }

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["events"],
                mode="auto",
                emits=[
                    InstanceEmit[dict](
                        table="user_values",
                        join_keys=[get("user_id")],
                        fields=[
                            FieldSpec(selector="user_id", transform=get("user_id")),
                            FieldSpec(
                                selector="first_value", transform=get("value")
                            ),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                        policies={"first_value": FirstNonNullPolicy()},
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    got = sorted(
        list(result["user_values"].instances.values()), key=lambda r: r["user_id"]
    )
    assert got == [
        {"user_id": "u1", "first_value": 42},
        {"user_id": "u2", "first_value": "text"},
        {"user_id": "u3", "first_value": False},  # False is valid non-null value
    ]
