import pytest
import warnings

from etielle.core import field_of, fields, FieldRef, Field, MappingSpec, TableEmit, TraversalSpec
from etielle.transforms import get


class UserModel:
    id: str
    email: str
    name: str

    # Deliberately define a method to ensure method calls are rejected
    def domain(self) -> str:  # pragma: no cover - test helper only
        return self.email.split("@")[-1]


def test_field_of_happy_path_single_attribute():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        assert field_of(UserModel, lambda u: u.email) == "email"
        assert field_of(UserModel, lambda u: u.id) == "id"


def test_field_of_emits_deprecation_warning():
    with pytest.warns(DeprecationWarning, match="field_of.*deprecated"):
        field_of(UserModel, lambda u: u.email)


@pytest.mark.parametrize(
    "selector, expected_message",
    [
        (
            lambda u: u.email.split("@")[0],
            "Invalid field selector: method call on attribute selector",
        ),
        (
            lambda u: u.domain(),
            "Invalid field selector: method call on attribute selector",
        ),
        (
            lambda u: u.email.lower(),
            "Invalid field selector: method call on attribute selector",
        ),
        (lambda u: (u.email), None),  # valid – just parentheses
    ],
)
def test_field_of_invalid_patterns(selector, expected_message):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        if expected_message is None:
            # Should succeed
            assert field_of(UserModel, selector) == "email"
            return
        with pytest.raises(ValueError) as err:
            field_of(UserModel, selector)
        assert expected_message in str(err.value)


def test_field_of_rejects_chained_attributes():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        with pytest.raises(ValueError) as err:
            field_of(
                UserModel, lambda u: u.name.title
            )  # chained attribute (attribute of attribute)
        assert "must access exactly one attribute" in str(err.value)


def test_field_of_rejects_indexing():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        with pytest.raises(ValueError) as err:
            field_of(UserModel, lambda u: u.email[0])
        assert "indexing on attribute selector" in str(err.value)


# -----------------------------
# fields() proxy tests
# -----------------------------


def test_fields_returns_field_ref():
    result = fields(UserModel).email
    assert isinstance(result, FieldRef)
    assert result.name == "email"


def test_fields_works_for_all_annotated_fields():
    assert fields(UserModel).id.name == "id"
    assert fields(UserModel).email.name == "email"
    assert fields(UserModel).name.name == "name"


def test_fields_rejects_nonexistent_field():
    with pytest.raises(AttributeError) as err:
        fields(UserModel).nonexistent
    assert "has no field 'nonexistent'" in str(err.value)


def test_fields_rejects_private_attributes():
    with pytest.raises(AttributeError):
        fields(UserModel)._private


def test_field_ref_is_frozen():
    ref = FieldRef("test")
    with pytest.raises(Exception):  # FrozenInstanceError
        ref.name = "other"


def test_integration_with_field_and_executor():
    """Integration test using the new fields() proxy."""
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
                    TableEmit(
                        table="users",
                        join_keys=[get("id")],
                        fields=[
                            Field(fields(UserModel).id.name, get("id")),
                            Field(fields(UserModel).email.name, get("email")),
                            Field(fields(UserModel).name.name, get("name")),
                        ],
                    )
                ],
            )
        ]
    )

    # Lazy import to avoid cycle in type checkers; the runtime import is fine
    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    rows = sorted(result["users"].instances.values(), key=lambda r: r["id"])
    assert rows == [
        {"id": "u1", "email": "ada@example.com", "name": "Ada"},
        {"id": "u2", "email": "linus@example.com", "name": "Linus"},
    ]


def test_integration_fields_with_instance_emit():
    """Integration test using fields() with InstanceEmit and FieldSpec."""
    from dataclasses import dataclass
    from etielle import InstanceEmit, FieldSpec, TypedDictBuilder

    @dataclass
    class User:
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
                    InstanceEmit(
                        table="users",
                        join_keys=[get("id")],
                        fields=[
                            FieldSpec(selector=fields(User).id, transform=get("id")),
                            FieldSpec(selector=fields(User).email, transform=get("email")),
                        ],
                        builder=TypedDictBuilder(lambda d: d),
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    rows = sorted(result["users"].instances.values(), key=lambda r: r["id"])
    assert rows == [
        {"id": "u1", "email": "ada@example.com"},
        {"id": "u2", "email": "linus@example.com"},
    ]
