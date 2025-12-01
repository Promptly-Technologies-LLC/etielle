import pytest

from etielle.core import field_of, Field, MappingSpec, TableEmit, TraversalSpec
from etielle.transforms import get


class UserModel:
    id: str
    email: str
    name: str

    # Deliberately define a method to ensure method calls are rejected
    def domain(self) -> str:  # pragma: no cover - test helper only
        return self.email.split("@")[-1]


def test_field_of_happy_path_single_attribute():
    assert field_of(UserModel, lambda u: u.email) == "email"
    assert field_of(UserModel, lambda u: u.id) == "id"


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
    if expected_message is None:
        # Should succeed
        assert field_of(UserModel, selector) == "email"
        return
    with pytest.raises(ValueError) as err:
        field_of(UserModel, selector)
    assert expected_message in str(err.value)


def test_field_of_rejects_chained_attributes():
    with pytest.raises(ValueError) as err:
        field_of(
            UserModel, lambda u: u.name.title
        )  # chained attribute (attribute of attribute)
    assert "must access exactly one attribute" in str(err.value)


def test_field_of_rejects_indexing():
    with pytest.raises(ValueError) as err:
        field_of(UserModel, lambda u: u.email[0])
    assert "indexing on attribute selector" in str(err.value)


def test_integration_with_field_and_executor():
    """Integration test using field_of."""
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
                            Field(field_of(UserModel, lambda u: u.id), get("id")),
                            Field(field_of(UserModel, lambda u: u.email), get("email")),
                            Field(field_of(UserModel, lambda u: u.name), get("name")),
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


def test_integration_field_of_with_instance_emit():
    """Integration test using field_of with InstanceEmit and FieldSpec."""
    from etielle import InstanceEmit, FieldSpec, PydanticBuilder

    # Try to import Pydantic for this test
    try:
        from pydantic import BaseModel
    except ImportError:
        import pytest

        pytest.skip("Pydantic not installed")

    class User(BaseModel):
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
                            FieldSpec(selector=lambda u: u.id, transform=get("id")),
                            FieldSpec(selector=lambda u: u.email, transform=get("email")),
                        ],
                        builder=PydanticBuilder(User),
                    )
                ],
            )
        ]
    )

    from etielle.executor import run_mapping

    result = run_mapping(data, spec)
    rows = sorted(result["users"].instances.values(), key=lambda r: r.id)
    assert [{"id": r.id, "email": r.email} for r in rows] == [
        {"id": "u1", "email": "ada@example.com"},
        {"id": "u2", "email": "linus@example.com"},
    ]
