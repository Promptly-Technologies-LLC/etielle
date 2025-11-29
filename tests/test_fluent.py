"""Tests for the fluent E→T→L API."""

import pytest
from etielle.fluent import Field, TempField, FieldUnion, transform
from etielle.transforms import get, literal
from etielle.core import Context
from typing import Any


class TestField:
    """Tests for Field dataclass."""

    def test_field_creation_with_transform(self):
        """Field stores name and transform."""
        field = Field("username", get("name"))
        assert field.name == "username"
        assert field.transform is not None

    def test_field_creation_with_literal(self):
        """Field works with literal values."""
        field = Field("count", literal(1))
        assert field.name == "count"

    def test_field_with_merge_policy(self):
        """Field accepts optional merge policy."""
        from etielle.instances import AddPolicy
        field = Field("total", literal(1), merge=AddPolicy())
        assert field.name == "total"
        assert isinstance(field.merge, AddPolicy)

    def test_field_is_frozen(self):
        """Field is immutable."""
        field = Field("name", get("name"))
        with pytest.raises(AttributeError):
            field.name = "other"


class TestTempField:
    """Tests for TempField dataclass."""

    def test_tempfield_creation(self):
        """TempField stores name and transform."""
        field = TempField("id", get("id"))
        assert field.name == "id"
        assert field.transform is not None

    def test_tempfield_is_frozen(self):
        """TempField is immutable."""
        field = TempField("id", get("id"))
        with pytest.raises(AttributeError):
            field.name = "other"

    def test_tempfield_distinct_from_field(self):
        """TempField is a different type from Field."""
        field = Field("name", get("name"))
        temp = TempField("id", get("id"))
        assert type(field) is not type(temp)


class TestFieldUnion:
    """Tests for FieldUnion type alias."""

    def test_field_is_fieldunion(self):
        """Field is a valid FieldUnion."""
        field: FieldUnion = Field("name", get("name"))
        assert isinstance(field, (Field, TempField))

    def test_tempfield_is_fieldunion(self):
        """TempField is a valid FieldUnion."""
        field: FieldUnion = TempField("id", get("id"))
        assert isinstance(field, (Field, TempField))


class TestTransformDecorator:
    """Tests for @transform decorator."""

    def test_transform_with_no_extra_args(self):
        """Transform with only ctx works as identity wrapper."""
        @transform
        def node_value(ctx: Context) -> Any:
            return ctx.node

        # Calling without args returns a Transform
        t = node_value()
        # The transform should work with a context
        ctx = Context(root={"x": 1}, node=42, path=(), parent=None, key=None, index=None, slots={})
        assert t(ctx) == 42

    def test_transform_with_extra_args(self):
        """Transform with extra args creates curried factory."""
        @transform
        def get_field(ctx: Context, field: str) -> Any:
            return ctx.node[field]

        # Calling with field arg returns a Transform
        t = get_field("name")
        ctx = Context(root={}, node={"name": "Alice"}, path=(), parent=None, key=None, index=None, slots={})
        assert t(ctx) == "Alice"

    def test_transform_with_multiple_args(self):
        """Transform with multiple extra args."""
        @transform
        def split_field(ctx: Context, field: str, index: int) -> str:
            return ctx.node[field].split("_")[index]

        t = split_field("composite_id", 0)
        ctx = Context(root={}, node={"composite_id": "user_123"}, path=(), parent=None, key=None, index=None, slots={})
        assert t(ctx) == "user"
