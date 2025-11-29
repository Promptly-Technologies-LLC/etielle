"""Tests for the fluent E→T→L API."""

import pytest
from etielle.fluent import Field
from etielle.transforms import get, literal


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
