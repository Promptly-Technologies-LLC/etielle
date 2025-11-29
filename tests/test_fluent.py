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


class TestNodeTransform:
    """Tests for node() transform."""

    def test_node_returns_current_node(self):
        """node() returns the current context node."""
        from etielle.fluent import node

        t = node()
        ctx = Context(root={}, node={"x": 1}, path=(), parent=None, key=None, index=None, slots={})
        assert t(ctx) == {"x": 1}

    def test_node_with_scalar(self):
        """node() works with scalar values."""
        from etielle.fluent import node

        t = node()
        ctx = Context(root={}, node=42, path=(), parent=None, key=None, index=None, slots={})
        assert t(ctx) == 42


class TestParentIndexTransform:
    """Tests for parent_index() transform."""

    def test_parent_index_depth_1(self):
        """parent_index() returns parent's list index."""
        from etielle.fluent import parent_index

        parent_ctx = Context(root={}, node=[1, 2], path=("items",), parent=None, key=None, index=0, slots={})
        child_ctx = Context(root={}, node=1, path=("items", 0), parent=parent_ctx, key=None, index=None, slots={})

        t = parent_index()
        assert t(child_ctx) == 0

    def test_parent_index_depth_2(self):
        """parent_index(depth=2) returns grandparent's index."""
        from etielle.fluent import parent_index

        grandparent = Context(root={}, node=[], path=("a",), parent=None, key=None, index=1, slots={})
        parent = Context(root={}, node=[], path=("a", 1), parent=grandparent, key=None, index=None, slots={})
        child = Context(root={}, node={}, path=("a", 1, "b"), parent=parent, key=None, index=None, slots={})

        t = parent_index(depth=2)
        assert t(child) == 1

    def test_parent_index_none_when_no_parent(self):
        """parent_index() returns None if no parent exists."""
        from etielle.fluent import parent_index

        ctx = Context(root={}, node={}, path=(), parent=None, key=None, index=None, slots={})
        t = parent_index()
        assert t(ctx) is None


class TestPipelineResult:
    """Tests for PipelineResult dataclass."""

    def test_tables_access_by_string(self):
        """Can access tables by string name."""
        from etielle.fluent import PipelineResult

        result = PipelineResult(
            tables={"users": {(1,): {"id": 1, "name": "Alice"}}},
            errors={}
        )
        assert result.tables["users"] == {(1,): {"id": 1, "name": "Alice"}}

    def test_tables_access_by_class(self):
        """Can access tables by model class."""
        from etielle.fluent import PipelineResult

        class User:
            __tablename__ = "users"

        result = PipelineResult(
            tables={"users": {(1,): {"id": 1}}},
            errors={},
            _table_class_map={"users": User}
        )
        assert result.tables[User] == {(1,): {"id": 1}}

    def test_errors_empty_by_default(self):
        """Errors dict is empty when no errors."""
        from etielle.fluent import PipelineResult

        result = PipelineResult(tables={}, errors={})
        assert result.errors == {}

    def test_errors_structure(self):
        """Errors are keyed by table then row key."""
        from etielle.fluent import PipelineResult

        result = PipelineResult(
            tables={},
            errors={"users": {(1,): ["Field 'email' is required"]}}
        )
        assert result.errors["users"][(1,)] == ["Field 'email' is required"]


class TestEtlEntryPoint:
    """Tests for etl() entry point."""

    def test_etl_returns_pipeline_builder(self):
        """etl() returns a PipelineBuilder."""
        from etielle.fluent import etl, PipelineBuilder

        builder = etl({"users": []})
        assert isinstance(builder, PipelineBuilder)

    def test_etl_accepts_single_root(self):
        """etl() accepts a single JSON root."""
        from etielle.fluent import etl

        builder = etl({"x": 1})
        assert builder._roots == ({"x": 1},)

    def test_etl_accepts_multiple_roots(self):
        """etl() accepts multiple JSON roots."""
        from etielle.fluent import etl

        builder = etl({"a": 1}, {"b": 2})
        assert builder._roots == ({"a": 1}, {"b": 2})

    def test_etl_default_error_mode(self):
        """etl() defaults to collect errors."""
        from etielle.fluent import etl

        builder = etl({})
        assert builder._error_mode == "collect"

    def test_etl_fail_fast_mode(self):
        """etl() can be configured for fail_fast."""
        from etielle.fluent import etl

        builder = etl({}, errors="fail_fast")
        assert builder._error_mode == "fail_fast"


class TestGotoRoot:
    """Tests for goto_root() navigation."""

    def test_goto_root_returns_self(self):
        """goto_root() returns the builder for chaining."""
        from etielle.fluent import etl

        builder = etl({}, {})
        result = builder.goto_root()
        assert result is builder

    def test_goto_root_defaults_to_zero(self):
        """goto_root() defaults to index 0."""
        from etielle.fluent import etl

        builder = etl({"a": 1}, {"b": 2})
        builder.goto_root()
        assert builder._current_root_index == 0

    def test_goto_root_with_index(self):
        """goto_root(n) selects the nth root."""
        from etielle.fluent import etl

        builder = etl({"a": 1}, {"b": 2})
        builder.goto_root(1)
        assert builder._current_root_index == 1

    def test_goto_root_resets_path(self):
        """goto_root() resets navigation path."""
        from etielle.fluent import etl

        builder = etl({"users": []})
        builder._current_path = ["users", "0", "posts"]
        builder._iteration_depth = 2
        builder.goto_root()
        assert builder._current_path == []
        assert builder._iteration_depth == 0

    def test_goto_root_invalid_index_raises(self):
        """goto_root() with invalid index raises."""
        from etielle.fluent import etl

        builder = etl({"a": 1})
        with pytest.raises(IndexError, match="Root index 5 out of range"):
            builder.goto_root(5)


class TestGoto:
    """Tests for goto() navigation."""

    def test_goto_returns_self(self):
        """goto() returns the builder for chaining."""
        from etielle.fluent import etl

        builder = etl({"users": []})
        result = builder.goto("users")
        assert result is builder

    def test_goto_string_path(self):
        """goto() with string adds to path."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto("users")
        assert builder._current_path == ["users"]

    def test_goto_chained(self):
        """Multiple goto() calls accumulate path."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto("data").goto("users")
        assert builder._current_path == ["data", "users"]

    def test_goto_list_path(self):
        """goto() with list adds all segments."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto(["data", "users"])
        assert builder._current_path == ["data", "users"]

    def test_goto_dot_notation(self):
        """goto() with dot notation splits path."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto("data.users.active")
        assert builder._current_path == ["data", "users", "active"]

    def test_goto_after_each_resets_iteration(self):
        """goto() after each() starts fresh inner path."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto("users")
        builder._iteration_depth = 1  # Simulating each() was called
        builder.goto("posts")
        # Path continues from current position
        assert builder._current_path == ["users", "posts"]


class TestEach:
    """Tests for each() iteration marker."""

    def test_each_returns_self(self):
        """each() returns the builder for chaining."""
        from etielle.fluent import etl

        builder = etl({"items": []})
        result = builder.goto("items").each()
        assert result is builder

    def test_each_increments_iteration_depth(self):
        """each() increments iteration depth."""
        from etielle.fluent import etl

        builder = etl({})
        assert builder._iteration_depth == 0
        builder.goto("items").each()
        assert builder._iteration_depth == 1

    def test_each_chained_for_nested_iteration(self):
        """Multiple each() calls for nested iteration."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto("rows").each().each()
        assert builder._iteration_depth == 2

    def test_each_records_iteration_point(self):
        """each() records the path where iteration occurs."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto("users").each()
        # Should record that iteration happens at ["users"]
        assert builder._iteration_points == [["users"]]

    def test_each_multiple_records_all_points(self):
        """Multiple each() records all iteration points."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto("users").each().goto("posts").each()
        assert builder._iteration_points == [["users"], ["users", "posts"]]


class TestMapTo:
    """Tests for map_to() emission."""

    def test_map_to_returns_self(self):
        """map_to() returns the builder for chaining."""
        from etielle.fluent import etl

        builder = etl({"users": []})
        result = builder.goto("users").each().map_to(
            table="users",
            fields=[Field("name", get("name"))]
        )
        assert result is builder

    def test_map_to_records_emission(self):
        """map_to() records the emission spec."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto("users").each().map_to(
            table="users",
            fields=[Field("name", get("name")), TempField("id", get("id"))]
        )
        assert len(builder._emissions) == 1
        emission = builder._emissions[0]
        assert emission["table"] == "users"
        assert len(emission["fields"]) == 2

    def test_map_to_with_model_class(self):
        """map_to() accepts model class as table."""
        from etielle.fluent import etl

        class User:
            __tablename__ = "users"

        builder = etl({})
        builder.goto("users").each().map_to(
            table=User,
            fields=[Field("name", get("name"))]
        )
        emission = builder._emissions[0]
        assert emission["table_class"] is User
        assert emission["table"] == "users"

    def test_map_to_captures_navigation_state(self):
        """map_to() captures current path and iteration state."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto("data").goto("users").each().map_to(
            table="users",
            fields=[Field("name", get("name"))]
        )
        emission = builder._emissions[0]
        assert emission["path"] == ["data", "users"]
        assert emission["iteration_depth"] == 1

    def test_map_to_with_join_on(self):
        """map_to() accepts join_on for row merging."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto("users").each().map_to(
            table="users",
            join_on=["id"],
            fields=[Field("email", get("email")), TempField("id", get("id"))]
        )
        emission = builder._emissions[0]
        assert emission["join_on"] == ["id"]

    def test_map_to_with_error_override(self):
        """map_to() can override error mode."""
        from etielle.fluent import etl

        builder = etl({}, errors="collect")
        builder.goto("users").each().map_to(
            table="users",
            fields=[Field("name", get("name"))],
            errors="fail_fast"
        )
        emission = builder._emissions[0]
        assert emission["errors"] == "fail_fast"


class TestLinkTo:
    """Tests for link_to() relationship definition."""

    def test_link_to_returns_self(self):
        """link_to() returns the builder for chaining."""
        from etielle.fluent import etl

        class User:
            __tablename__ = "users"

        builder = etl({})
        builder.goto("posts").each().map_to(
            table="posts",
            fields=[Field("title", get("title")), TempField("user_id", get("user_id"))]
        )
        result = builder.link_to(User, by={"user_id": "id"})
        assert result is builder

    def test_link_to_records_relationship(self):
        """link_to() records the relationship spec."""
        from etielle.fluent import etl

        class User:
            __tablename__ = "users"

        builder = etl({})
        builder.goto("posts").each().map_to(
            table="posts",
            fields=[TempField("user_id", get("user_id"))]
        )
        builder.link_to(User, by={"user_id": "id"})

        assert len(builder._relationships) == 1
        rel = builder._relationships[0]
        assert rel["parent_class"] is User
        assert rel["by"] == {"user_id": "id"}

    def test_link_to_multiple_parents(self):
        """Multiple link_to() calls for multiple parents."""
        from etielle.fluent import etl

        class User:
            __tablename__ = "users"
        class Post:
            __tablename__ = "posts"

        builder = etl({})
        builder.goto("comments").each().map_to(
            table="comments",
            fields=[TempField("user_id", get("uid")), TempField("post_id", get("pid"))]
        )
        builder.link_to(User, by={"user_id": "id"})
        builder.link_to(Post, by={"post_id": "id"})

        assert len(builder._relationships) == 2

    def test_link_to_associates_with_last_emission(self):
        """link_to() associates with the most recent map_to()."""
        from etielle.fluent import etl

        class User:
            __tablename__ = "users"

        builder = etl({})
        builder.goto("users").each().map_to(table="users", fields=[])
        builder.goto("posts").each().map_to(table="posts", fields=[TempField("user_id", get("uid"))])
        builder.link_to(User, by={"user_id": "id"})

        rel = builder._relationships[0]
        assert rel["child_table"] == "posts"

    def test_link_to_without_map_to_raises(self):
        """link_to() without preceding map_to() raises."""
        from etielle.fluent import etl

        class User:
            __tablename__ = "users"

        builder = etl({})
        with pytest.raises(ValueError, match="link_to.*map_to"):
            builder.link_to(User, by={"user_id": "id"})


class TestLoad:
    """Tests for load() session configuration."""

    def test_load_returns_self(self):
        """load() returns the builder for chaining."""
        from etielle.fluent import etl

        builder = etl({})
        mock_session = object()
        result = builder.load(mock_session)
        assert result is builder

    def test_load_stores_session(self):
        """load() stores the session reference."""
        from etielle.fluent import etl

        builder = etl({})
        mock_session = object()
        builder.load(mock_session)
        assert builder._session is mock_session

    def test_load_can_be_chained_before_run(self):
        """load() is typically chained before run()."""
        from etielle.fluent import etl

        builder = etl({})
        mock_session = object()
        # Should not raise
        builder.goto("users").each().map_to(
            table="users",
            fields=[Field("name", get("name"))]
        ).load(mock_session)
        assert builder._session is mock_session


class TestRunBasic:
    """Tests for run() basic execution without database."""

    def test_run_returns_pipeline_result(self):
        """run() returns a PipelineResult."""
        from etielle.fluent import etl, PipelineResult

        data = {"users": [{"name": "Alice"}]}
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[Field("name", get("name"))])
            .run()
        )
        assert isinstance(result, PipelineResult)

    def test_run_extracts_simple_list(self):
        """run() extracts data from a simple list."""
        from etielle.fluent import etl

        data = {"users": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]}
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .run()
        )
        # Access by string name
        users = result.tables["users"]
        assert len(users) == 2
        # Keyed by TempField values (join key)
        assert (1,) in users or (2,) in users

    def test_run_tempfield_not_in_output(self):
        """TempField values are not in the output dict."""
        from etielle.fluent import etl

        data = {"users": [{"user_id": 1, "name": "Alice"}]}
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[
                Field("name", get("name")),
                TempField("user_id", get("user_id"))
            ])
            .run()
        )
        users = result.tables["users"]
        row = list(users.values())[0]
        assert "name" in row
        assert "user_id" not in row  # TempField excluded
        # Note: "id" may be auto-injected by executor for single-key tables

    def test_run_with_nested_iteration(self):
        """run() handles nested iteration."""
        from etielle.fluent import etl
        from etielle.transforms import get_from_parent, index

        data = {"users": [
            {"id": 1, "posts": [{"title": "Post A"}, {"title": "Post B"}]}
        ]}
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[TempField("id", get("id"))])
            .goto("posts").each()
            .map_to(table="posts", fields=[
                Field("title", get("title")),
                TempField("user_id", get_from_parent("id")),
                TempField("post_index", index())
            ])
            .run()
        )
        posts = result.tables["posts"]
        assert len(posts) == 2
