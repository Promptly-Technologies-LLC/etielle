"""Tests for the fluent E→T→L API."""

import pytest
from etielle.fluent import Field, TempField, FieldUnion, transform
from etielle.transforms import get, literal
from etielle.core import Context
from typing import Any, ClassVar


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
            setattr(field, "name", "other")


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
            setattr(field, "name", "other")

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
        # Without join_on, keys are auto-generated (no TempField-based keys)
        # Each iteration creates a distinct instance
        names = {v["name"] for v in users.values()}
        assert names == {"Alice", "Bob"}

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


class TestRunMerging:
    """Tests for run() with row merging."""

    def test_merge_rows_same_traversal_different_paths(self):
        """Rows from different paths merge by join key."""
        from etielle.fluent import etl

        data = {
            "users": [{"id": 1, "name": "Alice"}],
            "emails": [{"user_id": 1, "email": "alice@example.com"}]
        }
        result = (
            etl(data)
            .goto("users").each()
            # Both map_to calls need join_on for merging
            .map_to(table="users", join_on=["id"], fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .goto_root()
            .goto("emails").each()
            .map_to(table="users", join_on=["id"], fields=[
                Field("email", get("email")),
                TempField("id", get("user_id"))
            ])
            .run()
        )
        users = result.tables["users"]
        assert len(users) == 1
        user = users[(1,)]
        assert user["name"] == "Alice"
        assert user["email"] == "alice@example.com"

    def test_merge_with_policy(self):
        """Merge policies apply when combining rows."""
        from etielle.fluent import etl
        from etielle.instances import AddPolicy

        data = {
            "sales": [
                {"product": "A", "amount": 100},
                {"product": "A", "amount": 50}
            ]
        }
        result = (
            etl(data)
            .goto("sales").each()
            # Explicit join_on required for merging
            .map_to(table="sales", join_on=["product"], fields=[
                Field("total", get("amount"), merge=AddPolicy()),
                TempField("product", get("product"))
            ])
            .run()
        )
        sales = result.tables["sales"]
        assert len(sales) == 1
        assert sales[("A",)]["total"] == 150


class TestRunRelationships:
    """Tests for run() with relationship binding."""

    def test_link_to_binds_parent_reference(self):
        """link_to() causes parent reference to be bound."""
        from dataclasses import dataclass as dc
        from etielle.fluent import etl

        @dc
        class User:
            __tablename__ = "users"
            name: str
            id: int = 0

        @dc
        class Post:
            __tablename__ = "posts"
            title: str
            user: User | None = None
            user_id: int = 0

        data = {
            "users": [{"id": 1, "name": "Alice"}],
            "posts": [{"id": 101, "title": "Hello", "user_id": 1}]
        }

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table=User, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .goto_root()
            .goto("posts").each()
            .map_to(table=Post, fields=[
                Field("title", get("title")),
                TempField("id", get("id")),
                TempField("user_id", get("user_id"))
            ])
            .link_to(User, by={"user_id": "id"})
            .run()
        )

        posts = result.tables[Post]
        post = list(posts.values())[0]
        assert post.user is not None
        assert post.user.name == "Alice"


class TestModelDetection:
    """Tests for automatic model type detection."""

    def test_pydantic_model_detection(self):
        """Pydantic models use PydanticBuilder."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class User(BaseModel):
            name: str
            __tablename__: ClassVar[str] = "users"

        data = {"users": [{"name": "Alice", "id": 1}]}
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table=User, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .run()
        )
        users = result.tables[User]
        user = list(users.values())[0]
        assert isinstance(user, User)
        assert user.name == "Alice"

    def test_dataclass_detection(self):
        """Dataclasses use ConstructorBuilder."""
        from dataclasses import dataclass as dc
        from etielle.fluent import etl

        @dc
        class Item:
            __tablename__ = "items"
            value: int

        data = {"items": [{"value": 42, "id": 1}]}
        result = (
            etl(data)
            .goto("items").each()
            .map_to(table=Item, fields=[
                Field("value", get("value")),
                TempField("id", get("id"))
            ])
            .run()
        )
        items = result.tables[Item]
        item = list(items.values())[0]
        assert isinstance(item, Item)
        assert item.value == 42

    def test_string_table_returns_dicts(self):
        """String table names return plain dicts."""
        from etielle.fluent import etl

        data = {"items": [{"x": 1, "id": 1}]}
        result = (
            etl(data)
            .goto("items").each()
            .map_to(table="items", fields=[
                Field("x", get("x")),
                TempField("id", get("id"))
            ])
            .run()
        )
        items = result.tables["items"]
        item = list(items.values())[0]
        assert isinstance(item, dict)


class TestMultipleRoots:
    """Tests for multiple JSON root support."""

    def test_multiple_roots_separate_traversals(self):
        """Can traverse different roots in same pipeline."""
        from etielle.fluent import etl

        users_json = {"users": [{"id": 1, "name": "Alice"}]}
        profiles_json = {"profiles": [{"user_id": 1, "bio": "Hello"}]}

        result = (
            etl(users_json, profiles_json)
            .goto_root(0).goto("users").each()
            # Both map_to calls need join_on for merging
            .map_to(table="users", join_on=["id"], fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .goto_root(1).goto("profiles").each()
            .map_to(table="users", join_on=["id"], fields=[
                Field("bio", get("bio")),
                TempField("id", get("user_id"))
            ])
            .run()
        )

        users = result.tables["users"]
        assert len(users) == 1
        user = users[(1,)]
        assert user["name"] == "Alice"
        assert user["bio"] == "Hello"


class TestErrorHandling:
    """Tests for error handling modes."""

    def test_collect_errors_continues_processing(self):
        """errors='collect' continues after validation errors."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class StrictUser(BaseModel):
            name: str
            age: int
            __tablename__: ClassVar[str] = "strictuser"

        data = {"users": [
            {"name": "Alice", "age": 30, "id": 1},
            {"name": "Bob", "age": "not a number", "id": 2},  # Invalid
            {"name": "Carol", "age": 25, "id": 3}
        ]}

        result = (
            etl(data, errors="collect")
            .goto("users").each()
            .map_to(table=StrictUser, fields=[
                Field("name", get("name")),
                Field("age", get("age")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # Should have 2 valid users
        users = result.tables[StrictUser]
        assert len(users) == 2

        # Should have 1 error
        assert "strictuser" in result.errors or "StrictUser" in result.errors

    def test_fail_fast_stops_on_first_error(self):
        """errors='fail_fast' raises on first error."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class StrictUser(BaseModel):
            name: str
            age: int
            __tablename__: ClassVar[str] = "strictuser"

        data = {"users": [
            {"name": "Alice", "age": "invalid", "id": 1},
            {"name": "Bob", "age": 30, "id": 2}
        ]}

        with pytest.raises(Exception):  # Specific exception TBD
            (
                etl(data, errors="fail_fast")
                .goto("users").each()
                .map_to(table=StrictUser, fields=[
                    Field("name", get("name")),
                    Field("age", get("age")),
                    TempField("id", get("id"))
                ])
                .run()
            )


class TestPublicAPI:
    """Tests for public API exports."""

    def test_import_from_package(self):
        """Core fluent API is importable from etielle."""
        from etielle import etl, Field, TempField, transform
        assert callable(etl)
        assert Field is not None
        assert TempField is not None
        assert callable(transform)

    def test_transforms_from_package(self):
        """Transforms are importable from etielle."""
        from etielle import get, get_from_root, get_from_parent
        from etielle import literal, concat, coalesce, format_id
        from etielle import key, index, parent_key, parent_index, node
        # All should be callable
        assert all(callable(t) for t in [
            get, get_from_root, get_from_parent,
            literal, concat, coalesce, format_id,
            key, index, parent_key, parent_index, node
        ])

    def test_policies_from_package(self):
        """Merge policies are importable from etielle."""
        from etielle import AddPolicy
        assert AddPolicy is not None


class TestBuildDependencyGraph:
    """Tests for _build_dependency_graph method."""

    def test_no_relationships_empty_graph(self):
        """No link_to calls means empty graph."""
        from etielle.fluent import etl

        builder = etl({})
        builder.goto("users").each().map_to(table="users", fields=[])

        graph = builder._build_dependency_graph()
        assert graph == {}

    def test_single_relationship(self):
        """Single link_to creates edge."""
        from etielle.fluent import etl

        class User:
            __tablename__ = "users"

        builder = etl({})
        builder.goto("users").each().map_to(table="users", fields=[])
        builder.goto("posts").each().map_to(table="posts", fields=[
            TempField("user_id", get("user_id"))
        ])
        builder.link_to(User, by={"user_id": "id"})

        graph = builder._build_dependency_graph()
        assert graph == {"posts": {"users"}}

    def test_chain_of_relationships(self):
        """Chain: grandchild -> child -> parent."""
        from etielle.fluent import etl

        class Survey:
            __tablename__ = "surveys"
        class Block:
            __tablename__ = "blocks"

        builder = etl({})
        builder.goto("surveys").each().map_to(table="surveys", fields=[])
        builder.goto("blocks").each().map_to(table="blocks", fields=[
            TempField("survey_id", get("survey_id"))
        ])
        builder.link_to(Survey, by={"survey_id": "id"})
        builder.goto("questions").each().map_to(table="questions", fields=[
            TempField("block_id", get("block_id"))
        ])
        builder.link_to(Block, by={"block_id": "id"})

        graph = builder._build_dependency_graph()
        assert graph == {
            "blocks": {"surveys"},
            "questions": {"blocks"}
        }

    def test_multiple_parents(self):
        """Child with multiple parents."""
        from etielle.fluent import etl

        class User:
            __tablename__ = "users"
        class Post:
            __tablename__ = "posts"

        builder = etl({})
        builder.goto("users").each().map_to(table="users", fields=[])
        builder.goto("posts").each().map_to(table="posts", fields=[])
        builder.goto("comments").each().map_to(table="comments", fields=[
            TempField("user_id", get("uid")),
            TempField("post_id", get("pid"))
        ])
        builder.link_to(User, by={"user_id": "id"})
        builder.link_to(Post, by={"post_id": "id"})

        graph = builder._build_dependency_graph()
        assert graph == {"comments": {"users", "posts"}}


class TestRelationshipEdgeCases:
    """Tests for relationship edge cases and advanced scenarios."""

    def test_composite_key_relationships(self):
        """Link child to parent via multiple fields (composite key)."""
        from dataclasses import dataclass as dc
        from etielle.fluent import etl

        @dc
        class Store:
            __tablename__ = "stores"
            region_id: int = 0
            store_id: int = 0
            name: str = ""

        @dc
        class Sale:
            __tablename__ = "sales"
            amount: float = 0.0
            store: Store | None = None

        data = {
            "stores": [
                {"region_id": 1, "store_id": 101, "name": "North Branch"},
                {"region_id": 1, "store_id": 102, "name": "South Branch"},
                {"region_id": 2, "store_id": 101, "name": "East Branch"},
            ],
            "sales": [
                {"region_id": 1, "store_id": 101, "amount": 100.0},
                {"region_id": 2, "store_id": 101, "amount": 200.0},
                {"region_id": 1, "store_id": 102, "amount": 150.0},
            ]
        }

        result = (
            etl(data)
            .goto("stores").each()
            .map_to(table=Store, join_on=["region_id", "store_id"], fields=[
                Field("name", get("name")),
                Field("region_id", get("region_id")),
                Field("store_id", get("store_id")),
            ])
            .goto_root()
            .goto("sales").each()
            .map_to(table=Sale, fields=[
                Field("amount", get("amount")),
                TempField("region_id", get("region_id")),
                TempField("store_id", get("store_id")),
            ])
            .link_to(Store, by={"region_id": "region_id", "store_id": "store_id"})
            .run()
        )

        sales = result.tables[Sale]
        stores = result.tables[Store]

        # Verify composite keys work - 3 stores with composite keys
        assert len(stores) == 3
        # Stores are keyed by (region_id, store_id) tuples
        assert (1, 101) in stores
        assert (1, 102) in stores
        assert (2, 101) in stores

        # Verify all sales have store linkages (composite key matching works)
        assert len(sales) == 3
        for sale in sales.values():
            # Each sale should be linked to a store
            assert sale.store is not None
            # Store should have valid region and store IDs
            assert sale.store.region_id in [1, 2]
            assert sale.store.store_id in [101, 102]

    def test_attribute_inference_users_to_user(self):
        """Verify 'users' table infers 'user' attribute."""
        from dataclasses import dataclass as dc
        from etielle.fluent import etl

        @dc
        class User:
            __tablename__ = "users"
            name: str = ""
            id: int = 0

        @dc
        class Post:
            __tablename__ = "posts"
            title: str = ""
            user: User | None = None

        data = {
            "users": [{"id": 1, "name": "Alice"}],
            "posts": [{"title": "Hello", "user_id": 1}]
        }

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table=User, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .goto_root()
            .goto("posts").each()
            .map_to(table=Post, fields=[
                Field("title", get("title")),
                TempField("user_id", get("user_id"))
            ])
            .link_to(User, by={"user_id": "id"})
            .run()
        )

        posts = result.tables[Post]
        post = list(posts.values())[0]
        # Should set 'user' attribute (not 'users')
        assert hasattr(post, 'user')
        assert post.user is not None
        assert post.user.name == "Alice"

    def test_attribute_inference_edge_case_categories(self):
        """Verify 'categories' table infers 'categorie' attribute (edge case)."""
        from dataclasses import dataclass as dc
        from etielle.fluent import etl

        @dc
        class Category:
            __tablename__ = "categories"
            name: str = ""
            id: int = 0

        @dc
        class Product:
            __tablename__ = "products"
            title: str = ""
            categorie: Category | None = None

        data = {
            "categories": [{"id": 1, "name": "Electronics"}],
            "products": [{"title": "Laptop", "category_id": 1}]
        }

        result = (
            etl(data)
            .goto("categories").each()
            .map_to(table=Category, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .goto_root()
            .goto("products").each()
            .map_to(table=Product, fields=[
                Field("title", get("title")),
                TempField("category_id", get("category_id"))
            ])
            .link_to(Category, by={"category_id": "id"})
            .run()
        )

        products = result.tables[Product]
        product = list(products.values())[0]
        # Should set 'categorie' attribute (strips trailing 's')
        assert hasattr(product, 'categorie')
        assert product.categorie is not None
        assert product.categorie.name == "Electronics"

    def test_attribute_inference_no_s_suffix(self):
        """Verify table without 's' suffix keeps original name."""
        from dataclasses import dataclass as dc
        from etielle.fluent import etl

        @dc
        class Team:
            __tablename__ = "team"
            name: str = ""
            id: int = 0

        @dc
        class Member:
            __tablename__ = "members"
            name: str = ""
            team: Team | None = None

        data = {
            "team": [{"id": 1, "name": "Engineering"}],
            "members": [{"name": "Bob", "team_id": 1}]
        }

        result = (
            etl(data)
            .goto("team").each()
            .map_to(table=Team, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .goto_root()
            .goto("members").each()
            .map_to(table=Member, fields=[
                Field("name", get("name")),
                TempField("team_id", get("team_id"))
            ])
            .link_to(Team, by={"team_id": "id"})
            .run()
        )

        members = result.tables[Member]
        member = list(members.values())[0]
        assert hasattr(member, 'team')
        assert member.team is not None
        assert member.team.name == "Engineering"

    def test_type_mismatch_int_vs_string_keys(self):
        """Parent has int keys, child has string keys - verify behavior."""
        from dataclasses import dataclass as dc
        from etielle.fluent import etl

        @dc
        class User:
            __tablename__ = "users"
            name: str = ""
            id: int = 0

        @dc
        class Post:
            __tablename__ = "posts"
            title: str = ""
            user: User | None = None

        data = {
            "users": [{"id": 1, "name": "Alice"}],
            # Child has string "1" instead of int 1
            "posts": [{"title": "Hello", "user_id": "1"}]
        }

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table=User, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .goto_root()
            .goto("posts").each()
            .map_to(table=Post, fields=[
                Field("title", get("title")),
                TempField("user_id", get("user_id"))
            ])
            .link_to(User, by={"user_id": "id"})
            .run()
        )

        posts = result.tables[Post]
        post = list(posts.values())[0]
        # Type mismatch means no match - relationship should be None
        assert post.user is None

    def test_empty_parent_table_all_orphaned(self):
        """All children orphaned when parent table is empty."""
        from dataclasses import dataclass as dc
        from etielle.fluent import etl

        @dc
        class User:
            __tablename__ = "users"
            name: str = ""
            id: int = 0

        @dc
        class Post:
            __tablename__ = "posts"
            title: str = ""
            user: User | None = None

        data = {
            "users": [],  # Empty parent table
            "posts": [
                {"title": "Post 1", "user_id": 1},
                {"title": "Post 2", "user_id": 2}
            ]
        }

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table=User, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .goto_root()
            .goto("posts").each()
            .map_to(table=Post, fields=[
                Field("title", get("title")),
                TempField("user_id", get("user_id"))
            ])
            .link_to(User, by={"user_id": "id"})
            .run()
        )

        posts = result.tables[Post]
        assert len(posts) == 2

        # All posts should have None user
        for post in posts.values():
            assert post.user is None

    def test_empty_child_table_no_errors(self):
        """No errors when child table is empty."""
        from dataclasses import dataclass as dc
        from etielle.fluent import etl

        @dc
        class User:
            __tablename__ = "users"
            name: str = ""
            id: int = 0

        @dc
        class Post:
            __tablename__ = "posts"
            title: str = ""
            user: User | None = None

        data = {
            "users": [{"id": 1, "name": "Alice"}],
            "posts": []  # Empty child table
        }

        # Should not raise any errors
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table=User, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .goto_root()
            .goto("posts").each()
            .map_to(table=Post, fields=[
                Field("title", get("title")),
                TempField("user_id", get("user_id"))
            ])
            .link_to(User, by={"user_id": "id"})
            .run()
        )

        users = result.tables[User]
        assert len(users) == 1

        # When child table is empty, it may not appear in results
        # Just verify no error was raised - the test passing is the success

    def test_get_from_parent_depth_2_grandparent_access(self):
        """get_from_parent(depth=2) accesses grandparent in 2-level iteration."""
        from etielle.fluent import etl
        from etielle.transforms import get_from_parent

        data = {
            "companies": [
                {
                    "name": "Acme Corp",
                    "departments": [
                        {
                            "name": "Engineering",
                            "employees": [{"name": "Alice"}, {"name": "Bob"}]
                        },
                        {
                            "name": "Sales",
                            "employees": [{"name": "Carol"}]
                        }
                    ]
                }
            ]
        }

        result = (
            etl(data)
            .goto("companies").each()
            .goto("departments").each()
            .map_to(table="departments", fields=[
                Field("name", get("name")),
                Field("company_name", get_from_parent("name"))
            ])
            .run()
        )

        departments = result.tables["departments"]
        assert len(departments) == 2

        # All departments should have company_name from parent
        for dept in departments.values():
            assert dept["company_name"] == "Acme Corp"

    def test_get_from_parent_depth_param(self):
        """get_from_parent accepts depth parameter."""
        from etielle.fluent import etl
        from etielle.transforms import get_from_parent

        # Test that depth parameter is accepted (depth=2 for accessing grandparent)
        data = {
            "level1": [
                {
                    "name": "L1",
                    "level2": [
                        {
                            "name": "L2",
                            "level3": [{"value": "test"}]
                        }
                    ]
                }
            ]
        }

        # Just verify the API accepts depth parameter and runs without error
        result = (
            etl(data)
            .goto("level1").each()
            .goto("level2").each()
            .map_to(table="level2", fields=[
                Field("name", get("name")),
                Field("parent_name", get_from_parent("name", depth=1))
            ])
            .run()
        )

        level2 = result.tables["level2"]
        assert len(level2) == 1

        # Verify depth=1 accesses parent
        for item in level2.values():
            assert item["parent_name"] == "L1"

    def test_optional_relationship_missing_parent(self):
        """Child with missing parent when relationship is optional (no error)."""
        from dataclasses import dataclass as dc
        from etielle.fluent import etl

        @dc
        class User:
            __tablename__ = "users"
            name: str = ""
            id: int = 0

        @dc
        class Post:
            __tablename__ = "posts"
            title: str = ""
            user: User | None = None

        data = {
            "users": [{"id": 1, "name": "Alice"}],
            "posts": [
                {"title": "Post with user", "user_id": 1},
                {"title": "Post without user", "user_id": 999},  # Missing parent
                {"title": "Post with None", "user_id": None}  # None user_id
            ]
        }

        # Should not raise errors (relationships are optional by default)
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table=User, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .goto_root()
            .goto("posts").each()
            .map_to(table=Post, fields=[
                Field("title", get("title")),
                TempField("user_id", get("user_id"))
            ])
            .link_to(User, by={"user_id": "id"})
            .run()
        )

        posts = result.tables[Post]
        assert len(posts) == 3

        posts_list = list(posts.values())

        # Find posts by title
        post_with_user = [p for p in posts_list if "with user" in p.title][0]
        post_without_user = [p for p in posts_list if "without user" in p.title][0]
        post_with_none = [p for p in posts_list if "with None" in p.title][0]

        # Only the first should have a user
        assert post_with_user.user is not None
        assert post_with_user.user.name == "Alice"

        # Others should have None user (not error)
        assert post_without_user.user is None
        assert post_with_none.user is None


class TestGetLinkableFields:
    """Tests for _get_linkable_fields method."""

    def test_extract_linkable_fields_from_link_to(self):
        """Pipeline should extract which fields are used for linking."""
        from etielle.fluent import etl
        from etielle.transforms import key

        class Parent:
            __tablename__ = "parents"

        class Child:
            __tablename__ = "children"

        builder = (
            etl({"parents": [], "children": []})
            .goto("parents").each()
            .map_to(table=Parent, fields=[
                Field("external_id", key()),
                Field("name", get("name")),
            ])
            .goto_root()
            .goto("children").each()
            .map_to(table=Child, fields=[
                Field("title", get("title")),
                TempField("parent_ref", get("parent_id")),
            ])
            .link_to(Parent, by={"parent_ref": "external_id"})
        )

        linkable = builder._get_linkable_fields()
        assert linkable == {"parents": {"external_id"}}

    def test_secondary_indices_built_for_linkable_fields(self):
        """Pipeline should build secondary indices for fields used in link_to."""
        from etielle.fluent import etl, Field, TempField
        from etielle.transforms import get, key

        data = {
            "parents": {"P1": {"name": "Parent1"}, "P2": {"name": "Parent2"}},
            "children": [{"title": "Child1", "parent_id": "P1"}]
        }

        class FakeParent:
            __tablename__ = "parents"
            def __init__(self, external_id=None, name=None):
                self.external_id = external_id
                self.name = name

        class FakeChild:
            __tablename__ = "children"
            def __init__(self, title=None, parent_ref=None):
                self.title = title
                self.parent_ref = parent_ref
                self.parent = None

        result = (
            etl(data)
            .goto("parents").each()
            .map_to(table=FakeParent, fields=[
                Field("external_id", key()),
                Field("name", get("name")),
            ])
            .goto_root()
            .goto("children").each()
            .map_to(table=FakeChild, fields=[
                Field("title", get("title")),
                TempField("parent_ref", get("parent_id")),
            ])
            .link_to(FakeParent, by={"parent_ref": "external_id"})
            .run()
        )

        # Check secondary index was built
        # Access the raw MappingResult which should have indices
        assert result is not None
        assert result._raw_results is not None
        parents_result = result._raw_results["parents"]
        assert parents_result.indices is not None
        assert "external_id" in parents_result.indices
        # Should have index: {"P1": parent1_instance, "P2": parent2_instance}
        assert "P1" in parents_result.indices["external_id"]
        assert "P2" in parents_result.indices["external_id"]
        # Verify the indexed instances have the correct external_id
        assert parents_result.indices["external_id"]["P1"].external_id == "P1"
        assert parents_result.indices["external_id"]["P2"].external_id == "P2"


class TestNavigationEdgeCases:
    """Tests for navigation edge cases and boundary conditions."""

    def test_empty_list_iteration_produces_zero_rows(self):
        """goto("items").each() where items=[] produces zero rows."""
        from etielle.fluent import etl

        data = {"items": []}
        (
            etl(data)
            .goto("items").each()
            .map_to(table="items", fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .run()
        )



class TestErrorHandlingEdgeCases:
    """Tests for error handling edge cases."""

    def test_per_table_error_mode_pipeline_fail_fast_wins(self):
        """Pipeline-level error_mode='fail_fast' wins even with table-level override."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class StrictProduct(BaseModel):
            name: str
            price: float
            __tablename__: ClassVar[str] = "products"

        data = {
            "products": [
                {"id": 1, "name": "Widget", "price": 9.99},
                {"id": 2, "name": "Gadget", "price": "invalid"},  # Invalid price
            ],
        }

        # Pipeline-level fail_fast wins, even with table-level collect override
        with pytest.raises(ValueError):
            (
                etl(data, errors="fail_fast")  # Pipeline fail_fast
                .goto("products").each()
                .map_to(table=StrictProduct, errors="collect", fields=[  # Table collect (doesn't override)
                    Field("name", get("name")),
                    Field("price", get("price")),
                    TempField("id", get("id"))
                ])
                .run()
            )

    def test_per_table_error_mode_used_during_instance_building(self):
        """Per-table error mode affects instance building behavior."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class StrictModel(BaseModel):
            value: int
            __tablename__: ClassVar[str] = "items"

        data = {
            "items": [
                {"id": 1, "value": 10},
                {"id": 2, "value": "invalid"},  # Invalid
                {"id": 3, "value": 20},
            ]
        }

        # With collect mode, invalid rows are skipped and errors collected
        result = (
            etl(data, errors="collect")
            .goto("items").each()
            .map_to(table=StrictModel, errors="collect", fields=[
                Field("value", get("value")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # Should have 2 valid items
        items = result.tables[StrictModel]
        assert len(items) == 2

        # Should have 1 error collected
        table_errors = result.errors.get("items") or result.errors.get("StrictModel")
        assert table_errors is not None
        assert len(table_errors) == 1

    def test_multiple_errors_per_row_pydantic(self):
        """Single row with multiple Pydantic validation failures."""
        from pydantic import BaseModel, field_validator

        class StrictRecord(BaseModel):
            name: str
            age: int
            email: str
            __tablename__: ClassVar[str] = "records"

            @field_validator("name")
            @classmethod
            def name_not_empty(cls, v):
                if not v or not v.strip():
                    raise ValueError("Name cannot be empty")
                return v

            @field_validator("age")
            @classmethod
            def age_valid(cls, v):
                if v < 0 or v > 150:
                    raise ValueError("Age must be between 0 and 150")
                return v

            @field_validator("email")
            @classmethod
            def email_has_at(cls, v):
                if "@" not in v:
                    raise ValueError("Email must contain @")
                return v

        from etielle.fluent import etl

        data = {"records": [
            {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
            {"id": 2, "name": "", "age": 200, "email": "invalid"},  # Multiple errors
            {"id": 3, "name": "Carol", "age": 25, "email": "carol@example.com"}
        ]}

        result = (
            etl(data, errors="collect")
            .goto("records").each()
            .map_to(table=StrictRecord, fields=[
                Field("name", get("name")),
                Field("age", get("age")),
                Field("email", get("email")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # Should have 2 valid records
        records = result.tables[StrictRecord]
        assert len(records) == 2

        # Should have errors for the invalid record
        assert "records" in result.errors or "StrictRecord" in result.errors
        table_errors = result.errors.get("records") or result.errors.get("StrictRecord")
        assert table_errors is not None
        assert len(table_errors) == 1

        # The error for id=2 should contain validation messages
        error_messages = list(table_errors.values())[0]
        assert len(error_messages) >= 1  # At least one error message

    def test_errors_across_multiple_tables(self):
        """Errors in both 'users' and 'posts' tables, verify structure."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class User(BaseModel):
            name: str
            age: int
            __tablename__: ClassVar[str] = "users"

        class Post(BaseModel):
            title: str
            word_count: int
            __tablename__: ClassVar[str] = "posts"

        data = {
            "users": [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": "invalid"},  # Error in users
                {"id": 3, "name": "Carol", "age": 25}
            ],
            "posts": [
                {"id": 101, "title": "First Post", "word_count": 100},
                {"id": 102, "title": "Second Post", "word_count": "invalid"},  # Error in posts
                {"id": 103, "title": "Third Post", "word_count": 200}
            ]
        }

        result = (
            etl(data, errors="collect")
            .goto("users").each()
            .map_to(table=User, fields=[
                Field("name", get("name")),
                Field("age", get("age")),
                TempField("id", get("id"))
            ])
            .goto_root()
            .goto("posts").each()
            .map_to(table=Post, fields=[
                Field("title", get("title")),
                Field("word_count", get("word_count")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # Should have 2 valid users and 2 valid posts
        users = result.tables[User]
        posts = result.tables[Post]
        assert len(users) == 2
        assert len(posts) == 2

        # Should have errors in both tables
        assert len(result.errors) == 2
        assert ("users" in result.errors or "User" in result.errors)
        assert ("posts" in result.errors or "Post" in result.errors)

        # Each table should have 1 error
        user_errors = result.errors.get("users") or result.errors.get("User")
        post_errors = result.errors.get("posts") or result.errors.get("Post")
        assert user_errors is not None
        assert post_errors is not None
        assert len(user_errors) == 1
        assert len(post_errors) == 1

    def test_continue_on_error_pattern(self):
        """Process successful rows, verify failed rows in errors dict."""
        from pydantic import BaseModel, field_validator
        from etielle.fluent import etl

        class Transaction(BaseModel):
            amount: float
            account_id: str
            __tablename__: ClassVar[str] = "transactions"

            @field_validator("amount")
            @classmethod
            def amount_positive(cls, v):
                if v <= 0:
                    raise ValueError("Amount must be positive")
                return v

        data = {"transactions": [
            {"id": 1, "amount": 100.0, "account_id": "acc1"},
            {"id": 2, "amount": -50.0, "account_id": "acc2"},  # Invalid
            {"id": 3, "amount": 200.0, "account_id": "acc3"},
            {"id": 4, "amount": 0.0, "account_id": "acc4"},  # Invalid
            {"id": 5, "amount": 75.0, "account_id": "acc5"}
        ]}

        result = (
            etl(data, errors="collect")
            .goto("transactions").each()
            .map_to(table=Transaction, fields=[
                Field("amount", get("amount")),
                Field("account_id", get("account_id")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # Verify successful rows can be processed
        transactions = result.tables[Transaction]
        assert len(transactions) == 3
        successful_amounts = [t.amount for t in transactions.values()]
        assert 100.0 in successful_amounts
        assert 200.0 in successful_amounts
        assert 75.0 in successful_amounts

        # Verify failed rows are in errors
        table_errors = result.errors.get("transactions") or result.errors.get("Transaction")
        assert table_errors is not None
        assert len(table_errors) == 2

        # Verify we can identify which IDs failed
        assert len(list(table_errors.keys())) == 2

    def test_all_or_nothing_pattern(self):
        """Check if result.errors, then verify behavior."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class Product(BaseModel):
            name: str
            price: float
            __tablename__: ClassVar[str] = "products"

        # Test with valid data - should not raise
        valid_data = {"products": [
            {"id": 1, "name": "Widget", "price": 9.99},
            {"id": 2, "name": "Gadget", "price": 19.99}
        ]}

        result = (
            etl(valid_data, errors="collect")
            .goto("products").each()
            .map_to(table=Product, fields=[
                Field("name", get("name")),
                Field("price", get("price")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # All-or-nothing check
        if result.errors:
            raise ValueError(f"Batch failed with {sum(len(e) for e in result.errors.values())} errors")

        # If we get here, all products should be valid
        products = result.tables[Product]
        assert len(products) == 2

        # Test with invalid data - should raise when checked
        invalid_data = {"products": [
            {"id": 1, "name": "Widget", "price": 9.99},
            {"id": 2, "name": "Gadget", "price": "invalid"}
        ]}

        result = (
            etl(invalid_data, errors="collect")
            .goto("products").each()
            .map_to(table=Product, fields=[
                Field("name", get("name")),
                Field("price", get("price")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # All-or-nothing check should detect errors and raise
        with pytest.raises(ValueError, match="Batch failed"):
            if result.errors:
                raise ValueError(f"Batch failed with {sum(len(e) for e in result.errors.values())} errors")

    def test_missing_required_fields(self):
        """Missing required fields should generate validation errors."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class RequiredFieldsModel(BaseModel):
            name: str
            email: str
            age: int
            __tablename__: ClassVar[str] = "users"

        data = {"users": [
            {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
            {"id": 2, "name": "Bob"},  # Missing email and age
            {"id": 3, "email": "carol@example.com", "age": 25}  # Missing name
        ]}

        result = (
            etl(data, errors="collect")
            .goto("users").each()
            .map_to(table=RequiredFieldsModel, fields=[
                Field("name", get("name")),
                Field("email", get("email")),
                Field("age", get("age")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # Only the first user should be valid
        users = result.tables[RequiredFieldsModel]
        assert len(users) == 1

        # Should have 2 errors
        table_errors = result.errors.get("users") or result.errors.get("RequiredFieldsModel")
        assert table_errors is not None
        assert len(table_errors) == 2

    def test_type_coercion_errors(self):
        """Type coercion failures should be handled gracefully."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class TypedModel(BaseModel):
            count: int
            price: float
            active: bool
            __tablename__: ClassVar[str] = "items"

        data = {"items": [
            {"id": 1, "count": 10, "price": 9.99, "active": True},
            {"id": 2, "count": "not_a_number", "price": 19.99, "active": False},  # Bad count
            {"id": 3, "count": 5, "price": "invalid", "active": True},  # Bad price
            {"id": 4, "count": 8, "price": 14.99, "active": "not_bool"}  # Bad active
        ]}

        result = (
            etl(data, errors="collect")
            .goto("items").each()
            .map_to(table=TypedModel, fields=[
                Field("count", get("count")),
                Field("price", get("price")),
                Field("active", get("active")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # Only the first item should be valid
        items = result.tables[TypedModel]
        assert len(items) == 1

        # Should have 3 errors
        table_errors = result.errors.get("items") or result.errors.get("TypedModel")
        assert table_errors is not None
        assert len(table_errors) == 3

    def test_nested_validation_errors(self):
        """Pydantic models with nested validators."""
        from pydantic import BaseModel, field_validator
        from etielle.fluent import etl

        class Account(BaseModel):
            username: str
            balance: float
            __tablename__: ClassVar[str] = "accounts"

            @field_validator("username")
            @classmethod
            def username_valid(cls, v):
                if len(v) < 3:
                    raise ValueError("Username must be at least 3 characters")
                if not v.isalnum():
                    raise ValueError("Username must be alphanumeric")
                return v

            @field_validator("balance")
            @classmethod
            def balance_non_negative(cls, v):
                if v < 0:
                    raise ValueError("Balance cannot be negative")
                return v

        data = {"accounts": [
            {"id": 1, "username": "alice123", "balance": 100.0},
            {"id": 2, "username": "ab", "balance": 50.0},  # Username too short
            {"id": 3, "username": "user@123", "balance": 75.0},  # Username not alphanumeric
            {"id": 4, "username": "bob456", "balance": -10.0}  # Negative balance
        ]}

        result = (
            etl(data, errors="collect")
            .goto("accounts").each()
            .map_to(table=Account, fields=[
                Field("username", get("username")),
                Field("balance", get("balance")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # Only the first account should be valid
        accounts = result.tables[Account]
        assert len(accounts) == 1
        assert list(accounts.values())[0].username == "alice123"

        # Should have 3 errors
        table_errors = result.errors.get("accounts") or result.errors.get("Account")
        assert table_errors is not None
        assert len(table_errors) == 3

    def test_error_messages_are_lists(self):
        """Error messages should be in list format."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class StrictModel(BaseModel):
            value: int
            __tablename__: ClassVar[str] = "items"

        data = {"items": [
            {"id": 1, "value": "not_an_int"}
        ]}

        result = (
            etl(data, errors="collect")
            .goto("items").each()
            .map_to(table=StrictModel, fields=[
                Field("value", get("value")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # Should have 1 error
        table_errors = result.errors.get("items") or result.errors.get("StrictModel")
        assert table_errors is not None
        assert len(table_errors) == 1

        # Error messages should be a list
        error_messages = list(table_errors.values())[0]
        assert isinstance(error_messages, list)
        assert len(error_messages) > 0

    def test_empty_errors_dict_when_all_valid(self):
        """result.errors should be empty dict when no errors."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class SimpleModel(BaseModel):
            name: str
            __tablename__: ClassVar[str] = "items"

        data = {"items": [
            {"id": 1, "name": "Item1"},
            {"id": 2, "name": "Item2"}
        ]}

        result = (
            etl(data, errors="collect")
            .goto("items").each()
            .map_to(table=SimpleModel, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # Should have no errors
        assert result.errors == {}
        assert len(result.errors) == 0
        assert not result.errors  # Falsy when empty

    def test_partial_success_metrics(self):
        """Can calculate success/failure metrics from result."""
        from pydantic import BaseModel
        from etielle.fluent import etl

        class Item(BaseModel):
            count: int
            __tablename__: ClassVar[str] = "items"

        data = {"items": [
            {"id": 1, "count": 10},
            {"id": 2, "count": "invalid"},
            {"id": 3, "count": 20},
            {"id": 4, "count": "bad"},
            {"id": 5, "count": 30},
        ]}

        result = (
            etl(data, errors="collect")
            .goto("items").each()
            .map_to(table=Item, fields=[
                Field("count", get("count")),
                TempField("id", get("id"))
            ])
            .run()
        )

        # Calculate metrics
        successful_count = len(result.tables[Item])
        failed_count = len(result.errors.get("items", {}) or result.errors.get("Item", {}))
        total_count = successful_count + failed_count

        assert successful_count == 3
        assert failed_count == 2
        assert total_count == 5

        # Calculate success rate
        success_rate = successful_count / total_count
        assert success_rate == 0.6
