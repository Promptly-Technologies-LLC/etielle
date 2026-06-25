"""Tests for issue #9A: single-pass execution and component-scoped flush/evict."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import patch

import pytest
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from etielle import Field, TempField, etl, get
from etielle.utils import partition_components, weakly_connected_components


Base = declarative_base()


class Tag(Base):
    __tablename__ = "tags"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Item(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    tag_id = Column(Integer, ForeignKey("tags.id"))


class UserA(Base):
    __tablename__ = "users_a"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class PostA(Base):
    __tablename__ = "posts_a"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    user_id = Column(Integer, ForeignKey("users_a.id"))


class UserB(Base):
    __tablename__ = "users_b"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class PostB(Base):
    __tablename__ = "posts_b"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    user_id = Column(Integer, ForeignKey("users_b.id"))


@dataclass
class Parent:
    id: str


@dataclass
class Child:
    id: str
    parent: Parent | None = None


class TestWeaklyConnectedComponents:
    def test_two_isolated_components(self):
        graph = {
            "posts_a": {"users_a"},
            "posts_b": {"users_b"},
        }
        nodes = {"users_a", "posts_a", "users_b", "posts_b"}
        components = weakly_connected_components(graph, nodes)
        assert len(components) == 2
        assert {"users_a", "posts_a"} in components
        assert {"users_b", "posts_b"} in components

    def test_orphan_tables_grouped(self):
        graph = {"posts": {"users"}}
        all_tables = {"users", "posts", "metrics", "logs"}
        components = partition_components(graph, all_tables, set())
        assert len(components) == 2
        assert {"users", "posts"} in components
        assert {"metrics", "logs"} in components

    def test_eager_edges_excluded(self):
        graph = {
            "items_0": {"tags"},
            "items_1": {"tags"},
        }
        all_tables = {"tags", "items_0", "items_1"}
        components = partition_components(graph, all_tables, {"tags"})
        # Orphan item tables batch together for mapping efficiency
        assert len(components) == 1
        assert components[0] == {"items_0", "items_1"}


class TestSinglePassLookupCapture:
    def test_lookup_values_captured_during_mapping(self):
        data = {
            "parents": [{"id": "p1"}],
            "children": [{"id": "c1", "parent_id": "p1"}],
        }

        result = (
            etl(data)
            .goto("parents")
            .each()
            .map_to(
                table=Parent,
                fields=[
                    Field("id", get("id")),
                ],
            )
            .goto_root()
            .goto("children")
            .each()
            .map_to(
                table=Child,
                fields=[
                    Field("id", get("id")),
                    TempField("parent_id", get("parent_id")),
                ],
            )
            .link_to(Parent, by={"parent_id": "id"})
            .run()
        )

        child = next(iter(result.tables[Child].values()))
        assert child.parent is not None
        assert child.parent.id == "p1"

        with patch(
            "etielle.relationships.compute_child_lookup_values",
            side_effect=AssertionError("should not re-traverse"),
        ):
            # Re-bind using captured lookup values only
            from etielle.relationships import bind_relationships_via_index

            raw = result._raw_results
            rels = [
                {
                    "child_table": "child",
                    "parent_table": "parent",
                    "by": {"parent_id": "id"},
                }
            ]
            lookup = {t: mr.lookup_values for t, mr in raw.items()}
            bind_relationships_via_index(raw, rels, lookup, fail_on_missing=False)

    def test_no_compute_child_lookup_in_run(self):
        data = {
            "parents": [{"id": "p1"}],
            "children": [{"id": "c1", "parent_id": "p1"}],
        }

        with patch(
            "etielle.relationships.compute_child_lookup_values",
            side_effect=AssertionError("compute_child_lookup_values called"),
        ), patch(
            "etielle.relationships.compute_backlink_lookup_values",
            side_effect=AssertionError("compute_backlink_lookup_values called"),
        ):
            (
                etl(data)
                .goto("parents")
                .each()
                .map_to(
                    table=Parent,
                    fields=[Field("id", get("id"))],
                )
                .goto_root()
                .goto("children")
                .each()
                .map_to(
                    table=Child,
                    fields=[
                        Field("id", get("id")),
                        TempField("parent_id", get("parent_id")),
                    ],
                )
                .link_to(Parent, by={"parent_id": "id"})
                .run()
            )


class TestComponentEviction:
    def test_component_instances_not_retained_after_load(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        data = {
            "users_a": [{"id": 1, "name": "A"}],
            "posts_a": [{"id": 1, "title": "pa", "user_id": 1}],
            "users_b": [{"id": 2, "name": "B"}],
            "posts_b": [{"id": 2, "title": "pb", "user_id": 2}],
        }

        result = (
            etl(data)
            .goto("users_a")
            .each()
            .map_to(
                table=UserA,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .goto_root()
            .goto("posts_a")
            .each()
            .map_to(
                table=PostA,
                fields=[
                    Field("title", get("title")),
                    TempField("id", get("id")),
                    TempField("user_id", get("user_id")),
                ],
            )
            .link_to(UserA, by={"user_id": "id"})
            .goto_root()
            .goto("users_b")
            .each()
            .map_to(
                table=UserB,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .goto_root()
            .goto("posts_b")
            .each()
            .map_to(
                table=PostB,
                fields=[
                    Field("title", get("title")),
                    TempField("id", get("id")),
                    TempField("user_id", get("user_id")),
                ],
            )
            .link_to(UserB, by={"user_id": "id"})
            .load(session)
            .run()
        )

        session.commit()
        assert "users_a" not in result.tables
        assert session.query(UserA).count() == 1
        assert session.query(UserB).count() == 1
        session.close()


class TestLoadEager:
    def test_eager_parent_shared_across_components(self):
        data = {
            "tags": [{"id": 1, "name": "t1"}, {"id": 2, "name": "t2"}],
            "items_0": [{"id": 1, "name": "i0", "tag_id": 1}],
            "items_1": [{"id": 2, "name": "i1", "tag_id": 2}],
        }

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        (
            etl(data)
            .goto("tags")
            .each()
            .map_to(
                table=Tag,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .load_eager(Tag)
            .goto_root()
            .goto("items_0")
            .each()
            .map_to(
                table="items_0",
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                    TempField("tag_id", get("tag_id")),
                ],
            )
            .link_to(Tag, by={"tag_id": "id"})
            .goto_root()
            .goto("items_1")
            .each()
            .map_to(
                table="items_1",
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                    TempField("tag_id", get("tag_id")),
                ],
            )
            .link_to(Tag, by={"tag_id": "id"})
            .load(session)
            .run()
        )

        session.commit()
        assert session.query(Tag).count() == 2
        session.close()

    def test_load_eager_requires_map_to(self):
        with pytest.raises(ValueError, match="requires a preceding map_to"):
            etl({}).load_eager("missing").run()

    def test_load_eager_rejects_non_eager_parent_dependency(self):
        data = {"parents": [], "children": []}

        builder = (
            etl(data)
            .goto("parents")
            .each()
            .map_to(table="parents", fields=[])
            .goto_root()
            .goto("children")
            .each()
            .map_to(table="children", fields=[TempField("pid", get("pid"))])
            .link_to("parents", by={"pid": "id"})
            .load_eager("children")
        )

        with pytest.raises(ValueError, match="cannot depend on non-eager"):
            builder.run()


class TestLoadModeResultContract:
    def test_load_mode_returns_stats_not_instances(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        result = (
            etl({"users": [{"id": 1, "name": "Alice"}]})
            .goto("users")
            .each()
            .map_to(
                table=UserA,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .load(session)
            .run()
        )

        assert "users_a" not in result.tables
        assert result.stats["users_a"].inserted == 1
        session.close()

    def test_non_load_mode_still_returns_instances(self):
        result = (
            etl({"users": [{"id": 1, "name": "Alice"}]})
            .goto("users")
            .each()
            .map_to(
                table=UserA,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .run()
        )
        assert "users_a" in result.tables
        assert len(result.tables["users_a"]) == 1
