"""Tests for issue #75: streaming + chunked execution (key-complete contract)."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from etielle import Field, TempField, etl, get, stream
from etielle.chunking import (
    CallableChunkSource,
    Chunk,
    FlushContext,
    KeyCompleteFlushStrategy,
    OneRecordPerChunkSource,
)
from etielle.instances import AddPolicy
from etielle.relationships import RelationshipIncompleteError


Base = declarative_base()


class StreamUser(Base):
    __tablename__ = "stream_users"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class StreamPost(Base):
    __tablename__ = "stream_posts"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    user_id = Column(Integer, ForeignKey("stream_users.id"))


class StreamTag(Base):
    __tablename__ = "stream_tags"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class StreamItem(Base):
    __tablename__ = "stream_items"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    tag_id = Column(Integer, ForeignKey("stream_tags.id"))


class StreamSale(Base):
    __tablename__ = "stream_sales"
    id = Column(Integer, primary_key=True)
    product = Column(String)
    total = Column(Integer)


@dataclass
class Parent:
    id: str


@dataclass
class Child:
    id: str
    parent: Parent | None = None


class TestStreamingRequiresLoad:
    def test_stream_without_load_raises(self):
        builder = (
            stream([{"users": []}])
            .goto("users")
            .each()
            .map_to(table="users", fields=[Field("name", get("name"))])
        )
        with pytest.raises(ValueError, match="requires load"):
            builder.run()


class TestResidentParity:
    def test_chunked_equals_resident(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        combined = {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
            "posts": [
                {"id": 10, "title": "Hello", "user_id": 1},
                {"id": 11, "title": "World", "user_id": 2},
            ],
        }

        resident = (
            etl(combined)
            .goto("users")
            .each()
            .map_to(
                table=StreamUser,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .goto_root()
            .goto("posts")
            .each()
            .map_to(
                table=StreamPost,
                fields=[
                    Field("title", get("title")),
                    TempField("id", get("id")),
                    TempField("user_id", get("user_id")),
                ],
            )
            .link_to(StreamUser, by={"user_id": "id"})
            .load(session)
            .run()
        )
        session.commit()
        session.close()

        engine2 = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine2)
        Session2 = sessionmaker(bind=engine2)
        session2 = Session2()

        chunks = [
            {
                "users": [{"id": 1, "name": "Alice"}],
                "posts": [{"id": 10, "title": "Hello", "user_id": 1}],
            },
            {
                "users": [{"id": 2, "name": "Bob"}],
                "posts": [{"id": 11, "title": "World", "user_id": 2}],
            },
        ]

        streamed = (
            stream(chunks)
            .goto("users")
            .each()
            .map_to(
                table=StreamUser,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .goto_root()
            .goto("posts")
            .each()
            .map_to(
                table=StreamPost,
                fields=[
                    Field("title", get("title")),
                    TempField("id", get("id")),
                    TempField("user_id", get("user_id")),
                ],
            )
            .link_to(StreamUser, by={"user_id": "id"})
            .load(session2)
            .run()
        )
        session2.commit()

        assert resident.stats["stream_users"].inserted == 2
        assert resident.stats["stream_posts"].inserted == 2
        assert streamed.stats["stream_users"].inserted == 2
        assert streamed.stats["stream_posts"].inserted == 2
        assert session2.query(StreamUser).count() == 2
        assert session2.query(StreamPost).count() == 2
        session2.close()


class TestAutoKeySafety:
    def test_auto_keys_do_not_collide_in_chunk(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        records = [
            {"users": [{"name": "a"}]},
            {"users": [{"name": "b"}]},
            {"users": [{"name": "c"}]},
        ]

        result = (
            stream(
                CallableChunkSource(
                    lambda: iter([Chunk(roots=tuple(records), sequential=True)])
                )
            )
            .goto("users")
            .each()
            .map_to(table=StreamUser, fields=[Field("name", get("name"))])
            .load(session)
            .run()
        )
        session.commit()

        assert result.stats["stream_users"].mapped == 3
        assert result.stats["stream_users"].inserted == 3
        assert session.query(StreamUser).count() == 3
        session.close()


class TestStatsAggregation:
    def test_cumulative_stats_across_chunks(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        chunks = [
            {"users": [{"id": 1, "name": "A"}]},
            {"users": [{"id": 2, "name": "B"}]},
            {"users": [{"id": 3, "name": "C"}]},
        ]

        result = (
            stream(chunks)
            .goto("users")
            .each()
            .map_to(
                table=StreamUser,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .load(session)
            .run()
        )
        session.commit()

        assert result.stats["stream_users"].mapped == 3
        assert result.stats["stream_users"].inserted == 3
        session.close()


class TestWithinChunkMerge:
    def test_merge_policy_within_single_chunk_root(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        chunk_root = {
            "sales": [
                {"product": "Widget", "amount": 100},
                {"product": "Widget", "amount": 50},
            ]
        }

        (
            stream([chunk_root])
            .goto("sales")
            .each()
            .map_to(
                table=StreamSale,
                join_on=["product"],
                fields=[
                    Field("product", get("product")),
                    Field("total", get("amount"), merge=AddPolicy()),
                    TempField("id", get("amount")),
                ],
            )
            .load(session)
            .run()
        )
        session.commit()

        rows = session.query(StreamSale).all()
        assert len(rows) == 1
        assert rows[0].total == 150
        session.close()


class TestRelationshipCompleteness:
    def test_incomplete_chunk_raises(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        with pytest.raises(RelationshipIncompleteError, match="relationship-complete"):
            (
                stream([{"posts": [{"id": 1, "title": "x", "user_id": 99}]}])
                .goto("posts")
                .each()
                .map_to(
                    table=StreamPost,
                    fields=[
                        Field("title", get("title")),
                        TempField("id", get("id")),
                        TempField("user_id", get("user_id")),
                    ],
                )
                .link_to(StreamUser, by={"user_id": "id"})
                .load(session)
                .run()
            )
        session.close()

    def test_complete_chunk_succeeds(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        chunk = {
            "users": [{"id": 1, "name": "Alice"}],
            "posts": [{"id": 10, "title": "Hi", "user_id": 1}],
        }

        result = (
            stream([chunk])
            .goto("users")
            .each()
            .map_to(
                table=StreamUser,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .goto_root()
            .goto("posts")
            .each()
            .map_to(
                table=StreamPost,
                fields=[
                    Field("title", get("title")),
                    TempField("id", get("id")),
                    TempField("user_id", get("user_id")),
                ],
            )
            .link_to(StreamUser, by={"user_id": "id"})
            .load(session)
            .run()
        )
        session.commit()
        assert result.stats["stream_posts"].inserted == 1
        session.close()


class TestLoadEagerStreaming:
    def test_eager_parent_shared_across_chunks(self):
        eager = {"tags": [{"id": 1, "name": "t1"}, {"id": 2, "name": "t2"}]}
        chunks = [
            {"items": [{"id": 1, "name": "i0", "tag_id": 1}]},
            {"items": [{"id": 2, "name": "i1", "tag_id": 2}]},
        ]

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        (
            stream(chunks, eager_roots=eager)
            .goto("tags")
            .each()
            .map_to(
                table=StreamTag,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .load_eager(StreamTag)
            .goto_root()
            .goto("items")
            .each()
            .map_to(
                table=StreamItem,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                    TempField("tag_id", get("tag_id")),
                ],
            )
            .link_to(StreamTag, by={"tag_id": "id"})
            .load(session)
            .run()
        )
        session.commit()
        assert session.query(StreamTag).count() == 2
        assert session.query(StreamItem).count() == 2
        session.close()


class TestMultiRootChunk:
    def test_indexed_multi_root_chunk_merge(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        users_root = {"users": [{"id": 1, "name": "Alice"}]}
        profiles_root = {"profiles": [{"user_id": 1, "name_suffix": " Smith"}]}
        source = CallableChunkSource(
            lambda: iter([Chunk(roots=(users_root, profiles_root), sequential=False)])
        )

        result = (
            stream(source)
            .goto("users")
            .each()
            .map_to(
                table=StreamUser,
                join_on=["id"],
                fields=[
                    Field("id", get("id")),
                    Field("name", get("name")),
                ],
            )
            .goto_root(1)
            .goto("profiles")
            .each()
            .map_to(
                table=StreamUser,
                join_on=["id"],
                fields=[
                    Field("name", get("name_suffix")),
                    TempField("id", get("user_id")),
                ],
            )
            .load(session)
            .run()
        )
        session.commit()

        rows = session.query(StreamUser).all()
        assert len(rows) == 1
        assert result.stats["stream_users"].mapped == 1
        assert result.stats["stream_users"].inserted == 1
        session.close()


class TestFlushStrategySeam:
    def test_strategy_receives_bind_context(self):
        seen: list[FlushContext] = []

        class RecordingStrategy(KeyCompleteFlushStrategy):
            def flush(self, ctx: FlushContext) -> None:
                seen.append(ctx)

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        (
            stream([{"users": [{"id": 1, "name": "A"}]}], flush_strategy=RecordingStrategy())
            .goto("users")
            .each()
            .map_to(
                table=StreamUser,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .load(session)
            .run()
        )

        assert len(seen) >= 1
        ctx = seen[0]
        assert "stream_users" in ctx.scope_tables
        assert "stream_users" in ctx.bind_context
        assert ctx.session is session
        assert ctx.is_supabase is False
        session.close()

    def test_custom_strategy_persists_via_public_fields(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        class PublicFieldStrategy:
            """Persists using only public FlushContext fields, no builder internals."""

            def flush(self, ctx: FlushContext) -> None:
                from etielle.utils import topological_sort

                for table in topological_sort(ctx.dep_graph, ctx.scope_tables):
                    result = ctx.local_results.get(table)
                    if result is None:
                        continue
                    for instance in result.instances.values():
                        if not isinstance(instance, dict):
                            ctx.session.add(instance)
                ctx.session.flush()

        (
            stream(
                [{"users": [{"id": 1, "name": "A"}]}],
                flush_strategy=PublicFieldStrategy(),
            )
            .goto("users")
            .each()
            .map_to(
                table=StreamUser,
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .load(session)
            .run()
        )
        session.commit()

        assert session.query(StreamUser).count() == 1
        session.close()


class TestStreamingValidation:
    def test_rejects_composite_by_at_build_time(self):
        builder = (
            stream([{}])
            .goto("children")
            .each()
            .map_to(table="children", fields=[TempField("a", get("a")), TempField("b", get("b"))])
            .link_to("parents", by={"a": "x", "b": "y"})
            .load(MagicMock())
        )
        with pytest.raises(ValueError, match="single-field by"):
            builder.run()

    def test_one_record_per_chunk_source(self):
        source = OneRecordPerChunkSource([{"x": 1}, {"x": 2}])
        chunks = list(source.chunks())
        assert len(chunks) == 2
        assert chunks[0].roots == ({"x": 1},)
        assert chunks[0].sequential is True

    def test_sequential_source_rejects_multi_root_pipeline_statically(self):
        builder = (
            stream([{"users": []}, {"profiles": []}])
            .goto("users")
            .each()
            .map_to(table="users", join_on=["id"], fields=[Field("id", get("id"))])
            .goto_root(1)
            .goto("profiles")
            .each()
            .map_to(table="users", join_on=["id"], fields=[TempField("id", get("uid"))])
            .load(MagicMock())
        )
        with pytest.raises(ValueError, match="requires multi-root chunks"):
            builder.run()

    def test_sequential_chunk_with_multi_root_emissions_raises_at_runtime(self):
        source = CallableChunkSource(
            lambda: iter([Chunk(roots=({"users": []},), sequential=True)])
        )
        builder = (
            stream(source)
            .goto("users")
            .each()
            .map_to(table="users", join_on=["id"], fields=[Field("id", get("id"))])
            .goto_root(1)
            .goto("profiles")
            .each()
            .map_to(table="users", join_on=["id"], fields=[TempField("id", get("uid"))])
            .load(MagicMock())
        )
        with pytest.raises(ValueError, match="support only a single root"):
            builder.run()

    def test_multi_root_chunk_missing_root_raises(self):
        source = CallableChunkSource(
            lambda: iter([Chunk(roots=({"users": [{"id": "u1"}]},), sequential=False)])
        )
        builder = (
            stream(source)
            .goto("users")
            .each()
            .map_to(table="users", join_on=["id"], fields=[Field("id", get("id"))])
            .goto_root(1)
            .goto("profiles")
            .each()
            .map_to(table="users", join_on=["id"], fields=[TempField("id", get("uid"))])
            .load(sessionmaker(bind=create_engine("sqlite:///:memory:"))())
        )
        with pytest.raises(ValueError, match="references goto_root\\(1\\)"):
            builder.run()
