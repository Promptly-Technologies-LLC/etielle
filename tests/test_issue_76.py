"""Tests for issue #76: mandatory chunking helpers.

Covers the single-pass streaming group-by ``GroupByChunkSource`` and the
pre-segmented passthrough ``PreSegmentedChunkSource``.
"""

from __future__ import annotations

from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from etielle import Field, TempField, get, stream
from etielle.chunking import (
    Chunk,
    ChunkSource,
    GroupByChunkSource,
    OneRecordPerChunkSource,
    PreSegmentedChunkSource,
)


Base = declarative_base()


class Order(Base):
    __tablename__ = "orders_76"
    id = Column(Integer, primary_key=True)
    customer = Column(String)


class LineItem(Base):
    __tablename__ = "line_items_76"
    id = Column(Integer, primary_key=True)
    sku = Column(String)
    order_id = Column(Integer, ForeignKey("orders_76.id"))
    # link_to infers the relationship attribute from the parent table name via
    # ``rstrip("s")``; "orders_76" has no trailing 's', so the attr is "orders_76".
    orders_76 = relationship("Order")


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


class TestGroupByChunkSource:
    def test_is_chunk_source(self):
        source = GroupByChunkSource([], key=lambda r: r)
        assert isinstance(source, ChunkSource)

    def test_groups_consecutive_records_by_key(self):
        records = [
            {"oid": 1, "v": "a"},
            {"oid": 1, "v": "b"},
            {"oid": 2, "v": "c"},
            {"oid": 3, "v": "d"},
            {"oid": 3, "v": "e"},
        ]
        source = GroupByChunkSource(records, key=lambda r: r["oid"])
        chunks = list(source.chunks())

        assert [c.roots for c in chunks] == [
            ({"oid": 1, "v": "a"}, {"oid": 1, "v": "b"}),
            ({"oid": 2, "v": "c"},),
            ({"oid": 3, "v": "d"}, {"oid": 3, "v": "e"}),
        ]
        assert all(c.sequential for c in chunks)

    def test_empty_input_yields_no_chunks(self):
        assert list(GroupByChunkSource([], key=lambda r: r).chunks()) == []

    def test_non_consecutive_same_key_splits_into_separate_chunks(self):
        # Documents the grouped-input requirement: unsorted input is grouped
        # only by consecutive runs, so key 1 lands in two separate chunks.
        records = [{"k": 1}, {"k": 2}, {"k": 1}]
        chunks = list(GroupByChunkSource(records, key=lambda r: r["k"]).chunks())
        assert [c.roots for c in chunks] == [
            ({"k": 1},),
            ({"k": 2},),
            ({"k": 1},),
        ]

    def test_single_pass_lazy_consumption(self):
        consumed: list[int] = []

        def gen():
            for i in [1, 1, 2]:
                consumed.append(i)
                yield {"k": i}

        chunk_iter = GroupByChunkSource(gen(), key=lambda r: r["k"]).chunks()

        # Pulling the first chunk only consumes up to the first key boundary
        # (the lookahead record that changes the key), not the whole input.
        first = next(chunk_iter)
        assert first.roots == ({"k": 1}, {"k": 1})
        assert consumed == [1, 1, 2]

        second = next(chunk_iter)
        assert second.roots == ({"k": 2},)

    def test_streaming_groups_relationship_complete_chunks(self):
        session = _session()
        # Each record is a parent subtree: an order plus its line items. The
        # group key is the owning order id -> a complete component root.
        records = [
            {
                "orders": [{"id": 1, "customer": "Alice"}],
                "items": [
                    {"id": 10, "sku": "x", "order_id": 1},
                    {"id": 11, "sku": "y", "order_id": 1},
                ],
            },
            {
                "orders": [{"id": 2, "customer": "Bob"}],
                "items": [{"id": 20, "sku": "z", "order_id": 2}],
            },
        ]
        source = GroupByChunkSource(records, key=lambda r: r["orders"][0]["id"])

        (
            stream(source)
            .goto("orders")
            .each()
            .map_to(
                table=Order,
                fields=[Field("customer", get("customer")), TempField("id", get("id"))],
            )
            .goto_root()
            .goto("items")
            .each()
            .map_to(
                table=LineItem,
                fields=[
                    Field("sku", get("sku")),
                    TempField("id", get("id")),
                    TempField("order_id", get("order_id")),
                ],
            )
            .link_to(Order, by={"order_id": "id"})
            .load(session)
            .run()
        )
        session.commit()

        assert session.query(Order).count() == 2
        assert session.query(LineItem).count() == 3
        items = session.query(LineItem).order_by(LineItem.id).all()
        assert [i.order_id for i in items] == [1, 1, 2]
        session.close()


class TestPreSegmentedChunkSource:
    def test_is_chunk_source(self):
        assert isinstance(PreSegmentedChunkSource([]), ChunkSource)

    def test_passes_chunks_through_unchanged(self):
        chunks = [
            Chunk(roots=({"a": 1},), sequential=True),
            Chunk(roots=({"b": 2}, {"c": 3}), sequential=False),
        ]
        out = list(PreSegmentedChunkSource(chunks).chunks())
        assert out == chunks

    def test_accepts_single_use_iterator(self):
        gen = (Chunk(roots=({"k": i},), sequential=True) for i in range(3))
        out = list(PreSegmentedChunkSource(gen).chunks())
        assert [c.roots for c in out] == [({"k": 0},), ({"k": 1},), ({"k": 2},)]

    def test_streaming_with_presegmented_source(self):
        session = _session()
        chunks = [
            Chunk(
                roots=(
                    {
                        "orders": [{"id": 1, "customer": "Alice"}],
                        "items": [{"id": 10, "sku": "x", "order_id": 1}],
                    },
                ),
                sequential=True,
            ),
            Chunk(
                roots=(
                    {
                        "orders": [{"id": 2, "customer": "Bob"}],
                        "items": [{"id": 20, "sku": "z", "order_id": 2}],
                    },
                ),
                sequential=True,
            ),
        ]

        (
            stream(PreSegmentedChunkSource(chunks))
            .goto("orders")
            .each()
            .map_to(
                table=Order,
                fields=[Field("customer", get("customer")), TempField("id", get("id"))],
            )
            .goto_root()
            .goto("items")
            .each()
            .map_to(
                table=LineItem,
                fields=[
                    Field("sku", get("sku")),
                    TempField("id", get("id")),
                    TempField("order_id", get("order_id")),
                ],
            )
            .link_to(Order, by={"order_id": "id"})
            .load(session)
            .run()
        )
        session.commit()

        assert session.query(Order).count() == 2
        assert session.query(LineItem).count() == 2
        session.close()


def test_helpers_exported_from_package():
    import etielle

    assert etielle.GroupByChunkSource is GroupByChunkSource
    assert etielle.PreSegmentedChunkSource is PreSegmentedChunkSource
    # Sanity: the pre-existing one-record helper is still the default wrapper.
    assert isinstance(OneRecordPerChunkSource([]), ChunkSource)
