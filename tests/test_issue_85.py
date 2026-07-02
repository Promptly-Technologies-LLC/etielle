"""Tests for issue #85: UpsertFlushStrategy(update) parent duplication."""

from __future__ import annotations

from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from etielle import Field, TempField, get, stream
from etielle.chunking import UpsertFlushStrategy

Base = declarative_base()


class Order(Base):
    __tablename__ = "orders_85"
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer = Column(String)


class LineItem(Base):
    __tablename__ = "line_items_85"
    id = Column(Integer, primary_key=True, autoincrement=True)
    sku = Column(String)
    order_id = Column(Integer, ForeignKey("orders_85.id"))
    orders_85 = relationship("Order")


class Tag(Base):
    __tablename__ = "tags_85"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)


class Item(Base):
    __tablename__ = "items_85"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    tag_id = Column(Integer, ForeignKey("tags_85.id"))
    tags_85 = relationship("Tag")


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


class TestUpsertFlushStrategyParentDuplication:
    def test_tempfield_parent_with_child_in_same_chunk(self):
        session = _session()
        chunks = [
            {
                "orders": [{"id": 1, "customer": "Alice"}],
                "items": [{"id": 10, "sku": "x", "order_id": 1}],
            }
        ]

        (
            stream(chunks, flush_strategy=UpsertFlushStrategy())
            .goto("orders")
            .each()
            .map_to(
                table=Order,
                fields=[
                    Field("customer", get("customer")),
                    TempField("id", get("id")),
                ],
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

        assert session.query(Order).count() == 1
        item = session.query(LineItem).one()
        assert item.order_id == 1
        session.close()

    def test_load_eager_parent_not_duplicated_across_chunks(self):
        session = _session()
        eager = {"tags": [{"id": 1, "name": "t1"}]}
        chunks = [
            {"items": [{"id": 1, "name": "i0", "tag_id": 1}]},
            {"items": [{"id": 2, "name": "i1", "tag_id": 1}]},
        ]

        (
            stream(chunks, eager_roots=eager, flush_strategy=UpsertFlushStrategy())
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
            .goto("items")
            .each()
            .map_to(
                table=Item,
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

        assert session.query(Tag).count() == 1
        items = session.query(Item).order_by(Item.name).all()
        assert len(items) == 2
        assert all(i.tag_id == 1 for i in items)
        session.close()
