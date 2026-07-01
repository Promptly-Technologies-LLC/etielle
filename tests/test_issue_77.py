"""Tests for issue #77: additional ``ChunkSource``/``FlushStrategy`` implementations.

Covers the disk-backed ``ExternalPartitionChunkSource`` (H2), the
``UpsertFlushStrategy`` (on-conflict update/skip), and the
``BufferedKeyFlushStrategy`` (bounded cross-chunk key merge).
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from etielle import Field, TempField, get, stream
from etielle.chunking import (
    BufferedKeyFlushStrategy,
    ChunkSource,
    ExternalPartitionChunkSource,
    FlushContext,
    UpsertFlushStrategy,
)


Base = declarative_base()


class Order(Base):
    __tablename__ = "orders_77"
    id = Column(Integer, primary_key=True)
    customer = Column(String)


class LineItem(Base):
    __tablename__ = "line_items_77"
    id = Column(Integer, primary_key=True)
    sku = Column(String)
    order_id = Column(Integer, ForeignKey("orders_77.id"))
    # link_to infers the relationship attribute from the parent table name via
    # ``rstrip("s")``; "orders_77" has no trailing 's', so the attr is "orders_77".
    orders_77 = relationship("Order")


class User(Base):
    __tablename__ = "users_77"
    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True)
    name = Column(String)
    phone = Column(String)


class Event(Base):
    __tablename__ = "events_77"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ext_id = Column(String)
    payload = Column(String)


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _supabase_ctx(strategy_session: object) -> FlushContext:
    return FlushContext(
        scope_tables={"t"},
        bind_context={},
        local_results={},
        dep_graph={},
        link_to_rels=[],
        backlink_rels=[],
        stats={},
        on_event=None,
        session=strategy_session,
        is_supabase=True,
        builder=None,  # type: ignore[arg-type]
    )


class TestExternalPartitionChunkSource:
    def test_is_chunk_source(self):
        source = ExternalPartitionChunkSource([], key=lambda r: r)
        assert isinstance(source, ChunkSource)

    def test_partitions_unsorted_input_by_key(self):
        records = [
            {"k": 1, "v": "a"},
            {"k": 2, "v": "b"},
            {"k": 1, "v": "c"},
            {"k": 3, "v": "d"},
            {"k": 2, "v": "e"},
        ]
        source = ExternalPartitionChunkSource(records, key=lambda r: r["k"])
        chunks = list(source.chunks())

        # One chunk per distinct key, in first-appearance order.
        assert [c.roots for c in chunks] == [
            ({"k": 1, "v": "a"}, {"k": 1, "v": "c"}),
            ({"k": 2, "v": "b"}, {"k": 2, "v": "e"}),
            ({"k": 3, "v": "d"},),
        ]
        assert all(c.sequential for c in chunks)

    def test_empty_input_yields_no_chunks(self):
        assert list(ExternalPartitionChunkSource([], key=lambda r: r).chunks()) == []

    def test_records_are_reconstructed_copies(self):
        record = {"k": 1, "nested": {"a": [1, 2]}}
        chunks = list(
            ExternalPartitionChunkSource([record], key=lambda r: r["k"]).chunks()
        )
        assert chunks[0].roots[0] == record
        assert chunks[0].roots[0] is not record

    def test_custom_dumps_loads(self):
        records = ["x:1", "x:2", "y:3"]
        source = ExternalPartitionChunkSource(
            records,
            key=lambda r: r.split(":")[0],
            dumps=lambda r: r,
            loads=lambda s: s,
        )
        chunks = list(source.chunks())
        assert [c.roots for c in chunks] == [("x:1", "x:2"), ("y:3",)]

    def test_spill_file_cleaned_up(self, tmp_path):
        records = [{"k": i % 2, "i": i} for i in range(10)]
        source = ExternalPartitionChunkSource(
            records, key=lambda r: r["k"], dir=str(tmp_path)
        )
        list(source.chunks())
        assert list(tmp_path.iterdir()) == []

    def test_spill_file_cleaned_up_on_early_close(self, tmp_path):
        records = [{"k": i, "i": i} for i in range(5)]
        source = ExternalPartitionChunkSource(
            records, key=lambda r: r["k"], dir=str(tmp_path)
        )
        chunk_iter = source.chunks()
        next(chunk_iter)
        chunk_iter.close()
        assert list(tmp_path.iterdir()) == []

    def test_streaming_partitions_relationship_complete_chunks(self):
        session = _session()
        # Records for the same order are interleaved with other orders: the
        # consecutive-only GroupByChunkSource would split them, but the
        # external partitioner reunites them into one chunk per order.
        records = [
            {
                "orders": [{"id": 1, "customer": "Alice"}],
                "items": [{"id": 10, "sku": "x", "order_id": 1}],
            },
            {
                "orders": [{"id": 2, "customer": "Bob"}],
                "items": [{"id": 20, "sku": "z", "order_id": 2}],
            },
            {
                "orders": [{"id": 1, "customer": "Alice"}],
                "items": [{"id": 11, "sku": "y", "order_id": 1}],
            },
        ]
        source = ExternalPartitionChunkSource(
            records, key=lambda r: r["orders"][0]["id"]
        )

        (
            stream(source)
            .goto("orders")
            .each()
            .map_to(
                table=Order,
                join_on=["id"],
                fields=[Field("id", get("id")), Field("customer", get("customer"))],
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
        items = session.query(LineItem).order_by(LineItem.sku).all()
        assert [i.order_id for i in items] == [1, 1, 2]
        session.close()


class TestUpsertFlushStrategy:
    def _run_users(self, session, records, strategy):
        return (
            stream(records, flush_strategy=strategy)
            .goto("users")
            .each()
            .map_to(
                table=User,
                join_on=["id"],
                fields=[
                    Field("id", get("id")),
                    Field("email", get("email")),
                    Field("name", get("name")),
                ],
            )
            .load(session)
            .run()
        )

    def test_invalid_on_conflict_rejected(self):
        with pytest.raises(ValueError, match="on_conflict"):
            UpsertFlushStrategy(on_conflict="explode")  # type: ignore[arg-type]

    def test_supabase_session_rejected(self):
        with pytest.raises(ValueError, match="SQLAlchemy"):
            UpsertFlushStrategy().flush(_supabase_ctx(object()))

    def test_update_mode_inserts_new_rows(self):
        session = _session()
        result = self._run_users(
            session,
            [{"users": [{"id": 1, "email": "a@x.com", "name": "Alice"}]}],
            UpsertFlushStrategy(),
        )
        session.commit()
        assert session.query(User).count() == 1
        assert result.stats["users_77"].inserted == 1
        session.close()

    def test_update_mode_overwrites_existing_row(self):
        session = _session()
        self._run_users(
            session,
            [{"users": [{"id": 1, "email": "a@x.com", "name": "Alice"}]}],
            UpsertFlushStrategy(),
        )
        session.commit()

        # Re-run with a changed name: same primary key, so the row is updated
        # in place instead of raising IntegrityError.
        self._run_users(
            session,
            [{"users": [{"id": 1, "email": "a@x.com", "name": "Alicia"}]}],
            UpsertFlushStrategy(),
        )
        session.commit()

        rows = session.query(User).all()
        assert len(rows) == 1
        assert rows[0].name == "Alicia"
        session.close()

    def test_update_mode_emits_upsert_telemetry(self):
        from etielle.telemetry import FlushCompleted

        session = _session()
        events = []
        (
            stream(
                [{"users": [{"id": 1, "email": "a@x.com", "name": "A"}]}],
                flush_strategy=UpsertFlushStrategy(),
            )
            .goto("users")
            .each()
            .map_to(
                table=User,
                join_on=["id"],
                fields=[
                    Field("id", get("id")),
                    Field("email", get("email")),
                    Field("name", get("name")),
                ],
            )
            .load(session)
            .run(on_event=events.append)
        )
        completed = [e for e in events if isinstance(e, FlushCompleted)]
        assert completed and all(e.upsert for e in completed)
        session.close()

    def test_skip_mode_skips_conflicting_rows_and_keeps_rest(self):
        session = _session()
        session.add(User(id=99, email="taken@x.com", name="Stored"))
        session.commit()

        result = self._run_users(
            session,
            [
                # Conflicts on the unique email despite the fresh primary key.
                {"users": [{"id": 1, "email": "taken@x.com", "name": "Dup"}]},
                {"users": [{"id": 2, "email": "new@x.com", "name": "New"}]},
            ],
            UpsertFlushStrategy(on_conflict="skip"),
        )
        session.commit()

        emails = {u.email: u.name for u in session.query(User).all()}
        assert emails == {"taken@x.com": "Stored", "new@x.com": "New"}
        assert result.stats["users_77"].mapped == 2
        assert result.stats["users_77"].inserted == 1
        session.close()

    def test_skip_mode_cascades_to_children_of_skipped_parent(self):
        session = _session()
        session.add(Order(id=1, customer="Stored"))
        session.commit()

        (
            stream(
                [
                    {
                        "orders": [{"id": 1, "customer": "Dup"}],
                        "items": [{"id": 10, "sku": "x", "order_id": 1}],
                    }
                ],
                flush_strategy=UpsertFlushStrategy(on_conflict="skip"),
            )
            .goto("orders")
            .each()
            .map_to(
                table=Order,
                join_on=["id"],
                fields=[Field("id", get("id")), Field("customer", get("customer"))],
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

        # The child is bound to the conflicting in-memory parent, so its
        # SAVEPOINT reproduces the conflict via cascade and the child is
        # skipped along with the duplicate parent.
        assert session.query(Order).count() == 1
        assert session.query(Order).one().customer == "Stored"
        assert session.query(LineItem).count() == 0
        session.close()


class TestBufferedKeyFlushStrategy:
    def test_invalid_max_keys_rejected(self):
        with pytest.raises(ValueError, match="max_keys"):
            BufferedKeyFlushStrategy(max_keys=0)

    def test_supabase_session_rejected(self):
        with pytest.raises(ValueError, match="SQLAlchemy"):
            BufferedKeyFlushStrategy().flush(_supabase_ctx(object()))

    def test_late_arriving_row_merges_into_cached_key(self):
        session = _session()
        # The second chunk re-visits user 1 with a phone number but no name;
        # non-None values merge onto the already-flushed instance.
        chunks = [
            {"users": [{"id": 1, "email": "a@x.com", "name": "Alice"}]},
            {"users": [{"id": 1, "phone": "555-0100"}]},
        ]
        result = (
            stream(chunks, flush_strategy=BufferedKeyFlushStrategy())
            .goto("users")
            .each()
            .map_to(
                table=User,
                join_on=["id"],
                fields=[
                    Field("id", get("id")),
                    Field("email", get("email")),
                    Field("name", get("name")),
                    Field("phone", get("phone")),
                ],
            )
            .load(session)
            .run()
        )
        session.commit()

        rows = session.query(User).all()
        assert len(rows) == 1
        assert rows[0].name == "Alice"
        assert rows[0].phone == "555-0100"
        assert result.stats["users_77"].mapped == 2
        assert result.stats["users_77"].inserted == 1
        session.close()

    def test_evicted_key_inserts_new_row(self):
        session = _session()
        chunks = [
            {"events": [{"ext_id": "a", "payload": "1"}]},
            {"events": [{"ext_id": "b", "payload": "2"}]},
            # "a" was evicted by "b" (max_keys=1), so it inserts a new row
            # instead of merging: correctness is bounded by cache size.
            {"events": [{"ext_id": "a", "payload": "3"}]},
        ]
        (
            stream(chunks, flush_strategy=BufferedKeyFlushStrategy(max_keys=1))
            .goto("events")
            .each()
            .map_to(
                table=Event,
                join_on=["ext_id"],
                fields=[
                    Field("ext_id", get("ext_id")),
                    Field("payload", get("payload")),
                ],
            )
            .load(session)
            .run()
        )
        session.commit()

        assert session.query(Event).count() == 3
        session.close()

    def test_auto_keyed_rows_always_insert(self):
        session = _session()
        chunks = [
            {"events": [{"ext_id": "a", "payload": "1"}]},
            {"events": [{"ext_id": "a", "payload": "1"}]},
        ]
        (
            stream(chunks, flush_strategy=BufferedKeyFlushStrategy())
            .goto("events")
            .each()
            .map_to(
                # No join_on: auto keys restart per chunk, so they never merge.
                table=Event,
                fields=[
                    Field("ext_id", get("ext_id")),
                    Field("payload", get("payload")),
                ],
            )
            .load(session)
            .run()
        )
        session.commit()

        assert session.query(Event).count() == 2
        session.close()

    def test_reappearing_parent_relinks_children_without_duplicate_insert(self):
        session = _session()
        chunks = [
            {
                "orders": [{"id": 1, "customer": "Alice"}],
                "items": [{"id": 10, "sku": "x", "order_id": 1}],
            },
            {
                "orders": [{"id": 1, "customer": "Alice"}],
                "items": [{"id": 11, "sku": "y", "order_id": 1}],
            },
        ]
        (
            stream(chunks, flush_strategy=BufferedKeyFlushStrategy())
            .goto("orders")
            .each()
            .map_to(
                table=Order,
                join_on=["id"],
                fields=[Field("id", get("id")), Field("customer", get("customer"))],
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
        items = session.query(LineItem).order_by(LineItem.sku).all()
        assert [i.order_id for i in items] == [1, 1]
        session.close()


def test_new_types_exported_from_package():
    import etielle

    assert etielle.ExternalPartitionChunkSource is ExternalPartitionChunkSource
    assert etielle.UpsertFlushStrategy is UpsertFlushStrategy
    assert etielle.BufferedKeyFlushStrategy is BufferedKeyFlushStrategy
