"""Tests for issue #88: etl() accepts flush_strategy."""

from __future__ import annotations

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from etielle import Field, TempField, etl, get
from etielle.chunking import FlushContext, KeyCompleteFlushStrategy


Base = declarative_base()


class ResidentUser(Base):
    __tablename__ = "resident_users"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class TestEtlFlushStrategy:
    def test_etl_uses_custom_flush_strategy(self):
        seen: list[FlushContext] = []

        class RecordingStrategy(KeyCompleteFlushStrategy):
            def flush(self, ctx: FlushContext) -> None:
                seen.append(ctx)
                super().flush(ctx)

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        (
            etl({"users": [{"id": 1, "name": "A"}]}, flush_strategy=RecordingStrategy())
            .goto("users")
            .each()
            .map_to(
                table=ResidentUser,
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
        assert "resident_users" in ctx.scope_tables
        assert "resident_users" in ctx.bind_context
        assert ctx.session is session
        session.close()
