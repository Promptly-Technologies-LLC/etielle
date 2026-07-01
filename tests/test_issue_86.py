"""Tests for issue #86: fk= on link_to() must fail for SQLAlchemy without load_eager()."""

from __future__ import annotations

import pytest
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from etielle import Field, TempField, etl, get


Base = declarative_base()


class Parent(Base):
    __tablename__ = "ps_86"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Child(Base):
    __tablename__ = "cs_86"
    id = Column(Integer, primary_key=True)
    p_id = Column(Integer, ForeignKey("ps_86.id"))
    p = relationship("Parent")


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _pipeline_with_fk(*, session=None, eager_parent: bool = False):
    builder = (
        etl({"ps": [{"id": 1, "name": "a"}], "cs": [{"pid": 1}]})
        .goto("ps")
        .each()
        .map_to(
            table=Parent,
            fields=[Field("name", get("name")), TempField("id", get("id"))],
        )
        .goto_root()
        .goto("cs")
        .each()
        .map_to(table=Child, fields=[TempField("pid", get("pid"))])
        .link_to(Parent, by={"pid": "id"}, fk={"p_id": "id"})
    )
    if eager_parent:
        builder = builder.load_eager(Parent)
    if session is not None:
        builder = builder.load(session)
    return builder


class TestIssue86FkValidation:
    def test_fk_on_link_to_raises_for_sqlalchemy_without_load_eager(self):
        """fk= must be rejected before eager phase when no load_eager() is used."""
        with pytest.raises(ValueError, match="fk parameter on link_to.*Supabase"):
            _pipeline_with_fk(session=_session()).run()

    def test_fk_on_link_to_raises_for_sqlalchemy_with_load_eager(self):
        """fk= is rejected for SQLAlchemy even when load_eager() is configured."""
        with pytest.raises(ValueError, match="fk parameter on link_to.*Supabase"):
            _pipeline_with_fk(session=_session(), eager_parent=True).run()

    def test_fk_on_link_to_without_load_does_not_raise(self):
        """Without load(), adapter type is unknown so fk= is not validated."""
        result = _pipeline_with_fk().run()
        assert "cs_86" in result.tables
