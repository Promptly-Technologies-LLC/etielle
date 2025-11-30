"""Tests for fluent API with SQLAlchemy integration."""

import pytest
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from etielle.fluent import etl, Field, TempField
from etielle.transforms import get, get_from_parent

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    posts = relationship("Post", back_populates="user")


class Post(Base):
    __tablename__ = "posts"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    user_id = Column(Integer, ForeignKey("users.id"))
    user = relationship("User", back_populates="posts")


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


class TestFluentSQLAlchemy:
    """Tests for fluent API with SQLAlchemy."""

    def test_load_persists_to_database(self, session):
        """load().run() persists instances to database."""
        data = {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ]
        }

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table=User, fields=[
                Field("name", get("name")),
                TempField("id", get("id"))
            ])
            .load(session)
            .run()
        )

        session.commit()

        # Query from database
        users = session.query(User).all()
        assert len(users) == 2
        assert {u.name for u in users} == {"Alice", "Bob"}

    def test_load_binds_relationships(self, session):
        """load().run() binds ORM relationships."""
        data = {
            "users": [{"id": 1, "name": "Alice"}],
            "posts": [
                {"id": 101, "title": "Hello", "user_id": 1},
                {"id": 102, "title": "World", "user_id": 1}
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
                TempField("id", get("id")),
                TempField("user_id", get("user_id"))
            ])
            .link_to(User, by={"user_id": "id"})
            .load(session)
            .run()
        )

        session.commit()

        # Verify relationships
        alice = session.query(User).first()
        assert len(alice.posts) == 2
        assert {p.title for p in alice.posts} == {"Hello", "World"}



# Separate models with NOT NULL FK constraint for testing flush ordering
NotNullBase = declarative_base()


class Parent(NotNullBase):
    __tablename__ = "parents"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    children = relationship("Child", back_populates="parent")


class Child(NotNullBase):
    __tablename__ = "children"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    # NOT NULL FK - this is the key constraint that requires proper flush ordering
    parent_id = Column(Integer, ForeignKey("parents.id"), nullable=False)
    parent = relationship("Parent", back_populates="children")


class TestNotNullFKFlushOrdering:
    """Tests for proper flush ordering with NOT NULL FK constraints.

    When a child table has a NOT NULL FK constraint, etielle must:
    1. Add parent to session and flush (to get parent.id)
    2. Add child to session
    3. Bind relationship (child.parent = parent) so FK gets set
    4. Flush child

    If etielle adds all instances then flushes everything at once,
    the child insert fails because parent_id is NULL.
    """

    @pytest.fixture
    def not_null_session(self):
        engine = create_engine("sqlite:///:memory:")
        NotNullBase.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        yield session
        session.close()

    def test_not_null_fk_with_auto_pk(self, not_null_session):
        """NOT NULL FK with auto-generated PKs requires proper flush ordering.

        This test would fail if etielle added all instances to session first,
        then flushed everything at once - the child insert would fail because
        parent_id would be NULL (relationship not bound yet).
        """
        data = {
            "parents": [{"name": "Parent1"}],
            "children": [
                {"name": "Child1", "parent_ref": "parent1"},
                {"name": "Child2", "parent_ref": "parent1"}
            ]
        }

        result = (
            etl(data)
            .goto("parents").each()
            .map_to(table=Parent, fields=[
                Field("name", get("name")),
                TempField("ref", lambda ctx: ctx.node.get("name", "").lower())
            ])
            .goto_root()
            .goto("children").each()
            .map_to(table=Child, fields=[
                Field("name", get("name")),
                TempField("child_id", lambda ctx: ctx.node.get("name", "").lower()),
                TempField("parent_ref", get("parent_ref"))
            ])
            .link_to(Parent, by={"parent_ref": "ref"})
            .load(not_null_session)
            .run()
        )

        not_null_session.commit()

        # Verify parent was created with auto-generated ID
        parent = not_null_session.query(Parent).first()
        assert parent is not None
        assert parent.id == 1
        assert parent.name == "Parent1"

        # Verify children have correct FK set (not NULL)
        children = not_null_session.query(Child).all()
        assert len(children) == 2
        for child in children:
            assert child.parent_id == 1, f"Child {child.name} has NULL parent_id"
            assert child.parent == parent

        # Verify relationship from parent side
        assert len(parent.children) == 2
