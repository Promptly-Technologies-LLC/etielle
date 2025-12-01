"""Tests for fluent API with SQLAlchemy integration."""

import pytest
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from etielle.fluent import etl, Field, TempField
from etielle.transforms import get, key

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


class TestDictIteration:
    """Tests for iterating over dict items with .goto().each()."""

    def test_dict_iteration_creates_instances(self, session):
        """Dict iteration (.goto("dict").each()) should create instances for each item.

        Regression test: After singleton fix, dict iteration stopped working.
        """
        data = {
            "questions": {
                "Q1": {"text": "Question 1"},
                "Q2": {"text": "Question 2"},
                "Q3": {"text": "Question 3"}
            }
        }

        result = (
            etl(data)
            .goto("questions").each()
            .map_to(table=User, fields=[
                Field("name", get("text")),
                TempField("qid", key()),  # Use the dict key
            ])
            .load(session)
            .run()
        )

        session.commit()

        # Verify instances were created
        users = session.query(User).all()
        assert len(users) == 3, f"Expected 3 users, got {len(users)}"
        names = {u.name for u in users}
        assert names == {"Question 1", "Question 2", "Question 3"}

    def test_dict_iteration_without_tempfield_or_join_on(self, session):
        """Dict iteration should work without explicit TempField or join_on.

        Bug: Without TempField or join_on, default join key was literal(None),
        which caused executor to skip all rows.

        Expected: For iteration, default join key should be key() (dict key or list index).
        """
        data = {
            "questions": {
                "Q1": {"text": "Question 1"},
                "Q2": {"text": "Question 2"},
            }
        }

        result = (
            etl(data)
            .goto("questions").each()
            .map_to(table=User, fields=[
                Field("name", get("text")),
                # NO TempField, NO join_on - should still work!
            ])
            .load(session)
            .run()
        )

        session.commit()

        # Verify instances were created
        users = session.query(User).all()
        assert len(users) == 2, f"Expected 2 users, got {len(users)}"
        names = {u.name for u in users}
        assert names == {"Question 1", "Question 2"}

    def test_no_join_key_creates_list_of_instances(self, session):
        """Without join_on, instances should be stored in a list, not keyed dict.

        This means duplicate values should create separate instances (no deduplication).
        Previously, the code used key() as default join key, which caused deduplication.
        Now, with no default join key, each iteration creates a distinct instance.
        """
        data = {
            "items": [
                {"name": "Item1"},
                {"name": "Item2"},
                {"name": "Item1"},  # Duplicate name - should create 3 instances, not 2
            ]
        }

        result = (
            etl(data)
            .goto("items").each()
            .map_to(table=User, fields=[
                Field("name", get("name")),
            ])
            .load(session)
            .run()
        )

        session.commit()

        # Should have 3 instances (no deduplication without join_on)
        users = session.query(User).all()
        assert len(users) == 3, f"Expected 3 users (no deduplication), got {len(users)}"
        assert [u.name for u in users].count("Item1") == 2


class TestJoinOnFieldPersistence:
    """Tests that join_on fields are persisted to the database.

    Prior to this fix, join_on excluded fields from output, requiring
    users to create redundant TempFields for fields that needed to be
    both persisted AND used as join keys.
    """

    def test_join_on_field_is_persisted(self, session):
        """Fields in join_on should be persisted, not excluded from output.

        Bug: join_on previously excluded fields from output, meaning:
        - Field("external_id", ...) with join_on=["external_id"]
        - Would NOT persist external_id to the database
        - User had to duplicate: Field("external_id", ...) + TempField("ext_id", ...)

        Expected: join_on fields should be BOTH persisted AND used as join keys.
        """
        data = {
            "questions": {
                "Q1": {"text": "Question 1"},
                "Q2": {"text": "Question 2"},
            }
        }

        result = (
            etl(data)
            .goto("questions").each()
            .map_to(table=User, fields=[
                Field("name", key()),  # Use dict key as both name AND join key
            ], join_on=["name"])
            .load(session)
            .run()
        )

        session.commit()

        # Verify instances were created with name field persisted
        users = session.query(User).all()
        assert len(users) == 2, f"Expected 2 users, got {len(users)}"
        names = {u.name for u in users}
        assert names == {"Q1", "Q2"}, f"Expected names Q1, Q2 but got {names}"


class TestSingletonMapping:
    """Tests for mapping a single root object (no iteration).

    This tests the case where map_to is called directly on the root
    without .each(), creating a single instance.
    """

    def test_singleton_mapping_persists_to_database(self, session):
        """map_to on root without .each() should persist a single instance."""
        data = {
            "name": "Alice",
            "email": "alice@example.com"
        }

        result = (
            etl(data)
            .map_to(table=User, fields=[
                Field("name", get("name")),
            ])
            .load(session)
            .run()
        )

        session.commit()

        # Verify instance was created and persisted
        users = session.query(User).all()
        assert len(users) == 1
        assert users[0].name == "Alice"

    def test_singleton_mapping_uses_auto_key(self, session):
        """Root-level mapping without join_on should use auto-generated key."""
        data = {"name": "Bob"}

        result = (
            etl(data)
            .map_to(table=User, fields=[
                Field("name", get("name")),
            ])
            .run()
        )

        # Check that an instance was created (with auto-generated key)
        users_dict = result.tables["users"]
        assert len(users_dict) == 1
        # The key should be auto-generated, not __singleton__
        user = next(iter(users_dict.values()))
        assert user.name == "Bob"

    def test_singleton_parent_can_be_linked_by_children(self, session):
        """Children should be able to link to singleton parents via natural keys.

        This tests the case where:
        - Parent is mapped at ROOT level with NO TempField (uses __singleton__ key)
        - Children link using a field value that matches parent's Field value
        - The FK should be properly set despite key mismatch

        Bug scenario:
        - User singleton stored with key ("__singleton__",) because no TempField
        - User has Field "name" = "Alice" (regular field, not join key)
        - Post has TempField "author_name" = "Alice"
        - link_to(User, by={"author_name": "name"}) tries to find parent by name
        - Lookup fails: parents indexed by ("__singleton__",), not ("Alice",)
        """
        data = {
            "name": "Alice",
            "posts": [
                {"title": "Post 1", "author": "Alice"},
                {"title": "Post 2", "author": "Alice"}
            ]
        }

        result = (
            etl(data)
            # ROOT-LEVEL singleton user mapping - NO TempField!
            # This means it gets __singleton__ as join key
            .map_to(table=User, fields=[
                Field("name", get("name")),
            ])
            # Posts with linking - try to link by matching name
            .goto("posts").each()
            .map_to(table=Post, fields=[
                Field("title", get("title")),
                TempField("post_id", get("title")),
                TempField("author_name", get("author")),
            ])
            .link_to(User, by={"author_name": "name"})
            .load(session)
            .run()
        )

        session.commit()

        # Verify user was created
        users = session.query(User).all()
        assert len(users) == 1
        alice = users[0]
        assert alice.name == "Alice"

        # Verify posts were created AND linked to user
        posts = session.query(Post).all()
        assert len(posts) == 2
        for post in posts:
            assert post.user_id == alice.id, f"Post '{post.title}' not linked to user"
            assert post.user == alice
