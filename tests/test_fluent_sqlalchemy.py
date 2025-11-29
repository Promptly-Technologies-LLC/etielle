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
