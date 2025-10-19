
from typing import Any, Dict

from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session

from etielle.core import MappingSpec, TraversalSpec
from etielle.transforms import get
from etielle.instances import InstanceEmit, FieldSpec, TypedDictBuilder
from etielle.adapters.sqlalchemy_adapter import bind_and_flush
from etielle.relationships import ManyToOneSpec


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    posts: Mapped[list["Post"]] = relationship(back_populates="user")


class Post(Base):
    __tablename__ = "posts"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str] = mapped_column(String)
    user_id: Mapped[str | None] = mapped_column(String, ForeignKey("users.id"), nullable=True)
    user: Mapped[User | None] = relationship(back_populates="posts")


def _user_factory(payload: Dict[str, Any]) -> User:
    return User(id=str(payload["id"]), name=str(payload.get("name", "")))


def _post_factory(payload: Dict[str, Any]) -> Post:
    return Post(id=str(payload["id"]), title=str(payload.get("title", "")))


def test_bind_and_flush_sqlalchemy():
    from sqlalchemy import create_engine

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    root = {
        "users": [
            {"id": "u1", "name": "Alice"},
            {"id": "u2", "name": "Bob"},
        ],
        "posts": [
            {"id": "p1", "title": "Hello", "user_id": "u1"},
            {"id": "p2", "title": "World", "user_id": "u2"},
        ],
    }

    users_emit = InstanceEmit[
        User
    ](
        table="users",
        join_keys=[get("id")],
        fields=[
            FieldSpec(selector="id", transform=get("id")),
            FieldSpec(selector="name", transform=get("name")),
        ],
        builder=TypedDictBuilder(_user_factory),
    )

    posts_emit = InstanceEmit[
        Post
    ](
        table="posts",
        join_keys=[get("id")],
        fields=[
            FieldSpec(selector="id", transform=get("id")),
            FieldSpec(selector="title", transform=get("title")),
        ],
        builder=TypedDictBuilder(_post_factory),
    )

    mapping = MappingSpec(
        traversals=[
            TraversalSpec(path=["users"], mode="auto", emits=[users_emit]),
            TraversalSpec(path=["posts"], mode="auto", emits=[posts_emit]),
        ]
    )

    rels = [
        ManyToOneSpec(
            child_table="posts",
            parent_table="users",
            attr="user",
            child_to_parent_key=[get("user_id")],
            required=True,
        )
    ]

    with Session(engine) as session:
        _ = bind_and_flush(session, root=root, mapping=mapping, relationships=rels)
        assert session.query(User).count() == 2
        assert session.query(Post).count() == 2
        p1 = session.get(Post, "p1")
        p2 = session.get(Post, "p2")
        assert p1 is not None and p1.user is not None and p1.user.id == "u1"
        assert p2 is not None and p2.user is not None and p2.user.id == "u2"
