from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Sequence

from sqlalchemy import event
from sqlmodel import Session as SQLModelSession

from ..core import MappingSpec
from ..executor import run_mapping
from ..relationships import ManyToOneSpec, bind_many_to_one, compute_relationship_keys


@contextmanager
def no_autoflush(session: SQLModelSession):
    prev = session.autoflush
    session.autoflush = False
    try:
        yield
    finally:
        session.autoflush = prev


def bind_and_flush(
    session: SQLModelSession,
    *,
    root: Any,
    mapping: MappingSpec,
    relationships: Sequence[ManyToOneSpec],
    add_all_instances: bool = True,
) -> dict[str, Any]:
    with no_autoflush(session):
        results = run_mapping(root, mapping)
        sidecar = compute_relationship_keys(root, mapping.traversals, relationships)
        bind_many_to_one(results, relationships, sidecar, fail_on_missing=True)
        if add_all_instances:
            for mr in results.values():
                for obj in mr.instances.values():
                    session.add(obj)
        session.flush()
    return results


def install_before_flush_binder(
    session: SQLModelSession,
    *,
    root: Any,
    mapping: MappingSpec,
    relationships: Sequence[ManyToOneSpec],
    add_all_instances: bool = True,
) -> None:
    state = {"done": False}

    def _before_flush(sess: SQLModelSession, *_: Any) -> None:
        if state["done"]:
            return
        state["done"] = True
        results = run_mapping(root, mapping)
        sidecar = compute_relationship_keys(root, mapping.traversals, relationships)
        bind_many_to_one(results, relationships, sidecar, fail_on_missing=True)
        if add_all_instances:
            for mr in results.values():
                for obj in mr.instances.values():
                    sess.add(obj)
        try:
            event.remove(session, "before_flush", _before_flush)
        except Exception:
            pass

    event.listen(session, "before_flush", _before_flush)


