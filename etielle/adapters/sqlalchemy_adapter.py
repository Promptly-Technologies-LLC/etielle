from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Sequence

from sqlalchemy.orm import Session
from sqlalchemy import event

from ..relationships import ManyToOneSpec, bind_many_to_one, compute_relationship_keys
from ..core import MappingSpec
from ..executor import run_mapping


@contextmanager
def no_autoflush(session: Session):
    """Temporarily disable autoflush on a SQLAlchemy Session."""
    prev = session.autoflush
    session.autoflush = False
    try:
        yield
    finally:
        session.autoflush = prev


def bind_and_flush(
    session: Session,
    *,
    root: Any,
    mapping: MappingSpec,
    relationships: Sequence[ManyToOneSpec],
    add_all_instances: bool = True,
) -> dict[str, Any]:
    """
    Convenience entrypoint:
    - Runs mapping to produce instances per table
    - Computes sidecar child->parent keys
    - Binds relationships via object refs
    - Adds instances to the session (optional)
    - Flushes once
    Returns the MappingResult dict.
    """

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
    session: Session,
    *,
    root: Any,
    mapping: MappingSpec,
    relationships: Sequence[ManyToOneSpec],
    add_all_instances: bool = True,
) -> None:
    """
    Installs a one-shot before_flush handler that performs binding prior to the
    first flush. Useful when the caller wants to control the transaction and
    commit timing, but still keep a single flush.
    """

    # Use a closure guard so we only bind once
    state = {"done": False}

    def _before_flush(sess: Session, *_: Any) -> None:  # type: ignore[override]
        if state["done"]:
            return
        state["done"] = True
        # Perform mapping + binding just-in-time before flush
        results = run_mapping(root, mapping)
        sidecar = compute_relationship_keys(root, mapping.traversals, relationships)
        bind_many_to_one(results, relationships, sidecar, fail_on_missing=True)
        if add_all_instances:
            for mr in results.values():
                for obj in mr.instances.values():
                    sess.add(obj)
        # Remove listener after one execution to avoid repeated work
        try:
            event.remove(Session, "before_flush", _before_flush)
        except Exception:
            pass

    # Register using SQLAlchemy's event system
    event.listen(Session, "before_flush", _before_flush)


