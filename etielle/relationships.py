from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Sequence, Tuple

from .core import Transform, TraversalSpec
from .executor import MappingResult, _iter_traversal_nodes
from .instances import InstanceEmit


KeyTuple = Tuple[Any, ...]


@dataclass(frozen=True)
class ManyToOneSpec:
    """
    Declarative specification for a many-to-one relationship.

    - child_table: name used in `InstanceEmit.table` when emitting child instances
    - parent_table: name used in `InstanceEmit.table` when emitting parent instances
    - attr: attribute name on the child instance that references the parent instance
    - child_to_parent_key: transforms evaluated in the child's traversal context that
      produce the composite logical key of the parent. Keys are computed during a
      dedicated traversal pass (see `compute_relationship_keys`).
    - required: if True, binding fails when a parent cannot be found
    """

    child_table: str
    parent_table: str
    attr: str
    child_to_parent_key: Sequence[Transform[Any]]
    required: bool = True


def compute_relationship_keys(
    root: Any,
    traversals: Sequence[TraversalSpec],
    specs: Sequence[ManyToOneSpec],
) -> Dict[str, Dict[KeyTuple, KeyTuple]]:
    """
    Compute child->parent composite keys for each ManyToOneSpec by re-walking the
    MappingSpec traversals. This avoids mutating domain objects and keeps the
    computed keys in a sidecar map keyed by the child's composite key.

    Returns a dict keyed by child_table containing a mapping of
    child_composite_key -> parent_composite_key.
    """

    # Organize specs by child table for quick checks during traversal
    specs_by_child: Dict[str, list[ManyToOneSpec]] = {}
    for s in specs:
        specs_by_child.setdefault(s.child_table, []).append(s)

    # We need to traverse similarly to executor._iter_traversal_nodes and
    # compute composite keys for InstanceEmit.

    out: Dict[str, Dict[KeyTuple, KeyTuple]] = {tbl: {} for tbl in specs_by_child.keys()}

    for trav in traversals:
        for ctx in _iter_traversal_nodes(root, trav):
            for emit in trav.emits:
                    # Only care about InstanceEmit with a spec registered on this child table
                    if not isinstance(emit, InstanceEmit):
                        continue
                    child_specs = specs_by_child.get(emit.table)
                    if not child_specs:
                        continue
                    # Compute child's composite key for this emit
                    child_key_parts = [tr(ctx) for tr in emit.join_keys]
                    if any(part is None or part == "" for part in child_key_parts):
                        continue
                    child_ck: KeyTuple = tuple(child_key_parts)
                    # For each spec on this child table, compute parent key and store
                    for spec in child_specs:
                        parent_key_parts = [tr(ctx) for tr in spec.child_to_parent_key]
                        if any(part is None or part == "" for part in parent_key_parts):
                            # Skip if parent key incomplete; binding phase will treat as missing
                            continue
                        parent_ck: KeyTuple = tuple(parent_key_parts)
                        out[spec.child_table][child_ck] = parent_ck

    return out


def bind_many_to_one(
    results: Mapping[str, MappingResult[Any]],
    specs: Sequence[ManyToOneSpec],
    child_to_parent: Mapping[str, Mapping[KeyTuple, KeyTuple]],
    *,
    fail_on_missing: bool = True,
) -> None:
    """
    Bind child -> parent object references in-place using plain attribute assignment.

    - results: output of executor.run_mapping(root, spec)
    - specs: relationship specs
    - child_to_parent: sidecar keys as returned by `compute_relationship_keys`
    - fail_on_missing: if True, raise RuntimeError aggregating missing parents
    """

    # Build parent indices per table
    table_to_instances: Dict[str, Dict[KeyTuple, Any]] = {
        table: mr.instances for table, mr in results.items()
    }

    errors: list[str] = []
    for rel in specs:
        parents = table_to_instances.get(rel.parent_table, {})
        children = table_to_instances.get(rel.child_table, {})
        key_map = child_to_parent.get(rel.child_table, {})
        for child_ck, child_obj in children.items():
            parent_ck = key_map.get(child_ck)
            if parent_ck is None:
                if rel.required:
                    errors.append(
                        f"missing parent key for child table={rel.child_table} key={child_ck}"
                    )
                continue
            parent_obj = parents.get(parent_ck)
            if parent_obj is None:
                if rel.required:
                    errors.append(
                        f"parent not found table={rel.parent_table} key={parent_ck} for child table={rel.child_table} key={child_ck}"
                    )
                continue
            try:
                setattr(child_obj, rel.attr, parent_obj)
            except Exception as e:  # pragma: no cover - defensive
                errors.append(
                    f"failed to set attribute '{rel.attr}' on child table={rel.child_table} key={child_ck}: {e}"
                )

    if errors and fail_on_missing:
        raise RuntimeError(
            "relationship binding failed (many-to-one):\n" + "\n".join(errors)
        )


