from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Sequence, Tuple

from .core import MappingResult, Transform, TraversalSpec
from .executor import _iter_traversal_nodes
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
) -> Dict[int, Dict[KeyTuple, KeyTuple]]:
    """
    Compute child->parent composite keys for each ManyToOneSpec by re-walking the
    MappingSpec traversals. This avoids mutating domain objects and keeps the
    computed keys in a sidecar map keyed by the child's composite key.

    Returns a dict keyed by the index of each ManyToOneSpec in `specs`, containing
    a mapping of child_composite_key -> parent_composite_key for that spec.
    """

    # Organize specs by child table for quick checks during traversal,
    # but keep track of each spec's index so keys can be stored per-spec.
    specs_by_child: Dict[str, list[tuple[int, ManyToOneSpec]]] = {}
    for idx, s in enumerate(specs):
        specs_by_child.setdefault(s.child_table, []).append((idx, s))

    # We need to traverse similarly to executor._iter_traversal_nodes and
    # compute composite keys for InstanceEmit. Keys are stored per relationship
    # spec index so multiple specs can share the same child table.

    # Auto-generated key counters for instances without join_on (must match executor)
    auto_key_counters: Dict[str, int] = {}

    out: Dict[int, Dict[KeyTuple, KeyTuple]] = {}

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
                if emit.join_keys:
                    # Join keys specified - compute composite key
                    child_key_parts = [tr(ctx) for tr in emit.join_keys]
                    if any(part is None or part == "" for part in child_key_parts):
                        continue
                    child_ck: KeyTuple = tuple(child_key_parts)
                else:
                    # No join keys - use auto-generated unique key (must match executor)
                    table_name = emit.table
                    counter = auto_key_counters.get(table_name, 0)
                    child_ck = (f"__auto_{counter}__",)
                    auto_key_counters[table_name] = counter + 1
                # For each spec on this child table, compute parent key and store
                for spec_idx, spec in child_specs:
                    parent_key_parts = [tr(ctx) for tr in spec.child_to_parent_key]
                    if any(part is None or part == "" for part in parent_key_parts):
                        # Skip if parent key incomplete; binding phase will treat as missing
                        continue
                    parent_ck: KeyTuple = tuple(parent_key_parts)
                    if spec_idx not in out:
                        out[spec_idx] = {}
                    out[spec_idx][child_ck] = parent_ck

    return out


def bind_many_to_one(
    results: Mapping[str, MappingResult[Any]],
    specs: Sequence[ManyToOneSpec],
    child_to_parent: Mapping[int, Mapping[KeyTuple, KeyTuple]],
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
    for idx, rel in enumerate(specs):
        parents = table_to_instances.get(rel.parent_table, {})
        children = table_to_instances.get(rel.child_table, {})
        key_map = child_to_parent.get(idx, {})

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


def bind_many_to_one_via_index(
    parent_result: MappingResult[Any],
    children: Sequence[Any],
    parent_index_field: str,
    child_lookup_field: str,
    relationship_attr: str,
    *,
    required: bool = True,
) -> list[str]:
    """
    Bind child -> parent using secondary index lookup.

    Args:
        parent_result: MappingResult containing parent instances with indices
        children: List of child instances
        parent_index_field: Field name in parent's secondary index
        child_lookup_field: Attribute name on child containing lookup value
        relationship_attr: Attribute name on child to set to parent
        required: If True, collect errors for missing parents

    Returns:
        List of error messages (empty if all bindings succeeded)
    """
    errors: list[str] = []
    parent_index = parent_result.indices.get(parent_index_field, {})

    for child in children:
        lookup_value = getattr(child, child_lookup_field, None)
        if lookup_value is None:
            if required:
                errors.append(f"Child has no value for {child_lookup_field}")
            continue

        parent = parent_index.get(lookup_value)
        if parent is None:
            if required:
                errors.append(
                    f"Parent not found for {parent_index_field}={lookup_value}"
                )
            continue

        setattr(child, relationship_attr, parent)

    return errors


def compute_child_lookup_values(
    root: Any,
    traversals: Sequence[TraversalSpec],
    relationships: Sequence[dict[str, Any]],
    emissions: Sequence[dict[str, Any]],
) -> Dict[str, Dict[KeyTuple, Dict[str, Any]]]:
    """
    Compute TempField/Field values for children and parents used in relationship binding.

    Returns dict: {table_name: {row_key: {field_name: value}}}

    This computes lookup values for:
    - Child tables: fields in the 'by' mapping (child_field -> parent_field)
    - Parent tables: fields in the 'by' mapping values (for FK population)
    """
    from .instances import InstanceEmit
    from .core import TableEmit

    # Map table -> {field_name: transform} for fields we need to compute
    field_transforms: Dict[str, Dict[str, Any]] = {}

    # Build map from emissions for quick lookup
    emission_by_table: Dict[str, dict[str, Any]] = {}
    for emission in emissions:
        emission_by_table[emission["table"]] = emission

    for rel in relationships:
        child_table = rel["child_table"]
        parent_table = rel["parent_table"]
        emission_idx = rel["emission_index"]
        child_emission = emissions[emission_idx]

        # Get transforms for child fields used in link_to
        for child_field in rel["by"].keys():
            for f in child_emission["fields"]:
                if f.name == child_field:
                    field_transforms.setdefault(child_table, {})[child_field] = f.transform
                    break

        # Get transforms for parent fields used in link_to (for FK population)
        parent_emission = emission_by_table.get(parent_table)
        if parent_emission:
            for parent_field in rel["by"].values():
                for f in parent_emission["fields"]:
                    if f.name == parent_field:
                        field_transforms.setdefault(parent_table, {})[parent_field] = f.transform
                        break

    # Auto-generated key counters (must match executor)
    auto_key_counters: Dict[str, int] = {}

    out: Dict[str, Dict[KeyTuple, Dict[str, Any]]] = {}

    for trav in traversals:
        for ctx in _iter_traversal_nodes(root, trav):
            for emit in trav.emits:
                # Handle both InstanceEmit and TableEmit
                if not isinstance(emit, (InstanceEmit, TableEmit)):
                    continue

                # Check if this table has relationship fields to compute
                transforms = field_transforms.get(emit.table)
                if not transforms:
                    continue

                # Compute row's key (must match executor logic)
                if emit.join_keys:
                    key_parts = [tr(ctx) for tr in emit.join_keys]
                    if any(part is None or part == "" for part in key_parts):
                        continue
                    row_key: KeyTuple = tuple(key_parts)
                else:
                    counter = auto_key_counters.get(emit.table, 0)
                    row_key = (f"__auto_{counter}__",)
                    auto_key_counters[emit.table] = counter + 1

                # Compute field values
                field_values: Dict[str, Any] = {}
                for field_name, transform in transforms.items():
                    field_values[field_name] = transform(ctx)

                out.setdefault(emit.table, {})[row_key] = field_values

    return out


def bind_relationships_via_index(
    raw_results: Mapping[str, MappingResult[Any]],
    relationships: Sequence[dict[str, Any]],
    child_lookup_values: Dict[str, Dict[KeyTuple, Dict[str, Any]]],
    *,
    fail_on_missing: bool = False,
) -> list[str]:
    """
    Bind all relationships using secondary indices.

    This is the preferred method when using the new "no default join key" approach.
    Instead of computing child->parent key mappings, it uses secondary indices
    built during instance creation.

    Args:
        raw_results: Dict mapping table name to MappingResult (with indices)
        relationships: List of relationship dicts from PipelineBuilder._relationships
                      Each has: child_table, parent_table, by (dict of child_field->parent_field)
        child_lookup_values: Pre-computed TempField values for children
                            {child_table: {child_key: {field_name: value}}}
        fail_on_missing: If True, raise error on missing parents; if False, return errors

    Returns:
        List of error messages (empty if all succeeded)
    """
    all_errors: list[str] = []

    for rel in relationships:
        child_table = rel["child_table"]
        parent_table = rel["parent_table"]
        by_mapping = rel["by"]  # {child_field: parent_field}

        # Get parent and child results
        parent_result = raw_results.get(parent_table)
        child_result = raw_results.get(child_table)

        if parent_result is None or child_result is None:
            continue

        # Get lookup values for this child table
        lookup_values = child_lookup_values.get(child_table, {})

        # Infer attr name from parent table name (singular)
        attr_name = parent_table.rstrip("s") if parent_table.endswith("s") else parent_table

        # Bind each child to its parent
        for child_key, child_obj in child_result.instances.items():
            child_values = lookup_values.get(child_key, {})

            # For each field mapping, look up parent
            for child_field, parent_field in by_mapping.items():
                lookup_value = child_values.get(child_field)
                if lookup_value is None:
                    if fail_on_missing:
                        all_errors.append(f"Child {child_table} key={child_key} has no value for {child_field}")
                    continue

                # Look up parent in secondary index
                parent_index = parent_result.indices.get(parent_field, {})
                parent_obj = parent_index.get(lookup_value)

                if parent_obj is None:
                    if fail_on_missing:
                        all_errors.append(f"Parent not found for {parent_field}={lookup_value}")
                    continue

                # Set relationship
                setattr(child_obj, attr_name, parent_obj)

    if all_errors and fail_on_missing:
        raise RuntimeError(
            "relationship binding failed (via index):\n" + "\n".join(all_errors)
        )

    return all_errors
