from typing import Any, Dict, List, Tuple
from .core import MappingSpec, Context, TraversalSpec, TableEmit
from .transforms import _iter_nodes, _resolve_path
from collections.abc import Mapping, Sequence, Iterable
from .instances import InstanceEmit, resolve_field_name_for_builder

# -----------------------------
# Executor
# -----------------------------


def _iter_traversal_nodes(root: Any, spec: TraversalSpec) -> Iterable[Context]:
    for base_ctx, outer in _iter_nodes(root, spec.path):
        def yield_from_container(parent_ctx: Context, container: Any, iterate_items: bool) -> Iterable[Context]:
            if iterate_items:
                if isinstance(container, Mapping):
                    for k, v in container.items():
                        yield Context(
                            root=root,
                            node=v,
                            path=parent_ctx.path + (str(k),),
                            parent=parent_ctx,
                            key=str(k),
                            index=None,
                            slots={},
                        )
            else:
                if isinstance(container, Sequence) and not isinstance(container, (str, bytes)):
                    for i, v in enumerate(container):
                        yield Context(
                            root=root,
                            node=v,
                            path=parent_ctx.path + (i,),
                            parent=parent_ctx,
                            key=None,
                            index=i,
                            slots={},
                        )
                else:
                    # Emit a single context for non-iterable container (e.g., root object)
                    yield Context(
                        root=root,
                        node=container,
                        path=parent_ctx.path,
                        parent=parent_ctx,
                        key=None,
                        index=None,
                        slots={},
                    )

        # If no inner path, iterate outer container directly
        if not spec.inner_path:
            yield from yield_from_container(base_ctx, outer, spec.iterate_items)
            continue

        # Iterate outer container first, then inner container under each outer node
        for outer_ctx in yield_from_container(base_ctx, outer, spec.iterate_items):
            inner_container = _resolve_path(outer_ctx.node, spec.inner_path)
            inner_iter_items = bool(spec.inner_iterate_items)
            for inner_ctx in yield_from_container(outer_ctx, inner_container, inner_iter_items):
                yield inner_ctx


def run_mapping(root: Any, spec: MappingSpec) -> Dict[str, List[Any]]:
    """
    Execute mapping spec against root JSON, returning rows per table.

    Rows are merged by composite join keys per table. If any join-key part is
    None/empty, the row is skipped.
    """
    # For classic table rows
    table_to_index: Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]] = {}

    # For instance emission
    instance_tables: Dict[
        str,
        Dict[str, Any]
    ] = {}

    for traversal in spec.traversals:
        for ctx in _iter_traversal_nodes(root, traversal):
            for emit in traversal.emits:
                # Compute join key
                key_parts: List[Any] = [tr(ctx) for tr in emit.join_keys]
                if any(part is None or part == "" for part in key_parts):
                    continue
                composite_key = tuple(key_parts)
                
                # Branch by emit type
                if isinstance(emit, TableEmit):
                    row = table_to_index.setdefault(emit.table, {}).setdefault(composite_key, {})
                    for fld in emit.fields:  # type: ignore[attr-defined]
                        value = fld.transform(ctx)
                        row[fld.name] = value
                    continue

                if isinstance(emit, InstanceEmit):
                    # Prepare table entry for instances
                    tbl = instance_tables.setdefault(
                        emit.table,
                        {
                            "builder": emit.builder,
                            "shadow": {},
                            "policies": dict(emit.policies),
                        },
                    )
                    # Merge policies if multiple emits target same table
                    tbl["policies"].update(getattr(emit, "policies", {}))

                    shadow: Dict[Tuple[Any, ...], Dict[str, Any]] = tbl["shadow"]
                    shadow_bucket = shadow.setdefault(composite_key, {})

                    # Build updates with optional merge policies
                    updates: Dict[str, Any] = {}
                    for spec_field in emit.fields:
                        field_name = resolve_field_name_for_builder(tbl["builder"], spec_field)
                        value = spec_field.transform(ctx)
                        policy = tbl["policies"].get(field_name)
                        if policy is not None:
                            prev = shadow_bucket.get(field_name)
                            value = policy.merge(prev, value)
                        shadow_bucket[field_name] = value
                        updates[field_name] = value

                    tbl["builder"].update(composite_key, updates)
                    continue

                # Unknown emit type: ignore gracefully
                continue

    # Convert indexes to lists, and ensure an 'id' exists if single join key is provided
    result: Dict[str, List[Any]] = {}
    for table, index in table_to_index.items():
        rows: List[Dict[str, Any]] = []
        for key_tuple, data in index.items():
            if len(key_tuple) == 1 and "id" not in data:
                data["id"] = key_tuple[0]
            rows.append(data)
        result[table] = rows
    # Finalize instance tables
    for table, meta in instance_tables.items():
        builder = meta["builder"]
        finalized = builder.finalize_all()
        instances = list(finalized.values())
        result[table] = instances
    return result
