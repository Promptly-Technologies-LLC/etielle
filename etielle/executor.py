from __future__ import annotations

from typing import Any, Dict, List, Tuple
from .core import MappingSpec, Context, TraversalSpec
from .transforms import _iter_nodes, _resolve_path
from collections.abc import Mapping, Sequence, Iterable

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


def run_mapping(root: Any, spec: MappingSpec) -> Dict[str, List[Dict[str, Any]]]:
    """
    Execute mapping spec against root JSON, returning rows per table.

    Rows are merged by composite join keys per table. If any join-key part is
    None/empty, the row is skipped.
    """
    table_to_index: Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]] = {}

    for traversal in spec.traversals:
        for ctx in _iter_traversal_nodes(root, traversal):
            for emit in traversal.emits:
                # Compute join key
                key_parts: List[Any] = [tr(ctx) for tr in emit.join_keys]
                if any(part is None or part == "" for part in key_parts):
                    continue
                composite_key = tuple(key_parts)

                row = table_to_index.setdefault(emit.table, {}).setdefault(composite_key, {})

                # Compute fields
                for fld in emit.fields:
                    value = fld.transform(ctx)
                    row[fld.name] = value

    # Convert indexes to lists, and ensure an 'id' exists if single join key is provided
    result: Dict[str, List[Dict[str, Any]]] = {}
    for table, index in table_to_index.items():
        rows: List[Dict[str, Any]] = []
        for key_tuple, data in index.items():
            if len(key_tuple) == 1 and "id" not in data:
                data["id"] = key_tuple[0]
            rows.append(data)
        result[table] = rows
    return result
