from typing import Any, Dict, List, Tuple
from difflib import get_close_matches
from .core import MappingSpec, Context, TraversalSpec, TableEmit, MappingResult
from .transforms import _iter_nodes, _resolve_path
from collections.abc import Mapping, Sequence, Iterable
from .instances import InstanceEmit, resolve_field_name_for_builder

# -----------------------------
# Executor
# -----------------------------


def _iter_traversal_nodes(root: Any, spec: TraversalSpec) -> Iterable[Context]:
    for base_ctx, outer in _iter_nodes(root, spec.path):

        def yield_from_container(
            parent_ctx: Context, container: Any, mode: str
        ) -> Iterable[Context]:
            # Determine iteration behavior from mode
            if mode == "items":
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
                return
            if mode == "single":
                yield Context(
                    root=root,
                    node=container,
                    path=parent_ctx.path,
                    parent=parent_ctx,
                    key=None,
                    index=None,
                    slots={},
                )
                return
            # auto mode
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
                return
            if isinstance(container, Sequence) and not isinstance(
                container, (str, bytes)
            ):
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
                return
            # Non-iterable in auto mode: treat as single
            # BUT: None means "no data" - don't iterate at all
            if container is not None:
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
            yield from yield_from_container(base_ctx, outer, spec.mode)
            continue

        # Iterate outer container first, then inner container under each outer node
        for outer_ctx in yield_from_container(base_ctx, outer, spec.mode):
            inner_container = _resolve_path(outer_ctx.node, spec.inner_path)
            inner_mode = spec.inner_mode
            for inner_ctx in yield_from_container(
                outer_ctx, inner_container, inner_mode
            ):
                yield inner_ctx


def run_mapping(
    root: Any,
    spec: MappingSpec,
    linkable_fields: Dict[str, set[str]] | None = None
) -> Dict[str, MappingResult[Any]]:
    """
    Execute mapping spec against root JSON, returning rows per table.

    When join_on is specified, rows are merged by composite join keys per table.
    If any join-key part is None/empty, the row is skipped.

    When join_on is NOT specified, each iteration creates a distinct instance
    with an auto-generated unique key (no merging, no deduplication).

    Args:
        root: The JSON data to process
        spec: The mapping specification
        linkable_fields: Dict mapping table name to set of field names used in link_to
    """
    if linkable_fields is None:
        linkable_fields = {}
    # For classic table rows (index by composite key)
    table_to_index: Dict[str, Dict[Tuple[Any, ...], Dict[str, Any]]] = {}
    table_row_order: Dict[str, List[Tuple[Any, ...]]] = {}

    # For instance emission
    instance_tables: Dict[str, Dict[str, Any]] = {}

    # Auto-generated key counter for instances without join_on
    auto_key_counters: Dict[str, int] = {}

    for traversal in spec.traversals:
        for ctx in _iter_traversal_nodes(root, traversal):
            for emit in traversal.emits:
                # Compute join key
                if emit.join_keys:
                    # Join keys specified - compute composite key
                    key_parts: List[Any] = [tr(ctx) for tr in emit.join_keys]
                    if any(part is None or part == "" for part in key_parts):
                        continue
                    composite_key = tuple(key_parts)
                else:
                    # No join keys - use auto-generated unique key
                    # This allows each iteration to create a distinct instance
                    table_name = emit.table
                    counter = auto_key_counters.get(table_name, 0)
                    composite_key = (f"__auto_{counter}__",)
                    auto_key_counters[table_name] = counter + 1

                # Branch by emit type
                if isinstance(emit, TableEmit):
                    index = table_to_index.setdefault(emit.table, {})
                    order = table_row_order.setdefault(emit.table, [])
                    row = index.setdefault(composite_key, {})
                    if composite_key not in order:
                        order.append(composite_key)
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
                    # TempFields are stored in shadow but not persisted to instances
                    temp_fields = getattr(emit, "temp_fields", frozenset())
                    updates: Dict[str, Any] = {}
                    for spec_field in emit.fields:
                        field_name = resolve_field_name_for_builder(
                            tbl["builder"], spec_field
                        )
                        is_temp = field_name in temp_fields
                        # Strict field checks with suggestions for string selectors
                        # Skip check for TempFields since they're not persisted
                        if emit.strict_fields and not is_temp:
                            known = tbl["builder"].known_fields()
                            if known and field_name not in known:
                                suggestions = get_close_matches(
                                    field_name, list(known), n=3, cutoff=0.6
                                )
                                suggest_str = (
                                    f"; did you mean {', '.join(suggestions)}?"
                                    if suggestions
                                    else ""
                                )
                                tbl["builder"].record_update_error(
                                    composite_key,
                                    f"field {field_name}: unknown field{suggest_str}",
                                )
                                if (
                                    getattr(emit, "strict_mode", "collect_all")
                                    == "fail_fast"
                                ):
                                    raise RuntimeError(
                                        f"Unknown field '{field_name}' for table '{emit.table}' and key {composite_key}"
                                    )
                                # Skip applying this unknown field
                                continue
                        value = spec_field.transform(ctx)
                        policy = tbl["policies"].get(field_name)
                        if policy is not None:
                            prev = shadow_bucket.get(field_name)
                            try:
                                value = policy.merge(prev, value)
                            except Exception as e:  # pragma: no cover - defensive
                                tbl["builder"].record_update_error(
                                    composite_key,
                                    f"field {field_name}: merge policy error: {e}",
                                )
                                # Skip updating this field on error
                                continue
                        # Always store in shadow (for secondary indices)
                        shadow_bucket[field_name] = value
                        # Only persist non-TempFields to instances
                        if not is_temp:
                            updates[field_name] = value

                    tbl["builder"].update(composite_key, updates)
                    continue

                # Unknown emit type: ignore gracefully
                continue

    # Build MappingResult outputs per table
    outputs: Dict[str, MappingResult[Any]] = {}

    # 1) Classic row tables
    for table, index in table_to_index.items():
        # Inject id for single-key tables, but only if key is user-provided
        # (not auto-generated like "__auto_0__")
        for key_tuple, data in index.items():
            if len(key_tuple) == 1 and "id" not in data:
                key_value = key_tuple[0]
                if not (isinstance(key_value, str) and key_value.startswith("__auto_")):
                    data["id"] = key_value
        # Deterministic order by traversal arrival order
        ordered_keys = table_row_order.get(table, list(index.keys()))
        ordered_instances: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for key_tuple in ordered_keys:
            ordered_instances[key_tuple] = index[key_tuple]
        outputs[table] = MappingResult(
            instances=ordered_instances,
            update_errors={},
            finalize_errors={},
            stats={
                "num_instances": len(ordered_instances),
                "num_update_errors": 0,
                "num_finalize_errors": 0,
            },
        )

    # 2) Instance tables (builders)
    for table, meta in instance_tables.items():
        builder = meta["builder"]
        finalized = builder.finalize_all()
        # Wrap errors with table/key context
        upd_errors_raw = builder.update_errors()
        fin_errors_raw = builder.finalize_errors()
        upd_errors: Dict[Tuple[Any, ...], List[str]] = {}
        fin_errors: Dict[Tuple[Any, ...], List[str]] = {}
        # Deterministic order by arrival: rely on insertion order of builder.acc keys
        instances: Dict[Tuple[Any, ...], Any] = {}
        for key_tuple, payload in finalized.items():
            instances[key_tuple] = payload
        for key_tuple, msgs in upd_errors_raw.items():
            upd_errors[key_tuple] = [f"table={table} key={key_tuple} {m}" for m in msgs]
        for key_tuple, msgs in fin_errors_raw.items():
            fin_errors[key_tuple] = [f"table={table} key={key_tuple} {m}" for m in msgs]

        # Build secondary indices for linkable fields
        # Use shadow values first (for TempFields), fall back to instance attributes (for Fields)
        shadow = meta["shadow"]
        indices: Dict[str, Dict[Any, Any]] = {}
        for field_name in linkable_fields.get(table, set()):
            field_index: Dict[Any, Any] = {}
            for key_tuple, instance in instances.items():
                # First try shadow (includes TempFields that aren't on instance)
                shadow_bucket = shadow.get(key_tuple, {})
                field_value = shadow_bucket.get(field_name)
                # Fall back to instance attribute for regular Fields
                if field_value is None:
                    field_value = getattr(instance, field_name, None)
                if field_value is not None:
                    field_index[field_value] = instance
            if field_index:
                indices[field_name] = field_index

        outputs[table] = MappingResult(
            instances=instances,
            update_errors=upd_errors,
            finalize_errors=fin_errors,
            stats={
                "num_instances": len(instances),
                "num_update_errors": sum(len(v) for v in upd_errors.values()),
                "num_finalize_errors": sum(len(v) for v in fin_errors.values()),
            },
            indices=indices,
        )

    return outputs
