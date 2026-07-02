"""Chunking and flush strategy interfaces for streaming execution."""

from __future__ import annotations

import json
import tempfile
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from itertools import groupby
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from etielle.fluent import PipelineBuilder, TableStats
    from etielle.telemetry import TelemetryCallback

RootTuple = tuple[Any, ...]


@dataclass(frozen=True)
class Chunk:
    """A key-complete batch of JSON roots to map together.

    Attributes:
        roots: One or more JSON payloads for this chunk.
        sequential: If True, every root is mapped against pipeline root index 0
            with shared auto-key counters (group-by / repeated single-root records).
            If False, root at position *i* maps to pipeline root index *i*
            (multi-root ``goto_root()`` semantics).
    """

    roots: RootTuple
    sequential: bool = False


@runtime_checkable
class ChunkSource(Protocol):
    """Produces key-complete chunks for streaming execution."""

    def chunks(self) -> Iterator[Chunk]:
        """Yield chunks in traversal order."""
        ...


class OneRecordPerChunkSource:
    """Wrap an iterable of JSON roots; each root becomes its own chunk.

    A re-iterable input (e.g. a list) can be streamed more than once. A
    single-use iterator (e.g. a generator or an ``ijson`` stream) is consumed
    on the first pass, matching the single-consumption nature of streaming
    sources; running the pipeline again would yield no chunks.
    """

    def __init__(self, records: Iterator[Any] | Sequence[Any]) -> None:
        self._records = records

    def chunks(self) -> Iterator[Chunk]:
        for record in self._records:
            yield Chunk(roots=(record,), sequential=True)


class CallableChunkSource:
    """Build chunks from a caller-supplied factory (tests and advanced callers)."""

    def __init__(self, factory: Callable[[], Iterator[Chunk]]) -> None:
        self._factory = factory

    def chunks(self) -> Iterator[Chunk]:
        yield from self._factory()


class GroupByChunkSource:
    """Group consecutive records that share a key into one chunk each.

    This is the single-pass, streaming group-by chunker. It reads the input
    once, accumulates consecutive records that map to the same ``key``, and
    emits a chunk whenever the key changes. Peak memory is one chunk: only the
    records for the current key are held at a time.

    Each emitted chunk is ``sequential`` -- every record in the group is mapped
    against pipeline root index 0 with shared auto-key counters, so a group of
    related records merges into one component (the repeated single-root shape).

    Grouped-input requirement:
        Correctness depends on the input *already being grouped (or sorted) by
        ``key``*, which is the common shape for paginated APIs and "one parent
        subtree at a time" feeds. Because grouping is consecutive only, records
        that share a key but are separated by records with a different key land
        in *separate* chunks. With a relationship key that is fine for
        key-completeness but breaks relationship-completeness; the runtime
        relationship-completeness check raises if a chunk is missing endpoints.
        For unsorted input, sort by ``key`` first or use
        ``ExternalPartitionChunkSource``, the disk-backed partitioner that
        handles arbitrarily-ordered input.

    Choosing a relationship-complete key:
        Pick a key that is a *complete component root* -- coarse enough that
        every record reachable through a relationship from one record sharing
        the key also shares it (e.g. the owning entity id), not merely a fine
        merge key. Grouping guarantees key-completeness for whatever key you
        choose; the runtime validation catches a key that is too fine.

    Args:
        records: An iterable (or single-use iterator) of JSON roots. Consumed
            exactly once.
        key: Function mapping a record to its grouping key. Adjacent records
            with equal keys are batched into the same chunk.
    """

    def __init__(
        self,
        records: Iterator[Any] | Iterable[Any],
        key: Callable[[Any], Any],
    ) -> None:
        self._records = records
        self._key = key

    def chunks(self) -> Iterator[Chunk]:
        for _, group in groupby(self._records, key=self._key):
            yield Chunk(roots=tuple(group), sequential=True)


class PreSegmentedChunkSource:
    """Pass an already-segmented iterable of chunks through unchanged.

    Use this when the caller has already partitioned input into key-complete,
    relationship-complete ``Chunk`` objects (e.g. a producer that knows its own
    boundaries). The chunks are yielded in order without buffering, so peak
    memory is whatever the upstream iterable holds at a time.

    A re-iterable input (e.g. a list) can be streamed more than once; a
    single-use iterator is consumed on the first pass.

    Args:
        chunks: An iterable (or single-use iterator) of ``Chunk`` objects.
    """

    def __init__(self, chunks: Iterator[Chunk] | Iterable[Chunk]) -> None:
        self._chunks = chunks

    def chunks(self) -> Iterator[Chunk]:
        yield from self._chunks


class ExternalPartitionChunkSource:
    """Partition arbitrarily-ordered input into key-complete chunks via disk.

    This is the two-pass, disk-backed partitioner: pass one serializes
    every record to a temporary spill file and builds an in-memory
    key -> offsets index; pass two emits one chunk per distinct key by reading
    that key's records back from disk. Unlike ``GroupByChunkSource`` it does
    not require the input to be grouped or sorted -- records that share a key
    are collected into the same chunk no matter how far apart they arrive.

    Trade-offs:
        Peak record memory is one chunk (the current partition), but the full
        dataset is written to temporary storage, and the offset index holds a
        few machine words per record for the duration of the stream. Pass two
        performs random reads, so a fast local temp filesystem is preferable.

    Serialization:
        Records are serialized with ``dumps`` (default ``json.dumps``) and
        deserialized with ``loads`` (default ``json.loads``), so chunks yield
        *reconstructed copies* of the input records. Non-JSON-serializable
        records need custom ``dumps``/``loads`` callables.

    Emission order:
        Chunks are emitted in first-appearance order of their keys. Each chunk
        is ``sequential`` -- every record in the partition is mapped against
        pipeline root index 0 with shared auto-key counters, matching
        ``GroupByChunkSource``.

    Choosing a relationship-complete key:
        As with ``GroupByChunkSource``, pick a key that is a complete
        component root; partitioning guarantees key-completeness for the
        chosen key, and the runtime relationship-completeness check catches a
        key that is too fine.

    Args:
        records: An iterable (or single-use iterator) of JSON roots. Consumed
            exactly once per ``chunks()`` iteration.
        key: Function mapping a record to its partition key. Must return a
            hashable value.
        dir: Optional directory for the temporary spill file (defaults to the
            platform temp directory). The file is deleted when iteration
            finishes or the iterator is closed.
        dumps: Serializer from record to ``str`` (default ``json.dumps``).
        loads: Deserializer from ``str`` to record (default ``json.loads``).
    """

    def __init__(
        self,
        records: Iterator[Any] | Iterable[Any],
        key: Callable[[Any], Any],
        *,
        dir: str | None = None,
        dumps: Callable[[Any], str] | None = None,
        loads: Callable[[str], Any] | None = None,
    ) -> None:
        self._records = records
        self._key = key
        self._dir = dir
        self._dumps = dumps if dumps is not None else json.dumps
        self._loads = loads if loads is not None else json.loads

    def chunks(self) -> Iterator[Chunk]:
        spill = tempfile.TemporaryFile(
            mode="w+b", dir=self._dir, prefix="etielle-partition-"
        )
        try:
            index: dict[Any, list[tuple[int, int]]] = {}
            offset = 0
            for record in self._records:
                data = self._dumps(record).encode("utf-8")
                spill.write(data)
                index.setdefault(self._key(record), []).append((offset, len(data)))
                offset += len(data)

            for spans in index.values():
                roots = []
                for span_offset, span_length in spans:
                    spill.seek(span_offset)
                    roots.append(self._loads(spill.read(span_length).decode("utf-8")))
                yield Chunk(roots=tuple(roots), sequential=True)
        finally:
            spill.close()


@dataclass
class FlushContext:
    """Inputs for a flush at a component boundary.

    The public fields provide everything a custom ``FlushStrategy`` needs to
    persist a component without touching engine internals:

    - ``scope_tables``: tables this flush is responsible for.
    - ``bind_context``: mapped results for the scope plus any resident/eager
      tables, used to resolve relationship parents.
    - ``local_results``: mapped results scoped to ``scope_tables`` only.
    - ``dep_graph``: child -> parents dependency graph (use
      ``etielle.utils.topological_sort`` for flush order).
    - ``link_to_rels`` / ``backlink_rels``: relationship specs in scope.
    - ``stats`` / ``on_event``: stats accumulator and telemetry sink.
    - ``session``: the SQLAlchemy session or Supabase client from ``load()``.
    - ``is_supabase``: whether ``session`` is a Supabase client.

    ``builder`` is the engine handle that the built-in strategies use to reuse
    etielle's standard insert/bind logic. Custom strategies should rely on the
    public fields above and implement their own persistence rather than calling
    builder internals.
    """

    scope_tables: set[str]
    bind_context: dict[str, Any]
    local_results: dict[str, Any]
    dep_graph: dict[str, set[str]]
    link_to_rels: list[dict[str, Any]]
    backlink_rels: list[dict[str, Any]]
    stats: dict[str, TableStats]
    on_event: TelemetryCallback | None
    session: Any
    is_supabase: bool
    builder: PipelineBuilder = field(repr=False)


@runtime_checkable
class FlushStrategy(Protocol):
    """Defines persistence behavior at a chunk/component boundary."""

    def flush(self, ctx: FlushContext) -> None:
        """Flush ``scope_tables`` using ``bind_context`` for relationship binding."""
        ...


class KeyCompleteFlushStrategy:
    """Default streaming strategy: plain insert/flush, no cross-chunk merge."""

    def flush(self, ctx: FlushContext) -> None:
        if ctx.session is None:
            return
        if ctx.is_supabase:
            from etielle.utils import topological_sort

            child_lookup = {
                t: mr.lookup_values for t, mr in ctx.bind_context.items()
            }
            component_tables_dict = {
                t: dict(ctx.local_results[t].instances)
                for t in ctx.scope_tables
                if t in ctx.local_results
            }
            component_order = topological_sort(ctx.dep_graph, ctx.scope_tables)
            ctx.builder._flush_to_supabase(
                component_tables_dict,
                component_order,
                child_lookup,
                ctx.stats,
                ctx.on_event,
            )
        else:
            ctx.builder._flush_sqlalchemy_scope(
                ctx.scope_tables,
                ctx.bind_context,
                ctx.dep_graph,
                ctx.link_to_rels,
                ctx.backlink_rels,
                ctx.stats,
                ctx.on_event,
            )


def _is_auto_key(key: tuple[Any, ...]) -> bool:
    """Return True for engine-generated auto keys (no ``join_on`` on the table)."""
    return len(key) == 1 and isinstance(key[0], str) and key[0].startswith("__auto_")


def _parent_attr_name(parent_table: str) -> str:
    """Infer the relationship attribute name from a parent table name."""
    return parent_table.rstrip("s") if parent_table.endswith("s") else parent_table


def _copy_instance_state(target: Any, incoming: Any) -> None:
    """Copy non-None scalar attribute values from ``incoming`` onto ``target``.

    Collection-valued attributes (lists/sets, e.g. backlink relationship
    collections) and private/engine attributes are left untouched, so merging
    a late-arriving row cannot detach previously bound children.
    """
    for attr_name, value in vars(incoming).items():
        if attr_name.startswith("_"):
            continue
        if value is None:
            continue
        if isinstance(value, (list, set)):
            continue
        setattr(target, attr_name, value)


def _replace_merged_instance(
    result: Any,
    key: tuple[Any, ...],
    instance: Any,
    merged: Any,
    replacements: dict[int, Any],
) -> None:
    """Record a merge replacement and keep instances/indices consistent."""
    replacements[id(instance)] = merged
    result.instances[key] = merged
    for field_index in result.indices.values():
        for lookup_value, indexed in field_index.items():
            if indexed is instance:
                field_index[lookup_value] = merged


class UpsertFlushStrategy:
    """Streaming strategy with database-level conflict handling (SQLAlchemy).

    Where the default ``KeyCompleteFlushStrategy`` uses plain ``session.add()``
    (a duplicate row aborts the chunk's transaction with ``IntegrityError``),
    this strategy resolves conflicts against rows that are *already stored*:

    - ``on_conflict="update"`` (default): each instance is persisted with
      ``session.merge()``. If a row with the same primary key exists, its
      columns are overwritten with the incoming values (last write wins);
      otherwise the row is inserted. Suited to idempotent re-runs.
    - ``on_conflict="skip"``: each instance is inserted inside a per-row
      ``SAVEPOINT``; a row that raises ``IntegrityError`` (duplicate primary
      key or unique constraint, including a concurrent-insert race) is rolled
      back and skipped while the rest of the chunk proceeds. Suited to
      on-conflict-skip deduplication of streaming ingest.

    Documented limitations:

    - **SQLAlchemy only.** For Supabase, pass ``load(upsert=True,
      upsert_on=...)`` with the default strategy; the Supabase adapter
      performs native upserts.
    - **Not a cross-chunk merge substitute.** Merge policies
      (``AddPolicy`` etc.) run only within a chunk's mapping pass; across
      re-runs, ``update`` mode overwrites whole rows (including ``None``
      values) rather than merging fields.
    - **``update`` mode detects conflicts by primary key.** Instances without
      primary key values are inserted as new rows, and each merge issues a
      per-row SELECT when the row is not already in the session's identity
      map. A concurrent insert between that SELECT and the INSERT can still
      raise ``IntegrityError``; use ``skip`` mode where races must be
      tolerated.
    - **``skip`` mode swallows every ``IntegrityError``,** not only duplicate
      keys (e.g. a NOT NULL violation also skips the row), and pays a per-row
      SAVEPOINT + flush round trip. A child bound to a skipped parent is
      itself skipped, because the cascaded parent insert reproduces the
      conflict inside the child's SAVEPOINT. Skipped rows are counted in
      ``mapped`` stats but in neither ``inserted`` nor ``failed``.
    - **Plain-dict rows** (string table targets) are not persisted, matching
      the default strategy.
    """

    def __init__(self, on_conflict: Literal["update", "skip"] = "update") -> None:
        if on_conflict not in ("update", "skip"):
            raise ValueError(
                f"on_conflict must be 'update' or 'skip', got {on_conflict!r}"
            )
        self._on_conflict = on_conflict

    def flush(self, ctx: FlushContext) -> None:
        if ctx.session is None:
            return
        if ctx.is_supabase:
            raise ValueError(
                "UpsertFlushStrategy supports SQLAlchemy sessions only. For "
                "Supabase, use load(upsert=True, upsert_on=...) with the default "
                "flush strategy; the Supabase adapter performs native upserts."
            )
        from etielle.telemetry import FlushStarted, _emit
        from etielle.utils import topological_sort

        parent_attrs: dict[str, list[str]] = {}
        for rel in ctx.link_to_rels:
            parent_attrs.setdefault(rel["child_table"], []).append(
                _parent_attr_name(rel["parent_table"])
            )
        # Maps id(transient chunk-local instance) -> session-bound merge result,
        # so children bound to a transient parent are relinked before merge.
        replacements: dict[int, Any] = {}

        for table_name in topological_sort(ctx.dep_graph, ctx.scope_tables):
            result = ctx.local_results.get(table_name)
            if result is None:
                continue
            _emit(
                FlushStarted(table=table_name, count=len(result.instances)),
                ctx.on_event,
            )
            if self._on_conflict == "update":
                self._flush_update(
                    ctx, table_name, result, parent_attrs, replacements
                )
            else:
                self._flush_skip(ctx, table_name, result)

    def _flush_update(
        self,
        ctx: FlushContext,
        table_name: str,
        result: Any,
        parent_attrs: dict[str, list[str]],
        replacements: dict[int, Any],
    ) -> None:
        from etielle.telemetry import FlushCompleted, FlushFailed, _emit

        merged_count = 0
        try:
            for key, instance in result.instances.items():
                if isinstance(instance, dict):
                    continue
                for attr in parent_attrs.get(table_name, ()):
                    bound = getattr(instance, attr, None)
                    if bound is not None and id(bound) in replacements:
                        setattr(instance, attr, replacements[id(bound)])
                merged = ctx.session.merge(instance)
                if merged is not instance:
                    _replace_merged_instance(
                        result, key, instance, merged, replacements
                    )
                merged_count += 1
            ctx.session.flush()
        except Exception as e:
            _emit(
                FlushFailed(
                    table=table_name, error=str(e), affected_count=merged_count
                ),
                ctx.on_event,
            )
            if table_name in ctx.stats:
                ctx.builder._accumulate_stats(
                    ctx.stats, table_name, failed=merged_count
                )
            raise
        _emit(
            FlushCompleted(
                table=table_name,
                inserted=merged_count,
                failed=0,
                batch_num=1,
                batch_total=1,
                upsert=True,
            ),
            ctx.on_event,
        )
        if table_name in ctx.stats:
            ctx.builder._accumulate_stats(
                ctx.stats, table_name, inserted=merged_count
            )

    def _flush_skip(self, ctx: FlushContext, table_name: str, result: Any) -> None:
        from sqlalchemy.exc import IntegrityError

        from etielle.telemetry import FlushCompleted, FlushFailed, _emit

        inserted = 0
        try:
            for _key, instance in result.instances.items():
                if isinstance(instance, dict):
                    continue
                try:
                    with ctx.session.begin_nested():
                        ctx.session.add(instance)
                        ctx.session.flush()
                    inserted += 1
                except IntegrityError:
                    continue
        except Exception as e:
            _emit(
                FlushFailed(table=table_name, error=str(e), affected_count=inserted),
                ctx.on_event,
            )
            if table_name in ctx.stats:
                ctx.builder._accumulate_stats(ctx.stats, table_name, failed=inserted)
            raise
        _emit(
            FlushCompleted(
                table=table_name,
                inserted=inserted,
                failed=0,
                batch_num=1,
                batch_total=1,
                upsert=True,
            ),
            ctx.on_event,
        )
        if table_name in ctx.stats:
            ctx.builder._accumulate_stats(ctx.stats, table_name, inserted=inserted)


class BufferedKeyFlushStrategy:
    """Streaming strategy that merges late-arriving rows for recently seen keys.

    Keeps a bounded LRU cache of the last ``max_keys`` flushed
    ``(table, join key)`` -> instance entries. When a later chunk maps a row
    whose key is still cached, the row is *not* inserted again; instead its
    non-None scalar attribute values are copied onto the already-persisted
    instance, which SQLAlchemy turns into an UPDATE at the next flush. Rows
    with new keys are inserted normally and recorded in the cache.

    Children mapped alongside a re-appearing parent are relinked to the
    originally persisted parent instance, so no duplicate parent row is
    inserted through relationship cascades.

    Documented limitations:

    - **Correctness is a heuristic bounded by ``max_keys``.** The cache
      assumes key reappearance distance is bounded: once a key is evicted, a
      reappearing row is inserted as a new row (or raises ``IntegrityError``
      under a unique constraint). This is not a guarantee -- size the cache
      for the worst-case reappearance gap or fall back to
      ``ExternalPartitionChunkSource`` for exact grouping.
    - **Requires natural keys.** Only tables with ``join_on`` participate;
      auto-keyed rows are always inserted because auto keys restart per chunk
      and would collide spuriously.
    - **Merge is last-non-None-write-wins per scalar attribute.** Merge
      policies (``AddPolicy`` etc.) run only within a chunk's mapping pass;
      collection-valued attributes (e.g. ``backlink()`` lists) are not merged
      across chunks.
    - **Relationship completeness is still validated per chunk.** A child
      whose parent appears only in an earlier chunk is rejected before the
      strategy runs; the cache merges repeated *rows*, it does not relax the
      chunk contract for relationships.
    - **Stateful across the run.** Use a fresh instance per pipeline run;
      merged (deduplicated) rows are counted in ``mapped`` stats but not in
      ``inserted``.
    - **SQLAlchemy only**; plain-dict rows are not persisted, matching the
      default strategy.

    Args:
        max_keys: Maximum number of ``(table, key)`` entries to retain.
            Bounds strategy memory to at most ``max_keys`` live instances.
    """

    def __init__(self, max_keys: int = 10_000) -> None:
        if max_keys < 1:
            raise ValueError(f"max_keys must be >= 1, got {max_keys}")
        self._max_keys = max_keys
        self._cache: OrderedDict[tuple[str, tuple[Any, ...]], Any] = OrderedDict()

    def flush(self, ctx: FlushContext) -> None:
        if ctx.session is None:
            return
        if ctx.is_supabase:
            raise ValueError(
                "BufferedKeyFlushStrategy supports SQLAlchemy sessions only."
            )
        from etielle.telemetry import (
            FlushCompleted,
            FlushFailed,
            FlushStarted,
            _emit,
        )
        from etielle.utils import topological_sort

        parent_attrs: dict[str, list[str]] = {}
        for rel in ctx.link_to_rels:
            parent_attrs.setdefault(rel["child_table"], []).append(
                _parent_attr_name(rel["parent_table"])
            )

        # Maps id(duplicate chunk-local instance) -> cached persisted instance,
        # so children bound to a duplicate parent are relinked before insert.
        replacements: dict[int, Any] = {}

        for table_name in topological_sort(ctx.dep_graph, ctx.scope_tables):
            result = ctx.local_results.get(table_name)
            if result is None:
                continue
            _emit(
                FlushStarted(table=table_name, count=len(result.instances)),
                ctx.on_event,
            )
            inserted = 0
            try:
                for key, instance in result.instances.items():
                    if isinstance(instance, dict):
                        continue
                    for attr in parent_attrs.get(table_name, ()):
                        bound = getattr(instance, attr, None)
                        if bound is not None and id(bound) in replacements:
                            setattr(instance, attr, replacements[id(bound)])
                    if _is_auto_key(key):
                        ctx.session.add(instance)
                        inserted += 1
                        continue
                    cache_key = (table_name, key)
                    cached = self._cache.get(cache_key)
                    if cached is not None and cached is not instance:
                        _copy_instance_state(cached, instance)
                        replacements[id(instance)] = cached
                        self._cache.move_to_end(cache_key)
                    else:
                        ctx.session.add(instance)
                        inserted += 1
                        self._cache[cache_key] = instance
                        self._cache.move_to_end(cache_key)
                        while len(self._cache) > self._max_keys:
                            self._cache.popitem(last=False)
                ctx.session.flush()
            except Exception as e:
                _emit(
                    FlushFailed(
                        table=table_name, error=str(e), affected_count=inserted
                    ),
                    ctx.on_event,
                )
                if table_name in ctx.stats:
                    ctx.builder._accumulate_stats(
                        ctx.stats, table_name, failed=inserted
                    )
                raise
            _emit(
                FlushCompleted(
                    table=table_name,
                    inserted=inserted,
                    failed=0,
                    batch_num=1,
                    batch_total=1,
                    upsert=False,
                ),
                ctx.on_event,
            )
            if table_name in ctx.stats:
                ctx.builder._accumulate_stats(ctx.stats, table_name, inserted=inserted)
