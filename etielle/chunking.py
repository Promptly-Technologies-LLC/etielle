"""Chunking and flush strategy interfaces for streaming execution."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from itertools import groupby
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

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
        For unsorted input, sort by ``key`` first, or wait for the robust
        unsorted-input partitioner (H2, tracked under sub-issue D).

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
