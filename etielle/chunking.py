"""Chunking and flush strategy interfaces for streaming execution."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
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


@dataclass
class FlushContext:
    """Inputs for a flush at a component boundary."""

    scope_tables: set[str]
    bind_context: dict[str, Any]
    local_results: dict[str, Any]
    dep_graph: dict[str, set[str]]
    link_to_rels: list[dict[str, Any]]
    backlink_rels: list[dict[str, Any]]
    stats: dict[str, TableStats]
    on_event: TelemetryCallback | None
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
        builder = ctx.builder
        if builder._session is None:
            return
        if builder._is_supabase_client(builder._session):
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
            builder._flush_to_supabase(
                component_tables_dict,
                component_order,
                child_lookup,
                ctx.stats,
                ctx.on_event,
            )
        else:
            builder._flush_sqlalchemy_scope(
                ctx.scope_tables,
                ctx.bind_context,
                ctx.dep_graph,
                ctx.link_to_rels,
                ctx.backlink_rels,
                ctx.stats,
                ctx.on_event,
            )
