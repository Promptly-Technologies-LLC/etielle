"""etielle - declarative JSON-to-relational mapping."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("etielle")
except PackageNotFoundError:
    # Running from a source tree that was never installed.
    __version__ = "0.0.0.dev"

from .core import (
    Context,
    IterationLevel,
    MappingResult,
    MappingSpec,
    TableEmit,
    Transform,
    TraversalSpec,
    field_of,
)
from .core import (
    Field as CoreField,  # Renamed to avoid conflict with fluent.Field
)

# Fluent API (v3.0.0)
from .fluent import (
    ErrorMode,
    Field,
    FieldUnion,
    PipelineBuilder,
    PipelineResult,
    TableStats,
    TempField,
    etl,
    node,
    parent_index,
    stream,
    transform,
)
from .instances import (
    AddPolicy,
    AppendPolicy,
    ConstructorBuilder,
    ExtendPolicy,
    FieldSpec,
    FirstNonNullPolicy,
    InstanceBuilder,
    InstanceEmit,
    MaxPolicy,
    MergePolicy,
    MinPolicy,
    PydanticBuilder,
    PydanticPartialBuilder,
    TypedDictBuilder,
)

# Telemetry
from .telemetry import (
    FlushCompleted,
    FlushFailed,
    FlushStarted,
    MapCompleted,
    MapStarted,
    TelemetryCallback,
    TelemetryEvent,
    TelemetryEventTypes,
)

# Re-export transforms for fluent API
from .transforms import (
    apply,
    coalesce,
    concat,
    format_id,
    get,
    get_from_parent,
    get_from_root,
    index,
    key,
    len_of,
    literal,
    lookup,
    parent_key,
)

__all__ = [
    "AddPolicy",
    "AppendPolicy",
    "ConstructorBuilder",
    "Context",
    "CoreField",  # Legacy core Field
    "ErrorMode",
    "ExtendPolicy",
    "Field",
    "FieldSpec",
    "FieldUnion",
    "FirstNonNullPolicy",
    "FlushCompleted",
    "FlushFailed",
    "FlushStarted",
    "InstanceBuilder",
    "InstanceEmit",
    "IterationLevel",
    "MapCompleted",
    "MapStarted",
    "MappingResult",
    "MappingSpec",
    "MaxPolicy",
    "MergePolicy",
    "MinPolicy",
    "PipelineBuilder",
    "PipelineResult",
    "PydanticBuilder",
    "PydanticPartialBuilder",
    "TableEmit",
    "TableStats",
    "TelemetryCallback",
    "TelemetryEvent",
    "TelemetryEventTypes",
    "TempField",
    "Transform",
    "TraversalSpec",
    "TypedDictBuilder",
    "apply",
    "coalesce",
    "concat",
    "etl",
    "field_of",
    "format_id",
    "get",
    "get_from_parent",
    "get_from_root",
    "index",
    "key",
    "len_of",
    "literal",
    "lookup",
    "node",
    "parent_index",
    "parent_key",
    "stream",
    "transform",
]

from .chunking import (
    BufferedKeyFlushStrategy,
    CallableChunkSource,
    Chunk,
    ChunkSource,
    ExternalPartitionChunkSource,
    FlushContext,
    FlushStrategy,
    GroupByChunkSource,
    KeyCompleteFlushStrategy,
    OneRecordPerChunkSource,
    PreSegmentedChunkSource,
    UpsertFlushStrategy,
)
from .executor import MappingRuntimeState
from .relationships import (
    ManyToOneSpec,
    RelationshipIncompleteError,
    bind_many_to_one,
    compute_relationship_keys,
    validate_relationship_completeness,
)

__all__ += [
    "BufferedKeyFlushStrategy",
    "CallableChunkSource",
    "Chunk",
    "ChunkSource",
    "ExternalPartitionChunkSource",
    "FlushContext",
    "FlushStrategy",
    "GroupByChunkSource",
    "KeyCompleteFlushStrategy",
    "ManyToOneSpec",
    "MappingRuntimeState",
    "OneRecordPerChunkSource",
    "PreSegmentedChunkSource",
    "RelationshipIncompleteError",
    "UpsertFlushStrategy",
    "bind_many_to_one",
    "compute_relationship_keys",
    "validate_relationship_completeness",
]
