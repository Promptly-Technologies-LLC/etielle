from .core import (
    Context,
    Field as CoreField,  # Renamed to avoid conflict with fluent.Field
    MappingResult,
    MappingSpec,
    TableEmit,
    Transform,
    TraversalSpec,
    field_of,
)

from .instances import (
    InstanceEmit,
    FieldSpec,
    InstanceBuilder,
    PydanticBuilder,
    PydanticPartialBuilder,
    TypedDictBuilder,
    ConstructorBuilder,
    MergePolicy,
    AddPolicy,
    AppendPolicy,
    ExtendPolicy,
    MinPolicy,
    MaxPolicy,
    FirstNonNullPolicy,
)

# Fluent API (v3.0.0)
from .fluent import (
    etl,
    ErrorMode,
    Field,
    TempField,
    FieldUnion,
    transform,
    node,
    parent_index,
    PipelineResult,
    PipelineBuilder,
    TableStats,
)

# Telemetry
from .telemetry import (
    TelemetryEvent,
    TelemetryEventTypes,
    TelemetryCallback,
    MapStarted,
    MapCompleted,
    FlushStarted,
    FlushCompleted,
    FlushFailed,
)

# Re-export transforms for fluent API
from .transforms import (
    apply,
    get,
    get_from_root,
    get_from_parent,
    literal,
    concat,
    coalesce,
    format_id,
    key,
    index,
    parent_key,
    len_of,
)

__all__ = [
    # core
    "Context",
    "CoreField",  # Legacy core Field
    "MappingResult",
    "MappingSpec",
    "TableEmit",
    "Transform",
    "TraversalSpec",
    "field_of",
    # instances
    "InstanceEmit",
    "FieldSpec",
    "InstanceBuilder",
    "PydanticBuilder",
    "PydanticPartialBuilder",
    "TypedDictBuilder",
    "ConstructorBuilder",
    "MergePolicy",
    "AddPolicy",
    "AppendPolicy",
    "ExtendPolicy",
    "MinPolicy",
    "MaxPolicy",
    "FirstNonNullPolicy",
    # Fluent API (v3.0.0)
    "etl",
    "ErrorMode",
    "Field",
    "TempField",
    "FieldUnion",
    "transform",
    "PipelineResult",
    "PipelineBuilder",
    "TableStats",
    # Telemetry
    "TelemetryEvent",
    "TelemetryEventTypes",
    "TelemetryCallback",
    "MapStarted",
    "MapCompleted",
    "FlushStarted",
    "FlushCompleted",
    "FlushFailed",
    # Transforms
    "apply",
    "get",
    "get_from_root",
    "get_from_parent",
    "literal",
    "concat",
    "coalesce",
    "format_id",
    "key",
    "index",
    "parent_key",
    "parent_index",
    "node",
    "len_of",
]

# relationships (core)
from .relationships import ManyToOneSpec, compute_relationship_keys, bind_many_to_one

__all__ += [
    # relationships
    "ManyToOneSpec",
    "compute_relationship_keys",
    "bind_many_to_one",
]
