from .core import (
    Context,
    Field,
    MappingSpec,
    TableEmit,
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
    MergePolicy,
    AddPolicy,
    AppendPolicy,
    ExtendPolicy,
    MinPolicy,
    MaxPolicy,
    FirstNonNullPolicy,
)

__all__ = [
    # core
    "Context",
    "Field",
    "MappingSpec",
    "TableEmit",
    "TraversalSpec",
    "field_of",
    # instances
    "InstanceEmit",
    "FieldSpec",
    "InstanceBuilder",
    "PydanticBuilder",
    "PydanticPartialBuilder",
    "TypedDictBuilder",
    "MergePolicy",
    "AddPolicy",
    "AppendPolicy",
    "ExtendPolicy",
    "MinPolicy",
    "MaxPolicy",
    "FirstNonNullPolicy",
]

# relationships (core)
from .relationships import ManyToOneSpec, compute_relationship_keys, bind_many_to_one

__all__ += [
    # relationships
    "ManyToOneSpec",
    "compute_relationship_keys",
    "bind_many_to_one",
]

