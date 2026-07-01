---
name: etielle
description: >
  A declarative, type-safe Python DSL for mapping complex nested JSON to relational database schemas. Use when writing Python code that uses the etielle package.
compatibility: Requires Python >=3.13.
---

# etielle

A declarative, type-safe Python DSL for mapping complex nested JSON to relational database schemas

## Installation

```bash
pip install etielle
```

## API overview

### Classes

Main classes provided by the package

- `AddPolicy`
- `AppendPolicy`
- `CallableChunkSource`: Build chunks from a caller-supplied factory (tests and advanced callers)
- `ConstructorBuilder`: Simplified builder for classes that accept keyword arguments in their constructor
- `ExtendPolicy`
- `FirstNonNullPolicy`
- `GroupByChunkSource`: Group consecutive records that share a key into one chunk each
- `InstanceBuilder`
- `KeyCompleteFlushStrategy`: Default streaming strategy: plain insert/flush, no cross-chunk merge
- `MaxPolicy`
- `MergePolicy`
- `MinPolicy`
- `OneRecordPerChunkSource`: Wrap an iterable of JSON roots; each root becomes its own chunk
- `PipelineBuilder`: Fluent builder for E→T→L pipelines
- `PreSegmentedChunkSource`: Pass an already-segmented iterable of chunks through unchanged
- `PydanticBuilder`
- `PydanticPartialBuilder`
- `TypedDictBuilder`

### Dataclasses

Dataclass definitions

- `Chunk`: A key-complete batch of JSON roots to map together
- `Context`: Runtime context while traversing the JSON structure
- `CoreField`: Field(name: 'str', transform: 'Transform[Any]')
- `Field`: A field that will be persisted to the output table
- `FieldSpec`: FieldSpec(selector: Union[str, Callable[[+T], Any]], transform: Callable[[etielle.core.Context], Any])
- `FlushCompleted`: Emitted when flush completes for a table (or batch)
- `FlushContext`: Inputs for a flush at a component boundary
- `FlushFailed`: Emitted when an entire batch/table flush fails
- `FlushStarted`: Emitted when flush begins for a table
- `InstanceEmit`: InstanceEmit(table: str, join_keys: Sequence[Callable[[etielle.core.Context], Any]], fields: Sequence[etielle.instances.FieldSpec[+T]], builder: etielle.instances.InstanceBuilder[+T], policies: Mapping[str, etielle.instances.MergePolicy] = <factory>, strict_fields: bool = True, allow_extras: bool = False, strict_mode: str = 'collect_all', temp_fields: frozenset[str] = <factory>)
- `IterationLevel`: Represents a single level of iteration in a traversal
- `ManyToOneSpec`: Declarative specification for a many-to-one relationship
- `MapCompleted`: Emitted when mapping completes for a table
- `MapStarted`: Emitted when mapping begins for a table
- `MappingResult`: Unified result for both classic table rows and instance builders
- `MappingRuntimeState`: Shared mapping state across multiple roots in one chunk
- `MappingSpec`: MappingSpec(traversals: 'Sequence[TraversalSpec]')
- `PipelineResult`: Result from running a pipeline
- `TableEmit`: Describes how to produce rows for a table from a given traversal context
- `TableStats`: Statistics for a single table after pipeline execution
- `TelemetryEvent`: Base class for all telemetry events
- `TempField`: A field used only for joining/linking, not persisted
- `TraversalSpec`: How to reach and iterate a collection of nodes under root

### Protocols

Protocol / structural-typing interfaces

- `ChunkSource`: Produces key-complete chunks for streaming execution
- `FlushStrategy`: Defines persistence behavior at a chunk/component boundary

### ConstructorBuilder Methods

Methods for the ConstructorBuilder class

- `ConstructorBuilder.update`
- `ConstructorBuilder.finalize_all`
- `ConstructorBuilder.get`
- `ConstructorBuilder.known_fields`
- `ConstructorBuilder.update_errors`
- `ConstructorBuilder.finalize_errors`
- `ConstructorBuilder.record_update_error`
- `ConstructorBuilder.record_finalize_error`

### InstanceBuilder Methods

Methods for the InstanceBuilder class

- `InstanceBuilder.update`
- `InstanceBuilder.finalize_all`
- `InstanceBuilder.get`
- `InstanceBuilder.errors`
- `InstanceBuilder.known_fields`
- `InstanceBuilder.update_errors`
- `InstanceBuilder.finalize_errors`
- `InstanceBuilder.record_update_error`
- `InstanceBuilder.record_finalize_error`

### PipelineBuilder Methods

Methods for the PipelineBuilder class

- `PipelineBuilder.goto_root`
- `PipelineBuilder.goto`
- `PipelineBuilder.each`
- `PipelineBuilder.build_index`
- `PipelineBuilder.map_to`
- `PipelineBuilder.link_to`
- `PipelineBuilder.backlink`
- `PipelineBuilder.load`
- `PipelineBuilder.load_eager`
- `PipelineBuilder.run`

### PydanticBuilder Methods

Methods for the PydanticBuilder class

- `PydanticBuilder.known_fields`
- `PydanticBuilder.update`
- `PydanticBuilder.finalize_all`
- `PydanticBuilder.get`
- `PydanticBuilder.update_errors`
- `PydanticBuilder.finalize_errors`
- `PydanticBuilder.record_update_error`
- `PydanticBuilder.record_finalize_error`

### PydanticPartialBuilder Methods

Methods for the PydanticPartialBuilder class

- `PydanticPartialBuilder.known_fields`
- `PydanticPartialBuilder.update`
- `PydanticPartialBuilder.finalize_all`
- `PydanticPartialBuilder.get`
- `PydanticPartialBuilder.update_errors`
- `PydanticPartialBuilder.finalize_errors`
- `PydanticPartialBuilder.record_update_error`
- `PydanticPartialBuilder.record_finalize_error`

### TypedDictBuilder Methods

Methods for the TypedDictBuilder class

- `TypedDictBuilder.update`
- `TypedDictBuilder.finalize_all`
- `TypedDictBuilder.get`
- `TypedDictBuilder.known_fields`
- `TypedDictBuilder.update_errors`
- `TypedDictBuilder.finalize_errors`
- `TypedDictBuilder.record_update_error`
- `TypedDictBuilder.record_finalize_error`

### Exceptions

Exception classes

- `RelationshipIncompleteError`: Raised when a chunk lacks a required relationship target

### Functions

Utility functions

- `apply`: Apply a function to the result of another transform
- `bind_many_to_one`: Bind child -> parent object references in-place using plain attribute assignment
- `coalesce`
- `compute_relationship_keys`: Compute child->parent composite keys for each ManyToOneSpec by re-walking the
- `concat`
- `etl`: Entry point for fluent E→T→L pipelines
- `field_of`: Resolve a model field name from a type-checked selector lambda
- `format_id`
- `get`: Resolve a value relative to the current node using a dot-separated path
- `get_from_parent`
- `get_from_root`
- `index`
- `key`
- `len_of`
- `literal`
- `lookup`: Look up a value in a named index
- `node`: Return the current node value
- `parent_index`: Return the list index of an ancestor context
- `parent_key`
- `stream`: Entry point for streaming/chunked E→T→L pipelines
- `transform`: Decorator to create custom transforms with curried arguments
- `validate_relationship_completeness`: Ensure every child lookup in the chunk resolves within chunk or eager tables

## Resources

- [Full documentation](https://promptly-technologies-llc.github.io/etielle/)
- [llms.txt](llms.txt) — Indexed API reference for LLMs
- [llms-full.txt](llms-full.txt) — Comprehensive documentation for LLMs
