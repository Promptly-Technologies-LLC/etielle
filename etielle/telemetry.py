"""Telemetry events for pipeline execution monitoring.

This module provides event types that can be emitted during pipeline
execution for progress tracking, logging, and monitoring.

Example usage:
    >>> from etielle import etl
    >>> from etielle.telemetry import FlushCompleted
    >>>
    >>> events = []
    >>> result = (
    ...     etl(data)
    ...     .goto("users").each()
    ...     .map_to(User, fields=[...])
    ...     .load(session)
    ...     .run(on_event=events.append)
    ... )
    >>> flush_events = [e for e in events if isinstance(e, FlushCompleted)]
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Union


@dataclass
class TelemetryEvent:
    """Base class for all telemetry events."""

    table: str


@dataclass
class MapStarted(TelemetryEvent):
    """Emitted when mapping begins for a table."""

    pass


@dataclass
class MapCompleted(TelemetryEvent):
    """Emitted when mapping completes for a table.

    Attributes:
        table: The table name being mapped.
        count: Number of instances created.
        error_count: Number of validation/transform errors.
    """

    count: int
    error_count: int


@dataclass
class FlushStarted(TelemetryEvent):
    """Emitted when flush begins for a table.

    Attributes:
        table: The table name being flushed.
        count: Number of instances to flush.
    """

    count: int


@dataclass
class FlushCompleted(TelemetryEvent):
    """Emitted when flush completes for a table (or batch).

    Attributes:
        table: The table name being flushed.
        inserted: Number of rows successfully inserted/upserted.
        failed: Number of rows that failed.
        batch_num: Which batch (1-indexed).
        batch_total: Total number of batches for this table.
        upsert: True if this was an upsert operation.
    """

    inserted: int
    failed: int
    batch_num: int
    batch_total: int
    upsert: bool


@dataclass
class FlushFailed(TelemetryEvent):
    """Emitted when an entire batch/table flush fails.

    Attributes:
        table: The table name that failed.
        error: Error message describing the failure.
        affected_count: Number of rows that were in the failed batch.
    """

    error: str
    affected_count: int


# Union of all event types for type hints
TelemetryEventTypes = Union[
    MapStarted, MapCompleted, FlushStarted, FlushCompleted, FlushFailed
]

# Callback type for receiving telemetry events
TelemetryCallback = Callable[[TelemetryEventTypes], None]


def _emit(event: TelemetryEventTypes, callback: TelemetryCallback | None) -> None:
    """Safely emit a telemetry event to a callback.

    If the callback raises an exception, it is silently ignored to avoid
    interrupting pipeline execution.

    Args:
        event: The event to emit.
        callback: The callback to invoke, or None.
    """
    if callback is not None:
        try:
            callback(event)
        except Exception:
            pass  # Don't let callback errors break the pipeline
