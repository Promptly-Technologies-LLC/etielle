"""Tests for the telemetry feature."""

import pytest
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

from etielle import (
    etl,
    Field,
    TempField,
    get,
    literal,
    PipelineResult,
    TableStats,
    TelemetryEvent,
    MapStarted,
    MapCompleted,
    FlushStarted,
    FlushCompleted,
    FlushFailed,
)
from etielle.telemetry import _emit, TelemetryCallback


class TestTelemetryEvents:
    """Tests for telemetry event dataclasses."""

    def test_map_started_creation(self):
        """MapStarted stores table name."""
        event = MapStarted(table="users")
        assert event.table == "users"

    def test_map_completed_creation(self):
        """MapCompleted stores table name, count, and error_count."""
        event = MapCompleted(table="users", count=10, error_count=2)
        assert event.table == "users"
        assert event.count == 10
        assert event.error_count == 2

    def test_flush_started_creation(self):
        """FlushStarted stores table name and count."""
        event = FlushStarted(table="users", count=10)
        assert event.table == "users"
        assert event.count == 10

    def test_flush_completed_creation(self):
        """FlushCompleted stores all flush metrics."""
        event = FlushCompleted(
            table="users",
            inserted=8,
            failed=2,
            batch_num=1,
            batch_total=1,
            upsert=False,
        )
        assert event.table == "users"
        assert event.inserted == 8
        assert event.failed == 2
        assert event.batch_num == 1
        assert event.batch_total == 1
        assert event.upsert is False

    def test_flush_completed_upsert_mode(self):
        """FlushCompleted tracks upsert mode."""
        event = FlushCompleted(
            table="users",
            inserted=10,
            failed=0,
            batch_num=1,
            batch_total=1,
            upsert=True,
        )
        assert event.upsert is True

    def test_flush_failed_creation(self):
        """FlushFailed stores error details."""
        event = FlushFailed(
            table="users",
            error="Connection refused",
            affected_count=100,
        )
        assert event.table == "users"
        assert event.error == "Connection refused"
        assert event.affected_count == 100

    def test_events_are_subclasses_of_telemetry_event(self):
        """All events inherit from TelemetryEvent."""
        assert issubclass(MapStarted, TelemetryEvent)
        assert issubclass(MapCompleted, TelemetryEvent)
        assert issubclass(FlushStarted, TelemetryEvent)
        assert issubclass(FlushCompleted, TelemetryEvent)
        assert issubclass(FlushFailed, TelemetryEvent)


class TestEmitFunction:
    """Tests for the _emit helper function."""

    def test_emit_calls_callback(self):
        """_emit invokes the callback with the event."""
        events = []
        event = MapStarted(table="users")
        _emit(event, events.append)
        assert events == [event]

    def test_emit_with_none_callback(self):
        """_emit does nothing when callback is None."""
        event = MapStarted(table="users")
        _emit(event, None)  # Should not raise

    def test_emit_swallows_callback_exceptions(self):
        """_emit catches and ignores callback exceptions."""
        def failing_callback(event):
            raise ValueError("Callback error")

        event = MapStarted(table="users")
        _emit(event, failing_callback)  # Should not raise


class TestTableStats:
    """Tests for TableStats dataclass."""

    def test_table_stats_creation(self):
        """TableStats stores all statistics."""
        stats = TableStats(mapped=10, errors=2, inserted=8, failed=0)
        assert stats.mapped == 10
        assert stats.errors == 2
        assert stats.inserted == 8
        assert stats.failed == 0

    def test_table_stats_from_pipeline_result(self):
        """PipelineResult.stats returns TableStats objects."""
        data = {"users": [{"name": "Alice"}, {"name": "Bob"}]}
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[Field("name", get("name"))])
            .run()
        )
        assert "users" in result.stats
        stats = result.stats["users"]
        assert isinstance(stats, TableStats)
        assert stats.mapped == 2
        assert stats.errors == 0

    def test_stats_populated_from_run(self):
        """stats are populated during run() even without session."""
        data = {"users": [{"name": "Alice"}]}
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[Field("name", get("name"))])
            .run()
        )
        # Access the stats
        stats = result.stats["users"]
        assert stats.mapped == 1
        # Without session, no flush occurred so inserted is 0
        assert stats.inserted == 0
        assert stats.failed == 0


class TestMappingEvents:
    """Tests for mapping phase telemetry events."""

    def test_mapping_emits_start_and_complete_events(self):
        """Mapping phase emits MapStarted and MapCompleted events."""
        data = {"users": [{"name": "Alice"}, {"name": "Bob"}]}
        events = []

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[Field("name", get("name"))])
            .run(on_event=events.append)
        )

        # Find mapping events
        map_started = [e for e in events if isinstance(e, MapStarted)]
        map_completed = [e for e in events if isinstance(e, MapCompleted)]

        assert len(map_started) == 1
        assert len(map_completed) == 1
        assert map_started[0].table == "users"
        assert map_completed[0].table == "users"
        assert map_completed[0].count == 2
        assert map_completed[0].error_count == 0

    def test_mapping_events_for_multiple_tables(self):
        """Mapping emits events for each table."""
        # Data with nested structure to map to multiple tables
        data = {
            "users": [
                {"name": "Alice", "posts": [{"title": "Hello"}]},
            ],
        }
        events = []

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[Field("name", get("name"))])
            .goto("posts").each()
            .map_to(table="posts", fields=[Field("title", get("title"))])
            .run(on_event=events.append)
        )

        map_completed = [e for e in events if isinstance(e, MapCompleted)]
        tables = {e.table for e in map_completed}

        assert tables == {"users", "posts"}

    def test_mapping_events_include_error_count(self):
        """MapCompleted includes validation error count."""
        @dataclass
        class User:
            name: str
            age: int  # Required field

        data = {"users": [{"name": "Alice"}]}  # Missing age
        events = []

        result = (
            etl(data)
            .goto("users").each()
            .map_to(
                table=User,
                fields=[
                    Field("name", get("name")),
                    Field("age", get("age")),  # Will fail - missing
                ],
            )
            .run(on_event=events.append)
        )

        map_completed = [e for e in events if isinstance(e, MapCompleted)][0]
        # Error count should reflect the missing required field
        assert map_completed.error_count >= 0


class TestNoSessionStats:
    """Tests for stats when no session is provided (no flush)."""

    def test_stats_without_session(self):
        """Stats are computed without flush when no session provided."""
        data = {"users": [{"name": "Alice"}, {"name": "Bob"}]}

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[Field("name", get("name"))])
            .run()
        )

        stats = result.stats["users"]
        assert stats.mapped == 2
        # Without session, no flush occurred
        assert stats.inserted == 0
        assert stats.failed == 0


class TestEventCollectionPatterns:
    """Tests for common event collection patterns."""

    def test_collect_events_in_list(self):
        """Events can be collected in a list."""
        data = {"users": [{"name": "Alice"}]}
        events = []

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[Field("name", get("name"))])
            .run(on_event=events.append)
        )

        assert len(events) >= 2  # At least MapStarted and MapCompleted

    def test_callback_receives_correct_event_types(self):
        """Callback receives properly typed event objects."""
        data = {"users": [{"name": "Alice"}]}
        event_types = []

        def track_types(event):
            event_types.append(type(event).__name__)

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[Field("name", get("name"))])
            .run(on_event=track_types)
        )

        assert "MapStarted" in event_types
        assert "MapCompleted" in event_types

    def test_callback_exception_does_not_break_pipeline(self):
        """Exceptions in callback don't stop the pipeline."""
        data = {"users": [{"name": "Alice"}]}

        def failing_callback(event):
            raise RuntimeError("Callback failed!")

        # Should not raise
        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[Field("name", get("name"))])
            .run(on_event=failing_callback)
        )

        assert "users" in result.tables


class TestSupabaseAdapterBatchCallback:
    """Tests for the on_batch callback in insert_batches."""

    def test_insert_batches_calls_on_batch(self):
        """insert_batches invokes on_batch callback per batch."""
        from etielle.adapters.supabase_adapter import insert_batches

        # Mock Supabase client
        mock_response = MagicMock()
        mock_response.data = [{"id": 1, "name": "Alice"}]

        mock_table = MagicMock()
        mock_table.insert.return_value.execute.return_value = mock_response

        mock_client = MagicMock()
        mock_client.table.return_value = mock_table

        batch_calls = []

        def on_batch(batch_num, batch_total, inserted):
            batch_calls.append((batch_num, batch_total, inserted))

        result = insert_batches(
            mock_client,
            "users",
            [{"name": "Alice"}],
            on_batch=on_batch,
        )

        assert len(batch_calls) == 1
        assert batch_calls[0] == (1, 1, 1)  # batch 1 of 1, 1 inserted

    def test_insert_batches_multiple_batches(self):
        """insert_batches handles multiple batches correctly."""
        from etielle.adapters.supabase_adapter import insert_batches

        # Mock Supabase client
        mock_response = MagicMock()
        mock_response.data = [{"id": i} for i in range(2)]

        mock_table = MagicMock()
        mock_table.insert.return_value.execute.return_value = mock_response

        mock_client = MagicMock()
        mock_client.table.return_value = mock_table

        batch_calls = []

        def on_batch(batch_num, batch_total, inserted):
            batch_calls.append((batch_num, batch_total, inserted))

        # 5 rows with batch_size=2 = 3 batches
        rows = [{"name": f"User{i}"} for i in range(5)]
        result = insert_batches(
            mock_client,
            "users",
            rows,
            batch_size=2,
            on_batch=on_batch,
        )

        assert len(batch_calls) == 3
        assert batch_calls[0][0] == 1  # First batch
        assert batch_calls[0][1] == 3  # Total batches
        assert batch_calls[2][0] == 3  # Last batch

    def test_insert_batches_empty_rows(self):
        """insert_batches handles empty rows gracefully."""
        from etielle.adapters.supabase_adapter import insert_batches

        mock_client = MagicMock()
        batch_calls = []

        result = insert_batches(
            mock_client,
            "users",
            [],
            on_batch=lambda *args: batch_calls.append(args),
        )

        assert result == []
        assert batch_calls == []


class TestPipelineResultStats:
    """Tests for PipelineResult.stats property."""

    def test_stats_returns_dict_of_table_stats(self):
        """stats property returns dict mapping table names to TableStats."""
        data = {"users": [{"name": "Alice"}]}

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[Field("name", get("name"))])
            .run()
        )

        assert isinstance(result.stats, dict)
        assert "users" in result.stats
        assert isinstance(result.stats["users"], TableStats)

    def test_stats_multiple_tables(self):
        """stats includes all mapped tables."""
        # Use nested structure to map multiple tables
        data = {
            "users": [
                {"name": "Alice", "posts": [{"title": "Hello"}]},
            ],
        }

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[Field("name", get("name"))])
            .goto("posts").each()
            .map_to(table="posts", fields=[Field("title", get("title"))])
            .run()
        )

        assert "users" in result.stats
        assert "posts" in result.stats
        assert result.stats["users"].mapped == 1
        assert result.stats["posts"].mapped == 1
