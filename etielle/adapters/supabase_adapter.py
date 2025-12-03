"""Supabase adapter for etielle.

Provides functions for inserting pipeline results to Supabase.
"""

from __future__ import annotations

from typing import Any, Callable, Sequence

# Callback type for batch progress: (batch_num, batch_total, rows_inserted) -> None
BatchCallback = Callable[[int, int, int], None]


def insert_batches(
    client: Any,
    table_name: str,
    rows: Sequence[dict[str, Any]],
    *,
    upsert: bool = False,
    on_conflict: str | None = None,
    batch_size: int = 1000,
    on_batch: BatchCallback | None = None,
) -> list[dict[str, Any]]:
    """Insert rows to a Supabase table in batches.

    Args:
        client: Supabase client instance.
        table_name: Name of the table to insert into.
        rows: List of row dicts to insert.
        upsert: If True, use upsert instead of insert.
        on_conflict: Column(s) to use for conflict detection in upsert.
            For composite keys, use comma-separated string (e.g., "user_id,slug").
        batch_size: Maximum rows per batch.
        on_batch: Optional callback invoked after each batch completes.
            Called with (batch_num, batch_total, rows_inserted).

    Returns:
        List of inserted/upserted rows from Supabase response.

    Raises:
        Exception: If Supabase returns an error.
    """
    if not rows:
        return []

    results: list[dict[str, Any]] = []
    total_batches = (len(rows) + batch_size - 1) // batch_size

    # Process in batches
    for batch_num, i in enumerate(range(0, len(rows), batch_size), start=1):
        batch = list(rows[i : i + batch_size])

        table = client.table(table_name)

        if upsert:
            response = table.upsert(batch, on_conflict=on_conflict).execute()
        else:
            response = table.insert(batch).execute()

        batch_inserted = 0
        if response.data:
            results.extend(response.data)
            batch_inserted = len(response.data)

        if on_batch is not None:
            try:
                on_batch(batch_num, total_batches, batch_inserted)
            except Exception:
                pass  # Don't let callback errors break the insert

    return results
