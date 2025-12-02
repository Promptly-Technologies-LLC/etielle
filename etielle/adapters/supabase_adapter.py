"""Supabase adapter for etielle.

Provides functions for inserting pipeline results to Supabase.
"""

from __future__ import annotations

from typing import Any, Sequence


def insert_batches(
    client: Any,
    table_name: str,
    rows: Sequence[dict[str, Any]],
    *,
    upsert: bool = False,
    batch_size: int = 1000,
) -> list[dict[str, Any]]:
    """Insert rows to a Supabase table in batches.

    Args:
        client: Supabase client instance.
        table_name: Name of the table to insert into.
        rows: List of row dicts to insert.
        upsert: If True, use upsert instead of insert.
        batch_size: Maximum rows per batch.

    Returns:
        List of inserted/upserted rows from Supabase response.

    Raises:
        Exception: If Supabase returns an error.
    """
    if not rows:
        return []

    results: list[dict[str, Any]] = []

    # Process in batches
    for i in range(0, len(rows), batch_size):
        batch = list(rows[i : i + batch_size])

        table = client.table(table_name)

        if upsert:
            response = table.upsert(batch).execute()
        else:
            response = table.insert(batch).execute()

        if response.data:
            results.extend(response.data)

    return results
