"""Tests for issue #87: Supabase FK population with load_eager() parents."""

from __future__ import annotations

from unittest.mock import MagicMock

from etielle import Field, TempField, get, stream


def _make_supabase_client(inserted_rows: dict[str, list[dict]]) -> MagicMock:
    client = MagicMock()
    client.__class__.__module__ = "supabase._sync.client"

    def make_table(table_name: str) -> MagicMock:
        table = MagicMock()

        def do_insert(batch: list[dict]) -> MagicMock:
            inserted_rows.setdefault(table_name, []).extend(batch)
            resp = MagicMock()
            resp.data = [
                {**row, "id": f"gen-{table_name}-{i}"} for i, row in enumerate(batch)
            ]
            exec_mock = MagicMock()
            exec_mock.execute.return_value = resp
            return exec_mock

        table.insert.side_effect = do_insert
        return table

    client.table.side_effect = make_table
    return client


class TestSupabaseEagerParentFkPopulation:
    def test_eager_parent_fk_populated_on_chunk_flush(self):
        """Chunk children get FK columns from eagerly flushed parent rows."""
        inserted_rows: dict[str, list[dict]] = {}
        client = _make_supabase_client(inserted_rows)

        eager = {"categories": [{"name": "books"}]}
        chunks = [{"products": [{"title": "p1", "category": "books"}]}]

        (
            stream(chunks, eager_roots=eager)
            .goto("categories")
            .each()
            .map_to(
                table="categories",
                fields=[Field("name", get("name")), TempField("_key", get("name"))],
            )
            .load_eager("categories")
            .goto_root()
            .goto("products")
            .each()
            .map_to(
                table="products",
                fields=[
                    Field("title", get("title")),
                    TempField("_cat", get("category")),
                ],
            )
            .link_to("categories", by={"_cat": "_key"}, fk={"category_id": "id"})
            .load(client)
            .run()
        )

        assert inserted_rows["categories"][0]["name"] == "books"
        assert inserted_rows["products"] == [
            {"title": "p1", "category_id": "gen-categories-0"}
        ]

    def test_eager_parent_fk_across_multiple_chunks(self):
        """Each chunk's children resolve FK against the shared eager parent index."""
        inserted_rows: dict[str, list[dict]] = {}
        client = _make_supabase_client(inserted_rows)

        eager = {
            "categories": [
                {"name": "books"},
                {"name": "music"},
            ]
        }
        chunks = [
            {"products": [{"title": "p1", "category": "books"}]},
            {"products": [{"title": "p2", "category": "music"}]},
        ]

        (
            stream(chunks, eager_roots=eager)
            .goto("categories")
            .each()
            .map_to(
                table="categories",
                fields=[Field("name", get("name")), TempField("_key", get("name"))],
            )
            .load_eager("categories")
            .goto_root()
            .goto("products")
            .each()
            .map_to(
                table="products",
                fields=[
                    Field("title", get("title")),
                    TempField("_cat", get("category")),
                ],
            )
            .link_to("categories", by={"_cat": "_key"}, fk={"category_id": "id"})
            .load(client)
            .run()
        )

        assert len(inserted_rows["categories"]) == 2
        assert inserted_rows["products"] == [
            {"title": "p1", "category_id": "gen-categories-0"},
            {"title": "p2", "category_id": "gen-categories-1"},
        ]
