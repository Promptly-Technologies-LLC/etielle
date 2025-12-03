"""Integration tests for Supabase adapter.

These tests require a running Supabase instance. Set environment variables:
- SUPABASE_URL: Your Supabase project URL
- SUPABASE_KEY: Your Supabase anon/service key

To run locally:
    supabase start
    export SUPABASE_URL=http://localhost:54321
    export SUPABASE_KEY=<your-anon-key>
    pytest tests/test_supabase_adapter.py -v
"""

import os
import pytest
from unittest.mock import MagicMock

from etielle import etl, Field, TempField, get, get_from_parent


# Skip all tests if Supabase env vars not set
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

requires_supabase = pytest.mark.skipif(
    not (SUPABASE_URL and SUPABASE_KEY),
    reason="SUPABASE_URL and SUPABASE_KEY environment variables not set"
)


@pytest.fixture
def supabase_client():
    """Create a Supabase client for testing."""
    from supabase import create_client
    assert SUPABASE_URL is not None
    assert SUPABASE_KEY is not None
    return create_client(SUPABASE_URL, SUPABASE_KEY)


@pytest.fixture
def mock_supabase_client():
    """Create a mock Supabase client for unit tests."""
    mock = MagicMock()
    # Make it look like a Supabase client for type detection
    mock.__class__.__module__ = "supabase._sync.client"
    mock.__class__.__name__ = "SyncClient"
    return mock


class TestSupabaseTypeDetection:
    """Test that .load() correctly detects Supabase clients."""

    def test_detects_supabase_client(self, mock_supabase_client):
        """Should detect mock Supabase client and use Supabase flush logic."""
        data = {"users": [{"id": "u1", "name": "Alice"}]}

        # This should detect the Supabase client and attempt to flush
        # For now, it will fail because _flush_to_supabase doesn't exist
        pipeline = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[
                Field("id", get("id")),
                Field("name", get("name")),
                TempField("id", get("id")),
            ])
            .load(mock_supabase_client)
        )

        # Check that the client was stored
        assert pipeline._session is mock_supabase_client

    def test_detects_supabase_client_with_options(self, mock_supabase_client):
        """Should store upsert and batch_size options."""
        data = {"users": [{"id": "u1", "name": "Alice"}]}

        pipeline = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[
                Field("id", get("id")),
                Field("name", get("name")),
                TempField("id", get("id")),
            ])
            .load(mock_supabase_client, upsert=True, batch_size=500)
        )

        assert pipeline._session is mock_supabase_client
        assert pipeline._upsert is True
        assert pipeline._batch_size == 500


class TestSupabaseFlush:
    """Test that .run() correctly flushes to Supabase."""

    def test_single_table_insert(self, mock_supabase_client):
        """Should insert single table data to Supabase."""
        data = {"users": [
            {"id": "u1", "name": "Alice"},
            {"id": "u2", "name": "Bob"},
        ]}

        # Configure mock to return success
        mock_supabase_client.table.return_value.insert.return_value.execute.return_value.data = [
            {"id": "u1", "name": "Alice"},
            {"id": "u2", "name": "Bob"},
        ]

        result = (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[
                Field("id", get("id")),
                Field("name", get("name")),
                TempField("id", get("id")),
            ])
            .load(mock_supabase_client)
            .run()
        )

        # Verify the mock was called correctly
        mock_supabase_client.table.assert_called_with("users")
        mock_supabase_client.table.return_value.insert.assert_called_once()

        # Check that we got results back
        assert "users" in result.tables
        assert len(result.tables["users"]) == 2

    def test_multi_table_insert_with_dependency_order(self, mock_supabase_client):
        """Should insert parent tables before child tables."""
        # Nested data structure - posts under users
        data = {
            "users": [{
                "id": "u1",
                "name": "Alice",
                "posts": [{"id": "p1", "title": "Hello"}]
            }],
        }

        # Track call order
        call_order = []

        def track_table(table_name):
            call_order.append(table_name)
            mock_table = MagicMock()
            mock_table.insert.return_value.execute.return_value.data = []
            return mock_table

        mock_supabase_client.table.side_effect = track_table

        (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[
                Field("id", get("id")),
                Field("name", get("name")),
                TempField("id", get("id")),
            ])
            .goto("posts").each()
            .map_to(table="posts", fields=[
                Field("id", get("id")),
                Field("title", get("title")),
                Field("user_id", get_from_parent("id")),
                TempField("id", get("id")),
                TempField("user_id", get_from_parent("id")),
            ])
            .link_to("users", by={"user_id": "id"})
            .load(mock_supabase_client)
            .run()
        )

        # Users should be inserted before posts
        assert call_order.index("users") < call_order.index("posts")

    def test_upsert_mode(self, mock_supabase_client):
        """Should use upsert when upsert=True."""
        data = {"users": [{"id": "u1", "name": "Alice"}]}

        mock_supabase_client.table.return_value.upsert.return_value.execute.return_value.data = [
            {"id": "u1", "name": "Alice"},
        ]

        (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[
                Field("id", get("id")),
                Field("name", get("name")),
                TempField("id", get("id")),
            ])
            .load(mock_supabase_client, upsert=True)
            .run()
        )

        # Should call upsert, not insert
        mock_supabase_client.table.return_value.upsert.assert_called_once()
        mock_supabase_client.table.return_value.insert.assert_not_called()

    def test_upsert_with_per_table_conflict_columns(self, mock_supabase_client):
        """Should pass on_conflict parameter when upsert is a dict."""
        data = {
            "users": [{"id": "u1", "email": "alice@example.com", "name": "Alice"}],
            "posts": [{"id": "p1", "user_id": "u1", "slug": "hello", "title": "Hello"}],
        }

        # Track upsert calls with their on_conflict arguments
        upsert_calls = {}

        def track_table(table_name):
            mock_table = MagicMock()

            def track_upsert(rows, on_conflict=None):
                upsert_calls[table_name] = {"rows": rows, "on_conflict": on_conflict}
                mock_response = MagicMock()
                mock_response.execute.return_value.data = rows
                return mock_response

            mock_table.upsert.side_effect = track_upsert
            return mock_table

        mock_supabase_client.table.side_effect = track_table

        (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[
                Field("id", get("id")),
                Field("email", get("email")),
                Field("name", get("name")),
            ])
            .goto_root()
            .goto("posts").each()
            .map_to(table="posts", fields=[
                Field("id", get("id")),
                Field("user_id", get("user_id")),
                Field("slug", get("slug")),
                Field("title", get("title")),
            ])
            .load(mock_supabase_client, upsert=True, upsert_on={
                "users": "email",
                "posts": ["user_id", "slug"],
            })
            .run()
        )

        # Verify on_conflict was passed correctly
        assert "users" in upsert_calls
        assert upsert_calls["users"]["on_conflict"] == "email"

        assert "posts" in upsert_calls
        assert upsert_calls["posts"]["on_conflict"] == "user_id,slug"

    def test_batching(self, mock_supabase_client):
        """Should batch inserts according to batch_size."""
        # Create 5 users, batch size 2 = 3 batches
        data = {"users": [{"id": f"u{i}", "name": f"User{i}"} for i in range(5)]}

        mock_supabase_client.table.return_value.insert.return_value.execute.return_value.data = []

        (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[
                Field("id", get("id")),
                Field("name", get("name")),
                TempField("id", get("id")),
            ])
            .load(mock_supabase_client, batch_size=2)
            .run()
        )

        # Should have called insert 3 times (2 + 2 + 1)
        assert mock_supabase_client.table.return_value.insert.call_count == 3

    def test_two_phase_insert_with_fk_parameter(self, mock_supabase_client):
        """Should populate child FK columns with DB-generated parent IDs."""
        # Parent has no ID in JSON - will be DB-generated
        data = {
            "users": [
                {"name": "Alice"},
                {"name": "Bob"},
            ],
        }

        # Track insert calls and simulate DB-generated IDs
        insert_calls = {}

        def track_table(table_name):
            mock_table = MagicMock()

            def track_insert(rows):
                insert_calls[table_name] = insert_calls.get(table_name, [])
                insert_calls[table_name].append(rows)

                # Simulate DB-generated UUIDs for users
                if table_name == "users":
                    returned = [
                        {**row, "id": f"generated-uuid-{i}"}
                        for i, row in enumerate(rows)
                    ]
                else:
                    returned = rows

                mock_response = MagicMock()
                mock_response.execute.return_value.data = returned
                return mock_response

            mock_table.insert.side_effect = track_insert
            return mock_table

        mock_supabase_client.table.side_effect = track_table

        (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[
                Field("name", get("name")),
                TempField("_key", get("name")),  # Business key for matching
            ])
            .goto("posts").each()
            .map_to(table="posts", fields=[
                Field("title", get("title")),
                TempField("_parent_key", get_from_parent("name")),
            ])
            .link_to("users", by={"_parent_key": "_key"}, fk={"user_id": "id"})
            .load(mock_supabase_client)
            .run()
        )

        # Verify users were inserted first
        assert "users" in insert_calls
        assert len(insert_calls["users"]) == 1
        users_inserted = insert_calls["users"][0]
        assert len(users_inserted) == 2
        assert users_inserted[0]["name"] == "Alice"
        assert users_inserted[1]["name"] == "Bob"

    def test_two_phase_insert_populates_child_fk(self, mock_supabase_client):
        """Should populate child FK columns with DB-generated parent IDs."""
        # Nested data - users with posts
        data = {
            "users": [
                {"name": "Alice", "posts": [{"title": "Hello"}, {"title": "World"}]},
                {"name": "Bob", "posts": [{"title": "Goodbye"}]},
            ],
        }

        # Track insert calls and simulate DB-generated IDs
        insert_calls = {}

        def track_table(table_name):
            mock_table = MagicMock()

            def track_insert(rows):
                insert_calls[table_name] = insert_calls.get(table_name, [])
                insert_calls[table_name].append(list(rows))  # Copy the rows

                # Simulate DB-generated UUIDs for users
                if table_name == "users":
                    returned = [
                        {**row, "id": f"generated-uuid-{i}"}
                        for i, row in enumerate(rows)
                    ]
                else:
                    returned = rows

                mock_response = MagicMock()
                mock_response.execute.return_value.data = returned
                return mock_response

            mock_table.insert.side_effect = track_insert
            return mock_table

        mock_supabase_client.table.side_effect = track_table

        (
            etl(data)
            .goto("users").each()
            .map_to(table="users", fields=[
                Field("name", get("name")),
                TempField("_key", get("name")),
            ])
            .goto("posts").each()
            .map_to(table="posts", fields=[
                Field("title", get("title")),
                TempField("_parent_key", get_from_parent("name")),
            ])
            .link_to("users", by={"_parent_key": "_key"}, fk={"user_id": "id"})
            .load(mock_supabase_client)
            .run()
        )

        # Verify posts were inserted with correct user_id FK
        assert "posts" in insert_calls
        posts_inserted = insert_calls["posts"][0]

        # Alice's posts should have user_id = generated-uuid-0
        alice_posts = [p for p in posts_inserted if p.get("user_id") == "generated-uuid-0"]
        assert len(alice_posts) == 2
        assert {p["title"] for p in alice_posts} == {"Hello", "World"}

        # Bob's posts should have user_id = generated-uuid-1
        bob_posts = [p for p in posts_inserted if p.get("user_id") == "generated-uuid-1"]
        assert len(bob_posts) == 1
        assert bob_posts[0]["title"] == "Goodbye"


@requires_supabase
class TestSupabaseIntegration:
    """Integration tests against a real Supabase instance."""

    @pytest.fixture(autouse=True)
    def setup_tables(self, supabase_client):
        """Clean up test tables before each test."""
        # Delete any existing test data
        try:
            supabase_client.table("test_posts").delete().neq("id", "").execute()
        except Exception:
            pass
        try:
            supabase_client.table("test_users").delete().neq("id", "").execute()
        except Exception:
            pass
        yield
        # Cleanup after test
        try:
            supabase_client.table("test_posts").delete().neq("id", "").execute()
        except Exception:
            pass
        try:
            supabase_client.table("test_users").delete().neq("id", "").execute()
        except Exception:
            pass

    def test_real_insert(self, supabase_client):
        """Test actual insert to Supabase."""
        data = {"users": [
            {"id": "test_u1", "name": "Alice"},
            {"id": "test_u2", "name": "Bob"},
        ]}

        (
            etl(data)
            .goto("users").each()
            .map_to(table="test_users", fields=[
                Field("id", get("id")),
                Field("name", get("name")),
                TempField("id", get("id")),
            ])
            .load(supabase_client)
            .run()
        )

        # Verify data was inserted
        response = supabase_client.table("test_users").select("*").execute()
        assert len(response.data) == 2
        names = {r["name"] for r in response.data}
        assert names == {"Alice", "Bob"}

    def test_real_upsert(self, supabase_client):
        """Test actual upsert to Supabase."""
        # First insert
        data1 = {"users": [{"id": "test_u1", "name": "Alice"}]}

        (
            etl(data1)
            .goto("users").each()
            .map_to(table="test_users", fields=[
                Field("id", get("id")),
                Field("name", get("name")),
                TempField("id", get("id")),
            ])
            .load(supabase_client)
            .run()
        )

        # Upsert with updated name
        data2 = {"users": [{"id": "test_u1", "name": "Alice Updated"}]}

        (
            etl(data2)
            .goto("users").each()
            .map_to(table="test_users", fields=[
                Field("id", get("id")),
                Field("name", get("name")),
                TempField("id", get("id")),
            ])
            .load(supabase_client, upsert=True)
            .run()
        )

        # Verify upsert worked
        response = supabase_client.table("test_users").select("*").execute()
        assert len(response.data) == 1
        assert response.data[0]["name"] == "Alice Updated"

    def test_real_upsert_with_custom_conflict_column(self, supabase_client):
        """Test upsert with custom on_conflict column against real Supabase."""
        # Clean up test_users_email table
        try:
            supabase_client.table("test_users_email").delete().neq("id", "").execute()
        except Exception:
            pass

        # First insert
        data1 = {"users": [
            {"id": "test_u1", "email": "alice@example.com", "name": "Alice"},
        ]}

        (
            etl(data1)
            .goto("users").each()
            .map_to(table="test_users_email", fields=[
                Field("id", get("id")),
                Field("email", get("email")),
                Field("name", get("name")),
            ])
            .load(supabase_client)
            .run()
        )

        # Upsert with DIFFERENT id but SAME email - should update based on email
        data2 = {"users": [
            {"id": "test_u2", "email": "alice@example.com", "name": "Alice Updated"},
        ]}

        (
            etl(data2)
            .goto("users").each()
            .map_to(table="test_users_email", fields=[
                Field("id", get("id")),
                Field("email", get("email")),
                Field("name", get("name")),
            ])
            .load(supabase_client, upsert=True, upsert_on={"test_users_email": "email"})
            .run()
        )

        # Verify upsert worked - should still be 1 row, updated name
        response = supabase_client.table("test_users_email").select("*").execute()
        assert len(response.data) == 1
        assert response.data[0]["name"] == "Alice Updated"
        assert response.data[0]["email"] == "alice@example.com"

        # Cleanup
        try:
            supabase_client.table("test_users_email").delete().neq("id", "").execute()
        except Exception:
            pass

    def test_two_phase_insert_with_db_generated_ids(self, supabase_client):
        """Test two-phase insert with DB-generated UUIDs.

        This test requires tables with UUID primary keys:

        CREATE TABLE test_orgs (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE test_members (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            name TEXT NOT NULL,
            org_id UUID REFERENCES test_orgs(id)
        );
        """
        # Clean up test tables (in reverse FK order)
        try:
            supabase_client.table("test_members").delete().neq("id", "").execute()
        except Exception:
            pass
        try:
            supabase_client.table("test_orgs").delete().neq("id", "").execute()
        except Exception:
            pass

        # Data with nested structure - no IDs provided
        data = {
            "orgs": [
                {
                    "name": "Acme Corp",
                    "members": [
                        {"name": "Alice"},
                        {"name": "Bob"},
                    ],
                },
                {
                    "name": "Globex Inc",
                    "members": [
                        {"name": "Charlie"},
                    ],
                },
            ],
        }

        try:
            (
                etl(data)
                .goto("orgs").each()
                .map_to(table="test_orgs", fields=[
                    Field("name", get("name")),
                    TempField("_key", get("name")),  # Business key for matching
                ])
                .goto("members").each()
                .map_to(table="test_members", fields=[
                    Field("name", get("name")),
                    TempField("_parent_key", get_from_parent("name")),
                ])
                .link_to("test_orgs", by={"_parent_key": "_key"}, fk={"org_id": "id"})
                .load(supabase_client)
                .run()
            )

            # Verify orgs were inserted with DB-generated UUIDs
            orgs_response = supabase_client.table("test_orgs").select("*").execute()
            assert len(orgs_response.data) == 2
            orgs_by_name = {o["name"]: o for o in orgs_response.data}
            assert "Acme Corp" in orgs_by_name
            assert "Globex Inc" in orgs_by_name
            # UUIDs should be proper UUID format (36 chars with hyphens)
            assert len(orgs_by_name["Acme Corp"]["id"]) == 36

            # Verify members were inserted with correct org_id FKs
            members_response = supabase_client.table("test_members").select("*").execute()
            assert len(members_response.data) == 3

            # Check FK relationships
            acme_id = orgs_by_name["Acme Corp"]["id"]
            globex_id = orgs_by_name["Globex Inc"]["id"]

            acme_members = [m for m in members_response.data if m["org_id"] == acme_id]
            assert len(acme_members) == 2
            assert {m["name"] for m in acme_members} == {"Alice", "Bob"}

            globex_members = [m for m in members_response.data if m["org_id"] == globex_id]
            assert len(globex_members) == 1
            assert globex_members[0]["name"] == "Charlie"

        finally:
            # Cleanup (in reverse FK order)
            try:
                supabase_client.table("test_members").delete().neq("id", "").execute()
            except Exception:
                pass
            try:
                supabase_client.table("test_orgs").delete().neq("id", "").execute()
            except Exception:
                pass
