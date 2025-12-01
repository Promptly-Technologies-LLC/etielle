from etielle.executor import run_mapping
from etielle.core import Field, MappingSpec, TableEmit, TraversalSpec
from etielle.transforms import get, get_from_parent, key


def test_list_traversal_inner_path_and_auto_id():
    data = {
        "users": [
            {"id": "u1", "name": "Ada", "posts": [{"id": "p1"}, {"id": "p2"}]},
            {"id": "u2", "name": "Linus", "posts": []},
        ]
    }

    users_trav = TraversalSpec(
        path=["users"],
        mode="auto",
        emits=[
            TableEmit(
                table="users",
                join_keys=[get("id")],
                fields=[Field("name", get("name"))],
            )
        ],
    )

    posts_trav = TraversalSpec(
        path=["users"],
        mode="auto",
        inner_path=["posts"],
        inner_mode="auto",
        emits=[
            TableEmit(
                table="posts",
                join_keys=[get("id")],
                fields=[
                    Field("user_id", get_from_parent("id")),
                ],
            )
        ],
    )

    spec = MappingSpec(traversals=[users_trav, posts_trav])
    result = run_mapping(data, spec)

    # users should auto-populate 'id' from the single join key
    assert sorted(result["users"].instances.values(), key=lambda r: r["id"]) == [
        {"id": "u1", "name": "Ada"},
        {"id": "u2", "name": "Linus"},
    ]

    assert sorted(result["posts"].instances.values(), key=lambda r: r["id"]) == [
        {"id": "p1", "user_id": "u1"},
        {"id": "p2", "user_id": "u1"},
    ]


def test_dict_item_iteration_and_parent_key():
    # Iterate a dict's items, using the current key as part of the data
    data = {"metrics": {"m1": {"value": 10}, "m2": {"value": 20}}}

    spec = MappingSpec(
        traversals=[
            TraversalSpec(
                path=["metrics"],
                mode="items",  # iterate dict items: key/value
                emits=[
                    TableEmit(
                        table="metrics",
                        join_keys=[key()],
                        fields=[
                            Field("id", key()),
                            Field("value", get("value")),
                        ],
                    )
                ],
            )
        ]
    )

    result = run_mapping(data, spec)
    assert sorted(result["metrics"].instances.values(), key=lambda r: r["id"]) == [
        {"id": "m1", "value": 10},
        {"id": "m2", "value": 20},
    ]


def test_composite_join_keys_merging_from_multiple_traversals():
    # Two traversals contribute to the same logical table rows via composite keys
    data = {
        "people": [
            {"id": "p1", "year": 2024, "score": 7},
            {"id": "p1", "year": 2025, "score": 9},
        ],
        "bonuses": [
            {"person_id": "p1", "year": 2024, "bonus": 100},
            {"person_id": "p1", "year": 2025, "bonus": 200},
        ],
    }

    people_trav = TraversalSpec(
        path=["people"],
        mode="auto",
        emits=[
            TableEmit(
                table="scores",
                join_keys=[get("id"), get("year")],
                fields=[
                    Field("person_id", get("id")),
                    Field("year", get("year")),
                    Field("score", get("score")),
                ],
            )
        ],
    )

    bonus_trav = TraversalSpec(
        path=["bonuses"],
        mode="auto",
        emits=[
            TableEmit(
                table="scores",
                join_keys=[get("person_id"), get("year")],
                fields=[
                    Field("bonus", get("bonus")),
                ],
            )
        ],
    )

    spec = MappingSpec(traversals=[people_trav, bonus_trav])
    result = run_mapping(data, spec)

    assert sorted(
        result["scores"].instances.values(), key=lambda r: (r["person_id"], r["year"])
    ) == [
        {"person_id": "p1", "year": 2024, "score": 7, "bonus": 100},
        {"person_id": "p1", "year": 2025, "score": 9, "bonus": 200},
    ]


def test_mapping_result_supports_secondary_indices():
    """MappingResult should support secondary indices for relationship linking."""
    from etielle.core import MappingResult

    # Create instances
    class FakeUser:
        def __init__(self, id, name):
            self.id = id
            self.name = name

    user1 = FakeUser(1, "Alice")
    user2 = FakeUser(2, "Bob")

    # MappingResult with instances dict and secondary index
    result = MappingResult(
        instances={("k1",): user1, ("k2",): user2},
        update_errors={},
        finalize_errors={},
        stats={},
        indices={"name": {"Alice": user1, "Bob": user2}}
    )

    assert result.indices["name"]["Alice"] is user1
    assert result.indices["name"]["Bob"] is user2
