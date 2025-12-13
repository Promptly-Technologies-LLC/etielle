"""Tests for .each().each() nested iteration on dict-of-lists and list-of-lists.

These tests verify that consecutive .each() calls without .goto() work correctly
for dict-of-lists and list-of-lists data structures.

Related issue: https://github.com/Promptly-Technologies-LLC/etielle/issues/66
"""

from dataclasses import dataclass

import pytest

from etielle.fluent import etl, Field, TempField, node, parent_index
from etielle.transforms import get, key, get_from_parent, index, parent_key


# =============================================================================
# Models
# =============================================================================


@dataclass
class QuestionChoice:
    """Junction table for Question-Choice M2M relationship."""

    __tablename__ = "question_choices"
    question_id: str
    choice_id: str


@dataclass
class Cell:
    """A cell in a 2D grid with row and column indices."""

    __tablename__ = "cells"
    row: int
    col: int
    value: int


# =============================================================================
# Test Dict-of-Lists Pattern
# =============================================================================


class TestDictOfListsIteration:
    """Test .each().each() on dict-of-lists structures.

    Data pattern:
    {
        "question_choices": {
            "Q1": ["c1", "c2"],
            "Q2": ["c2", "c3"]
        }
    }
    """

    def test_basic_dict_of_lists_iteration(self):
        """Basic dict-of-lists with .each().each() works."""
        data = {
            "question_choices": {
                "Q1": ["c1", "c2"],
                "Q2": ["c2", "c3"],
            }
        }

        result = (
            etl(data)
            .goto("question_choices")
            .each()  # Iterate dict keys: "Q1", "Q2"
            .each()  # Iterate list values: "c1", "c2", "c2", "c3"
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", parent_key()),  # Get dict key from parent
                    Field("choice_id", node()),  # Get current list item
                ],
            )
            .run()
        )

        junctions = result.tables[QuestionChoice]
        assert len(junctions) == 4

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions.values()}
        expected_pairs = {("Q1", "c1"), ("Q1", "c2"), ("Q2", "c2"), ("Q2", "c3")}
        assert junction_pairs == expected_pairs

    def test_dict_of_lists_with_key_transform(self):
        """key() returns current dict key, parent_key() returns parent."""
        data = {
            "items": {
                "A": ["x", "y"],
            }
        }

        # Using a plain dict table to capture the transforms
        result = (
            etl(data)
            .goto("items")
            .each()
            .each()
            .map_to(
                table="results",
                fields=[
                    Field("parent_key", parent_key()),  # Parent dict key: "A"
                    Field("value", node()),  # Current list item
                    Field("list_index", index()),  # Index within the list
                ],
            )
            .run()
        )

        rows = list(result.tables["results"].values())
        assert len(rows) == 2

        # Check that we captured the correct values
        row_data = {(r["parent_key"], r["value"], r["list_index"]) for r in rows}
        expected = {("A", "x", 0), ("A", "y", 1)}
        assert row_data == expected

    def test_dict_of_lists_with_empty_list(self):
        """Empty lists in dict-of-lists are handled gracefully."""
        data = {
            "question_choices": {
                "Q1": ["c1"],
                "Q2": [],  # Empty list
                "Q3": ["c2"],
            }
        }

        result = (
            etl(data)
            .goto("question_choices")
            .each()
            .each()
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", parent_key()),
                    Field("choice_id", node()),
                ],
            )
            .run()
        )

        junctions = result.tables[QuestionChoice]
        assert len(junctions) == 2  # Only Q1 and Q3 have choices

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions.values()}
        expected_pairs = {("Q1", "c1"), ("Q3", "c2")}
        assert junction_pairs == expected_pairs

    def test_dict_of_lists_with_single_item_lists(self):
        """Single-item lists work correctly."""
        data = {
            "mapping": {
                "A": ["1"],
                "B": ["2"],
            }
        }

        result = (
            etl(data)
            .goto("mapping")
            .each()
            .each()
            .map_to(
                table="pairs",
                fields=[
                    Field("key", parent_key()),
                    Field("value", node()),
                ],
            )
            .run()
        )

        rows = list(result.tables["pairs"].values())
        assert len(rows) == 2

        row_data = {(r["key"], r["value"]) for r in rows}
        expected = {("A", "1"), ("B", "2")}
        assert row_data == expected


# =============================================================================
# Test List-of-Lists Pattern
# =============================================================================


class TestListOfListsIteration:
    """Test .each().each() on list-of-lists (2D array) structures.

    Data pattern:
    {
        "rows": [
            [1, 2, 3],
            [4, 5, 6]
        ]
    }
    """

    def test_basic_list_of_lists_iteration(self):
        """Basic list-of-lists (2D grid) with .each().each() works."""
        data = {
            "rows": [
                [1, 2],
                [3, 4],
            ]
        }

        result = (
            etl(data)
            .goto("rows")
            .each()  # Iterate outer list (rows)
            .each()  # Iterate inner list (columns)
            .map_to(
                table=Cell,
                fields=[
                    Field("row", parent_index()),  # Outer list index
                    Field("col", index()),  # Inner list index
                    Field("value", node()),  # Current value
                ],
            )
            .run()
        )

        cells = result.tables[Cell]
        assert len(cells) == 4

        cell_data = {(c.row, c.col, c.value) for c in cells.values()}
        expected = {(0, 0, 1), (0, 1, 2), (1, 0, 3), (1, 1, 4)}
        assert cell_data == expected

    def test_list_of_lists_with_empty_inner_list(self):
        """Empty inner lists are handled gracefully."""
        data = {
            "rows": [
                [1, 2],
                [],  # Empty inner list
                [3],
            ]
        }

        result = (
            etl(data)
            .goto("rows")
            .each()
            .each()
            .map_to(
                table=Cell,
                fields=[
                    Field("row", parent_index()),
                    Field("col", index()),
                    Field("value", node()),
                ],
            )
            .run()
        )

        cells = result.tables[Cell]
        assert len(cells) == 3  # 2 from first row, 0 from empty, 1 from third

        cell_data = {(c.row, c.col, c.value) for c in cells.values()}
        expected = {(0, 0, 1), (0, 1, 2), (2, 0, 3)}
        assert cell_data == expected

    def test_list_of_lists_varying_lengths(self):
        """Lists with varying lengths (jagged array) work correctly."""
        data = {
            "rows": [
                [1],
                [2, 3, 4],
                [5, 6],
            ]
        }

        result = (
            etl(data)
            .goto("rows")
            .each()
            .each()
            .map_to(
                table="cells",
                fields=[
                    Field("row", parent_index()),
                    Field("col", index()),
                    Field("value", node()),
                ],
            )
            .run()
        )

        rows = list(result.tables["cells"].values())
        assert len(rows) == 6

        cell_data = {(r["row"], r["col"], r["value"]) for r in rows}
        expected = {
            (0, 0, 1),
            (1, 0, 2), (1, 1, 3), (1, 2, 4),
            (2, 0, 5), (2, 1, 6),
        }
        assert cell_data == expected

    def test_list_of_strings_treated_as_scalars(self):
        """Strings are treated as scalar values, not character sequences.

        This is intentional behavior - strings are not iterated character-by-character
        in nested iteration. Use explicit list of chars if needed.
        """
        data = {
            "words": ["ab", "cd"],
        }

        result = (
            etl(data)
            .goto("words")
            .each()  # Iterate words
            .map_to(
                table="words",
                fields=[
                    Field("word_index", index()),
                    Field("word", node()),
                ],
            )
            .run()
        )

        rows = list(result.tables["words"].values())
        assert len(rows) == 2

        word_data = {(r["word_index"], r["word"]) for r in rows}
        expected = {(0, "ab"), (1, "cd")}
        assert word_data == expected


# =============================================================================
# Test Context and Parent Access
# =============================================================================


class TestNestedIterationContext:
    """Test that context is properly maintained in nested iteration."""

    def test_get_from_parent_works_in_nested_dict_iteration(self):
        """get_from_parent() accesses the parent dict entry."""
        data = {
            "categories": {
                "fruits": ["apple", "banana"],
                "vegetables": ["carrot"],
            }
        }

        # Note: In dict-of-lists, the parent node is the list (e.g., ["apple", "banana"])
        # So get_from_parent("0") would get the first element of the parent list
        result = (
            etl(data)
            .goto("categories")
            .each()
            .each()
            .map_to(
                table="items",
                fields=[
                    Field("category", parent_key()),  # Dict key
                    Field("item", node()),  # Current item
                ],
            )
            .run()
        )

        rows = list(result.tables["items"].values())
        assert len(rows) == 3

        item_data = {(r["category"], r["item"]) for r in rows}
        expected = {
            ("fruits", "apple"),
            ("fruits", "banana"),
            ("vegetables", "carrot"),
        }
        assert item_data == expected

    def test_parent_key_with_goto_between_each(self):
        """parent_key() works when using .goto() between .each() calls.

        This tests the traditional .each().goto().each() pattern where we
        navigate to a nested field between iterations.
        """
        data = {
            "categories": [
                {"name": "fruits", "items": ["apple", "banana"]},
                {"name": "vegetables", "items": ["carrot"]},
            ]
        }

        result = (
            etl(data)
            .goto("categories")
            .each()  # Iterate category objects
            .goto("items")
            .each()  # Iterate items in each category
            .map_to(
                table="items",
                fields=[
                    Field("category", get_from_parent("name")),
                    Field("item", node()),
                    Field("item_index", index()),
                ],
            )
            .run()
        )

        rows = list(result.tables["items"].values())
        assert len(rows) == 3

        data_set = {(r["category"], r["item"], r["item_index"]) for r in rows}
        expected = {
            ("fruits", "apple", 0),
            ("fruits", "banana", 1),
            ("vegetables", "carrot", 0),
        }
        assert data_set == expected

    def test_three_level_nested_iteration(self):
        """Test .each().each().each() for 3-level nested structures.

        This tests the N-level iteration support with dict-of-dict-of-lists.
        """
        data = {
            "levels": {
                "L1": {
                    "A": [1, 2],
                    "B": [3],
                },
                "L2": {
                    "C": [4, 5, 6],
                },
            }
        }

        result = (
            etl(data)
            .goto("levels")
            .each()  # Iterate "L1", "L2"
            .each()  # Iterate "A", "B", "C"
            .each()  # Iterate list values
            .map_to(
                table="values",
                fields=[
                    Field("level", parent_key(depth=2)),  # Grandparent key: "L1" or "L2"
                    Field("group", parent_key(depth=1)),  # Parent key: "A", "B", or "C"
                    Field("value", node()),
                ],
            )
            .run()
        )

        rows = list(result.tables["values"].values())
        assert len(rows) == 6

        data_set = {(r["level"], r["group"], r["value"]) for r in rows}
        expected = {
            ("L1", "A", 1),
            ("L1", "A", 2),
            ("L1", "B", 3),
            ("L2", "C", 4),
            ("L2", "C", 5),
            ("L2", "C", 6),
        }
        assert data_set == expected


# =============================================================================
# Integration Tests
# =============================================================================


class TestNestedIterationIntegration:
    """Integration tests combining nested iteration with other features."""

    def test_dict_of_lists_with_lookup(self):
        """Dict-of-lists combined with lookup() for ID resolution."""
        from etielle.transforms import lookup

        data = {
            "question_choices": {
                "q1": ["c1", "c2"],
                "q2": ["c2"],
            }
        }

        # Pre-built indices for slug -> DB ID mapping
        indices = {
            "question_ids": {"q1": 101, "q2": 102},
            "choice_ids": {"c1": 201, "c2": 202},
        }

        result = (
            etl(data, indices=indices)
            .goto("question_choices")
            .each()
            .each()
            .map_to(
                table="junction",
                fields=[
                    Field("question_id", lookup("question_ids", parent_key())),
                    Field("choice_id", lookup("choice_ids", node())),
                ],
            )
            .run()
        )

        rows = list(result.tables["junction"].values())
        assert len(rows) == 3

        pairs = {(r["question_id"], r["choice_id"]) for r in rows}
        expected = {(101, 201), (101, 202), (102, 202)}
        assert pairs == expected

    def test_mixed_goto_and_direct_nested_iteration(self):
        """Combine .goto().each() with .each().each() in same pipeline."""
        data = {
            "questions": [
                {"id": "Q1", "text": "Question 1"},
            ],
            "mappings": {
                "Q1": ["c1", "c2"],
            },
        }

        @dataclass
        class Question:
            __tablename__ = "questions"
            id: str
            text: str

        result = (
            etl(data)
            # Traditional .goto().each() for questions
            .goto("questions")
            .each()
            .map_to(
                table=Question,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # .each().each() for mappings dict-of-lists
            .goto_root()
            .goto("mappings")
            .each()
            .each()
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", parent_key()),
                    Field("choice_id", node()),
                ],
            )
            .run()
        )

        questions = result.tables[Question]
        assert len(questions) == 1
        assert list(questions.values())[0].id == "Q1"

        junctions = result.tables[QuestionChoice]
        assert len(junctions) == 2

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions.values()}
        expected_pairs = {("Q1", "c1"), ("Q1", "c2")}
        assert junction_pairs == expected_pairs

    def test_trailing_goto_after_each_navigates_without_iterating(self):
        """`.each().goto(...).map_to(...)` should map against the navigated node.

        This is navigation without iteration: for each element yielded by `.each()`,
        `.goto("child")` should select the child node as the mapping context.
        """
        data = {
            "items": [
                {"id": 1, "child": {"x": 10}},
                {"id": 2, "child": {"x": 20}},
            ]
        }

        result = (
            etl(data)
            .goto("items")
            .each()
            .goto("child")
            .map_to(
                table="children",
                fields=[
                    Field("id", get_from_parent("id")),
                    Field("x", get("x")),
                ],
            )
            .run()
        )

        rows = list(result.tables["children"].values())
        assert len(rows) == 2
        assert {(r["id"], r["x"]) for r in rows} == {(1, 10), (2, 20)}
