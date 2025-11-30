"""Tests for utility functions."""

import pytest
from etielle.utils import topological_sort


class TestTopologicalSort:
    """Tests for topological_sort function."""

    def test_empty_graph(self):
        """Empty graph returns empty list."""
        result = topological_sort({}, set())
        assert result == []

    def test_single_node_no_edges(self):
        """Single node with no edges."""
        result = topological_sort({}, {"a"})
        assert result == ["a"]

    def test_linear_chain(self):
        """Linear dependency chain."""
        # b depends on a, c depends on b
        graph = {"b": {"a"}, "c": {"b"}}
        result = topological_sort(graph, {"a", "b", "c"})
        # a must come before b, b must come before c
        assert result.index("a") < result.index("b")
        assert result.index("b") < result.index("c")

    def test_diamond_dependency(self):
        """Diamond-shaped dependency."""
        # b and c depend on a, d depends on b and c
        graph = {"b": {"a"}, "c": {"a"}, "d": {"b", "c"}}
        result = topological_sort(graph, {"a", "b", "c", "d"})
        assert result.index("a") < result.index("b")
        assert result.index("a") < result.index("c")
        assert result.index("b") < result.index("d")
        assert result.index("c") < result.index("d")

    def test_multiple_roots(self):
        """Multiple root nodes (no parents)."""
        graph = {"c": {"a"}, "d": {"b"}}
        result = topological_sort(graph, {"a", "b", "c", "d"})
        # a before c, b before d
        assert result.index("a") < result.index("c")
        assert result.index("b") < result.index("d")

    def test_cycle_raises_error(self):
        """Cycle in graph raises ValueError."""
        graph = {"a": {"b"}, "b": {"a"}}
        with pytest.raises(ValueError, match="Circular dependency"):
            topological_sort(graph, {"a", "b"})

    def test_nodes_not_in_graph_treated_as_roots(self):
        """Nodes without edges in graph are roots."""
        graph = {"b": {"a"}}
        result = topological_sort(graph, {"a", "b", "c"})
        # a before b, c can be anywhere (it's independent)
        assert result.index("a") < result.index("b")
        assert "c" in result
