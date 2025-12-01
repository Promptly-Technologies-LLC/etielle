"""Utility functions for etielle."""

from typing import Dict, Set, List


def topological_sort(
    graph: Dict[str, Set[str]],
    nodes: Set[str]
) -> List[str]:
    """Topological sort of nodes based on dependency graph.

    Args:
        graph: Dict mapping node -> set of nodes it depends on (parents).
        nodes: All nodes to include in the sort.

    Returns:
        List of nodes in dependency order (parents before children).

    Raises:
        ValueError: If the graph contains a cycle.
    """
    # Build in-degree count and adjacency list (parent -> children)
    in_degree: Dict[str, int] = {node: 0 for node in nodes}
    children: Dict[str, List[str]] = {node: [] for node in nodes}

    for child, parents in graph.items():
        if child not in nodes:
            continue
        for parent in parents:
            if parent in nodes:
                in_degree[child] += 1
                children[parent].append(child)

    # Kahn's algorithm
    queue = [node for node in nodes if in_degree[node] == 0]
    result: List[str] = []

    while queue:
        # Sort for deterministic ordering
        queue.sort()
        node = queue.pop(0)
        result.append(node)

        for child in children[node]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(result) != len(nodes):
        # Find nodes in cycle for error message
        remaining = [n for n in nodes if n not in result]
        raise ValueError(f"Circular dependency detected involving: {remaining}")

    return result
