"""Utility functions for etielle."""

from typing import Dict, List, Set


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


def weakly_connected_components(
    graph: Dict[str, Set[str]],
    nodes: Set[str],
) -> List[Set[str]]:
    """Partition nodes into weakly connected components.

    Args:
        graph: Dict mapping child -> set of parent nodes (directed edges).
        nodes: All nodes to include.

    Returns:
        List of component sets in deterministic order (sorted by first member).
    """
    adj: Dict[str, Set[str]] = {node: set() for node in nodes}

    for child, parents in graph.items():
        if child not in nodes:
            continue
        for parent in parents:
            if parent in nodes:
                adj[child].add(parent)
                adj[parent].add(child)

    visited: Set[str] = set()
    components: List[Set[str]] = []

    for start in sorted(nodes):
        if start in visited:
            continue
        component: Set[str] = set()
        stack = [start]
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            component.add(node)
            for neighbor in sorted(adj.get(node, set())):
                if neighbor not in visited:
                    stack.append(neighbor)
        components.append(component)

    return components


def partition_components(
    graph: Dict[str, Set[str]],
    all_tables: Set[str],
    eager_tables: Set[str],
) -> List[Set[str]]:
    """Partition non-eager tables into weakly connected components.

    Edges involving eager tables are excluded from partitioning so shared
    dimension tables do not collapse unrelated subgraphs into one component.
    """
    partition_nodes = all_tables - eager_tables
    if not partition_nodes:
        return []

    partition_graph: Dict[str, Set[str]] = {}
    related_tables: Set[str] = set()
    for child, parents in graph.items():
        if child not in partition_nodes:
            continue
        filtered = {
            parent
            for parent in parents
            if parent in partition_nodes and parent not in eager_tables
        }
        if filtered:
            partition_graph[child] = filtered
            related_tables.add(child)
            related_tables.update(filtered)

    orphan_tables = partition_nodes - related_tables
    components: List[Set[str]] = []

    if related_tables:
        components.extend(
            weakly_connected_components(partition_graph, related_tables)
        )
    if orphan_tables:
        components.append(orphan_tables)

    return components

