from __future__ import annotations

import pandas as pd


def _compute_with_igraph(edges_df: pd.DataFrame) -> pd.DataFrame:
    from igraph import Graph

    vertices = sorted(set(edges_df["citing_source_id"]).union(edges_df["cited_source_id"]))
    graph = Graph(directed=True)
    graph.add_vertices(vertices)
    graph.add_edges(list(zip(edges_df["citing_source_id"], edges_df["cited_source_id"], strict=False)))
    graph.es["weight"] = edges_df["weight"].tolist()

    scores = graph.pagerank(weights="weight", directed=True)
    result = pd.DataFrame({"source_id": vertices, "pagerank": scores})
    return result


def _compute_with_networkx(edges_df: pd.DataFrame) -> pd.DataFrame:
    import networkx as nx

    graph = nx.DiGraph()
    for row in edges_df.itertuples(index=False):
        graph.add_edge(row.citing_source_id, row.cited_source_id, weight=row.weight)

    scores = nx.pagerank(graph, weight="weight")
    return pd.DataFrame(
        {"source_id": list(scores.keys()), "pagerank": list(scores.values())}
    )


def compute_pagerank(edges_df: pd.DataFrame) -> pd.DataFrame:
    if edges_df.empty:
        return pd.DataFrame(columns=["source_id", "pagerank", "rank"])

    try:
        result = _compute_with_igraph(edges_df)
    except ModuleNotFoundError:
        result = _compute_with_networkx(edges_df)

    result = result.sort_values("pagerank", ascending=False).reset_index(drop=True)
    result["rank"] = result.index + 1
    return result
