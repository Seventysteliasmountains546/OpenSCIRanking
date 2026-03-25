#!/usr/bin/env python
from __future__ import annotations

import argparse
import math

import pandas as pd

from opensci_v2.ranking import compute_pagerank


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--edges", required=True, help="Edge Parquet path")
    parser.add_argument("--sources", required=True, help="Source metadata Parquet path")
    parser.add_argument("--resolved", required=True, help="Resolved source CSV path")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument("--gamma", type=float, default=2.0, help="Exponent for normalized entropy penalty")
    parser.add_argument(
        "--size-mode",
        choices=["none", "sqrt_norm"],
        default="sqrt_norm",
        help="Optional post-PageRank size normalization",
    )
    return parser.parse_args()


def normalized_entropy(group: pd.DataFrame) -> float:
    weights = group["weight"].astype(float)
    total = weights.sum()
    if total <= 0:
        return 0.0
    probabilities = weights / total
    entropy = -float((probabilities * probabilities.map(math.log)).sum())
    k = len(probabilities)
    if k <= 1:
        return 0.0
    return entropy / math.log(k)


def main() -> None:
    args = parse_args()
    edges = pd.read_parquet(args.edges)
    rank = compute_pagerank(edges).rename(columns={"pagerank": "base_pagerank", "rank": "base_rank"})
    entropy = (
        edges.groupby("cited_source_id", as_index=False)
        .apply(normalized_entropy, include_groups=False)
        .rename(columns={None: "entropy_norm"})
    )

    sources = pd.read_parquet(args.sources)
    resolved = pd.read_csv(args.resolved)

    full = rank.merge(entropy, left_on="source_id", right_on="cited_source_id", how="left")
    full = full.merge(sources[["source_id", "works_count", "cited_by_count", "display_name"]], on="source_id", how="left")
    full = full.merge(
        resolved[["openalex_source_id", "journal_name", "cas_partition"]],
        left_on="source_id",
        right_on="openalex_source_id",
        how="left",
    )
    full["entropy_norm"] = full["entropy_norm"].fillna(0.0)
    full["diversity_penalty"] = full["entropy_norm"] ** args.gamma
    full["base_score"] = full["base_pagerank"]
    if args.size_mode == "sqrt_norm":
        full["base_score"] = full["base_score"] / full["works_count"].pow(0.5)

    full["adjusted_score"] = full["base_score"] * full["diversity_penalty"]
    full = full.sort_values(
        ["adjusted_score", "base_pagerank", "cited_by_count", "journal_name"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    full["adjusted_rank"] = full.index + 1
    full.to_csv(args.output, index=False)
    print(f"Wrote {len(full)} rows to {args.output}")


if __name__ == "__main__":
    main()
