#!/usr/bin/env python
from __future__ import annotations

import argparse
import math

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rankings", required=True, help="Base PageRank Parquet path")
    parser.add_argument("--sources", required=True, help="Source metadata Parquet path with works_count")
    parser.add_argument("--resolved", default=None, help="Optional resolved CSV with journal_name and cas_partition")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["raw", "per_work", "sqrt_norm"],
        help="Scoring mode",
    )
    parser.add_argument("--output", required=True, help="Output CSV path")
    return parser.parse_args()


def compute_score(row: pd.Series, mode: str) -> float | None:
    pagerank = row["pagerank"]
    works_count = row["works_count"]
    if pd.isna(pagerank):
        return None
    if mode == "raw":
        return float(pagerank)
    if not works_count:
        return None
    if mode == "per_work":
        return float(pagerank) / float(works_count)
    if mode == "sqrt_norm":
        return float(pagerank) / math.sqrt(float(works_count))
    raise ValueError(f"Unsupported mode: {mode}")


def main() -> None:
    args = parse_args()
    rankings = pd.read_parquet(args.rankings)
    sources = pd.read_parquet(args.sources)

    full = rankings.merge(
        sources[["source_id", "display_name", "issn", "works_count", "cited_by_count"]],
        on="source_id",
        how="left",
    )

    if args.resolved:
        resolved = pd.read_csv(args.resolved)
        full = full.merge(
            resolved[["openalex_source_id", "journal_name", "cas_partition"]],
            left_on="source_id",
            right_on="openalex_source_id",
            how="left",
        )

    full["adjusted_score"] = full.apply(compute_score, axis=1, mode=args.mode)
    full = full.sort_values("adjusted_score", ascending=False, na_position="last").reset_index(drop=True)
    full["adjusted_rank"] = full.index + 1
    full.to_csv(args.output, index=False)
    print(f"Wrote {len(full)} rows to {args.output}")


if __name__ == "__main__":
    main()
