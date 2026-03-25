#!/usr/bin/env python
from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rankings", required=True, help="Base PageRank Parquet path")
    parser.add_argument("--resolved", required=True, help="Resolved source CSV path")
    parser.add_argument("--works", required=True, help="Works Parquet path for review/reference statistics")
    parser.add_argument("--output-prefix", required=True, help="Output prefix without extension")
    parser.add_argument(
        "--size-mode",
        choices=["raw", "sqrt_norm"],
        default="sqrt_norm",
        help="Size normalization applied to base PageRank",
    )
    parser.add_argument("--mega-journal-cap", type=float, default=0.0, help="If > 0, apply mega-journal cap penalty")
    parser.add_argument("--review-penalty", action="store_true", help="Apply a mild review-heavy penalty")
    parser.add_argument(
        "--review-threshold-quantile",
        type=float,
        default=0.75,
        help="Quantile used as the threshold for review-heavy penalty",
    )
    parser.add_argument(
        "--review-penalty-exp",
        type=float,
        default=0.15,
        help="Exponent used in the review-heavy penalty",
    )
    return parser.parse_args()


def compute_base_score(pagerank: float, works_count: float, mode: str) -> float | None:
    if pd.isna(pagerank):
        return None
    if mode == "raw":
        return float(pagerank)
    if works_count <= 0:
        return None
    return float(pagerank) / math.sqrt(float(works_count))


def main() -> None:
    args = parse_args()
    rankings = pd.read_parquet(args.rankings).rename(columns={"pagerank": "base_pagerank", "rank": "base_pagerank_rank"})
    resolved = pd.read_csv(args.resolved)
    works = pd.read_parquet(args.works)
    works["ref_count"] = works["referenced_works"].map(len)
    ref_stats = works.groupby("source_id", as_index=False).agg(avg_ref_count=("ref_count", "mean"))
    review_threshold = float(ref_stats["avg_ref_count"].quantile(args.review_threshold_quantile))

    base = resolved.copy()
    base = base.merge(rankings, left_on="openalex_source_id", right_on="source_id", how="left")
    base = base.merge(ref_stats, left_on="openalex_source_id", right_on="source_id", how="left", suffixes=("", "_refs"))
    base["cited_by_count"] = base["resolved_cited_by_count"].fillna(0)
    base["works_count"] = base["resolved_works_count"].fillna(0)
    base["avg_ref_count"] = base["avg_ref_count"].fillna(0)
    base["base_score"] = base.apply(
        lambda row: compute_base_score(row["base_pagerank"], row["works_count"], args.size_mode),
        axis=1,
    )

    if args.review_penalty:
        def review_penalty(avg_ref: float) -> float:
            if avg_ref <= review_threshold or avg_ref <= 0:
                return 1.0
            return (review_threshold / avg_ref) ** args.review_penalty_exp
        base["review_penalty_factor"] = base["avg_ref_count"].apply(review_penalty)
    else:
        base["review_penalty_factor"] = 1.0

    if args.mega_journal_cap > 0:
        base["mega_journal_factor"] = base["works_count"].apply(
            lambda x: min(1.0, math.sqrt(args.mega_journal_cap / float(x))) if float(x) > 0 else 0.0
        )
    else:
        base["mega_journal_factor"] = 1.0

    base["final_score"] = base.apply(
        lambda row: float(row["base_score"]) * float(row["review_penalty_factor"]) * float(row["mega_journal_factor"])
        if pd.notna(row["base_score"]) else pd.NA,
        axis=1,
    )

    base["ranking_status"] = "unresolved"
    base.loc[base["resolve_status"].eq("resolved") & base["final_score"].isna(), "ranking_status"] = "fallback_source_impact"
    base.loc[base["final_score"].notna(), "ranking_status"] = "final_ranked"

    ranked = base[base["ranking_status"] == "final_ranked"].sort_values(
        ["final_score", "base_pagerank", "cited_by_count", "journal_name"],
        ascending=[False, False, False, True],
    ).copy()
    ranked["final_rank"] = range(1, len(ranked) + 1)

    fallback = base[base["ranking_status"] == "fallback_source_impact"].sort_values(
        ["cited_by_count", "works_count", "journal_name"],
        ascending=[False, False, True],
    ).copy()
    fallback["final_rank"] = range(len(ranked) + 1, len(ranked) + len(fallback) + 1)

    unresolved = base[base["ranking_status"] == "unresolved"].sort_values(["journal_name"]).copy()
    unresolved["final_rank"] = range(len(ranked) + len(fallback) + 1, len(ranked) + len(fallback) + len(unresolved) + 1)

    final = pd.concat([ranked, fallback, unresolved], ignore_index=True)
    final = final[
        [
            "final_rank",
            "ranking_status",
            "journal_name",
            "issn",
            "cas_partition",
            "openalex_source_id",
            "resolved_display_name",
            "base_pagerank",
            "base_pagerank_rank",
            "base_score",
            "avg_ref_count",
            "review_penalty_factor",
            "mega_journal_factor",
            "final_score",
            "cited_by_count",
            "works_count",
            "resolve_status",
            "resolve_method",
            "resolve_error",
        ]
    ].rename(columns={"openalex_source_id": "source_id", "resolved_display_name": "display_name"})

    prefix = Path(args.output_prefix)
    final.to_csv(prefix.with_suffix(".csv"), index=False)
    final.to_parquet(prefix.with_suffix(".parquet"), index=False)
    final.head(100).to_csv(prefix.parent / f"{prefix.name}_top100.csv", index=False)
    with pd.ExcelWriter(prefix.with_suffix(".xlsx"), engine="openpyxl") as writer:
        final.to_excel(writer, index=False, sheet_name="ranking")
        final.head(100).to_excel(writer, index=False, sheet_name="top100")

    print(f"Wrote {len(final)} rows to {prefix.with_suffix('.csv')}")
    print(
        f"Settings: size_mode={args.size_mode} "
        f"mega_journal_cap={args.mega_journal_cap} "
        f"review_penalty={args.review_penalty} "
        f"review_threshold={review_threshold:.6f}"
    )


if __name__ == "__main__":
    main()
