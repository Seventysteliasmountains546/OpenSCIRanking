#!/usr/bin/env python
from __future__ import annotations

import argparse

import pandas as pd

from opensci_v2.io import write_parquet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Resolved source CSV from resolve_openalex_sources.py")
    parser.add_argument("--output", required=True, help="Output Parquet path for downstream fetch scripts")
    parser.add_argument(
        "--include-unresolved",
        action="store_true",
        help="Keep unresolved rows with empty source_id values",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    resolved = pd.read_csv(args.input)

    required_columns = {
        "journal_name",
        "issn",
        "cas_partition",
        "source_sheet",
        "resolve_status",
        "resolve_method",
        "resolve_error",
        "openalex_source_id",
        "resolved_display_name",
        "resolved_issn_l",
        "resolved_issn",
        "resolved_works_count",
        "resolved_cited_by_count",
        "resolved_is_oa",
        "resolved_country_code",
    }
    missing = sorted(required_columns - set(resolved.columns))
    if missing:
        raise ValueError(f"Missing required columns in resolved CSV: {missing}")

    if args.include_unresolved:
        filtered = resolved.copy()
    else:
        filtered = resolved[resolved["resolve_status"].eq("resolved")].copy()

    filtered["source_id"] = filtered["openalex_source_id"].fillna("").astype(str)
    filtered = filtered[filtered["source_id"].ne("")].copy()

    sources = filtered[
        [
            "source_id",
            "resolved_display_name",
            "resolved_issn_l",
            "resolved_issn",
            "resolved_works_count",
            "resolved_cited_by_count",
            "resolved_is_oa",
            "resolved_country_code",
            "journal_name",
            "issn",
            "cas_partition",
            "source_sheet",
            "resolve_status",
            "resolve_method",
            "resolve_error",
        ]
    ].rename(
        columns={
            "resolved_display_name": "display_name",
            "resolved_issn_l": "issn_l",
            "resolved_issn": "source_issn",
            "resolved_works_count": "works_count",
            "resolved_cited_by_count": "cited_by_count",
            "resolved_is_oa": "is_oa",
            "resolved_country_code": "country_code",
        }
    )

    sources = (
        sources.sort_values(["source_id", "journal_name"], ascending=[True, True])
        .drop_duplicates(subset=["source_id"], keep="first")
        .reset_index(drop=True)
    )

    write_parquet(sources, args.output)
    print(f"Wrote {len(sources)} rows to {args.output}")


if __name__ == "__main__":
    main()
