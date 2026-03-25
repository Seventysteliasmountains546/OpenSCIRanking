#!/usr/bin/env python
from __future__ import annotations

import argparse

from opensci_v2.io import read_parquet, write_parquet
from opensci_v2.transform import build_journal_edges


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--works", required=True, help="Works Parquet path")
    parser.add_argument("--output", required=True, help="Journal edge Parquet path")
    parser.add_argument("--drop-self-citations", action="store_true", help="Remove self-citation edges")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    works_df = read_parquet(args.works)
    edges_df = build_journal_edges(works_df)
    if args.drop_self_citations and not edges_df.empty:
        edges_df = edges_df[edges_df["citing_source_id"] != edges_df["cited_source_id"]].copy()
    write_parquet(edges_df, args.output)
    print(f"Wrote {len(edges_df)} journal edges to {args.output}")


if __name__ == "__main__":
    main()
