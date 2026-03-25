#!/usr/bin/env python
from __future__ import annotations

import argparse

from opensci_v2.io import read_parquet, write_parquet
from opensci_v2.ranking import compute_pagerank


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--edges", required=True, help="Journal edge Parquet path")
    parser.add_argument("--output", required=True, help="Ranking output Parquet path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    edges_df = read_parquet(args.edges)
    rankings_df = compute_pagerank(edges_df)
    write_parquet(rankings_df, args.output)
    print(f"Wrote {len(rankings_df)} ranked journals to {args.output}")


if __name__ == "__main__":
    main()
