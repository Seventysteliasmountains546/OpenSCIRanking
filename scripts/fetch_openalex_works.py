#!/usr/bin/env python
from __future__ import annotations

import argparse

import pandas as pd
from tqdm import tqdm

from opensci_v2.config import ensure_data_dirs
from opensci_v2.io import read_parquet, write_parquet
from opensci_v2.openalex import OpenAlexClient
from opensci_v2.transform import normalize_works


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", required=True, help="Parquet file with source_id column")
    parser.add_argument("--output", required=True, help="Output Parquet path")
    parser.add_argument("--start-year", type=int, required=True, help="Start publication year")
    parser.add_argument("--end-year", type=int, default=None, help="Optional end publication year")
    parser.add_argument("--mailto", default=None, help="Contact email for polite OpenAlex requests")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_data_dirs()
    sources_df = read_parquet(args.sources)
    client = OpenAlexClient(mailto=args.mailto)

    all_records: list[dict] = []
    for source_id in tqdm(sources_df["source_id"].dropna().astype(str).unique(), desc="Fetching works"):
        all_records.extend(client.get_works_for_source(source_id, args.start_year, args.end_year))

    works_df = normalize_works(all_records)
    write_parquet(works_df, args.output)
    print(f"Wrote {len(works_df)} works to {args.output}")


if __name__ == "__main__":
    main()
