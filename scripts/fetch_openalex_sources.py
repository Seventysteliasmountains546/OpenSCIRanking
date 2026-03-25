#!/usr/bin/env python
from __future__ import annotations

import argparse

import pandas as pd

from opensci_v2.config import ensure_data_dirs
from opensci_v2.io import write_parquet
from opensci_v2.openalex import OpenAlexClient
from opensci_v2.transform import normalize_sources


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="CSV with openalex_source_id column")
    parser.add_argument("--output", required=True, help="Output Parquet path")
    parser.add_argument("--mailto", default=None, help="Contact email for polite OpenAlex requests")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_data_dirs()
    seeds = pd.read_csv(args.input)
    if "openalex_source_id" not in seeds.columns:
        raise ValueError("Input CSV must contain an 'openalex_source_id' column.")

    client = OpenAlexClient(mailto=args.mailto)
    records = client.get_sources_by_ids(seeds["openalex_source_id"].dropna().astype(str).tolist())
    sources_df = normalize_sources(records)
    write_parquet(sources_df, args.output)
    print(f"Wrote {len(sources_df)} sources to {args.output}")


if __name__ == "__main__":
    main()
