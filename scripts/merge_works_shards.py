#!/usr/bin/env python
from __future__ import annotations

import argparse

from opensci_v2.batch import merge_shards


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True, help="Directory with per-source Parquet shards")
    parser.add_argument("--output", required=True, help="Merged Parquet output path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    count = merge_shards(args.input_dir, args.output)
    print(f"Merged {count} works into {args.output}")


if __name__ == "__main__":
    main()
