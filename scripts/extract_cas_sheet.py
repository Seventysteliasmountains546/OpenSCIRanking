#!/usr/bin/env python
from __future__ import annotations

import argparse

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="CAS Excel workbook path")
    parser.add_argument("--sheet", required=True, help="Sheet name to extract")
    parser.add_argument("--output", required=True, help="CSV output path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = pd.read_excel(args.input, sheet_name=args.sheet)
    keep_columns = [column for column in ["Journal", "ISSN", "分区"] if column in df.columns]
    if not keep_columns:
        raise ValueError("No expected columns found in the selected sheet.")
    df = df[keep_columns].copy()
    if "Journal" in df.columns:
        df = df.rename(columns={"Journal": "journal_name"})
    if "ISSN" in df.columns:
        df = df.rename(columns={"ISSN": "issn"})
    if "分区" in df.columns:
        df = df.rename(columns={"分区": "cas_partition"})
    df["source_sheet"] = args.sheet
    df.to_csv(args.output, index=False)
    print(f"Wrote {len(df)} rows to {args.output}")


if __name__ == "__main__":
    main()
