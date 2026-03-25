#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from opensci_v2.batch import (
    build_source_manifest,
    load_state,
    merge_shards,
    save_state,
    select_sources,
    shard_path,
    upsert_state_row,
    utc_now,
)
from opensci_v2.config import ensure_data_dirs
from opensci_v2.openalex import OpenAlexClient
from opensci_v2.transform import normalize_works


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", required=True, help="Parquet file with source_id column")
    parser.add_argument("--output-dir", required=True, help="Directory for per-source Parquet shards")
    parser.add_argument("--state-path", required=True, help="CSV state file for resumable batch fetch")
    parser.add_argument("--merged-output", default=None, help="Optional merged Parquet output path")
    parser.add_argument("--start-year", type=int, required=True, help="Start publication year")
    parser.add_argument("--end-year", type=int, default=None, help="Optional end publication year")
    parser.add_argument("--mailto", default=None, help="Contact email for polite OpenAlex requests")
    parser.add_argument("--per-page", type=int, default=200, help="OpenAlex page size")
    parser.add_argument("--delay-seconds", type=float, default=0.1, help="Delay between paginated requests")
    parser.add_argument("--max-retries", type=int, default=4, help="Max retries per HTTP request")
    parser.add_argument("--retry-failures", action="store_true", help="Retry sources marked failed in the state file")
    parser.add_argument("--resume", action="store_true", help="Skip sources already marked success")
    parser.add_argument("--overwrite", action="store_true", help="Ignore state and overwrite existing successful shards")
    parser.add_argument("--limit-sources", type=int, default=None, help="Only process the first N selected sources")
    parser.add_argument("--max-records-per-source", type=int, default=None, help="Optional cap for development sampling")
    parser.add_argument("--write-empty-shards", action="store_true", help="Persist empty shard files for zero-work sources")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_data_dirs()

    sources_df = pd.read_parquet(args.sources)
    if "source_id" not in sources_df.columns:
        raise ValueError("Sources Parquet must contain a 'source_id' column.")

    state_df = load_state(args.state_path)
    manifest_df = build_source_manifest(sources_df, state_df)
    selection = select_sources(
        manifest_df,
        resume=args.resume,
        retry_failures=args.retry_failures,
        overwrite=args.overwrite,
        limit_sources=args.limit_sources,
    )
    pending_df = selection.pending.sort_values(["status", "source_id"]).reset_index(drop=True)
    state_df = manifest_df.copy()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    client = OpenAlexClient(
        mailto=args.mailto,
        per_page=args.per_page,
        delay_seconds=args.delay_seconds,
        max_retries=args.max_retries,
    )

    processed = 0
    succeeded = 0
    failed = 0

    for row in tqdm(pending_df.itertuples(index=False), total=len(pending_df), desc="Fetching sources"):
        source_id = str(row.source_id)
        display_name = str(row.display_name or "")
        shard = shard_path(output_dir, source_id)
        attempts = int(row.attempts) + 1

        try:
            records: list[dict] = []
            for record in client.get_works_for_source(source_id, args.start_year, args.end_year):
                records.append(record)
                if args.max_records_per_source is not None and len(records) >= args.max_records_per_source:
                    break

            works_df = normalize_works(records)
            if not works_df.empty or args.write_empty_shards:
                works_df.to_parquet(shard, index=False)
                output_path = str(shard)
            else:
                output_path = ""

            state_df = upsert_state_row(
                state_df,
                {
                    "source_id": source_id,
                    "display_name": display_name,
                    "status": "success",
                    "attempts": attempts,
                    "works_count": len(works_df),
                    "output_path": output_path,
                    "last_error": "",
                    "updated_at": utc_now(),
                },
            )
            succeeded += 1
        except Exception as exc:
            state_df = upsert_state_row(
                state_df,
                {
                    "source_id": source_id,
                    "display_name": display_name,
                    "status": "failed",
                    "attempts": attempts,
                    "works_count": 0,
                    "output_path": str(shard) if shard.exists() else "",
                    "last_error": str(exc),
                    "updated_at": utc_now(),
                },
            )
            failed += 1
        finally:
            save_state(state_df, args.state_path)
            processed += 1

    if args.merged_output:
        merged_count = merge_shards(output_dir, args.merged_output)
        print(f"Merged {merged_count} works into {args.merged_output}")

    print(
        f"Processed {processed} sources | success={succeeded} failed={failed} "
        f"skipped={len(selection.skipped)} state={args.state_path}"
    )


if __name__ == "__main__":
    main()
