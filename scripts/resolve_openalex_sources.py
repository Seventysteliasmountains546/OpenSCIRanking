#!/usr/bin/env python
from __future__ import annotations

import argparse

import pandas as pd
import requests
from tqdm import tqdm

from opensci_v2.openalex import OpenAlexClient
from opensci_v2.resolve import choose_best_search_result, normalize_name, source_to_row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="CSV input with journal_name and optional issn columns")
    parser.add_argument("--output", required=True, help="CSV output with OpenAlex source resolution")
    parser.add_argument("--mailto", default=None, help="Contact email for polite OpenAlex requests")
    parser.add_argument("--search-fallback", action="store_true", help="Use source search when ISSN lookup fails")
    parser.add_argument("--limit", type=int, default=None, help="Only resolve the first N journals")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_df = pd.read_csv(args.input)
    if "journal_name" not in input_df.columns:
        raise ValueError("Input CSV must contain a 'journal_name' column.")

    if args.limit is not None:
        input_df = input_df.head(args.limit).copy()

    client = OpenAlexClient(mailto=args.mailto, delay_seconds=0.0)
    resolved_rows: list[dict] = []

    for row in tqdm(input_df.itertuples(index=False), total=len(input_df), desc="Resolving sources"):
        journal_name = str(getattr(row, "journal_name", "") or "")
        raw_issn = str(getattr(row, "issn", "") or "")
        row_dict = row._asdict()
        result = {
            **row_dict,
            "resolve_status": "unresolved",
            "resolve_method": "",
            "resolve_error": "",
            "openalex_source_id": "",
            "resolved_display_name": "",
            "resolved_issn_l": "",
            "resolved_issn": "",
            "resolved_works_count": 0,
            "resolved_cited_by_count": 0,
            "resolved_is_oa": False,
            "resolved_country_code": "",
        }

        source = None
        try:
            normalized_issn = client.normalize_issn(raw_issn)
            if normalized_issn:
                source = client.get_source_by_issn(normalized_issn)
                result.update(source_to_row(source))
                result["resolve_status"] = "resolved"
                result["resolve_method"] = "issn_lookup"
        except requests.HTTPError as exc:
            result["resolve_error"] = f"issn_lookup_failed: {exc}"
        except ValueError:
            pass
        except Exception as exc:
            result["resolve_error"] = f"issn_lookup_failed: {exc}"

        if source is None and args.search_fallback:
            try:
                results = client.search_sources(journal_name, per_page=10)
                best, method = choose_best_search_result(
                    results,
                    journal_name=journal_name,
                    issn=client.normalize_issn(raw_issn),
                )
                if best is not None:
                    source = best
                    result.update(source_to_row(source))
                    result["resolve_status"] = "resolved"
                    result["resolve_method"] = method
                    result["resolve_error"] = ""
                else:
                    result["resolve_error"] = method
            except Exception as exc:
                result["resolve_error"] = f"search_failed: {exc}"

        result["journal_name_norm"] = normalize_name(journal_name)
        resolved_rows.append(result)

    resolved_df = pd.DataFrame(resolved_rows)
    resolved_df.to_csv(args.output, index=False)
    print(
        f"Wrote {len(resolved_df)} rows to {args.output} | "
        f"resolved={(resolved_df['resolve_status'] == 'resolved').sum()} "
        f"unresolved={(resolved_df['resolve_status'] != 'resolved').sum()}"
    )


if __name__ == "__main__":
    main()
