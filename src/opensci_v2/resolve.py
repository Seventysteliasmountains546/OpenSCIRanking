from __future__ import annotations

import re
from typing import Any

import pandas as pd


def normalize_name(value: str | None) -> str:
    if value is None:
        return ""
    text = re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()
    return re.sub(r"\s+", " ", text)


def source_to_row(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "openalex_source_id": source.get("id", ""),
        "resolved_display_name": source.get("display_name", ""),
        "resolved_issn_l": source.get("issn_l", ""),
        "resolved_issn": "|".join(source.get("issn") or []),
        "resolved_works_count": source.get("works_count", 0),
        "resolved_cited_by_count": source.get("cited_by_count", 0),
        "resolved_is_oa": source.get("is_oa", False),
        "resolved_country_code": source.get("country_code", ""),
    }


def choose_best_search_result(
    results: list[dict[str, Any]],
    *,
    journal_name: str,
    issn: str,
) -> tuple[dict[str, Any] | None, str]:
    if not results:
        return None, "search_no_result"

    normalized_name = normalize_name(journal_name)
    normalized_issn = {token for token in str(issn).split("|") if token}

    def score(item: dict[str, Any]) -> tuple[int, int, int]:
        item_name = normalize_name(item.get("display_name", ""))
        item_issns = set(item.get("issn") or [])
        exact_name = int(item_name == normalized_name and normalized_name != "")
        exact_issn = int(bool(normalized_issn.intersection(item_issns)))
        cited_by = int(item.get("cited_by_count") or 0)
        return (exact_issn, exact_name, cited_by)

    best = max(results, key=score)
    best_score = score(best)
    if best_score[0] == 0 and best_score[1] == 0:
        return None, "search_ambiguous"
    if best_score[0] == 1:
        return best, "search_issn_match"
    return best, "search_name_match"


def prepare_resolution_output(input_df: pd.DataFrame, resolved_rows: list[dict[str, Any]]) -> pd.DataFrame:
    resolved_df = pd.DataFrame(resolved_rows)
    if resolved_df.empty:
        resolved_df = pd.DataFrame(
            columns=[
                *input_df.columns,
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
            ]
        )
    return resolved_df
