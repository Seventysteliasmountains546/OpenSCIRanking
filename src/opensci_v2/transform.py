from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


def normalize_sources(records: Iterable[dict]) -> pd.DataFrame:
    rows = []
    for record in records:
        rows.append(
            {
                "source_id": record.get("id"),
                "display_name": record.get("display_name"),
                "issn_l": record.get("issn_l"),
                "issn": "|".join(record.get("issn") or []),
                "works_count": record.get("works_count"),
                "cited_by_count": record.get("cited_by_count"),
                "is_in_doaj": (record.get("is_in_doaj") or False),
                "is_oa": (record.get("is_oa") or False),
                "country_code": (record.get("country_code") or ""),
            }
        )
    return pd.DataFrame(rows)


def normalize_works(records: Iterable[dict]) -> pd.DataFrame:
    rows = []
    for record in records:
        primary_location = record.get("primary_location") or {}
        source = primary_location.get("source") or {}
        authorships = record.get("authorships") or []
        author_ids = [item.get("author", {}).get("id") for item in authorships if item.get("author")]
        rows.append(
            {
                "work_id": record.get("id"),
                "title": record.get("title"),
                "publication_year": record.get("publication_year"),
                "cited_by_count": record.get("cited_by_count"),
                "source_id": source.get("id"),
                "source_display_name": source.get("display_name"),
                "referenced_works": record.get("referenced_works") or [],
                "author_ids": [author_id for author_id in author_ids if author_id],
            }
        )
    return pd.DataFrame(rows)


def build_journal_edges(works_df: pd.DataFrame) -> pd.DataFrame:
    source_lookup = works_df.set_index("work_id")["source_id"].to_dict()
    edge_rows = []
    for row in works_df.itertuples(index=False):
        citing_source = row.source_id
        if not citing_source:
            continue
        for referenced_work in row.referenced_works:
            cited_source = source_lookup.get(referenced_work)
            if not cited_source:
                continue
            edge_rows.append({"citing_source_id": citing_source, "cited_source_id": cited_source, "weight": 1})

    if not edge_rows:
        return pd.DataFrame(columns=["citing_source_id", "cited_source_id", "weight"])

    edges_df = pd.DataFrame(edge_rows)
    return (
        edges_df.groupby(["citing_source_id", "cited_source_id"], as_index=False)["weight"]
        .sum()
        .sort_values(["weight", "citing_source_id", "cited_source_id"], ascending=[False, True, True])
    )


def build_fractional_journal_edges(works_df: pd.DataFrame) -> pd.DataFrame:
    source_lookup = works_df.set_index("work_id")["source_id"].to_dict()
    edge_rows = []
    for row in works_df.itertuples(index=False):
        citing_source = row.source_id
        if not citing_source:
            continue
        total_references = len(row.referenced_works)
        if total_references == 0:
            continue
        per_reference_weight = 1.0 / total_references
        for referenced_work in row.referenced_works:
            cited_source = source_lookup.get(referenced_work)
            if not cited_source:
                continue
            edge_rows.append(
                {
                    "citing_source_id": citing_source,
                    "cited_source_id": cited_source,
                    "weight": per_reference_weight,
                }
            )

    if not edge_rows:
        return pd.DataFrame(columns=["citing_source_id", "cited_source_id", "weight"])

    edges_df = pd.DataFrame(edge_rows)
    return (
        edges_df.groupby(["citing_source_id", "cited_source_id"], as_index=False)["weight"]
        .sum()
        .sort_values(["weight", "citing_source_id", "cited_source_id"], ascending=[False, True, True])
    )
