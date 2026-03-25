from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from opensci_v2.io import write_parquet


STATE_COLUMNS = [
    "source_id",
    "display_name",
    "status",
    "attempts",
    "works_count",
    "output_path",
    "last_error",
    "updated_at",
]


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def source_token(source_id: str) -> str:
    token = source_id.rstrip("/").split("/")[-1]
    return token.replace(":", "_")


def shard_path(output_dir: str | Path, source_id: str) -> Path:
    return Path(output_dir) / f"{source_token(source_id)}.parquet"


def load_state(state_path: str | Path) -> pd.DataFrame:
    path = Path(state_path)
    if not path.exists():
        return pd.DataFrame(columns=STATE_COLUMNS)

    state_df = pd.read_csv(path)
    for column in STATE_COLUMNS:
        if column not in state_df.columns:
            state_df[column] = ""
    return state_df[STATE_COLUMNS].copy()


def build_source_manifest(sources_df: pd.DataFrame, state_df: pd.DataFrame) -> pd.DataFrame:
    manifest = sources_df[["source_id"]].dropna().drop_duplicates().copy()
    if "display_name" in sources_df.columns:
        manifest = manifest.merge(
            sources_df[["source_id", "display_name"]].drop_duplicates(),
            on="source_id",
            how="left",
        )
    else:
        manifest["display_name"] = ""

    if state_df.empty:
        manifest["status"] = "pending"
        manifest["attempts"] = 0
        manifest["works_count"] = 0
        manifest["output_path"] = ""
        manifest["last_error"] = ""
        manifest["updated_at"] = ""
        return manifest[STATE_COLUMNS].copy()

    merged = manifest.merge(
        state_df,
        on=["source_id", "display_name"],
        how="left",
        suffixes=("", "_state"),
    )
    merged["status"] = merged["status"].fillna("pending")
    merged["attempts"] = merged["attempts"].fillna(0).astype(int)
    merged["works_count"] = merged["works_count"].fillna(0).astype(int)
    merged["output_path"] = merged["output_path"].fillna("")
    merged["last_error"] = merged["last_error"].fillna("")
    merged["updated_at"] = merged["updated_at"].fillna("")
    return merged[STATE_COLUMNS].copy()


def save_state(state_df: pd.DataFrame, state_path: str | Path) -> None:
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    state_df[STATE_COLUMNS].to_csv(path, index=False)


def upsert_state_row(state_df: pd.DataFrame, row: dict[str, Any]) -> pd.DataFrame:
    new_row = pd.DataFrame([{column: row.get(column, "") for column in STATE_COLUMNS}])
    if state_df.empty:
        return new_row

    remaining = state_df[state_df["source_id"] != row["source_id"]].copy()
    return pd.concat([remaining, new_row], ignore_index=True)


def collect_shards(shard_dir: str | Path) -> list[Path]:
    path = Path(shard_dir)
    if not path.exists():
        return []
    return sorted(path.glob("*.parquet"))


def merge_shards(shard_dir: str | Path, output_path: str | Path) -> int:
    shards = collect_shards(shard_dir)
    if not shards:
        write_parquet(pd.DataFrame(), output_path)
        return 0

    merged = pd.concat((pd.read_parquet(shard) for shard in shards), ignore_index=True)
    write_parquet(merged, output_path)
    return len(merged)


@dataclass
class BatchSelection:
    pending: pd.DataFrame
    skipped: pd.DataFrame


def select_sources(
    manifest_df: pd.DataFrame,
    *,
    resume: bool,
    retry_failures: bool,
    overwrite: bool,
    limit_sources: int | None,
) -> BatchSelection:
    selection = manifest_df.copy()
    if overwrite:
        selection["status"] = "pending"

    def should_run(status: str) -> bool:
        if overwrite:
            return True
        if status in ("pending", "", None):
            return True
        if status == "failed":
            return retry_failures
        if status == "success":
            return not resume
        return True

    run_mask = selection["status"].apply(should_run)
    pending = selection[run_mask].copy()
    skipped = selection[~run_mask].copy()
    if limit_sources is not None:
        pending = pending.head(limit_sources).copy()
    return BatchSelection(pending=pending, skipped=skipped)
