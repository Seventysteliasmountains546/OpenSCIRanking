from pathlib import Path

import pandas as pd


def write_parquet(df: pd.DataFrame, output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def read_parquet(input_path: str | Path) -> pd.DataFrame:
    return pd.read_parquet(Path(input_path))


def append_csv(df: pd.DataFrame, output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    df.to_csv(path, mode="a", index=False, header=write_header)
