from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from . import config


def ensure_output_dirs() -> None:
    """Create curated/quality directories if needed."""
    config.CURATED_DIR.mkdir(parents=True, exist_ok=True)
    config.QUALITY_DIR.mkdir(parents=True, exist_ok=True)


def load_raw_json(path: Path | str = config.RAW_JSON_PATH) -> list[dict[str, Any]]:
    """Load raw JSON and enforce top-level list semantics."""
    path = Path(path)
    records = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError(f"Expected top-level list in {path}, got: {type(records).__name__}")
    return records


def write_csv(df: pd.DataFrame, path: Path | str, index: bool = False) -> None:
    """Persist dataframe to CSV and ensure parent directory exists."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=index)


def is_blank(value: Any) -> bool:
    """True for null/empty string values."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False
