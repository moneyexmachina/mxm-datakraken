"""
Persistence helpers for the ETF Profile Index.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

from mxm_datakraken.sources.justetf.profile_index.discover import (
    ETFProfileIndexEntry,
)


def save_profile_index(
    index: Sequence[ETFProfileIndexEntry],
    base_path: Path,
    as_of: date | None = None,
    write_latest: bool = True,
) -> Path:
    """
    Save an ETF Profile Index snapshot to disk.

    Args:
        index: Sequence of ETFProfileIndexEntry dicts to save.
        base_path: Root folder where profile_index files are stored.
        as_of: Optional date for the snapshot. Defaults to today.
        write_latest: Whether to also write/update a 'latest.json'.

    Returns:
        Path to the snapshot file written.
    """
    as_of = as_of or date.today()

    # Ensure directory exists
    index_dir = base_path / "profile_index"
    index_dir.mkdir(parents=True, exist_ok=True)

    filename = f"profile_index_{as_of.isoformat()}.json"
    filepath = index_dir / filename

    with filepath.open("w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    if write_latest:
        latest_path = index_dir / "latest.json"
        with latest_path.open("w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=False, indent=2)

    return filepath


def load_profile_index(
    base_path: Path, as_of: date | None = None
) -> list[ETFProfileIndexEntry]:
    """
    Load an ETF Profile Index snapshot from disk.

    Args:
        base_path: Root folder where profile_index files are stored.
        as_of: Optional date. If None, load 'latest.json'.
               If provided, load the most recent snapshot <= as_of.

    Returns:
        A list of ETFProfileIndexEntry dicts.

    Raises:
        FileNotFoundError: If no matching snapshot is found.
        ValueError: If profile_index directory is missing or empty.
    """
    index_dir = base_path / "profile_index"
    if not index_dir.exists():
        raise ValueError(f"Profile index directory not found: {index_dir}")

    if as_of is None:
        latest_path = index_dir / "latest.json"
        if not latest_path.exists():
            raise FileNotFoundError(f"No latest.json found in {index_dir}")
        return json.loads(latest_path.read_text(encoding="utf-8"))

    # Gather all dated snapshots
    snapshots: list[tuple[date, Path]] = []
    for file in index_dir.glob("profile_index_*.json"):
        stem = file.stem  # e.g. "profile_index_2025-10-01"
        try:
            snap_date = datetime.strptime(
                stem.replace("profile_index_", ""), "%Y-%m-%d"
            ).date()
        except ValueError:
            continue
        if snap_date <= as_of:
            snapshots.append((snap_date, file))

    if not snapshots:
        raise FileNotFoundError(
            f"No profile index snapshot found on or before {as_of.isoformat()}"
        )

    # Pick the latest snapshot <= as_of
    _, chosen_file = max(snapshots, key=lambda x: x[0])
    return json.loads(chosen_file.read_text(encoding="utf-8"))
