"""
Persistence helpers for ETF profile discovery.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

from mxm_datakraken.sources.justetf.discovery.discover import ETFProfile


def save_discovery_results(
    profiles: Sequence[ETFProfile],
    base_path: Path,
    as_of: date | None = None,
    write_latest: bool = True,
) -> Path:
    """
    Save ETF profile discovery results to disk.

    Args:
        profiles: Sequence of ETFProfile dicts to save.
        base_path: Root folder where discovery files are stored.
        as_of: Optional date for the snapshot. Defaults to today.
        write_latest: Whether to also write/update a 'latest.json'.

    Returns:
        Path to the snapshot file written.
    """
    as_of = as_of or date.today()

    # Ensure directory exists
    discovery_dir = base_path / "discovery"
    discovery_dir.mkdir(parents=True, exist_ok=True)

    filename = f"discovery_{as_of.isoformat()}.json"
    filepath = discovery_dir / filename

    with filepath.open("w", encoding="utf-8") as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)

    if write_latest:
        latest_path = discovery_dir / "latest.json"
        with latest_path.open("w", encoding="utf-8") as f:
            json.dump(profiles, f, ensure_ascii=False, indent=2)

    return filepath


def load_discovery_results(
    base_path: Path, as_of: date | None = None
) -> list[ETFProfile]:
    """
    Load ETF profile discovery results from disk.

    Args:
        base_path: Root folder where discovery files are stored.
        as_of: Optional date. If None, load 'latest.json'.
               If provided, load the most recent snapshot <= as_of.

    Returns:
        A list of ETFProfile dicts.

    Raises:
        FileNotFoundError: If no matching snapshot is found.
        ValueError: If discovery directory is missing or empty.
    """
    discovery_dir = base_path / "discovery"
    if not discovery_dir.exists():
        raise ValueError(f"Discovery directory not found: {discovery_dir}")

    if as_of is None:
        latest_path = discovery_dir / "latest.json"
        if not latest_path.exists():
            raise FileNotFoundError(f"No latest.json found in {discovery_dir}")
        return json.loads(latest_path.read_text(encoding="utf-8"))

    # Gather all dated snapshots
    snapshots: list[tuple[date, Path]] = []
    for file in discovery_dir.glob("discovery_*.json"):
        stem = file.stem  # e.g. "discovery_2025-10-01"
        try:
            snap_date = datetime.strptime(
                stem.replace("discovery_", ""), "%Y-%m-%d"
            ).date()
        except ValueError:
            continue
        if snap_date <= as_of:
            snapshots.append((snap_date, file))

    if not snapshots:
        raise FileNotFoundError(
            f"No discovery snapshot found on or before {as_of.isoformat()}"
        )

    # Pick the latest snapshot <= as_of
    _, chosen_file = max(snapshots, key=lambda x: x[0])
    return json.loads(chosen_file.read_text(encoding="utf-8"))
