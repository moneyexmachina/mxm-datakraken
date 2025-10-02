"""
Persistence helpers for ETF profile data.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any


def save_profile(profile: dict[str, Any], base_path: Path) -> Path:
    """
    Save a single ETF profile to {base_path}/profiles/{isin}.json.

    Args:
        profile: Parsed ETF profile dictionary, must include "isin".
        base_path: Root directory for profile storage.

    Returns:
        Path to the written JSON file.
    """
    raw_isin = profile.get("isin")
    if raw_isin is None or not isinstance(raw_isin, str) or not raw_isin.strip():
        raise ValueError("Profile is missing required key 'isin'")

    isin: str = raw_isin

    profile_dir: Path = base_path / "profiles"
    profile_dir.mkdir(parents=True, exist_ok=True)

    path: Path = profile_dir / f"{isin}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(profile, f, ensure_ascii=False, indent=2)

    return path


def save_profiles_snapshot(
    profiles: list[dict[str, Any]],
    base_path: Path,
    as_of: date | None = None,
    write_latest: bool = True,
) -> Path:
    """
    Save a full snapshot of ETF profiles.

    Creates:
        - A dated file: profiles_YYYY-MM-DD.json
        - Optionally, profiles/latest.json

    Args:
        profiles: List of ETF profiles.
        base_path: Root directory for profile storage.
        as_of: Date of snapshot (defaults to today).
        write_latest: Whether to also update latest.json.

    Returns:
        Path to the dated snapshot file.
    """
    snapshot_date: date = as_of or date.today()

    profile_dir: Path = base_path / "profiles"
    profile_dir.mkdir(parents=True, exist_ok=True)

    filename: str = f"profiles_{snapshot_date.isoformat()}.json"
    filepath: Path = profile_dir / filename

    with filepath.open("w", encoding="utf-8") as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)

    if write_latest:
        latest_path: Path = profile_dir / "latest.json"
        with latest_path.open("w", encoding="utf-8") as f:
            json.dump(profiles, f, ensure_ascii=False, indent=2)

    return filepath
