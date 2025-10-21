"""
Persistence helpers for ETF profile data.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from mxm_dataio.models import Response as IoResponse

from mxm_datakraken.sources.justetf.common.models import JustETFProfile


def _write_json(path: Path, data: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _save_profile_provenance(base_path: Path, *, isin: str, resp: IoResponse) -> Path:
    meta = {
        "isin": isin,
        "dataio_response_id": resp.id,
        "dataio_request_id": resp.request_id,
        "checksum": resp.checksum,
        "path": resp.path,
        "created_at": resp.created_at.isoformat(),
        "sequence": resp.sequence,
        "size_bytes": resp.size_bytes,
    }
    out = base_path / "profiles" / "provenance" / f"{isin}.json"
    return _write_json(out, meta)


def save_profile(
    profile: JustETFProfile,
    base_path: Path,
    *,
    provenance: IoResponse | None = None,  # NEW
) -> Path:
    """Persist a single parsed profile JSON and (optionally) its provenance sidecar."""
    profiles_dir = base_path / "profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)
    isin = profile.get("isin")
    if not isinstance(isin, str) or not isin:
        raise ValueError("profile missing isin")

    out = profiles_dir / f"{isin}.json"
    _write_json(out, profile)

    if provenance is not None:
        _save_profile_provenance(base_path, isin=isin, resp=provenance)

    return out


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
