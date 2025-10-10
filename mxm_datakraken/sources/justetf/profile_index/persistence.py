"""
Persistence helpers for the ETF Profile Index.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Tuple

from mxm_dataio.models import Response as IoResponse

from mxm_datakraken.sources.justetf.profile_index.discover import (
    ETFProfileIndexEntry,
)


def _write_json(path: Path, data: Any) -> Path:
    """Write JSON to disk, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _read_json(path: Path) -> Any:
    """Read JSON from disk."""
    return json.loads(path.read_text(encoding="utf-8"))


def _save_index_provenance(base_path: Path, *, as_of: date, resp: IoResponse) -> Path:
    """Write a sidecar provenance file for a given snapshot date."""
    meta = {
        "dataio_response_id": resp.id,
        "dataio_request_id": resp.request_id,
        "checksum": resp.checksum,
        "path": resp.path,
        "created_at": resp.created_at.isoformat(),
        "sequence": resp.sequence,
        "size_bytes": resp.size_bytes,
    }
    out = base_path / "profile_index" / f"provenance_{as_of.isoformat()}.json"
    return _write_json(out, meta)


def _iter_snapshot_files(pi_dir: Path) -> Iterable[Tuple[date, Path]]:
    """
    Yield (snapshot_date, path) for files named 'profile_index_YYYY-MM-DD.json'.
    Ignores files that do not match the expected pattern.
    """
    prefix = "profile_index_"
    suffix = ".json"
    for p in pi_dir.glob("profile_index_*.json"):
        name = p.name
        if not (name.startswith(prefix) and name.endswith(suffix)):
            continue
        # Extract the date portion between prefix and suffix
        dt_str = name[len(prefix) : -len(suffix)]
        try:
            yield datetime.strptime(dt_str, "%Y-%m-%d").date(), p
        except ValueError:
            # Skip malformed filenames
            continue


def _best_snapshot_path(pi_dir: Path, *, as_of: date) -> Path:
    """
    Return the path to the most recent snapshot <= as_of.
    Raises FileNotFoundError if none found.
    """
    candidates = [(d, p) for d, p in _iter_snapshot_files(pi_dir) if d <= as_of]
    if not candidates:
        raise FileNotFoundError(
            f"No profile index snapshot found on or before {as_of.isoformat()}"
        )
    _, best_path = max(candidates, key=lambda t: t[0])
    return best_path


def save_profile_index(
    entries: list[ETFProfileIndexEntry],
    base_path: Path,
    *,
    as_of: date,
    write_latest: bool = True,
    provenance: IoResponse | None = None,
) -> Path:
    """Persist the profile index snapshot and (optionally) its provenance sidecar."""
    pi_dir = base_path / "profile_index"
    pi_dir.mkdir(parents=True, exist_ok=True)

    out = pi_dir / f"profile_index_{as_of.isoformat()}.json"
    _write_json(out, entries)

    if write_latest:
        _write_json(pi_dir / "latest.json", entries)

    if provenance is not None:
        _save_index_provenance(base_path, as_of=as_of, resp=provenance)

    return out


def load_profile_index(
    base_path: Path,
    *,
    as_of: date | None = None,
) -> list[ETFProfileIndexEntry]:
    """
    Load the ETF Profile Index.

    Behavior
    --------
    - If as_of is None:
        * Prefer 'latest.json' if present.
        * Otherwise, load the newest dated snapshot 'profile_index_YYYY-MM-DD.json'.
    - If as_of is provided:
        * Load the most recent snapshot with date <= as_of.
        * If none exist, raise FileNotFoundError.

    Returns
    -------
    list[ETFProfileIndexEntry]
    """
    pi_dir = base_path / "profile_index"
    if not pi_dir.exists():
        raise FileNotFoundError(f"Profile index directory not found: {pi_dir}")

    if as_of is None:
        latest_path = pi_dir / "latest.json"
        if latest_path.exists():
            data = _read_json(latest_path)
            return list(data)  # type: ignore[return-value]
        # Fallback to newest dated snapshot
        candidates = list(_iter_snapshot_files(pi_dir))
        if not candidates:
            raise FileNotFoundError("No profile index snapshots found.")
        # Pick newest by date
        _, newest_path = max(candidates, key=lambda t: t[0])
        data = _read_json(newest_path)
        return list(data)  # type: ignore[return-value]

    # as_of provided: pick best snapshot <= as_of
    snap_path = _best_snapshot_path(pi_dir, as_of=as_of)
    data = _read_json(snap_path)
    return list(data)  # type: ignore[return-value]
