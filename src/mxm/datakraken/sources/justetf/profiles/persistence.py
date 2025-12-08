"""
Persistence helpers for ETF profile data (bucketed, volatile-source friendly).

Layout (per as_of_bucket):
    <base_path>/profiles/
        ├─ <as_of_bucket>/                     # e.g., "2025-10-30"
        │   ├─ <ISIN>/
        │   │   ├─ profile.parsed.json
        │   │   └─ profile.response.json       # provenance sidecar (optional)
        │   └─ profiles.parsed.json
        # optional aggregate snapshot for the bucket
        └─ latest → <as_of_bucket>/            # symlink (or fallback file pointer)

Bucket resolution order for writes:
1) provenance.as_of_bucket (if provided),
2) explicit as_of_bucket argument (if provided),
3) date.today().isoformat() fallback.

Reads default to the "latest" pointer if no bucket is given.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import cast

from mxm.dataio.models import Response as IoResponse
from mxm.types import JSONLike

from mxm.datakraken.common.file_io import read_json, write_json
from mxm.datakraken.common.latest_bucket import (
    resolve_latest_bucket,
    update_latest_pointer,
)
from mxm.datakraken.sources.justetf.common.models import JustETFProfile

# ---------------------------
# Internal path helper
# ---------------------------


def _bucket_dir(base_path: Path, *, bucket: str) -> Path:
    return base_path / "profiles" / bucket


def _profile_dir(base_path: Path, *, bucket: str, isin: str) -> Path:
    return _bucket_dir(base_path, bucket=bucket) / isin


# ---------------------------
# Provenance writer (bucketed)
# ---------------------------


def _write_profile_provenance(
    base_path: Path,
    *,
    isin: str,
    bucket: str,
    resp: IoResponse,
) -> Path:
    meta: JSONLike = {
        "isin": isin,
        "as_of_bucket": bucket,
        "dataio_response_id": resp.id,
        "dataio_request_id": resp.request_id,
        "checksum": resp.checksum,
        "path": resp.path,
        "created_at": resp.created_at.isoformat(),
        "sequence": resp.sequence,
        "size_bytes": resp.size_bytes,
    }
    out = _profile_dir(base_path, bucket=bucket, isin=isin) / "profile.response.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    return write_json(out, meta)


# ---------------------------
# Public API
# ---------------------------


def save_profile(
    profile: JustETFProfile,
    base_path: Path,
    *,
    provenance: IoResponse | None = None,
    as_of_bucket: str | None = None,
    write_latest: bool = True,
) -> Path:
    """
    Persist a single parsed profile JSON and (optionally) its provenance sidecar,
    under the bucketed layout.

    Writes:
        <base>/profiles/<bucket>/<ISIN>/profile.parsed.json
        <base>/profiles/<bucket>/<ISIN>/profile.response.json (if provenance)

    Returns:
        Path to 'profile.parsed.json'.
    """
    # Determine bucket
    bucket = (
        getattr(provenance, "as_of_bucket", None)
        or as_of_bucket
        or date.today().isoformat()
    )

    isin = profile.get("isin")
    if not isinstance(isin, str) or not isin:  # pyright: ignore[reportUnnecessaryIsInstance]
        raise ValueError("Profile must include non-empty 'isin' (str)")

    out_dir = _profile_dir(base_path, bucket=bucket, isin=isin)
    out_dir.mkdir(parents=True, exist_ok=True)

    parsed_path = out_dir / "profile.parsed.json"
    write_json(parsed_path, cast(JSONLike, profile))

    if provenance is not None:
        _write_profile_provenance(base_path, isin=isin, bucket=bucket, resp=provenance)

    if write_latest:
        update_latest_pointer(base_path / "profiles", bucket)

    return parsed_path


def load_profile(
    base_path: Path,
    *,
    isin: str,
    bucket: str | None = None,
) -> JustETFProfile:
    """
    Load a single parsed profile from a bucket (or 'latest' if none specified).
    """
    profiles_root = base_path / "profiles"
    use_bucket = bucket or resolve_latest_bucket(profiles_root)
    if use_bucket is None:
        raise FileNotFoundError(
            "No buckets found under <base>/profiles and no 'latest' pointer."
        )

    path = _profile_dir(base_path, bucket=use_bucket, isin=isin) / "profile.parsed.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Profile for ISIN '{isin}' not found in bucket '{use_bucket}'."
        )
    return cast(JustETFProfile, read_json(path))


def save_profiles_snapshot(
    profiles: list[JustETFProfile],
    base_path: Path,
    *,
    provenance: IoResponse | None = None,
    as_of_bucket: str | None = None,
    write_latest: bool = True,
) -> Path:
    """
    Save an aggregate snapshot of ETF profiles into the bucket root.

    Writes:
        <base>/profiles/<bucket>/profiles.parsed.json

    Notes:
        - This does **not** write per-ISIN files. Use `save_profile(...)` for that.
        - Kept as a convenience for full-bucket aggregate views and quick inspection.
    """
    bucket = (
        getattr(provenance, "as_of_bucket", None)
        or as_of_bucket
        or date.today().isoformat()
    )
    bucket_root = _bucket_dir(base_path, bucket=bucket)
    bucket_root.mkdir(parents=True, exist_ok=True)

    agg_path = bucket_root / "profiles.parsed.json"
    write_json(agg_path, cast(JSONLike, list(profiles)))

    if write_latest:
        update_latest_pointer(base_path / "profiles", bucket)

    return agg_path
