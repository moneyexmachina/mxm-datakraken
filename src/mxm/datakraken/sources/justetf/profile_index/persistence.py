"""
Persistence helpers for the ETF Profile Index.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, cast

from mxm_dataio.models import Response as IoResponse

from mxm.datakraken.common.file_io import read_json, write_json
from mxm.datakraken.common.latest_bucket import resolve_latest_bucket
from mxm.datakraken.common.types import JSONLike
from mxm.datakraken.sources.justetf.profile_index.discover import (
    ETFProfileIndexEntry,
)


def _write_index_provenance(
    bucket_dir: Path,
    *,
    bucket: str,
    resp: IoResponse,
) -> Path:
    """Write the bucketed provenance sidecar for the profile index.

    Layout (new only):
        <pi_root>/<bucket>/profile_index.response.json

    Args:
        pi_root: The 'profile_index' directory (i.e., base_path / 'profile_index').
        bucket_dir: The resolved '<pi_root>/<bucket>' directory (already created).
        bucket: The as_of_bucket identifier.
        resp: The DataIO Response returned from the sitemap fetch.

    Returns: Path to the written sidecar JSON.
    """
    sidecar: JSONLike = {
        "source": "justetf",
        "kind": "profile_index",
        "bucket": bucket,
        "response": {
            "id": getattr(resp, "id", None),
            "request_id": getattr(resp, "request_id", None),
            "path": getattr(resp, "path", None),
            "checksum": getattr(resp, "checksum", None),
            "created_at": getattr(resp, "created_at", None)
            and resp.created_at.isoformat(),  # type: ignore[attr-defined]
            "size_bytes": getattr(resp, "size_bytes", None),
            "sequence": getattr(resp, "sequence", None),
            # Policy identity (mxm-dataio >= 0.3.0)
            "cache_mode": getattr(resp, "cache_mode", None),
            "ttl_seconds": getattr(resp, "ttl", None),
            "as_of_bucket": getattr(resp, "as_of_bucket", None),
            "cache_tag": getattr(resp, "cache_tag", None),
        },
    }
    return write_json(bucket_dir / "profile_index.response.json", sidecar)


def save_profile_index(
    entries: list[ETFProfileIndexEntry],
    base_path: Path,
    *,
    provenance: IoResponse | None = None,
    as_of_bucket: str | None = None,
    write_latest: bool = True,
) -> Path:
    """Persist the profile index snapshot and (optionally) its provenance sidecar."""
    from mxm.datakraken.common.latest_bucket import update_latest_pointer

    if provenance is None and not as_of_bucket:
        raise ValueError("Provide either 'provenance' (preferred) or 'as_of_bucket'.")
    bucket = as_of_bucket or getattr(provenance, "as_of_bucket", None)
    if not isinstance(bucket, str) or not bucket:
        raise ValueError("Missing/invalid as_of_bucket.")

    pi_root = base_path / "profile_index"
    bucket_dir = pi_root / bucket
    bucket_dir.mkdir(parents=True, exist_ok=True)

    parsed_path = bucket_dir / "profile_index.parsed.json"
    write_json(parsed_path, cast(JSONLike, entries))

    if provenance is not None:
        _write_index_provenance(
            bucket_dir=bucket_dir,
            bucket=bucket,
            resp=provenance,
        )
    if write_latest:
        update_latest_pointer(pi_root, bucket)
    return parsed_path


def load_profile_index(
    base_path: Path,
    *,
    as_of_bucket: str | None = None,
) -> List[ETFProfileIndexEntry]:
    """
    Load the ETF Profile Index from the bucketed layout.

    Priority:
      1) If `as_of_bucket` is provided:
      load profile_index/<bucket>/profile_index.parsed.json
      2) Else, try the 'latest' pointer (symlink or LATEST_BUCKET)
      3) Else, fall back to the lexicographically last bucket directory

    Raises:
      FileNotFoundError if nothing suitable is found.
    """
    pi_root = base_path / "profile_index"
    if not pi_root.exists():
        raise FileNotFoundError(f"Profile index directory not found: {pi_root}")

    bucket = as_of_bucket or resolve_latest_bucket(pi_root)
    if bucket is None:
        # fallback: pick lexicographically last bucket dir
        buckets = sorted(
            [
                p.name
                for p in pi_root.iterdir()
                if p.is_dir() and p.name not in {"latest"}
            ]
        )
        if not buckets:
            raise FileNotFoundError("No profile index buckets found.")
        bucket = buckets[-1]

    parsed_path = pi_root / bucket / "profile_index.parsed.json"
    if not parsed_path.exists():
        raise FileNotFoundError(
            f"Parsed index not found for bucket '{bucket}': {parsed_path}"
        )

    return cast(List[ETFProfileIndexEntry], read_json(parsed_path))
