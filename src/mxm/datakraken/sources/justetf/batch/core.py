"""

Core helpers for bucket-aware justETF batch orchestration.

This module keeps the orchestration (`run.py`) thin by factoring out:
- result typing (`BatchStats`)
- bucket resolution policy (`resolve_bucket`)
- quick skip predicate for idempotency (`should_skip`)
- a single-entry processing unit (`process_one_entry`)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Optional, Tuple

from mxm_config import MXMConfig

from mxm.datakraken.common.latest_bucket import resolve_latest_bucket
from mxm.datakraken.sources.justetf.common.models import (
    ETFProfileIndexEntry,
    JustETFProfile,
)

# ---------- Result typing ----------


@dataclass(frozen=True)
class BatchStats:
    bucket: str
    ok: int
    skip: int
    err: int
    snapshot_path: Path


# ---------- Internal path utility ----------


def _bucket_profile_path(base_path: Path, bucket: str, isin: str) -> Path:
    """profiles/<bucket>/<ISIN>/profile.parsed.json"""
    return base_path / "profiles" / bucket / isin / "profile.parsed.json"


# ---------- Policies / Predicates ----------


def resolve_bucket(
    *,
    provided: Optional[str],
    first_resp_bucket: Optional[str],
    profiles_root: Path,
    today_iso: Optional[str] = None,
) -> str:
    """
    Resolve the as_of_bucket to use for a run.

    Precedence:
      1) provided (CLI/arg)
      2) first_resp_bucket (from provenance of first HTTP fetch)
      3) latest bucket present on disk under profiles_root (if any)
      4) today (YYYY-MM-DD)
    """
    if provided:
        return provided
    if first_resp_bucket:
        return first_resp_bucket
    try:
        latest = resolve_latest_bucket(profiles_root)
        if latest:
            return latest
    except Exception:
        pass
    return today_iso or date.today().isoformat()


def should_skip(
    *,
    base_path: Path,
    bucket: Optional[str],
    isin: str,
    force_refresh: bool,
) -> Tuple[bool, Optional[str]]:
    """
    Decide whether to skip downloading/persisting a profile.

    Skip only when:
      - a bucket is already known, AND
      - force_refresh is False, AND
      - the bucketed profile file exists.

    Returns:
      (skip?, reason) where reason is typically "exists" when True.
    """
    if force_refresh or bucket is None:
        return (False, None)
    target = _bucket_profile_path(base_path, bucket, isin)
    if target.exists():
        return (True, "exists")
    return (False, None)


# ---------- Single-entry processing unit ----------


def process_one_entry(
    *,
    cfg: MXMConfig,
    base_path: Path,
    entry: ETFProfileIndexEntry,
    bucket: Optional[str],
    download_html: Callable[[MXMConfig, str, str], Tuple[str, object]],
    parse: Callable[[str, str], JustETFProfile],
    save: Callable[..., Path],  # expected signature of save_profile(...)
    write_latest: bool,
) -> Tuple[str, Optional[JustETFProfile], Optional[str], Optional[str]]:
    """
    Process a single ETF index entry:
      - download HTML
      - parse to profile
      - choose bucket if None (prefer response.as_of_bucket, else today)
      - persist via save()
    Returns:
      (status, profile_or_None, bucket_used_or_None, error_msg_or_None)
      where status ∈ {"ok","skip","err"}; "skip" is not produced here (caller decides).
    """
    isin = entry["isin"]
    url = entry["url"]

    try:
        html, resp = download_html(cfg, isin, url)
        profile: JustETFProfile = parse(html, isin)
        profile["source_url"] = url

        bucket_used = (
            bucket or getattr(resp, "as_of_bucket", None) or date.today().isoformat()
        )

        # Persist to bucket
        save(
            profile,
            base_path=base_path,
            as_of_bucket=bucket_used,
            provenance=resp,
            write_latest=write_latest,
        )

        return ("ok", profile, bucket_used, None)

    except (
        Exception
    ) as exc:  # pragma: no cover (error paths covered by orchestrator tests)
        return ("err", None, bucket, str(exc))
