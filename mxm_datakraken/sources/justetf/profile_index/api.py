"""
mxm_datakraken.sources.justetf.profile_index.api

Public entry point for the ETF Profile Index (bucketed layout).

Behavior:
- Reads from <base>/profile_index/<bucket>/profile_index.parsed.json
  (defaults to 'latest' when no bucket provided).
- On refresh, writes to a chosen bucket and updates 'latest'.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from mxm_config import MXMConfig

from mxm_datakraken.sources.justetf.profile_index.discover import (
    SITEMAP_URL,
    ETFProfileIndexEntry,
    build_profile_index,
)
from mxm_datakraken.sources.justetf.profile_index.persistence import (
    load_profile_index,
    save_profile_index,
)


def get_profile_index(
    cfg: MXMConfig,
    base_path: Path,
    *,
    as_of_bucket: str | None = None,
    force_refresh: bool = False,
    sitemap_url: str = SITEMAP_URL,
) -> list[ETFProfileIndexEntry]:
    """
    Return the ETF profile index using bucketed storage.

    Bucket resolution:
      - For reads: use `as_of_bucket` if provided; otherwise default to 'latest'.
      - For refresh: prefer provenance.as_of_bucket; else `as_of_bucket`;
      else today's date.
    """
    if not force_refresh:
        try:
            return load_profile_index(base_path, as_of_bucket=as_of_bucket)
        except (FileNotFoundError, ValueError):
            # Fall through to build-and-save path
            pass

    # Build fresh via HTTP + DataIO
    entries, resp = build_profile_index(cfg, sitemap_url=sitemap_url)

    # Decide target bucket for write
    bucket = (
        getattr(resp, "as_of_bucket", None) or as_of_bucket or date.today().isoformat()
    )

    # Persist and update 'latest'
    save_profile_index(
        entries,
        base_path,
        provenance=resp,
        as_of_bucket=bucket,
        write_latest=True,
    )

    return entries
