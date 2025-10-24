"""
mxm_datakraken.sources.justetf.profile_index.api

Public entry point for the ETF Profile Index.
Provides a stable interface for the rest of MXM to access discovered ETF profile pages.
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
    as_of: date | None = None,
    force_refresh: bool = False,
    sitemap_url: str = SITEMAP_URL,
) -> list[ETFProfileIndexEntry]:
    if force_refresh:
        entries, resp = build_profile_index(cfg, sitemap_url=sitemap_url)
        today = as_of or date.today()
        save_profile_index(entries, base_path, as_of=today, provenance=resp)
        return entries

    try:
        return load_profile_index(base_path, as_of=as_of)
    except (FileNotFoundError, ValueError):
        entries, resp = build_profile_index(cfg, sitemap_url=sitemap_url)
        today = as_of or date.today()
        save_profile_index(entries, base_path, as_of=today, provenance=resp)
        return entries
