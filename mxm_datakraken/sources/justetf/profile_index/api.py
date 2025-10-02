"""
mxm_datakraken.sources.justetf.profile_index.api

Public entry point for the ETF Profile Index.
Provides a stable interface for the rest of MXM to access discovered ETF profile pages.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from mxm_datakraken.sources.justetf.profile_index.discover import (
    ETFProfileIndexEntry,
    build_profile_index,
)
from mxm_datakraken.sources.justetf.profile_index.persistence import (
    load_profile_index,
    save_profile_index,
)


def get_profile_index(
    base_path: Path,
    as_of: date | None = None,
    force_refresh: bool = False,
) -> list[ETFProfileIndexEntry]:
    """
    Get the ETF Profile Index discovered from justETF.

    Args:
        base_path: Root folder for storing index snapshots.
        as_of: Optional date. If None, returns latest.
               If provided, returns most recent snapshot <= as_of.
        force_refresh: If True, force a new index build regardless of cache.

    Returns:
        List of ETFProfileIndexEntry dicts.
    """
    if force_refresh:
        index: list[ETFProfileIndexEntry] = build_profile_index()
        save_profile_index(index, base_path, as_of=date.today())
        return index

    try:
        return load_profile_index(base_path, as_of=as_of)
    except (FileNotFoundError, ValueError):
        # No cached data → fallback to fresh discovery
        index = build_profile_index()
        save_profile_index(index, base_path, as_of=date.today())
        return index
