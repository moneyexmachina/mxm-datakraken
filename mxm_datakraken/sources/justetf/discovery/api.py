"""
mxm-datakraken.sources.justetf.api

Public entry point for ETF profile discovery.
Provides a stable interface for the rest of MXM.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from mxm_datakraken.sources.justetf.discovery.discover import (
    ETFProfile,
    discover_etf_profiles,
)
from mxm_datakraken.sources.justetf.discovery.persistence import (
    load_discovery_results,
    save_discovery_results,
)


def get_profiles(
    base_path: Path,
    as_of: date | None = None,
    force_refresh: bool = False,
) -> list[ETFProfile]:
    """
    Get ETF profiles discovered from justETF.

    Args:
        base_path: Root folder for storing discovery snapshots.
        as_of: Optional date. If None, returns latest.
               If provided, returns most recent snapshot <= as_of.
        force_refresh: If True, force a new discovery run regardless of cache.

    Returns:
        List of ETFProfile dicts.
    """
    if force_refresh:
        profiles: list[ETFProfile] = discover_etf_profiles()
        save_discovery_results(profiles, base_path, as_of=date.today())
        return profiles

    try:
        return load_discovery_results(base_path, as_of=as_of)
    except (FileNotFoundError, ValueError):
        # No cached data → fallback to fresh discovery
        profiles = discover_etf_profiles()
        save_discovery_results(profiles, base_path, as_of=date.today())
        return profiles
