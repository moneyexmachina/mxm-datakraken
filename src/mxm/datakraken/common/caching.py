from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from mxm.dataio.api import CacheMode


@dataclass(frozen=True)
class CachePolicy:
    """Generic caching policy resolved from config."""

    cache_mode: CacheMode
    ttl_seconds: float | None
    as_of_bucket: str


def resolve_cache_mode(value: str | None) -> CacheMode:
    """Convert config string to CacheMode (case-insensitive)."""
    key = (value or "default").strip().upper()
    return CacheMode[key]


def resolve_as_of_bucket(fmt_or_value: str | None) -> str:
    """
    Resolve a date-format-based or literal as_of_bucket string.
    - "%Y-%m-%d" → today's ISO date (e.g. "2025-10-28")
    - "2025Q4"   → returned unchanged
    - None       → today's ISO date
    """
    if not fmt_or_value:
        return date.today().isoformat()
    return date.today().strftime(fmt_or_value) if "%" in fmt_or_value else fmt_or_value
