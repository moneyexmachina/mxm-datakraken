"""
JustETF source models (minimal, source-shaped, and lossless).

This module defines *source-facing* schemas for objects parsed from JustETF HTML.
They mirror what the site displays and deliberately avoid normalization. Keep all
values exactly as shown on the page (e.g., "0.20 %", "€ 1,234.56", "accumulating").
Mapping to MXM domain models and any semantic normalization happens downstream in
`mxm-refdata`.

Design choices
--------------
- TypedDicts are used to keep persistence ergonomic (JSON-friendly) while remaining
  static-type-checker friendly.
- Only the invariants are required (`isin`, `source_url`); everything else is optional
  and may be absent if not present on the page or not yet parsed.
- Extend these models incrementally as parsers gain coverage; don’t over-specify now.
"""

from typing import NotRequired, Required, TypedDict


class JustETFProfile(TypedDict, total=False):
    """
    Parsed ETF profile in JustETF *source* shape.

    Keep rich text and key–value sections as-is; no normalization here.
    """

    # Invariants (always provided by the parser)
    isin: Required[str]  # Primary identifier
    source_url: Required[str]  # Canonical profile URL scraped

    # Headline / summary
    name: NotRequired[str]  # <h1> fund name
    description: NotRequired[str]  # Strategy/summary text (plain)

    # Loose, display-form sections (expand later as needed)
    data: NotRequired[dict[str, str]]  # "Key data" table (labels → values)
    identifiers: NotRequired[dict[str, str]]  # e.g., WKN, ticker, SEDOL, etc.
    listings: NotRequired[list[dict[str, str]]]  # Exchange rows as displayed

    # Provenance
    last_fetched: NotRequired[str]  # ISO8601 UTC timestamp, e.g. "2025-10-21T11:05:00Z"


class ETFProfileIndexEntry(TypedDict):
    """Single entry discovered from the JustETF profile index/sitemap."""

    isin: str
    url: str
    lastmod: NotRequired[str]


__all__ = [
    "JustETFProfile",
    "ETFProfileIndexEntry",
]
