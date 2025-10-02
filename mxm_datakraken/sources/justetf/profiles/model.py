"""
Model definition for JustETF profiles as parsed from HTML pages.

This schema mirrors the JustETF site structure as faithfully as possible.
Normalization and mapping to MXM domain models happens in mxm-refdata.
"""

from __future__ import annotations

from typing import TypedDict


class JustETFProfile(TypedDict, total=False):
    # Core identifiers
    isin: str  # Primary identifier, always present
    name: str  # Fund name from <h1>
    description: str  # Fund description / strategy text

    # Structured sections
    data: dict[str, str]  # Key-value pairs from "Data" section table
    identifiers: dict[str, str]  # Extra codes like WKN, ticker, etc. (optional)
    listings: list[dict[str, str]]  # Stock exchange listings (optional)

    # Provenance
    source_url: str  # Canonical URL we scraped
    last_fetched: str  # ISO8601 timestamp of when scraping happened
