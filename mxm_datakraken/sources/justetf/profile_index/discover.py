"""
mxm-datakraken.sources.justetf.profile_index.discover

Build the ETF Profile Index from the justETF sitemap.

We use the public sitemap (https://www.justetf.com/sitemap5.xml) to collect all ETF
profile URLs. Each ISIN appears multiple times under different language subdomains.
We deduplicate by ISIN, preferring the `/en/` profile URL as canonical.

The result is the ETF Profile Index: a list of entries with ISIN and canonical URL.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import NotRequired, TypedDict
from urllib.parse import parse_qs, urlparse

import requests

SITEMAP_URL: str = "https://www.justetf.com/sitemap5.xml"
NS: dict[str, str] = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class ETFProfileIndexEntry(TypedDict):
    """Single entry in the ETF Profile Index (from sitemap)."""

    isin: str
    url: str
    lastmod: NotRequired[str]


def build_profile_index(sitemap_url: str = SITEMAP_URL) -> list[ETFProfileIndexEntry]:
    """
    Build the ETF Profile Index from the justETF sitemap.

    Args:
        sitemap_url: URL of the justETF sitemap containing ETF profile pages.

    Returns:
        A list of ETFProfileIndexEntry dicts with keys:
            - isin: ISIN string
            - url: canonical URL (English `/en/` profile preferred)
            - lastmod: optional last modification date if present
    """
    resp = requests.get(sitemap_url, headers={"User-Agent": "mxm-datakraken/0.1"})
    resp.raise_for_status()
    root: ET.Element = ET.fromstring(resp.content)

    profiles: dict[str, ETFProfileIndexEntry] = {}

    for url_el in root.findall("sm:url", NS):
        loc_el = url_el.find("sm:loc", NS)
        if loc_el is None or loc_el.text is None:
            continue
        loc: str = loc_el.text.strip()

        lastmod_el = url_el.find("sm:lastmod", NS)
        lastmod: str | None = (
            lastmod_el.text.strip()
            if (lastmod_el is not None and lastmod_el.text is not None)
            else None
        )

        # Extract ISIN from query string
        parsed = urlparse(loc)
        qs = parse_qs(parsed.query)
        isin: str | None = qs.get("isin", [None])[0]
        if isin is None:
            continue

        entry: ETFProfileIndexEntry = {"isin": isin, "url": loc}
        if lastmod is not None:
            entry["lastmod"] = lastmod

        # Deduplicate: prefer English version
        if isin not in profiles:
            profiles[isin] = entry
        else:
            existing = profiles[isin]
            if "/en/" in loc and "/en/" not in existing["url"]:
                profiles[isin] = entry

    return list(profiles.values())


if __name__ == "__main__":
    index: list[ETFProfileIndexEntry] = build_profile_index()
    print(f"Built ETF Profile Index with {len(index)} entries.")
    for entry in index[:10]:
        print(entry)
