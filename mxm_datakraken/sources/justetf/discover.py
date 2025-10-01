"""
mxm-datakraken.sources.justetf.discover

Discovery of ETF profile pages from the justETF sitemap.

We use the public sitemap (https://www.justetf.com/sitemap5.xml) to collect all ETF
profile URLs. Each ISIN appears multiple times under different language subdomains.
We deduplicate by ISIN, preferring the `/en/` profile URL as canonical.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import TypedDict
from urllib.parse import parse_qs, urlparse

import requests

SITEMAP_URL: str = "https://www.justetf.com/sitemap5.xml"
NS: dict[str, str] = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class ETFProfile(TypedDict):
    isin: str
    url: str
    lastmod: str | None


def discover_etf_profiles(sitemap_url: str = SITEMAP_URL) -> list[ETFProfile]:
    """
    Discover ETF profiles from the justETF sitemap.

    Args:
        sitemap_url: URL of the justETF sitemap containing ETF profile pages.

    Returns:
        A list of ETFProfile dicts with keys:
            - isin: ISIN string
            - url: canonical URL (English `/en/` profile preferred)
            - lastmod: last modification date if present, else None
    """
    resp = requests.get(sitemap_url, headers={"User-Agent": "mxm-datakraken/0.1"})
    resp.raise_for_status()
    root: ET.Element = ET.fromstring(resp.content)

    profiles: dict[str, ETFProfile] = {}

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

        profile: ETFProfile = {"isin": isin, "url": loc, "lastmod": lastmod}

        # Deduplicate: prefer English version
        if isin not in profiles:
            profiles[isin] = profile
        else:
            existing = profiles[isin]
            if "/en/" in loc and "/en/" not in existing["url"]:
                profiles[isin] = profile

    return list(profiles.values())


if __name__ == "__main__":
    profiles: list[ETFProfile] = discover_etf_profiles()
    print(f"Discovered {len(profiles)} ETF profiles.")
    for p in profiles[:10]:
        print(p)
