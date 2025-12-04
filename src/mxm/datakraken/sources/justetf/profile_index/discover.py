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
from pathlib import Path
from typing import cast
from urllib.parse import parse_qs, urlparse

from mxm_config import MXMConfig
from mxm_dataio.models import Response as IoResponse
from mxm_dataio.types import RequestParams

from mxm.datakraken.sources.justetf.common.io import open_justetf_session
from mxm.datakraken.sources.justetf.common.models import ETFProfileIndexEntry

SITEMAP_URL: str = "https://www.justetf.com/sitemap5.xml"
NS: dict[str, str] = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def _response_bytes(resp: IoResponse) -> bytes:
    if not resp.path:
        raise ValueError("DataIO Response has no payload path")
    data = Path(resp.path).read_bytes()
    if resp.checksum and not resp.verify(data):
        raise ValueError("DataIO Response checksum mismatch")
    return data


def build_profile_index(
    cfg: MXMConfig,
    sitemap_url: str = SITEMAP_URL,
) -> tuple[list[ETFProfileIndexEntry], IoResponse]:
    """
    Fetch and parse the justETF sitemap via mxm-dataio.

    Returns both the parsed entries and the DataIO Response so callers can
    persist a snapshot and write a provenance sidecar.

    Parameters
    ----------
    cfg
        Resolved mxm-config mapping (used by DataIoSession and adapter).
    sitemap_url
        Absolute URL of the justETF sitemap to fetch.

    Returns
    -------
    tuple[list[ETFProfileIndexEntry], IoResponse]
        (entries, dataio_response)

    Raises
    ------
    ValueError
        If the DataIO response has no payload path or fails checksum verification.
    Exception
        Any exception propagated from DataIoSession or the registered adapter.
    """
    with open_justetf_session(cfg) as io:
        resp = io.fetch(
            io.request(
                kind="sitemap",
                params=cast(
                    RequestParams,
                    {
                        "url": sitemap_url,
                        "method": "GET",
                        "headers": {"Accept": "application/xml"},
                    },
                ),
            )
        )

    xml_bytes = _response_bytes(resp)
    entries = parse_profile_index_from_bytes(xml_bytes)
    return entries, resp


def parse_profile_index_from_bytes(xml_bytes: bytes) -> list[ETFProfileIndexEntry]:
    """
    Parse a justETF sitemap from raw bytes. Preferred when you have payloads
    from mxm-dataio or you want ElementTree to honor the XML prolog encoding.
    """
    try:
        root: ET.Element = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []
    return _parse_index_from_root(root)


def _parse_index_from_root(root: ET.Element) -> list[ETFProfileIndexEntry]:
    profiles: dict[str, ETFProfileIndexEntry] = {}

    for url_el in root.findall("sm:url", NS):
        loc_el = url_el.find("sm:loc", NS)
        if loc_el is None or loc_el.text is None:
            continue
        loc: str = loc_el.text.strip()
        if not loc:
            continue

        lastmod_el = url_el.find("sm:lastmod", NS)
        lastmod: str | None = (
            lastmod_el.text.strip()
            if (lastmod_el is not None and lastmod_el.text is not None)
            else None
        )

        parsed = urlparse(loc)
        qs = parse_qs(parsed.query)
        isin: str | None = (qs.get("isin") or [None])[0]
        if not isin:
            continue

        entry: ETFProfileIndexEntry = {"isin": isin, "url": loc}
        if lastmod is not None:
            entry["lastmod"] = lastmod

        existing = profiles.get(isin)
        if existing is None or ("/en/" in loc and "/en/" not in existing["url"]):
            profiles[isin] = entry

    return list(profiles.values())
