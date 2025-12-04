"""
HTML parser for JustETF profile pages.

This module extracts data from a single ETF profile page, returning a
JustETFProfile structure faithful to the JustETF site’s layout.

Helpers (`extract_name`, `extract_description`, `extract_data_table`,
`extract_listings`) are factored out for granular testing.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import cast

from bs4 import BeautifulSoup, Tag

from mxm.datakraken.sources.justetf.common.models import JustETFProfile


def parse_profile(
    html: str, isin: str, source_url: str | None = None
) -> JustETFProfile:
    """
    Parse a JustETF profile HTML page into a structured dict.

    Args:
        html: Raw HTML of the ETF profile page.
        isin: ISIN of the ETF (from sitemap / index).
        source_url: Optional canonical profile URL (for provenance).

    Returns:
        A JustETFProfile dictionary with parsed fields.
    """
    soup: BeautifulSoup = BeautifulSoup(html, "html.parser")

    name: str = extract_name(soup)
    description: str = extract_description(soup)
    data: dict[str, str] = extract_data_table(soup)
    listings: list[dict[str, str]] = extract_listings(soup)

    profile: JustETFProfile = {
        "isin": isin,
        "name": name,
        "description": description,
        "data": data,
        "listings": listings,
        "source_url": source_url or "",
        "last_fetched": datetime.now(timezone.utc).isoformat(),
    }
    return profile


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def extract_name(soup: BeautifulSoup) -> str:
    """Extract the ETF name from the <h1> header."""
    name_el: Tag | None = soup.find("h1")
    return name_el.get_text(strip=True) if name_el else ""


def extract_description(soup: BeautifulSoup) -> str:
    """
    Extracts the fund description text from the profile page.

    Args:
        soup: BeautifulSoup-parsed HTML.

    Returns:
        A cleaned string containing the description.
    """
    desc_container: Tag | None = soup.find("div", id="etf-description-content")
    if not desc_container:
        return ""

    # Collect text fragments while skipping separators
    parts: list[str] = []
    # Narrow the type of find_all result to a list[Tag]
    for child in cast(list[Tag], desc_container.find_all("div", recursive=False)):
        text: str = child.get_text(" ", strip=True)
        if text:
            parts.append(text)

    raw: str = " ".join(parts)

    # Normalize whitespace
    cleaned: str = re.sub(r"\s+", " ", raw)

    # Remove spaces before punctuation like ". , ; :"
    cleaned = re.sub(r"\s+([.,;:])", r"\1", cleaned)

    return cleaned.strip()


def extract_data_table(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract key-value pairs from the ETF data table.
    Keys are in <td class="vallabel">, values in <td>/<div>/<span> with
    class val/val2 or plain text.
    """
    data: dict[str, str] = {}

    table: Tag | None = soup.find("table", class_="etf-data-table")
    if not table:
        return data

    for row in cast(list[Tag], table.find_all("tr")):
        label_cell: Tag | None = row.find("td", class_="vallabel")
        value_cell: Tag | None = None

        if label_cell:
            siblings: list[Tag] = cast(list[Tag], row.find_all("td"))
            if len(siblings) > 1:
                value_cell = siblings[1]

        if not label_cell or not value_cell:
            continue

        key: str = label_cell.get_text(" ", strip=True)

        # values may be nested in <div class="val">, <span class="val2">, etc.
        vals: list[str] = []
        for cls in ["val", "val2"]:
            for el in cast(list[Tag], value_cell.find_all(class_=cls)):
                vals.append(el.get_text(" ", strip=True))

        # fallback: if no val/val2, take raw text
        if not vals:
            vals.append(value_cell.get_text(" ", strip=True))

        # Join a typed list (avoid generator-of-Unknown complaints)
        non_empty: list[str] = [v for v in vals if v]
        value: str = " ".join(non_empty)
        data[key] = value

    return data


def extract_listings(soup: BeautifulSoup) -> list[dict[str, str]]:
    """
    Extract ETF listings from the 'Stock exchange' section.
    Robust against other mobile-table uses (e.g. dividends).
    """
    listings: list[dict[str, str]] = []

    # 1️⃣Find the section anchored by id="stock-exchange"
    stock_anchor: Tag | None = soup.select_one("div#stock-exchange")
    if not stock_anchor:
        return listings

    # 2️⃣Find the nearest following table after the anchor
    table: Tag | None = stock_anchor.find_next("table", class_="mobile-table")
    if not table:
        return listings

    # 3️⃣Extract headers
    th_tags: list[Tag] = cast(list[Tag], table.select("thead th"))
    headers: list[str] = [th.get_text(strip=True) for th in th_tags]
    # Normalize: collapse duplicate spaces and unify capitalization
    headers = [h.replace("\xa0", " ").strip() for h in headers]

    # 4️⃣Extract rows
    for tr in cast(list[Tag], table.select("tbody tr")):
        td_tags: list[Tag] = cast(list[Tag], tr.select("td"))
        cells: list[str] = [td.get_text(strip=True) for td in td_tags]
        if len(cells) != len(headers):
            # Sometimes rowspan/colspan causes mismatch — skip partial rows
            continue
        row: dict[str, str] = dict(zip(headers, cells, strict=False))
        listings.append(row)

    return listings
