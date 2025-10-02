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

from bs4 import BeautifulSoup, Tag

from mxm_datakraken.sources.justetf.profiles.model import JustETFProfile


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
    soup = BeautifulSoup(html, "html.parser")

    name = extract_name(soup)
    description = extract_description(soup)
    data = extract_data_table(soup)
    listings = extract_listings(soup)

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
    desc_container = soup.find("div", id="etf-description-content")
    if not desc_container:
        return ""

    # Collect text fragments while skipping separators
    parts: list[str] = []
    for child in desc_container.find_all("div", recursive=False):
        text = child.get_text(" ", strip=True)
        if text:
            parts.append(text)

    raw = " ".join(parts)

    # Normalize whitespace
    cleaned = re.sub(r"\s+", " ", raw)

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

    table = soup.find("table", class_="etf-data-table")
    if not table:
        return data

    for row in table.find_all("tr"):
        label_cell = row.find("td", class_="vallabel")
        value_cell = None
        if label_cell:
            # find the sibling cell (second <td>)
            siblings = row.find_all("td")
            if len(siblings) > 1:
                value_cell = siblings[1]

        if not label_cell or not value_cell:
            continue

        key = label_cell.get_text(" ", strip=True)

        # values may be nested in <div class="val">, <span class="val2">, etc.
        vals = []
        for cls in ["val", "val2"]:
            for el in value_cell.find_all(class_=cls):
                vals.append(el.get_text(" ", strip=True))

        # fallback: if no val/val2, take raw text
        if not vals:
            vals.append(value_cell.get_text(" ", strip=True))

        value = " ".join(v for v in vals if v)
        data[key] = value

    return data


def extract_listings(soup: BeautifulSoup) -> list[dict[str, str]]:
    listings: list[dict[str, str]] = []
    table = soup.find("table", class_="mobile-table")
    if not table:
        return listings

    headers = [
        th.get_text(" ", strip=True) for th in table.find("thead").find_all("th")
    ]

    for row in table.find("tbody").find_all("tr"):
        cells = row.find_all("td")
        if len(cells) != len(headers):
            continue

        row_dict: dict[str, str] = {}
        for i, cell in enumerate(cells):
            parts = [
                t.strip()
                for t in cell.stripped_strings
                if t.strip() and t.strip() != "-"
            ]
            text = " ".join(parts)
            row_dict[headers[i]] = text
        listings.append(row_dict)

    return listings
