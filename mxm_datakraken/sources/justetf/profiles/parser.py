"""
mxm_datakraken.sources.justetf.profiles.parser

Robust parsing of a single justETF profile HTML page.

We extract:
- name (from: h1#etf-title)
- description (from: #etf-description-content; fallback to #etf-description)
- data table under the "Basics" / "Data" section (h3 == "Data" → table.etf-data-table)

Notes on HTML structure (from the sample fixture):
- Labels are in <td class="vallabel">; the value cell is the last <td> in the row and
  can contain nested spans/divs. We flatten its text with single spaces.
- Some fields we care about include "Index", "Total expense ratio", "Fund Provider".
"""

from __future__ import annotations

from typing import Dict, TypedDict

from bs4 import BeautifulSoup  # type: ignore[reportMissingTypeStubs]


class ETFProfileParsed(TypedDict, total=False):
    isin: str
    name: str
    description: str
    data: Dict[str, str]
    # Future-friendly: identifiers like WKN / Ticker could be added later:
    # identifiers: Dict[str, str]


def _norm_text(s: str) -> str:
    """Normalize whitespace: collapse inner whitespace and strip ends."""
    return " ".join(s.split()).strip()


def parse_profile(html: str, isin: str) -> ETFProfileParsed:
    """
    Parse a justETF profile HTML into a structured dict.

    Args:
        html: Raw HTML of the profile page.
        isin: ISIN we expect for this profile (used as authoritative ID).

    Returns:
        A dict with keys: 'isin', 'name', 'description', 'data'.

    Raises:
        ValueError: if we cannot find a valid title/name.
    """
    # BeautifulSoup's types are dynamically typed; suppress strict type noise locally.
    soup = BeautifulSoup(html, "html.parser")  # type: ignore[no-untyped-call]

    # --- Name (required) ---
    name_el = soup.select_one("#etf-title")  # type: ignore[no-untyped-call]
    if name_el is None or name_el.get_text(strip=True) == "":
        raise ValueError("Could not locate ETF title (#etf-title)")
    name = name_el.get_text(strip=True)

    # --- Description (best source first, with fallback) ---
    desc_el = soup.select_one("#etf-description-content")  # type: ignore[no-untyped-call]
    if desc_el is None:
        desc_el = soup.select_one("#etf-description")  # type: ignore[no-untyped-call]
    description = ""
    if desc_el is not None:
        # Keep sentences together but normalize whitespace.
        description = _norm_text(desc_el.get_text(separator=" "))  # type: ignore[no-untyped-call]

    # --- Data table under Basics → "Data" ---
    data: Dict[str, str] = {}

    # Find the specific "Data" heading first (h3 whose text equals "Data")
    data_h = soup.find(  # type: ignore[no-untyped-call]
        ["h2", "h3"],
        string=lambda t: isinstance(t, str) and t.strip().lower() == "data",
    )
    table = None
    if data_h is not None:
        table = data_h.find_next("table", class_="etf-data-table")  # type: ignore[no-untyped-call]

    # Fallback: first table with the known class if heading heuristic fails
    if table is None:
        table = soup.find("table", class_="etf-data-table")  # type: ignore[no-untyped-call]

    if table is not None:
        for row in table.find_all("tr"):  # type: ignore[no-untyped-call]
            cells = row.find_all("td")  # type: ignore[no-untyped-call]
            if len(cells) < 2:
                continue
            # Label: prefer .vallabel cell if present
            label_el = row.find("td", class_="vallabel")  # type: ignore[no-untyped-call]
            label = (
                label_el.get_text(strip=True)
                if label_el is not None
                else cells[0].get_text(strip=True)
            )
            if not label:
                continue
            # Value: last cell, flatten text with single spaces
            val_raw = cells[-1].get_text(separator=" ", strip=True)
            val = _norm_text(val_raw)
            data[label] = val

    parsed: ETFProfileParsed = {
        "isin": isin,
        "name": name,
        "description": description,
        "data": data,
    }
    return parsed
