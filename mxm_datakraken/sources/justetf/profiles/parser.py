"""
Parser for justETF profile pages.
"""

from __future__ import annotations

from typing import Optional, TypedDict

from bs4 import BeautifulSoup, Tag


class ETFProfileParsed(TypedDict):
    isin: str
    name: Optional[str]
    data: dict[str, str]
    description: Optional[str]


def _get_text_safe(tag: Optional[Tag]) -> Optional[str]:
    """Extract text from a BeautifulSoup Tag, or None if tag is None/empty."""
    if tag is None:
        return None
    text: str = tag.get_text(strip=True)
    return text if text else None


def parse_profile(html: str, isin: str) -> ETFProfileParsed:
    """
    Parse a justETF ETF profile page into structured data.

    Args:
        html: Raw HTML content of the profile page.
        isin: ISIN of the ETF (used as identity key).

    Returns:
        Parsed ETF profile with keys:
            - isin
            - name
            - data (dict of 'Data' section key/values)
            - description
    """
    soup: BeautifulSoup = BeautifulSoup(html, "html.parser")

    # --- Name ---
    title_el = soup.find("h1", id="etf-title") or soup.find("h1")
    name: Optional[str] = _get_text_safe(title_el)

    # --- Data section ---
    data: dict[str, str] = {}
    heading_tag: Optional[Tag] = soup.find(  # type: ignore
        ["h2", "h3"],
        string=lambda t: isinstance(t, str) and "Data" in t,  # type: ignore
    )
    if heading_tag is not None:
        table_tag: Optional[Tag] = heading_tag.find_next("table")  # type: ignore
        if table_tag is not None:
            for row in table_tag.find_all("tr"):  #  type: ignore
                cells: list[Tag] = row.find_all("td")  # type: ignore
                if len(cells) == 2:  # type: ignore
                    label: str = cells[0].get_text(strip=True)  # type: ignore
                    value: str = cells[1].get_text(strip=True)  # type: ignore
                    data[label] = value

    # --- Description section ---
    description: Optional[str] = None
    desc_heading: Optional[Tag] = soup.find(  # type: ignore
        ["h2", "h3"],
        string=lambda t: isinstance(t, str) and "Description" in t,  # type: ignore
    )
    if desc_heading is not None:
        desc_div: Optional[Tag] = desc_heading.find_next("div")  # type: ignore
        description = _get_text_safe(desc_div)  # type: ignore

    return {
        "isin": isin,
        "name": name,
        "data": data,
        "description": description,
    }
