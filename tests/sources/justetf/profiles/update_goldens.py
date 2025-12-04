"""
Helper script to (re)generate golden fixtures for parser tests.

Run this manually when you want to refresh the expected outputs.
"""

from __future__ import annotations

import json
from pathlib import Path

from bs4 import BeautifulSoup

from mxm.datakraken.sources.justetf.profiles.parser import (
    extract_data_table,
    extract_description,
    extract_listings,
    extract_name,
    parse_profile,
)

DATA_DIR = Path(__file__).parent.parent / "data"
HTML_PATH = DATA_DIR / "sample_etf.html"
GOLDEN_DIR = DATA_DIR / "golden"


def main() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")

    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)

    # Name
    name = extract_name(soup)
    (GOLDEN_DIR / "name.json").write_text(
        json.dumps(name, indent=2, ensure_ascii=False)
    )

    # Description
    desc = extract_description(soup)
    (GOLDEN_DIR / "description.txt").write_text(desc or "", encoding="utf-8")

    # Data table
    data = extract_data_table(soup)
    (GOLDEN_DIR / "data_table.json").write_text(
        json.dumps(data, indent=2, ensure_ascii=False)
    )

    # Listings table
    listings = extract_listings(soup)
    (GOLDEN_DIR / "listings_table.json").write_text(
        json.dumps(listings, indent=2, ensure_ascii=False)
    )
    # Full profile
    full_profile = parse_profile(html, "IE00B4L5Y983", source_url="dummy-url")
    (GOLDEN_DIR / "full_profile.json").write_text(
        json.dumps(full_profile, indent=2, ensure_ascii=False)
    )

    print(f"Golden fixtures updated in {GOLDEN_DIR}")


if __name__ == "__main__":
    main()
