"""
Unit tests for individual extractor helpers in parser.py,
using golden fixtures for stable expected values.
"""

from __future__ import annotations

import json
from pathlib import Path

from bs4 import BeautifulSoup

from mxm_datakraken.sources.justetf.profiles.parser import (
    extract_data_table,
    extract_description,
    extract_name,
)
from tests.sources.justetf.profiles.update_goldens import extract_listings

DATA_DIR = Path(__file__).parent.parent / "data"
HTML_PATH = DATA_DIR / "sample_etf.html"
GOLDEN_DIR = DATA_DIR / "golden"


def load_soup() -> BeautifulSoup:
    """Load the sample ETF HTML into a BeautifulSoup object."""
    html = HTML_PATH.read_text(encoding="utf-8")
    return BeautifulSoup(html, "html.parser")


def test_extract_name_against_golden() -> None:
    soup = load_soup()
    got = extract_name(soup)

    expected = json.loads((GOLDEN_DIR / "name.json").read_text(encoding="utf-8"))
    assert got == expected


def test_extract_description_against_golden() -> None:
    soup = load_soup()
    got = extract_description(soup)

    expected = (GOLDEN_DIR / "description.txt").read_text(encoding="utf-8")
    assert got == expected


def test_extract_data_table_against_golden() -> None:
    soup = load_soup()
    got = extract_data_table(soup)

    expected = json.loads((GOLDEN_DIR / "data_table.json").read_text(encoding="utf-8"))
    assert got == expected


def test_extract_listings_table_against_golden() -> None:
    soup = load_soup()
    got = extract_listings(soup)

    expected = json.loads(
        (GOLDEN_DIR / "listings_table.json").read_text(encoding="utf-8")
    )
    assert got == expected
