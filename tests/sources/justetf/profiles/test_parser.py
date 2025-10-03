"""
Regression test for the full profile parser.
"""

from __future__ import annotations

import json
from pathlib import Path

from mxm_datakraken.sources.justetf.profiles.parser import parse_profile

DATA_DIR = Path(__file__).parent.parent / "data"
HTML_PATH = DATA_DIR / "sample_etf.html"
GOLDEN_DIR = DATA_DIR / "golden"


def test_parse_profile_matches_golden() -> None:
    """Full parser output should exactly match the golden profile snapshot."""
    html = HTML_PATH.read_text(encoding="utf-8")
    expected = json.loads(
        (GOLDEN_DIR / "full_profile.json").read_text(encoding="utf-8")
    )

    actual = parse_profile(html, "IE00B4L5Y983", source_url="dummy-url")

    # Ignore volatile fields
    actual.pop("last_fetched", None)
    expected.pop("last_fetched", None)

    assert actual == expected
