from __future__ import annotations

from pathlib import Path

from mxm_datakraken.sources.justetf.profiles.parser import parse_profile


def test_parse_profile_success() -> None:
    """Should parse ISIN, name, description, and the Data table."""
    html_path = Path(__file__).parent.parent / "data" / "sample_etf.html"
    html = html_path.read_text(encoding="utf-8")

    profile = parse_profile(html, "IE00B4L5Y983")

    # Top-level
    assert profile["isin"] == "IE00B4L5Y983"
    assert isinstance(profile["name"], str)
    assert "iShares" in profile["name"]

    # Data table essentials
    data = profile["data"]
    assert isinstance(data, dict)
    assert data.get("Index") == "MSCI World"
    assert "0.20%" in (data.get("Total expense ratio") or "")
    assert data.get("Fund Provider") == "iShares"

    # Description contains the index name somewhere
    desc = profile["description"]
    assert isinstance(desc, str) and "MSCI World" in desc and len(desc) > 50
