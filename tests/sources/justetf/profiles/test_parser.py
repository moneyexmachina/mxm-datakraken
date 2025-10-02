from pathlib import Path

from mxm_datakraken.sources.justetf.profiles.parser import parse_profile


def test_parse_profile_extracts_data_and_description() -> None:
    """Ensure parse_profile extracts ISIN, name, Data section, and Description."""

    # Load fixture HTML
    html_path: Path = Path(__file__).parent.parent / "data" / "sample_etf.html"
    html: str = html_path.read_text(encoding="utf-8")

    profile = parse_profile(html, "IE00B4L5Y983")

    # Top-level fields
    assert profile["isin"] == "IE00B4L5Y983"
    assert isinstance(profile["name"], str)
    assert "MSCI World" in profile["name"]

    # Data section
    data: dict[str, str | None] = profile["data"]  # type: ignore[assignment]
    assert isinstance(data, dict)
    assert data.get("Index") == "MSCI World"
    assert "0.20%" in (data.get("Total expense ratio") or "")
    assert data.get("Fund Provider") == "iShares"

    # Description section
    description: str | None = profile["description"]  # type: ignore[assignment]
    assert description is not None
    assert "MSCI World" in description
    assert len(description) > 50  # ensure we got a decent chunk of text
