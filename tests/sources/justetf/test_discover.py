from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from mxm_datakraken.sources.justetf.discover import ETFProfile, discover_etf_profiles


class DummyResp:
    """Minimal dummy response object for monkeypatching requests.get."""

    def __init__(self, text: str) -> None:
        self.content: bytes = text.encode("utf-8")

    def raise_for_status(self) -> None:
        return None


def test_discover_from_sample(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure discovery works correctly on a sample sitemap fixture."""

    # Load local XML fixture
    sample_path: Path = Path(__file__).parent / "data" / "sample_sitemap.xml"
    xml_content: str = sample_path.read_text(encoding="utf-8")

    def fake_get(*args: Any, **kwargs: Any) -> DummyResp:  # type: ignore[no-untyped-def]
        return DummyResp(xml_content)

    # Monkeypatch requests.get to return our dummy response
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.discover.requests.get", fake_get
    )

    profiles: list[ETFProfile] = discover_etf_profiles("dummy-url")

    # Expect exactly 3 ISINs from the sample fixture
    assert len(profiles) == 3
    isins: set[str] = {p["isin"] for p in profiles}
    assert isins == {"BG9000011163", "BGCROEX03189", "BGCZPX003174"}

    # Ensure all canonical URLs are English (/en/)
    for profile in profiles:
        assert "/en/" in profile["url"]
