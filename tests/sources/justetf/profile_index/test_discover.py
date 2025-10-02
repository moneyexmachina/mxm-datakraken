"""
Tests for building the ETF Profile Index from the justETF sitemap.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from mxm_datakraken.sources.justetf.profile_index.discover import (
    ETFProfileIndexEntry,
    build_profile_index,
)


class DummyResp:
    """Minimal dummy response object for monkeypatching requests.get."""

    def __init__(self, text: str) -> None:
        self.content: bytes = text.encode("utf-8")

    def raise_for_status(self) -> None:
        return None


def test_build_profile_index_from_sample(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure build_profile_index works correctly on a sample sitemap fixture."""

    # Load local XML fixture
    sample_path: Path = Path(__file__).parent.parent / "data" / "sample_sitemap.xml"
    xml_content: str = sample_path.read_text(encoding="utf-8")

    def fake_get(*args: Any, **kwargs: Any) -> DummyResp:  # type: ignore[no-untyped-def]
        return DummyResp(xml_content)

    # Monkeypatch requests.get to return our dummy response
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profile_index.discover.requests.get",
        fake_get,
    )

    index: list[ETFProfileIndexEntry] = build_profile_index("dummy-url")

    # Expect exactly 3 ISINs from the sample fixture
    assert len(index) == 3
    isins: set[str] = {entry["isin"] for entry in index}
    assert isins == {"BG9000011163", "BGCROEX03189", "BGCZPX003174"}

    # Ensure all canonical URLs are English (/en/)
    for entry in index:
        assert "/en/" in entry["url"]
