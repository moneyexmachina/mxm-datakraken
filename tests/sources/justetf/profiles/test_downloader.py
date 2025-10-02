"""
Tests for downloader of ETF profile HTML.
"""

from __future__ import annotations

import pytest

from mxm_datakraken.sources.justetf.profiles.downloader import download_etf_profile_html


class DummyResponse:
    """A dummy HTTP response object for testing."""

    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code != 200:
            raise Exception("HTTP error")


def test_download_etf_profile_html_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure HTML is returned when request succeeds."""

    dummy_html = "<html><h1>ETF Test</h1></html>"

    def fake_get(url: str, headers: dict[str, str], timeout: int) -> DummyResponse:
        return DummyResponse(dummy_html)

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profiles.downloader.requests.get", fake_get
    )

    html = download_etf_profile_html("TEST123", "http://dummy.url")
    assert "<h1>ETF Test</h1>" in html


def test_download_etf_profile_html_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure exceptions are propagated when request fails."""

    def fake_get(url: str, headers: dict[str, str], timeout: int) -> DummyResponse:
        return DummyResponse("<html></html>", status_code=500)

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profiles.downloader.requests.get", fake_get
    )

    with pytest.raises(Exception):
        download_etf_profile_html("TEST123", "http://dummy.url")
