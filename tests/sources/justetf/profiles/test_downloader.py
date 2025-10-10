"""
Tests for downloader of ETF profile HTML (DataIO-backed).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import Any, Optional, Type

import pytest

from mxm_datakraken.sources.justetf.profiles.downloader import download_etf_profile_html

# --- DataIO test doubles ------------------------------------------------------


class _DummyIoResponse:
    """Minimal stand-in for mxm_dataio.models.Response."""

    def __init__(self, path: Path) -> None:
        self.path = str(path)
        self._bytes = path.read_bytes()
        self.checksum = hashlib.sha256(self._bytes).hexdigest()

    def verify(self, data: bytes) -> bool:
        return hashlib.sha256(data).hexdigest() == self.checksum


class _DummyIo:
    """Context manager that mimics DataIoSession enough for the tests."""

    def __init__(
        self,
        *,
        payload_path: Path | None = None,
        raise_on_fetch: Exception | None = None,
    ) -> None:
        self._payload_path = payload_path
        self._raise = raise_on_fetch
        self.requests: list[Any] = []

    def __enter__(self) -> "_DummyIo":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Finalize the Session by setting ended_at."""

        _ = (exc_type, exc_val, exc_tb)

        return None

    def request(self, kind: str, params: dict[str, Any]) -> SimpleNamespace:
        ns = SimpleNamespace(kind=kind, params=params)
        self.requests.append(ns)
        return ns

    def fetch(self, _req: Any) -> _DummyIoResponse:
        _ = _req
        if self._raise is not None:
            raise self._raise
        assert self._payload_path is not None, (
            "payload_path must be set for success path"
        )
        return _DummyIoResponse(self._payload_path)


# --- Tests -------------------------------------------------------------------


def test_download_etf_profile_html_success(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Ensure HTML is returned when DataIO fetch succeeds."""
    # Create a fake HTML payload on disk to mimic DataIO's persisted response
    html_bytes = b"<html><h1>ETF Test</h1></html>"
    payload_path = tmp_path / "profile.html"
    payload_path.write_bytes(html_bytes)

    dummy_io = _DummyIo(payload_path=payload_path)

    # Patch the DataIoSession and alias resolver inside the downloader module
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profiles.downloader.DataIoSession",
        lambda source, cfg, use_cache=True: dummy_io,  # noqa: ARG005
    )

    # Patch the config helper to return (alias, dio_cfg)
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profiles.downloader.dataio_for_justetf",
        lambda cfg: ("http", {}),
    )

    cfg: dict[str, Any] = {}
    html, _ = download_etf_profile_html(
        cfg, "TEST123", "https://example.test/etf-profile.html?isin=TEST123"
    )
    assert "<h1>ETF Test</h1>" in html


def test_download_etf_profile_html_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure exceptions are propagated when DataIO/adapter raises."""
    # Simulate adapter/network failure by raising from fetch()
    dummy_io = _DummyIo(raise_on_fetch=RuntimeError("network down"))

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profiles.downloader.DataIoSession",
        lambda source, cfg, use_cache=True: dummy_io,  # noqa: ARG005
    )
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profiles.downloader.dataio_for_justetf",
        lambda cfg: ("http", {}),  # noqa: ARG005
    )

    cfg: dict[str, Any] = {}
    with pytest.raises(RuntimeError):
        _ = download_etf_profile_html(
            cfg, "TEST123", "https://example.test/etf-profile.html?isin=TEST123"
        )
