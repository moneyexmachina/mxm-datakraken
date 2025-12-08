"""
Tests for downloader of ETF profile HTML (DataIO-backed).
"""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import Iterator, Optional, Type, cast

import pytest
from mxm.types import JSONObj
from mxm.config import MXMConfig

from mxm.datakraken.sources.justetf.profiles.downloader import download_etf_profile_html

# --- DataIO test doubles ------------------------------------------------------


class _DummyIoResponse:
    """Minimal stand-in for mxm_dataio.models.Response."""

    def __init__(self, path: Path) -> None:
        self.path: str = str(path)
        self._bytes: bytes = path.read_bytes()
        self.checksum: str = hashlib.sha256(self._bytes).hexdigest()

    def verify(self, data: bytes) -> bool:
        return hashlib.sha256(data).hexdigest() == self.checksum


class _DummyIo:
    """Context manager that mimics DataIoSession enough for the tests."""

    def __init__(
        self,
        *,
        payload_path: Optional[Path] = None,
        raise_on_fetch: Optional[Exception] = None,
    ) -> None:
        self._payload_path: Optional[Path] = payload_path
        self._raise: Optional[Exception] = raise_on_fetch
        self.requests: list[SimpleNamespace] = []

    def __enter__(self) -> "_DummyIo":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        _ = (exc_type, exc_val, exc_tb)
        return None

    def request(self, kind: str, params: JSONObj) -> SimpleNamespace:
        ns = SimpleNamespace(kind=kind, params=params)
        self.requests.append(ns)
        return ns

    def fetch(self, _req: object) -> _DummyIoResponse:
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

    # Patch the session helper used by the downloader module
    @contextmanager
    def _patched_open_justetf_session(_cfg: MXMConfig) -> Iterator[_DummyIo]:
        _ = _cfg
        yield dummy_io

    monkeypatch.setattr(
        "mxm.datakraken.sources.justetf.profiles.downloader.open_justetf_session",
        _patched_open_justetf_session,
        raising=True,
    )

    cfg: MXMConfig = cast(MXMConfig, {})
    html, _ = download_etf_profile_html(
        cfg, "TEST123", "https://example.test/etf-profile.html?isin=TEST123"
    )
    assert "<h1>ETF Test</h1>" in html

    # Optional: sanity-check the request shape
    assert len(dummy_io.requests) == 1
    req = dummy_io.requests[0]
    assert req.kind == "profile_html"
    assert req.params["method"] == "GET"
    assert req.params["headers"]["Accept"] == "text/html"


def test_download_etf_profile_html_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure exceptions are propagated when DataIO/adapter raises."""
    dummy_io = _DummyIo(raise_on_fetch=RuntimeError("network down"))

    @contextmanager
    def _patched_open_justetf_session(_cfg: MXMConfig) -> Iterator[_DummyIo]:
        _ = _cfg
        yield dummy_io

    monkeypatch.setattr(
        "mxm.datakraken.sources.justetf.profiles.downloader.open_justetf_session",
        _patched_open_justetf_session,
        raising=True,
    )

    cfg: MXMConfig = cast(MXMConfig, {})
    with pytest.raises(RuntimeError):
        _ = download_etf_profile_html(
            cfg, "TEST123", "https://example.test/etf-profile.html?isin=TEST123"
        )
