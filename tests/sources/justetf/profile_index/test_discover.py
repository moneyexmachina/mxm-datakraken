"""
Tests for building the ETF Profile Index from the justETF sitemap (DataIO-backed).
"""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import Iterator, Optional, Type, cast

import pytest
from mxm.types import JSONObj
from mxm_config import MXMConfig

from mxm.datakraken.sources.justetf.profile_index.discover import (
    build_profile_index,
)


class _DummyIoResponse:
    """Minimal stand-in for mxm_dataio.models.Response."""

    def __init__(self, path: Path, checksum: str) -> None:
        self.path: str = str(path)
        self.checksum: str = checksum

    def verify(self, data: bytes) -> bool:
        return hashlib.sha256(data).hexdigest() == self.checksum


class _DummyIo:
    """Context manager that mimics DataIoSession enough for the test."""

    def __init__(self, payload_path: Path) -> None:
        self._payload_path: Path = payload_path
        self._checksum: str = hashlib.sha256(payload_path.read_bytes()).hexdigest()
        self.requests: list[SimpleNamespace] = []

    def __enter__(self) -> "_DummyIo":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        _ = exc_type, exc, tb
        return None

    def request(self, kind: str, params: JSONObj) -> SimpleNamespace:
        ns = SimpleNamespace(kind=kind, params=params)
        self.requests.append(ns)
        return ns

    def fetch(self, _req: object) -> _DummyIoResponse:
        _ = _req
        return _DummyIoResponse(self._payload_path, self._checksum)


def test_build_profile_index_from_sample_via_dataio(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Ensure build_profile_index works correctly on a sample sitemap fixture
    using DataIO."""
    # Prepare a fake DataIO payload file
    sample_path: Path = Path(__file__).parent.parent / "data" / "sample_sitemap.xml"
    xml_bytes: bytes = sample_path.read_bytes()
    payload_path = tmp_path / "sitemap.xml"
    payload_path.write_bytes(xml_bytes)

    dummy_io = _DummyIo(payload_path)

    # Patch the helper used by discover.py to open a session
    @contextmanager
    def _patched_open_justetf_session(_cfg: MXMConfig) -> Iterator[_DummyIo]:
        _ = _cfg
        yield dummy_io

    monkeypatch.setattr(
        "mxm.datakraken.sources.justetf.profile_index.discover.open_justetf_session",
        _patched_open_justetf_session,
        raising=True,
    )

    # Provide a cfg that satisfies the MXMConfig protocol to the type checker
    cfg: MXMConfig = cast(MXMConfig, {})  # runtime untouched due to patch
    index, _ = build_profile_index(cfg, sitemap_url="dummy-url")

    # Expect exactly 3 ISINs from the sample fixture
    assert len(index) == 3
    isins = {entry["isin"] for entry in index}
    assert isins == {"BG9000011163", "BGCROEX03189", "BGCZPX003174"}

    # Ensure all canonical URLs are English (/en/)
    for entry in index:
        assert "/en/" in entry["url"]

    # (Optional) sanity: the function issued exactly one sitemap request
    assert len(dummy_io.requests) == 1
    req = dummy_io.requests[0]
    assert req.kind == "sitemap"
    assert req.params["method"] == "GET"
    assert req.params["headers"]["Accept"] == "application/xml"
