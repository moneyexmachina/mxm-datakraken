"""
Tests for building the ETF Profile Index from the justETF sitemap (DataIO-backed).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from mxm_datakraken.sources.justetf.profile_index.discover import (
    build_profile_index,
)


class _DummyIoResponse:
    """Minimal stand-in for mxm_dataio.models.Response."""

    def __init__(self, path: Path, checksum: str) -> None:
        self.path = str(path)
        self.checksum = checksum

    # Keep signature compatible with the real Response.verify(data: bytes) -> bool
    def verify(self, data: bytes) -> bool:  # noqa: D401 - simple stand-in
        return hashlib.sha256(data).hexdigest() == self.checksum


class _DummyIo:
    """Context manager that mimics DataIoSession enough for the test."""

    def __init__(self, payload_path: Path) -> None:
        self._payload_path = payload_path
        self._checksum = hashlib.sha256(payload_path.read_bytes()).hexdigest()
        self.requests: list[Any] = []

    def __enter__(self) -> "_DummyIo":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def request(self, kind: str, params: dict[str, Any]) -> SimpleNamespace:
        ns = SimpleNamespace(kind=kind, params=params)
        self.requests.append(ns)
        return ns

    def fetch(self, _req: Any) -> _DummyIoResponse:
        return _DummyIoResponse(self._payload_path, self._checksum)


def test_build_profile_index_from_sample_via_dataio(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Ensure build_profile_index works correctly on a sample sitemap fixture using DataIO."""
    # Load local XML fixture as BYTES and persist to a temp file to mimic DataIO payload-on-disk
    sample_path: Path = Path(__file__).parent.parent / "data" / "sample_sitemap.xml"
    xml_bytes: bytes = sample_path.read_bytes()
    payload_path = tmp_path / "sitemap.xml"
    payload_path.write_bytes(xml_bytes)

    dummy_io = _DummyIo(payload_path)

    # Patch the DataIoSession used inside discover.py to return our dummy context manager
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profile_index.discover.DataIoSession",
        lambda source, cfg, use_cache=True: dummy_io,  # noqa: ARG005
    )
    # The alias resolution doesn't matter for this unit; just return any string
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profile_index.discover.dataio_for_justetf",
        lambda cfg: ("http", {}),  # noqa: ARG005
    )

    # Call with a minimal cfg; the patched DataIoSession ignores it
    cfg: dict[str, Any] = {}
    index, _ = build_profile_index(cfg, sitemap_url="dummy-url")

    # Expect exactly 3 ISINs from the sample fixture
    assert len(index) == 3
    isins = {entry["isin"] for entry in index}
    assert isins == {"BG9000011163", "BGCROEX03189", "BGCZPX003174"}

    # Ensure all canonical URLs are English (/en/)
    for entry in index:
        assert "/en/" in entry["url"]
