"""
Tests for profile_index.api (get_profile_index).
"""

from __future__ import annotations

import datetime as dt
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Tuple, cast

import pytest
from mxm_config import MXMConfig

from mxm_datakraken.sources.justetf.common.models import ETFProfileIndexEntry
from mxm_datakraken.sources.justetf.profile_index.api import get_profile_index
from mxm_datakraken.sources.justetf.profile_index.persistence import save_profile_index


@pytest.fixture
def fake_index() -> list[ETFProfileIndexEntry]:
    return [
        {
            "isin": "TEST123",
            "url": "https://example.com/en/etf-profile.html?isin=TEST123",
            "lastmod": "2025-10-01",
        }
    ]


def test_get_profile_index_first_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_index: list[ETFProfileIndexEntry],
) -> None:
    """On first run with no cache, should build index and save results."""
    cfg: MXMConfig = cast(MXMConfig, {})  # patched build ignores cfg

    def _fake_resp(tmp_path: Path) -> SimpleNamespace:
        payload = tmp_path / "sitemap.bin"
        payload.write_bytes(b"<xml/>")
        return SimpleNamespace(
            id="resp-1",
            request_id="req-1",
            path=str(payload),
            checksum=None,
            sequence=None,
            size_bytes=payload.stat().st_size,
            created_at=dt.datetime.now(dt.timezone.utc),
            verify=lambda _: True,  # type: ignore[no-any-return]
        )

    def fake_build(
        *_args: Any, **_kwargs: Any
    ) -> Tuple[list[ETFProfileIndexEntry], SimpleNamespace]:
        _ = _args
        _ = _kwargs
        return fake_index, _fake_resp(tmp_path)

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profile_index.api.build_profile_index",
        fake_build,
        raising=True,
    )

    results: list[ETFProfileIndexEntry] = get_profile_index(cfg, tmp_path)
    assert results == fake_index

    # Should also have written latest.json
    latest = tmp_path / "profile_index" / "latest.json"
    assert latest.exists()


def test_get_profile_index_force_refresh(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_index: list[ETFProfileIndexEntry],
) -> None:
    """With force_refresh=True, should always rebuild index."""
    cfg: MXMConfig = cast(MXMConfig, {})

    calls: dict[str, int] = {"count": 0}

    def _fake_resp(tmp_path: Path) -> SimpleNamespace:
        payload = tmp_path / "sitemap.bin"
        payload.write_bytes(b"<xml/>")
        return SimpleNamespace(
            id="resp-1",
            request_id="req-1",
            path=str(payload),
            checksum=None,
            sequence=None,
            size_bytes=payload.stat().st_size,
            created_at=dt.datetime.now(dt.timezone.utc),
            verify=lambda _: True,  # type: ignore[no-any-return]
        )

    def fake_build(
        *_args: Any, **_kwargs: Any
    ) -> Tuple[list[ETFProfileIndexEntry], SimpleNamespace]:
        calls["count"] += 1
        _ = _args
        _ = _kwargs
        return fake_index, _fake_resp(tmp_path)

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profile_index.api.build_profile_index",
        fake_build,
        raising=True,
    )

    # Call twice with force_refresh=True
    _ = get_profile_index(cfg, tmp_path, force_refresh=True)
    _ = get_profile_index(cfg, tmp_path, force_refresh=True)

    assert calls["count"] == 2


def test_get_profile_index_as_of(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_index: list[ETFProfileIndexEntry],
) -> None:
    """With as_of set, should return the closest snapshot <= that date."""
    cfg: MXMConfig = cast(MXMConfig, {})

    def _fake_resp(tmp_path: Path) -> SimpleNamespace:
        payload = tmp_path / "sitemap.bin"
        payload.write_bytes(b"<xml/>")
        return SimpleNamespace(
            id="resp-1",
            request_id="req-1",
            path=str(payload),
            checksum=None,
            sequence=None,
            size_bytes=payload.stat().st_size,
            created_at=dt.datetime.now(dt.timezone.utc),
            verify=lambda _: True,  # type: ignore[no-any-return]
        )

    def fake_build(
        *_args: Any, **_kwargs: Any
    ) -> Tuple[list[ETFProfileIndexEntry], SimpleNamespace]:
        _ = _args
        _ = _kwargs
        return fake_index, _fake_resp(tmp_path)

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.profile_index.api.build_profile_index",
        fake_build,
        raising=True,
    )

    # Save two snapshots manually
    save_profile_index(fake_index, tmp_path, as_of=date(2025, 9, 30))
    save_profile_index(fake_index, tmp_path, as_of=date(2025, 10, 5))

    results: list[ETFProfileIndexEntry] = get_profile_index(
        cfg, tmp_path, as_of=date(2025, 10, 1)
    )
    assert results == fake_index

    # It should pick the 2025-09-30 snapshot, not the 2025-10-05 one
    chosen_path: Path = tmp_path / "profile_index" / "profile_index_2025-09-30.json"
    assert chosen_path.exists()
