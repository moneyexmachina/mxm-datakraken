"""
Tests for profile_index.api (get_profile_index) with bucketed storage.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from types import SimpleNamespace
from typing import NoReturn, cast

import pytest
from mxm_config import MXMConfig

from mxm.datakraken.common.latest_bucket import resolve_latest_bucket
from mxm.datakraken.sources.justetf.common.models import ETFProfileIndexEntry
from mxm.datakraken.sources.justetf.profile_index.api import get_profile_index
from mxm.datakraken.sources.justetf.profile_index.persistence import save_profile_index


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
    """On first run with no cache, should build index and save results into
    a bucket and update 'latest'."""
    cfg: MXMConfig = cast(MXMConfig, {})  # patched build ignores cfg

    def _fake_resp(tmp: Path) -> SimpleNamespace:
        payload = tmp / "sitemap.bin"
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
            # no as_of_bucket -> api will fall back to today
        )

    def fake_build(
        *_args: object, **_kwargs: object
    ) -> tuple[list[ETFProfileIndexEntry], SimpleNamespace]:
        _ = _args
        _ = _kwargs
        return fake_index, _fake_resp(tmp_path)

    monkeypatch.setattr(
        "mxm.datakraken.sources.justetf.profile_index.api.build_profile_index",
        fake_build,
        raising=True,
    )

    results: list[ETFProfileIndexEntry] = get_profile_index(cfg, tmp_path)
    assert results == fake_index

    # Latest pointer exists and resolves to a bucket
    pi_root = tmp_path / "profile_index"
    latest_bucket = resolve_latest_bucket(pi_root)
    assert latest_bucket is not None

    # The parsed file exists in that bucket
    parsed_path = pi_root / latest_bucket / "profile_index.parsed.json"
    assert parsed_path.exists()


def test_get_profile_index_force_refresh(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_index: list[ETFProfileIndexEntry],
) -> None:
    """With force_refresh=True, should always rebuild index."""
    cfg: MXMConfig = cast(MXMConfig, {})

    calls: dict[str, int] = {"count": 0}

    def _fake_resp(tmp: Path) -> SimpleNamespace:
        payload = tmp / "sitemap.bin"
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
        *_args: object, **_kwargs: object
    ) -> tuple[list[ETFProfileIndexEntry], SimpleNamespace]:
        _ = _args
        _ = _kwargs
        calls["count"] += 1
        return fake_index, _fake_resp(tmp_path)

    monkeypatch.setattr(
        "mxm.datakraken.sources.justetf.profile_index.api.build_profile_index",
        fake_build,
        raising=True,
    )

    # Call twice with force_refresh=True
    _ = get_profile_index(cfg, tmp_path, force_refresh=True)
    _ = get_profile_index(cfg, tmp_path, force_refresh=True)

    assert calls["count"] == 2


def test_get_profile_index_bucket_selection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_index: list[ETFProfileIndexEntry],
) -> None:
    """
    With explicit as_of_bucket, should return that bucket without rebuilding.
    """
    cfg: MXMConfig = cast(MXMConfig, {})

    # Save two snapshots manually under different buckets
    save_profile_index(fake_index, tmp_path, as_of_bucket="2025-09-30")
    save_profile_index(fake_index, tmp_path, as_of_bucket="2025-10-05")

    # If build is called, fail the test (we expect a pure load)
    def _boom(*_a: object, **_k: object) -> NoReturn:
        _ = _a
        _ = _k
        raise AssertionError(
            "build_profile_index should not be called for existing bucket load"
        )

    monkeypatch.setattr(
        "mxm.datakraken.sources.justetf.profile_index.api.build_profile_index",
        _boom,
        raising=True,
    )

    # Request the older bucket explicitly
    results: list[ETFProfileIndexEntry] = get_profile_index(
        cfg,
        tmp_path,
        as_of_bucket="2025-09-30",
        force_refresh=False,
    )
    assert results == fake_index

    # Verify file exists exactly at the chosen bucket path
    chosen_path = (
        tmp_path / "profile_index" / "2025-09-30" / "profile_index.parsed.json"
    )
    assert chosen_path.exists()
