from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from mxm_datakraken.sources.justetf.discovery.api import get_profiles
from mxm_datakraken.sources.justetf.discovery.discover import ETFProfile


@pytest.fixture
def fake_profiles() -> list[ETFProfile]:
    return [
        {
            "isin": "TEST123",
            "url": "https://example.com/en/etf-profile.html?isin=TEST123",
            "lastmod": "2025-10-01",
        }
    ]


def test_get_profiles_first_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_profiles: list[ETFProfile]
) -> None:
    """On first run with no cache, should call discovery and save results."""

    def fake_discover(*args: Any, **kwargs: Any) -> list[ETFProfile]:
        return fake_profiles

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.discovery.api.discover_etf_profiles",
        fake_discover,
    )

    results: list[ETFProfile] = get_profiles(tmp_path)
    assert results == fake_profiles

    # Should also have written latest.json
    latest = tmp_path / "discovery" / "latest.json"
    assert latest.exists()


def test_get_profiles_force_refresh(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_profiles: list[ETFProfile]
) -> None:
    """With force_refresh=True, should always re-run discovery."""

    calls: dict[str, int] = {"count": 0}

    def fake_discover(*args: Any, **kwargs: Any) -> list[ETFProfile]:
        calls["count"] += 1
        return fake_profiles

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.discovery.api.discover_etf_profiles",
        fake_discover,
    )

    # Call twice with force_refresh=True
    _ = get_profiles(tmp_path, force_refresh=True)
    _ = get_profiles(tmp_path, force_refresh=True)

    assert calls["count"] == 2


def test_get_profiles_as_of(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_profiles: list[ETFProfile]
) -> None:
    """With as_of set, should return the closest snapshot <= that date."""

    def fake_discover(*args: Any, **kwargs: Any) -> list[ETFProfile]:
        return fake_profiles

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.discovery.api.discover_etf_profiles",
        fake_discover,
    )

    # Save two snapshots manually
    from mxm_datakraken.sources.justetf.discovery.persistence import (
        save_discovery_results,
    )

    save_discovery_results(fake_profiles, tmp_path, as_of=date(2025, 9, 30))
    save_discovery_results(fake_profiles, tmp_path, as_of=date(2025, 10, 5))

    results: list[ETFProfile] = get_profiles(tmp_path, as_of=date(2025, 10, 1))
    assert results == fake_profiles

    # It should pick the 2025-09-30 snapshot, not the 2025-10-05 one
    chosen_path: Path = tmp_path / "discovery" / "discovery_2025-09-30.json"
    assert chosen_path.exists()
