from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from mxm_datakraken.sources.justetf.discovery.discover import ETFProfile
from mxm_datakraken.sources.justetf.discovery.persistence import (
    load_discovery_results,
    save_discovery_results,
)


@pytest.fixture
def profiles() -> list[ETFProfile]:
    return [
        {
            "isin": "TEST123",
            "url": "https://example.com/en/etf-profile.html?isin=TEST123",
            "lastmod": "2025-10-01",
        },
        {
            "isin": "TEST456",
            "url": "https://example.com/en/etf-profile.html?isin=TEST456",
            "lastmod": None,
        },
    ]


def test_load_latest(tmp_path: Path, profiles: list[ETFProfile]) -> None:
    """Should load latest.json when no as_of is provided."""
    save_discovery_results(profiles, tmp_path, as_of=date(2025, 10, 1))
    loaded: list[ETFProfile] = load_discovery_results(tmp_path)
    assert loaded == profiles


def test_load_exact_date(tmp_path: Path, profiles: list[ETFProfile]) -> None:
    """Should load snapshot for exact matching date."""
    save_discovery_results(profiles, tmp_path, as_of=date(2025, 10, 1))
    loaded: list[ETFProfile] = load_discovery_results(tmp_path, as_of=date(2025, 10, 1))
    assert loaded == profiles


def test_load_closest_before(tmp_path: Path, profiles: list[ETFProfile]) -> None:
    """Should pick the closest snapshot <= as_of date."""
    save_discovery_results(profiles, tmp_path, as_of=date(2025, 9, 30))
    save_discovery_results(profiles, tmp_path, as_of=date(2025, 10, 2))

    loaded: list[ETFProfile] = load_discovery_results(tmp_path, as_of=date(2025, 10, 1))
    # Should pick 2025-09-30 snapshot
    snapshot_path: Path = tmp_path / "discovery" / "discovery_2025-09-30.json"
    expected: list[ETFProfile] = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert loaded == expected


def test_load_before_any_snapshot(tmp_path: Path, profiles: list[ETFProfile]) -> None:
    """Should raise FileNotFoundError if no snapshot exists <= as_of."""
    save_discovery_results(profiles, tmp_path, as_of=date(2025, 10, 5))
    with pytest.raises(FileNotFoundError):
        load_discovery_results(tmp_path, as_of=date(2025, 9, 1))
