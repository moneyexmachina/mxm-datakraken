"""
Tests for loading ETF Profile Index snapshots from disk.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from mxm_datakraken.sources.justetf.profile_index.discover import ETFProfileIndexEntry
from mxm_datakraken.sources.justetf.profile_index.persistence import (
    load_profile_index,
    save_profile_index,
)


@pytest.fixture
def index() -> list[ETFProfileIndexEntry]:
    return [
        {
            "isin": "TEST123",
            "url": "https://example.com/en/etf-profile.html?isin=TEST123",
            "lastmod": "2025-10-01",
        },
        {
            "isin": "TEST456",
            "url": "https://example.com/en/etf-profile.html?isin=TEST456",
        },
    ]


def test_load_latest(tmp_path: Path, index: list[ETFProfileIndexEntry]) -> None:
    """Should load latest.json when no as_of is provided."""
    save_profile_index(index, tmp_path, as_of=date(2025, 10, 1))
    loaded: list[ETFProfileIndexEntry] = load_profile_index(tmp_path)
    assert loaded == index


def test_load_exact_date(tmp_path: Path, index: list[ETFProfileIndexEntry]) -> None:
    """Should load snapshot for exact matching date."""
    save_profile_index(index, tmp_path, as_of=date(2025, 10, 1))
    loaded: list[ETFProfileIndexEntry] = load_profile_index(
        tmp_path, as_of=date(2025, 10, 1)
    )
    assert loaded == index


def test_load_closest_before(tmp_path: Path, index: list[ETFProfileIndexEntry]) -> None:
    """Should pick the closest snapshot <= as_of date."""
    save_profile_index(index, tmp_path, as_of=date(2025, 9, 30))
    save_profile_index(index, tmp_path, as_of=date(2025, 10, 2))

    loaded: list[ETFProfileIndexEntry] = load_profile_index(
        tmp_path, as_of=date(2025, 10, 1)
    )
    # Should pick 2025-09-30 snapshot
    snapshot_path: Path = tmp_path / "profile_index" / "profile_index_2025-09-30.json"
    expected: list[ETFProfileIndexEntry] = json.loads(
        snapshot_path.read_text(encoding="utf-8")
    )
    assert loaded == expected


def test_load_before_any_snapshot(
    tmp_path: Path, index: list[ETFProfileIndexEntry]
) -> None:
    """Should raise FileNotFoundError if no snapshot exists <= as_of."""
    save_profile_index(index, tmp_path, as_of=date(2025, 10, 5))
    with pytest.raises(FileNotFoundError):
        load_profile_index(tmp_path, as_of=date(2025, 9, 1))
